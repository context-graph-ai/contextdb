//! Immutable commit membership in one retained, registered storage snapshot.
use super::*;
use contextdb_core::read_memory::ReadCredit;
use contextdb_tx::{ReadCommitContext, ReadCommitCursor, ReadCommitDirectory};
use std::ops::Bound;

pub(crate) struct RetainedCommitDirectory {
    source: Arc<RetainedReadSource>,
    count: usize,
    first: Option<(Lsn, TxId)>,
    last: Option<(Lsn, TxId)>,
    _controls: ReadCredit,
}

impl RetainedCommitDirectory {
    pub(crate) fn new(
        source: Arc<RetainedReadSource>,
        bounds: &ReadHistoryOnRequest,
        context: &ReadCommitContext<'_>,
    ) -> Result<Arc<Self>> {
        context.checkpoint()?;
        let controls = ReadCredit::try_new(
            context.read_memory.clone(),
            std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>(),
            "reader",
            "commit_directory_controls",
            "Raise the declared reader memory limit.",
        )?;
        let directory = Arc::new(Self {
            source,
            count: bounds.commit_index_count,
            first: bounds.commit_index_first,
            last: bounds.commit_index_last,
            _controls: controls,
        });
        let _capability_credit = ReadCredit::try_new(
            context.read_memory.clone(),
            std::mem::size_of::<OpenEndpointProof<'_, '_>>(),
            "reader",
            "commit_open_endpoint_controls",
            "Raise the declared reader memory limit.",
        )?;
        let open = OpenEndpointProof {
            source: &directory.source,
            context,
        };
        for pair in [directory.first, directory.last].into_iter().flatten() {
            if directory.get_value(pair.0, context, Some(&open))? != Some(pair.1) {
                return Err(directory
                    .source
                    .history_damage("captured commit endpoint differs from its proof"));
            }
        }
        if directory.count == 0 {
            // Authenticated count authorizes only this constructor's actual
            // empty/floor probe; ordinary floor uses the first-use gate.
            if directory.source.history_open_control.control().commit_count != 0 {
                return Err(directory
                    .source
                    .history_damage("empty open capability has nonempty controls"));
            }
            let observed = graph_read::with_pages(context.read_memory.clone(), || {
                directory.source.with_read(|read| {
                    directory
                        .source
                        .history_open_control
                        .check_snapshot(read.storage_snapshot_identity())?;
                    let table = read
                        .open_table(COMMIT_INDEX_TABLE)
                        .map_err(RedbPersistence::storage_error)?;
                    let mut range = table
                        .range::<u64>(..)
                        .map_err(RedbPersistence::storage_error)?;
                    let pair = range
                        .next_back()
                        .transpose()
                        .map_err(RedbPersistence::storage_error)?;
                    if let Some((_key, _)) = &pair {
                        #[cfg(any(test, feature = "test-seams"))]
                        crate::read_work_probe::note_graph(
                            "commit_index",
                            "iteration_step",
                            &_key.value().to_be_bytes(),
                            Some(contextdb_core::Lsn(_key.value())),
                            None,
                        );
                    }
                    context.checkpoint()?;
                    Ok(pair.map(|(key, value)| (Lsn(key.value()), TxId(value.value()))))
                })
            })?;
            if observed.is_some() {
                return Err(directory
                    .source
                    .history_damage("empty commit directory has a committed member"));
            }
        }
        #[cfg(any(test, feature = "test-seams"))]
        directory.observe_residency();
        context.checkpoint()?;
        Ok(directory)
    }

    #[cfg(any(test, feature = "test-seams"))]
    fn observe_residency(&self) {
        // The representation holds only scalars and a source. Caller-owned
        // output and source cache pages are not decoded directory entries.
        crate::read_work_probe::note_commit_directory_residency(0, self.count as u64);
    }
}

struct OpenEndpointProof<'a, 'operation> {
    source: &'a RetainedReadSource,
    context: &'a ReadCommitContext<'operation>,
}

impl RetainedCommitDirectory {
    fn get_value(
        &self,
        lsn: Lsn,
        context: &ReadCommitContext<'_>,
        open: Option<&OpenEndpointProof<'_, '_>>,
    ) -> Result<Option<TxId>> {
        context.checkpoint()?;
        let memory = context.read_memory.clone();
        graph_read::with_pages(memory.clone(), || {
            self.source.with_read(|read| {
                context.checkpoint()?;
                match open {
                    Some(capability) => {
                        if !std::ptr::eq(capability.source, self.source.as_ref())
                            || !std::ptr::eq(capability.context, context)
                        {
                            return Err(self.source.history_damage(
                                "open endpoint capability escaped its source or operation",
                            ));
                        }
                        self.source
                            .history_open_control
                            .check_snapshot(read.storage_snapshot_identity())?;
                    }
                    None => self.source.check_history_directory_in_read(
                        read,
                        memory.clone(),
                        &|| context.checkpoint(),
                    )?,
                }
                let table = open_optional_read_table(read, COMMIT_INDEX_TABLE)?;
                #[cfg(any(test, feature = "test-seams"))]
                crate::read_work_probe::note_graph(
                    "commit_index",
                    "get",
                    &lsn.0.to_be_bytes(),
                    Some(contextdb_core::Lsn(lsn.0)),
                    None,
                );
                let tx = table
                    .as_ref()
                    .map(|table| {
                        table
                            .get(lsn.0)
                            .map(|value| value.map(|value| TxId(value.value())))
                            .map_err(RedbPersistence::storage_error)
                    })
                    .transpose()?
                    .flatten();
                context.checkpoint()?;
                let _mutation_credit = ReadCredit::try_new(
                    memory.clone(),
                    std::mem::size_of::<content_identity::ContentIdentityMutation>()
                        + 128
                        + 2 * crate::sync_types::held::ALLOCATION_BYTES,
                    "reader",
                    "commit_membership_witness",
                    "Raise the declared reader memory limit.",
                )?;
                let mutation = match tx {
                    Some(tx) => content_identity::commit_index_mutation(lsn, tx),
                    None => content_identity::remove_commit_index_mutation(lsn),
                };
                match open {
                    Some(_) => self
                        .source
                        .history_open_control
                        .check_endpoint(lsn, tx, &mutation)?,
                    None => self
                        .source
                        .content_identity
                        .verify_with_read_memory(&mutation, memory)?,
                }
                context.checkpoint()?;
                Ok(tx)
            })
        })
    }
}

impl ReadCommitDirectory for RetainedCommitDirectory {
    fn get(&self, lsn: Lsn, context: &ReadCommitContext<'_>) -> Result<Option<TxId>> {
        self.get_value(lsn, context, None)
    }
    fn floor(
        &self,
        upper: Bound<Lsn>,
        context: &ReadCommitContext<'_>,
    ) -> Result<Option<(Lsn, TxId)>> {
        self.source
            .commit_entry_from(upper, true, context.read_memory.clone(), &|| {
                context.checkpoint()
            })
    }

    fn census(&self, context: &ReadCommitContext<'_>) -> Result<(usize, Lsn)> {
        context.checkpoint()?;
        #[cfg(any(test, feature = "test-seams"))]
        self.observe_residency();
        self.source
            .check_history_directory(context.read_memory.clone(), &|| context.checkpoint())?;
        Ok((self.count, self.first.map_or(Lsn(0), |(lsn, _)| lsn)))
    }

    fn cursor(
        &self,
        lower: Bound<Lsn>,
        upper: Bound<Lsn>,
        context: &ReadCommitContext<'_>,
    ) -> Result<Box<dyn ReadCommitCursor>> {
        context.checkpoint()?;
        self.source
            .check_history_directory(context.read_memory.clone(), &|| context.checkpoint())?;
        let controls = ReadCredit::try_new(
            context.read_memory.clone(),
            std::mem::size_of::<RetainedCommitCursor>() + 2 * std::mem::size_of::<usize>(),
            "reader",
            "commit_cursor_controls",
            "Raise the declared reader memory limit.",
        )?;
        let covers_first = self.first.is_none_or(|(first, _)| match lower {
            Bound::Unbounded => true,
            Bound::Included(low) => low <= first,
            Bound::Excluded(low) => low < first,
        });
        let covers_last = self.last.is_none_or(|(last, _)| match upper {
            Bound::Unbounded => true,
            Bound::Included(high) => high >= last,
            Bound::Excluded(high) => high > last,
        });
        Ok(Box::new(RetainedCommitCursor {
            source: self.source.clone(),
            lower,
            upper,
            after: None,
            finished: false,
            complete_range: covers_first && covers_last,
            seen_count: 0,
            seen_first: None,
            seen_last: None,
            _controls: controls,
        }))
    }

    #[cfg(feature = "test-seams")]
    fn retained_entry_bytes(&self) -> usize {
        0
    }
}

struct RetainedCommitCursor {
    source: Arc<RetainedReadSource>,
    lower: Bound<Lsn>,
    upper: Bound<Lsn>,
    after: Option<Lsn>,
    finished: bool,
    complete_range: bool,
    seen_count: u64,
    seen_first: Option<(Lsn, TxId)>,
    seen_last: Option<(Lsn, TxId)>,
    _controls: ReadCredit,
}

impl RetainedCommitCursor {
    fn finish(&mut self, context: &ReadCommitContext<'_>) -> Result<Option<(Lsn, TxId)>> {
        context.checkpoint()?;
        if self.complete_range {
            let _controls = ReadCredit::try_new(
                context.read_memory.clone(),
                std::mem::size_of::<blake3::Hasher>()
                    + 2 * std::mem::size_of::<Option<(Lsn, TxId)>>(),
                "reader",
                "complete_commit_controls",
                "Raise the declared reader memory limit.",
            )?;
            self.source.history_open_control.check_complete_commits(
                self.seen_count,
                self.seen_first,
                self.seen_last,
            )?;
        }
        context.checkpoint()?;
        self.finished = true;
        Ok(None)
    }
}

impl ReadCommitCursor for RetainedCommitCursor {
    fn next_entry(&mut self, context: &ReadCommitContext<'_>) -> Result<Option<(Lsn, TxId)>> {
        context.checkpoint()?;
        self.source
            .check_history_directory(context.read_memory.clone(), &|| context.checkpoint())?;
        if self.finished {
            return Ok(None);
        }
        let lower = self.after.map_or(self.lower, Bound::Excluded);
        let pair =
            self.source
                .commit_entry_from(lower, false, context.read_memory.clone(), &|| {
                    context.checkpoint()
                })?;
        let Some((lsn, tx)) = pair else {
            return self.finish(context);
        };
        let in_range = match self.upper {
            Bound::Included(upper) => lsn <= upper,
            Bound::Excluded(upper) => lsn < upper,
            Bound::Unbounded => true,
        };
        if !in_range {
            return self.finish(context);
        }
        self.seen_count = self
            .seen_count
            .checked_add(1)
            .ok_or_else(|| self.source.history_damage("complete commit count overflow"))?;
        if self.seen_first.is_none() {
            self.seen_first = Some((lsn, tx));
        }
        self.seen_last = Some((lsn, tx));
        self.after = Some(lsn);
        context.checkpoint()?;
        Ok(Some((lsn, tx)))
    }
}
