//! Requested committed history streams under the existing captured image root.
use super::*;
use contextdb_core::read_memory::ReadMemoryBudget;

impl RetainedReadSource {
    pub(super) fn check_history_directory_in_read(
        &self,
        read: &redb::ReadTransaction,
        memory: Arc<dyn ReadMemoryBudget>,
        checkpoint: &dyn Fn() -> Result<()>,
    ) -> Result<()> {
        checkpoint()?;
        let _controls = contextdb_core::read_memory::ReadCredit::try_new(
            memory,
            content_identity::history_open_control::DIRECTORY_CHECK_SCRATCH_BYTES,
            "reader",
            "history_directory_agreement",
            "Raise the declared reader memory limit.",
        )?;
        self.history_open_control.check_directory(read)?;
        checkpoint()
    }
    pub(super) fn check_history_directory(
        &self,
        memory: Arc<dyn ReadMemoryBudget>,
        checkpoint: &dyn Fn() -> Result<()>,
    ) -> Result<()> {
        graph_read::with_pages(memory.clone(), || {
            self.with_read(|read| self.check_history_directory_in_read(read, memory, checkpoint))
        })
    }

    fn visit_history_bytes(
        &self,
        kind: content_identity::HistoryKind,
        definition: TableDefinition<&'static str, &'static [u8]>,
        memory: Arc<dyn ReadMemoryBudget>,
        checkpoint: &dyn Fn() -> Result<()>,
        visit: &mut dyn FnMut(&redb::ReadTransaction, &str, &[u8]) -> Result<()>,
    ) -> Result<()> {
        graph_read::with_pages(memory.clone(), || {
            self.with_read(|read| {
                self.check_history_directory_in_read(read, memory.clone(), checkpoint)?;
                if kind == content_identity::HistoryKind::DdlLog {
                    self.history_open_control.check_ddl(read)?;
                    checkpoint()?;
                }
                let owner = writer_source::SourceWindowOwner::Read(memory.clone());
                let _pool = owner.reserve(READ_HISTORY_PROOF_POOL_BYTES, "history_stream_proof")?;
                let proof =
                    content_identity::ProofMemory::with_limit(READ_HISTORY_PROOF_POOL_BYTES);
                #[cfg(any(test, feature = "test-seams"))] { crate::read_work_probe::bind_graph_root(read.storage_snapshot_identity()); crate::read_work_probe::note_graph(redb::TableHandle::name(&definition), "population_walk", &[], None, None); }
                let table = open_optional_read_table(read, definition)?;
                let mut ddl_last = None;
                let _ddl_controls = if kind == content_identity::HistoryKind::DdlLog {
                    Some(contextdb_core::read_memory::ReadCredit::try_new(
                        memory.clone(),
                        std::mem::size_of::<Option<(u64, Lsn, [u8;32], [u8;32])>>()
                            + std::mem::size_of::<blake3::Hasher>(),
                        "reader", "ddl_complete_controls", "Raise the declared reader memory limit.",
                    )?)
                } else { None };
                let mut stored = match &table {
                    Some(table) => Some(
                        table
                            .iter()
                            .map_err(RedbPersistence::storage_error)?
                            .inspect(|_entry| {
#[cfg(any(test, feature = "test-seams"))] if let Ok((key, _)) = _entry { crate::read_work_probe::graph_storage_key(redb::TableHandle::name(&definition), "iteration_step", key.value()); }                            }),
                    ),
                    None => None,
                };
                let complete = self.content_identity.visit_history::<Error>(
                    kind,
                    false,
                    &proof,
                    &mut || checkpoint(),
                    &mut |coordinate, committed| {
                        let Some(next) = stored.as_mut().and_then(Iterator::next) else {
                            return Err(
                                self.history_damage("committed history occurrence is missing")
                            );
                        };
                        let (key, bytes) = next.map_err(RedbPersistence::storage_error)?;
                        let key = key.value();
                        if key.as_bytes() != coordinate {
                            return Err(self.history_damage(
                                "stored history coordinate differs from committed range",
                            ));
                        }
                        note_open_history(
                            match kind {
                                content_identity::HistoryKind::ChangeLog => {
                                    OpenHistoryClass::ChangeLog
                                }
                                content_identity::HistoryKind::DdlLog => OpenHistoryClass::DdlLog,
                                _ => unreachable!("text history only"),
                            },
                            bytes.value().len(),
                        );
                        let scratch = self.graph_window_memory_for_history(&owner);
                        scratch.scope(|| {
                            let actual = match kind {
                                content_identity::HistoryKind::ChangeLog => {
                                    RedbPersistence::stored_change_log_value_hash(bytes.value())?
                                }
                                content_identity::HistoryKind::DdlLog => {
                                    let change: DdlChange = RedbPersistence::decode(bytes.value())?;
                                    content_identity::history_record_value_hash(&change)?
                                }
                                _ => unreachable!("text history only"),
                            };
                            if actual != committed {
                                return Err(self.history_damage(
                                    "stored history bytes differ from their commitment",
                                ));
                            }
                            if kind == content_identity::HistoryKind::DdlLog {
                                ddl_last = Some((
                                    u64::try_from(key.len()).map_err(|_| self.history_damage("DDL coordinate length overflow"))?,
                                    RedbPersistence::ddl_lsn_from_key(key)?,
                                    content_identity::history_open_control::ddl_coordinate_digest(key.as_bytes(), Some(checkpoint))?,
                                    actual,
                                ));
                            }
                            visit(read, key, bytes.value())
                        })?;
                        Ok(true)
                    },
                )?;
                if !complete {
                    return Err(
                        self.unprovable_history(&self._source_memory.memory.accountant, kind)
                    );
                }
                if stored.as_mut().and_then(Iterator::next).is_some() {
                    return Err(
                        self.history_damage("stored history has an extra uncommitted occurrence")
                    );
                }
                if kind == content_identity::HistoryKind::DdlLog {
                    self.history_open_control.check_complete_ddl(ddl_last)?;
                }
                checkpoint()?;
                Ok(())
            })
        })
    }

    fn graph_window_memory_for_history(
        &self,
        owner: &writer_source::SourceWindowOwner,
    ) -> Arc<crate::read_image_memory::ImageMemory> {
        let writer_source::SourceWindowOwner::Read(memory) = owner else {
            unreachable!("image read owner")
        };
        crate::read_image_memory::ImageMemory::charging_reader_for(
            self._source_memory.memory.accountant.clone(),
            memory.clone(),
            "history_source",
            "history_stream",
            "Raise the reader limit to read history windows.",
        )
    }

    /// Check just this selected deletion against its captured commit directory.
    pub(super) fn check_requested_tombstone_commit_in_read(
        &self,
        read: &redb::ReadTransaction,
        record: &graph_history_record::StoredChangeLogEntry,
    ) -> std::result::Result<(), LoadReadImageError> {
        let Some(witness) = record.witness.as_ref() else {
            return Ok(());
        };
        let missing = || {
            direct_read_requires_writer_for(
                ReadImageRequiresWriterCause::MissingCommitIndex,
                "commit index is missing the requested edge tombstone",
            )
        };
        let commits = match read.open_table(COMMIT_INDEX_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Err(missing()),
            Err(error) => return Err(read_table_error(error)),
        };
        let committed = commits
            .get(record.entry.lsn().0)
            .map_err(read_storage_error)?
            .ok_or_else(missing)?;
        if committed.value() != witness.deletion_tx.0 {
            return Err(read_image_corruption(
                ReadImageCorruptionCause::MalformedRecord,
                "commit index names another transaction for the requested edge tombstone",
            ));
        }
        Ok(())
    }

    pub(crate) fn visit_changes_since(
        &self,
        since: Lsn,
        memory: Arc<dyn ReadMemoryBudget>,
        checkpoint: &dyn Fn() -> Result<()>,
        keep: &mut dyn FnMut(ChangeLogEntry) -> Result<()>,
    ) -> Result<Option<Lsn>> {
        let mut first = None;
        self.visit_history_bytes(
            content_identity::HistoryKind::ChangeLog,
            CHANGE_LOG_TABLE,
            memory.clone(),
            checkpoint,
            &mut |read, key, bytes| {
                // Public LSN is decoded; its physical address is not a selector.
                let record = RedbPersistence::decode_stored_change_log(bytes)?;
                if first.is_none() {
                    first = Some(record.entry.lsn());
                }
                if record.entry.lsn() > since {
                    self.check_requested_tombstone_commit_in_read(read, &record)
                        .map_err(|error| graph_read::image_diagnosis(self, error))?;
                    self.verify_stored_graph_history_in_read(read, key, bytes, memory.clone())?;
                    keep(record.entry)?;
                }
                Ok(())
            },
        )?;
        Ok(first)
    }

    pub(crate) fn visit_ddl_since(
        &self,
        since: Lsn,
        memory: Arc<dyn ReadMemoryBudget>,
        checkpoint: &dyn Fn() -> Result<()>,
        keep: &mut dyn FnMut(Lsn, DdlChange) -> Result<()>,
    ) -> Result<Option<Lsn>> {
        let mut first = None;
        self.visit_history_bytes(
            content_identity::HistoryKind::DdlLog,
            DDL_LOG_TABLE,
            memory,
            checkpoint,
            &mut |_, key, bytes| {
                let raw_lsn = key.split_once(':').map_or(key, |(lsn, _)| lsn);
                let lsn = Lsn(raw_lsn.parse().map_err(|e| self.history_damage(e))?);
                if first.is_none() {
                    first = Some(lsn);
                }
                if lsn > since {
                    keep(lsn, RedbPersistence::decode(bytes)?)?;
                }
                Ok(())
            },
        )?;
        Ok(first)
    }

    pub(crate) fn commit_entry_from(
        &self,
        bound: std::ops::Bound<Lsn>,
        reverse: bool,
        memory: Arc<dyn ReadMemoryBudget>,
        checkpoint: &dyn Fn() -> Result<()>,
    ) -> Result<Option<(Lsn, TxId)>> {
        checkpoint()?;
        let _controls = contextdb_core::read_memory::ReadCredit::try_new(
            memory.clone(),
            2 * (8 + 2 * std::mem::size_of::<usize>()),
            "reader",
            "commit_seek_controls",
            "Raise the declared reader memory limit.",
        )?;
        let physical_bound = bound.map(|lsn| lsn.0);
        let proof_bound = physical_bound.map(|lsn| lsn.to_be_bytes().to_vec());
        graph_read::with_pages(memory.clone(), || {
            self.with_read(|read| {
                self.check_history_directory_in_read(read, memory.clone(), checkpoint)?;
                let owner = writer_source::SourceWindowOwner::Read(memory);
                let _pool = owner.reserve(READ_HISTORY_PROOF_POOL_BYTES, "commit_floor_proof")?;
                let proof =
                    content_identity::ProofMemory::with_limit(READ_HISTORY_PROOF_POOL_BYTES);
                checkpoint()?;
                let table = open_optional_read_table(read, COMMIT_INDEX_TABLE)?;
                // Both descents use this operation's captured root and the same
                // inclusive LSN bound. Only the addressed physical floor is read.
                let stored = match &table {
                    Some(table) => {
                        checkpoint()?;
                        let bounds = if reverse {
                            (std::ops::Bound::Unbounded, physical_bound)
                        } else {
                            (physical_bound, std::ops::Bound::Unbounded)
                        };
                        let mut range = table
                            .range(bounds)
                            .map_err(RedbPersistence::storage_error)?;
                        checkpoint()?;
                        let entry = if reverse {
                            range.next_back()
                        } else {
                            range.next()
                        };
                        let entry = entry.transpose().map_err(RedbPersistence::storage_error)?;
                        #[cfg(any(test, feature = "test-seams"))]
                        if let Some((key, _)) = &entry {
                            crate::read_work_probe::note_graph("commit_index", "iteration_step", &key.value().to_be_bytes(), Some(contextdb_core::Lsn(key.value())), None);
                        }
                        entry
                    }
                    None => None,
                };
                if stored.is_some() {
                    note_open_history(
                        OpenHistoryClass::CommitIndex,
                        2 * std::mem::size_of::<u64>(),
                    );
                }
                let mut pair = None;
                let mut found = false;
                let complete = self.content_identity.visit_history_from::<Error>(
                    content_identity::HistoryKind::CommitIndex,
                    proof_bound,
                    reverse,
                    &proof,
                    &mut || checkpoint(),
                    &mut |coordinate, committed| {
                        checkpoint()?;
                        let Some((key, tx)) = stored.as_ref() else {
                            return Err(
                                self.history_damage("committed commit-index occurrence is missing")
                            );
                        };
                        if coordinate != key.value().to_be_bytes()
                            || content_identity::commit_index_value_hash(TxId(tx.value()))
                                != committed
                        {
                            return Err(self.history_damage(
                                "commit-index floor differs from committed range",
                            ));
                        }
                        pair = Some((Lsn(key.value()), TxId(tx.value())));
                        found = true;
                        Ok(false)
                    },
                )?;
                if !complete {
                    return Err(self.unprovable_history(
                        &self._source_memory.memory.accountant,
                        content_identity::HistoryKind::CommitIndex,
                    ));
                }
                if !found && stored.is_some() {
                    return Err(self.history_damage("commit index has an uncommitted occurrence"));
                }
                checkpoint()?;
                Ok(pair)
            })
        })
    }
}
