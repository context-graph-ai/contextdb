use super::*;

impl RedbPersistence {
    /// Rewrite the persisted change log to exactly `entries`, in order. Used by
    /// currency-table version compaction to drop, in lockstep with the pruned
    /// superseded row versions, the `RowInsert`/`RowDelete` entries that
    /// referenced them — so `changes_since`'s change-log replay (which reads the
    /// row version AT each logged LSN) never encounters an entry whose version
    /// was removed and silently substitutes the latest one. The table is fully
    /// cleared and rebuilt from `entries`; keys are re-derived as
    /// `{lsn:020}:{index}` with `index` restarting per LSN (entries sharing an
    /// LSN are contiguous, exactly as the commit path wrote them), so the load
    /// order is preserved byte-for-byte with what a fresh sequence of commits
    /// would have produced.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn rewrite_change_log(&self, entries: &[ChangeLogEntry]) -> Result<()> {
        let _decode_scope = self.writer_decode_scope()?;
        self.prepare_proof_owner()?;
        self.with_db(|db| {
            let previous_root = db.begin_read().map_err(Self::storage_error)?;
            let write = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
            let mut previous = None;
            let mut index = 0;
            self.publish_public_change_log_in_write(
                &write,
                &previous_root,
                true,
                entries.iter().map(|entry| {
                    let lsn = entry.lsn();
                    if previous == Some(lsn) {
                        index += 1;
                    } else {
                        index = 0;
                        previous = Some(lsn);
                    }
                    (lsn, index, entry)
                }),
            )?;
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            {
                #[cfg(any(test, feature = "test-seams"))]
                graph_observation_checkpoint(10)?;
                let graph_commit_result = write.commit();
                #[cfg(any(test, feature = "test-seams"))]
                if graph_commit_result.is_ok() {
                    graph_publication_attempt.committed();
                    graph_observation_checkpoint(11)?;
                }
                graph_commit_result
            }
            .map_err(Self::storage_error)?;
            Ok(())
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn load_ddl_log(&self) -> Result<Vec<(Lsn, DdlChange)>> {
        let _decode_scope = self.reader_decode_scope()?;
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(DDL_LOG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let key = key.value();
                let lsn = key
                    .split_once(':')
                    .map(|(lsn, _)| lsn)
                    .unwrap_or(key)
                    .parse::<u64>()
                    .map_err(|err| Error::Other((format!("invalid ddl log key: {err}")).into()))?;
                #[cfg(any(test, feature = "test-seams"))]
                crate::writer_query_test_seams::note_ddl_occurrence();
                entries.push((Lsn(lsn), Self::decode(value.value())?));
            }
            Ok(entries)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn load_trigger_audit_state(
        &self,
        ring_capacity: usize,
    ) -> Result<(Vec<TriggerAuditEntry>, u64)> {
        let _decode_scope = self.reader_decode_scope()?;
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            if let Ok(config_table) = read_txn.open_table(CONFIG_TABLE) {
                let ring = config_table
                    .get(TRIGGER_AUDIT_RING_CONFIG_KEY)
                    .map_err(Self::storage_error)?
                    .map(|value| Self::decode::<Vec<TriggerAuditEntry>>(value.value()))
                    .transpose()?;
                let next_index = config_table
                    .get(TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY)
                    .map_err(Self::storage_error)?
                    .map(|value| Self::decode::<u64>(value.value()))
                    .transpose()?;
                if let (Some(mut ring), Some(next_index)) = (ring, next_index) {
                    let overflow = ring.len().saturating_sub(ring_capacity);
                    if overflow > 0 {
                        ring.drain(0..overflow);
                    }
                    return Ok((ring, next_index));
                }
            }

            let table = match read_txn.open_table(TRIGGER_AUDIT_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok((Vec::new(), 0)),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut ring = VecDeque::new();
            let mut next_index = 0;
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                if let Some(index) = key
                    .value()
                    .split(':')
                    .next()
                    .and_then(|raw| raw.parse::<u64>().ok())
                {
                    next_index = next_index.max(index.saturating_add(1));
                }
                ring.push_back(Self::decode(value.value())?);
                if ring.len() > ring_capacity {
                    ring.pop_front();
                }
            }
            Ok((ring.into_iter().collect(), next_index))
        })
    }

    /// Delete every durable audit row written before `cutoff_millis`,
    /// whatever table it came from — including rows a DROP TABLE orphaned.
    /// Returns the number removed. Rows written before stamps existed carry no
    /// stamp and are left alone rather than guessed at.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn prune_trigger_audit_history(&self, cutoff_millis: u64) -> Result<u64> {
        self.prepare_proof_owner()?;
        self.with_db(|db| {
            // Find the expired keys under a READ transaction first. The
            // maintenance loop calls this every tick, and on the overwhelmingly
            // common tick nothing has aged out — committing an empty write
            // transaction each time would be a durable write per tick forever
            // for no effect.
            let expired = {
                let read_txn = db.begin_read().map_err(Self::storage_error)?;
                match read_txn.open_table(TRIGGER_AUDIT_STAMPS_TABLE) {
                    Ok(stamps) => {
                        let mut expired = Vec::new();
                        for entry in stamps.iter().map_err(Self::storage_error)? {
                            let (key, stamped_at) = entry.map_err(Self::storage_error)?;
                            if stamped_at.value() < cutoff_millis {
                                expired.push(key.value().to_string());
                            }
                        }
                        expired
                    }
                    Err(redb::TableError::TableDoesNotExist(_)) => Vec::new(),
                    Err(err) => return Err(Self::storage_error(err)),
                }
            };
            if expired.is_empty() {
                return Ok(0);
            }

            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            let mut removed = 0u64;
            let mut identity_mutations = Vec::with_capacity(expired.len().saturating_mul(2));
            {
                let mut stamps = write_txn
                    .open_table(TRIGGER_AUDIT_STAMPS_TABLE)
                    .map_err(Self::storage_error)?;
                let mut audit_table = write_txn
                    .open_table(TRIGGER_AUDIT_TABLE)
                    .map_err(Self::storage_error)?;
                for key in expired {
                    audit_table
                        .remove(key.as_str())
                        .map_err(Self::storage_error)?;
                    stamps.remove(key.as_str()).map_err(Self::storage_error)?;
                    identity_mutations.push(content_identity::remove_status_mutation(
                        b"trigger-audit",
                        key.as_bytes(),
                    ));
                    identity_mutations.push(content_identity::remove_status_mutation(
                        b"trigger-stamp",
                        key.as_bytes(),
                    ));
                    removed = removed.saturating_add(1);
                }
            }
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &identity_mutations,
                &mut self.proof_admission(),
            )?;
            Self::write_writer_recovery_bounds_in_write(&write_txn, None)?;
            {
                #[cfg(any(test, feature = "test-seams"))]
                graph_observation_checkpoint(10)?;
                let graph_commit_result = write_txn.commit();
                #[cfg(any(test, feature = "test-seams"))]
                if graph_commit_result.is_ok() {
                    graph_publication_attempt.committed();
                    graph_observation_checkpoint(11)?;
                }
                graph_commit_result
            }
            .map_err(Self::storage_error)?;
            Ok(removed)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn load_trigger_audit_history(&self) -> Result<Vec<TriggerAuditEntry>> {
        self.load_trigger_audit_history_with_read_memory(None)
            .map(|(entries, _)| entries)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_trigger_audit_history_with_read_memory(
        &self,
        read_memory: Option<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(
        Vec<TriggerAuditEntry>,
        Option<contextdb_core::read_memory::ReadCredit>,
    )> {
        // With a declared reader, that reader funds every decode and keeps
        // every returned entry, once: each entry is decoded under its own row
        // owner of the reader, which pays the decoder's control and scratch
        // before allocating and moves the entry's payload credit into the
        // answer's credit; the answer adds only the entry's slot.
        let decode = match &read_memory {
            Some(read_memory) => Some(crate::read_image_memory::ImageMemory::operation_reader(
                self.require_decode_accountant()?,
                read_memory.clone(),
            )?),
            None => None,
        };
        let _decode_scope = match &decode {
            Some(decode) => decode.enter(),
            None => self.reader_decode_scope()?,
        };
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(TRIGGER_AUDIT_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok((Vec::new(), None)),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut entries = Vec::new();
            let mut credit = read_memory.map(contextdb_core::read_memory::ReadCredit::new);
            for entry in table.iter().map_err(Self::storage_error)? {
                let (_, value) = entry.map_err(Self::storage_error)?;
                let decoded: TriggerAuditEntry = match (&mut credit, &decode) {
                    (Some(credit), Some(decode)) => {
                        credit.try_grow_for(
                            std::mem::size_of::<TriggerAuditEntry>().saturating_add(96),
                            "reader",
                            "trigger_audit_history",
                            "Reduce the selected audit history or raise the declared read memory limit.",
                        )?;
                        let row = decode.row_owner()?;
                        let decoded = row.scope(|| Self::decode(value.value()))?;
                        credit.try_merge_prepaid(
                            row.take_retained_credit(row.retained_payload_bytes())?,
                        )?;
                        decoded
                    }
                    _ => Self::decode(value.value())?,
                };
                entries.push(decoded);
            }
            Ok((entries, credit))
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn load_commit_index(&self) -> Result<BTreeMap<Lsn, TxId>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(COMMIT_INDEX_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(BTreeMap::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut index = BTreeMap::new();
            for entry in table
                .iter()
                .map_err(Self::storage_error)?
                .inspect(|_entry| {
                    #[cfg(any(test, feature = "test-seams"))]
                    if let Ok((key, _)) = _entry {
                        crate::read_work_probe::note_graph(
                            "commit_index",
                            "iteration_step",
                            &key.value().to_be_bytes(),
                            Some(Lsn(key.value())),
                            None,
                        );
                    }
                })
            {
                let (lsn, tx) = entry.map_err(Self::storage_error)?;
                #[cfg(feature = "in-process-test-seams")]
                crate::writer_query_test_seams::note_history(
                    crate::writer_query_test_seams::HistoryFamily::CommitIndex,
                );
                index.insert(Lsn(lsn.value()), TxId(tx.value()));
            }
            Ok(index)
        })
    }

    /// A crash may leave physical commit-index entries past the authenticated
    /// committed frontier. They were never published into content identity,
    /// so a writable open removes only that suffix before hydrating history.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn truncate_uncommitted_commit_index_tail(&self) -> Result<()> {
        self.with_db(|db| {
            let (frontier, stored_last) = {
                let read = db.begin_read().map_err(Self::storage_error)?;
                let table = match read.open_table(COMMIT_INDEX_TABLE) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
                    Err(error) => return Err(Self::storage_error(error)),
                };
                let Some((stored_last, _)) = table.last().map_err(Self::storage_error)? else {
                    return Ok(());
                };
                #[cfg(feature = "in-process-test-seams")]
                crate::writer_query_test_seams::note_history(
                    crate::writer_query_test_seams::HistoryFamily::CommitBound,
                );
                let stored_last = Lsn(stored_last.value());
                let memory = crate::read_image_memory::ImageMemory::funded_for_original(
                    self.require_decode_original()?,
                    "writer_source",
                    "decode_stored_value",
                    "Raise MEMORY_LIMIT to decode this writer operation's stored values.",
                )?;
                let checkpoint = || Ok(());
                let context = contextdb_tx::ReadCommitContext::new(memory.clone(), &checkpoint);
                let frontier =
                    read_authenticated_history_frontiers(&read, &self.path, &memory, &context)
                        .map_err(|error| {
                            crate::funded_error::corruption_display(
                                self.require_decode_original()
                                    .and_then(|original| original.read_budget()),
                                self.path.display(),
                                error,
                            )
                        })?
                        .commit_last
                        .map(|(lsn, _)| lsn);
                (frontier, stored_last)
            };
            if frontier.is_some_and(|committed| stored_last <= committed) {
                return Ok(());
            }
            let write = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
            let mut removed = false;
            {
                let mut table = write
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?;
                loop {
                    let last = table
                        .last()
                        .map_err(Self::storage_error)?
                        .map(|(lsn, _)| Lsn(lsn.value()));
                    if last.is_none_or(|lsn| frontier.is_some_and(|committed| lsn <= committed)) {
                        break;
                    }
                    let _ = table.pop_last().map_err(Self::storage_error)?;
                    removed = true;
                }
            }
            if removed {
                Self::write_writer_recovery_bounds_in_write(&write, None)?;
                {
                    #[cfg(any(test, feature = "test-seams"))]
                    graph_observation_checkpoint(10)?;
                    let graph_commit_result = write.commit();
                    #[cfg(any(test, feature = "test-seams"))]
                    if graph_commit_result.is_ok() {
                        graph_publication_attempt.committed();
                        graph_observation_checkpoint(11)?;
                    }
                    graph_commit_result
                }
                .map_err(Self::storage_error)?;
            }
            Ok(())
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn flush_commit_index_entries(&self, entries: &BTreeMap<Lsn, TxId>) -> Result<()> {
        self.prepare_proof_owner()?;
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            let guarded = Self::final_graph_receipts_nonempty(&write_txn)?;
            {
                let mut table = write_txn
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?;
                for (lsn, tx) in entries {
                    let previous = table
                        .get(lsn.0)
                        .map_err(Self::storage_error)?
                        .map_or(0, |v| v.value());
                    let final_tx = if guarded && lsn.0 != 0 {
                        previous.max(tx.0)
                    } else {
                        tx.0
                    };
                    table.insert(lsn.0, final_tx).map_err(Self::storage_error)?;
                }
            }
            let mutations = entries
                .keys()
                .map(|lsn| {
                    let table = write_txn
                        .open_table(COMMIT_INDEX_TABLE)
                        .map_err(Self::storage_error)?;
                    let tx = table
                        .get(lsn.0)
                        .map_err(Self::storage_error)?
                        .ok_or_else(|| Error::Other("published commit entry missing".into()))?;
                    Ok(content_identity::commit_index_mutation(
                        *lsn,
                        TxId(tx.value()),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &mutations,
                &mut self.proof_admission(),
            )?;
            Self::raise_noncanonical_writer_floors_in_write(
                &write_txn,
                TxId(entries.values().map(|tx| tx.0).max().unwrap_or(0)),
                Lsn(0),
                RowId(0),
            )?;
            Self::write_writer_recovery_bounds_in_write(&write_txn, None)?;
            {
                #[cfg(any(test, feature = "test-seams"))]
                graph_observation_checkpoint(10)?;
                let graph_commit_result = write_txn.commit();
                #[cfg(any(test, feature = "test-seams"))]
                if graph_commit_result.is_ok() {
                    graph_publication_attempt.committed();
                    graph_observation_checkpoint(11)?;
                }
                graph_commit_result
            }
            .map_err(Self::storage_error)?;
            Ok(())
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn rewrite_pruned_state(
        &self,
        table_rows: &HashMap<String, Vec<VersionedRow>>,
        vectors: &[VectorEntry],
        edges: &[AdjEntry],
    ) -> Result<()> {
        self.prepare_proof_owner()?;
        let table_meta = self.load_all_table_meta()?;
        let vector_quantization = Self::vector_quantization_map(&table_meta);
        let source_lsns = self.load_sync_source_lsns()?;
        let previous_vectors = self.load_vectors()?;
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);

            for (name, rows) in table_rows {
                Self::invalidate_retention_clock_in_write(&write_txn, name)?;
                let table_name = Self::rel_table_name(name);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                self.retire_relational_manifests_in_write(&write_txn, name)?;
                content_size::reset_rows(&write_txn, name)?;
                match write_txn.delete_table(table_def) {
                    Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(redb::TableError::TableTypeMismatch { .. }) => {}
                    Err(err) => return Err(Self::storage_error(err)),
                }
                let legacy_table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                match write_txn.delete_table(legacy_table_def) {
                    Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(redb::TableError::TableTypeMismatch { .. }) => {}
                    Err(err) => return Err(Self::storage_error(err)),
                }
                let mut redb_table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                for row in rows {
                    let encoded = Self::encode_versioned_row(row, table_meta.get(name))?;
                    let key = Self::rel_row_key(row);
                    {
                        let predecessor = redb_table
                            .get(key.as_slice())
                            .map_err(Self::storage_error)?;
                        self.replace_relational_manifest_in_write(
                            &write_txn,
                            name,
                            key.as_slice(),
                            predecessor.as_ref().map(|body| body.value()),
                            Some(encoded.as_slice()),
                        )?;
                    }
                    redb_table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
                Self::retain_sync_source_provenance_for_table_rows(&write_txn, name, rows)?;
            }

            content_size::reset_vector_bytes(&write_txn)?;
            match write_txn.delete_table(VECTORS_TABLE) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            {
                let mut table =
                    content_size::open_vectors(&write_txn).map_err(Self::storage_error)?;
                for entry in vectors {
                    let quantization = vector_quantization
                        .get(&entry.index)
                        .copied()
                        .unwrap_or_default();
                    let encoded = Self::encode_vector_entry(entry, quantization)?;
                    let key = Self::vector_key(entry);
                    table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            self.retire_graph_replacement_in_write(&write_txn, edges, None)?;
            match write_txn.delete_table(GRAPH_FWD_TABLE) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            match write_txn.delete_table(GRAPH_REV_TABLE) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            {
                let mut fwd_table = write_txn
                    .open_table(GRAPH_FWD_TABLE)
                    .map_err(Self::storage_error)?;
                let mut rev_table = write_txn
                    .open_table(GRAPH_REV_TABLE)
                    .map_err(Self::storage_error)?;

                for entry in edges {
                    let encoded = Self::encode(entry)?;
                    let fwd_key = Self::graph_fwd_key(entry);
                    let rev_key = Self::graph_rev_key(entry);
                    fwd_table
                        .insert(
                            observed_graph_key!("graph_fwd", "insert", fwd_key.as_slice(), 5),
                            encoded.as_slice(),
                        )
                        .map_err(Self::storage_error)?;
                    self.graph_writer_window(|| {
                        Self::install_graph_creation_in_write(&write_txn, entry, &encoded)
                    })?;
                    rev_table
                        .insert(
                            observed_graph_key!("graph_rev", "insert", rev_key.as_slice(), 5),
                            encoded.as_slice(),
                        )
                        .map_err(Self::storage_error)?;
                }
            }

            Self::reset_content_identity_families_in_write(
                &write_txn,
                &[
                    content_identity::LogicalFamily::Relational,
                    content_identity::LogicalFamily::Graph,
                    content_identity::LogicalFamily::F32Vectors,
                    content_identity::LogicalFamily::Sq8Vectors,
                    content_identity::LogicalFamily::Sq4Vectors,
                ],
                &mut self.proof_admission(),
            )?;
            let mut identity_mutations = previous_vectors
                .iter()
                .map(|entry| {
                    content_identity::remove_vector_version_mutation(
                        &entry.index,
                        entry.row_id,
                        entry.created_tx,
                        entry.lsn,
                        vector_quantization
                            .get(&entry.index)
                            .copied()
                            .unwrap_or_default(),
                    )
                })
                .collect::<Vec<_>>();
            let mut live_rows = HashSet::new();
            for (name, rows) in table_rows {
                let mut rows = rows.iter().collect::<Vec<_>>();
                rows.sort_by_key(|row| (row.lsn, row.created_tx));
                for row in rows {
                    if row.deleted_tx.is_none() {
                        live_rows.insert((name.clone(), row.row_id));
                    }
                    identity_mutations.push(content_identity::row_mutation(
                        name,
                        row,
                        table_meta.get(name),
                    )?);
                }
            }
            let mut ordered_vectors = vectors.iter().collect::<Vec<_>>();
            ordered_vectors.sort_by_key(|entry| (entry.lsn, entry.created_tx));
            for entry in ordered_vectors {
                let quantization = vector_quantization
                    .get(&entry.index)
                    .copied()
                    .unwrap_or_default();
                identity_mutations.push(content_identity::vector_mutation(entry, quantization));
                identity_mutations.push(content_identity::vector_version_mutation(
                    entry,
                    quantization,
                ));
            }
            let mut ordered_edges = edges.iter().collect::<Vec<_>>();
            ordered_edges.sort_by_key(|entry| (entry.lsn, entry.created_tx));
            for entry in ordered_edges {
                identity_mutations.push(content_identity::edge_mutation(entry)?);
            }
            for (table, row_id) in source_lsns.keys() {
                if !live_rows.contains(&(table.clone(), *row_id)) {
                    identity_mutations.push(content_identity::remove_sync_source_mutation(
                        table, *row_id,
                    ));
                }
            }
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &identity_mutations,
                &mut self.proof_admission(),
            )?;

            Self::write_writer_recovery_bounds_in_write(&write_txn, None)?;
            {
                #[cfg(any(test, feature = "test-seams"))]
                graph_observation_checkpoint(10)?;
                let graph_commit_result = write_txn.commit();
                #[cfg(any(test, feature = "test-seams"))]
                if graph_commit_result.is_ok() {
                    graph_publication_attempt.committed();
                    graph_observation_checkpoint(11)?;
                }
                graph_commit_result
            }
            .map_err(Self::storage_error)?;
            Ok(())
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn rewrite_commit_index(&self, entries: &BTreeMap<Lsn, TxId>) -> Result<()> {
        const SUPPORT: TableDefinition<u64, u64> =
            TableDefinition::new("graph_commit_support_scratch");
        self.with_db(|db| {
            let write = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
            let guarded = Self::final_graph_receipts_nonempty(&write)?;
            if write
                .list_tables()
                .map_err(Self::storage_error)?
                .any(|t| t.name() == SUPPORT.name())
            {
                return Err(crate::funded_error::corruption_text(
                    self.require_decode_original()
                        .and_then(|original| original.read_budget()),
                    self.path.display(),
                    "published graph commit-support scratch".into(),
                ));
            }
            {
                let previous = write
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?;
                let mut final_index = write.open_table(SUPPORT).map_err(Self::storage_error)?;
                for item in previous
                    .iter()
                    .map_err(Self::storage_error)?
                    .inspect(|_entry| {
                        #[cfg(any(test, feature = "test-seams"))]
                        if let Ok((key, _)) = _entry {
                            crate::read_work_probe::note_graph(
                                "commit_index",
                                "iteration_step",
                                &key.value().to_be_bytes(),
                                Some(Lsn(key.value())),
                                None,
                            );
                        }
                    })
                {
                    let (lsn, tx) = item.map_err(Self::storage_error)?;
                    if lsn.value() != 0
                        && (guarded || Self::has_graph_creation_at_lsn(&write, Lsn(lsn.value()))?)
                    {
                        final_index
                            .insert(lsn.value(), tx.value())
                            .map_err(Self::storage_error)?;
                    }
                }
                for (lsn, tx) in entries {
                    let old = final_index
                        .get(lsn.0)
                        .map_err(Self::storage_error)?
                        .map_or(0, |v| v.value());
                    final_index
                        .insert(lsn.0, old.max(tx.0))
                        .map_err(Self::storage_error)?;
                }
            }
            let mut after = None::<u64>;
            loop {
                // Copy the addressed scalar before identity publication can
                // finalize pending roots in this same write transaction.
                let next = {
                    let previous = write
                        .open_table(COMMIT_INDEX_TABLE)
                        .map_err(Self::storage_error)?;
                    let mut range = previous
                        .range((
                            after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                            std::ops::Bound::Unbounded,
                        ))
                        .map_err(Self::storage_error)?
                        .inspect(|_entry| {
                            #[cfg(any(test, feature = "test-seams"))]
                            if let Ok((key, _)) = _entry {
                                crate::read_work_probe::note_graph(
                                    "commit_index",
                                    "iteration_step",
                                    &key.value().to_be_bytes(),
                                    Some(Lsn(key.value())),
                                    None,
                                );
                            }
                        });
                    range
                        .next()
                        .transpose()
                        .map_err(Self::storage_error)?
                        .map(|(lsn, _)| lsn.value())
                };
                let Some(lsn) = next else {
                    break;
                };
                after = Some(lsn);
                let absent = {
                    let final_index = write.open_table(SUPPORT).map_err(Self::storage_error)?;
                    final_index.get(lsn).map_err(Self::storage_error)?.is_none()
                };
                if absent {
                    Self::apply_content_identity_mutations_in_write(
                        &write,
                        &[content_identity::remove_commit_index_mutation(Lsn(lsn))],
                        &mut self.proof_admission(),
                    )?;
                }
            }
            write
                .delete_table(COMMIT_INDEX_TABLE)
                .map_err(Self::storage_error)?;
            let mut after = None::<u64>;
            loop {
                let next = {
                    let support = write.open_table(SUPPORT).map_err(Self::storage_error)?;
                    let mut range = support
                        .range((
                            after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                            std::ops::Bound::Unbounded,
                        ))
                        .map_err(Self::storage_error)?;
                    range
                        .next()
                        .transpose()
                        .map_err(Self::storage_error)?
                        .map(|(lsn, tx)| (lsn.value(), tx.value()))
                };
                let Some((lsn, tx)) = next else {
                    break;
                };
                after = Some(lsn);
                write
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?
                    .insert(lsn, tx)
                    .map_err(Self::storage_error)?;
                Self::apply_content_identity_mutations_in_write(
                    &write,
                    &[content_identity::commit_index_mutation(Lsn(lsn), TxId(tx))],
                    &mut self.proof_admission(),
                )?;
            }
            write.delete_table(SUPPORT).map_err(Self::storage_error)?;
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            {
                #[cfg(any(test, feature = "test-seams"))]
                graph_observation_checkpoint(10)?;
                let graph_commit_result = write.commit();
                #[cfg(any(test, feature = "test-seams"))]
                if graph_commit_result.is_ok() {
                    graph_publication_attempt.committed();
                    graph_observation_checkpoint(11)?;
                }
                graph_commit_result
            }
            .map_err(Self::storage_error)?;
            Ok(())
        })
    }

    /// Remove exactly the named relational row VERSIONS, in ONE redb write
    /// transaction, together with the change-log entries that referenced
    /// them and the sync-source-lsn tracking rows that lost their last live
    /// version. Nothing else in the file is read or rewritten: a table's
    /// surviving rows stay exactly where they are, and vectors, edges, and
    /// the commit index are untouched (see [`Self::prune_vectors_and_edges_scoped`]
    /// for those).
    ///
    /// `row_keys` are `(table, row_id, created_tx, lsn)` — the full
    /// relational key, so each is a point `remove` with no scan needed to
    /// find it (`rel_row_key`, computed from a value already in hand — the
    /// `VersionedRow` being pruned).
    ///
    /// `pruned_change_keys` are the `(table, row_id, lsn)` identities of the
    /// same pruned versions, used to find which change-log entries
    /// referenced them. This does NOT recompute each entry's on-disk
    /// `(lsn, index)` coordinate from an in-memory recount: the moment a
    /// scoped pass leaves holes (rather than the old wholesale rewrite's
    /// full re-densify every pass), an in-memory recount goes stale on the
    /// SECOND pass over an lsn group and can silently orphan a change-log
    /// entry (see `changes_since`'s replay, which reads the row version AT
    /// each logged LSN and substitutes the latest on a miss). Instead, for
    /// every LSN any pruned version's change-log entry names, this reads
    /// that LSN group's entries AS THEY ACTUALLY STAND ON DISK right now,
    /// drops the ones naming a pruned version, and rewrites ONLY that LSN
    /// group, densely renumbered from zero — exactly as a fresh sequence of
    /// commits would have written them, and correct regardless of how many
    /// prior scoped passes already touched that group. Cost is proportional
    /// to the number of DISTINCT lsns touched (each an O(entries-in-that-
    /// commit) read+rewrite), never to the whole change log.
    ///
    /// `orphaned_source_lsn_rows` are the `(table, row_id)` pairs that lost
    /// their last LIVE version this pass — looked up by key
    /// (`sync_row_source_lsn_key`) rather than by iterating the table, as
    /// `retain_sync_source_lsns_for_table_rows` does today.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn prune_versions_scoped(
        &self,
        row_keys: &[(String, RowId, TxId, Lsn)],
        pruned_change_keys: &[(String, RowId, Lsn)],
        orphaned_source_lsn_rows: &[(String, RowId)],
    ) -> Result<PruneScopedStats> {
        if row_keys.is_empty()
            && pruned_change_keys.is_empty()
            && orphaned_source_lsn_rows.is_empty()
        {
            return Ok(PruneScopedStats::default());
        }
        let table_meta = self.load_all_table_meta()?;
        self.prune_retention_scoped(
            &table_meta,
            row_keys,
            pruned_change_keys,
            orphaned_source_lsn_rows,
            None,
            &[],
            &[],
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn posting_output_original(
        &self,
    ) -> Result<crate::memory_accounting::OwnedMemoryReservation> {
        let accountant = self
            .proof_accountant()
            .ok_or_else(|| Error::Other("posting outputs have no writer accountant".into()))?;
        self.proof_accountant_handle_for(&accountant)
            .ok_or_else(|| Error::Other("posting outputs have no pinned writer role".into()))?
            .preparation_original()
    }

    /// Reclamation and the identity of reader-deferred adjacency commit with
    /// the row/expiry removal. A restart therefore sees all removals or can
    /// still nominate every remaining copy for a later maintenance pass.
    // These already-admitted removal groups share one durable boundary.
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn prune_retention_scoped(
        &self,
        table_meta: &HashMap<String, TableMeta>,
        row_keys: &[(String, RowId, TxId, Lsn)],
        pruned_change_keys: &[(String, RowId, Lsn)],
        orphaned_source_lsn_rows: &[(String, RowId)],
        auxiliary: Option<(
            &[contextdb_vector::VectorVersionIdentity],
            &[AdjEntry],
            &HashSet<NodeId>,
        )>,
        commit_index_lsns: &[Lsn],
        selection_progress: &[(String, Vec<u8>)],
    ) -> Result<PruneScopedStats> {
        self.prune_retention_with_owned_scoped(
            table_meta,
            row_keys,
            pruned_change_keys,
            orphaned_source_lsn_rows,
            auxiliary.map(|(vectors, edges, deferred)| {
                (
                    vectors,
                    edges,
                    &[] as &[contextdb_tx::OwnedAdjEntry],
                    deferred,
                )
            }),
            commit_index_lsns,
            selection_progress,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Preserve typed inline storage errors."
    )]
    pub(crate) fn prune_retention_with_owned_scoped(
        &self,
        table_meta: &HashMap<String, TableMeta>,
        row_keys: &[(String, RowId, TxId, Lsn)],
        pruned_change_keys: &[(String, RowId, Lsn)],
        orphaned_source_lsn_rows: &[(String, RowId)],
        auxiliary: Option<(
            &[contextdb_vector::VectorVersionIdentity],
            &[AdjEntry],
            &[contextdb_tx::OwnedAdjEntry],
            &HashSet<NodeId>,
        )>,
        commit_index_lsns: &[Lsn],
        selection_progress: &[(String, Vec<u8>)],
    ) -> Result<PruneScopedStats> {
        self.prepare_proof_owner()?;
        if row_keys.is_empty()
            && pruned_change_keys.is_empty()
            && orphaned_source_lsn_rows.is_empty()
            && auxiliary.is_none()
            && commit_index_lsns.is_empty()
            && selection_progress.is_empty()
        {
            return Ok(PruneScopedStats::default());
        }
        let mut rewrite_workspace = self
            .proof_accountant()
            .map(crate::memory_accounting::OwnedMemoryReservation::new);
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            // Declared first, so dropped last: these charges outlive the
            // mutations they pay for.
            let mut commitment_credits = Vec::new();
            let original = self.posting_output_original()?;
            let mut posting_outputs =
                content_identity::OriginalIdentityMutations::new(&original, 0)?;
            let mut identity_mutations = Vec::new();
            let change_log_keys_removed = self.prune_versions_in_write(
                &write_txn,
                row_keys,
                pruned_change_keys,
                orphaned_source_lsn_rows,
                table_meta,
                &mut identity_mutations,
                &mut posting_outputs,
                &mut commitment_credits,
                rewrite_workspace.as_mut(),
            )?;

            #[cfg(any(test, feature = "test-seams"))]
            retention_reclaim_checkpoint_for_test(1);
            if let Some((vectors, edges, owned_edges, deferred_nodes)) = auxiliary {
                self.prune_vector_versions_in_write(
                    &write_txn,
                    vectors,
                    rewrite_workspace.as_mut(),
                )?;
                self.prune_edges_in_write(&write_txn, edges)?;
                Self::retire_owned_edges_in_write(
                    &write_txn,
                    owned_edges,
                    row_keys,
                    &mut identity_mutations,
                )?;
                identity_mutations.extend(vectors.iter().map(|entry| {
                    content_identity::remove_vector_version_mutation(
                        &entry.index,
                        entry.row_id,
                        entry.created_tx,
                        entry.lsn,
                        Self::column_quantization(
                            table_meta.get(&entry.index.table),
                            &entry.index.column,
                        ),
                    )
                }));
                let mut config = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                if deferred_nodes.is_empty() {
                    config
                        .remove(RETENTION_DEFERRED_EDGE_NODES)
                        .map_err(Self::storage_error)?;
                    identity_mutations.extend(content_identity::config_mutations(
                        RETENTION_DEFERRED_EDGE_NODES,
                        None,
                    ));
                } else {
                    let nodes = deferred_nodes.iter().copied().collect::<BTreeSet<_>>();
                    let bytes = Self::encode_config_value(&nodes)?;
                    config
                        .insert(RETENTION_DEFERRED_EDGE_NODES, bytes.as_slice())
                        .map_err(Self::storage_error)?;
                    identity_mutations.extend(content_identity::config_mutations(
                        RETENTION_DEFERRED_EDGE_NODES,
                        Some(bytes.as_slice()),
                    ));
                }
                drop(config);
                for (index, row_id) in vectors
                    .iter()
                    .map(|vector| (vector.index.clone(), vector.row_id))
                    .collect::<HashSet<_>>()
                {
                    let quantization =
                        Self::column_quantization(table_meta.get(&index.table), &index.column);
                    identity_mutations.push(Self::current_vector_identity_mutation_in_write(
                        &write_txn,
                        &index,
                        row_id,
                        quantization,
                    )?);
                }
                for (source, edge_type, target) in edges
                    .iter()
                    .map(|edge| (edge.source, edge.edge_type.clone(), edge.target))
                    .collect::<HashSet<_>>()
                {
                    identity_mutations.push(Self::current_edge_identity_mutation_in_write(
                        &write_txn, source, &edge_type, target,
                    )?);
                }
            }
            self.remove_commit_index_entries_in_write(&write_txn, commit_index_lsns)?;
            identity_mutations.extend(Self::retired_commit_identity_mutations_in_write(
                &write_txn,
                commit_index_lsns,
            )?);
            {
                let mut config = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, bytes) in selection_progress {
                    config
                        .insert(key.as_str(), bytes.as_slice())
                        .map_err(Self::storage_error)?;
                    identity_mutations.extend(content_identity::config_mutations(
                        key,
                        Some(bytes.as_slice()),
                    ));
                }
            }
            #[cfg(any(test, feature = "test-seams"))]
            graph_observation_checkpoint(12)?;
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &posting_outputs.mutations,
                &mut self.proof_admission(),
            )?;
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &identity_mutations,
                &mut self.proof_admission(),
            )?;
            Self::write_writer_recovery_bounds_in_write(&write_txn, None)?;
            {
                #[cfg(any(test, feature = "test-seams"))]
                graph_observation_checkpoint(10)?;
                let graph_commit_result = write_txn.commit();
                #[cfg(any(test, feature = "test-seams"))]
                if graph_commit_result.is_ok() {
                    graph_publication_attempt.committed();
                    graph_observation_checkpoint(11)?;
                }
                graph_commit_result
            }
            .map_err(Self::storage_error)?;
            #[cfg(any(test, feature = "test-seams"))]
            retention_reclaim_checkpoint_for_test(2);
            Ok(PruneScopedStats {
                row_keys_removed: row_keys.len() as u64,
                change_log_keys_removed,
                vector_keys_removed: auxiliary.map_or(0, |(vectors, _, _, _)| vectors.len() as u64),
                edge_keys_removed: auxiliary.map_or(0, |(_, edges, owned, _)| {
                    ((edges.len() + owned.len()) as u64).saturating_mul(2)
                }),
                proof_admission_refused: None,
            })
        })
    }

    /// Apply selected relational and change-log removals inside the caller's
    /// transaction. Auxiliary state must join that transaction before it commits.
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prune_versions_in_write(
        &self,
        write_txn: &redb::WriteTransaction,
        row_keys: &[(String, RowId, TxId, Lsn)],
        pruned_change_keys: &[(String, RowId, Lsn)],
        orphaned_source_lsn_rows: &[(String, RowId)],
        table_meta: &HashMap<String, TableMeta>,
        identity_mutations: &mut Vec<content_identity::ContentIdentityMutation>,
        posting_outputs: &mut content_identity::OriginalIdentityMutations,
        commitment_credits: &mut Vec<historical_index_commitment::CommitmentCredit>,
        mut rewrite_workspace: Option<&mut crate::memory_accounting::OwnedMemoryReservation>,
    ) -> Result<u64> {
        let _decode_scope = self.writer_decode_scope()?;
        {
            let mut config = write_txn
                .open_table(CONFIG_TABLE)
                .map_err(Self::storage_error)?;
            for (table, row, created, _) in row_keys {
                let key = crate::composite_store::retention_expiry_key(table, *row, *created);
                config.remove(key.as_str()).map_err(Self::storage_error)?;
                identity_mutations.extend(content_identity::config_mutations(&key, None));
            }
        }
        for (table, row, created, lsn) in row_keys {
            Self::erase_row_retirements_in_write(
                write_txn,
                table,
                *row,
                *created,
                *lsn,
                identity_mutations,
            )?;
        }
        let mut change_log_keys_removed = 0u64;

        // CHANGE-LOG-FIRST within this one transaction, for the same
        // reason the shipped wholesale path orders it first: an orphaned
        // change-log entry corrupts replay, while an orphaned row
        // version is inert. Merging both into ONE transaction (rather
        // than the two separate transactions the wholesale path used)
        // removes the interleaving window entirely — after a crash the
        // file has either both removals or neither.
        if !pruned_change_keys.is_empty() {
            let mut affected: BTreeMap<Lsn, HashSet<(String, RowId)>> = BTreeMap::new();
            for (table, row_id, lsn) in pruned_change_keys {
                affected
                    .entry(*lsn)
                    .or_default()
                    .insert((table.clone(), *row_id));
            }
            match write_txn.open_table(CHANGE_LOG_TABLE) {
                Ok(mut table) => {
                    let mut key_buf = String::with_capacity(Self::change_log_entry_key_len());
                    for (lsn, pruned_here) in &affected {
                        let prefix = format!("{:020}:", lsn.0);
                        let existing: Vec<(String, Vec<u8>)> = {
                            let mut entries = Vec::new();
                            for entry in table
                                .range(prefix.as_str()..)
                                .map_err(Self::storage_error)?
                                .inspect(|_entry| {
                                    #[cfg(any(test, feature = "test-seams"))]
                                    if let Ok((key, _)) = _entry {
                                        crate::read_work_probe::graph_storage_key(
                                            "history",
                                            "iteration_step",
                                            key.value(),
                                        );
                                    }
                                })
                            {
                                let (key, value) = entry.map_err(Self::storage_error)?;
                                let key = key.value();
                                if !key.starts_with(prefix.as_str()) {
                                    break;
                                }
                                if let Some(workspace) = rewrite_workspace.as_mut() {
                                    workspace.try_grow_for(
                                        1024usize.saturating_add(key.len().saturating_mul(8)).saturating_add(value.value().len().saturating_mul(16)),
                                        "retention", "prepare_prune", "Raise MEMORY_LIMIT to rewrite the affected retention change-log group.",
                                    )?;
                                }
                                entries.push((key.to_string(), value.value().to_vec()));
                            }
                            entries
                        };
                        let mut survivors: Vec<Vec<u8>> = Vec::with_capacity(existing.len());
                        for (key, bytes) in existing {
                            let _row_scope = crate::read_image_memory::active_owner()?
                                .row_owner()?
                                .enter();
                            let decoded = Self::decode_stored_change_log(&bytes)?.entry;
                            let referenced = match &decoded {
                                ChangeLogEntry::RowInsert { table, row_id, .. }
                                | ChangeLogEntry::RowDelete { table, row_id, .. } => {
                                    pruned_here.contains(&(table.clone(), *row_id))
                                }
                                _ => false,
                            };
                            table
                                .remove(observed_graph_key!("history", "remove", key.as_str(), 8))
                                .map_err(Self::storage_error)?;
                            Self::detach_change_log_selectors_in_write(
                                write_txn,
                                key.as_str(),
                                !referenced,
                            )?;
                            identity_mutations.push(
                                content_identity::remove_encoded_history_mutation(&key, &decoded)?,
                            );
                            if referenced {
                                change_log_keys_removed = change_log_keys_removed.saturating_add(1);
                            } else {
                                survivors.push(bytes);
                            }
                        }
                        for (index, bytes) in survivors.into_iter().enumerate() {
                            Self::write_change_log_entry_key(*lsn, index, &mut key_buf);
                            table
                                .insert(
                                    observed_graph_key!("history", "insert", key_buf.as_str(), 8),
                                    bytes.as_slice(),
                                )
                                .map_err(Self::storage_error)?;
                            let _row_scope = crate::read_image_memory::active_owner()?
                                .row_owner()?
                                .enter();
                            self.graph_writer_window(|| {
                                Self::write_carried_change_log_selectors_in_write(
                                    write_txn,
                                    key_buf.as_str(),
                                    bytes.as_slice(),
                                )
                            })?;
                            identity_mutations
                                .push(Self::stored_change_log_mutation(&key_buf, &bytes)?);
                        }
                    }
                }
                Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
        }

        // Carry-only retries have no relational commitment to fold. Keep their
        // headroom for the auxiliary proof publication that actually changes.
        if row_keys.is_empty() && orphaned_source_lsn_rows.is_empty() {
            return Ok(change_log_keys_removed);
        }

        // Postings and the row versions they project are committed to
        // separately and must agree: see `historical_index_commitment`.
        let mut commitments = historical_index_commitment::CommitmentBatch::for_writer()?;
        let mut opened_rel_tables: HashMap<String, content_size::ContentTable<'_>> = HashMap::new();
        for (table, row_id, created_tx, lsn) in row_keys {
            if !opened_rel_tables.contains_key(table) {
                let table_name = Self::rel_table_name(table);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                match content_size::open_rows(write_txn, table_def, table_meta.get(table)) {
                    Ok(redb_table) => {
                        opened_rel_tables.insert(table.clone(), redb_table);
                    }
                    Err(redb::TableError::TableDoesNotExist(_)) => continue,
                    Err(err) => return Err(Self::storage_error(err)),
                }
            }
            if let Some(redb_table) = opened_rel_tables.get_mut(table) {
                let key = Self::rel_row_key_from_parts(*row_id, *created_tx, *lsn);
                {
                    let predecessor = redb_table
                        .get(key.as_slice())
                        .map_err(Self::storage_error)?;
                    if let Some(body) = predecessor.as_ref() {
                        let _row_scope = crate::read_image_memory::active_owner()?
                            .row_owner()?
                            .enter();
                        if self.relational_manifest_has_successor_in_write(
                            write_txn,
                            table,
                            key.as_slice(),
                        )? {
                            self.prune_relational_posting_intervals_in_write(
                                write_txn,
                                table,
                                table_meta.get(table),
                                body.value(),
                                &mut commitments,
                                posting_outputs,
                            )?;
                        } else {
                            self.prune_terminal_relational_posting_in_write(
                                write_txn,
                                table,
                                table_meta.get(table),
                                body.value(),
                                &mut commitments,
                                posting_outputs,
                            )?;
                        }
                    }
                    self.replace_relational_manifest_in_write(
                        write_txn,
                        table,
                        key.as_slice(),
                        predecessor.as_ref().map(|body| body.value()),
                        None,
                    )?;
                }
                redb_table.remove_body(&mut commitments, key.as_slice())?;
                self.erase_authored_row_in_write(write_txn, table, key.as_slice())?;
            }
        }
        drop(opened_rel_tables);
        let (published, credit) = commitments.finish(write_txn)?.into_parts();
        commitment_credits.push(credit);
        identity_mutations.extend(published);
        for (table, row_id) in row_keys
            .iter()
            .map(|(table, row_id, _, _)| (table.as_str(), *row_id))
            .collect::<HashSet<_>>()
        {
            let current_identity = Self::current_row_identity_mutation_in_write(
                write_txn,
                table,
                row_id,
                table_meta.get(table),
                posting_outputs.original(),
            )?;
            posting_outputs.push_original(current_identity)?;
        }

        if !orphaned_source_lsn_rows.is_empty() {
            match (
                write_txn.open_table(SYNC_ROW_SOURCE_LSN_TABLE),
                write_txn.open_table(SYNC_ROW_SOURCE_KIND_TABLE),
            ) {
                (Ok(mut lsn_table), Ok(mut kind_table)) => {
                    for (table_name, row_id) in orphaned_source_lsn_rows {
                        let key = Self::sync_row_source_lsn_key(table_name, *row_id);
                        lsn_table
                            .remove(key.as_slice())
                            .map_err(Self::storage_error)?;
                        kind_table
                            .remove(key.as_slice())
                            .map_err(Self::storage_error)?;
                        identity_mutations.push(content_identity::remove_sync_source_mutation(
                            table_name, *row_id,
                        ));
                    }
                }
                (Err(redb::TableError::TableDoesNotExist(_)), _) => {}
                (_, Err(redb::TableError::TableDoesNotExist(_))) => {}
                (Err(err), _) | (_, Err(err)) => return Err(Self::storage_error(err)),
            }
        }

        Ok(change_log_keys_removed)
    }

    // Read the borrowed collection prefix before allocating a row-source
    // adapter. A carry-only retry therefore admits its full destination at the
    // caller's original headroom. Recheck the same price under the reclamation
    // commit guard before decoding; a concurrent pass may replace the record.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn retention_deferred_edge_workspace_bytes(&self) -> Result<usize> {
        self.with_db(|db| {
            let txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match txn.open_table(CONFIG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let value = table
                .get(RETENTION_DEFERRED_EDGE_NODES)
                .map_err(Self::storage_error)?;
            value.as_ref().map_or(Ok(0), |value| {
                self.retention_deferred_edge_value_bytes(value.value())
            })
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn retention_deferred_edge_value_bytes(&self, value: &[u8]) -> Result<usize> {
        let (count, _) =
            bincode::serde::decode_from_slice::<usize, _>(value, bincode::config::standard())
                .map_err(|error| {
                    crate::funded_error::corruption_display(
                        self.require_decode_original()
                            .and_then(|original| original.read_budget()),
                        self.path.display(),
                        format_args!("bincode retention count decode error: {error}"),
                    )
                })?;
        Ok(count.saturating_mul(256).saturating_add(value.len()))
    }

    /// Read only the carry-over identities. Admit their decoded B-tree and
    /// destination hash buckets before decoding, including carry-only retries.
    #[cfg(test)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(crate) fn load_retention_deferred_edge_nodes_admitted(
        &self,
        workspace: &mut crate::memory_accounting::OwnedMemoryReservation,
    ) -> Result<BTreeSet<NodeId>> {
        self.load_retention_deferred_edge_nodes_prepaid(workspace, 0)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_retention_deferred_edge_nodes_prepaid(
        &self,
        workspace: &mut crate::memory_accounting::OwnedMemoryReservation,
        prepaid_bytes: usize,
    ) -> Result<BTreeSet<NodeId>> {
        let _decode_scope = self.reader_decode_scope()?;
        self.with_db(|db| {
            let txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match txn.open_table(CONFIG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => {
                    workspace.try_shrink(prepaid_bytes)?;
                    return Ok(BTreeSet::new());
                }
                Err(error) => return Err(Self::storage_error(error)),
            };
            let Some(value) = table
                .get(RETENTION_DEFERRED_EDGE_NODES)
                .map_err(Self::storage_error)?
            else {
                workspace.try_shrink(prepaid_bytes)?;
                return Ok(BTreeSet::new());
            };
            let bytes = self.retention_deferred_edge_value_bytes(value.value())?;
            if prepaid_bytes > bytes {
                workspace.try_shrink(prepaid_bytes - bytes)?;
            }
            workspace.try_grow_for(
                bytes.saturating_sub(prepaid_bytes),
                "retention",
                "prepare_prune",
                "Raise MEMORY_LIMIT to load reader-deferred retention identities.",
            )?;
            Self::decode(value.value())
        })
    }

    /// Commit the complete selected currency cleanup in one durable boundary.
    /// Point removal uses the supplied identities, including dormant vector
    /// bodies and commit-index entries; no retry relies on an already removed row.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn prune_currency_scoped(
        &self,
        row_keys: &[(String, RowId, TxId, Lsn)],
        pruned_change_keys: &[(String, RowId, Lsn)],
        vector_versions: &[contextdb_vector::VectorVersionIdentity],
        commit_index_lsns: &[Lsn],
        durable: &std::cell::Cell<bool>,
        cleanup_progress: &[(String, writer_cleanup_progress::CurrencyCleanupProgress)],
        cleanup_rotation: Option<[u8; 32]>,
    ) -> Result<PruneScopedStats> {
        self.prepare_proof_owner()?;
        if row_keys.is_empty()
            && pruned_change_keys.is_empty()
            && vector_versions.is_empty()
            && commit_index_lsns.is_empty()
            && cleanup_progress.is_empty()
            && cleanup_rotation.is_none()
        {
            return Ok(PruneScopedStats::default());
        }
        // Only declarations named by the removal set are needed to encode
        // row/index identities and vector quantization. Do not decode the
        // unrelated catalog for one finite cleanup batch.
        let table_meta = self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let mut selected = HashMap::new();
            for table in row_keys
                .iter()
                .map(|key| key.0.as_str())
                .chain(pruned_change_keys.iter().map(|key| key.0.as_str()))
                .chain(
                    vector_versions
                        .iter()
                        .map(|version| version.index.table.as_str()),
                )
            {
                if !selected.contains_key(table)
                    && let Some(meta) = Self::load_table_meta_in_read_txn(&read, table)?
                {
                    selected.insert(table.to_owned(), meta);
                }
            }
            Ok(selected)
        })?;
        let vector_quantization = Self::vector_quantization_map(&table_meta);
        let run = |admission: &mut content_identity::ProofAdmission| -> Result<PruneScopedStats> {
            self.with_db(|db| {
                let write_txn = Self::begin_history_write(db)?;
                #[cfg(any(test, feature = "test-seams"))]
                let mut graph_publication_attempt =
                    crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
                // Declared first, so dropped last: these charges outlive the
                // mutations they pay for.
                let mut commitment_credits = Vec::new();
                let original = self.posting_output_original()?;
                let mut posting_outputs =
                    content_identity::OriginalIdentityMutations::new(&original, 0)?;
                let mut identity_mutations = Vec::new();
                let change_log_keys_removed = self.prune_versions_in_write(
                    &write_txn,
                    row_keys,
                    pruned_change_keys,
                    &[],
                    &table_meta,
                    &mut identity_mutations,
                    &mut posting_outputs,
                    &mut commitment_credits,
                    None,
                )?;
                #[cfg(test)]
                currency_reclaim_checkpoint_for_test(1)?;
                self.prune_vector_versions_in_write(&write_txn, vector_versions, None)?;
                identity_mutations.extend(vector_versions.iter().map(|version| {
                    content_identity::remove_vector_version_mutation(
                        &version.index,
                        version.row_id,
                        version.created_tx,
                        version.lsn,
                        vector_quantization
                            .get(&version.index)
                            .copied()
                            .unwrap_or_default(),
                    )
                }));
                for (index, row_id) in vector_versions
                    .iter()
                    .map(|vector| (vector.index.clone(), vector.row_id))
                    .collect::<HashSet<_>>()
                {
                    let quantization = vector_quantization.get(&index).copied().unwrap_or_default();
                    identity_mutations.push(Self::current_vector_identity_mutation_in_write(
                        &write_txn,
                        &index,
                        row_id,
                        quantization,
                    )?);
                }
                #[cfg(test)]
                currency_reclaim_checkpoint_for_test(2)?;
                self.remove_commit_index_entries_in_write(&write_txn, commit_index_lsns)?;
                identity_mutations.extend(Self::retired_commit_identity_mutations_in_write(
                    &write_txn,
                    commit_index_lsns,
                )?);
                Self::apply_content_identity_mutations_in_write(
                    &write_txn,
                    &posting_outputs.mutations,
                    admission,
                )?;
                Self::apply_content_identity_mutations_in_write(
                    &write_txn,
                    &identity_mutations,
                    admission,
                )?;
                Self::write_currency_cleanup_progress(
                    &write_txn,
                    cleanup_progress,
                    cleanup_rotation,
                )?;
                Self::write_writer_recovery_bounds_in_write(&write_txn, None)?;
                {
                    #[cfg(any(test, feature = "test-seams"))]
                    graph_observation_checkpoint(10)?;
                    let graph_commit_result = write_txn.commit();
                    #[cfg(any(test, feature = "test-seams"))]
                    if graph_commit_result.is_ok() {
                        graph_publication_attempt.committed();
                        graph_observation_checkpoint(11)?;
                    }
                    graph_commit_result
                }
                .map_err(Self::storage_error)?;
                durable.set(true);
                #[cfg(test)]
                currency_reclaim_checkpoint_for_test(3)?;
                Ok(PruneScopedStats {
                    row_keys_removed: row_keys.len() as u64,
                    change_log_keys_removed,
                    vector_keys_removed: vector_versions.len() as u64,
                    edge_keys_removed: 0,
                    proof_admission_refused: None,
                })
            })
        };
        // A refused proof leaves the transaction uncommitted and the selected
        // identities available for a later accounted maintenance attempt.
        run(&mut self.proof_admission())
    }

    /// Apply the durable half of one authoritative purge in exactly one Redb
    /// write transaction.  This is deliberately not composed from scoped
    /// pruning helpers: the permanent lifecycle record must become durable
    /// atomically with every selected copy's disappearance.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn commit_authoritative_purge(
        &self,
        projection: &AuthoritativePurgePersistenceProjection,
        marker: Option<purge_publication::DurableMarker>,
        frontier: Lsn,
    ) -> Result<()> {
        #[cfg(test)]
        let _purge_enclosing_producer = crate::persistence::content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::site(
            "RedbPersistence::commit_authoritative_purge");
        #[cfg(test)]
        content_identity::proof_allocation_oracle_tests::note_purge_publication_frontier(
            frontier.0,
        );
        #[cfg(test)]
        let _purge_with_db_producer = crate::persistence::content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::site(
            "RedbPersistence::commit_authoritative_purge::with_db");
        let _native_decode = self.writer_decode_scope()?;
        let native_projection = self.require_decode_original()?;
        let native_original = native_projection.preparation_original()?;
        let native_admission =
            native_storage_admission::StorageAdmissionScope::writer_original(&native_original)?;
        let native_basis = if projection.vector_partition_identities.is_empty() {
            None
        } else {
            Some((self.paged_root_capture()?)()?)
        };
        self.with_db(|db| {
            #[cfg(test)]
            let _purge_begin_producer = crate::persistence::content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::site(
                "RedbPersistence::commit_authoritative_purge::begin_history_write");
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(feature="test-seams")]
            let mut native_observation=native_basis.as_ref().map(|_|crate::vector_ivf_observations::NativeIvfOuterWriteObservation::begin());
            let mut native_session=if let Some(basis)=&native_basis {
                let (_,snapshot,source_frontier)=basis.authenticate()?;
                Some(vector_ivf::write::WriteSession::borrow(&write_txn,&native_admission,native_projection.vector_budget()?,snapshot,source_frontier,frontier)?)
            } else {None};
            #[cfg(test)]
            drop(_purge_begin_producer);

            #[cfg(test)]
            let _purge_graph_producer = crate::persistence::content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::site(
                "RedbPersistence::commit_authoritative_purge::begin_graph_publication_attempt");
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            #[cfg(test)]
            drop(_purge_graph_producer);
            #[cfg(test)]
            let _purge_erasure_producer = crate::persistence::content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::site(
                "RedbPersistence::commit_authoritative_purge::apply_local_erasure");
            self.apply_local_erasure(
                &write_txn,
                projection,
                marker,
                None,
            )?;
            if let Some(session)=&mut native_session {session.erase_rows(self,&native_original,&projection.vector_partition_identities)?;session.bind_final()?;}
            drop(native_session);
            #[cfg(test)]
            drop(_purge_erasure_producer);
            #[cfg(test)]
            let _purge_floors_producer = crate::persistence::content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::site(
                "RedbPersistence::commit_authoritative_purge::raise_noncanonical_writer_floors");
            Self::raise_noncanonical_writer_floors_in_write(
                &write_txn,
                TxId(0),
                frontier,
                RowId(0),
            )?;
            #[cfg(test)]
            drop(_purge_floors_producer);
            #[cfg(test)]
            let _purge_bounds_producer = crate::persistence::content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::site(
                "RedbPersistence::commit_authoritative_purge::write_writer_recovery_bounds");
            Self::write_writer_recovery_bounds_in_write(&write_txn, None)?;
            #[cfg(test)]
            drop(_purge_bounds_producer);
            {
                #[cfg(test)]
                let _purge_commit_producer = crate::persistence::content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::site(
                    "RedbPersistence::commit_authoritative_purge::commit");
                #[cfg(any(test, feature = "test-seams"))]
                graph_observation_checkpoint(10)?;
                #[cfg(feature="test-seams")]
                if let Some(observer)=&native_observation {observer.before_commit()?;}
                let graph_commit_result = write_txn.commit();
                #[cfg(any(test, feature = "test-seams"))]
                if graph_commit_result.is_ok() {
                    #[cfg(feature="test-seams")]
                    if let Some(observer)=&mut native_observation {observer.committed(frontier);}
                    graph_publication_attempt.committed();
                    graph_observation_checkpoint(11)?;
                }
                graph_commit_result
            }
            .map_err(Self::storage_error)
        })?;
        #[cfg(test)]
        drop(_purge_with_db_producer);
        for identity in &projection.vector_partition_identities {
            self.native_ivf_cache
                .forget_retained_route(vector_ivf::retained::route_identity(
                    &identity.index,
                    &identity.partition_key,
                ));
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    pub(super) fn checked_reader_audit(
        &self,
        owner: &Arc<LockedWriterFile>,
        operation: &str,
    ) -> writer_file::ReaderAuditOutcome {
        #[cfg(test)]
        let _purge_enclosing_producer = crate::persistence::content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::site(
            "RedbPersistence::checked_reader_audit");
        use writer_file::ReaderAuditOutcome;
        match owner.audit_reader_snapshots_under_hold() {
            ReaderAuditOutcome::Proven(audit) => {
                if audit.unknown {
                    return ReaderAuditOutcome::Refused(
                        self.path.clone(),
                        std::io::Error::other(
                            "a reader holds the database-file presence lock but has no registration in any known runtime root",
                        ),
                    );
                }
                if operation == "authoritative purge"
                    && let ReaderSnapshotRegistry::Complete(snapshots) = &audit.registry
                    && let Some(acquiring) = snapshots
                        .iter()
                        .find(|snapshot| snapshot_is_provisional(&snapshot.storage_snapshot))
                {
                    return ReaderAuditOutcome::Refused(
                        self.path.clone(),
                        std::io::Error::other(format!(
                            "reader {} ({}) is still acquiring its snapshot",
                            acquiring.process.process_id, acquiring.process.process_name
                        )),
                    );
                }
                ReaderAuditOutcome::Proven(audit)
            }
            other => other,
        }
    }

    #[cfg(target_os = "linux")]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reader_audit_error(
        &self,
        outcome: writer_file::ReaderAuditOutcome,
        operation: &str,
    ) -> Result<writer_file::ReaderAudit> {
        match outcome {
            writer_file::ReaderAuditOutcome::Proven(audit) => Ok(audit),
            writer_file::ReaderAuditOutcome::Busy(path) => {
                Err(Error::ReaderCoordinationUnavailable {
                    operation: operation.to_owned(),
                    path: path.display().to_string(),
                    reason: "reader publication or registry inventory is contended".to_owned(),
                })
            }
            writer_file::ReaderAuditOutcome::Refused(path, error) => {
                Err(Error::ReaderCoordinationUnavailable {
                    operation: operation.to_owned(),
                    path: path.display().to_string(),
                    reason: error.to_string(),
                })
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(test)]
    pub(super) fn refuse_unknown_reader_presence(
        &self,
        owner: &Arc<LockedWriterFile>,
        operation: &str,
    ) -> Result<()> {
        self.reader_audit_error(self.checked_reader_audit(owner, operation), operation)
            .map(drop)
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    pub(super) fn refuse_unprovable_reader_inventory(&self, operation: &str) -> Result<()> {
        if default_reader_runtime_directory().is_none() {
            return Err(Error::ReaderCoordinationUnavailable {
                operation: operation.to_owned(),
                path: self.path.display().to_string(),
                reason:
                    "no common platform runtime root or database-file presence signal is available"
                        .to_owned(),
            });
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn acquire_authoritative_purge_publication(
        &self,
    ) -> Result<Option<purge_publication::AcquiredPurgePublication>> {
        #[cfg(test)]
        let _purge_enclosing_producer = crate::persistence::content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::site(
            "RedbPersistence::acquire_authoritative_purge_publication");
        #[cfg(not(unix))]
        {
            let _ = self;
            return Ok(None);
        }
        #[cfg(unix)]
        {
            let owner = self
                .writer_file
                .read()
                .map_err(|_| Error::ReaderCoordinationUnavailable {
                    operation: "authoritative purge".to_owned(),
                    path: self.path.display().to_string(),
                    reason: "writer file state is unavailable".to_owned(),
                })?
                .clone()
                .ok_or_else(|| Error::ReaderCoordinationUnavailable {
                    operation: "authoritative purge".to_owned(),
                    path: self.path.display().to_string(),
                    reason: "writer file ownership is closed".to_owned(),
                })?;
            #[cfg(target_os = "linux")]
            let physical = self.reader_audit_error(
                self.checked_reader_audit(&owner, "authoritative purge"),
                "authoritative purge",
            )?;
            #[cfg(not(target_os = "linux"))]
            self.refuse_unprovable_reader_inventory("authoritative purge")?;
            let runtimes = reader_inventory_directories(self.runtime_dir.as_deref());
            let database_identity = inspect_companion_record(&self.path)
                .map_err(|error| Error::ReaderCoordinationUnavailable {
                    operation: "authoritative purge".to_owned(),
                    path: self.path.display().to_string(),
                    reason: format!("cannot inspect companion publication incarnation: {error}"),
                })?
                .fields
                .database_identity;
            let mut fences = Vec::new();
            for runtime in runtimes {
                // Under the audit's exclusive reader hold, another store's join
                // or leave holding this root is neither waited for nor refused.
                #[cfg(target_os = "linux")]
                let serializer = serialize_runtime_root_for_inventory(
                    runtime.root(),
                    Some(physical.admission_hold()),
                );
                #[cfg(not(target_os = "linux"))]
                let serializer = hold_runtime_root(runtime.root(), true).map(Some);
                let _serializer =
                    serializer.map_err(|error| Error::ReaderCoordinationUnavailable {
                        operation: "authoritative purge".to_owned(),
                        path: runtime.path().display().to_string(),
                        reason: error.to_string(),
                    })?;
                let live_here =
                    match capture_reader_snapshot_registry_one(owner.identity(), &runtime) {
                        ReaderSnapshotRegistry::Complete(snapshots) => !snapshots.is_empty(),
                        _ => {
                            return Err(Error::ReaderCoordinationUnavailable {
                                operation: "authoritative purge".to_owned(),
                                path: runtime.path().display().to_string(),
                                reason: "candidate reader registry cannot be authenticated"
                                    .to_owned(),
                            });
                        }
                    };
                let fence = purge_publication::open_for_writer(
                    runtime.path(),
                    owner.identity(),
                    database_identity,
                )
                .map_err(|error| Error::ReaderCoordinationUnavailable {
                    operation: "authoritative purge".to_owned(),
                    path: runtime.path().display().to_string(),
                    reason: format!("publication fence cannot be opened: {error}"),
                })?;
                match fence {
                    Some(fence) => fences.push(fence.acquire_for_prepare().map_err(|error| {
                        Error::ReaderCoordinationUnavailable {
                            operation: "authoritative purge".to_owned(),
                            path: runtime.path().display().to_string(),
                            reason: error.to_string(),
                        }
                    })?),
                    None if !live_here => {}
                    None => {
                        return Err(Error::ReaderCoordinationUnavailable {
                            operation: "authoritative purge".to_owned(),
                            path: runtime.path().display().to_string(),
                            reason: "a live reader has no publication fence".to_owned(),
                        });
                    }
                }
            }
            Ok(Some(purge_publication::AcquiredPurgePublication {
                fences: (!fences.is_empty()).then_some(purge_publication::FenceSet(fences)),
                #[cfg(target_os = "linux")]
                physical,
            }))
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn prepare_acquired_authoritative_purge_publication(
        &self,
        fence: Option<purge_publication::AcquiredPurgePublication>,
        frontier: Lsn,
        tables: &BTreeSet<String>,
    ) -> Result<Option<purge_publication::PreparedPurgePublication>> {
        #[cfg(test)]
        let _purge_enclosing_producer = crate::persistence::content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::site(
            "RedbPersistence::prepare_acquired_authoritative_purge_publication");
        match fence {
            Some(fence) => fence.prepare(frontier, tables).map(Some),
            None => Ok(None),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn durable_purge_publication_marker(
        &self,
    ) -> Result<Option<purge_publication::DurableMarker>> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let table = match read.open_table(PURGE_PUBLICATION_METADATA_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let Some(bytes) = table
                .get(PURGE_PUBLICATION_MARKER_KEY)
                .map_err(Self::storage_error)?
            else {
                return Ok(None);
            };
            let bytes = bytes.value();
            if bytes.len() != 24 {
                return Err(crate::funded_error::corruption_text(
                    self.require_decode_original()
                        .and_then(|original| original.read_budget()),
                    self.path.display(),
                    "durable purge publication marker has the wrong length".into(),
                ));
            }
            let token = bytes[..16].try_into().map_err(|_| {
                crate::funded_error::corruption_text(
                    self.require_decode_original()
                        .and_then(|original| original.read_budget()),
                    self.path.display(),
                    "durable purge marker token width".into(),
                )
            })?;
            let frontier = Lsn(u64::from_le_bytes(bytes[16..].try_into().map_err(
                |_| {
                    crate::funded_error::corruption_text(
                        self.require_decode_original()
                            .and_then(|original| original.read_budget()),
                        self.path.display(),
                        "durable purge marker frontier width".into(),
                    )
                },
            )?));
            Ok(Some(purge_publication::DurableMarker { token, frontier }))
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn recover_authoritative_purge_publication(&self) -> Result<()> {
        #[cfg(not(unix))]
        return Ok(());
        #[cfg(unix)]
        {
            let owner = self
                .writer_file
                .read()
                .map_err(|error| Error::ReaderCoordinationUnavailable {
                    operation: "authoritative purge recovery".to_owned(),
                    path: self.path.display().to_string(),
                    reason: format!("writer file state is unavailable: {error}"),
                })?
                .clone()
                .ok_or_else(|| Error::ReaderCoordinationUnavailable {
                    operation: "authoritative purge recovery".to_owned(),
                    path: self.path.display().to_string(),
                    reason: "writer file ownership is closed".to_owned(),
                })?;
            let runtimes = reader_inventory_directories(self.runtime_dir.as_deref());
            let database_identity = inspect_companion_record(&self.path)
                .map_err(|error| Error::ReaderCoordinationUnavailable {
                    operation: "authoritative purge recovery".to_owned(),
                    path: self.path.display().to_string(),
                    reason: format!("cannot inspect companion publication incarnation: {error}"),
                })?
                .fields
                .database_identity;
            let marker = self.durable_purge_publication_marker()?;
            for runtime in runtimes {
                let Some(mut fence) = purge_publication::open_for_writer(
                    runtime.path(),
                    owner.identity(),
                    database_identity,
                )
                .map_err(|error| Error::ReaderCoordinationUnavailable {
                    operation: "authoritative purge recovery".to_owned(),
                    path: runtime.path().display().to_string(),
                    reason: format!("publication fence cannot be opened: {error}"),
                })?
                else {
                    continue;
                };
                if fence
                    .is_pending()
                    .map_err(|error| Error::ReaderCoordinationUnavailable {
                        operation: "authoritative purge recovery".to_owned(),
                        path: runtime.path().display().to_string(),
                        reason: format!("publication fence state cannot be inspected: {error}"),
                    })?
                {
                    fence.recover(marker).map_err(|error| {
                        Error::ReaderCoordinationUnavailable {
                            operation: "authoritative purge recovery".to_owned(),
                            path: runtime.path().display().to_string(),
                            reason: format!("publication fence cannot be recovered: {error}"),
                        }
                    })?;
                }
            }
            Ok(())
        }
    }

    // Both erasure verbs share the exact physical writer and fault boundary.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn apply_local_erasure(
        &self,
        write_txn: &redb::WriteTransaction,
        projection: &AuthoritativePurgePersistenceProjection,
        marker: Option<purge_publication::DurableMarker>,
        original_history: Option<&OriginalPersistenceFrame<'_>>,
    ) -> Result<()> {
        #[cfg(test)]
        let _purge_enclosing_producer = crate::persistence::content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::site(
            "RedbPersistence::apply_local_erasure");
        let _decode_scope = self.writer_decode_scope()?;
        self.prepare_proof_owner()?;
        // Declared first, so dropped last: these charges outlive the mutations they pay for.
        let mut identity_mutations_commitment_credits: Vec<
            historical_index_commitment::CommitmentCredit,
        > = Vec::new();
        let mut identity_mutations = Vec::new();
        let mut history_mutations = original_history
            .map(|frame| schema_projection::OriginalMutationLog::new(self, frame, write_txn))
            .transpose()?;
        if let (Some(frame), Some(mutations)) = (original_history, history_mutations.as_mut()) {
            self.retire_discard_change_log(frame, write_txn, mutations)?;
            self.erase_original_discard_rows(frame, write_txn, mutations)?;
        }
        // A selected change-log occurrence is identified by its full
        // value inside its LSN group.  Read and re-densify only touched
        // groups so earlier scoped maintenance cannot leave an orphaned
        // or mis-indexed survivor.
        let mut selected_by_lsn = BTreeMap::<Lsn, Vec<ChangeLogEntry>>::new();
        for entry in &projection.change_log_entries {
            selected_by_lsn
                .entry(entry.lsn())
                .or_default()
                .push(entry.clone());
        }
        if !selected_by_lsn.is_empty() {
            let mut table = write_txn
                .open_table(CHANGE_LOG_TABLE)
                .map_err(Self::storage_error)?;
            let mut key_buf = String::with_capacity(Self::change_log_entry_key_len());
            for (lsn, mut witnesses) in selected_by_lsn {
                let prefix = format!("{:020}:", lsn.0);
                let existing = {
                    let mut entries = Vec::new();
                    for entry in table
                        .range(prefix.as_str()..)
                        .map_err(Self::storage_error)?
                        .inspect(|_entry| {
                            #[cfg(any(test, feature = "test-seams"))]
                            if let Ok((key, _)) = _entry {
                                crate::read_work_probe::graph_storage_key(
                                    "history",
                                    "iteration_step",
                                    key.value(),
                                );
                            }
                        })
                    {
                        let (key, value) = entry.map_err(Self::storage_error)?;
                        if !key.value().starts_with(prefix.as_str()) {
                            break;
                        }
                        entries.push((key.value().to_string(), value.value().to_vec()));
                    }
                    entries
                };
                let mut survivors = Vec::with_capacity(existing.len());
                for (key, bytes) in existing {
                    let _row_scope = crate::read_image_memory::active_owner()?
                        .row_owner()?
                        .enter();
                    let decoded = Self::decode_stored_change_log(&bytes)?.entry;
                    let removed = if let Some(position) =
                        witnesses.iter().position(|wanted| *wanted == decoded)
                    {
                        witnesses.remove(position);
                        true
                    } else {
                        survivors.push(bytes);
                        false
                    };
                    table
                        .remove(observed_graph_key!("history", "remove", key.as_str(), 8))
                        .map_err(Self::storage_error)?;
                    Self::detach_change_log_selectors_in_write(write_txn, key.as_str(), !removed)?;
                    identity_mutations.push(content_identity::remove_encoded_history_mutation(
                        &key, &decoded,
                    )?);
                }
                if !witnesses.is_empty() {
                    return Err(Error::SyncError(
                        ("authoritative purge change-log witness is missing from its LSN group"
                            .to_string())
                        .into(),
                    ));
                }
                for (index, bytes) in survivors.into_iter().enumerate() {
                    Self::write_change_log_entry_key(lsn, index, &mut key_buf);
                    table
                        .insert(
                            observed_graph_key!("history", "insert", key_buf.as_str(), 8),
                            bytes.as_slice(),
                        )
                        .map_err(Self::storage_error)?;
                    let _row_scope = crate::read_image_memory::active_owner()?
                        .row_owner()?
                        .enter();
                    self.graph_writer_window(|| {
                        Self::write_carried_change_log_selectors_in_write(
                            write_txn,
                            key_buf.as_str(),
                            bytes.as_slice(),
                        )
                    })?;
                    identity_mutations.push(Self::stored_change_log_mutation(&key_buf, &bytes)?);
                }
            }
        }

        {
            let mut config = write_txn
                .open_table(CONFIG_TABLE)
                .map_err(Self::storage_error)?;
            for (table, row, created, _) in &projection.row_versions {
                let key = crate::composite_store::retention_expiry_key(table, *row, *created);
                config.remove(key.as_str()).map_err(Self::storage_error)?;
                identity_mutations.extend(content_identity::config_mutations(&key, None));
            }
        }

        let purged_tables = projection
            .row_versions
            .iter()
            .map(|(table, _, _, _)| table.clone())
            .collect::<BTreeSet<_>>();
        let purged_table_meta = {
            let meta_table = write_txn
                .open_table(META_TABLE)
                .map_err(Self::storage_error)?;
            purged_tables
                .iter()
                .map(|table| {
                    let key = Self::meta_key(table);
                    let meta = meta_table.get(key.as_str()).map_err(Self::storage_error)?;
                    meta.map(|meta| {
                        Self::decode_table_meta_versioned(meta.value())
                            .map(|(meta, _)| (table.clone(), meta))
                    })
                    .transpose()
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
        };
        // Postings and the row versions they project are committed to
        // separately and must agree: see `historical_index_commitment`.
        let mut commitments = historical_index_commitment::CommitmentBatch::for_writer()?;
        let mut relational_tables: HashMap<String, content_size::ContentTable<'_>> = HashMap::new();
        #[cfg(any(test, feature = "test-seams"))]
        let first_table_end = projection.row_versions.first().and_then(|first| {
            projection
                .row_versions
                .iter()
                .rposition(|row| row.0 == first.0)
        });
        #[allow(
            clippy::unused_enumerate_index,
            reason = "The index identifies the actual destructive fault boundary with test-seams"
        )]
        for (_row_index, (table, row_id, created_tx, lsn)) in
            projection.row_versions.iter().enumerate()
        {
            let key = Self::rel_row_key_from_parts(*row_id, *created_tx, *lsn);
            let archived_present = self.erase_authored_row_in_write(write_txn, table, &key)?;
            if !purged_table_meta.iter().any(|(name, _)| name == table) {
                if !archived_present {
                    return Err(Error::SyncError(
                        "authoritative purge lost its retired row occurrence".into(),
                    ));
                }
                continue;
            }
            if !relational_tables.contains_key(table) {
                let name = Self::rel_table_name(table);
                let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(name.as_str());
                let meta = purged_table_meta
                    .iter()
                    .find_map(|(name, meta)| (name == table).then_some(meta));
                relational_tables.insert(
                    table.clone(),
                    content_size::open_rows(write_txn, definition, meta)
                        .map_err(Self::storage_error)?,
                );
            }
            {
                let relation = relational_tables
                    .get(table)
                    .expect("opened relational table");
                let predecessor = relation.get(key.as_slice()).map_err(Self::storage_error)?;
                self.replace_relational_manifest_in_write(
                    write_txn,
                    table,
                    key.as_slice(),
                    predecessor.as_ref().map(|body| body.value()),
                    None,
                )?;
            }
            let relation = relational_tables
                .get_mut(table)
                .expect("opened relational table");
            let present = relation
                .get(key.as_slice())
                .map_err(Self::storage_error)?
                .is_some();
            relation.remove_body(&mut commitments, key.as_slice())?;
            if !present && !archived_present {
                return Err(Error::SyncError((
                        "authoritative purge selected relational version disappeared before durable commit"
                            .to_string()).into()));
            }
            #[cfg(any(test, feature = "test-seams"))]
            if Some(_row_index) == first_table_end {
                erasure_first_table_staged_for_test()?;
            }
        }
        drop(relational_tables);
        for (table, meta) in &purged_table_meta {
            Self::rebuild_relational_indexes_in_write(
                write_txn,
                &self.path,
                table,
                meta,
                None,
                &mut self.proof_admission(),
                Some(&mut commitments),
            )?;
        }
        let (published, credit) = commitments.finish(write_txn)?.into_parts();
        identity_mutations_commitment_credits.push(credit);
        identity_mutations.extend(published);
        for (table, row_id) in projection
            .row_versions
            .iter()
            .map(|(table, row_id, _, _)| (table, *row_id))
            .collect::<HashSet<_>>()
        {
            identity_mutations.push(content_identity::remove_row_mutation(table, row_id));
        }

        if !projection.source_provenance.is_empty() {
            let mut lsns = write_txn
                .open_table(SYNC_ROW_SOURCE_LSN_TABLE)
                .map_err(Self::storage_error)?;
            let mut kinds = write_txn
                .open_table(SYNC_ROW_SOURCE_KIND_TABLE)
                .map_err(Self::storage_error)?;
            for (table, row_id, expected_lsn, expected_kind) in &projection.source_provenance {
                let key = Self::sync_row_source_lsn_key(table, *row_id);
                let lsn = lsns
                    .get(key.as_slice())
                    .map_err(Self::storage_error)?
                    .map(|value| value.value());
                let kind = kinds
                    .get(key.as_slice())
                    .map_err(Self::storage_error)?
                    .map(|value| value.value());
                if lsn != Some(expected_lsn.0) || kind != Some(*expected_kind) {
                    return Err(Error::SyncError((
                            "authoritative purge source-provenance witness changed before durable commit"
                                .to_string()).into()));
                }
                lsns.remove(key.as_slice()).map_err(Self::storage_error)?;
                kinds.remove(key.as_slice()).map_err(Self::storage_error)?;
                identity_mutations.push(content_identity::remove_sync_source_mutation(
                    table, *row_id,
                ));
            }
        }

        self.purge_vector_partition_generation_identities_in_write(
            write_txn,
            &projection.vector_partition_identities,
        )?;

        if !projection.vectors.is_empty() {
            let mut vectors = content_size::open_vectors(write_txn).map_err(Self::storage_error)?;
            for entry in &projection.vectors {
                let key = Self::vector_key(entry);
                let removed = vectors
                    .remove(key.as_slice())
                    .map_err(Self::storage_error)?;
                if removed.is_none() {
                    return Err(Error::SyncError((
                        "authoritative purge selected vector occurrence disappeared before durable commit"
                            .to_string()).into()));
                }
                let quantization = Self::vector_quantization_in_write(write_txn, &entry.index)?;
                identity_mutations.push(content_identity::remove_vector_mutation(
                    &entry.index,
                    entry.row_id,
                    quantization,
                ));
                identity_mutations.push(content_identity::remove_vector_version_mutation(
                    &entry.index,
                    entry.row_id,
                    entry.created_tx,
                    entry.lsn,
                    quantization,
                ));
            }
        }

        for (table, row, created, lsn) in &projection.row_versions {
            Self::erase_row_retirements_in_write(
                write_txn,
                table,
                *row,
                *created,
                *lsn,
                &mut identity_mutations,
            )?;
        }
        self.graph_writer_window(|| {
            Self::erase_row_graph_in_write(write_txn, &projection.row_graph_entries)
        })?;
        if let (Some(frame), Some(mutations)) = (original_history, history_mutations.as_mut()) {
            self.erase_original_discard_graph(frame, write_txn, mutations)?;
        }
        if !projection.graph_entries.is_empty() {
            let mut forward = write_txn
                .open_table(GRAPH_FWD_TABLE)
                .map_err(Self::storage_error)?;
            let mut reverse = write_txn
                .open_table(GRAPH_REV_TABLE)
                .map_err(Self::storage_error)?;
            for entry in &projection.graph_entries {
                let forward_key = Self::graph_fwd_key(entry);
                let reverse_key = Self::graph_rev_key(entry);
                {
                    let body = forward
                        .get(observed_graph_key!(
                            "graph_fwd",
                            "get",
                            forward_key.as_slice()
                        ))
                        .map_err(Self::storage_error)?
                        .ok_or_else(|| {
                            crate::funded_error::corruption_text(
                                self.require_decode_original()
                                    .and_then(|original| original.read_budget()),
                                self.path.display(),
                                "purged graph forward projection missing".into(),
                            )
                        })?;
                    let other = reverse
                        .get(observed_graph_key!(
                            "graph_rev",
                            "get",
                            reverse_key.as_slice()
                        ))
                        .map_err(Self::storage_error)?
                        .ok_or_else(|| {
                            crate::funded_error::corruption_text(
                                self.require_decode_original()
                                    .and_then(|original| original.read_budget()),
                                self.path.display(),
                                "purged graph reverse projection missing".into(),
                            )
                        })?;
                    if body.value() != other.value() {
                        return Err(crate::funded_error::corruption_text(
                            self.require_decode_original()
                                .and_then(|original| original.read_budget()),
                            self.path.display(),
                            "purged graph projections disagree".into(),
                        ));
                    }
                    self.graph_writer_window(|| {
                        Self::reclaim_graph_creation_in_write(write_txn, &forward_key, body.value())
                    })?;
                }
                let removed_forward = forward
                    .remove(observed_graph_key!(
                        "graph_fwd",
                        "remove",
                        forward_key.as_slice(),
                        5
                    ))
                    .map_err(Self::storage_error)?;
                let removed_reverse = reverse
                    .remove(observed_graph_key!(
                        "graph_rev",
                        "remove",
                        reverse_key.as_slice(),
                        5
                    ))
                    .map_err(Self::storage_error)?;
                if removed_forward.is_none() || removed_reverse.is_none() {
                    return Err(Error::SyncError((
                            "authoritative purge selected graph occurrence disappeared before durable commit"
                                .to_string()).into()));
                }
                identity_mutations.push(content_identity::remove_edge_mutation(
                    entry.source,
                    &entry.edge_type,
                    entry.target,
                ));
            }
        }

        for (sink, queue_id, expected_digest) in &projection.sink_entries {
            let name = Self::sink_queue_table_name(sink);
            let definition: TableDefinition<u64, &[u8]> = TableDefinition::new(name.as_str());
            let mut queue = write_txn
                .open_table(definition)
                .map_err(Self::storage_error)?;
            let digest = queue
                .get(*queue_id)
                .map_err(Self::storage_error)?
                .map(|value| *blake3::hash(value.value()).as_bytes());
            if digest.as_ref() != Some(expected_digest) {
                return Err(Error::SyncError((
                        "authoritative purge selected sink occurrence disappeared before durable commit"
                            .to_string()).into()));
            }
            queue.remove(*queue_id).map_err(Self::storage_error)?;
            let key = format!("{}:{sink}:{queue_id}", sink.len());
            identity_mutations.push(content_identity::remove_status_mutation(
                b"sink-queue",
                key.as_bytes(),
            ));
        }

        {
            let mut config = write_txn
                .open_table(CONFIG_TABLE)
                .map_err(Self::storage_error)?;
            for key in &projection.config_keys_removed {
                config.remove(key.as_str()).map_err(Self::storage_error)?;
                identity_mutations.extend(content_identity::config_mutations(key, None));
            }
            for (key, bytes) in &projection.lifecycle_records {
                Self::raise_config_identity_floors_in_write(write_txn, key, bytes)?;
                config
                    .insert(key.as_str(), bytes.as_slice())
                    .map_err(Self::storage_error)?;
                identity_mutations.extend(content_identity::config_mutations(
                    key,
                    Some(bytes.as_slice()),
                ));
            }
            for (key, bytes) in &projection.purge_delivery_items {
                let previous = config
                    .insert(key.as_str(), bytes.as_slice())
                    .map_err(Self::storage_error)?;
                if previous.is_some() {
                    return Err(Error::SyncError(
                        (format!("authoritative purge delivery journal collision at {key}")).into(),
                    ));
                }
                identity_mutations.extend(content_identity::config_mutations(
                    key,
                    Some(bytes.as_slice()),
                ));
            }
        }

        if let (Some(frame), Some(mutations)) = (original_history, history_mutations.as_mut()) {
            self.erase_original_discard_configs(frame, write_txn, mutations)?;
        }
        apply_authoritative_purge_in_write(write_txn, &projection.blob_purge, original_history)?;

        if let Some(marker) = marker {
            let mut bytes = Vec::with_capacity(24);
            bytes.extend_from_slice(&marker.token);
            bytes.extend_from_slice(&marker.frontier.0.to_le_bytes());
            write_txn
                .open_table(PURGE_PUBLICATION_METADATA_TABLE)
                .map_err(Self::storage_error)?
                .insert(PURGE_PUBLICATION_MARKER_KEY, bytes.as_slice())
                .map_err(Self::storage_error)?;
        }

        #[cfg(any(test, feature = "test-seams"))]
        if take_authoritative_purge_point_remove_persistence_failure_for_test() {
            return Err(Error::Other(
                ("authoritative purge point-remove persistence failure injected".to_string())
                    .into(),
            ));
        }
        if let Some(mutations) = history_mutations {
            self.finish_original_discard_history(write_txn)?;
            mutations.apply(self, None)?;
        }
        Self::apply_content_identity_mutations_in_write(
            write_txn,
            &identity_mutations,
            &mut self.proof_admission(),
        )?;
        Ok(())
    }

    /// Remove exactly the named vector versions, in ONE redb write
    /// transaction, by identity alone. Currency cleanup joins this same removal
    /// to its row/change/commit-index transaction. Cleanup names the superseded
    /// copies from the partitions' durable directories, so a copy that lives
    /// in a dormant partition is pruned durably without its body ever being
    /// faulted in; a version pruned here is gone on the partition's next load
    /// and after any restart. The key is the same identity key a body-bearing
    /// entry would produce, so the two doors remove the same durable record.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn prune_vector_versions_scoped(
        &self,
        versions: &[contextdb_vector::VectorVersionIdentity],
    ) -> Result<PruneScopedStats> {
        self.prepare_proof_owner()?;
        if versions.is_empty() {
            return Ok(PruneScopedStats::default());
        }
        let quantization = Self::vector_quantization_map(&self.load_all_table_meta()?);
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            self.prune_vector_versions_in_write(&write_txn, versions, None)?;
            let mut mutations = versions
                .iter()
                .map(|version| {
                    content_identity::remove_vector_version_mutation(
                        &version.index,
                        version.row_id,
                        version.created_tx,
                        version.lsn,
                        quantization
                            .get(&version.index)
                            .copied()
                            .unwrap_or_default(),
                    )
                })
                .collect::<Vec<_>>();
            mutations.extend(
                versions
                    .iter()
                    .map(|version| (version.index.clone(), version.row_id))
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .map(|(index, row_id)| {
                        Self::current_vector_identity_mutation_in_write(
                            &write_txn,
                            &index,
                            row_id,
                            quantization.get(&index).copied().unwrap_or_default(),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &mutations,
                &mut self.proof_admission(),
            )?;
            Self::write_writer_recovery_bounds_in_write(&write_txn, None)?;
            {
                #[cfg(any(test, feature = "test-seams"))]
                graph_observation_checkpoint(10)?;
                let graph_commit_result = write_txn.commit();
                #[cfg(any(test, feature = "test-seams"))]
                if graph_commit_result.is_ok() {
                    graph_publication_attempt.committed();
                    graph_observation_checkpoint(11)?;
                }
                graph_commit_result
            }
            .map_err(Self::storage_error)?;
            Ok(PruneScopedStats {
                row_keys_removed: 0,
                change_log_keys_removed: 0,
                vector_keys_removed: versions.len() as u64,
                edge_keys_removed: 0,
                proof_admission_refused: None,
            })
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prune_vector_versions_in_write(
        &self,
        write_txn: &redb::WriteTransaction,
        versions: &[contextdb_vector::VectorVersionIdentity],
        workspace: Option<&mut crate::memory_accounting::OwnedMemoryReservation>,
    ) -> Result<()> {
        if versions.is_empty() {
            return Ok(());
        }
        self.prune_vector_memberships_in_write(
            write_txn,
            versions.iter().map(|version| {
                (
                    &version.index,
                    version.row_id,
                    version.created_tx,
                    version.lsn,
                )
            }),
            workspace,
        )?;
        match content_size::open_vectors(write_txn) {
            Ok(mut table) => {
                for version in versions {
                    let key = Self::vector_identity_key(
                        &version.index,
                        version.row_id,
                        version.created_tx,
                        version.lsn,
                    );
                    table.remove(key.as_slice()).map_err(Self::storage_error)?;
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(err) => return Err(Self::storage_error(err)),
        }
        Ok(())
    }

    /// Remove exactly the named vectors and edges, in ONE redb write
    /// transaction. A received complete image uses this for the vector copies
    /// it superseded; retention uses the shared in-write helper to join these
    /// removals to its row/expiry transaction. Neither touches edges through version
    /// cleanup (edge identity is `(source, target, edge_type)` plus its own
    /// `created_tx`/`lsn`, self-owned and versioned by no relational row).
    /// Currency cleanup uses the same identity removal inside its complete
    /// cleanup transaction, never needing a body. Retention similarly joins
    /// auxiliary removal to its row/expiry transaction.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn prune_vectors_and_edges_scoped(
        &self,
        vectors: &[VectorEntry],
        edges: &[AdjEntry],
    ) -> Result<PruneScopedStats> {
        self.prepare_proof_owner()?;
        if vectors.is_empty() && edges.is_empty() {
            return Ok(PruneScopedStats::default());
        }
        let quantization = Self::vector_quantization_map(&self.load_all_table_meta()?);
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            self.prune_vectors_and_edges_in_write(&write_txn, vectors, edges)?;
            let mut mutations = vectors
                .iter()
                .map(|entry| {
                    content_identity::remove_vector_version_mutation(
                        &entry.index,
                        entry.row_id,
                        entry.created_tx,
                        entry.lsn,
                        quantization.get(&entry.index).copied().unwrap_or_default(),
                    )
                })
                .collect::<Vec<_>>();
            mutations.extend(
                vectors
                    .iter()
                    .map(|entry| (entry.index.clone(), entry.row_id))
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .map(|(index, row_id)| {
                        Self::current_vector_identity_mutation_in_write(
                            &write_txn,
                            &index,
                            row_id,
                            quantization.get(&index).copied().unwrap_or_default(),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
            mutations.extend(
                edges
                    .iter()
                    .map(|entry| (entry.source, entry.edge_type.clone(), entry.target))
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .map(|(source, edge_type, target)| {
                        Self::current_edge_identity_mutation_in_write(
                            &write_txn, source, &edge_type, target,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &mutations,
                &mut self.proof_admission(),
            )?;
            Self::write_writer_recovery_bounds_in_write(&write_txn, None)?;
            {
                #[cfg(any(test, feature = "test-seams"))]
                graph_observation_checkpoint(10)?;
                let graph_commit_result = write_txn.commit();
                #[cfg(any(test, feature = "test-seams"))]
                if graph_commit_result.is_ok() {
                    graph_publication_attempt.committed();
                    graph_observation_checkpoint(11)?;
                }
                graph_commit_result
            }
            .map_err(Self::storage_error)?;
            Ok(PruneScopedStats {
                row_keys_removed: 0,
                change_log_keys_removed: 0,
                vector_keys_removed: vectors.len() as u64,
                edge_keys_removed: (edges.len() as u64).saturating_mul(2),
                proof_admission_refused: None,
            })
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prune_vectors_and_edges_in_write(
        &self,
        write_txn: &redb::WriteTransaction,
        vectors: &[VectorEntry],
        edges: &[AdjEntry],
    ) -> Result<()> {
        self.prune_vector_memberships_in_write(
            write_txn,
            vectors
                .iter()
                .map(|entry| (&entry.index, entry.row_id, entry.created_tx, entry.lsn)),
            None,
        )?;
        if !vectors.is_empty() {
            match content_size::open_vectors(write_txn) {
                Ok(mut table) => {
                    for entry in vectors {
                        let key = Self::vector_key(entry);
                        table.remove(key.as_slice()).map_err(Self::storage_error)?;
                    }
                }
                Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
        }
        self.prune_edges_in_write(write_txn, edges)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prune_edges_in_write(
        &self,
        write_txn: &redb::WriteTransaction,
        edges: &[AdjEntry],
    ) -> Result<()> {
        if !edges.is_empty() {
            let fwd = write_txn.open_table(GRAPH_FWD_TABLE);
            let rev = write_txn.open_table(GRAPH_REV_TABLE);
            match (fwd, rev) {
                (Ok(mut fwd), Ok(mut rev)) => {
                    for entry in edges {
                        let fwd_key = Self::graph_fwd_key(entry);
                        let rev_key = Self::graph_rev_key(entry);
                        {
                            let body = fwd
                                .get(observed_graph_key!("graph_fwd", "get", fwd_key.as_slice()))
                                .map_err(Self::storage_error)?
                                .ok_or_else(|| {
                                    crate::funded_error::corruption_text(
                                        self.require_decode_original()
                                            .and_then(|original| original.read_budget()),
                                        self.path.display(),
                                        "reclaimed graph forward projection missing".into(),
                                    )
                                })?;
                            let other = rev
                                .get(observed_graph_key!("graph_rev", "get", rev_key.as_slice()))
                                .map_err(Self::storage_error)?
                                .ok_or_else(|| {
                                    crate::funded_error::corruption_text(
                                        self.require_decode_original()
                                            .and_then(|original| original.read_budget()),
                                        self.path.display(),
                                        "reclaimed graph reverse projection missing".into(),
                                    )
                                })?;
                            if body.value() != other.value() {
                                return Err(crate::funded_error::corruption_text(
                                    self.require_decode_original()
                                        .and_then(|original| original.read_budget()),
                                    self.path.display(),
                                    "reclaimed graph projections disagree".into(),
                                ));
                            }
                            self.graph_writer_window(|| {
                                Self::reclaim_graph_creation_in_write(
                                    write_txn,
                                    &fwd_key,
                                    body.value(),
                                )
                            })?;
                        }
                        fwd.remove(observed_graph_key!(
                            "graph_fwd",
                            "remove",
                            fwd_key.as_slice(),
                            5
                        ))
                        .map_err(Self::storage_error)?;
                        rev.remove(observed_graph_key!(
                            "graph_rev",
                            "remove",
                            rev_key.as_slice(),
                            5
                        ))
                        .map_err(Self::storage_error)?;
                    }
                }
                (Err(redb::TableError::TableDoesNotExist(_)), _)
                | (_, Err(redb::TableError::TableDoesNotExist(_))) => {}
                (Err(err), _) | (_, Err(err)) => return Err(Self::storage_error(err)),
            }
        }
        Ok(())
    }

    /// Retiring a raw body also retires every directory pairing that names it.
    /// Keep both removals in the caller's transaction so reopen cannot restore
    /// a directory entry whose body was already reclaimed. Visit only the
    /// selected indexes; partition and row identities are checked from their
    /// checksummed records without loading vector bodies.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prune_vector_memberships_in_write<'a>(
        &self,
        write_txn: &redb::WriteTransaction,
        versions: impl Iterator<Item = (&'a VectorIndexRef, RowId, TxId, Lsn)>,
        mut workspace: Option<&mut crate::memory_accounting::OwnedMemoryReservation>,
    ) -> Result<()> {
        let mut selected = HashMap::<&VectorIndexRef, HashSet<(RowId, TxId, Lsn)>>::new();
        for (index, row, tx, lsn) in versions {
            selected.entry(index).or_default().insert((row, tx, lsn));
        }
        if selected.is_empty() {
            return Ok(());
        }
        let mut memberships = write_txn
            .open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE)
            .map_err(Self::storage_error)?;
        let mut tombstones = write_txn
            .open_table(VECTOR_PARTITION_TOMBSTONE_TABLE)
            .map_err(Self::storage_error)?;
        let mut journal = write_txn
            .open_table(VECTOR_PARTITION_JOURNAL_TABLE)
            .map_err(Self::storage_error)?;
        for (index, identities) in selected {
            let mut prefix = Vec::new();
            Self::append_partition_key_component(&mut prefix, index.table.as_bytes());
            Self::append_partition_key_component(&mut prefix, index.column.as_bytes());
            let mut removed = Vec::new();
            for item in memberships
                .range(prefix.as_slice()..)
                .map_err(Self::storage_error)?
            {
                let (key, value) = item.map_err(Self::storage_error)?;
                if !key.value().starts_with(&prefix) {
                    break;
                }
                // Decoded record, validation key/canonical scratch, and grown
                // owned-list capacity. Nonselected scratch is returned before
                // reading the next record; selected copies keep their charge.
                let copy_bytes = 512usize
                    .saturating_add(value.value().len().saturating_mul(2))
                    .saturating_add(key.value().len().saturating_mul(2));
                if let Some(workspace) = workspace.as_mut() {
                    workspace.try_grow_for(
                        copy_bytes,
                        "retention",
                        "prepare_prune",
                        "Raise MEMORY_LIMIT to prepare retention vector membership records.",
                    )?;
                }
                let row_memory = crate::read_image_memory::active_owner()?.row_owner()?;
                let _row_scope = row_memory.enter();
                let record: VectorPartitionMembershipRecord = self.decode_vector_partition_record(
                    VectorPartitionRecordKind::Membership,
                    value.value(),
                )?;
                if record.index != *index
                    || Self::vector_partition_membership_key(&record).as_slice() != key.value()
                {
                    return Err(self.vector_partition_corruption(
                        "pruned vector membership key disagrees with its payload",
                    ));
                }
                if identities.contains(&(
                    record.row_id,
                    record.vector_created_tx,
                    record.vector_lsn,
                )) {
                    // Tuple order drops the payload before its decode credit
                    // on an error while later selected records are retained.
                    removed.push((record, copy_bytes, row_memory));
                } else {
                    drop(record);
                    if let Some(workspace) = workspace.as_mut() {
                        workspace.try_shrink(copy_bytes)?;
                    }
                }
            }
            for selected in removed {
                // Keep the tuple intact across every fallible deletion so
                // error unwinding also drops the record before its owner.
                let (record, copy_bytes, _) = &selected;
                let copy_bytes = *copy_bytes;
                let mut key = Self::vector_partition_membership_key(record);
                memberships
                    .remove(key.as_slice())
                    .map_err(Self::storage_error)?;
                self.write_open_directory_entry(write_txn, &key, None)?;
                if let Some(deleted_tx) = record.deleted_tx {
                    // The tombstone key extends this exact membership key.
                    key.extend_from_slice(&deleted_tx.0.to_be_bytes());
                    tombstones
                        .remove(key.as_slice())
                        .map_err(Self::storage_error)?;
                }
                drop(key);
                drop(selected);
                if let Some(workspace) = workspace.as_mut() {
                    workspace.try_shrink(copy_bytes)?;
                }
            }
            // A reclaimed immutable vector cannot remain a replay dependency.
            // Remove its exact journal references in this same transaction;
            // other row versions and their ordered changes remain untouched.
            let mut journal_keys = Vec::new();
            for item in journal
                .range(prefix.as_slice()..)
                .map_err(Self::storage_error)?
            {
                let (key, value) = item.map_err(Self::storage_error)?;
                if !key.value().starts_with(&prefix) {
                    break;
                }
                let copy_bytes = 512usize
                    .saturating_add(value.value().len().saturating_mul(2))
                    .saturating_add(key.value().len().saturating_mul(2));
                if let Some(workspace) = workspace.as_mut() {
                    workspace.try_grow_for(
                        copy_bytes,
                        "retention",
                        "prepare_prune",
                        "Raise MEMORY_LIMIT to prepare retention vector journal keys.",
                    )?;
                }
                let _row_scope = crate::read_image_memory::active_owner()?
                    .row_owner()?
                    .enter();
                let record: VectorPartitionJournalRecord = self.decode_vector_partition_record(
                    VectorPartitionRecordKind::Journal,
                    value.value(),
                )?;
                if record.index != *index
                    || Self::vector_partition_journal_key(&record).as_slice() != key.value()
                {
                    return Err(self.vector_partition_corruption(
                        "pruned vector journal key disagrees with its payload",
                    ));
                }
                let membership = match &record.change {
                    VectorPartitionJournalChange::Upsert(membership) => membership,
                    VectorPartitionJournalChange::Tombstone(tombstone) => &tombstone.membership,
                };
                if identities.contains(&(
                    membership.row_id,
                    membership.vector_created_tx,
                    membership.vector_lsn,
                )) {
                    journal_keys.push((key.value().to_vec(), copy_bytes));
                } else {
                    drop(record);
                    if let Some(workspace) = workspace.as_mut() {
                        workspace.try_shrink(copy_bytes)?;
                    }
                }
            }
            for (key, copy_bytes) in journal_keys {
                journal
                    .remove(key.as_slice())
                    .map_err(Self::storage_error)?;
                drop(key);
                if let Some(workspace) = workspace.as_mut() {
                    workspace.try_shrink(copy_bytes)?;
                }
            }
        }
        vector_membership_pages::settle_dirty_occupancy(write_txn, self.proof_accountant())?;
        Ok(())
    }

    /// Remove exactly the named commit-index entries — the scoped
    /// counterpart of `rewrite_commit_index`, used when a version-cleanup
    /// pass only made a handful of LSNs' commit-index entries redundant
    /// (their change-log entries are all gone and nothing else names them).
    /// Point removes; nothing else in the index is read or rewritten.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn remove_commit_index_entries_scoped(&self, lsns: &[Lsn]) -> Result<u64> {
        self.prepare_proof_owner()?;
        if lsns.is_empty() {
            return Ok(0);
        }
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            let removed = self.remove_commit_index_entries_in_write(&write_txn, lsns)?;
            let mutations = Self::retired_commit_identity_mutations_in_write(&write_txn, lsns)?;
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &mutations,
                &mut self.proof_admission(),
            )?;
            Self::write_writer_recovery_bounds_in_write(&write_txn, None)?;
            {
                #[cfg(any(test, feature = "test-seams"))]
                graph_observation_checkpoint(10)?;
                let graph_commit_result = write_txn.commit();
                #[cfg(any(test, feature = "test-seams"))]
                if graph_commit_result.is_ok() {
                    graph_publication_attempt.committed();
                    graph_observation_checkpoint(11)?;
                }
                graph_commit_result
            }
            .map_err(Self::storage_error)?;
            Ok(removed)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn remove_commit_index_entries_in_write(
        &self,
        write_txn: &redb::WriteTransaction,
        lsns: &[Lsn],
    ) -> Result<u64> {
        if lsns.is_empty() {
            return Ok(0);
        }
        let mut removed = 0u64;
        if Self::final_graph_receipts_nonempty(write_txn)? {
            return Ok(0);
        }
        match write_txn.open_table(COMMIT_INDEX_TABLE) {
            Ok(mut table) => {
                for lsn in lsns {
                    if Self::has_graph_creation_at_lsn(write_txn, *lsn)? {
                        continue;
                    }
                    if table.remove(lsn.0).map_err(Self::storage_error)?.is_some() {
                        removed = removed.saturating_add(1);
                    }
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(err) => return Err(Self::storage_error(err)),
        }
        Ok(removed)
    }

    /// Compacts the store to its minimal on-disk size, THEN recycles the
    /// redb handle (closes it and reopens the same, now-minimal, file) —
    /// checkpoint export runs this on the finished artifact so
    /// `bytes_written` reflects content, not allocator slack from the
    /// batched copy.
    ///
    /// The recycle exists because redb retains in-process allocator/region
    /// bookkeeping sized to a database's historical peak allocation, and a
    /// file-level `compact()` does not reset it even though it fully
    /// normalizes the on-disk btree and file size — confirmed by direct
    /// measurement: two databases compacted down to a byte-identical
    /// on-disk btree, one carrying a much larger prior-churn history, still
    /// showed a large gap in per-transaction commit cost after compaction
    /// alone; closing and reopening the handle closed that gap. A
    /// long-running embedded consumer has no other opportunity to do this
    /// (it cannot process-restart), so the recycle happens HERE,
    /// transparently, under the SAME locks compaction itself already
    /// holds: no other caller can observe (or race) an intermediate closed
    /// state, because every access to the underlying handle — this
    /// function included — goes through `self.db`'s mutex.
    ///
    /// A deliberate working-headroom margin between the shrink and the
    /// recycle was tried and measured NOT to help: redb's own close-time
    /// bookkeeping (`ensure_allocator_state_table_and_trim` in `Drop for
    /// redb::Database`) regrows a maximally-shrunk file to roughly the
    /// same final size whether or not a margin was left beforehand, so the
    /// margin only added the cost of two extra write transactions for no
    /// measured benefit — removed. The steady-state cost story for a file
    /// carrying a large amount of PRE-cleanup history (never yet compacted
    /// before) is the still-open retrofit-scenario question the version-
    /// cleanup-scaling bench's A2 arm measures directly.
    ///
    /// Returns the recycle's own duration in microseconds, separate from
    /// compaction's own timing (which the caller already measures around
    /// the whole call), matching the existing convention of reporting every
    /// compaction-adjacent cost as its own explicit number.
    ///
    /// On a reopen failure, the file itself is untouched (compaction had
    /// already finished; nothing more is written to it here) and this
    /// `RedbPersistence` is left closed — [`Error::StoreHandleRecycleFailed`]
    /// names the failure; a fresh `RedbPersistence::open`/`Database::open`
    /// on the same path recovers all data once this instance's lock is
    /// released via `close()`.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn compact(
        &self,
        accountant: crate::memory_accounting::AccountantHandle,
    ) -> Result<Option<u64>> {
        let fragmentation = Some(self.fragmentation_ratio()?);
        loop {
            let (progress, recycled) =
                self.compact_incremental(accountant.clone(), fragmentation)?;
            if progress.complete {
                return Ok(recycled);
            }
            // Each transaction has committed and released its write slot.
            std::thread::yield_now();
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn recycle_database_handle(
        &self,
        db_guard: &mut Option<redb::Database>,
    ) -> Result<Option<u64>> {
        // Source operations clone a view under this same lock. Never strand
        // an active operation on an old PageManager or create a second pool.
        let roots = self.live_writer_roots();
        let mut leases = Vec::with_capacity(roots.len());
        for root in &roots {
            let Some(lease) = root.read.try_write() else {
                return Ok(None);
            };
            if lease
                .as_ref()
                .is_some_and(|read| Arc::strong_count(read) != 1)
            {
                return Ok(None);
            }
            leases.push(lease);
        }
        // A prepared native root may be newer than the live header after a
        // postflush fault. Authenticate its exact lower pin before releasing
        // ANY native view; a refusal leaves all existing readers intact.
        if let Some(db) = db_guard.as_ref() {
            for root in &roots {
                drop(
                    db.begin_external_read(root.storage_identity())
                        .map_err(Self::storage_error)?,
                );
            }
        }
        let preparation_accountant = self.require_decode_accountant()?;
        let mut recycle = || {
            for lease in &mut leases {
                **lease = None;
            }
            if let Some(db) = db_guard.as_ref()
                && db.has_live_read_transactions()
            {
                for (root, lease) in roots.iter().zip(leases.iter_mut()) {
                    **lease = Some(Arc::new(
                        db.begin_external_read(root.storage_identity())
                            .map_err(Self::storage_error)?,
                    ));
                }
                return Ok(None);
            }
            let recycle_started = std::time::Instant::now();
            let owner = self.writer_file.read().map_err(Self::storage_error)?;
            let owner = owner
                .as_ref()
                .ok_or_else(|| Error::StoreHandleRecycleFailed {
                    path: self.path.display().to_string(),
                    reason: "database physical ownership is closed".to_owned(),
                })?;
            #[cfg(any(test, feature = "test-seams"))]
            owner.observe(
                read_persistence_test_scaffold::WriterFileRecycleBoundary::BeforeViewClose,
            );
            // The reopened handle keeps the growth step the store declared.
            let growth_step_ratio = db_guard
                .as_ref()
                .and_then(redb::Database::growth_step_ratio);
            let old = db_guard.take();
            drop(old);
            #[cfg(any(test, feature = "test-seams"))]
            owner
                .observe(read_persistence_test_scaffold::WriterFileRecycleBoundary::AfterViewClose);
            if take_handle_recycle_reopen_fault_for_test() {
                return Err(Error::StoreHandleRecycleFailed {
                    path: self.path.display().to_string(),
                    reason: "injected reopen failure (test seam)".to_string(),
                });
            }
            let external = owner.external_snapshot_provider();
            let local = self.writer_root_provider();
            let installed = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let provider_installed = installed.clone();
            // The fresh allocator must see old roots before its first housekeeping.
            let provider: redb::ExternalSnapshotProvider = Arc::new(move || {
                let mut roots = external()?;
                if !provider_installed.load(std::sync::atomic::Ordering::Acquire) {
                    roots.extend(local()?);
                }
                Ok(roots)
            });
            let capacity = self.recycle_cache_capacity();
            // Reuse the same actual occupancy controls: retired physical page
            // aliases consume room in the new generation too. No second fixed
            // reserve is invoiced, and constructor failure/unwind restores TLS.
            let cache_owner = self.cache_installation.owner.lock().clone();
            let reopen = || {
                Self::open_hook_suppressed(|| {
                    redb_builder_with_cache(capacity)
                        .open_with_native_backend_and_external_snapshots(owner.view(), provider)
                })
            };
            let reopened = match cache_owner.as_ref() {
                Some(cache_owner) => cache_owner.with_recycled_cache(reopen),
                None => reopen(), // genuinely unowned standalone persistence
            };
            match reopened {
                Ok(Ok(fresh)) => {
                    // Settle the fresh cache at the installed capacity before any
                    // root is published on it, and refuse a reserve its funding
                    // does not cover: the reopened cache never holds memory no
                    // owner was charged for.
                    if self.cache_installation.capacity().is_some() {
                        let reserve = fresh
                            .settle_startup_cache_budget(capacity)
                            .map_err(|error| self.recycle_storage_error(error))?;
                        if let Some(funded) = self.cache_funded_bytes()
                            && reserve > funded
                        {
                            return Err(Error::StoreHandleRecycleFailed {
                                path: self.path.display().to_string(),
                                reason: format!(
                                    "the reopened page cache needs {reserve} bytes; {funded} are funded"
                                ),
                            });
                        }
                        self.cache_installation.record(capacity, reserve);
                    }
                    fresh.install_local_snapshot_provider(self.writer_root_provider());
                    installed.store(true, std::sync::atomic::Ordering::Release);
                    for (root, lease) in roots.iter().zip(leases.iter_mut()) {
                        **lease = Some(Arc::new(
                            fresh.begin_external_read(root.storage_identity()).map_err(
                                |error| Error::StoreHandleRecycleFailed {
                                    path: self.path.display().to_string(),
                                    reason: error.to_string(),
                                },
                            )?,
                        ));
                    }
                    if let Some(ratio) = growth_step_ratio {
                        fresh.set_growth_step_ratio(ratio);
                    }
                    *db_guard = Some(fresh);
                    #[cfg(any(test, feature = "test-seams"))]
                    owner.observe(
                        read_persistence_test_scaffold::WriterFileRecycleBoundary::AfterViewOpen,
                    );
                    Ok(Some(recycle_started.elapsed().as_micros() as u64))
                }
                Ok(Err(err)) => Err(self.recycle_storage_error(err)),
                Err(_) => Err(Error::StoreHandleRecycleFailed {
                    path: self.path.display().to_string(),
                    reason: "reopen panicked; store may be truncated or corrupt".to_string(),
                }),
            }
        };
        if self.cache_installation.owner.lock().is_some() {
            cache_reserve::with_preparation(&preparation_accountant, false, recycle)
        } else {
            recycle()
        }
    }

    /// `Some` while a storage compaction sweep is active, holding the
    /// measurement that sweep started on -- itself `None` when that
    /// measurement was unknown. Whether a sweep is active never depends on
    /// whether it was measured.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn active_storage_compaction(&self) -> Result<Option<Option<f64>>> {
        let state = self
            .storage_compaction
            .lock()
            .map_err(Self::storage_error)?;
        Ok(state.0.is_active().then_some(state.2))
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn storage_compaction_progress_for_test(&self) -> (bool, usize, usize) {
        let state = self.storage_compaction.lock().unwrap();
        (
            state.0.is_active(),
            state.1.pages_examined,
            state.1.pages_relocated,
        )
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(crate) fn external_snapshot_retention_for_test(
        &self,
    ) -> Result<read_persistence_test_scaffold::ExternalSnapshotRetentionObservation> {
        self.with_db(|db| {
            let stats = db
                .external_snapshot_retention_stats()
                .map_err(Self::storage_error)?;
            let retained_page_allocations = db
                .external_snapshot_retained_pages_for_test()
                .map_err(Self::storage_error)?;
            Ok(
                read_persistence_test_scaffold::ExternalSnapshotRetentionObservation {
                    snapshots: stats.snapshots,
                    reachable_pages: stats.reachable_pages,
                    referenced_pages: stats.referenced_pages,
                    retained_pages: stats.retained_pages,
                    reachable_bytes: stats.reachable_bytes,
                    referenced_bytes: stats.referenced_bytes,
                    retained_bytes: stats.retained_bytes,
                    retained_page_allocations,
                },
            )
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(crate) fn external_snapshot_page_allocated_for_test(&self, page: [u8; 8]) -> Result<bool> {
        self.with_db(|db| Ok(db.external_snapshot_page_allocated_for_test(page)))
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(crate) fn deferred_free_backlog_for_test(&self) -> Result<[(usize, usize); 2]> {
        self.with_db(|db| {
            db.deferred_free_backlog_for_test()
                .map_err(Self::storage_error)
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(crate) fn commit_storage_transaction_for_test(&self) -> Result<()> {
        self.with_db(|db| {
            db.begin_write()
                .map_err(Self::storage_error)?
                .commit()
                .map_err(Self::storage_error)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn restore_storage_compaction(&self) -> Result<()> {
        let _decode_scope = self.writer_decode_scope()?;
        let checkpoint = self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let table = match read.open_table(TableDefinition::<&str, &[u8]>::new(
                "writer_storage_compaction",
            )) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let Some(value) = table.get("cursor").map_err(Self::storage_error)? else {
                return Ok(None);
            };
            let memory = self
                .proof_accountant()
                .map(|accountant| {
                    PersistenceBytes::try_new_for(
                        accountant.clone(),
                        512usize.saturating_add(value.value().len().saturating_mul(3)),
                        "storage_compaction",
                        "restore_checkpoint",
                        "Raise MEMORY_LIMIT to restore storage maintenance controls.",
                    )
                })
                .transpose()?;
            let checkpoint: (u8, Option<String>, bool, Vec<usize>, bool, Option<f64>) =
                Self::decode_exact(value.value())?;
            Ok(Some((checkpoint, memory)))
        })?;
        if let Some(((phase, table, advance, position, active, ratio), memory)) = checkpoint {
            let cursor =
                redb::CompactionCursor::restore_checkpoint(phase, table, advance, position, active)
                    .map_err(Self::storage_error)?;
            let mut state = self
                .storage_compaction
                .lock()
                .map_err(Self::storage_error)?;
            state.0 = cursor;
            state.2 = ratio;
            state.3 = if active { memory } else { None };
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn storage_compaction_control_charge(&self) -> usize {
        self.storage_compaction
            .lock()
            .unwrap()
            .3
            .as_ref()
            .map_or(0, |memory| memory.bytes())
    }

    /// One bounded durable relocation batch. Only a position survives the
    /// call: no read snapshot, transaction, companion lock, or page buffer
    /// remains pinned while the maintenance worker yields to foreground work.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn compact_incremental(
        &self,
        accountant: crate::memory_accounting::AccountantHandle,
        fragmentation_ratio: Option<f64>,
    ) -> Result<(redb::CompactionProgress, Option<u64>)> {
        self.compact_incremental_with_deferral(accountant, fragmentation_ratio, false)?
            .ok_or_else(|| Error::ReaderCoordinationUnavailable {
                operation: "storage reclamation".to_owned(),
                path: self.path.display().to_string(),
                reason: "reader publication is contended".to_owned(),
            })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn compact_incremental_with_deferral(
        &self,
        accountant: crate::memory_accounting::AccountantHandle,
        fragmentation_ratio: Option<f64>,
        defer_busy: bool,
    ) -> Result<Option<(redb::CompactionProgress, Option<u64>)>> {
        #[cfg(target_os = "linux")]
        {
            let owner =
                self.writer_file
                    .read()
                    .map_err(|error| Error::ReaderCoordinationUnavailable {
                        operation: "storage reclamation".to_owned(),
                        path: self.path.display().to_string(),
                        reason: format!("writer file state is unavailable: {error}"),
                    })?;
            let owner = owner
                .as_ref()
                .ok_or_else(|| Error::ReaderCoordinationUnavailable {
                    operation: "storage reclamation".to_owned(),
                    path: self.path.display().to_string(),
                    reason: "writer file ownership is closed".to_owned(),
                })?;
            let outcome = self.checked_reader_audit(owner, "storage reclamation");
            if defer_busy && matches!(outcome, writer_file::ReaderAuditOutcome::Busy(_)) {
                return Ok(None);
            }
            drop(self.reader_audit_error(outcome, "storage reclamation")?);
        }
        #[cfg(all(unix, not(target_os = "linux")))]
        self.refuse_unprovable_reader_inventory("storage reclamation")?;
        let mut state = self
            .storage_compaction
            .lock()
            .map_err(Self::storage_error)?;
        if !state.0.is_active() {
            state.2 = fragmentation_ratio;
        }
        let checkpoint_accountant = accountant.clone();
        // Fund only the paths this batch can retain. Leave at least half the
        // currently available budget for actual page buffers and checkpoint
        // controls; a small declaration advances through fewer pages instead
        // of paying the maximum batch's path reservation up front.
        const PATH_BYTES_PER_PAGE: usize = 1024;
        let maximum =
            accountant
                .usage()
                .available
                .map_or(STORAGE_MAINTENANCE_PAGE_BATCH, |available| {
                    (available / 2 / PATH_BYTES_PER_PAGE).clamp(1, STORAGE_MAINTENANCE_PAGE_BATCH)
                });
        let workspace = crate::memory_accounting::OwnedMemoryReservation::new(accountant)
            .into_admitted_mutex_arc()?;
        // Position paths and the relocation map are bounded independently of
        // file size. Source and destination page buffers are admitted below
        // using their actual allocation sizes, including oversized values.
        workspace.lock().try_grow_for(
            maximum * PATH_BYTES_PER_PAGE,
            "storage_compaction",
            "page_paths",
            "Raise MEMORY_LIMIT to admit a storage maintenance batch.",
        )?;
        let admission = Arc::new(move |bytes| {
            workspace
                .lock()
                .try_grow_for(
                    bytes,
                    "storage_compaction",
                    "relocate_pages",
                    "Raise MEMORY_LIMIT to admit a storage maintenance batch.",
                )
                .map_err(|error| redb::StorageError::AdmissionRefused(Box::new(error)))
        });
        // A Redb transaction owns its storage references. Keep only that
        // bounded writer transaction during relocation, not the handle or
        // companion mutexes needed by foreground reads and writes.
        let txn = self.with_db(Self::begin_history_write)?;
        #[cfg(any(test, feature = "test-seams"))]
        let mut graph_compaction_attempt =
            crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&txn);
        let fingerprint_failure = Arc::new(parking_lot::Mutex::new(None));
        let on_fingerprint_failure = Arc::clone(&fingerprint_failure);
        let checkpoint_ratio = state.2;
        let mut next_controls = None;
        let result = redb::with_read_memory_admission_storage(admission.clone(), || {
            content_identity::payloads::reclaim(&txn, maximum)?;
            txn.compact_step_with_cursor_hook_storage(
                &mut state.0,
                maximum,
                |bytes| {
                    #[cfg(test)]
                    COMPACTION_BATCH_OBSERVER.with(|slot| {
                        if let Some(observe) = slot.borrow_mut().take() {
                            observe();
                        }
                    });
                    admission(bytes)
                },
                |txn, successor| {
                    #[cfg(any(test, feature = "test-seams"))]
                    crate::read_work_probe::note_graph(
                        "publication",
                        "compaction_hook",
                        &[],
                        None,
                        None,
                    );
                    Self::rebind_ddl_selector_roots_in_write(txn)
                        .and_then(|()| {
                            let (phase, table, advance, position, active) = successor.checkpoint();
                            if active {
                                // The cursor's controls are charged in whole
                                // KiB classes: a cursor advancing into a named
                                // table or a deeper page path keeps the same
                                // retained owner instead of re-pricing it on
                                // every batch.
                                next_controls = Some(Self::reserve_compaction_checkpoint_controls(
                                    checkpoint_accountant.clone(),
                                    table,
                                    position,
                                )?);
                            }
                            let bytes = Self::encode(&(
                                phase,
                                table,
                                advance,
                                position,
                                active,
                                checkpoint_ratio,
                            ))?;
                            let mut checkpoints = txn
                                .open_table(TableDefinition::<&str, &[u8]>::new(
                                    "writer_storage_compaction",
                                ))
                                .map_err(Self::storage_error)?;
                            checkpoints
                                .insert("cursor", bytes.as_slice())
                                .map_err(Self::storage_error)?;
                            Ok(())
                        })
                        // Publish fingerprints only after selectors and the
                        // successor checkpoint have reached their final state.
                        .and_then(|()| {
                            #[cfg(any(test, feature = "test-seams"))]
                            graph_observation_checkpoint(27)?;
                            Self::write_vector_body_root_fingerprint(txn)?;
                            // Relocation rewrites membership and open-directory
                            // pages too; their root fingerprints follow.
                            vector_membership_pages::write_fingerprints(txn)?;
                            #[cfg(any(test, feature = "test-seams"))]
                            graph_observation_checkpoint(28)?;
                            Self::write_commit_directory_root_in_write(txn)?;
                            #[cfg(any(test, feature = "test-seams"))]
                            graph_observation_checkpoint(29)?;
                            Self::write_row_heads_root(txn)?;
                            #[cfg(any(test, feature = "test-seams"))]
                            graph_observation_checkpoint(30)?;
                            Self::write_read_bounds_roots_in_write(txn)?;
                            #[cfg(any(test, feature = "test-seams"))]
                            graph_observation_checkpoint(31)?;
                            #[cfg(any(test, feature = "test-seams"))]
                            graph_observation_checkpoint(10)?;
                            Ok(())
                        })
                        .map_err(|error| {
                            Self::compaction_checkpoint_storage_error(
                                error,
                                &on_fingerprint_failure,
                            )
                        })
                },
            )
        });
        #[cfg(any(test, feature = "test-seams"))]
        if result.is_ok() {
            graph_compaction_attempt.committed();
            graph_observation_checkpoint(11)?;
        }
        if let Some(error) = fingerprint_failure.lock().take() {
            return Err(error);
        }
        let progress = result.map_err(Self::storage_error)?;
        #[cfg(any(test, feature = "test-seams"))]
        crate::database::resident_history_test_seams::note(|work| {
            work.compaction_pages_examined += progress.pages_examined;
            work.compaction_pages_relocated += progress.pages_relocated;
        });
        state.1 = progress;
        state.3 = next_controls;
        for _ in 0..2 {
            let txn = self.with_db(Self::begin_history_write)?;
            txn.commit_compaction_frees().map_err(Self::storage_error)?;
        }
        let recycle_micros = if progress.complete {
            let _companion = self.lock_companion_slot()?;
            let mut db_guard = self.lock_database()?;
            self.recycle_database_handle(&mut db_guard)?
        } else {
            None
        };
        Ok(Some((progress, recycle_micros)))
    }

    /// Fraction of the on-disk file that is free/fragmented pages (dead space):
    /// `fragmented / (stored + metadata + fragmented)`. Used to decide whether a
    /// version-compaction pass should follow up with a full redb `compact()` to
    /// reclaim the freed pages. Only system metadata is sampled under a short
    /// writer boundary; application-page traversal uses a read snapshot after
    /// persistence access has been released.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn fragmentation_ratio(&self) -> Result<f64> {
        let sample = self.storage_statistics_sample()?;
        Self::sampled_fragmentation_ratio(&sample)
    }

    /// Captures the point-in-time statistics snapshot without walking it.
    /// Capturing costs one short writer boundary and the system totals; the
    /// application-page walk happens only if [`Self::sampled_fragmentation_ratio`]
    /// is later asked for the ratio, and then reports the file exactly as it
    /// stood at capture. A caller that may not need the ratio captures first
    /// and walks only once it knows it does.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn storage_statistics_sample(&self) -> Result<redb::StorageStatisticsSnapshot> {
        #[cfg(any(test, feature = "test-seams"))]
        STORAGE_STATISTICS_CAPTURES.with(|captures| captures.set(captures.get() + 1));
        self.with_db(|db| {
            db.storage_statistics_snapshot()
                .map_err(Self::storage_error)
        })
    }
}
