use super::*;

impl RedbPersistence {
    /// The free pages only relocation can give back, over the file a trim
    /// would leave: `(free - trailing) / (region pages - trailing)`, from the
    /// totals the allocator maintains as it allocates and frees plus the
    /// bounded free run that ends the last region. No page is read and the
    /// full bitmap is not counted, so the cost is the same at any store size.
    ///
    /// This is the input of the maintenance compaction decisions. Whole
    /// interior free pages are what relocation reclaims; in-page slack is
    /// not, and the trailing free run (growth headroom) is reclaimed by any
    /// trim without a relocation sweep. `None` while the totals are untrusted,
    /// when no decision may be taken.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn allocator_free_page_ratio(&self) -> Result<Option<f64>> {
        self.with_db(|db| {
            Ok(db
                .allocator_free_totals()
                .map(|(free, pages, trailing)| reclaimable_free_ratio(free, pages, trailing)))
        })
    }

    /// The exact fragmentation ratio of a captured sample. This walks every
    /// application page reachable from the sample's snapshot, so its cost is
    /// proportional to the store; it is never part of a cycle that has no
    /// decision to take.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn sampled_fragmentation_ratio(
        sample: &redb::StorageStatisticsSnapshot,
    ) -> Result<f64> {
        let stats = sample.stats().map_err(Self::storage_error)?;
        #[cfg(any(test, feature = "test-seams"))]
        {
            let pages = stats.leaf_pages().saturating_add(stats.branch_pages());
            STORAGE_STATISTICS_PAGES_WALKED
                .with(|walked| walked.set(walked.get().saturating_add(pages)));
            if PRUNING_GUARD_HELD_DEPTH.with(std::cell::Cell::get) > 0 {
                STORAGE_STATISTICS_PAGES_WALKED_UNDER_PRUNING_GUARD
                    .with(|walked| walked.set(walked.get().saturating_add(pages)));
            }
        }
        let stored = stats.stored_bytes();
        let metadata = stats.metadata_bytes();
        let fragmented = stats.fragmented_bytes();
        let total = stored.saturating_add(metadata).saturating_add(fragmented);
        if total == 0 {
            Ok(0.0)
        } else {
            Ok(fragmented as f64 / total as f64)
        }
    }

    // --- Checkpoint-export seams ------------------------------------------
    //
    // Raw dumps pair with the existing raw config writer so export can copy
    // dynamic-key state (tenant watermarks, per-sink queues, audit rings)
    // without decoding it. The append-batch writers stream per-category
    // batches into a fresh artifact; unlike the `rewrite_*` family they never
    // delete-then-rewrite a full slice.

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(test)]
    pub(crate) fn dump_config_raw(&self) -> Result<Vec<(String, Vec<u8>)>> {
        self.dump_str_keyed_table_raw(CONFIG_TABLE)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn digest_canonical_relational_table(
        &self,
        read_txn: &redb::ReadTransaction,
        table_name: &str,
        hasher: &mut blake3::Hasher,
    ) -> Result<u64> {
        let _decode_scope = self.reader_decode_scope()?;
        digest_field(hasher, table_name.as_bytes());
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(table_name);
        let table = read_txn
            .open_table(definition)
            .map_err(Self::storage_error)?;
        let mut count = 0_u64;
        for entry in table.iter().map_err(Self::storage_error)? {
            let (key, value) = entry.map_err(Self::storage_error)?;
            digest_field(hasher, key.value());
            let _row_scope = crate::read_image_memory::active_owner()?
                .row_owner()?
                .enter();
            let row: PersistedVersionedRow = Self::decode(value.value())?;
            rmp_serde::encode::write(
                &mut *hasher,
                &(
                    row.row_id,
                    row.created_tx,
                    row.deleted_tx,
                    row.lsn,
                    row.created_at,
                ),
            )
            .map_err(|err| {
                Error::Other((format!("failed to fingerprint relational row: {err}")).into())
            })?;
            let mut columns = row.values.into_iter().collect::<Vec<_>>();
            columns.sort_by(|left, right| left.0.cmp(&right.0));
            rmp_serde::encode::write(&mut *hasher, &(columns.len() as u64)).map_err(|err| {
                Error::Other((format!("failed to fingerprint relational columns: {err}")).into())
            })?;
            for (column, value) in columns {
                digest_field(hasher, column.as_bytes());
                rmp_serde::encode::write(&mut *hasher, &value).map_err(|err| {
                    Error::Other((format!("failed to fingerprint relational value: {err}")).into())
                })?;
            }
            count = count.saturating_add(1);
        }
        Ok(count)
    }

    /// Fingerprint every Redb table an authenticated schema or data apply can
    /// mutate. Relational rows are decoded and hashed with column-sorted
    /// values because snapshot export re-encodes their `HashMap`; every other
    /// category remains an exact persisted key/value-byte hash. Dynamic
    /// relational and sink-queue table names are part of the digest, so
    /// creating or removing an empty table is still visible.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn raw_sync_apply_state_digest(&self) -> Result<RawSyncApplyStateDigest> {
        let _decode_scope = self.reader_decode_scope()?;
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let mut hasher = blake3::Hasher::new();
            digest_field(&mut hasher, b"contextdb.raw-sync-apply-state.v2");

            digest_redb_table(&read_txn, META_TABLE, "meta", &mut hasher)?;
            digest_redb_table(
                &read_txn,
                FORMAT_METADATA_TABLE,
                "format-metadata",
                &mut hasher,
            )?;
            let persisted_config_entries =
                digest_redb_table(&read_txn, CONFIG_TABLE, "config", &mut hasher)?;
            digest_redb_table(
                &read_txn,
                READ_OPEN_CONFIG_TABLE,
                "read-open-config",
                &mut hasher,
            )?;
            digest_redb_table(
                &read_txn,
                READ_CUSTODY_CONFIG_TABLE,
                "read-custody-config",
                &mut hasher,
            )?;
            digest_redb_table(
                &read_txn,
                READ_LINEAGE_CONFIG_TABLE,
                "read-lineage-config",
                &mut hasher,
            )?;
            let change_log_entries =
                digest_redb_table(&read_txn, CHANGE_LOG_TABLE, "change-log", &mut hasher)?;
            let ddl_log_entries =
                digest_redb_table(&read_txn, DDL_LOG_TABLE, "ddl-log", &mut hasher)?;
            let commit_index_entries =
                digest_redb_table(&read_txn, COMMIT_INDEX_TABLE, "commit-index", &mut hasher)?;
            let sink_audit_entries =
                digest_redb_table(&read_txn, SINK_AUDIT_TABLE, "sink-audit", &mut hasher)?;
            let trigger_audit_entries =
                digest_redb_table(&read_txn, TRIGGER_AUDIT_TABLE, "trigger-audit", &mut hasher)?;
            digest_redb_table(
                &read_txn,
                TRIGGER_AUDIT_STAMPS_TABLE,
                "trigger-audit-stamps",
                &mut hasher,
            )?;
            let edges =
                digest_redb_table(&read_txn, GRAPH_FWD_TABLE, "graph-forward", &mut hasher)?;
            digest_redb_table(&read_txn, GRAPH_REV_TABLE, "graph-reverse", &mut hasher)?;
            digest_redb_table(
                &read_txn,
                graph_history_record::GRAPH_CONTRIBUTIONS,
                "graph-contributions",
                &mut hasher,
            )?;
            digest_redb_table(
                &read_txn,
                graph_history_record::GRAPH_OCCURRENCE_LOCATIONS,
                "graph-occurrence-locations",
                &mut hasher,
            )?;
            digest_redb_table(
                &read_txn,
                graph_history_record::GRAPH_PREDECESSOR_LIFECYCLE,
                "graph-predecessor-lifecycle",
                &mut hasher,
            )?;
            let vectors = digest_redb_table(&read_txn, VECTORS_TABLE, "vectors", &mut hasher)?;
            digest_redb_table(
                &read_txn,
                VECTOR_PARTITION_MEMBERSHIP_TABLE,
                "vector-partition-membership",
                &mut hasher,
            )?;
            digest_redb_table(
                &read_txn,
                VECTOR_OPEN_DIRECTORY_TABLE,
                "vector-open-directory",
                &mut hasher,
            )?;
            digest_redb_table(
                &read_txn,
                COMPACT_INDEX_SET_TABLE,
                "compact-index-set-commitments",
                &mut hasher,
            )?;
            digest_redb_table(
                &read_txn,
                VECTOR_PARTITION_JOURNAL_TABLE,
                "vector-partition-journal",
                &mut hasher,
            )?;
            digest_redb_table(
                &read_txn,
                VECTOR_PARTITION_TOMBSTONE_TABLE,
                "vector-partition-tombstones",
                &mut hasher,
            )?;
            digest_redb_table(
                &read_txn,
                VECTOR_PARTITION_BASE_GENERATION_TABLE,
                "vector-partition-base-generations",
                &mut hasher,
            )?;
            digest_redb_table(
                &read_txn,
                VECTOR_PARTITION_CHANGE_GENERATION_TABLE,
                "vector-partition-change-generations",
                &mut hasher,
            )?;
            digest_redb_table(
                &read_txn,
                VECTOR_PARTITION_GRAPH_PAGE_TABLE,
                "vector-partition-graph-pages",
                &mut hasher,
            )?;
            digest_redb_table(
                &read_txn,
                VECTOR_PARTITION_GENERATION_CATALOG_TABLE,
                "vector-partition-generation-catalog",
                &mut hasher,
            )?;
            digest_redb_table(
                &read_txn,
                SYNC_ROW_SOURCE_LSN_TABLE,
                "sync-row-source-lsn",
                &mut hasher,
            )?;
            digest_redb_table(
                &read_txn,
                SYNC_ROW_SOURCE_KIND_TABLE,
                "sync-row-source-kind",
                &mut hasher,
            )?;

            let mut dynamic_names = read_txn
                .list_tables()
                .map_err(Self::storage_error)?
                .map(|handle| handle.name().to_string())
                .filter(|name| name.starts_with("rel_") || name.starts_with("__sink_queue_"))
                .collect::<Vec<_>>();
            dynamic_names.sort();
            let mut sink_queue_entries = 0_u64;
            let mut retained_row_versions = 0_u64;
            for name in dynamic_names {
                if name.starts_with("rel_") {
                    retained_row_versions = retained_row_versions.saturating_add(
                        self.digest_canonical_relational_table(&read_txn, &name, &mut hasher)?,
                    );
                } else {
                    let definition: TableDefinition<u64, &[u8]> =
                        TableDefinition::new(name.as_str());
                    sink_queue_entries = sink_queue_entries.saturating_add(digest_redb_table(
                        &read_txn,
                        definition,
                        &name,
                        &mut hasher,
                    )?);
                }
            }

            let mut tables = 0_u64;
            let mut index_postings = 0_u64;
            let meta_table = match read_txn.open_table(META_TABLE) {
                Ok(table) => Some(table),
                Err(redb::TableError::TableDoesNotExist(_)) => None,
                Err(error) => return Err(Self::storage_error(error)),
            };
            if let Some(meta_table) = meta_table {
                for entry in meta_table
                    .range("table:".."table;")
                    .map_err(Self::storage_error)?
                {
                    let (key, value) = entry.map_err(Self::storage_error)?;
                    let table = key.value().strip_prefix("table:").ok_or_else(|| {
                        Error::Other("invalid inspection table metadata key".into())
                    })?;
                    let (meta, _) = Self::decode_table_meta_versioned(value.value())?;
                    tables = tables.saturating_add(1);
                    for declaration in Self::relational_index_declarations(&meta) {
                        let name = Self::relational_index_table_name(table, &declaration.name);
                        let postings = read_txn
                            .open_table(TableDefinition::<DurableRelationalIndexKey, &[u8]>::new(
                                &name,
                            ))
                            .map_err(Self::storage_error)?;
                        index_postings = index_postings
                            .saturating_add(postings.len().map_err(Self::storage_error)?);
                    }
                }
            }

            Ok(RawSyncApplyStateDigest {
                digest: hasher.finalize().to_hex().to_string(),
                trigger_audit_entries,
                sink_audit_entries,
                sink_queue_entries,
                tables,
                retained_row_versions,
                index_postings,
                edges,
                vectors,
                ddl_log_entries,
                change_log_entries,
                commit_index_entries,
                persisted_config_entries,
            })
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn dump_trigger_audit_raw(&self) -> Result<Vec<(String, Vec<u8>)>> {
        self.dump_str_keyed_table_raw(TRIGGER_AUDIT_TABLE)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn dump_trigger_audit_stamps_raw(&self) -> Result<Vec<(String, u64)>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(TRIGGER_AUDIT_STAMPS_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                entries.push((key.value().to_string(), value.value()));
            }
            Ok(entries)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn dump_sink_audit_raw(&self) -> Result<Vec<(String, Vec<u8>)>> {
        self.dump_str_keyed_table_raw(SINK_AUDIT_TABLE)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn dump_str_keyed_table_raw(
        &self,
        definition: TableDefinition<&str, &[u8]>,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(definition) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                entries.push((key.value().to_string(), value.value().to_vec()));
            }
            Ok(entries)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn append_trigger_audit_raw(&self, entries: &[(String, Vec<u8>)]) -> Result<()> {
        self.append_str_keyed_table_raw(TRIGGER_AUDIT_TABLE, b"trigger-audit", entries)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn append_trigger_audit_stamps_raw(&self, entries: &[(String, u64)]) -> Result<()> {
        self.prepare_proof_owner()?;
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            {
                let mut table = write_txn
                    .open_table(TRIGGER_AUDIT_STAMPS_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, value) in entries {
                    table
                        .insert(key.as_str(), *value)
                        .map_err(Self::storage_error)?;
                }
            }
            let mutations = entries
                .iter()
                .map(|(key, value)| {
                    content_identity::status_mutation(
                        b"trigger-stamp",
                        key.as_bytes(),
                        &value.to_le_bytes(),
                    )
                })
                .collect::<Vec<_>>();
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
            Ok(())
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn append_sink_audit_raw(&self, entries: &[(String, Vec<u8>)]) -> Result<()> {
        self.append_str_keyed_table_raw(SINK_AUDIT_TABLE, b"sink-audit", entries)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn append_str_keyed_table_raw(
        &self,
        definition: TableDefinition<&str, &[u8]>,
        namespace: &[u8],
        entries: &[(String, Vec<u8>)],
    ) -> Result<()> {
        self.prepare_proof_owner()?;
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            {
                let mut table = write_txn
                    .open_table(definition)
                    .map_err(Self::storage_error)?;
                for (key, value) in entries {
                    table
                        .insert(key.as_str(), value.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            let mutations = entries
                .iter()
                .map(|(key, value)| {
                    content_identity::status_mutation(namespace, key.as_bytes(), value)
                })
                .collect::<Vec<_>>();
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
            Ok(())
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn sink_queue_names(&self) -> Result<Vec<String>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let mut names = Vec::new();
            for handle in read_txn.list_tables().map_err(Self::storage_error)? {
                if let Some(sink) = handle.name().strip_prefix("__sink_queue_") {
                    names.push(sink.to_string());
                }
            }
            Ok(names)
        })
    }

    /// Visit every entry of one durable sink queue in id order, in one read
    /// transaction, without copying the queue.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn visit_sink_queue(
        &self,
        sink: &str,
        mut visit: impl FnMut(u64, &[u8]) -> Result<()>,
    ) -> Result<()> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table_name = Self::sink_queue_table_name(sink);
            let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new(table_name.as_str());
            let table = match read_txn.open_table(table_def) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
                Err(err) => return Err(Self::storage_error(err)),
            };
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                visit(key.value(), value.value())?;
            }
            Ok(())
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn dump_sink_queue_raw(&self, sink: &str) -> Result<Vec<(u64, Vec<u8>)>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table_name = Self::sink_queue_table_name(sink);
            let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new(table_name.as_str());
            let table = match read_txn.open_table(table_def) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                entries.push((key.value(), value.value().to_vec()));
            }
            Ok(entries)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn append_sink_queue_raw(
        &self,
        sink: &str,
        entries: &[(u64, Vec<u8>)],
    ) -> Result<()> {
        let _decode_scope = self.writer_decode_scope()?;
        self.prepare_proof_owner()?;
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            {
                let table_name = Self::sink_queue_table_name(sink);
                let table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                for (id, value) in entries {
                    table
                        .insert(*id, value.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            let mutations = entries
                .iter()
                .map(|(id, value)| {
                    let key = format!("{}:{sink}:{id}", sink.len());
                    let entry: SinkQueueEntry = Self::decode(value)?;
                    content_identity::encoded_status_mutation(b"sink-queue", key.as_bytes(), &entry)
                })
                .collect::<Result<Vec<_>>>()?;
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
            Ok(())
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn append_table_rows_batch(
        &self,
        name: &str,
        meta: Option<&TableMeta>,
        rows: &[VersionedRow],
    ) -> Result<()> {
        self.prepare_proof_owner()?;
        if rows.is_empty() {
            return Ok(());
        }
        // The batch's historical index commitments are paid by this writer.
        let _decode_scope = self.writer_decode_scope()?;
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            Self::invalidate_retention_clock_in_write(&write_txn, name)?;
            let mut commitments = historical_index_commitment::CommitmentBatch::for_writer()?;
            {
                let table_name = Self::rel_table_name(name);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut redb_table = content_size::open_rows(&write_txn, table_def, meta)
                    .map_err(Self::storage_error)?;
                // These rows carry no postings: the caller rebuilds the
                // table's indexes once every batch is written.
                commitments.defer_until_rebuild(name, redb_table.indexes())?;
                let mut encoded = Vec::new();
                for row in rows {
                    Self::encode_versioned_row_into(row, meta, &mut encoded)?;
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
                    let keys = content_size::VersionLife::keys_of(redb_table.indexes(), row)?;
                    redb_table.insert_version(
                        &mut commitments,
                        key.as_slice(),
                        encoded.as_slice(),
                        &content_size::VersionLife {
                            keys: &keys,
                            created: row.created_tx,
                            deleted: row.deleted_tx,
                        },
                    )?;
                }
            }
            // Declared first, so dropped last: these charges outlive the mutations they pay for.
            let mut mutations_commitment_credits: Vec<
                historical_index_commitment::CommitmentCredit,
            > = Vec::new();
            let mut mutations = rows
                .iter()
                .map(|row| content_identity::row_mutation(name, row, meta))
                .collect::<Result<Vec<_>>>()?;
            let (published, credit) = commitments.finish(&write_txn)?.into_parts();
            mutations_commitment_credits.push(credit);
            mutations.extend(published);
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &mutations,
                &mut self.proof_admission(),
            )?;
            for row in rows {
                Self::raise_table_read_lsn_in_write(&write_txn, name, row.lsn)?;
            }
            Self::raise_noncanonical_writer_floors_in_write(
                &write_txn,
                TxId(
                    rows.iter()
                        .flat_map(|row| std::iter::once(row.created_tx).chain(row.deleted_tx))
                        .map(|tx| tx.0)
                        .max()
                        .unwrap_or(0),
                ),
                Lsn(rows.iter().map(|row| row.lsn.0).max().unwrap_or(0)),
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
    pub(crate) fn rebuild_relational_indexes_for_current_table(
        &self,
        name: &str,
        meta: &TableMeta,
    ) -> Result<()> {
        let _decode_scope = self.writer_decode_scope()?;
        self.prepare_proof_owner()?;
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            Self::rebuild_relational_indexes_in_write(
                &write_txn,
                &self.path,
                name,
                meta,
                None,
                &mut self.proof_admission(),
                None,
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
            .map_err(Self::storage_error)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(test)]
    pub(crate) fn checkpoint_read_source(&self) -> Result<CheckpointReadSource> {
        let _decode_scope = self.writer_decode_scope()?;
        let decode_memory = crate::read_image_memory::active_owner()?.row_owner()?;
        self.checkpoint_read_source_with_owner(decode_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn checkpoint_read_source_with_read_memory(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<CheckpointReadSource> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let accountant = self.proof_accountant().ok_or_else(|| {
            Error::Other("checkpoint source requires its installed accountant identity".into())
        })?;
        let decode_memory =
            crate::read_image_memory::ImageMemory::operation_reader(accountant, read_memory)?;
        self.checkpoint_read_source_with_owner(decode_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn checkpoint_read_source_with_owner(
        &self,
        decode_memory: crate::read_image_memory::ImageMemoryHandle,
    ) -> Result<CheckpointReadSource> {
        let _scope = decode_memory.enter();
        self.prepare_proof_owner()?;
        self.with_db(|database| {
            let read = database.begin_read().map_err(Self::storage_error)?;
            let content_identity = content_identity::ContentIdentityReadVerifier::open(
                &read,
                self.path(),
                self.proof_accountant(),
            )?;
            Ok(CheckpointReadSource {
                database_path: self.path().to_path_buf(),
                read,
                content_identity,
                _writer_use: self
                    .proof_accountant()
                    .as_ref()
                    .and_then(crate::memory_accounting::WriterUse::retain_if_writer),
                accountant: self.proof_accountant(),
                decode_memory,
            })
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn damage_checkpoint_f32_projection_for_test(&self) -> Result<()> {
        self.with_db(|database| {
            read_persistence_test_scaffold::damage_f32_row_projection_for_test(database)
                .map_err(|error| Error::Other(error.into()))
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn append_sync_source_provenance_batch(
        &self,
        entries: &[(String, RowId, Lsn, u8)],
    ) -> Result<()> {
        self.commit_sync_reply_effects(entries, &[], &[])
    }

    /// The authoritative hub reply's provenance and config progress share
    /// one Redb transaction; memory publication happens only after success.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn commit_sync_reply_effects(
        &self,
        entries: &[(String, RowId, Lsn, u8)],
        config_writes: &[(String, Vec<u8>)],
        config_deletes: &[String],
    ) -> Result<()> {
        self.prepare_proof_owner()?;
        if entries.is_empty() && config_writes.is_empty() && config_deletes.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            {
                let mut lsns = write_txn
                    .open_table(SYNC_ROW_SOURCE_LSN_TABLE)
                    .map_err(Self::storage_error)?;
                let mut kinds = write_txn
                    .open_table(SYNC_ROW_SOURCE_KIND_TABLE)
                    .map_err(Self::storage_error)?;
                for (table, row_id, lsn, kind) in entries {
                    let key = Self::sync_row_source_lsn_key(table, *row_id);
                    lsns.insert(key.as_slice(), lsn.0)
                        .map_err(Self::storage_error)?;
                    kinds
                        .insert(key.as_slice(), *kind)
                        .map_err(Self::storage_error)?;
                }
            }
            let mut identity_mutations = entries
                .iter()
                .map(|(table, row_id, lsn, kind)| {
                    content_identity::sync_source_mutation(table, *row_id, lsn.0, *kind)
                })
                .collect::<Vec<_>>();
            {
                let mut config = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for key in config_deletes {
                    config.remove(key.as_str()).map_err(Self::storage_error)?;
                    identity_mutations.extend(content_identity::config_mutations(key, None));
                }
                for (key, value) in config_writes {
                    Self::raise_config_identity_floors_in_write(&write_txn, key, value)?;
                    config
                        .insert(key.as_str(), value.as_slice())
                        .map_err(Self::storage_error)?;
                    identity_mutations.extend(content_identity::config_mutations(key, Some(value)));
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
            .map_err(Self::storage_error)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn append_graph_edges_batch(&self, edges: &[AdjEntry]) -> Result<()> {
        self.prepare_proof_owner()?;
        if edges.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            {
                let mut fwd_table = write_txn
                    .open_table(GRAPH_FWD_TABLE)
                    .map_err(Self::storage_error)?;
                let mut rev_table = write_txn
                    .open_table(GRAPH_REV_TABLE)
                    .map_err(Self::storage_error)?;
                let mut encoded = Vec::new();
                for entry in edges {
                    Self::encode_into(entry, &mut encoded)?;
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
            let mutations = edges
                .iter()
                .map(content_identity::edge_mutation)
                .collect::<Result<Vec<_>>>()?;
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &mutations,
                &mut self.proof_admission(),
            )?;
            Self::raise_noncanonical_writer_floors_in_write(
                &write_txn,
                TxId(
                    edges
                        .iter()
                        .flat_map(|edge| std::iter::once(edge.created_tx).chain(edge.deleted_tx))
                        .map(|tx| tx.0)
                        .max()
                        .unwrap_or(0),
                ),
                Lsn(edges.iter().map(|edge| edge.lsn.0).max().unwrap_or(0)),
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
    pub(crate) fn append_vector_entries_batch(
        &self,
        entries: &[VectorEntry],
        quantization: &HashMap<VectorIndexRef, VectorQuantization>,
    ) -> Result<()> {
        self.prepare_proof_owner()?;
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            {
                let mut table =
                    content_size::open_vectors(&write_txn).map_err(Self::storage_error)?;
                for entry in entries {
                    let quantization = quantization.get(&entry.index).copied().unwrap_or_default();
                    let encoded = Self::encode_vector_entry(entry, quantization)?;
                    let key = Self::vector_key(entry);
                    table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            let mut mutations = Vec::with_capacity(entries.len().saturating_mul(2));
            for entry in entries {
                let quantization = quantization.get(&entry.index).copied().unwrap_or_default();
                mutations.push(content_identity::vector_mutation(entry, quantization));
                mutations.push(content_identity::vector_version_mutation(
                    entry,
                    quantization,
                ));
            }
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &mutations,
                &mut self.proof_admission(),
            )?;
            for entry in entries {
                Self::raise_table_read_lsn_in_write(&write_txn, &entry.index.table, entry.lsn)?;
            }
            Self::raise_noncanonical_writer_floors_in_write(
                &write_txn,
                TxId(
                    entries
                        .iter()
                        .flat_map(|entry| std::iter::once(entry.created_tx).chain(entry.deleted_tx))
                        .map(|tx| tx.0)
                        .max()
                        .unwrap_or(0),
                ),
                Lsn(entries.iter().map(|entry| entry.lsn.0).max().unwrap_or(0)),
                RowId(
                    entries
                        .iter()
                        .map(|entry| entry.row_id.0)
                        .max()
                        .unwrap_or(0),
                ),
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

    /// Append the current-format placement directory paired with an exported
    /// vector body. Snapshot artifacts are opened through this directory;
    /// writing only the body table leaves a valid F32 row with no searchable
    /// vector after reopen.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn append_vector_partition_memberships_batch(
        &self,
        records: &[VectorPartitionMembershipRecord],
    ) -> Result<()> {
        let _decode_scope = self.writer_decode_scope()?;
        self.prepare_proof_owner()?;
        if records.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            {
                let mut table = write_txn
                    .open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE)
                    .map_err(Self::storage_error)?;
                for record in records {
                    self.insert_vector_partition_membership(&write_txn, &mut table, record)?;
                }
            }
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &[],
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

    /// Corrupt-store-only fixture seam. This writes valid checksummed records
    /// and authenticated directory companions, deliberately bypassing placement
    /// overlap validation so loader corruption detection can be tested separately.
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from vector_current_identity.rs"
        )
    )]
    #[allow(dead_code, reason = "KEEP-SEAM: test helper behind a test cfg; unit tests or the test-seams build use it")]
    pub(crate) fn write_corrupt_store_vector_memberships_for_test(
        &self,
        records: &[VectorPartitionMembershipRecord],
    ) -> Result<()> {
        let _decode_scope = self.writer_decode_scope()?;
        self.prepare_proof_owner()?;
        if records.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            {
                let mut table = write_txn
                    .open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE)
                    .map_err(Self::storage_error)?;
                for record in records {
                    let key = Self::vector_partition_membership_key(record);
                    let encoded = Self::encode_vector_partition_record(
                        VectorPartitionRecordKind::Membership,
                        record,
                    )?;
                    table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    self.write_open_directory_entry_impl::<true>(
                        &write_txn,
                        &key,
                        Some(&encoded),
                        true,
                    )?;
                }
            }
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &[],
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

    /// Each entry carries its per-LSN key index, tracked by the caller across
    /// batches so a multi-entry commit split over two batches never collides.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn append_change_log_entries_batch(
        &self,
        entries: &[(usize, ChangeLogEntry)],
    ) -> Result<()> {
        let _decode_scope = self.writer_decode_scope()?;
        self.prepare_proof_owner()?;
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let previous_root = db.begin_read().map_err(Self::storage_error)?;
            let write = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
            self.publish_public_change_log_in_write(
                &write,
                &previous_root,
                false,
                entries
                    .iter()
                    .map(|(index, entry)| (entry.lsn(), *index, entry)),
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
    pub(crate) fn append_ddl_log_entries(&self, entries: &[(Lsn, DdlChange)]) -> Result<()> {
        self.prepare_proof_owner()?;
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            let mut identity_mutations = Vec::new();
            {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                // The log is LSN-ordered; key same-LSN runs exactly as the
                // commit-time writer does (plain key for a single change,
                // indexed keys for a multi-change commit).
                let mut start = 0usize;
                while start < entries.len() {
                    let lsn = entries[start].0;
                    let mut end = start + 1;
                    while end < entries.len() && entries[end].0 == lsn {
                        end += 1;
                    }
                    let count = end - start;
                    for (offset, (_, change)) in entries[start..end].iter().enumerate() {
                        let key = Self::ddl_log_key_for_index(lsn, offset, count);
                        self.append_ddl_summary_in_write(&write_txn, &key, change)?;
                        let encoded = Self::encode(change)?;
                        ddl_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        identity_mutations
                            .push(content_identity::encoded_history_mutation(&key, change)?);
                    }
                    start = end;
                }
            }
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &identity_mutations,
                &mut self.proof_admission(),
            )?;
            Self::raise_noncanonical_writer_floors_in_write(
                &write_txn,
                TxId(0),
                Lsn(entries.iter().map(|(lsn, _)| lsn.0).max().unwrap_or(0)),
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
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from persistence.rs"
        )
    )]
    pub(super) fn load_graph_table(
        &self,
        definition: TableDefinition<&[u8], &[u8]>,
    ) -> Result<Vec<AdjEntry>> {
        let _decode_scope = self.reader_decode_scope()?;
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(definition) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (_key, value) = entry.map_err(Self::storage_error)?;
                #[cfg(feature = "in-process-test-seams")]
                crate::writer_query_test_seams::note_history(
                    crate::writer_query_test_seams::HistoryFamily::Graph,
                );
                #[cfg(any(test, feature = "test-seams"))]
                crate::read_work_probe::note_graph(
                    "graph_payload",
                    "payload_decode",
                    _key.value(),
                    None,
                    Some("complete_graph_load"),
                );
                entries.push(Self::decode(value.value())?);
            }
            Ok(entries)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn load_table_meta_in_read_txn(
        read_txn: &redb::ReadTransaction,
        name: &str,
    ) -> Result<Option<TableMeta>> {
        let meta_table = match read_txn.open_table(META_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(err) => return Err(Self::storage_error(err)),
        };
        let key = Self::meta_key(name);
        meta_table
            .get(key.as_str())
            .map_err(Self::storage_error)?
            .map(|value| Self::decode_table_meta_versioned(value.value()).map(|(meta, _)| meta))
            .transpose()
    }

    pub(crate) fn vector_quantization_map(
        table_meta: &HashMap<String, TableMeta>,
    ) -> HashMap<VectorIndexRef, VectorQuantization> {
        let mut indexes = HashMap::new();
        for (table, meta) in table_meta {
            for column in &meta.columns {
                if matches!(column.column_type, ColumnType::Vector(_)) {
                    indexes.insert(
                        VectorIndexRef::new(table.clone(), column.name.clone()),
                        column.quantization,
                    );
                }
            }
        }
        indexes
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn admitted_vector_quantization_map(
        table_meta: &HashMap<String, TableMeta>,
        mut admit: impl FnMut(usize) -> Result<()>,
    ) -> Result<HashMap<VectorIndexRef, VectorQuantization>> {
        let mut count = 0usize;
        let mut names = 0usize;
        for (table, meta) in table_meta {
            for column in &meta.columns {
                if matches!(column.column_type, ColumnType::Vector(_)) {
                    count = count
                        .checked_add(1)
                        .ok_or_else(|| Error::Other("vector declaration count overflow".into()))?;
                    names = names
                        .saturating_add(table.len())
                        .saturating_add(column.name.len());
                }
            }
        }
        admit(
            crate::memory_accounting::native_map_plan::<VectorIndexRef, VectorQuantization>(count)?
                .layout
                .size()
                .saturating_add(names),
        )?;
        let mut indexes = HashMap::with_capacity(count);
        for (table, meta) in table_meta {
            for column in &meta.columns {
                if matches!(column.column_type, ColumnType::Vector(_)) {
                    indexes.insert(
                        VectorIndexRef::new(table.clone(), column.name.clone()),
                        column.quantization,
                    );
                }
            }
        }
        Ok(indexes)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn vector_quantization_in_write(
        write_txn: &redb::WriteTransaction,
        index: &VectorIndexRef,
    ) -> Result<VectorQuantization> {
        let meta_table = write_txn
            .open_table(META_TABLE)
            .map_err(Self::storage_error)?;
        let key = Self::meta_key(&index.table);
        let meta = meta_table
            .get(key.as_str())
            .map_err(Self::storage_error)?
            .map(|value| Self::decode_table_meta_versioned(value.value()).map(|(meta, _)| meta))
            .transpose()?;
        Ok(Self::column_quantization(meta.as_ref(), &index.column))
    }

    pub(super) fn column_quantization(
        meta: Option<&TableMeta>,
        column_name: &str,
    ) -> VectorQuantization {
        meta.and_then(|meta| {
            meta.columns
                .iter()
                .find(|column| {
                    column.name == column_name
                        && matches!(column.column_type, ColumnType::Vector(_))
                })
                .map(|column| column.quantization)
        })
        .unwrap_or_default()
    }

    pub(super) fn write_set_visibility_tx(ws: &WriteSet) -> Option<TxId> {
        if let Some(tx) = ws.finalized_tx {
            return Some(tx);
        }
        ws.relational_inserts
            .iter()
            .flat_map(|(_, row)| std::iter::once(row.created_tx).chain(row.deleted_tx))
            .chain(
                ws.relational_deletes
                    .iter()
                    .map(|(_, _, deleted_tx)| *deleted_tx),
            )
            .chain(
                ws.adj_inserts
                    .iter()
                    .flat_map(|entry| std::iter::once(entry.created_tx).chain(entry.deleted_tx)),
            )
            .chain(
                ws.adj_deletes
                    .iter()
                    .map(|(_, _, _, deleted_tx)| *deleted_tx),
            )
            .chain(
                ws.vector_inserts
                    .iter()
                    .flat_map(|entry| std::iter::once(entry.created_tx).chain(entry.deleted_tx)),
            )
            .chain(
                ws.vector_deletes
                    .iter()
                    .map(|(_, _, deleted_tx)| *deleted_tx),
            )
            .chain(ws.vector_moves.iter().map(|(_, _, _, tx)| *tx))
            .max()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn encode_versioned_row(
        row: &VersionedRow,
        meta: Option<&TableMeta>,
    ) -> Result<Vec<u8>> {
        let mut encoded = Vec::new();
        Self::encode_versioned_row_into(row, meta, &mut encoded)?;
        Ok(encoded)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn encode_versioned_row_into(
        row: &VersionedRow,
        meta: Option<&TableMeta>,
        encoded: &mut Vec<u8>,
    ) -> Result<()> {
        Self::encode_versioned_row_with_deleted_tx_into(row, row.deleted_tx, meta, encoded)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn encode_versioned_row_with_deleted_tx_into(
        row: &VersionedRow,
        deleted_tx: Option<TxId>,
        meta: Option<&TableMeta>,
        encoded: &mut Vec<u8>,
    ) -> Result<()> {
        Self::encode_versioned_row_with_f32_overlay_into(row, deleted_tx, meta, None, encoded)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn encode_versioned_row_with_f32_overlay_into(
        row: &VersionedRow,
        deleted_tx: Option<TxId>,
        meta: Option<&TableMeta>,
        f32_overlay: Option<&F32RowOverlay<'_>>,
        encoded: &mut Vec<u8>,
    ) -> Result<()> {
        // Each owned buffer also carries native allocation bookkeeping.
        // Keep the pointer/size pair with the buffer's admission; the native
        // allocation oracle includes that bookkeeping in its physical peak.
        let allocation_control = std::mem::size_of::<[usize; 2]>();
        let retained =
            crate::memory_accounting::native_map_plan::<String, PersistedValue>(row.values.len())?
                .layout
                .size()
                .saturating_add(if row.values.is_empty() {
                    0
                } else {
                    allocation_control
                })
                .saturating_add(
                    row.values
                        .iter()
                        .map(|(column, value)| {
                            column
                                .len()
                                .saturating_add(if column.is_empty() {
                                    0
                                } else {
                                    allocation_control
                                })
                                .saturating_add(value.estimated_bytes())
                                .saturating_add(match value {
                                    Value::Vector(vector)
                                        if !vector.is_empty()
                                            && matches!(
                                                Self::column_quantization(meta, column),
                                                VectorQuantization::F32
                                            ) =>
                                    {
                                        allocation_control
                                    }
                                    Value::Text(text) if !text.is_empty() => allocation_control,
                                    _ => 0,
                                })
                        })
                        .sum::<usize>(),
                )
                .saturating_add(f32_overlay.map_or(0, |vectors| {
                    vectors
                        .values()
                        .map(|vector| {
                            vector
                                .len()
                                .saturating_mul(std::mem::size_of::<f32>())
                                .saturating_add(if vector.is_empty() {
                                    0
                                } else {
                                    allocation_control
                                })
                        })
                        .sum::<usize>()
                }));
        crate::read_image_memory::reserve(retained)?;
        let values = row
            .values
            .iter()
            .map(|(column, value)| {
                let persisted = match value {
                    Value::Vector(vector) => {
                        let quantization = Self::column_quantization(meta, column);
                        if matches!(quantization, VectorQuantization::F32) {
                            PersistedValue::Vector(PersistedVector::from_f32(vector, quantization))
                        } else {
                            PersistedValue::Plain(Value::Null)
                        }
                    }
                    Value::Null
                        if f32_overlay
                            .and_then(|vectors| vectors.get(column))
                            .is_some()
                            && matches!(
                                Self::column_quantization(meta, column),
                                VectorQuantization::F32
                            ) =>
                    {
                        PersistedValue::Vector(PersistedVector::from_f32(
                            f32_overlay
                                .and_then(|vectors| vectors.get(column))
                                .expect("checked F32 row-vector overlay"),
                            VectorQuantization::F32,
                        ))
                    }
                    _ => PersistedValue::Plain(value.clone()),
                };
                (column.clone(), persisted)
            })
            .collect::<HashMap<_, _>>();
        let persisted = PersistedVersionedRow {
            row_id: row.row_id,
            values,
            created_tx: row.created_tx,
            deleted_tx,
            lsn: row.lsn,
            created_at: row.created_at,
        };
        let mut measured = OwnedEventEncodedLength(0);
        bincode::serde::encode_into_std_write(
            &persisted,
            &mut measured,
            bincode::config::standard(),
        )
        .map_err(|error| Error::Other(format!("bincode encode error: {error}").into()))?;
        crate::read_image_memory::reserve_output(measured.0.saturating_add(if measured.0 == 0 {
            0
        } else {
            allocation_control
        }))?;
        // The row-local output is empty; one exact buffer avoids geometric growth.
        *encoded = vec![0; measured.0];
        let written = bincode::serde::encode_into_slice(
            &persisted,
            encoded.as_mut_slice(),
            bincode::config::standard(),
        )
        .map_err(|error| Error::Other(format!("bincode encode error: {error}").into()))?;
        if written != measured.0 {
            return Err(Error::Other(
                "row encoding differs from its measured size".into(),
            ));
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn f32_vectors_from_row<'a>(
        row: &VersionedRow,
        meta: Option<&TableMeta>,
    ) -> Result<F32RowOverlay<'a>> {
        crate::read_image_memory::reserve(
            crate::memory_accounting::native_map_plan::<String, std::borrow::Cow<'_, [f32]>>(
                row.values.len(),
            )?
            .layout
            .size()
            .saturating_add(
                row.values
                    .iter()
                    .map(|(column, value)| column.len().saturating_add(value.estimated_bytes()))
                    .sum::<usize>(),
            ),
        )?;
        let mut vectors = HashMap::with_capacity(row.values.len());
        for (column, value) in &row.values {
            match value {
                Value::Vector(vector)
                    if matches!(
                        Self::column_quantization(meta, column),
                        VectorQuantization::F32
                    ) =>
                {
                    vectors.insert(column.clone(), std::borrow::Cow::Owned(vector.clone()));
                }
                _ => {}
            }
        }
        Ok(vectors)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn decode_versioned_row(
        bytes: &[u8],
        _meta: Option<&TableMeta>,
    ) -> Result<VersionedRow> {
        // Same wire order and variants, decoded directly into the final map.
        // No intermediate persisted HashMap or copied column names exist.
        struct RowValue(Value);
        impl<'de> serde::Deserialize<'de> for RowValue {
            fn deserialize<D: serde::Deserializer<'de>>(
                d: D,
            ) -> std::result::Result<Self, D::Error> {
                let value = PersistedValue::deserialize(d)?;
                Ok(Self(match value {
                    PersistedValue::Plain(value) => value,
                    PersistedValue::Vector(PersistedVector::F32(values)) => Value::Vector(values),
                    PersistedValue::Vector(vector) => {
                        crate::read_image_memory::reserve_output(vector.decoded_bytes())
                            .map_err(serde::de::Error::custom)?;
                        Value::Vector(vector.to_f32())
                    }
                }))
            }
        }
        struct RowValues(HashMap<String, Value>);
        impl<'de> serde::Deserialize<'de> for RowValues {
            fn deserialize<D: serde::Deserializer<'de>>(
                d: D,
            ) -> std::result::Result<Self, D::Error> {
                struct Values;
                impl<'de> serde::de::Visitor<'de> for Values {
                    type Value = RowValues;
                    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        f.write_str("row values")
                    }
                    fn visit_map<A: serde::de::MapAccess<'de>>(
                        self,
                        mut map: A,
                    ) -> std::result::Result<RowValues, A::Error> {
                        let len = map
                            .size_hint()
                            .ok_or_else(|| serde::de::Error::custom("row map length absent"))?;
                        let mut values = HashMap::with_capacity(len);
                        let mut seen = 0usize;
                        while let Some((key, value)) = map.next_entry::<String, RowValue>()? {
                            if seen == len {
                                return Err(serde::de::Error::custom(
                                    "row map exceeded encoded length",
                                ));
                            }
                            seen += 1;
                            values.insert(key, value.0);
                        }
                        if seen != len {
                            return Err(serde::de::Error::custom(
                                "row map ended before encoded length",
                            ));
                        }
                        Ok(RowValues(values))
                    }
                }
                crate::read_image_memory::with_exact_map::<String, Value, _>(|| {
                    d.deserialize_map(Values)
                })
            }
        }
        #[derive(Deserialize)]
        struct RowWire {
            row_id: RowId,
            values: RowValues,
            created_tx: TxId,
            deleted_tx: Option<TxId>,
            lsn: Lsn,
            created_at: Option<contextdb_core::Wallclock>,
        }
        let scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
        let (row, _): (RowWire, usize) =
            crate::read_image_memory::decode_exact_buffers(&scope, bytes)?;
        Ok(VersionedRow {
            row_id: row.row_id,
            values: row.values.0,
            created_tx: row.created_tx,
            deleted_tx: row.deleted_tx,
            lsn: row.lsn,
            created_at: row.created_at,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn encode_vector_entry(
        entry: &VectorEntry,
        quantization: VectorQuantization,
    ) -> Result<Vec<u8>> {
        Self::encode(&PersistedVectorEntry {
            index: entry.index.clone(),
            row_id: entry.row_id,
            vector: PersistedVector::from_f32(&entry.vector, quantization),
            created_tx: entry.created_tx,
            deleted_tx: entry.deleted_tx,
            lsn: entry.lsn,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn encode_vector_entry_admitted(
        entry: &VectorEntry,
        quantization: VectorQuantization,
    ) -> Result<Vec<u8>> {
        crate::read_image_memory::reserve(
            entry
                .index
                .table
                .len()
                .saturating_add(entry.index.column.len())
                .saturating_add(
                    entry
                        .vector
                        .len()
                        .saturating_mul(std::mem::size_of::<f32>()),
                ),
        )?;
        let persisted = PersistedVectorEntry {
            index: entry.index.clone(),
            row_id: entry.row_id,
            vector: PersistedVector::from_f32(&entry.vector, quantization),
            created_tx: entry.created_tx,
            deleted_tx: entry.deleted_tx,
            lsn: entry.lsn,
        };
        let mut measured = OwnedEventEncodedLength(0);
        bincode::serde::encode_into_std_write(
            &persisted,
            &mut measured,
            bincode::config::standard(),
        )
        .map_err(|error| Error::Other(format!("bincode encode error: {error}").into()))?;
        crate::read_image_memory::reserve_output(measured.0)?;
        let mut encoded = vec![0; measured.0];
        let written = bincode::serde::encode_into_slice(
            &persisted,
            encoded.as_mut_slice(),
            bincode::config::standard(),
        )
        .map_err(|error| Error::Other(format!("bincode encode error: {error}").into()))?;
        if written != measured.0 {
            return Err(Error::Other(
                "vector encoding differs from its measured size".into(),
            ));
        }
        Ok(encoded)
    }

    /// Decode one stored vector entry with every vector body at exactly its
    /// encoded length. A stored f32 body is admitted before it is allocated
    /// as a kept output -- a caller that prepaid its kept vectors has that
    /// credit adopted -- and a quantized payload as exact-length scratch.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn decode_persisted_vector_entry_exact(
        bytes: &[u8],
    ) -> Result<PersistedVectorEntry> {
        // The stored layout of `PersistedVectorEntry`, field for field and
        // variant for variant.
        #[derive(Deserialize)]
        struct StoredVectorEntry {
            index: VectorIndexRef,
            row_id: contextdb_core::RowId,
            vector: StoredVector,
            created_tx: contextdb_core::TxId,
            deleted_tx: Option<contextdb_core::TxId>,
            lsn: Lsn,
        }
        #[derive(Deserialize)]
        enum StoredVector {
            F32(#[serde(deserialize_with = "kept_f32")] Vec<f32>),
            SQ8 {
                min: f32,
                max: f32,
                len: u32,
                #[serde(deserialize_with = "scratch_bytes")]
                payload: Vec<u8>,
            },
            SQ4 {
                min: f32,
                max: f32,
                len: u32,
                #[serde(deserialize_with = "scratch_bytes")]
                payload: Vec<u8>,
            },
        }
        fn kept_f32<'de, D: serde::Deserializer<'de>>(
            deserializer: D,
        ) -> std::result::Result<Vec<f32>, D::Error> {
            crate::read_image_memory::deserialize_exact_vec(deserializer, true)
        }
        fn scratch_bytes<'de, D: serde::Deserializer<'de>>(
            deserializer: D,
        ) -> std::result::Result<Vec<u8>, D::Error> {
            crate::read_image_memory::deserialize_exact_vec(deserializer, false)
        }
        let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
        let stored: StoredVectorEntry = Self::decode(bytes)?;
        Ok(PersistedVectorEntry {
            index: stored.index,
            row_id: stored.row_id,
            vector: match stored.vector {
                StoredVector::F32(values) => PersistedVector::F32(values),
                StoredVector::SQ8 {
                    min,
                    max,
                    len,
                    payload,
                } => PersistedVector::SQ8 {
                    min,
                    max,
                    len,
                    payload,
                },
                StoredVector::SQ4 {
                    min,
                    max,
                    len,
                    payload,
                } => PersistedVector::SQ4 {
                    min,
                    max,
                    len,
                    payload,
                },
            },
            created_tx: stored.created_tx,
            deleted_tx: stored.deleted_tx,
            lsn: stored.lsn,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn decode_vector_entry(bytes: &[u8]) -> Result<VectorEntry> {
        crate::read_image_memory::ImageMemory::with_vector_body_decode(|| {
            Self::decode_vector_entry_inner(bytes)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn decode_vector_entry_inner(bytes: &[u8]) -> Result<VectorEntry> {
        #[cfg(any(test, feature = "test-seams"))]
        crate::database::resident_history_test_seams::note(|work| work.stored_vector_decodes += 1);
        // Borrow the stored body and scalar header. Only the final F32 Vec
        // and route strings are allocated; quantized payloads need no mirror.
        enum Encoded<'a> {
            F32(&'a [u8]),
            Quantized {
                min: f32,
                max: f32,
                len: usize,
                payload: &'a [u8],
                packed: bool,
            },
        }
        let parsed = (|| {
            let (((table, column), row_id, variant), prefix): (((&str, &str), RowId, u32), usize) =
                bincode::serde::borrow_decode_from_slice(bytes, bincode::config::standard()).ok()?;
            let (body, end) = if variant == 0 {
                let (len, length_bytes): (usize, usize) = bincode::serde::decode_from_slice(
                    bytes.get(prefix..)?,
                    bincode::config::standard(),
                )
                .ok()?;
                let start = prefix.checked_add(length_bytes)?;
                let end = start.checked_add(len.checked_mul(std::mem::size_of::<f32>())?)?;
                (Encoded::F32(bytes.get(start..end)?), end)
            } else if variant == 1 || variant == 2 {
                let ((min, max, len, payload), used): ((f32, f32, u32, &[u8]), usize) =
                    bincode::serde::borrow_decode_from_slice(
                        bytes.get(prefix..)?,
                        bincode::config::standard(),
                    )
                    .ok()?;
                let len = len as usize;
                let packed = variant == 2;
                if payload.len() != if packed { len.div_ceil(2) } else { len } {
                    return None;
                }
                (
                    Encoded::Quantized {
                        min,
                        max,
                        len,
                        payload,
                        packed,
                    },
                    prefix.checked_add(used)?,
                )
            } else {
                return None;
            };
            let ((created_tx, deleted_tx, lsn), used): ((TxId, Option<TxId>, Lsn), usize) =
                bincode::serde::decode_from_slice(bytes.get(end..)?, bincode::config::standard())
                    .ok()?;
            if end.checked_add(used)? != bytes.len() {
                return None;
            }
            Some((table, column, row_id, body, created_tx, deleted_tx, lsn))
        })();
        if let Some((table, column, row_id, body, created_tx, deleted_tx, lsn)) = parsed {
            let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
            let len = match &body {
                Encoded::F32(bytes) => bytes.len() / std::mem::size_of::<f32>(),
                Encoded::Quantized { len, .. } => *len,
            };
            crate::read_image_memory::reserve_output(
                len.checked_mul(std::mem::size_of::<f32>())
                    .ok_or_else(|| Error::Other("vector body size overflow".into()))?,
            )?;
            // The vector body's prepaid workspace does not cover index names.
            crate::read_image_memory::reserve(
                table
                    .len()
                    .checked_add(column.len())
                    .ok_or_else(|| Error::Other("vector index size overflow".into()))?,
            )?;
            let mut vector = Vec::with_capacity(len);
            match body {
                Encoded::F32(body) => {
                    for component in body.chunks_exact(4) {
                        vector.push(f32::from_le_bytes(
                            component.try_into().expect("one f32 component"),
                        ));
                    }
                }
                Encoded::Quantized {
                    min,
                    max,
                    len,
                    payload,
                    packed,
                } => {
                    for position in 0..len {
                        let byte = payload[if packed { position / 2 } else { position }];
                        let q = if packed {
                            if position % 2 == 0 {
                                byte >> 4
                            } else {
                                byte & 0x0f
                            }
                        } else {
                            byte
                        };
                        vector.push(if packed {
                            sq4_component(min, max, q)
                        } else {
                            sq8_component(min, max, q)
                        });
                    }
                }
            }
            return Ok(VectorEntry {
                index: VectorIndexRef::new(table.to_owned(), column.to_owned()),
                row_id,
                vector,
                created_tx,
                deleted_tx,
                lsn,
            });
        }
        // A stored f32 body becomes the entry's vector without a copy; a
        // quantized body's expansion is the kept output.
        let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
        let stored = Self::decode_persisted_vector_entry_exact(bytes)?;
        let vector = match stored.vector {
            PersistedVector::F32(values) => values,
            quantized => {
                crate::read_image_memory::reserve_output(quantized.decoded_bytes())?;
                quantized.to_f32()
            }
        };
        Ok(VectorEntry {
            index: stored.index,
            row_id: stored.row_id,
            vector,
            created_tx: stored.created_tx,
            deleted_tx: stored.deleted_tx,
            lsn: stored.lsn,
        })
    }

    pub(crate) fn rel_table_name(name: &str) -> String {
        format!("rel_{name}")
    }

    /// The durable index order, over arbitrary stored bytes.
    ///
    /// Redb calls this while navigating its own B-tree, where it cannot return
    /// an error, so it must be total and it must not panic: the derived
    /// `RelationalIndexPosition` ordering panics on unlike `Value` variants and
    /// answers `Equal` for a direction mismatch, and stored bytes can carry
    /// either. Every pair the engine itself writes is validated and ordered
    /// exactly as before, through one lexicographic order that is total and
    /// transitive over every byte string: decoding keys before non-decoding
    /// ones, typed components then row and transaction, raw bytes as the final
    /// tiebreak. Choosing between typed and raw order per pair cannot be
    /// transitive, and the read path still refuses a malformed posting as
    /// corrupt before any row derived from it is delivered.
    pub(crate) fn compare_relational_index_order_keys(
        left: &[u8],
        right: &[u8],
    ) -> std::cmp::Ordering {
        content_identity::total_posting_order(left, right)
    }

    /// Typed validation of one decoded physical posting against the index that
    /// is supposed to have written it. A stored key that disagrees with the
    /// declaration cannot be ordered against its siblings, so it is refused
    /// before it can contribute a row.
    pub(crate) fn validated_index_position<'a>(
        declaration: &'a IndexDecl,
        meta: &TableMeta,
        position: &RelationalIndexPosition,
    ) -> std::result::Result<(), IndexValidationFailure<'a>> {
        Self::validated_index_key(declaration, meta, &position.key)
    }

    pub(crate) fn validated_index_key<'a>(
        declaration: &'a IndexDecl,
        meta: &TableMeta,
        key: &[DirectedValue],
    ) -> std::result::Result<(), IndexValidationFailure<'a>> {
        if key.len() != declaration.columns.len() {
            return Err(IndexValidationFailure {
                declaration,
                column: None,
                parts: Some(key.len()),
                suffix: "",
            });
        }
        for (part, (column, direction)) in key.iter().zip(declaration.columns.iter()) {
            let (descending, value) = match part {
                DirectedValue::Asc(TotalOrdAsc(value)) => (false, value),
                DirectedValue::Desc(TotalOrdDesc(value)) => (true, value),
            };
            if descending != matches!(direction, contextdb_core::SortDirection::Desc) {
                return Err(IndexValidationFailure {
                    declaration,
                    column: Some((column, "orders ")),
                    parts: None,
                    suffix: " in the direction the declaration does not",
                });
            }
            let Some(declared) = meta
                .columns
                .iter()
                .find(|declared| declared.name.as_str() == column.as_str())
            else {
                return Err(IndexValidationFailure {
                    declaration,
                    column: Some((column, "names column ")),
                    parts: None,
                    suffix: ", which the table does not have",
                });
            };
            let matches_declared = match (&declared.column_type, value) {
                (_, Value::Null) => true,
                (ColumnType::Integer, Value::Int64(_))
                | (ColumnType::Real, Value::Float64(_))
                | (ColumnType::Text, Value::Text(_))
                | (ColumnType::Boolean, Value::Bool(_))
                | (ColumnType::Uuid, Value::Uuid(_))
                | (ColumnType::Timestamp, Value::Timestamp(_))
                | (ColumnType::TxId, Value::TxId(_)) => true,
                _ => false,
            };
            if !matches_declared {
                return Err(IndexValidationFailure {
                    declaration,
                    column: Some((column, "holds a ")),
                    parts: None,
                    suffix: " value of a type the column does not declare",
                });
            }
        }
        Ok(())
    }

    pub(super) fn relational_index_family_name(table: &str, index: &str, suffix: &str) -> String {
        let mut hash = blake3::Hasher::new();
        hash.update(table.as_bytes());
        hash.update(&[0]);
        hash.update(index.as_bytes());
        let encoded = hash.finalize().to_hex();
        let mut name = String::with_capacity(12 + encoded.len() + suffix.len());
        name.push_str("__rel_index_");
        name.push_str(encoded.as_str());
        name.push_str(suffix);
        name
    }

    pub(super) fn relational_index_table_name(table: &str, index: &str) -> String {
        Self::relational_index_family_name(table, index, "")
    }

    pub(super) fn current_relational_index_table_name(table: &str, index: &str) -> String {
        Self::relational_index_family_name(table, index, "_current")
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn relational_index_count_key(
        table: &str,
        columns: &[(String, contextdb_core::SortDirection)],
        prefix: &[DirectedValue],
    ) -> Result<Vec<u8>> {
        struct Prefix<'a>(&'a [DirectedValue]);
        struct Canonical<'a>(&'a Value);
        impl serde::Serialize for Canonical<'_> {
            fn serialize<S: serde::Serializer>(
                &self,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                match self.0 {
                    Value::Float64(value) => {
                        Value::Float64(contextdb_core::float8_canonical(*value))
                            .serialize(serializer)
                    }
                    value => value.serialize(serializer),
                }
            }
        }
        impl serde::Serialize for Prefix<'_> {
            fn serialize<S: serde::Serializer>(
                &self,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                use serde::ser::SerializeSeq;
                let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
                for part in self.0 {
                    let (descending, value) = match part {
                        DirectedValue::Asc(TotalOrdAsc(value)) => (false, value),
                        DirectedValue::Desc(TotalOrdDesc(value)) => (true, value),
                    };
                    seq.serialize_element(&(descending, Canonical(value)))?;
                }
                seq.end()
            }
        }
        // Bincode structs and tuples have the same ordered field encoding.
        let view = (table, columns, Prefix(prefix));
        let mut size = bincode::enc::write::SizeWriter::default();
        bincode::serde::encode_into_writer(&view, &mut size, bincode::config::standard())
            .map_err(|e| Error::Other((format!("bincode size error: {e}")).into()))?;
        let mut bytes = Vec::with_capacity(size.bytes_written);
        bincode::serde::encode_into_std_write(&view, &mut bytes, bincode::config::standard())
            .map_err(|e| Error::Other((format!("bincode encode error: {e}")).into()))?;
        Ok(bytes)
    }

    /// A count is kept per index key, and float keys follow PostgreSQL's
    /// float8 equality, so `-0.0` and `0.0`, and every NaN, share one record.
    pub(super) fn count_key_value(value: Value) -> Value {
        match value {
            Value::Float64(value) => Value::Float64(contextdb_core::float8_canonical(value)),
            value => value,
        }
    }

    /// The stored-key prefix shared by every maintained count of one column
    /// list whose leading values are `prefix`, and by no other record. A count
    /// key is the bincode of `EncodedRelationalIndexCountKey`, which writes the
    /// table, the list, the full prefix's length and then each value in order,
    /// so those form a byte prefix of every such key. Checked against one
    /// encoded full key, so a change to that layout fails here instead of
    /// reading the wrong range.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn relational_index_count_range_prefix(
        table: &str,
        columns: &[(String, contextdb_core::SortDirection)],
        prefix: &[DirectedValue],
    ) -> Result<Vec<u8>> {
        if prefix.len() > columns.len() {
            return Err(Error::Other(
                ("a maintained count prefix is longer than its column list".to_owned()).into(),
            ));
        }
        let mut encoded = Self::encode(&(table, columns))?;
        encoded.extend_from_slice(&Self::encode(&(columns.len() as u64))?);
        for part in prefix {
            let (descending, value) = match part {
                DirectedValue::Asc(TotalOrdAsc(value)) => (false, value),
                DirectedValue::Desc(TotalOrdDesc(value)) => (true, value),
            };
            encoded.extend_from_slice(&Self::encode(&(
                descending,
                Self::count_key_value(value.clone()),
            ))?);
        }
        let mut full = prefix.to_vec();
        full.extend(
            columns[prefix.len()..]
                .iter()
                .map(|(_, direction)| match direction {
                    contextdb_core::SortDirection::Asc => {
                        DirectedValue::Asc(TotalOrdAsc(Value::Null))
                    }
                    contextdb_core::SortDirection::Desc => {
                        DirectedValue::Desc(TotalOrdDesc(Value::Null))
                    }
                }),
        );
        if !Self::relational_index_count_key(table, columns, &full)?.starts_with(&encoded) {
            return Err(Error::Other(
                ("the maintained count key layout no longer begins with its leading values"
                    .to_owned())
                .into(),
            ));
        }
        Ok(encoded)
    }

    pub(super) fn relational_index_count_mutation(
        key: &[u8],
        count: u64,
    ) -> content_identity::ContentIdentityMutation {
        content_identity::status_mutation(
            RELATIONAL_INDEX_COUNT_IDENTITY_NAMESPACE,
            key,
            &count.to_le_bytes(),
        )
    }

    pub(super) fn remove_relational_index_count_mutation(
        key: &[u8],
    ) -> content_identity::ContentIdentityMutation {
        content_identity::remove_status_mutation(RELATIONAL_INDEX_COUNT_IDENTITY_NAMESPACE, key)
    }

    pub(super) fn relational_index_declarations(meta: &TableMeta) -> Vec<IndexDecl> {
        // The shared plan is allocation-free; the caller pre-admits it when
        // constructing declarations under a bounded row owner.
        let (_, capacity, _) = crate::executor::relational_declaration_plan(meta)
            .expect("declaration sizes fit address space");
        let mut declarations = Vec::with_capacity(capacity);
        let mut automatic = crate::executor::auto_indexes_for_table_meta(meta);
        declarations.append(&mut automatic);
        drop(automatic);
        for index in meta
            .indexes
            .iter()
            .filter(|i| i.kind == IndexKind::UserDeclared)
        {
            let mut columns = Vec::with_capacity(index.columns.len());
            for (name, direction) in &index.columns {
                columns.push((name.to_owned(), *direction));
            }
            declarations.push(IndexDecl {
                name: index.name.to_owned(),
                columns,
                kind: index.kind,
            });
        }
        // The explicit permutation is the only sorting allocation. The
        // original ordinal breaks name ties, preserving stable precedence;
        // sort_unstable_by itself uses no heap allocation.
        let mut order = Vec::with_capacity(declarations.len());
        order.extend(0..declarations.len());
        order.sort_unstable_by(|left, right| {
            declarations[*left]
                .name
                .cmp(&declarations[*right].name)
                .then(left.cmp(right))
        });
        for start in 0..order.len() {
            let mut at = start;
            while order[at] != start && order[at] != at {
                let next = order[at];
                declarations.swap(at, next);
                order[at] = at;
                at = next;
            }
            order[at] = at;
        }
        drop(order);
        declarations.dedup_by(|left, right| left.name == right.name);
        declarations
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn relational_indexes_ready_in_read(
        read_txn: &redb::ReadTransaction,
        table_meta: &HashMap<String, TableMeta>,
    ) -> std::result::Result<bool, LoadReadImageError> {
        let ready = match read_txn.open_table(RELATIONAL_INDEX_READY_TABLE) {
            Ok(ready) => ready,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
            Err(error) => return Err(read_table_error(error)),
        };
        for (table, meta) in table_meta {
            let Some(stored) = ready.get(table.as_str()).map_err(read_storage_error)? else {
                return Ok(false);
            };
            if stored.value() != RELATIONAL_INDEX_READY_VERSION {
                return Err(read_image_corruption(
                    ReadImageCorruptionCause::MalformedRecord,
                    format!(
                        "relational index readiness marker for {table} has unsupported version"
                    ),
                ));
            }
            // A marker is useful only when every declared posting table is
            // present in this same Redb image. Empty indexes have no entries
            // to force their table open later, so accepting the marker alone
            // would let a reader claim indexed readiness without a physical
            // source for that declaration.
            for declaration in Self::relational_index_declarations(meta) {
                let name = Self::relational_index_table_name(table, &declaration.name);
                let definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                    TableDefinition::new(name.as_str());
                match read_txn.open_table(definition) {
                    Ok(_) => {}
                    Err(redb::TableError::TableDoesNotExist(_)) => {
                        return Err(read_image_corruption(
                            ReadImageCorruptionCause::MalformedRecord,
                            format!(
                                "relational index readiness marker for {table} names missing posting table {}",
                                declaration.name
                            ),
                        ));
                    }
                    Err(error) => return Err(read_table_error(error)),
                }
                let current_name =
                    Self::current_relational_index_table_name(table, &declaration.name);
                let current_definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                    TableDefinition::new(current_name.as_str());
                match read_txn.open_table(current_definition) {
                    Ok(_) => {}
                    Err(redb::TableError::TableDoesNotExist(_)) => {
                        return Err(read_image_corruption(
                            ReadImageCorruptionCause::MalformedRecord,
                            format!(
                                "relational index readiness for {table} names missing current posting table {}",
                                declaration.name
                            ),
                        ));
                    }
                    Err(error) => return Err(read_table_error(error)),
                }
            }
        }
        match read_txn.open_table(RELATIONAL_INDEX_COUNT_TABLE) {
            Ok(_) => {}
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Err(read_image_corruption(
                    ReadImageCorruptionCause::MalformedRecord,
                    "relational index readiness marker names a missing count table",
                ));
            }
            Err(error) => return Err(read_table_error(error)),
        }
        Ok(true)
    }

    /// Bare persistence flushes have no prepared predecessor snapshot. Their
    /// row phase has already closed the old version, so recover only that
    /// exact predecessor by a reverse point window within this write.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn deleted_index_predecessor_in_write(
        write: &redb::WriteTransaction,
        table: &str,
        meta: &TableMeta,
        row_id: RowId,
        deleted_tx: TxId,
    ) -> Result<Option<VersionedRow>> {
        crate::read_image_memory::reserve_output(
            table
                .len()
                .saturating_add(4)
                .saturating_add(crate::sync_types::held::ALLOCATION_BYTES),
        )?;
        let name = Self::rel_table_name(table);
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(&name);
        let rows = write.open_table(definition).map_err(Self::storage_error)?;
        let lower = row_id.0.to_be_bytes();
        let mut upper = [0; 16];
        upper[..8].copy_from_slice(&lower);
        upper[8..].copy_from_slice(&deleted_tx.0.to_be_bytes());
        let mut range = rows
            .range(lower.as_slice()..upper.as_slice())
            .map_err(Self::storage_error)?;
        let Some((_, body)) = range.next_back().transpose().map_err(Self::storage_error)? else {
            return Ok(None);
        };
        let row = Self::decode_versioned_row(body.value(), Some(meta))?;
        if row.row_id != row_id || row.created_tx >= deleted_tx {
            return Err(crate::funded_error::corruption_text(
                crate::read_image_memory::active_owner()
                    .and_then(|owner| owner.diagnostic_budget()),
                table,
                "index predecessor disagrees with its row-version coordinate".into(),
            ));
        }
        Ok((row.deleted_tx == Some(deleted_tx)).then_some(row))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn flush_relational_index_changes_in_write(
        write_txn: &redb::WriteTransaction,
        ws: &WriteSet,
        table_meta: &HashMap<String, TableMeta>,
        deleted_rows: Option<&HashMap<String, HashMap<RowId, VersionedRow>>>,
        commitments: &mut historical_index_commitment::CommitmentBatch,
    ) -> Result<Vec<content_identity::ContentIdentityMutation>> {
        let inserted = ws
            .relational_inserts
            .iter()
            .map(|(table, row)| ((table.as_str(), row.row_id), row))
            .collect::<HashMap<_, _>>();
        let mut count_changes = BTreeMap::<Vec<u8>, (BTreeSet<RowId>, BTreeSet<RowId>)>::new();
        let mut identity_mutations = Vec::new();
        let affected = ws
            .relational_inserts
            .iter()
            .map(|(table, _)| table)
            .chain(ws.relational_deletes.iter().map(|(table, _, _)| table))
            .collect::<BTreeSet<_>>();
        for table in affected {
            let Some(meta) = table_meta.get(table) else {
                continue;
            };
            let maintain_counts = {
                let ready = write_txn
                    .open_table(RELATIONAL_INDEX_READY_TABLE)
                    .map_err(Self::storage_error)?;
                ready
                    .get(table.as_str())
                    .map_err(Self::storage_error)?
                    .is_some_and(|version| version.value() == RELATIONAL_INDEX_READY_VERSION)
            };
            for declaration in Self::relational_index_declarations(meta) {
                let name = Self::relational_index_table_name(table, &declaration.name);
                let mut index = historical_index_commitment::HistoricalPostings::open(
                    write_txn,
                    table,
                    &declaration,
                )
                .map_err(Self::storage_error)?;
                let current_name =
                    Self::current_relational_index_table_name(table, &declaration.name);
                let current_definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                    TableDefinition::new(current_name.as_str());
                let mut current = write_txn
                    .open_table(current_definition)
                    .map_err(Self::storage_error)?;
                for (deleted_table, row_id, deleted_tx) in &ws.relational_deletes {
                    if deleted_table != table {
                        continue;
                    }
                    // A staged delete can reach commit after another writer
                    // has already deleted the same row. There is then no live
                    // version to take the posting's key from, and no posting
                    // to retire: the delete is the commit no-op it is in
                    // memory, here too.
                    let _fallback_scope = if deleted_rows.is_none() {
                        Some(
                            crate::read_image_memory::active_owner()?
                                .row_owner()?
                                .enter(),
                        )
                    } else {
                        None
                    };
                    let fallback = if deleted_rows.is_none() {
                        Self::deleted_index_predecessor_in_write(
                            write_txn,
                            table,
                            meta,
                            *row_id,
                            *deleted_tx,
                        )?
                    } else {
                        None
                    };
                    let Some(old) = deleted_rows
                        .and_then(|tables| tables.get(table))
                        .and_then(|rows| rows.get(row_id))
                        .or(fallback.as_ref())
                    else {
                        continue;
                    };
                    let old_key =
                        contextdb_relational::index_key_for_row(&declaration.columns, &old.values);
                    let replacement_keeps_key =
                        inserted.get(&(table.as_str(), *row_id)).is_some_and(|new| {
                            (deleted_rows.is_some() || new.created_tx == *deleted_tx)
                                && contextdb_relational::index_key_for_row(
                                    &declaration.columns,
                                    &new.values,
                                ) == old_key
                        });
                    if replacement_keeps_key {
                        continue;
                    }
                    if maintain_counts {
                        for prefix_len in 1..=old_key.len() {
                            // Exactly the declared members, and nothing else:
                            // silence maintains none.
                            if !crate::maintained_counts::maintained_count_member(
                                meta,
                                &declaration.columns[..prefix_len],
                            ) {
                                continue;
                            }
                            let count_key = Self::relational_index_count_key(
                                table,
                                &declaration.columns[..prefix_len],
                                &old_key[..prefix_len],
                            )?;
                            #[cfg(any(test, feature = "test-seams"))]
                            observe_index_prefix_count(
                                table,
                                meta,
                                &declaration,
                                prefix_len,
                                &count_key,
                            );
                            let changes = count_changes.entry(count_key).or_default();
                            changes.1.insert(*row_id);
                        }
                    }
                    // An update that leaves these indexed columns unchanged
                    // keeps the original posting. Its creating transaction
                    // can therefore precede the current row version's.
                    let logical_key =
                        DurableRelationalIndexKey::new(old_key.clone(), old.row_id, TxId(0))?;
                    let created_tx: TxId = current
                        .get(&logical_key)
                        .map_err(Self::storage_error)?
                        .map(|value| Self::decode_exact(value.value()))
                        .transpose()?
                        .ok_or_else(|| {
                            crate::funded_error::corruption_text(
                                crate::read_image_memory::active_owner()
                                    .and_then(|owner| owner.diagnostic_budget()),
                                &current_name,
                                "current index has no posting for a deleted row".into(),
                            )
                        })?;
                    let key = DurableRelationalIndexKey::new(old_key, old.row_id, created_tx)?;
                    let stamp: Option<TxId> = index
                        .get(&key)
                        .map_err(Self::storage_error)?
                        .map(|value| Self::decode_exact(value.value()))
                        .transpose()?
                        .ok_or_else(|| {
                            crate::funded_error::corruption_text(
                                crate::read_image_memory::active_owner()
                                    .and_then(|owner| owner.diagnostic_budget()),
                                &name,
                                "current index names a missing versioned posting".into(),
                            )
                        })?;
                    if stamp.is_some() {
                        return Err(crate::funded_error::corruption_text(
                            crate::read_image_memory::active_owner()
                                .and_then(|owner| owner.diagnostic_budget()),
                            &name,
                            "current index names a deleted posting".into(),
                        ));
                    }
                    current.remove(&logical_key).map_err(Self::storage_error)?;
                    index.put(commitments, write_txn, key, Some(*deleted_tx))?;
                    let logical_key = DurableRelationalIndexKey::new(
                        contextdb_relational::index_key_for_row(&declaration.columns, &old.values),
                        old.row_id,
                        TxId(0),
                    )?;
                    identity_mutations.push(
                        content_identity::remove_relational_index_posting_mutation(
                            table,
                            &declaration.name,
                            &logical_key.0,
                        ),
                    );
                }
                for (inserted_table, row) in &ws.relational_inserts {
                    if inserted_table != table {
                        continue;
                    }
                    let key =
                        contextdb_relational::index_key_for_row(&declaration.columns, &row.values);
                    let fallback_tx = deleted_rows
                        .is_none()
                        .then(|| {
                            ws.relational_deletes.iter().find_map(|(name, id, tx)| {
                                (name == table && *id == row.row_id).then_some(*tx)
                            })
                        })
                        .flatten();
                    let _fallback_scope = if fallback_tx.is_some() {
                        Some(
                            crate::read_image_memory::active_owner()?
                                .row_owner()?
                                .enter(),
                        )
                    } else {
                        None
                    };
                    let fallback = fallback_tx
                        .map(|tx| {
                            Self::deleted_index_predecessor_in_write(
                                write_txn, table, meta, row.row_id, tx,
                            )
                        })
                        .transpose()?
                        .flatten();
                    let replacement_keeps_key = deleted_rows
                        .and_then(|tables| tables.get(table))
                        .and_then(|rows| rows.get(&row.row_id))
                        .or(fallback.as_ref())
                        .is_some_and(|old| {
                            (fallback.is_none() || old.deleted_tx == Some(row.created_tx))
                                && contextdb_relational::index_key_for_row(
                                    &declaration.columns,
                                    &old.values,
                                ) == key
                        });
                    if replacement_keeps_key {
                        continue;
                    }
                    if maintain_counts {
                        for prefix_len in 1..=key.len() {
                            if !crate::maintained_counts::maintained_count_member(
                                meta,
                                &declaration.columns[..prefix_len],
                            ) {
                                continue;
                            }
                            let count_key = Self::relational_index_count_key(
                                table,
                                &declaration.columns[..prefix_len],
                                &key[..prefix_len],
                            )?;
                            #[cfg(any(test, feature = "test-seams"))]
                            observe_index_prefix_count(
                                table,
                                meta,
                                &declaration,
                                prefix_len,
                                &count_key,
                            );
                            let changes = count_changes.entry(count_key).or_default();
                            changes.0.insert(row.row_id);
                        }
                    }
                    let key = DurableRelationalIndexKey::new(key, row.row_id, row.created_tx)?;
                    index.put(commitments, write_txn, key, None)?;
                    let logical_key = DurableRelationalIndexKey::new(
                        contextdb_relational::index_key_for_row(&declaration.columns, &row.values),
                        row.row_id,
                        TxId(0),
                    )?;
                    let created = Self::encode(&row.created_tx)?;
                    current
                        .insert(&logical_key, created.as_slice())
                        .map_err(Self::storage_error)?;
                    #[cfg(any(test, feature = "test-seams"))]
                    observe_index_posting(table, &declaration);
                    identity_mutations.push(content_identity::relational_index_posting_mutation(
                        table,
                        &declaration.name,
                        &logical_key.0,
                    ));
                }
            }
        }
        if !count_changes.is_empty() {
            {
                let mut counts = write_txn
                    .open_table(RELATIONAL_INDEX_COUNT_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, (incremented_rows, decremented_rows)) in count_changes {
                    let increments = u64::try_from(incremented_rows.len()).map_err(|_| {
                        Error::Other(
                            ("relational index count increment overflow".to_owned()).into(),
                        )
                    })?;
                    let decrements = u64::try_from(decremented_rows.len()).map_err(|_| {
                        Error::Other(
                            ("relational index count decrement overflow".to_owned()).into(),
                        )
                    })?;
                    let current = counts
                        .get(key.as_slice())
                        .map_err(Self::storage_error)?
                        .map(|count| count.value())
                        .unwrap_or(0);
                    let next = current
                        .checked_sub(decrements)
                        .and_then(|count| count.checked_add(increments))
                        .ok_or_else(|| Error::StoreCorrupted {
                            path: "relational index count table".into(),
                            reason: "live index-prefix cardinality underflowed or overflowed"
                                .into(),
                        })?;
                    #[cfg(any(test, feature = "test-seams"))]
                    observe_index_prefix_count_value(&key, (next > 0).then_some(next));
                    if next == 0 {
                        counts.remove(key.as_slice()).map_err(Self::storage_error)?;
                        identity_mutations.push(Self::remove_relational_index_count_mutation(&key));
                    } else {
                        counts
                            .insert(key.as_slice(), next)
                            .map_err(Self::storage_error)?;
                        identity_mutations.push(Self::relational_index_count_mutation(&key, next));
                    }
                }
            }
        }
        Ok(identity_mutations)
    }

    /// Take out every durable posting of an index this table has stopped
    /// declaring, in the write that stops declaring it.
    ///
    /// A physical posting table is named for its table and its index NAME
    /// (`relational_index_table_name`), so an index dropped and declared again
    /// under one name shares one table. Leaving the dropped index's postings
    /// behind hands them to whatever is declared next: the rebuild then meets
    /// rows shaped by a declaration that no longer exists, and by then they can
    /// no longer even be retired -- the removals land in the same logical
    /// family as the new declaration's postings, and the canonical key order
    /// refuses to compare a Boolean-keyed posting with an Integer-keyed one.
    /// Here, while the old declaration is still the only one, every posting has
    /// the shape it was written with and comes out cleanly.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn retire_undeclared_relational_indexes_in_write(
        write_txn: &redb::WriteTransaction,
        database_path: &Path,
        table: &str,
        previous_meta: &TableMeta,
        meta: &TableMeta,
        admission: &mut content_identity::ProofAdmission,
    ) -> Result<()> {
        let still_declared = Self::relational_index_declarations(meta)
            .into_iter()
            .map(|declaration| declaration.name)
            .collect::<HashSet<_>>();
        // Declared first, so dropped last: these charges outlive the mutations they pay for.
        let mut mutations_commitment_credits: Vec<historical_index_commitment::CommitmentCredit> =
            Vec::new();
        let mut mutations = Vec::new();
        // A retired index's commitments go with its postings.
        let mut commitments = historical_index_commitment::CommitmentBatch::for_writer()?;
        for declaration in Self::relational_index_declarations(previous_meta) {
            if still_declared.contains(&declaration.name) {
                continue;
            }
            commitments.drop_index(table, &declaration.name)?;
            let name = Self::relational_index_table_name(table, &declaration.name);
            let definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                TableDefinition::new(name.as_str());
            if let Ok(existing) = write_txn.open_table(definition) {
                for entry in existing.iter().map_err(Self::storage_error)? {
                    let decoded = decode_relational_index_entry(
                        database_path,
                        Some(entry.map_err(Self::storage_error)?),
                    )?
                    .expect("an existing index iterator entry decodes");
                    let (_, position, deleted_tx) = decoded;
                    // The declaration being retired is the one that wrote these
                    // postings, so a posting that does not match it IS a damaged
                    // store and is reported as one.
                    Self::validated_index_position(&declaration, previous_meta, &position)
                        .map_err(|reason| {
                            crate::funded_error::corruption_display(
                                admission.diagnostic_budget(),
                                database_path.display(),
                                reason,
                            )
                        })?;
                    if deleted_tx.is_some() {
                        continue;
                    }
                    let logical_key =
                        DurableRelationalIndexKey::new(position.key, position.row_id, TxId(0))?;
                    mutations.push(content_identity::remove_relational_index_posting_mutation(
                        table,
                        &declaration.name,
                        &logical_key.0,
                    ));
                }
            }
            #[cfg(any(test, feature = "test-seams"))]
            let retire = !historical_index_commitment::take_retirement_skip_for_test();
            #[cfg(not(any(test, feature = "test-seams")))]
            let retire = true;
            if retire {
                match write_txn.delete_table(definition) {
                    Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(error) => return Err(Self::storage_error(error)),
                }
            }
            let current_name = Self::current_relational_index_table_name(table, &declaration.name);
            let current_definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                TableDefinition::new(current_name.as_str());
            write_txn
                .delete_table(current_definition)
                .map_err(Self::storage_error)?;
        }
        let (published, credit) = commitments.finish(write_txn)?.into_parts();
        mutations_commitment_credits.push(credit);
        mutations.extend(published);
        if mutations.is_empty() {
            return Ok(());
        }
        Self::apply_content_identity_mutations_in_write(write_txn, &mutations, admission)?;
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn rebuild_relational_indexes_in_write(
        write_txn: &redb::WriteTransaction,
        database_path: &Path,
        table: &str,
        meta: &TableMeta,
        supplied_rows: Option<&[VersionedRow]>,
        admission: &mut content_identity::ProofAdmission,
        shared_commitments: Option<&mut historical_index_commitment::CommitmentBatch>,
    ) -> Result<()> {
        // A rebuild alone in its transaction checks and publishes its own
        // commitments; inside a larger replacement it folds into the caller's.
        let mut own_commitments = match shared_commitments {
            Some(_) => None,
            None => Some(historical_index_commitment::CommitmentBatch::for_writer()?),
        };
        let commitments = match shared_commitments {
            Some(shared) => shared,
            None => own_commitments
                .as_mut()
                .expect("a rebuild alone owns its batch"),
        };
        // Declared first, so dropped last: these charges outlive the mutations they pay for.
        let mut count_mutations_commitment_credits: Vec<
            historical_index_commitment::CommitmentCredit,
        > = Vec::new();
        let mut count_mutations = Vec::new();
        let mut rebuilt_counts = BTreeMap::<Vec<u8>, BTreeSet<RowId>>::new();
        {
            let mut counts = write_txn
                .open_table(RELATIONAL_INDEX_COUNT_TABLE)
                .map_err(Self::storage_error)?;
            let mut retired = Vec::new();
            for entry in counts.iter().map_err(Self::storage_error)? {
                let (key, _) = entry.map_err(Self::storage_error)?;
                let key = key.value();
                let decoded: EncodedRelationalIndexCountKey = Self::decode(key)?;
                if decoded.table == table {
                    retired.push(key.to_vec());
                }
            }
            for key in retired {
                counts.remove(key.as_slice()).map_err(Self::storage_error)?;
                count_mutations.push(Self::remove_relational_index_count_mutation(&key));
            }
        }
        for declaration in Self::relational_index_declarations(meta) {
            let name = Self::relational_index_table_name(table, &declaration.name);
            let definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                TableDefinition::new(name.as_str());
            if let Ok(existing) = write_txn.open_table(definition) {
                for entry in existing.iter().map_err(Self::storage_error)? {
                    let decoded = decode_relational_index_entry(
                        database_path,
                        Some(entry.map_err(Self::storage_error)?),
                    )?
                    .expect("an existing index iterator entry decodes");
                    let (_, position, deleted_tx) = decoded;
                    Self::validated_index_position(&declaration, meta, &position).map_err(
                        |reason| {
                            crate::funded_error::corruption_display(
                                admission.diagnostic_budget(),
                                database_path.display(),
                                reason,
                            )
                        },
                    )?;
                    if deleted_tx.is_some() {
                        continue;
                    }
                    let logical_key =
                        DurableRelationalIndexKey::new(position.key, position.row_id, TxId(0))?;
                    count_mutations.push(
                        content_identity::remove_relational_index_posting_mutation(
                            table,
                            &declaration.name,
                            &logical_key.0,
                        ),
                    );
                }
            }
            match write_txn.delete_table(definition) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(error) => return Err(Self::storage_error(error)),
            }
            let current_name = Self::current_relational_index_table_name(table, &declaration.name);
            let current_definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                TableDefinition::new(current_name.as_str());
            write_txn
                .delete_table(current_definition)
                .map_err(Self::storage_error)?;
            // A rebuild derives both commitments of this index from the rows
            // it walks, which are the authority for what the postings cover.
            commitments.reset(
                write_txn,
                table,
                &declaration,
                historical_index_commitment::Side::Postings,
            )?;
            commitments.reset(
                write_txn,
                table,
                &declaration,
                historical_index_commitment::Side::Versions,
            )?;
            let mut index = historical_index_commitment::HistoricalPostings::open(
                write_txn,
                table,
                &declaration,
            )
            .map_err(Self::storage_error)?;
            let mut active = HashMap::<RowId, (IndexKey, TxId, Option<TxId>)>::new();
            let mut write_row = |row: &VersionedRow| -> Result<()> {
                let key =
                    contextdb_relational::index_key_for_row(&declaration.columns, &row.values);
                commitments.fold_interval(
                    write_txn,
                    table,
                    &declaration,
                    historical_index_commitment::Side::Versions,
                    &historical_index_commitment::key_row(&key, row.row_id)?,
                    row.created_tx,
                    row.deleted_tx,
                    true,
                )?;
                // A run continues only while the key is unchanged and the
                // versions are contiguous; a gap starts a new interval.
                let (created_tx, durable_key) = match active.get(&row.row_id) {
                    Some((prior, created_tx, end))
                        if prior == &key && *end == Some(row.created_tx) =>
                    {
                        (
                            *created_tx,
                            DurableRelationalIndexKey::new(key.clone(), row.row_id, *created_tx)?,
                        )
                    }
                    _ => (
                        row.created_tx,
                        DurableRelationalIndexKey::new(key.clone(), row.row_id, row.created_tx)?,
                    ),
                };
                index.put(commitments, write_txn, durable_key, row.deleted_tx)?;
                active.insert(row.row_id, (key, created_tx, row.deleted_tx));
                Ok(())
            };
            if let Some(rows) = supplied_rows {
                // Match the durable relation's key order without copying row
                // bodies. Callers already own these rows; this pointer-only
                // ordering keeps version publication identical to a Redb walk.
                let mut ordered = rows.iter().collect::<Vec<_>>();
                ordered.sort_unstable_by_key(|row| (row.row_id, row.created_tx, row.lsn));
                for row in ordered {
                    write_row(row)?;
                }
            } else {
                let relation_name = Self::rel_table_name(table);
                let relation_definition: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(relation_name.as_str());
                match write_txn.open_table(relation_definition) {
                    Ok(relation) => {
                        for entry in relation.iter().map_err(Self::storage_error)? {
                            let (_, value) = entry.map_err(Self::storage_error)?;
                            #[cfg(any(test, feature = "test-seams"))]
                            RELATIONAL_INDEX_REBUILD_ROWS_DECODED
                                .with(|count| count.set(count.get().saturating_add(1)));
                            let _row_scope = crate::read_image_memory::active_owner()?
                                .row_owner()?
                                .enter();
                            let row = Self::decode_versioned_row(value.value(), Some(meta))?;
                            write_row(&row)?;
                        }
                    }
                    Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(error) => return Err(Self::storage_error(error)),
                }
            }
            drop(write_row);
            drop(index);
            let mut current = write_txn
                .open_table(current_definition)
                .map_err(Self::storage_error)?;
            for (row_id, (key, created_tx, end)) in active {
                if end.is_some() {
                    continue;
                }
                let logical_key = DurableRelationalIndexKey::new(key.clone(), row_id, TxId(0))?;
                let created = Self::encode(&created_tx)?;
                current
                    .insert(&logical_key, created.as_slice())
                    .map_err(Self::storage_error)?;
                count_mutations.push(content_identity::relational_index_posting_mutation(
                    table,
                    &declaration.name,
                    &logical_key.0,
                ));
                for prefix_len in 1..=key.len() {
                    // The rebuild writes exactly the declared members, so a
                    // recompute after a declaration change lands the same set
                    // an append would have written.
                    if !crate::maintained_counts::maintained_count_member(
                        meta,
                        &declaration.columns[..prefix_len],
                    ) {
                        continue;
                    }
                    let count_key = Self::relational_index_count_key(
                        table,
                        &declaration.columns[..prefix_len],
                        &key[..prefix_len],
                    )?;
                    #[cfg(any(test, feature = "test-seams"))]
                    observe_index_prefix_count(table, meta, &declaration, prefix_len, &count_key);
                    rebuilt_counts.entry(count_key).or_default().insert(row_id);
                }
            }
        }
        {
            let mut counts = write_txn
                .open_table(RELATIONAL_INDEX_COUNT_TABLE)
                .map_err(Self::storage_error)?;
            for (key, rows) in rebuilt_counts {
                let count = u64::try_from(rows.len()).map_err(|_| {
                    Error::Other(("relational index count rebuild overflow".to_owned()).into())
                })?;
                counts
                    .insert(key.as_slice(), count)
                    .map_err(Self::storage_error)?;
                count_mutations.push(Self::relational_index_count_mutation(&key, count));
            }
        }
        write_txn
            .open_table(RELATIONAL_INDEX_READY_TABLE)
            .map_err(Self::storage_error)?
            .insert(table, RELATIONAL_INDEX_READY_VERSION)
            .map_err(Self::storage_error)?;
        count_mutations.push(content_identity::status_mutation(
            RELATIONAL_INDEX_COUNT_READY_IDENTITY_NAMESPACE,
            table.as_bytes(),
            RELATIONAL_INDEX_READY_VERSION,
        ));
        if let Some(own) = own_commitments {
            let (published, credit) = own.finish(write_txn)?.into_parts();
            count_mutations_commitment_credits.push(credit);
            count_mutations.extend(published);
        }
        Self::apply_content_identity_mutations_in_write(write_txn, &count_mutations, admission)?;
        Ok(())
    }

    pub(super) fn sink_queue_table_name(sink: &str) -> String {
        format!("__sink_queue_{sink}")
    }

    pub(super) fn sink_audit_key(lsn: Lsn, index: usize, sink: &str) -> String {
        format!("{:020}:{index:06}:{sink}", lsn.0)
    }

    pub(super) fn trigger_audit_key(index: u64, trigger: &str) -> String {
        format!("{index:020}:{trigger}")
    }

    pub(super) fn rel_row_key(row: &VersionedRow) -> Vec<u8> {
        Self::rel_row_key_from_parts(row.row_id, row.created_tx, row.lsn)
    }

    /// The same key `rel_row_key` derives from a `VersionedRow`, built from
    /// the raw identity parts directly — for a scoped prune, which has the
    /// pruned version's `(row_id, created_tx, lsn)` in hand without needing
    /// the whole row.
    pub(super) fn rel_row_key_from_parts(row_id: RowId, created_tx: TxId, lsn: Lsn) -> Vec<u8> {
        let mut key = Vec::with_capacity(24);
        key.extend_from_slice(&row_id.0.to_be_bytes());
        key.extend_from_slice(&created_tx.0.to_be_bytes());
        key.extend_from_slice(&lsn.0.to_be_bytes());
        key
    }

    pub(super) fn sync_row_source_lsn_key(table: &str, row_id: RowId) -> Vec<u8> {
        let mut key = Vec::with_capacity(table.len() + 8);
        key.extend_from_slice(table.as_bytes());
        key.extend_from_slice(&row_id.0.to_be_bytes());
        key
    }

    pub(super) fn decode_sync_row_source_lsn_key(key: &[u8]) -> Option<(String, RowId)> {
        let split = key.len().checked_sub(8)?;
        let (table, row_id) = key.split_at(split);
        let table = String::from_utf8(table.to_vec()).ok()?;
        let row_id = RowId(u64::from_be_bytes(row_id.try_into().ok()?));
        Some((table, row_id))
    }

    /// Retire one table's source metadata without a source-sized key list.
    /// The two physical sidecars and their logical identity leave together.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn remove_table_source_provenance_in_write(
        &self,
        write: &redb::WriteTransaction,
        table: &str,
    ) -> Result<()> {
        self.retire_table_source_provenance_in_write(write, table, true, true, None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn retire_table_source_provenance_in_write(
        &self,
        write: &redb::WriteTransaction,
        table: &str,
        erase_records: bool,
        erase_identity: bool,
        mut retired: Option<&mut schema_projection::ReceivedRetirementLog>,
    ) -> Result<()> {
        let mut cursor_credit = self.proof_admission();
        let mut cursor_bytes = 0usize;
        macro_rules! erase_sidecar {
            ($definition:expr) => {{
                let mut after: Option<Vec<u8>> = None;
                loop {
                    let mut window = self.proof_admission();
                    let selected = {
                        let source = write.open_table($definition).map_err(Self::storage_error)?;
                        let lower = after.as_deref().map_or(
                            std::ops::Bound::Included(table.as_bytes()),
                            std::ops::Bound::Excluded,
                        );
                        let mut cursor = source
                            .range::<&[u8]>((lower, std::ops::Bound::Unbounded))
                            .map_err(Self::storage_error)?;
                        cursor
                            .next()
                            .transpose()
                            .map_err(Self::storage_error)?
                            .filter(|(key, _)| key.value().starts_with(table.as_bytes()))
                            .map(|(key, _)| {
                                window.admit(
                                    key.value().len().saturating_mul(4).saturating_add(256),
                                )?;
                                Ok::<_, Error>(key.value().to_vec())
                            })
                            .transpose()?
                    };
                    let Some(key) = selected else {
                        break;
                    };
                    let required = key.len().saturating_mul(2).saturating_add(128);
                    if required > cursor_bytes {
                        cursor_credit.admit(required - cursor_bytes)?;
                        cursor_bytes = required;
                    }
                    // A longer table name can share the byte prefix; its
                    // self-described key width keeps that neighbor untouched.
                    if key.len() == table.len().saturating_add(8) {
                        let row_id = RowId(u64::from_be_bytes(
                            key[table.len()..]
                                .try_into()
                                .expect("source row key suffix"),
                        ));
                        if erase_records {
                            let mut source =
                                write.open_table($definition).map_err(Self::storage_error)?;
                            source.remove(key.as_slice()).map_err(Self::storage_error)?;
                        }
                        if erase_identity {
                            let mutations =
                                [content_identity::remove_sync_source_mutation(table, row_id)];
                            if let Some(retired) = retired.as_deref_mut() {
                                retired.append(self, write, &mutations)?;
                            } else {
                                Self::apply_content_identity_mutations_in_write(
                                    write,
                                    &mutations,
                                    &mut self.proof_admission(),
                                )?;
                            }
                        }
                    }
                    after = Some(key);
                }
            }};
        }
        erase_sidecar!(SYNC_ROW_SOURCE_LSN_TABLE);
        erase_sidecar!(SYNC_ROW_SOURCE_KIND_TABLE);
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn retire_table_source_provenance_in_write_original(
        &self,
        original: &OriginalPersistenceFrame<'_>,
        write: &redb::WriteTransaction,
        table: &str,
        erase_records: bool,
        erase_identity: bool,
        mut retired: Option<&mut schema_projection::OriginalMutationLog<'_, '_, '_>>,
    ) -> Result<()> {
        let cursor_credit = OriginalPersistenceFrame::new(original.original)?;
        let mut cursor_bytes = 0usize;
        macro_rules! erase_sidecar {
            ($definition:expr) => {{
                let mut after: Option<Vec<u8>> = None;
                loop {
                    let window = OriginalPersistenceFrame::new(original.original)?;
                    let selected = {
                        let source = write.open_table($definition).map_err(Self::storage_error)?;
                        let lower = after.as_deref().map_or(
                            std::ops::Bound::Included(table.as_bytes()),
                            std::ops::Bound::Excluded,
                        );
                        let mut cursor = source
                            .range::<&[u8]>((lower, std::ops::Bound::Unbounded))
                            .map_err(Self::storage_error)?;
                        cursor
                            .next()
                            .transpose()
                            .map_err(Self::storage_error)?
                            .filter(|(key, _)| key.value().starts_with(table.as_bytes()))
                            .map(|(key, _)| {
                                window.reserve(
                                    key.value().len().saturating_mul(4).saturating_add(256),
                                )?;
                                Ok::<_, Error>(key.value().to_vec())
                            })
                            .transpose()?
                    };
                    let Some(key) = selected else {
                        break;
                    };
                    let required = key.len().saturating_mul(2).saturating_add(128);
                    if required > cursor_bytes {
                        cursor_credit.reserve(required - cursor_bytes)?;
                        cursor_bytes = required;
                    }
                    // A longer table name can share the byte prefix; its
                    // self-described key width keeps that neighbor untouched.
                    if key.len() == table.len().saturating_add(8) {
                        let row_id = RowId(u64::from_be_bytes(
                            key[table.len()..]
                                .try_into()
                                .expect("source row key suffix"),
                        ));
                        if erase_records {
                            let mut source =
                                write.open_table($definition).map_err(Self::storage_error)?;
                            source.remove(key.as_slice()).map_err(Self::storage_error)?;
                        }
                        if erase_identity {
                            let mutations = [
                                content_identity::original_producers::remove_sync_source_mutation(
                                    &window, table, row_id,
                                )?,
                            ];
                            if let Some(retired) = retired.as_deref_mut() {
                                retired.extend(mutations)?;
                            } else {
                                Self::apply_owned_content_identity_with_manifests_in_write(
                                    write,
                                    &mutations,
                                    &[],
                                    &mut content_identity::original_writer::ProofAdmission::new(
                                        original.original,
                                        self.proof_scratch_owner(),
                                    )?,
                                )?;
                            }
                        }
                    }
                    after = Some(key);
                }
            }};
        }
        erase_sidecar!(SYNC_ROW_SOURCE_LSN_TABLE);
        erase_sidecar!(SYNC_ROW_SOURCE_KIND_TABLE);
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn remove_table_retention_expiries_in_write(
        &self,
        write: &redb::WriteTransaction,
        table: &str,
    ) -> Result<()> {
        self.retire_table_retention_expiries_in_write(write, table, None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn retire_table_retention_expiries_in_write(
        &self,
        write: &redb::WriteTransaction,
        table: &str,
        mut retired: Option<&mut schema_projection::ReceivedRetirementLog>,
    ) -> Result<()> {
        let mut cursor_credit = self.proof_admission();
        cursor_credit.admit(table.len().saturating_mul(4).saturating_add(512))?;
        let prefix = format!(
            "{}{}:{table}:",
            crate::composite_store::RETENTION_EXPIRY_PREFIX,
            table.len()
        );
        let mut after: Option<String> = None;
        let mut cursor_bytes = 0usize;
        loop {
            let mut window = self.proof_admission();
            let key = {
                let source = write
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                let lower = after.as_deref().map_or(
                    std::ops::Bound::Included(prefix.as_str()),
                    std::ops::Bound::Excluded,
                );
                let mut cursor = source
                    .range::<&str>((lower, std::ops::Bound::Unbounded))
                    .map_err(Self::storage_error)?;
                cursor
                    .next()
                    .transpose()
                    .map_err(Self::storage_error)?
                    .filter(|(key, _)| key.value().starts_with(&prefix))
                    .map(|(key, _)| {
                        window.admit(key.value().len().saturating_mul(4).saturating_add(256))?;
                        Ok::<_, Error>(key.value().to_owned())
                    })
                    .transpose()?
            };
            let Some(key) = key else {
                break;
            };
            let required = key.len().saturating_mul(2).saturating_add(128);
            if required > cursor_bytes {
                cursor_credit.admit(required - cursor_bytes)?;
                cursor_bytes = required;
            }
            {
                let mut source = write
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                source.remove(key.as_str()).map_err(Self::storage_error)?;
            }
            let mutations = content_identity::config_mutations(&key, None);
            if let Some(retired) = retired.as_deref_mut() {
                retired.append(self, write, &mutations)?;
            } else {
                Self::apply_content_identity_mutations_in_write(
                    write,
                    &mutations,
                    &mut self.proof_admission(),
                )?;
            }
            after = Some(key);
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn retain_sync_source_provenance_for_table_rows(
        write_txn: &redb::WriteTransaction,
        table: &str,
        rows: &[VersionedRow],
    ) -> Result<()> {
        let live_row_ids = rows
            .iter()
            .filter(|row| row.deleted_tx.is_none())
            .map(|row| row.row_id)
            .collect::<HashSet<_>>();
        let mut source_lsn_table = write_txn
            .open_table(SYNC_ROW_SOURCE_LSN_TABLE)
            .map_err(Self::storage_error)?;
        let lsn_keys_to_remove =
            Self::sync_source_lsn_keys_for_table(&source_lsn_table, table, |row_id| {
                !live_row_ids.contains(&row_id)
            })?;
        let mut source_kind_table = write_txn
            .open_table(SYNC_ROW_SOURCE_KIND_TABLE)
            .map_err(Self::storage_error)?;
        let kind_keys_to_remove =
            Self::sync_source_kind_keys_for_table(&source_kind_table, table, |row_id| {
                !live_row_ids.contains(&row_id)
            })?;
        for key in lsn_keys_to_remove {
            source_lsn_table
                .remove(key.as_slice())
                .map_err(Self::storage_error)?;
        }
        for key in kind_keys_to_remove {
            source_kind_table
                .remove(key.as_slice())
                .map_err(Self::storage_error)?;
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn sync_source_lsn_keys_for_table(
        source_lsn_table: &impl ReadableTable<&'static [u8], u64>,
        table: &str,
        should_remove: impl Fn(RowId) -> bool,
    ) -> Result<Vec<Vec<u8>>> {
        let mut keys = Vec::new();
        for entry in source_lsn_table.iter().map_err(Self::storage_error)? {
            let (key, _) = entry.map_err(Self::storage_error)?;
            if let Some((entry_table, row_id)) = Self::decode_sync_row_source_lsn_key(key.value())
                && entry_table == table
                && should_remove(row_id)
            {
                keys.push(key.value().to_vec());
            }
        }
        Ok(keys)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn sync_source_kind_keys_for_table(
        source_kind_table: &impl ReadableTable<&'static [u8], u8>,
        table: &str,
        should_remove: impl Fn(RowId) -> bool,
    ) -> Result<Vec<Vec<u8>>> {
        let mut keys = Vec::new();
        for entry in source_kind_table.iter().map_err(Self::storage_error)? {
            let (key, _) = entry.map_err(Self::storage_error)?;
            if let Some((entry_table, row_id)) = Self::decode_sync_row_source_lsn_key(key.value())
                && entry_table == table
                && should_remove(row_id)
            {
                keys.push(key.value().to_vec());
            }
        }
        Ok(keys)
    }

    pub(super) fn rel_row_key_range(row_id: RowId) -> ([u8; 8], [u8; 8]) {
        let lower = row_id.0.to_be_bytes();
        let upper = row_id.0.saturating_add(1).to_be_bytes();
        (lower, upper)
    }

    pub(super) fn meta_key(name: &str) -> String {
        format!("table:{name}")
    }

    pub(super) fn change_log_entry_key_len() -> usize {
        20 + 1 + 6
    }

    pub(super) fn write_change_log_entry_key(lsn: Lsn, index: usize, key: &mut String) {
        key.clear();
        write!(key, "{:020}:{index:06}", lsn.0).expect("writing change-log key to String");
    }

    pub(super) fn ddl_log_key(lsn: Lsn) -> String {
        format!("{:020}", lsn.0)
    }

    pub(super) fn ddl_log_key_for_index(lsn: Lsn, index: usize, count: usize) -> String {
        if count <= 1 {
            Self::ddl_log_key(lsn)
        } else {
            format!("{:020}:{index:06}", lsn.0)
        }
    }

    pub(super) fn graph_fwd_key(entry: &AdjEntry) -> Vec<u8> {
        let mut key = Vec::with_capacity(48 + entry.edge_type.len());
        key.extend_from_slice(entry.source.as_bytes());
        key.extend_from_slice(entry.target.as_bytes());
        key.extend_from_slice(entry.edge_type.as_bytes());
        key.push(0);
        key.extend_from_slice(&entry.created_tx.0.to_be_bytes());
        key.extend_from_slice(&entry.lsn.0.to_be_bytes());
        key
    }

    pub(super) fn graph_rev_key(entry: &AdjEntry) -> Vec<u8> {
        let mut key = Vec::with_capacity(48 + entry.edge_type.len());
        key.extend_from_slice(entry.target.as_bytes());
        key.extend_from_slice(entry.source.as_bytes());
        key.extend_from_slice(entry.edge_type.as_bytes());
        key.push(0);
        key.extend_from_slice(&entry.created_tx.0.to_be_bytes());
        key.extend_from_slice(&entry.lsn.0.to_be_bytes());
        key
    }

    pub(super) fn vector_key(entry: &VectorEntry) -> Vec<u8> {
        Self::vector_identity_key(&entry.index, entry.row_id, entry.created_tx, entry.lsn)
    }

    pub(super) fn vector_identity_key(
        index: &VectorIndexRef,
        row_id: RowId,
        created_tx: TxId,
        lsn: Lsn,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(index.table.len() + index.column.len() + 34);
        key.extend_from_slice(index.table.as_bytes());
        key.push(0);
        key.extend_from_slice(index.column.as_bytes());
        key.push(0);
        key.extend_from_slice(&row_id.0.to_be_bytes());
        key.extend_from_slice(&created_tx.0.to_be_bytes());
        key.extend_from_slice(&lsn.0.to_be_bytes());
        key
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn current_row_identity_mutation_in_write(
        write_txn: &redb::WriteTransaction,
        table_name: &str,
        row_id: RowId,
        meta: Option<&TableMeta>,
        mut workspace: Option<&mut crate::memory_accounting::OwnedMemoryReservation>,
    ) -> Result<content_identity::ContentIdentityMutation> {
        let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
        let redb_name = Self::rel_table_name(table_name);
        let table_def: TableDefinition<&[u8], &[u8]> = TableDefinition::new(redb_name.as_str());
        let table = match content_size::open_rows(write_txn, table_def, meta) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Ok(content_identity::remove_row_mutation(table_name, row_id));
            }
            Err(error) => return Err(Self::storage_error(error)),
        };
        let prefix = row_id.0.to_be_bytes();
        let memory = crate::read_image_memory::active_owner()?;
        let mut current = None::<(VersionedRow, crate::read_image_memory::ImageMemoryHandle)>;
        let mut admitted_body_bytes = 0usize;
        for item in table
            .range(prefix.as_slice()..)
            .map_err(Self::storage_error)?
        {
            let (key, value) = item.map_err(Self::storage_error)?;
            if !key.value().starts_with(&prefix) {
                break;
            }
            // Committed directories can omit large unindexed TEXT/JSON.
            // Price the borrowed durable encoding before decoding it. The
            // envelope covers the previous winner, replacement decode and
            // canonical leaf; the caller holds it through identity publication.
            if let Some(workspace) = workspace.as_deref_mut() {
                let required = 1024usize
                    .saturating_add(table_name.len().saturating_mul(8))
                    .saturating_add(value.value().len().saturating_mul(16));
                if required > admitted_body_bytes {
                    workspace.try_grow_for(
                        required - admitted_body_bytes,
                        "retention",
                        "prepare_prune",
                        "Raise MEMORY_LIMIT to repair the selected retained row's durable identity.",
                    )?;
                    admitted_body_bytes = required;
                }
            }
            let row_memory = memory.row_owner()?;
            let row = row_memory.scope(|| Self::decode_versioned_row(value.value(), meta))?;
            if current
                .as_ref()
                .is_none_or(|(seen, _)| (row.lsn, row.created_tx) > (seen.lsn, seen.created_tx))
            {
                current = Some((row, row_memory));
            }
        }
        match current {
            Some(row) => content_identity::row_mutation(table_name, &row.0, meta),
            None => Ok(content_identity::remove_row_mutation(table_name, row_id)),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn current_vector_identity_mutation_in_write(
        write_txn: &redb::WriteTransaction,
        index: &VectorIndexRef,
        row_id: RowId,
        quantization: VectorQuantization,
    ) -> Result<content_identity::ContentIdentityMutation> {
        let table = match content_size::open_vectors(write_txn) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Ok(content_identity::remove_vector_mutation(
                    index,
                    row_id,
                    quantization,
                ));
            }
            Err(error) => return Err(Self::storage_error(error)),
        };
        let mut prefix = Vec::with_capacity(index.table.len() + index.column.len() + 18);
        prefix.extend_from_slice(index.table.as_bytes());
        prefix.push(0);
        prefix.extend_from_slice(index.column.as_bytes());
        prefix.push(0);
        prefix.extend_from_slice(&row_id.0.to_be_bytes());
        let mut current = None;
        for item in table
            .range(prefix.as_slice()..)
            .map_err(Self::storage_error)?
        {
            let (key, value) = item.map_err(Self::storage_error)?;
            if !key.value().starts_with(&prefix) {
                break;
            }
            let entry = Self::decode_vector_entry(value.value())?;
            if entry.deleted_tx.is_none()
                && current.as_ref().is_none_or(|seen: &VectorEntry| {
                    (entry.lsn, entry.created_tx) > (seen.lsn, seen.created_tx)
                })
            {
                current = Some(entry);
            }
        }
        Ok(match current {
            Some(entry) => content_identity::decoded_vector_mutation(&entry, quantization),
            None => content_identity::remove_vector_mutation(index, row_id, quantization),
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn current_edge_identity_mutation_in_write(
        write_txn: &redb::WriteTransaction,
        source: NodeId,
        edge_type: &str,
        target: NodeId,
    ) -> Result<content_identity::ContentIdentityMutation> {
        let table = match write_txn.open_table(GRAPH_FWD_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Ok(content_identity::remove_edge_mutation(
                    source, edge_type, target,
                ));
            }
            Err(error) => return Err(Self::storage_error(error)),
        };
        let mut prefix = Vec::with_capacity(33 + edge_type.len());
        prefix.extend_from_slice(source.as_bytes());
        prefix.extend_from_slice(target.as_bytes());
        prefix.extend_from_slice(edge_type.as_bytes());
        prefix.push(0);
        let mut current = None;
        for item in table
            .range(prefix.as_slice()..)
            .map_err(Self::storage_error)?
            .inspect(|_entry| {
                #[cfg(any(test, feature = "test-seams"))]
                if let Ok((key, _)) = _entry {
                    crate::read_work_probe::graph_storage_key(
                        "graph_fwd",
                        "iteration_step",
                        key.value(),
                    );
                }
            })
        {
            let (key, value) = item.map_err(Self::storage_error)?;
            if !key.value().starts_with(&prefix) {
                break;
            }
            #[cfg(any(test, feature = "test-seams"))]
            crate::read_work_probe::note_graph(
                "graph_payload",
                "payload_decode",
                key.value(),
                None,
                Some("current_edge_identity"),
            );
            let edge = Self::decode::<AdjEntry>(value.value())?;
            if edge.deleted_tx.is_none()
                && current.as_ref().is_none_or(|seen: &AdjEntry| {
                    (edge.lsn, edge.created_tx) > (seen.lsn, seen.created_tx)
                })
            {
                current = Some(edge);
            }
        }
        match current {
            Some(edge) => content_identity::edge_mutation(&edge),
            None => Ok(content_identity::remove_edge_mutation(
                source, edge_type, target,
            )),
        }
    }

    pub(super) fn append_partition_key_component(key: &mut Vec<u8>, bytes: &[u8]) {
        key.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
        key.extend_from_slice(bytes);
    }

    pub(super) fn vector_partition_prefix_with_suffix_capacity(
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        suffix_bytes: usize,
    ) -> Vec<u8> {
        let canonical_partition = partition_key.canonical_bytes();
        let mut key = Vec::with_capacity(
            index.table.len() + index.column.len() + canonical_partition.len() + 24 + suffix_bytes,
        );
        Self::append_partition_key_component(&mut key, index.table.as_bytes());
        Self::append_partition_key_component(&mut key, index.column.as_bytes());
        Self::append_partition_key_component(&mut key, &canonical_partition);
        key
    }

    pub(crate) fn vector_partition_prefix(
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
    ) -> Vec<u8> {
        Self::vector_partition_prefix_with_suffix_capacity(index, partition_key, 0)
    }

    pub(super) fn vector_partition_membership_key(
        record: &VectorPartitionMembershipRecord,
    ) -> Vec<u8> {
        let mut key = Self::vector_partition_prefix_with_suffix_capacity(
            &record.index,
            &record.partition_key,
            40,
        );
        key.extend_from_slice(&record.row_id.0.to_be_bytes());
        key.extend_from_slice(&record.row_created_tx.0.to_be_bytes());
        key.extend_from_slice(&record.row_lsn.0.to_be_bytes());
        key.extend_from_slice(&record.vector_created_tx.0.to_be_bytes());
        key.extend_from_slice(&record.vector_lsn.0.to_be_bytes());
        key
    }

    pub(super) fn vector_partition_membership_row_prefix(
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        row_id: RowId,
    ) -> Vec<u8> {
        let mut key = Self::vector_partition_prefix_with_suffix_capacity(index, partition_key, 8);
        key.extend_from_slice(&row_id.0.to_be_bytes());
        key
    }

    pub(super) fn vector_partition_version_identity_key(
        identity: &VectorPartitionVersionIdentity,
    ) -> Vec<u8> {
        let mut key = Self::vector_partition_prefix(&identity.index, &identity.partition_key);
        key.extend_from_slice(&identity.row_id.0.to_be_bytes());
        key.extend_from_slice(&identity.row_created_tx.0.to_be_bytes());
        key.extend_from_slice(&identity.row_lsn.0.to_be_bytes());
        key.extend_from_slice(&identity.vector_created_tx.0.to_be_bytes());
        key.extend_from_slice(&identity.vector_lsn.0.to_be_bytes());
        key
    }

    pub(super) fn vector_partition_tombstone_key(
        record: &VectorPartitionTombstoneRecord,
    ) -> Vec<u8> {
        let mut key = Self::vector_partition_membership_key(&record.membership);
        key.extend_from_slice(&record.deleted_tx.0.to_be_bytes());
        key
    }

    pub(super) fn vector_partition_journal_key(record: &VectorPartitionJournalRecord) -> Vec<u8> {
        let mut key = Self::vector_partition_prefix(&record.index, &record.partition_key);
        key.extend_from_slice(&record.lsn.0.to_be_bytes());
        key.extend_from_slice(&record.ordinal.to_be_bytes());
        key
    }

    pub(super) fn vector_partition_catalog_key_from_parts(
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
    ) -> Vec<u8> {
        Self::vector_partition_prefix(index, partition_key)
    }

    pub(super) fn vector_partition_catalog_key(
        record: &VectorPartitionGenerationCatalogRecord,
    ) -> Vec<u8> {
        Self::vector_partition_catalog_key_from_parts(&record.index, &record.partition_key)
    }

    pub(super) fn vector_partition_base_generation_key_from_parts(
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        generation_id: u64,
    ) -> Vec<u8> {
        let mut key = Self::vector_partition_prefix_with_suffix_capacity(index, partition_key, 8);
        key.extend_from_slice(&generation_id.to_be_bytes());
        key
    }

    pub(super) fn vector_partition_base_generation_key(
        record: &VectorPartitionBaseGenerationRecord,
    ) -> Vec<u8> {
        Self::vector_partition_base_generation_key_from_parts(
            &record.index,
            &record.partition_key,
            record.generation_id,
        )
    }

    pub(super) fn vector_partition_change_generation_key_from_parts(
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        base_generation_id: u64,
        change_generation_id: u64,
    ) -> Vec<u8> {
        let mut key = Self::vector_partition_prefix_with_suffix_capacity(index, partition_key, 16);
        key.extend_from_slice(&base_generation_id.to_be_bytes());
        key.extend_from_slice(&change_generation_id.to_be_bytes());
        key
    }

    pub(super) fn vector_partition_change_generation_key(
        record: &VectorPartitionChangeGenerationRecord,
    ) -> Vec<u8> {
        Self::vector_partition_change_generation_key_from_parts(
            &record.index,
            &record.partition_key,
            record.base_generation_id,
            record.change_generation_id,
        )
    }

    pub(super) fn vector_partition_graph_page_root_key(
        generation_key: &[u8],
        is_base: bool,
    ) -> Vec<u8> {
        let route_marker = if is_base { 0xb0 } else { 0xc0 };
        let identity_bytes = generation_key
            .len()
            .saturating_add(1)
            .saturating_add(VECTOR_PARTITION_GRAPH_PAGE_KEY_SUFFIX_BYTES)
            .saturating_add(VECTOR_PARTITION_GRAPH_PAGE_HEADER_BYTES)
            .saturating_add(VECTOR_PARTITION_GRAPH_PAGE_LEAF_FRAME_BYTES);
        if identity_bytes < VECTOR_PARTITION_GRAPH_PAGE_RECORD_BYTES {
            let mut key = Vec::with_capacity(generation_key.len().saturating_add(1));
            key.extend_from_slice(generation_key);
            key.push(route_marker);
            return key;
        }

        // Partition values are not bounded by the graph page size. Keep the
        // existing physical identity for every generation that previously
        // fit, and compact only identities that could never have been
        // published in the paged format.
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"contextdb-vector-graph-page-root-v1");
        hasher.update(&[route_marker]);
        hasher.update(generation_key);
        let digest = hasher.finalize();
        let mut key = Vec::with_capacity(4 + digest.as_bytes().len());
        key.extend_from_slice(&[0xff, b'V', b'G', route_marker]);
        key.extend_from_slice(digest.as_bytes());
        key
    }

    pub(super) fn vector_partition_graph_page_key(root_key: &[u8], page: u32) -> Vec<u8> {
        let mut key = Vec::with_capacity(root_key.len().saturating_add(4));
        key.extend_from_slice(root_key);
        key.extend_from_slice(&page.to_be_bytes());
        key
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn remove_vector_partition_graph_pages_in_write(
        write_txn: &redb::WriteTransaction,
        generation_key: &[u8],
        is_base: bool,
    ) -> Result<(usize, u64)> {
        let root_key = Self::vector_partition_graph_page_root_key(generation_key, is_base);
        Self::remove_vector_partition_graph_page_root_in_write(write_txn, &root_key)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn remove_vector_partition_graph_page_root_in_write(
        write_txn: &redb::WriteTransaction,
        root_key: &[u8],
    ) -> Result<(usize, u64)> {
        let mut removed = 0usize;
        let mut bytes = 0u64;
        let mut cursor = None::<Vec<u8>>;
        match write_txn.open_table(VECTOR_PARTITION_GRAPH_PAGE_TABLE) {
            Ok(mut pages) => loop {
                let next = {
                    let lower = cursor
                        .as_ref()
                        .map_or(std::ops::Bound::Included(root_key), |key| {
                            std::ops::Bound::Excluded(key.as_slice())
                        });
                    let mut next = None;
                    for entry in pages
                        .range::<&[u8]>((lower, std::ops::Bound::Unbounded))
                        .map_err(Self::storage_error)?
                    {
                        let (key, value) = entry.map_err(Self::storage_error)?;
                        let key = key.value();
                        if !key.starts_with(root_key) {
                            break;
                        }
                        // An ordinary base root can prefix another generation's
                        // root. Only this manifest and its four-byte ordinal
                        // children belong to this removal; leave other roots
                        // intact for their caller's manifest authentication.
                        if key.len() == root_key.len()
                            || key.len().checked_sub(root_key.len())
                                == Some(VECTOR_PARTITION_GRAPH_PAGE_KEY_SUFFIX_BYTES)
                        {
                            next = Some((key.to_vec(), value.value().len() as u64));
                            break;
                        }
                    }
                    next
                };
                let Some((key, value_bytes)) = next else {
                    break;
                };
                bytes = bytes.saturating_add(value_bytes);
                pages.remove(key.as_slice()).map_err(Self::storage_error)?;
                removed = removed.saturating_add(1);
                cursor = Some(key);
            },
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(error) => return Err(Self::storage_error(error)),
        }
        Ok((removed, bytes))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn remove_vector_partition_graph_pages_for_partition_in_write(
        &self,
        write_txn: &redb::WriteTransaction,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
    ) -> Result<()> {
        let mut cursor = None::<(
            Vec<u8>,
            Option<crate::memory_accounting::OwnedMemoryReservation>,
        )>;
        loop {
            let next = {
                let table = match write_txn.open_table(VECTOR_PARTITION_GRAPH_PAGE_TABLE) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
                    Err(error) => return Err(Self::storage_error(error)),
                };
                let lower = cursor
                    .as_ref()
                    .map_or(std::ops::Bound::Unbounded, |(key, _)| {
                        std::ops::Bound::Excluded(key.as_slice())
                    });
                let mut next = None;
                for entry in table
                    .range::<&[u8]>((lower, std::ops::Bound::Unbounded))
                    .map_err(Self::storage_error)?
                {
                    let (key, value) = entry.map_err(Self::storage_error)?;
                    if Self::is_vector_partition_graph_page_record_key(key.value()) {
                        continue;
                    }
                    // Admit the one manifest, derived identity and cursor before
                    // allocating. Physical graph bodies are never copied here.
                    let memory = self
                        .proof_accountant()
                        .map(|accountant| {
                            crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                                accountant,
                                key.value()
                                    .len()
                                    .saturating_add(value.value().len())
                                    .saturating_mul(64)
                                    .saturating_add(8192),
                                "vector",
                                "authoritative_purge_graph_pages",
                                "Raise MEMORY_LIMIT to erase vector graph pages.",
                            )
                        })
                        .transpose()?;
                    let manifest: VectorPartitionGraphPageManifestRecord = self
                        .decode_vector_partition_record(
                            VectorPartitionRecordKind::GraphPageManifest,
                            value.value(),
                        )?;
                    let root = self.authenticated_vector_partition_graph_manifest_root(
                        key.value(),
                        &manifest,
                        "authoritative purge graph page manifest key disagrees with its payload",
                    )?;
                    let selected =
                        manifest.index == *index && manifest.partition_key == *partition_key;
                    next = Some((root, selected, memory));
                    break;
                }
                next
            };
            let Some((root, selected, memory)) = next else {
                break;
            };
            if selected {
                Self::remove_vector_partition_graph_page_root_in_write(write_txn, &root)?;
            }
            // Hold the admitted cursor through the next seek. The enclosing
            // purge transaction also owns membership and catalog removal: any
            // manifest refusal aborts all durable effects together.
            cursor = Some((root, memory));
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn insert_preencoded_immutable_vector_partition_record(
        table: &mut redb::Table<'_, &[u8], &[u8]>,
        key: &[u8],
        encoded: &[u8],
        persistence: &Self,
    ) -> Result<()> {
        if let Some(existing) = table.get_bounded(key).map_err(Self::storage_error)? {
            if existing.value() != encoded {
                return Err(persistence.vector_partition_corruption(
                    "one immutable generation identity has conflicting payload bytes",
                ));
            }
            return Ok(());
        }
        table.insert(key, encoded).map_err(Self::storage_error)?;
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_vector_partition_base_generation(
        &self,
        base: &VectorPartitionBaseGenerationRecord,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        generation_id: u64,
    ) -> Result<()> {
        Self::validate_vector_partition_base_generation_at(
            base,
            index,
            partition_key,
            generation_id,
            |reason| self.vector_partition_corruption(reason),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_vector_partition_base_generation_at(
        base: &VectorPartitionBaseGenerationRecord,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        generation_id: u64,
        corruption: impl Fn(contextdb_core::error::DiagnosticText) -> Error,
    ) -> Result<()> {
        if base.index != *index
            || base.partition_key != *partition_key
            || base.generation_id != generation_id
            || base.dimension == 0
            || base.highest_included_tx.0 < base.base_tx.0
            || base.highest_included_lsn.0 < base.base_lsn.0
        {
            return Err(corruption(
                "base generation identity, dimension, or inclusion boundary is invalid".into(),
            ));
        }
        base.graph.verify(corruption)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_vector_partition_change_generation(
        &self,
        change: &VectorPartitionChangeGenerationRecord,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        base: &VectorPartitionBaseGenerationRecord,
        generation_id: u64,
    ) -> Result<()> {
        Self::validate_vector_partition_change_generation_at(
            change,
            index,
            partition_key,
            base,
            generation_id,
            |reason| self.vector_partition_corruption(reason),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_vector_partition_change_generation_at(
        change: &VectorPartitionChangeGenerationRecord,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        base: &VectorPartitionBaseGenerationRecord,
        generation_id: u64,
        corruption: impl Fn(contextdb_core::error::DiagnosticText) -> Error,
    ) -> Result<()> {
        if change.index != *index
            || change.partition_key != *partition_key
            || change.base_generation_id != base.generation_id
            || change.change_generation_id != generation_id
            || change.dimension != base.dimension
            || change.quantization != base.quantization
            || change.algorithm_version != base.algorithm_version
            || !vector_paged::change_format_compatible(
                change.graph_format_version,
                base.graph_format_version,
            )
            || change.covered_through_tx.0 < base.highest_included_tx.0
            || change.covered_through_lsn.0 < base.highest_included_lsn.0
            || change.fresh_tail_from_tx.0 < change.covered_through_tx.0
            || change.fresh_tail_from_lsn.0 < change.covered_through_lsn.0
        {
            return Err(corruption(
                "sealed change identity, format, or replay boundary is invalid".into(),
            ));
        }
        change.graph.verify(corruption)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_vector_partition_generation_candidate(
        &self,
        candidate: &VectorPartitionGenerationCandidate,
    ) -> Result<()> {
        let base = &candidate.base;
        self.validate_vector_partition_base_generation(
            base,
            &base.index,
            &base.partition_key,
            base.generation_id,
        )?;
        if candidate.catalog.index != base.index
            || candidate.catalog.partition_key != base.partition_key
            || candidate.catalog.base.generation_id != base.generation_id
        {
            return Err(self.vector_partition_corruption(
                "catalog does not identify its candidate base generation",
            ));
        }
        self.validate_vector_partition_catalog_descriptor(
            &candidate.catalog.base,
            base.highest_included_tx,
            base.highest_included_lsn,
            &base.graph,
        )?;
        match (&candidate.change, candidate.catalog.change) {
            (None, None) => Ok(()),
            (Some(change), Some(descriptor)) => {
                self.validate_vector_partition_change_generation(
                    change,
                    &base.index,
                    &base.partition_key,
                    base,
                    descriptor.generation_id,
                )?;
                self.validate_vector_partition_catalog_descriptor(
                    &descriptor,
                    change.covered_through_tx,
                    change.covered_through_lsn,
                    &change.graph,
                )
            }
            _ => Err(self.vector_partition_corruption(
                "catalog and candidate disagree about the sealed change generation",
            )),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_vector_partition_catalog_descriptor(
        &self,
        descriptor: &VectorPartitionGenerationDescriptor,
        covered_tx: TxId,
        covered_lsn: Lsn,
        graph: &VectorPartitionGraphBytes,
    ) -> Result<()> {
        let durable_bytes = u64::try_from(graph.bytes.len()).map_err(|_| {
            self.vector_partition_corruption(
                "generation graph byte length exceeds the durable descriptor range",
            )
        })?;
        // An empty HNSW has zero resident bytes but still has a non-empty,
        // checksummed durable envelope. The generation loader decodes that
        // envelope and verifies its exact resident-byte estimate before use.
        if descriptor.generation_id == 0
            || descriptor.covered_tx != covered_tx
            || descriptor.covered_lsn != covered_lsn
            || descriptor.durable_bytes != durable_bytes
        {
            return Err(self.vector_partition_corruption(
                "generation catalog descriptor disagrees with its immutable graph",
            ));
        }
        Ok(())
    }

    // The transaction, table codec, and two record projections are the one
    // atomic durable rename operation; bundling them would hide that boundary.
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn rename_vector_partition_records_in_write<T>(
        &self,
        write_txn: &redb::WriteTransaction,
        definition: TableDefinition<&'static [u8], &'static [u8]>,
        kind: VectorPartitionRecordKind,
        from: &VectorIndexRef,
        to: &VectorIndexRef,
        rename: impl Fn(&mut T, &VectorIndexRef) -> Result<()>,
        key_for: impl Fn(&T) -> Vec<u8>,
    ) -> Result<()>
    where
        T: serde::de::DeserializeOwned + Serialize,
        T: VectorPartitionRecordIndex,
    {
        let mut cursor_credit = self.proof_admission();
        cursor_credit.admit(
            from.table
                .len()
                .saturating_add(from.column.len())
                .saturating_add(64),
        )?;
        let mut prefix = Vec::with_capacity(from.table.len() + from.column.len() + 16);
        Self::append_partition_key_component(&mut prefix, from.table.as_bytes());
        Self::append_partition_key_component(&mut prefix, from.column.as_bytes());
        let mut cursor_bytes = 0usize;
        let mut after: Option<Vec<u8>> = None;
        loop {
            let mut window = self.proof_admission();
            let selected = {
                let table = write_txn
                    .open_table(definition)
                    .map_err(Self::storage_error)?;
                let lower = after.as_deref().map_or(
                    std::ops::Bound::Included(prefix.as_slice()),
                    std::ops::Bound::Excluded,
                );
                let mut cursor = table
                    .range::<&[u8]>((lower, std::ops::Bound::Unbounded))
                    .map_err(Self::storage_error)?;
                cursor
                    .next()
                    .transpose()
                    .map_err(Self::storage_error)?
                    .filter(|(key, _)| key.value().starts_with(&prefix))
                    .map(|(key, value)| {
                        window.admit(
                            key.value()
                                .len()
                                .saturating_mul(8)
                                .saturating_add(value.value().len().saturating_mul(32))
                                .saturating_add(to.table.len().saturating_mul(8))
                                .saturating_add(to.column.len().saturating_mul(8))
                                .saturating_add(2048),
                        )?;
                        let record: T = self.decode_vector_partition_record(kind, value.value())?;
                        Ok::<_, Error>((key.value().to_vec(), record))
                    })
                    .transpose()?
            };
            let Some((old_key, mut record)) = selected else {
                break;
            };
            // The cursor retains only this key; replacements inserted later in
            // the keyspace have the destination identity and are not renamed again.
            let required_cursor = old_key.len().saturating_mul(2).saturating_add(128);
            if required_cursor > cursor_bytes {
                cursor_credit.admit(required_cursor - cursor_bytes)?;
                cursor_bytes = required_cursor;
            }
            after = Some(old_key.clone());
            if record.vector_partition_record_index() != from || key_for(&record) != old_key {
                return Err(self.vector_partition_corruption(
                    "vector generation key disagrees with its addressed record",
                ));
            }
            rename(&mut record, to)?;
            let new_key = key_for(&record);
            let bytes = Self::encode_vector_partition_record(kind, &record)?;
            let mut table = write_txn
                .open_table(definition)
                .map_err(Self::storage_error)?;
            if let Some(existing) = table.get(new_key.as_slice()).map_err(Self::storage_error)?
                && existing.value() != bytes.as_slice()
            {
                return Err(self.vector_partition_corruption(
                    "vector rename collides with a different durable generation record",
                ));
            }
            table
                .insert(new_key.as_slice(), bytes.as_slice())
                .map_err(Self::storage_error)?;
            table
                .remove(old_key.as_slice())
                .map_err(Self::storage_error)?;
            drop(table);
            if kind == VectorPartitionRecordKind::Membership {
                self.write_open_directory_entry_with_pending_transition(
                    write_txn, &old_key, None, false,
                )?;
                self.write_open_directory_entry_with_pending_transition(
                    write_txn,
                    &new_key,
                    Some(&bytes),
                    false,
                )?;
            }
        }
        vector_membership_pages::settle_dirty_occupancy(write_txn, self.proof_accountant())?;
        Ok(())
    }

    /// Remove only the raw vector records owned by the selected logical
    /// indexes. Schema changes must never reconstruct this table from the
    /// process's lazy resident cache, because dormant partitions are durable
    /// state even when no body is currently loaded.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn remove_vector_entries_matching_in_write(
        &self,
        write_txn: &redb::WriteTransaction,
        matches: &impl Fn(&VectorIndexRef) -> bool,
    ) -> Result<()> {
        self.remove_vector_entries_with_identity_in_write(write_txn, matches, false, None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn remove_vector_entries_with_identity_in_write(
        &self,
        write_txn: &redb::WriteTransaction,
        matches: &impl Fn(&VectorIndexRef) -> bool,
        retire_identity: bool,
        table_scope: Option<&str>,
    ) -> Result<()> {
        let _decode_scope = self.writer_decode_scope()?;
        match content_size::open_vectors(write_txn) {
            Ok(table) => drop(table),
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
            Err(error) => return Err(Self::storage_error(error)),
        };
        let mut cursor_credit = self.proof_admission();
        let prefix = table_scope
            .map(|name| {
                cursor_credit.admit(name.len().saturating_add(1).saturating_mul(2))?;
                let mut prefix = Vec::with_capacity(name.len().saturating_add(1));
                prefix.extend_from_slice(name.as_bytes());
                prefix.push(0);
                Ok::<_, Error>(prefix)
            })
            .transpose()?;
        let mut cursor_bytes = 0usize;
        let mut after: Option<Vec<u8>> = None;
        loop {
            let mut window = self.proof_admission();
            let selected = {
                let table = content_size::open_vectors(write_txn).map_err(Self::storage_error)?;
                let lower = after.as_deref().map_or_else(
                    || {
                        prefix
                            .as_deref()
                            .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Included)
                    },
                    std::ops::Bound::Excluded,
                );
                let mut cursor = table
                    .range::<&[u8]>((lower, std::ops::Bound::Unbounded))
                    .map_err(Self::storage_error)?;
                cursor
                    .next()
                    .transpose()
                    .map_err(Self::storage_error)?
                    .filter(|(key, _)| {
                        prefix
                            .as_ref()
                            .is_none_or(|prefix| key.value().starts_with(prefix))
                    })
                    .map(|(key, value)| {
                        window.admit(
                            key.value()
                                .len()
                                .saturating_mul(4)
                                .saturating_add(value.value().len().saturating_mul(32))
                                .saturating_add(1024),
                        )?;
                        // Retirement needs coordinates and the stored codec, not
                        // an expanded f32 body. Keep one encoded placement window.
                        let entry = Self::decode_persisted_vector_entry_exact(value.value())?;
                        let remove = matches(&entry.index);
                        let mutations = if remove && retire_identity {
                            let quantization = entry.vector.quantization();
                            vec![
                                content_identity::remove_vector_mutation(
                                    &entry.index,
                                    entry.row_id,
                                    quantization,
                                ),
                                content_identity::remove_vector_version_mutation(
                                    &entry.index,
                                    entry.row_id,
                                    entry.created_tx,
                                    entry.lsn,
                                    quantization,
                                ),
                            ]
                        } else {
                            Vec::new()
                        };
                        Ok::<_, Error>((key.value().to_vec(), remove, mutations))
                    })
                    .transpose()?
            };
            let Some((key, remove, mutations)) = selected else {
                break;
            };
            let bytes = key.len().saturating_mul(2).saturating_add(128);
            if bytes > cursor_bytes {
                cursor_credit.admit(bytes - cursor_bytes)?;
                cursor_bytes = bytes;
            }
            after = Some(key.clone());
            if remove {
                {
                    let mut table =
                        content_size::open_vectors(write_txn).map_err(Self::storage_error)?;
                    table.remove(key.as_slice()).map_err(Self::storage_error)?;
                }
                if retire_identity {
                    Self::apply_content_identity_mutations_in_write(
                        write_txn,
                        &mutations,
                        &mut self.proof_admission(),
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Apply vector-column rename/drop effects inside the same Redb
    /// transaction as metadata, rows, generation records, and DDL. Entries
    /// for every unaffected table and column remain byte-for-byte untouched.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn rewrite_vector_entries_for_schema_in_write(
        &self,
        write_txn: &redb::WriteTransaction,
        table_name: &str,
        renames: &[(VectorIndexRef, VectorIndexRef)],
        dropped_columns: &HashSet<String>,
        quantization: &HashMap<VectorIndexRef, VectorQuantization>,
    ) -> Result<()> {
        let _decode_scope = self.writer_decode_scope()?;
        if renames.is_empty() && dropped_columns.is_empty() {
            return Ok(());
        }
        let mut cursor_credit = self.proof_admission();
        for source_column in renames
            .iter()
            .map(|(from, _)| from.column.as_str())
            .chain(dropped_columns.iter().map(String::as_str))
        {
            cursor_credit.admit(
                table_name
                    .len()
                    .saturating_add(source_column.len())
                    .saturating_add(64),
            )?;
            let mut prefix = Vec::with_capacity(table_name.len() + source_column.len() + 2);
            prefix.extend_from_slice(table_name.as_bytes());
            prefix.push(0);
            prefix.extend_from_slice(source_column.as_bytes());
            prefix.push(0);
            let mut cursor_bytes = 0usize;
            let mut after: Option<Vec<u8>> = None;
            loop {
                let mut window = self.proof_admission();
                let selected = {
                    let table =
                        content_size::open_vectors(write_txn).map_err(Self::storage_error)?;
                    let lower = after.as_deref().map_or(
                        std::ops::Bound::Included(prefix.as_slice()),
                        std::ops::Bound::Excluded,
                    );
                    let mut cursor = table
                        .range::<&[u8]>((lower, std::ops::Bound::Unbounded))
                        .map_err(Self::storage_error)?;
                    cursor
                        .next()
                        .transpose()
                        .map_err(Self::storage_error)?
                        .filter(|(key, _)| key.value().starts_with(&prefix))
                        .map(|(key, value)| {
                            window.admit(
                                key.value()
                                    .len()
                                    .saturating_mul(8)
                                    .saturating_add(value.value().len().saturating_mul(32))
                                    .saturating_add(2048),
                            )?;
                            Ok::<_, Error>((
                                key.value().to_vec(),
                                Self::decode_vector_entry(value.value())?,
                            ))
                        })
                        .transpose()?
                };
                let Some((old_key, mut entry)) = selected else {
                    break;
                };
                let required_cursor = old_key.len().saturating_mul(2).saturating_add(128);
                if required_cursor > cursor_bytes {
                    cursor_credit.admit(required_cursor - cursor_bytes)?;
                    cursor_bytes = required_cursor;
                }
                after = Some(old_key.clone());
                if entry.index.table != table_name
                    || entry.index.column != source_column
                    || Self::vector_key(&entry) != old_key
                {
                    return Err(self.vector_partition_corruption(
                        "raw vector key disagrees with its addressed record",
                    ));
                }
                let dropped = dropped_columns.contains(&entry.index.column);
                let target = renames
                    .iter()
                    .find(|(from, _)| *from == entry.index)
                    .map(|(_, to)| to);
                if !dropped && target.is_none() {
                    continue;
                }
                let previous_quantization =
                    Self::vector_quantization_in_write(write_txn, &entry.index)?;
                let mut mutations = vec![
                    content_identity::remove_vector_mutation(
                        &entry.index,
                        entry.row_id,
                        previous_quantization,
                    ),
                    content_identity::remove_vector_version_mutation(
                        &entry.index,
                        entry.row_id,
                        entry.created_tx,
                        entry.lsn,
                        previous_quantization,
                    ),
                ];
                if let Some(to) = target.filter(|_| !dropped) {
                    window.admit(
                        to.table
                            .len()
                            .saturating_add(to.column.len())
                            .saturating_mul(8),
                    )?;
                    entry.index = to.clone();
                    let quantization = quantization.get(to).copied().unwrap_or_default();
                    let encoded = Self::encode_vector_entry(&entry, quantization)?;
                    let new_key = Self::vector_key(&entry);
                    {
                        let mut table =
                            content_size::open_vectors(write_txn).map_err(Self::storage_error)?;
                        if let Some(existing) =
                            table.get(new_key.as_slice()).map_err(Self::storage_error)?
                            && existing.value() != encoded.as_slice()
                        {
                            return Err(self.vector_partition_corruption(
                                "vector rename collides with a different durable raw vector",
                            ));
                        }
                        table
                            .insert(new_key.as_slice(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        table
                            .remove(old_key.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                    mutations.push(content_identity::vector_mutation(&entry, quantization));
                    mutations.push(content_identity::vector_version_mutation(
                        &entry,
                        quantization,
                    ));
                } else {
                    content_size::open_vectors(write_txn)
                        .map_err(Self::storage_error)?
                        .remove(old_key.as_slice())
                        .map_err(Self::storage_error)?;
                }
                Self::apply_content_identity_mutations_in_write(
                    write_txn,
                    &mutations,
                    &mut self.proof_admission(),
                )?;
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn rename_vector_partition_generation_index_in_write(
        &self,
        write_txn: &redb::WriteTransaction,
        from: &VectorIndexRef,
        to: &VectorIndexRef,
    ) -> Result<()> {
        self.prepare_proof_owner()?;
        if from == to {
            return Ok(());
        }
        // Page keys include the canonical index identity. Remove their marker
        // before rewriting the complete generation records; the renamed root
        // remains a valid explicit complete-record fallback until its writer
        // next publishes a materialized generation.
        self.remove_vector_partition_graph_pages_matching_in_write(
            write_txn,
            self.proof_accountant(),
            &|index| index == from,
        )?;
        self.rename_vector_partition_records_in_write::<VectorPartitionMembershipRecord>(
            write_txn,
            VECTOR_PARTITION_MEMBERSHIP_TABLE,
            VectorPartitionRecordKind::Membership,
            from,
            to,
            |record, to| {
                record.index = to.clone();
                Ok(())
            },
            Self::vector_partition_membership_key,
        )?;
        vector_membership_pages::rename_pending_routes(
            write_txn,
            from,
            to,
            self.proof_accountant(),
        )?;
        self.rename_vector_partition_records_in_write::<VectorPartitionTombstoneRecord>(
            write_txn,
            VECTOR_PARTITION_TOMBSTONE_TABLE,
            VectorPartitionRecordKind::Tombstone,
            from,
            to,
            |record, to| {
                record.membership.index = to.clone();
                Ok(())
            },
            Self::vector_partition_tombstone_key,
        )?;
        self.rename_vector_partition_records_in_write::<VectorPartitionJournalRecord>(
            write_txn,
            VECTOR_PARTITION_JOURNAL_TABLE,
            VectorPartitionRecordKind::Journal,
            from,
            to,
            |record, to| {
                record.index = to.clone();
                match &mut record.change {
                    VectorPartitionJournalChange::Upsert(membership) => {
                        membership.index = to.clone()
                    }
                    VectorPartitionJournalChange::Tombstone(tombstone) => {
                        tombstone.membership.index = to.clone()
                    }
                }
                Ok(())
            },
            Self::vector_partition_journal_key,
        )?;
        self.rename_vector_partition_records_in_write::<VectorPartitionBaseGenerationRecord>(
            write_txn,
            VECTOR_PARTITION_BASE_GENERATION_TABLE,
            VectorPartitionRecordKind::BaseGeneration,
            from,
            to,
            |record, to| {
                record.index = to.clone();
                self.rebind_paged_generation_in_write(
                    write_txn,
                    to,
                    &record.partition_key,
                    &mut record.graph,
                )
            },
            Self::vector_partition_base_generation_key,
        )?;
        self.rename_vector_partition_records_in_write::<VectorPartitionChangeGenerationRecord>(
            write_txn,
            VECTOR_PARTITION_CHANGE_GENERATION_TABLE,
            VectorPartitionRecordKind::ChangeGeneration,
            from,
            to,
            |record, to| {
                record.index = to.clone();
                self.rebind_paged_generation_in_write(
                    write_txn,
                    to,
                    &record.partition_key,
                    &mut record.graph,
                )
            },
            Self::vector_partition_change_generation_key,
        )?;
        self.rename_vector_partition_records_in_write::<VectorPartitionGenerationCatalogRecord>(
            write_txn,
            VECTOR_PARTITION_GENERATION_CATALOG_TABLE,
            VectorPartitionRecordKind::GenerationCatalog,
            from,
            to,
            |record, to| {
                record.index = to.clone();
                let bases = write_txn
                    .open_table(VECTOR_PARTITION_BASE_GENERATION_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::vector_partition_base_generation_key_from_parts(
                    to,
                    &record.partition_key,
                    record.base.generation_id,
                );
                if let Some(value) = bases.get(key.as_slice()).map_err(Self::storage_error)? {
                    let base: VectorPartitionBaseGenerationRecord = self
                        .decode_vector_partition_record(
                            VectorPartitionRecordKind::BaseGeneration,
                            value.value(),
                        )?;
                    record.base.durable_bytes = base.graph.bytes.len() as u64;
                }
                if let Some(change) = &mut record.change {
                    let changes = write_txn
                        .open_table(VECTOR_PARTITION_CHANGE_GENERATION_TABLE)
                        .map_err(Self::storage_error)?;
                    let key = Self::vector_partition_change_generation_key_from_parts(
                        to,
                        &record.partition_key,
                        record.base.generation_id,
                        change.generation_id,
                    );
                    if let Some(value) = changes.get(key.as_slice()).map_err(Self::storage_error)? {
                        let changed: VectorPartitionChangeGenerationRecord = self
                            .decode_vector_partition_record(
                                VectorPartitionRecordKind::ChangeGeneration,
                                value.value(),
                            )?;
                        change.durable_bytes = changed.graph.bytes.len() as u64;
                    }
                }
                Ok(())
            },
            Self::vector_partition_catalog_key,
        )?;
        self.rename_paged_build_bindings_in_write(write_txn, from, to)?;
        Self::ensure_open_vector_directory_in_write(write_txn)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn remove_vector_partition_records_matching_all_in_write(
        &self,
        write_txn: &redb::WriteTransaction,
        table: &str,
        columns: Option<&HashSet<String>>,
        matches: &impl Fn(&VectorIndexRef) -> bool,
    ) -> Result<()> {
        self.prepare_proof_owner()?;
        self.remove_vector_partition_graph_pages_matching_in_write(
            write_txn,
            self.proof_accountant(),
            matches,
        )?;
        self.remove_vector_partition_records_matching_in_write::<VectorPartitionMembershipRecord>(
            write_txn,
            VECTOR_PARTITION_MEMBERSHIP_TABLE,
            VectorPartitionRecordKind::Membership,
            matches,
        )?;
        self.remove_vector_partition_records_matching_in_write::<VectorPartitionTombstoneRecord>(
            write_txn,
            VECTOR_PARTITION_TOMBSTONE_TABLE,
            VectorPartitionRecordKind::Tombstone,
            matches,
        )?;
        self.remove_vector_partition_records_matching_in_write::<VectorPartitionJournalRecord>(
            write_txn,
            VECTOR_PARTITION_JOURNAL_TABLE,
            VectorPartitionRecordKind::Journal,
            matches,
        )?;
        self.remove_vector_partition_records_matching_in_write::<VectorPartitionBaseGenerationRecord>(
            write_txn, VECTOR_PARTITION_BASE_GENERATION_TABLE, VectorPartitionRecordKind::BaseGeneration, matches,
        )?;
        self.remove_vector_partition_records_matching_in_write::<VectorPartitionChangeGenerationRecord>(
            write_txn, VECTOR_PARTITION_CHANGE_GENERATION_TABLE, VectorPartitionRecordKind::ChangeGeneration, matches,
        )?;
        self.remove_vector_partition_records_matching_in_write::<VectorPartitionGenerationCatalogRecord>(
            write_txn, VECTOR_PARTITION_GENERATION_CATALOG_TABLE, VectorPartitionRecordKind::GenerationCatalog, matches,
        )?;
        if let Some(columns) = columns {
            for column in columns {
                vector_membership_pages::remove_routes_matching(
                    write_txn,
                    table,
                    Some(column),
                    self.proof_accountant(),
                )?;
            }
        } else {
            vector_membership_pages::remove_routes_matching(
                write_txn,
                table,
                None,
                self.proof_accountant(),
            )?;
        }
        Self::ensure_open_vector_directory_in_write(write_txn)
    }

    // Physical pages share the manifest table but append an ordinal to a root.
    // Recognize only the two generation suffixes and the compact-root namespace;
    // every other key must still pass the typed manifest decoder.
    pub(super) fn is_vector_partition_graph_page_record_key(key: &[u8]) -> bool {
        if key.starts_with(&[0xff, b'V', b'G']) {
            return key.len() == 40 && matches!(key[3], 0xb0 | 0xc0);
        }
        let mut remaining = key;
        for _ in 0..3 {
            let Some(length_bytes) = remaining.get(..8) else {
                return false;
            };
            let Ok(length) = usize::try_from(u64::from_be_bytes(
                length_bytes
                    .try_into()
                    .expect("eight-byte component length"),
            )) else {
                return false;
            };
            let Some(component_end) = 8usize.checked_add(length) else {
                return false;
            };
            let Some(suffix) = remaining.get(component_end..) else {
                return false;
            };
            remaining = suffix;
        }
        (remaining.len() == 13 && remaining[8] == 0xb0)
            || (remaining.len() == 21 && remaining[16] == 0xc0)
    }

    // Call only after admitting the manifest and producer-key scratch. The
    // envelope authenticates the value; this binds it to its physical root.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn authenticated_vector_partition_graph_manifest_root(
        &self,
        physical_key: &[u8],
        manifest: &VectorPartitionGraphPageManifestRecord,
        corruption_reason: &str,
    ) -> Result<Vec<u8>> {
        let generation_key = if manifest.is_base {
            Self::vector_partition_base_generation_key_from_parts(
                &manifest.index,
                &manifest.partition_key,
                manifest.generation_id,
            )
        } else {
            Self::vector_partition_change_generation_key_from_parts(
                &manifest.index,
                &manifest.partition_key,
                manifest.base_generation_id,
                manifest.generation_id,
            )
        };
        let root = Self::vector_partition_graph_page_root_key(&generation_key, manifest.is_base);
        if root.as_slice() != physical_key
            || (manifest.is_base && manifest.base_generation_id != manifest.generation_id)
        {
            return Err(self.vector_partition_corruption(corruption_reason));
        }
        Ok(root)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn remove_vector_partition_graph_pages_matching_in_write(
        &self,
        write_txn: &redb::WriteTransaction,
        accountant: Option<crate::memory_accounting::AccountantHandle>,
        matches: &impl Fn(&VectorIndexRef) -> bool,
    ) -> Result<()> {
        let mut cursor = None::<(Vec<u8>, crate::memory_accounting::OwnedMemoryReservation)>;
        loop {
            let next = {
                let table = match write_txn.open_table(VECTOR_PARTITION_GRAPH_PAGE_TABLE) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
                    Err(error) => return Err(Self::storage_error(error)),
                };
                let lower = cursor
                    .as_ref()
                    .map_or(std::ops::Bound::Unbounded, |(key, _)| {
                        std::ops::Bound::Excluded(key.as_slice())
                    });
                let mut next = None;
                for entry in table
                    .range::<&[u8]>((lower, std::ops::Bound::Unbounded))
                    .map_err(Self::storage_error)?
                {
                    let (key, value) = entry.map_err(Self::storage_error)?;
                    if Self::is_vector_partition_graph_page_record_key(key.value()) {
                        continue;
                    }
                    let owner = accountant.as_ref().ok_or_else(|| {
                        Error::Other(
                            "graph manifest cleanup requires its writer memory accountant".into(),
                        )
                    })?;
                    // One decoded route, producer key, physical root and page
                    // deletion cursors, plus the preceding seek cursor. Price
                    // the borrowed value even for a 36-byte compact root.
                    let memory = crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                        owner.clone(),
                        key.value()
                            .len()
                            .saturating_add(value.value().len())
                            .saturating_mul(64)
                            .saturating_add(8192),
                        "vector",
                        "matching_graph_pages",
                        "Raise MEMORY_LIMIT to remove vector graph pages.",
                    )?;
                    let manifest: VectorPartitionGraphPageManifestRecord = self
                        .decode_vector_partition_record(
                            VectorPartitionRecordKind::GraphPageManifest,
                            value.value(),
                        )?;
                    // Authenticate every manifest before selection, including
                    // unrelated routes: a corrupt value cannot authorize erasure.
                    let root = self.authenticated_vector_partition_graph_manifest_root(
                        key.value(),
                        &manifest,
                        "graph page manifest key disagrees with its payload",
                    )?;
                    let selected = matches(&manifest.index);
                    next = Some((root, selected, memory));
                    break;
                }
                next
            };
            let Some((root, selected, memory)) = next else {
                break;
            };
            if selected {
                Self::remove_vector_partition_graph_page_root_in_write(write_txn, &root)?;
            }
            // Reservation also funds the exact-root remover's key copies and
            // remains held until the next admitted root replaces this cursor.
            // Any refusal escapes the enclosing write transaction uncommitted.
            cursor = Some((root, memory));
        }
        Ok(())
    }

    pub(super) fn vector_partition_membership_matches_identity(
        membership: &VectorPartitionMembershipRecord,
        identity: &VectorPartitionVersionIdentity,
    ) -> bool {
        membership.index == identity.index
            && membership.partition_key == identity.partition_key
            && membership.row_id == identity.row_id
            && membership.row_created_tx == identity.row_created_tx
            && membership.row_lsn == identity.row_lsn
            && membership.vector_created_tx == identity.vector_created_tx
            && membership.vector_lsn == identity.vector_lsn
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn remove_vector_partition_prefix_records_in_write<T>(
        &self,
        write_txn: &redb::WriteTransaction,
        definition: TableDefinition<&'static [u8], &'static [u8]>,
        kind: VectorPartitionRecordKind,
        prefix: &[u8],
        key_for: impl Fn(&T) -> Vec<u8>,
    ) -> Result<()>
    where
        T: serde::de::DeserializeOwned,
    {
        let mut table = match write_txn.open_table(definition) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
            Err(error) => return Err(Self::storage_error(error)),
        };
        let mut keys = Vec::new();
        for entry in table.range(prefix..).map_err(Self::storage_error)? {
            let (key, value) = entry.map_err(Self::storage_error)?;
            if !key.value().starts_with(prefix) {
                break;
            }
            let record: T = self.decode_vector_partition_record(kind, value.value())?;
            if key_for(&record).as_slice() != key.value() {
                return Err(self.vector_partition_corruption(
                    "partition generation key does not match its checksummed payload",
                ));
            }
            keys.push(key.value().to_vec());
        }
        for key in keys {
            table.remove(key.as_slice()).map_err(Self::storage_error)?;
        }
        Ok(())
    }

    /// Storage of the replacement generations an erasure prepared for itself.
    pub(super) fn preencoded_native_storage(
        prepared: &[PreencodedVectorPartitionGenerationCandidate],
    ) -> Vec<u64> {
        prepared
            .iter()
            .flat_map(|candidate| candidate.native_storage.iter().flatten().copied())
            .collect()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn purge_vector_partition_generation_identities_in_write(
        &self,
        write_txn: &redb::WriteTransaction,
        identities: &[VectorPartitionVersionIdentity],
        accountant: impl Into<contextdb_vector::MemoryBudgetOwner>,
        operation: &str,
        own_storage: &[u64],
    ) -> Result<()> {
        let accountant: contextdb_vector::MemoryBudgetOwner = accountant.into();

        if identities.is_empty() {
            return Ok(());
        }

        {
            let mut memberships = write_txn
                .open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE)
                .map_err(Self::storage_error)?;
            for identity in identities {
                let key = Self::vector_partition_version_identity_key(identity);
                let bytes = memberships
                    .get(key.as_slice())
                    .map_err(Self::storage_error)?
                    .map(|value| value.value().to_vec())
                    .ok_or_else(|| {
                        self.vector_partition_corruption(
                            "authoritative purge membership disappeared before durable commit",
                        )
                    })?;
                let membership: VectorPartitionMembershipRecord = self
                    .decode_vector_partition_record(
                        VectorPartitionRecordKind::Membership,
                        &bytes,
                    )?;
                if Self::vector_partition_membership_key(&membership) != key
                    || !Self::vector_partition_membership_matches_identity(&membership, identity)
                {
                    return Err(self.vector_partition_corruption(
                        "authoritative purge membership key disagrees with its selected identity",
                    ));
                }
                memberships
                    .remove(key.as_slice())
                    .map_err(Self::storage_error)?;
                self.write_open_directory_entry(write_txn, &key, None)?;
            }
        }

        let mut affected_partitions = Vec::new();
        for identity in identities {
            let partition = (identity.index.clone(), identity.partition_key.clone());
            if !affected_partitions.contains(&partition) {
                affected_partitions.push(partition);
            }
        }

        {
            let mut journal = write_txn
                .open_table(VECTOR_PARTITION_JOURNAL_TABLE)
                .map_err(Self::storage_error)?;
            let mut keys = Vec::new();
            for (index, partition_key) in &affected_partitions {
                let prefix = Self::vector_partition_prefix(index, partition_key);
                for entry in journal
                    .range(prefix.as_slice()..)
                    .map_err(Self::storage_error)?
                {
                    let (key, value) = entry.map_err(Self::storage_error)?;
                    if !key.value().starts_with(&prefix) {
                        break;
                    }
                    let record: VectorPartitionJournalRecord = self
                        .decode_vector_partition_record(
                            VectorPartitionRecordKind::Journal,
                            value.value(),
                        )?;
                    if Self::vector_partition_journal_key(&record).as_slice() != key.value() {
                        return Err(self.vector_partition_corruption(
                            "authoritative purge journal key disagrees with its payload",
                        ));
                    }
                    let membership = match &record.change {
                        VectorPartitionJournalChange::Upsert(membership) => membership,
                        VectorPartitionJournalChange::Tombstone(tombstone) => &tombstone.membership,
                    };
                    if identities.iter().any(|identity| {
                        Self::vector_partition_membership_matches_identity(membership, identity)
                    }) {
                        keys.push(key.value().to_vec());
                    }
                }
            }
            for key in keys {
                journal
                    .remove(key.as_slice())
                    .map_err(Self::storage_error)?;
            }
        }

        {
            let mut tombstones = write_txn
                .open_table(VECTOR_PARTITION_TOMBSTONE_TABLE)
                .map_err(Self::storage_error)?;
            let mut keys = Vec::new();
            for (index, partition_key) in &affected_partitions {
                let prefix = Self::vector_partition_prefix(index, partition_key);
                for entry in tombstones
                    .range(prefix.as_slice()..)
                    .map_err(Self::storage_error)?
                {
                    let (key, value) = entry.map_err(Self::storage_error)?;
                    if !key.value().starts_with(&prefix) {
                        break;
                    }
                    let record: VectorPartitionTombstoneRecord = self
                        .decode_vector_partition_record(
                            VectorPartitionRecordKind::Tombstone,
                            value.value(),
                        )?;
                    if Self::vector_partition_tombstone_key(&record).as_slice() != key.value() {
                        return Err(self.vector_partition_corruption(
                            "authoritative purge tombstone key disagrees with its payload",
                        ));
                    }
                    if identities.iter().any(|identity| {
                        Self::vector_partition_membership_matches_identity(
                            &record.membership,
                            identity,
                        )
                    }) {
                        keys.push(key.value().to_vec());
                    }
                }
            }
            for key in keys {
                tombstones
                    .remove(key.as_slice())
                    .map_err(Self::storage_error)?;
            }
        }

        for (index, partition_key) in affected_partitions {
            let prefix = Self::vector_partition_prefix(&index, &partition_key);
            // Erasure covers native paged generations and unpublished paged
            // builds too: their pages hold copies of the erased vectors.
            self.erase_partition_paged_storage_in_write(
                write_txn,
                &prefix,
                accountant.clone(),
                operation,
                own_storage,
            )?;
            self.remove_vector_partition_graph_pages_for_partition_in_write(
                write_txn,
                &index,
                &partition_key,
            )?;
            self.remove_vector_partition_prefix_records_in_write::<
                VectorPartitionBaseGenerationRecord,
            >(
                write_txn,
                VECTOR_PARTITION_BASE_GENERATION_TABLE,
                VectorPartitionRecordKind::BaseGeneration,
                &prefix,
                Self::vector_partition_base_generation_key,
            )?;
            self.remove_vector_partition_prefix_records_in_write::<
                VectorPartitionChangeGenerationRecord,
            >(
                write_txn,
                VECTOR_PARTITION_CHANGE_GENERATION_TABLE,
                VectorPartitionRecordKind::ChangeGeneration,
                &prefix,
                Self::vector_partition_change_generation_key,
            )?;
            self.remove_vector_partition_prefix_records_in_write::<
                VectorPartitionGenerationCatalogRecord,
            >(
                write_txn,
                VECTOR_PARTITION_GENERATION_CATALOG_TABLE,
                VectorPartitionRecordKind::GenerationCatalog,
                &prefix,
                Self::vector_partition_catalog_key,
            )?;
            vector_membership_pages::reset_pending_frontier(
                write_txn,
                &VectorPartitionRef::new(index, partition_key),
            )?;
        }
        vector_membership_pages::settle_dirty_occupancy(write_txn, self.proof_accountant())?;
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn remove_vector_partition_records_matching_in_write<T>(
        &self,
        write_txn: &redb::WriteTransaction,
        definition: TableDefinition<&'static [u8], &'static [u8]>,
        kind: VectorPartitionRecordKind,
        matches: &impl Fn(&VectorIndexRef) -> bool,
    ) -> Result<()>
    where
        T: serde::de::DeserializeOwned + VectorPartitionRecordIndex,
    {
        let mut table = match write_txn.open_table(definition) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
            Err(error) => return Err(Self::storage_error(error)),
        };
        let mut cursor = None::<(
            Vec<u8>,
            Option<crate::memory_accounting::OwnedMemoryReservation>,
        )>;
        loop {
            let next = {
                let lower = cursor
                    .as_ref()
                    .map_or(std::ops::Bound::Unbounded, |(key, _)| {
                        std::ops::Bound::Excluded(key.as_slice())
                    });
                let mut entries = table
                    .range::<&[u8]>((lower, std::ops::Bound::Unbounded))
                    .map_err(Self::storage_error)?;
                entries
                    .next()
                    .transpose()
                    .map_err(Self::storage_error)?
                    .map(|(key, value)| -> Result<_> {
                        let memory =
                            self.admit_vector_image_build_route(key.value(), value.value().len())?;
                        let record: T = self.decode_vector_partition_record(kind, value.value())?;
                        Ok((key.value().to_vec(), record, memory))
                    })
                    .transpose()?
            };
            let Some((key, record, memory)) = next else {
                break;
            };
            if matches(record.vector_partition_record_index()) {
                if let Some(meta) = record.native_paged_graph() {
                    Self::queue_paged_generation_cleanup_in_write(write_txn, meta)?;
                }
                table.remove(key.as_slice()).map_err(Self::storage_error)?;
                if kind == VectorPartitionRecordKind::Membership {
                    self.write_open_directory_entry(write_txn, &key, None)?;
                }
            }
            cursor = Some((key, memory));
        }
        vector_membership_pages::settle_dirty_occupancy(write_txn, self.proof_accountant())?;
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn membership_from_binding(
        index: &VectorIndexRef,
        binding: &PreparedVectorPartitionBinding,
        deleted_tx: Option<TxId>,
    ) -> Result<VectorPartitionMembershipRecord> {
        let derived = vector_partition_key_for_row(
            index,
            &binding.table_meta,
            binding
                .table_meta
                .columns
                .get(binding.declaration)
                .filter(|column| column.name == index.column)
                .ok_or_else(|| {
                    Error::Other("prepared vector declaration ordinal is invalid".into())
                })?,
            &binding.row,
        )?;
        if derived != binding.partition_key {
            return Err(Error::Other(
                (format!(
                    "prepared vector partition key no longer matches its row/declaration on {}.{}",
                    index.table, index.column
                ))
                .into(),
            ));
        }
        Ok(VectorPartitionMembershipRecord {
            index: index.clone(),
            partition_key: binding.partition_key.clone(),
            row_id: binding.row.row_id,
            row_created_tx: binding.row.created_tx,
            row_lsn: binding.row.lsn,
            vector_created_tx: binding.vector_created_tx,
            vector_lsn: binding.vector_lsn,
            visible_from: TxId(binding.row.created_tx.0.max(binding.vector_created_tx.0)),
            deleted_tx,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn vector_partition_corruption(&self, reason: impl std::fmt::Display) -> Error {
        crate::funded_error::corruption_display(
            crate::read_image_memory::active_owner().and_then(|owner| owner.diagnostic_budget()),
            self.path.display(),
            format_args!("vector partition durability record is corrupt: {reason}"),
        )
    }

    /// Envelope layout: magic bytes, u16 big-endian format version, u8 table
    /// kind, u64 big-endian payload length, bincode payload, then a 32-byte
    /// BLAKE3 checksum over every preceding byte.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn encode_vector_partition_record<T: Serialize>(
        kind: VectorPartitionRecordKind,
        record: &T,
    ) -> Result<Vec<u8>> {
        let payload = Self::encode(record)?;
        let mut encoded = Vec::with_capacity(
            VECTOR_PARTITION_RECORD_MAGIC.len() + 2 + 1 + 8 + payload.len() + 32,
        );
        encoded.extend_from_slice(VECTOR_PARTITION_RECORD_MAGIC);
        encoded.extend_from_slice(&VECTOR_PARTITION_RECORD_VERSION.to_be_bytes());
        encoded.push(kind as u8);
        encoded.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        encoded.extend_from_slice(&payload);
        let checksum = blake3::hash(&encoded);
        encoded.extend_from_slice(checksum.as_bytes());
        Ok(encoded)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn decode_vector_partition_record<T: serde::de::DeserializeOwned>(
        &self,
        expected_kind: VectorPartitionRecordKind,
        encoded: &[u8],
    ) -> Result<T> {
        let _decode_scope = self.reader_decode_scope()?;
        let payload = vector_partition_record_payload(expected_kind, encoded)
            .map_err(|reason| self.vector_partition_corruption(reason))?;
        prevalidate_vector_partition_metadata_routes(expected_kind, payload)
            .map_err(|reason| self.vector_partition_corruption(reason))?;
        Self::decode_exact(payload).map_err(|error| match error {
            Error::MemoryBudgetExceeded { .. } | Error::ReadFailure(_) | Error::ReadCancelled => {
                error
            }
            error => {
                self.vector_partition_corruption(format_args!("payload decode failed: {error}"))
            }
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn decode_expected_vector_partition_metadata<T: serde::de::DeserializeOwned>(
        &self,
        kind: VectorPartitionRecordKind,
        encoded: &[u8],
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
    ) -> Result<T> {
        let _decode_scope = self.reader_decode_scope()?;
        let payload =
            vector_partition_expected_metadata_payload(kind, encoded, index, partition_key)
                .map_err(|reason| self.vector_partition_corruption(reason))?;
        Self::decode_exact(payload).map_err(|error| match error {
            Error::MemoryBudgetExceeded { .. } | Error::ReadFailure(_) | Error::ReadCancelled => {
                error
            }
            error => {
                self.vector_partition_corruption(format_args!("payload decode failed: {error}"))
            }
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn insert_vector_partition_record<T: Serialize>(
        &self,
        table: &mut redb::Table<'_, &[u8], &[u8]>,
        key: &[u8],
        kind: VectorPartitionRecordKind,
        record: &T,
    ) -> Result<()> {
        let encoded = Self::encode_vector_partition_record(kind, record)?;
        if let Some(existing) = table.get_bounded(key).map_err(Self::storage_error)? {
            if existing.value() != encoded.as_slice() {
                return Err(self.vector_partition_corruption(
                    "one durable identity has conflicting checksummed payloads",
                ));
            }
            return Ok(());
        }
        table
            .insert(key, encoded.as_slice())
            .map_err(Self::storage_error)?;
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn insert_vector_partition_membership(
        &self,
        write_txn: &redb::WriteTransaction,
        table: &mut redb::Table<'_, &[u8], &[u8]>,
        record: &VectorPartitionMembershipRecord,
    ) -> Result<()> {
        let key = Self::vector_partition_membership_key(record);
        let existing = table
            .get(key.as_slice())
            .map_err(Self::storage_error)?
            .map(|value| {
                self.decode_vector_partition_record::<VectorPartitionMembershipRecord>(
                    VectorPartitionRecordKind::Membership,
                    value.value(),
                )
            })
            .transpose()?;
        if let Some(existing) = existing {
            if Self::vector_partition_membership_key(&existing) != key || existing != *record {
                return Err(self.vector_partition_corruption(
                    "conflicting membership record at one durable identity",
                ));
            }
            let encoded = Self::encode_vector_partition_record(
                VectorPartitionRecordKind::Membership,
                record,
            )?;
            self.write_open_directory_entry(write_txn, &key, Some(&encoded))?;
            return Ok(());
        }
        self.insert_vector_partition_record(
            table,
            &key,
            VectorPartitionRecordKind::Membership,
            record,
        )?;
        let encoded =
            Self::encode_vector_partition_record(VectorPartitionRecordKind::Membership, record)?;
        self.write_open_directory_entry(write_txn, &key, Some(&encoded))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn ensure_open_vector_directory_in_write(
        write: &redb::WriteTransaction,
    ) -> Result<()> {
        let mut directory = write
            .open_table(VECTOR_OPEN_DIRECTORY_TABLE)
            .map_err(Self::storage_error)?;
        if directory
            .get(VECTOR_OPEN_DIRECTORY_ROOT_KEY)
            .map_err(Self::storage_error)?
            .is_some()
        {
            return Ok(());
        }
        if !directory.is_empty().map_err(Self::storage_error)? {
            return Err(Error::StoreCorrupted {
                path: "vector_open_directory".into(),
                reason: "non-empty membership directory has no root".into(),
            });
        }
        directory
            .insert(VECTOR_OPEN_DIRECTORY_ROOT_KEY, &[0_u8; 40][..])
            .map_err(Self::storage_error)?;
        drop(directory);
        content_identity::SetCommitment::clear_in_write(
            write,
            content_identity::CompactIndexSet::VectorDirectory,
        )?;
        Self::write_open_index_commitment(write, VECTOR_OPEN_DIRECTORY_COMMITMENT_KEY, &[0_u8; 40])
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn write_open_directory_entry(
        &self,
        write_txn: &redb::WriteTransaction,
        key: &[u8],
        membership: Option<&[u8]>,
    ) -> Result<()> {
        self.write_open_directory_entry_with_pending_transition(write_txn, key, membership, true)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn write_open_directory_entry_with_pending_transition(
        &self,
        write_txn: &redb::WriteTransaction,
        key: &[u8],
        membership: Option<&[u8]>,
        pending_transition: bool,
    ) -> Result<()> {
        self.write_open_directory_entry_impl::<false>(
            write_txn,
            key,
            membership,
            pending_transition,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn write_open_directory_entry_impl<const CORRUPT_STORE: bool>(
        &self,
        write_txn: &redb::WriteTransaction,
        key: &[u8],
        membership: Option<&[u8]>,
        pending_transition: bool,
    ) -> Result<()> {
        let _writer_scope = self.writer_decode_scope()?;
        let decoded = crate::read_image_memory::active_owner()?.row_owner()?;
        let _record_scope = decoded.enter();
        // No decoded membership escapes this mutation. Release its scratch
        // before publishing the next member of a batch.

        self.prepare_proof_owner()?;
        #[cfg(any(test, feature = "test-seams"))]
        if CORRUPT_STORE {
            vector_membership_pages::publish_corrupt_store_membership_for_test(
                write_txn,
                key,
                membership,
                self.proof_accountant(),
                pending_transition,
            )?;
        }
        if !CORRUPT_STORE {
            vector_membership_pages::publish_membership(
                write_txn,
                key,
                membership,
                self.proof_accountant(),
                pending_transition,
            )?;
        }
        let mut directory = write_txn
            .open_table(VECTOR_OPEN_DIRECTORY_TABLE)
            .map_err(Self::storage_error)?;
        let previous = directory
            .get(key)
            .map_err(Self::storage_error)?
            .map(|value| value.value().to_vec());
        let old_root = directory
            .get(VECTOR_OPEN_DIRECTORY_ROOT_KEY)
            .map_err(Self::storage_error)?
            .map(|value| value.value().to_vec())
            .unwrap_or_else(|| vec![0; 40]);
        if old_root.len() != 40 || previous.as_ref().is_some_and(|value| value.len() != 32) {
            return Err(Error::Other(
                "compact vector directory has invalid record size".into(),
            ));
        }
        let mut count = u64::from_be_bytes(old_root[..8].try_into().unwrap());
        let mut xor: [u8; 32] = old_root[8..].try_into().unwrap();
        // The committed family root binds the complete membership set through
        // this running commitment; only the record changed here is folded in.
        let mut set = content_identity::SetCommitment::load_in_write(
            write_txn,
            content_identity::CompactIndexSet::VectorDirectory,
        )?;
        if let Some(previous) = previous.as_ref() {
            for (dst, src) in xor.iter_mut().zip(previous) {
                *dst ^= src;
            }
            count = count
                .checked_sub(1)
                .ok_or_else(|| Error::Other("compact vector directory count underflow".into()))?;
            set.remove(
                key,
                previous
                    .as_slice()
                    .try_into()
                    .expect("checked witness size"),
            )?;
        }
        if let Some(membership) = membership {
            let mut hasher = blake3::Hasher::new();
            hasher.update(key);
            hasher.update(membership);
            let digest = hasher.finalize();
            for (dst, src) in xor.iter_mut().zip(digest.as_bytes()) {
                *dst ^= src;
            }
            count = count
                .checked_add(1)
                .ok_or_else(|| Error::Other("compact vector directory count overflow".into()))?;
            set.insert(key, digest.as_bytes())?;
            directory
                .insert(key, digest.as_bytes().as_slice())
                .map_err(Self::storage_error)?;
        } else {
            directory.remove(key).map_err(Self::storage_error)?;
        }
        set.store_in_write(write_txn)?;
        #[cfg(any(test, feature = "test-seams"))]
        content_identity::observe_vector_directory_binding_records(1);
        let mut root = [0; 40];
        root[..8].copy_from_slice(&count.to_be_bytes());
        root[8..].copy_from_slice(&xor);
        directory
            .insert(VECTOR_OPEN_DIRECTORY_ROOT_KEY, root.as_slice())
            .map_err(Self::storage_error)?;
        Self::write_open_index_commitment(write_txn, VECTOR_OPEN_DIRECTORY_COMMITMENT_KEY, &root)?;
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn write_open_index_commitment(
        write_txn: &redb::WriteTransaction,
        key: &str,
        root: &[u8],
    ) -> Result<()> {
        write_txn
            .open_table(CONTENT_IDENTITY_METADATA_TABLE)
            .map_err(Self::storage_error)?
            .insert(key, blake3::hash(root).as_bytes().as_slice())
            .map_err(Self::storage_error)?;
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn write_vector_body_root_fingerprint(
        write_txn: &redb::WriteTransaction,
    ) -> Result<()> {
        let fingerprint = write_txn
            .table_root_fingerprint(VECTORS_TABLE)
            .map_err(Self::storage_error)?
            .unwrap_or([0; 32]);
        let mut metadata = write_txn
            .open_table(CONTENT_IDENTITY_METADATA_TABLE)
            .map_err(Self::storage_error)?;
        let unchanged = metadata
            .get(VECTOR_BODY_ROOT_FINGERPRINT_KEY)
            .map_err(Self::storage_error)?
            .is_some_and(|old| old.value() == fingerprint);
        if !unchanged {
            metadata
                .insert(VECTOR_BODY_ROOT_FINGERPRINT_KEY, fingerprint.as_slice())
                .map_err(Self::storage_error)?;
        }
        drop(metadata);
        vector_membership_pages::write_fingerprints(write_txn)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn write_read_open_config_entry(
        write_txn: &redb::WriteTransaction,
        key: &str,
        value: Option<&[u8]>,
    ) -> Result<()> {
        Self::write_compact_config_entry(
            write_txn,
            READ_OPEN_CONFIG_TABLE,
            READ_OPEN_CONFIG_COMMITMENT_KEY,
            None,
            key,
            value,
        )
    }

    /// Measurement only: submissions re-verified by custody pending
    /// maintenance so far, process-wide.
    #[cfg(feature = "test-seams")]
    pub(crate) fn custody_pending_rechecked_for_test() -> u64 {
        custody_pending::rechecked_for_test()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn write_read_lineage_config_entry(
        write_txn: &redb::WriteTransaction,
        key: &str,
        value: Option<&[u8]>,
    ) -> Result<()> {
        erasure_frontiers::fold_lineage(write_txn, key, value)?;
        Self::write_compact_config_entry(
            write_txn,
            READ_LINEAGE_CONFIG_TABLE,
            READ_LINEAGE_CONFIG_COMMITMENT_KEY,
            Some(content_identity::CompactIndexSet::LineageIndex),
            key,
            value,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn write_read_custody_config_entry(
        write_txn: &redb::WriteTransaction,
        key: &str,
        value: Option<&[u8]>,
        funding: custody_groups::Funding<'_>,
        defer_pending: bool,
        verification: &custody_pending::Owner,
    ) -> Result<()> {
        // A write that restates the stored bytes (the authority certificate
        // every delivery restates) changes no group membership, no pending
        // verdict and no commitment: skip all three. Without this, every
        // restated certificate re-verified every current local root.
        {
            let table = write_txn
                .open_table(READ_CUSTODY_CONFIG_TABLE)
                .map_err(Self::storage_error)?;
            let stored = table.get(key).map_err(Self::storage_error)?;
            let unchanged = match (&stored, value) {
                (Some(stored), Some(value)) => stored.value() == value,
                (None, None) => true,
                _ => false,
            };
            if unchanged {
                return Ok(());
            }
        }
        custody_groups::maintain(write_txn, key, value, funding)?;
        custody_pending::maintain(write_txn, key, value, defer_pending, verification)?;
        Self::write_compact_config_entry(
            write_txn,
            READ_CUSTODY_CONFIG_TABLE,
            READ_CUSTODY_CONFIG_COMMITMENT_KEY,
            Some(content_identity::CompactIndexSet::CustodyIndex),
            key,
            value,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn write_compact_config_entry(
        write_txn: &redb::WriteTransaction,
        definition: TableDefinition<&str, &[u8]>,
        commitment_key: &str,
        bound_set: Option<content_identity::CompactIndexSet>,
        key: &str,
        value: Option<&[u8]>,
    ) -> Result<()> {
        let mut table = write_txn
            .open_table(definition)
            .map_err(Self::storage_error)?;
        let root_guard = table
            .get(READ_OPEN_CONFIG_ROOT_KEY)
            .map_err(Self::storage_error)?
            .ok_or_else(|| Error::Other("read-open config index root is absent".into()))?;
        if root_guard.value().len() != 40 {
            return Err(Error::Other(
                "read-open config index root has invalid size".into(),
            ));
        }
        let mut count = u64::from_be_bytes(root_guard.value()[..8].try_into().unwrap());
        let mut xor: [u8; 32] = root_guard.value()[8..].try_into().unwrap();
        drop(root_guard);
        let digest = |bytes: &[u8]| {
            let mut hasher = blake3::Hasher::new();
            hasher.update(key.as_bytes());
            hasher.update(bytes);
            hasher.finalize()
        };
        // A copy bound to a committed family root folds only this record's
        // change into its running set commitment.
        let mut set = bound_set
            .map(|set| content_identity::SetCommitment::load_in_write(write_txn, set))
            .transpose()?;
        if let Some(previous) = table.get(key).map_err(Self::storage_error)? {
            let witness = digest(previous.value());
            for (dst, src) in xor.iter_mut().zip(witness.as_bytes()) {
                *dst ^= src;
            }
            count = count
                .checked_sub(1)
                .ok_or_else(|| Error::Other("read-open config index count underflow".into()))?;
            if let Some(set) = set.as_mut() {
                set.remove(key.as_bytes(), witness.as_bytes())?;
            }
        }
        if let Some(value) = value {
            let witness = digest(value);
            for (dst, src) in xor.iter_mut().zip(witness.as_bytes()) {
                *dst ^= src;
            }
            count = count
                .checked_add(1)
                .ok_or_else(|| Error::Other("read-open config index count overflow".into()))?;
            if let Some(set) = set.as_mut() {
                set.insert(key.as_bytes(), witness.as_bytes())?;
            }
            table.insert(key, value).map_err(Self::storage_error)?;
        } else {
            table.remove(key).map_err(Self::storage_error)?;
        }
        if let Some(set) = set {
            set.store_in_write(write_txn)?;
            #[cfg(any(test, feature = "test-seams"))]
            content_identity::observe_custody_binding_records(1);
        }
        let mut root = [0_u8; 40];
        root[..8].copy_from_slice(&count.to_be_bytes());
        root[8..].copy_from_slice(&xor);
        table
            .insert(READ_OPEN_CONFIG_ROOT_KEY, root.as_slice())
            .map_err(Self::storage_error)?;
        Self::write_open_index_commitment(write_txn, commitment_key, &root)?;
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from transaction_repair.rs"
        )
    )]
    pub(crate) fn rebuild_read_open_config_in_write(
        write_txn: &redb::WriteTransaction,
    ) -> Result<()> {
        match write_txn.delete_table(READ_OPEN_CONFIG_TABLE) {
            Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(error) => return Err(Self::storage_error(error)),
        }
        write_txn
            .open_table(READ_OPEN_CONFIG_TABLE)
            .map_err(Self::storage_error)?
            .insert(READ_OPEN_CONFIG_ROOT_KEY, &[0_u8; 40][..])
            .map_err(Self::storage_error)?;
        Self::write_open_index_commitment(write_txn, READ_OPEN_CONFIG_COMMITMENT_KEY, &[0_u8; 40])?;
        let config = match write_txn.open_table(CONFIG_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
            Err(error) => return Err(Self::storage_error(error)),
        };
        for entry in config.iter().map_err(Self::storage_error)? {
            let (key, value) = entry.map_err(Self::storage_error)?;
            if read_image_open_config_key(key.value()) {
                Self::write_read_open_config_entry(write_txn, key.value(), Some(value.value()))?;
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from erasure_frontier_contract_tests.rs"
        )
    )]
    pub(crate) fn rebuild_read_lineage_config_in_write(
        write_txn: &redb::WriteTransaction,
    ) -> Result<()> {
        match write_txn.delete_table(READ_LINEAGE_CONFIG_TABLE) {
            Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(error) => return Err(Self::storage_error(error)),
        }
        content_identity::SetCommitment::clear_in_write(
            write_txn,
            content_identity::CompactIndexSet::LineageIndex,
        )?;
        write_txn
            .open_table(READ_LINEAGE_CONFIG_TABLE)
            .map_err(Self::storage_error)?
            .insert(READ_OPEN_CONFIG_ROOT_KEY, &[0_u8; 40][..])
            .map_err(Self::storage_error)?;
        Self::write_open_index_commitment(
            write_txn,
            READ_LINEAGE_CONFIG_COMMITMENT_KEY,
            &[0_u8; 40],
        )?;
        let config = match write_txn.open_table(CONFIG_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
            Err(error) => return Err(Self::storage_error(error)),
        };
        let start = crate::database::Database::DURABLE_LINEAGE_CONFIG_PREFIX
            .min(crate::database::Database::DURABLE_PURGED_LINEAGE_HISTORY_PREFIX)
            .min("sync_lineage.v1.pending.");
        for entry in config.range(start..).map_err(Self::storage_error)? {
            let (key, value) = entry.map_err(Self::storage_error)?;
            if !key.value().starts_with("sync_lineage.v1.") {
                break;
            }
            if read_image_lineage_config_key(key.value()) {
                Self::write_read_lineage_config_entry(write_txn, key.value(), Some(value.value()))?;
            }
        }
        Ok(())
    }

    /// Rebuild the custody read mirror and its derived group index from the
    /// committed CONFIG journal. The index is rebuilt complete, so a repaired
    /// store keeps durable custody reads instead of losing them.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from custody_source.rs"
        )
    )]
    pub(crate) fn rebuild_read_custody_config_in_write(
        write_txn: &redb::WriteTransaction,
        admission: &mut content_identity::ProofAdmission,
    ) -> Result<()> {
        custody_groups::reset_complete(write_txn)?;
        match write_txn.delete_table(READ_CUSTODY_CONFIG_TABLE) {
            Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(error) => return Err(Self::storage_error(error)),
        }
        content_identity::SetCommitment::clear_in_write(
            write_txn,
            content_identity::CompactIndexSet::CustodyIndex,
        )?;
        write_txn
            .open_table(READ_CUSTODY_CONFIG_TABLE)
            .map_err(Self::storage_error)?
            .insert(READ_OPEN_CONFIG_ROOT_KEY, &[0_u8; 40][..])
            .map_err(Self::storage_error)?;
        Self::write_open_index_commitment(
            write_txn,
            READ_CUSTODY_CONFIG_COMMITMENT_KEY,
            &[0_u8; 40],
        )?;
        let config = match write_txn.open_table(CONFIG_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
            Err(error) => return Err(Self::storage_error(error)),
        };
        let verification = admission.verification_owner()?;
        for entry in config.iter().map_err(Self::storage_error)? {
            let (key, value) = entry.map_err(Self::storage_error)?;
            if crate::custody::records::owned_key(key.value()) {
                let held = admission.held();
                Self::write_read_custody_config_entry(
                    write_txn,
                    key.value(),
                    Some(value.value()),
                    custody_groups::Funding::Legacy(&mut *admission),
                    true,
                    &verification,
                )?;
                // The entry's reverse scratch is already dropped; return its
                // debit so the rebuild holds one entry, never the journal sum.
                admission.release(admission.held() - held);
            }
        }
        drop(config);
        // Pending membership is recomputed, and every exclusion verified, once
        // the whole journal is indexed.
        custody_pending::recompute_all(write_txn, &verification)
    }

    /// Rebuild after a bulk schema or repair rewrite. Ordinary row commits
    /// update only their changed membership keys above.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from transaction_repair.rs"
        )
    )]
    pub(crate) fn rebuild_open_vector_directory_in_write(
        &self,
        write_txn: &redb::WriteTransaction,
    ) -> Result<()> {
        let _decode_scope = self.writer_decode_scope()?;
        self.prepare_proof_owner()?;
        vector_membership_pages::reset_in_write(write_txn)?;
        match write_txn.delete_table(VECTOR_OPEN_DIRECTORY_TABLE) {
            Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(error) => return Err(Self::storage_error(error)),
        }
        content_identity::SetCommitment::clear_in_write(
            write_txn,
            content_identity::CompactIndexSet::VectorDirectory,
        )?;
        write_txn
            .open_table(VECTOR_OPEN_DIRECTORY_TABLE)
            .map_err(Self::storage_error)?
            .insert(VECTOR_OPEN_DIRECTORY_ROOT_KEY, &[0_u8; 40][..])
            .map_err(Self::storage_error)?;
        Self::write_open_index_commitment(
            write_txn,
            VECTOR_OPEN_DIRECTORY_COMMITMENT_KEY,
            &[0_u8; 40],
        )?;
        match write_txn.open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE) {
            Ok(memberships) => {
                for entry in memberships.iter().map_err(Self::storage_error)? {
                    let (key, value) = entry.map_err(Self::storage_error)?;
                    self.write_open_directory_entry(write_txn, key.value(), Some(value.value()))?;
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(error) => return Err(Self::storage_error(error)),
        }
        let meta = write_txn
            .open_table(META_TABLE)
            .map_err(Self::storage_error)?;
        for entry in meta.iter().map_err(Self::storage_error)? {
            let (key, value) = entry.map_err(Self::storage_error)?;
            let Some(table) = key.value().strip_prefix("table:") else {
                continue;
            };
            let _memory = self
                .proof_accountant()
                .map(|accountant| {
                    crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                        accountant,
                        value.value().len().saturating_mul(16).saturating_add(4096),
                        "vector",
                        "rebuild_membership_summary",
                        "Raise MEMORY_LIMIT to rebuild this vector membership route.",
                    )
                })
                .transpose()?;
            let (declaration, _) = Self::decode_table_meta_versioned(value.value())?;
            vector_membership_pages::ensure_declared_routes(
                write_txn,
                table,
                &declaration,
                self.proof_accountant(),
            )?;
        }
        drop(meta);
        vector_membership_pages::write_fingerprints(write_txn)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn tombstone_vector_partition_membership(
        &self,
        write_txn: &redb::WriteTransaction,
        memberships: &mut redb::Table<'_, &[u8], &[u8]>,
        tombstones: &mut redb::Table<'_, &[u8], &[u8]>,
        prepared: VectorPartitionMembershipRecord,
        deleted_tx: TxId,
        lsn: Lsn,
        cause: VectorPartitionTombstoneCause,
    ) -> Result<VectorPartitionTombstoneRecord> {
        let membership_key = Self::vector_partition_membership_key(&prepared);
        let existing = memberships
            .get(membership_key.as_slice())
            .map_err(Self::storage_error)?
            .map(|value| {
                self.decode_vector_partition_record::<VectorPartitionMembershipRecord>(
                    VectorPartitionRecordKind::Membership,
                    value.value(),
                )
            })
            .transpose()?;
        let Some(mut membership) = existing else {
            return Err(self.vector_partition_corruption(
                "a vector delete or move has no durable source membership",
            ));
        };
        if Self::vector_partition_membership_key(&membership) != membership_key {
            return Err(self.vector_partition_corruption(
                "membership key does not match its checksummed payload",
            ));
        }
        let mut live_membership = membership.clone();
        live_membership.deleted_tx = None;
        if live_membership != prepared {
            return Err(self.vector_partition_corruption(
                "durable source membership does not match the prepared row and vector",
            ));
        }
        if let Some(existing_deleted_tx) = membership.deleted_tx
            && existing_deleted_tx != deleted_tx
        {
            return Err(self
                .vector_partition_corruption("membership has conflicting deletion transactions"));
        }
        membership.deleted_tx = Some(deleted_tx);
        let encoded = Self::encode_vector_partition_record(
            VectorPartitionRecordKind::Membership,
            &membership,
        )?;
        memberships
            .insert(membership_key.as_slice(), encoded.as_slice())
            .map_err(Self::storage_error)?;
        self.write_open_directory_entry(write_txn, &membership_key, Some(&encoded))?;

        let tombstone = VectorPartitionTombstoneRecord {
            membership,
            deleted_tx,
            lsn,
            cause,
        };
        let tombstone_key = Self::vector_partition_tombstone_key(&tombstone);
        self.insert_vector_partition_record(
            tombstones,
            &tombstone_key,
            VectorPartitionRecordKind::Tombstone,
            &tombstone,
        )?;
        Ok(tombstone)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn append_vector_partition_journal(
        &self,
        journal: &mut redb::Table<'_, &[u8], &[u8]>,
        record: &VectorPartitionJournalRecord,
    ) -> Result<()> {
        let key = Self::vector_partition_journal_key(record);
        self.insert_vector_partition_record(
            journal,
            &key,
            VectorPartitionRecordKind::Journal,
            record,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn require_decode_original(
        &self,
    ) -> Result<crate::memory_accounting::StoreAccountantHandle> {
        let accountant = self.require_decode_accountant()?;
        self.proof_accountant_handle_for(&accountant)
            .ok_or_else(|| Error::Other("decode accountant has no original provenance".into()))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn require_decode_accountant(
        &self,
    ) -> Result<crate::memory_accounting::AccountantHandle> {
        if let Some(slot) = self.proof_accountant.get() {
            return Ok(slot.accountant());
        }
        let accountant = crate::read_image_memory::active_owner()?.accountant.clone();
        // OnceLock publishes only the winner; a racing unpublished original
        // drops normally, including each physical native control invoice.
        let _ = self
            .proof_accountant
            .set(ProofAccountantSlot::attached(accountant));
        Ok(self
            .proof_accountant
            .get()
            .expect("decode accountant published")
            .accountant())
    }

    /// A route's maintained pending counters as `(covered_tx, inserts,
    /// tombstones)`: the serving frontier's and, while a replacement builds,
    /// its candidate's.
    #[cfg(any(test, feature = "test-seams"))]
    #[allow(clippy::type_complexity)]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from vector_import_tail.rs"
        )
    )]
    pub(crate) fn vector_pending_counters_for_test(
        &self,
        partition: &VectorPartitionRef,
    ) -> Result<((TxId, u64, u64), Option<(TxId, u64, u64)>)> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let summary = vector_membership_pages::read_partition_summary(&read, partition)?;
            let counts = |counts: &vector_membership_pages::PendingCounts| {
                (counts.covered_tx, counts.pending_inserts, counts.tombstones)
            };
            Ok((
                counts(&summary.pending),
                summary.candidate_pending.as_ref().map(counts),
            ))
        })
    }

    /// Read routes install their reader owner before entering here, and a
    /// Database operation its own owner. A decode that arrives without one is
    /// writer-side work on this store: it is charged to the store's own
    /// writer accountant -- the declared MEMORY_LIMIT -- never to a private
    /// reader at the shipped default that no consumer declared.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reader_decode_scope(&self) -> Result<crate::read_image_memory::ImageMemoryScope> {
        if let Ok(owner) = crate::read_image_memory::active_owner() {
            // A request's per-call reader owner charges each decode call
            // separately; every other owner is entered as is.
            if owner.is_per_call() && owner.reader_budget().is_some() {
                return owner.enter_call();
            }
            return Ok(owner.enter());
        }
        // A maintenance route inspection is generation preparation: its
        // decodes are refused under that operation, like its page reads.
        if vector_membership_source::in_maintenance_inspection() {
            return Ok(crate::read_image_memory::ImageMemory::charging_for(
                self.require_decode_accountant()?,
                "vector_maintenance",
                "prepare_vector_generation",
                "Raise MEMORY_LIMIT before inspecting this maintained route.",
            )
            .enter());
        }
        crate::read_image_memory::ImageMemory::enter_writer_operation(
            self.require_decode_accountant()?,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn writer_decode_scope(&self) -> Result<crate::read_image_memory::ImageMemoryScope> {
        let accountant = self.require_decode_accountant()?;
        if let Ok(owner) = crate::read_image_memory::active_owner()
            && owner.funded_by_writer(&accountant)
        {
            return Ok(owner.enter());
        }
        crate::read_image_memory::ImageMemory::enter_writer_operation(accountant)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
        #[cfg(test)]
        let _purge_producer =
            content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::new::<T>(
                "RedbPersistence::encode",
            );
        bincode::serde::encode_to_vec(value, bincode::config::standard())
            .map_err(|err| Error::Other((format!("bincode encode error: {err}")).into()))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn encode_into<T: serde::Serialize>(value: &T, encoded: &mut Vec<u8>) -> Result<()> {
        encoded.clear();
        bincode::serde::encode_into_std_write(value, encoded, bincode::config::standard())
            .map(|_| ())
            .map_err(|err| Error::Other((format!("bincode encode error: {err}")).into()))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
        #[cfg(test)]
        let _purge_producer =
            content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::new::<T>(
                "RedbPersistence::decode",
            );
        let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
        let (value, _) = crate::read_image_memory::decode(&_decode_scope, bytes)?;
        Ok(value)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn decode_exact<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
        let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
        let owner = crate::read_image_memory::active_owner()?;
        let before = owner.held();
        let (value, consumed) = crate::read_image_memory::decode(&_decode_scope, bytes)?;
        if consumed != bytes.len() {
            drop(value);
            owner.release(owner.held() - before);
            return Err(Error::Other(
                ("bincode exact decode left trailing bytes".to_string()).into(),
            ));
        }
        Ok(value)
    }

    /// Decode one `TableMeta` blob, trying the CURRENT layout first and
    /// falling back to the exact `v1.0.0` layout on the SPECIFIC decode
    /// failure that layout produces. Returns whether the fallback fired so
    /// callers can refuse an incompatible store.
    ///
    /// bincode's struct-as-tuple encoding carries no field-count marker, so
    /// a `Vec<ColumnDef>`/`TableMeta` decoder that optimistically reads past
    /// its OWN declared trailing fields does not cleanly stop at "no more
    /// fields for me" — it keeps consuming bytes that actually belong to the
    /// NEXT `ColumnDef` (or the next `TableMeta` field), and only surfaces an
    /// error once one of those borrowed bytes fails to satisfy the type it's
    /// forced into (observed as `InvalidBooleanValue` when a borrowed length
    /// or variant-tag byte lands on a `bool` field). The current
    /// `TableMeta`/`ColumnDef` `Deserialize` impls' own trailing-field
    /// `unwrap_or_default()` therefore only protects a genuinely SHORTER
    /// CURRENT-shaped payload (e.g. one written before the most recent
    /// additive field) — never a payload from a release whose `ColumnDef`
    /// had fewer fields than today's, which is exactly the `v1.0.0` case
    /// this fallback exists for.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn decode_table_meta_versioned(bytes: &[u8]) -> Result<(TableMeta, bool)> {
        let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
        if bytes.starts_with(schema_binding::SCHEMA_MAGIC) {
            return Self::decode_persisted_schema(bytes).map(|(meta, _, _)| (meta, false));
        }
        match Self::decode::<TableMeta>(bytes) {
            Ok(meta) => Ok((meta, false)),
            Err(error)
                if crate::read_image_memory::is_memory_refusal(&error)
                    || crate::read_image_memory::is_missing_decode_owner(&error) =>
            {
                Err(error)
            }
            Err(_) => {
                let legacy: LegacyTableMetaV1 = Self::decode(bytes)?;
                crate::read_image_memory::reserve(
                    legacy
                        .columns
                        .len()
                        .saturating_mul(std::mem::size_of::<contextdb_core::ColumnDef>()),
                )?;
                Ok((legacy.into(), true))
            }
        }
    }

    // Classification belongs to the relational boundary. Decode admission and
    // recorded native-page refusals retain precedence over malformed bytes.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn relational_decode_error(path: &str, error: Error) -> Error {
        match error {
            Error::Other(reason) => crate::funded_error::corruption_display(
                crate::read_image_memory::active_owner()
                    .and_then(|owner| owner.diagnostic_budget()),
                path,
                reason,
            ),
            other => other,
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn relational_storage_error(path: &str, error: redb::StorageError) -> Error {
        let corrupted = matches!(&error, redb::StorageError::Corrupted(_));
        match Self::storage_error(error) {
            Error::Other(reason) if corrupted => crate::funded_error::corruption_display(
                crate::read_image_memory::active_owner()
                    .and_then(|owner| owner.diagnostic_budget()),
                path,
                reason,
            ),
            other => other,
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn relational_table_error(path: &str, error: redb::TableError) -> Error {
        match error {
            redb::TableError::Storage(error) => Self::relational_storage_error(path, error),
            redb::TableError::TableDoesNotExist(reason) => crate::funded_error::corruption_display(
                crate::read_image_memory::active_owner()
                    .and_then(|owner| owner.diagnostic_budget()),
                path,
                format_args!("required relational table is absent: {reason}"),
            ),
            other => Self::storage_error(other),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reserve_compaction_checkpoint_controls(
        accountant: crate::memory_accounting::AccountantHandle,
        table: Option<&str>,
        position: &[usize],
    ) -> Result<PersistenceBytes> {
        PersistenceBytes::try_new_for(
            accountant,
            512usize
                .saturating_add(table.map_or(0, |name| name.len().saturating_mul(2)))
                .saturating_add(
                    position
                        .len()
                        .saturating_mul(2 * std::mem::size_of::<usize>()),
                )
                .div_ceil(1024)
                .saturating_mul(1024),
            "storage_compaction",
            "checkpoint_controls",
            "Raise MEMORY_LIMIT to retain storage maintenance controls.",
        )
    }

    // The hook transports reservations and recovered page admissions as
    // memory refusals. Other root-binding failures retain the existing I/O
    // hook result and exact engine error in the per-batch recovery slot.
    pub(super) fn compaction_checkpoint_storage_error(
        error: Error,
        failure: &parking_lot::Mutex<Option<Error>>,
    ) -> redb::StorageError {
        match error {
            error @ Error::MemoryBudgetExceeded { .. } => {
                redb::StorageError::AdmissionRefused(Box::new(error))
            }
            error => {
                *failure.lock() = Some(error);
                redb::StorageError::Io(std::io::Error::other(
                    "storage root binding failed during compaction",
                ))
            }
        }
    }

    pub(super) fn scalar_storage_refusal(
        error: &redb::Error,
    ) -> Option<contextdb_core::read_memory::NativeRefusal> {
        let redb::Error::Native(redb::NativeColdReadError::AdmissionRefusedAt(owner)) = error
        else {
            return None;
        };
        owner.inspect_failure(|cause| {
            cause
                .and_then(|cause| {
                    cause.downcast_ref::<native_storage_admission::NativeStorageFailure>()
                })
                .and_then(|cause| match cause {
                    native_storage_admission::NativeStorageFailure::Refused(refusal) => {
                        Some(*refusal)
                    }
                    _ => None,
                })
        })
    }

    pub(super) fn recycle_storage_error(&self, error: impl Into<redb::Error>) -> Error {
        let error: redb::Error = error.into();
        if let Some(refusal) = Self::scalar_storage_refusal(&error) {
            return native_storage_admission::original_storage_error(
                native_storage_admission::NativeStorageFailure::Refused(refusal),
            );
        }
        // Existing allocating backend/recycle diagnostics stay unqualified.
        Error::StoreHandleRecycleFailed {
            path: self.path.display().to_string(),
            reason: error.to_string(),
        }
    }

    pub(crate) fn storage_error(err: impl Into<redb::Error>) -> Error {
        let err: redb::Error = err.into();
        // Preserve the actual native allocator facts before any owning formatter.
        if let redb::Error::Native(redb::NativeColdReadError::AllocatorFailed { site, layout }) =
            err
        {
            return Error::NativeEngineOther(
                contextdb_core::read_memory::NativeRefusal::AllocatorFailed { site, layout },
            );
        }
        // Native markers lend only their prepaid scalar diagnosis here.
        if let Some(refusal) = Self::scalar_storage_refusal(&err) {
            return native_storage_admission::original_storage_error(
                native_storage_admission::NativeStorageFailure::Refused(refusal),
            );
        }
        if let redb::Error::AdmissionRefused(error) = err {
            return match error.downcast::<Error>() {
                Ok(error) => *error,
                Err(error) => Error::Other((format!("storage admission refused: {error}")).into()),
            };
        }
        if let redb::Error::Corrupted(reason) = err {
            return Error::StoreCorrupted {
                path: "storage".into(),
                reason: reason.into(),
            };
        }
        Self::storage_message_error(err)
    }

    pub(crate) fn storage_error_funded(
        diagnostic: crate::funded_error::PreparedDiagnostic,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        err: impl Into<redb::Error>,
    ) -> crate::funded_error::OriginalError {
        let err: redb::Error = err.into();
        if matches!(
            &err,
            redb::Error::AdmissionRefused(_) | redb::Error::Corrupted(_)
        ) {
            return diagnostic.storage(err, original);
        }
        if let Some((path, reason)) = writer_file::take_reader_coordination_refusal() {
            return diagnostic.reader_coordination(path, reason, original);
        }
        if let redb::Error::Native(redb::NativeColdReadError::BackendIo(
            redb::NativeIoCause::DiskGrowthRefused {
                current_bytes,
                limit_bytes,
                ..
            },
        )) = &err
        {
            // Consume the legacy consumer signal, but certify only this returned cause.
            let _ = writer_file::take_disk_growth_refusal();
            let (current_bytes, limit_bytes) = (*current_bytes, *limit_bytes);
            return diagnostic.disk(current_bytes, limit_bytes, err, original);
        }
        if let Some(refusal) = writer_file::take_disk_growth_refusal() {
            // Keep the legacy disk category, but the TLS signal cannot certify
            // the allocation provenance of this independently returned failure.
            return diagnostic.disk(refusal.current_bytes, refusal.limit_bytes, err, original);
        }
        diagnostic.storage(err, original)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn storage_source_error(path: &str, err: impl Into<redb::Error>) -> Error {
        match Self::storage_error(err) {
            Error::StoreCorrupted { reason, .. } => crate::funded_error::corruption_display(
                crate::read_image_memory::active_owner()
                    .and_then(|owner| owner.diagnostic_budget()),
                path,
                reason,
            ),
            other => other,
        }
    }

    pub(super) fn storage_message_error(err: impl std::fmt::Display) -> Error {
        let message = err.to_string();
        if let Some((path, reason)) = writer_file::take_reader_coordination_refusal() {
            return Error::ReaderCoordinationUnavailable {
                operation: "storage reclamation".to_owned(),
                path: path.display().to_string(),
                reason,
            };
        }
        if let Some(refusal) = writer_file::take_disk_growth_refusal() {
            return Error::DiskBudgetExceeded {
                operation: "allocate storage pages".to_owned(),
                current_bytes: refusal.current_bytes,
                budget_limit_bytes: refusal.limit_bytes,
                hint:
                    "Reduce retained file-backed data or raise DISK_LIMIT before writing more data."
                        .to_owned(),
            };
        }
        Error::Other((format!("{STORAGE_IO_ERROR_PREFIX}{message}")).into())
    }

    pub(super) fn storage_open_error(path: &Path, err: redb::DatabaseError) -> Error {
        match err {
            redb::DatabaseError::DatabaseAlreadyOpen => Self::held_store_error(path),
            other => Self::storage_error(other),
        }
    }

    /// An open refused because somebody else holds this store, told WHO.
    ///
    /// The two holders a caller can be told about need different actions --
    /// wait for hydration to finish, or go and stop a writer -- so naming the
    /// wrong one sends the caller after something that will never happen.
    ///
    /// Reaching this refusal at all means the advisory companion beside the
    /// store was there for the taking, which a live writer never allows: a
    /// writer holds it for as long as it holds the store, and an opener that
    /// meets one is refused by name long before here. So ordinarily the
    /// holder is a direct reader, and the observed count is floored at one --
    /// the refusal itself proves a reader is there even if the scan raced its
    /// own best-effort breadcrumb.
    ///
    /// The exception is a store whose companion carries no published writer
    /// run at all. That record is durable and is never removed in the ordinary
    /// course, so its absence beside a store somebody is holding means the
    /// advisory file was taken away from under its holder and this opener
    /// created the replacement it now holds. The holder that lost it is a
    /// WRITER, and saying "held by readers" there invents readers nobody can
    /// find and sends the caller to wait for hydration that is not happening.
    pub(super) fn held_store_error(path: &Path) -> Error {
        Self::held_store_error_in_runtime_dir(path, None)
    }

    pub(super) fn held_store_error_in_runtime_dir(
        path: &Path,
        runtime_dir: Option<&Path>,
    ) -> Error {
        #[cfg(any(test, feature = "test-seams"))]
        read_persistence_test_scaffold::note_writer_admission_for_test(
            path,
            read_persistence_test_scaffold::WriterAdmissionEvent::Refused,
        );
        let (observed_direct_readers, verified_readers) = live_reader_holders(path, runtime_dir);
        if observed_direct_readers == 0
            && verified_readers.is_empty()
            && inspect_companion_record(path).is_err()
        {
            return Self::held_by_writer_error_from_record(path);
        }
        let detail = HeldByReadersDetail {
            observed_direct_readers: observed_direct_readers.max(1),
            verified_readers,
        };
        Error::ReadFailure(
            ReadFailure::held_by_readers(detail)
                .expect("held reader observation constructs a valid typed refusal"),
        )
    }

    /// A writer holds this store, named from the record it published beside
    /// the store. A store whose record cannot be read -- a companion this
    /// opener had to create for itself, because the advisory file was taken
    /// away -- still names the store and the holder's kind, which is the part
    /// the caller acts on.
    pub(crate) fn held_by_writer_error_from_record(path: &Path) -> Error {
        let process_id = inspect_companion_record(path)
            .ok()
            .map(|record| u64::from(record.fields.process_id));
        Error::ReadFailure(
            ReadFailure::new(
                ReadFailureKind::HeldByWriter,
                ReadFailureDetail::HeldByWriter(HeldByWriterDetail {
                    process_id,
                    store_path: path.display().to_string(),
                }),
            )
            .expect("held-by-writer carries the writer-contention detail"),
        )
    }

    /// The filesystem refused a writable open. That is never a statement
    /// about the store's contents, so it must never become a corruption
    /// verdict that tells the operator to run a repair. Live direct readers
    /// make it the typed reader refusal, carrying the observed count and the
    /// verified breadcrumbs; without one it stays an I/O refusal naming the
    /// exact operating-system reason.
    pub(super) fn permission_refused_open_error_in_runtime_dir(
        path: &Path,
        err: redb::DatabaseError,
        runtime_dir: Option<&Path>,
    ) -> Error {
        let (observed_direct_readers, verified_readers) = live_reader_holders(path, runtime_dir);
        if observed_direct_readers == 0 {
            return Self::storage_error(err);
        }
        let detail = HeldByReadersDetail {
            observed_direct_readers,
            verified_readers,
        };
        Error::ReadFailure(
            ReadFailure::held_by_readers(detail)
                .expect("held reader observation constructs a valid typed refusal"),
        )
    }

    /// Declared relational indexes whose physical table root differs from
    /// the root its producer witnessed. Constant work per index: one root
    /// comparison, no posting is read.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn index_root_damage(
        &self,
        indexes: &[(String, String)],
    ) -> Result<Vec<(String, String)>> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let mut damaged = Vec::new();
            for (table, index) in indexes {
                let name = Self::relational_index_table_name(table, index);
                match Self::verify_history_root_canonical(&read, &name) {
                    Ok(true) => {}
                    Ok(false) | Err(Error::StoreCorrupted { .. }) => {
                        damaged.push((table.clone(), index.clone()));
                    }
                    Err(other) => return Err(other),
                }
            }
            Ok(damaged)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn with_db<T>(&self, f: impl FnOnce(&redb::Database) -> Result<T>) -> Result<T> {
        // The common writer handle boundary covers commit, reopen loading and
        // maintenance decoders. An installed reader child keeps its own owner.
        let _decode_scope = self.reader_decode_scope()?;
        self.prepare_proof_owner()?;
        self.with_db_handle(f)
    }

    #[cfg(test)]
    pub(crate) fn with_db_original_funded<T>(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        f: impl FnOnce(&redb::Database) -> crate::funded_error::OriginalResult<T>,
    ) -> crate::funded_error::OriginalResult<T> {
        let diagnostic = crate::funded_error::OriginalError::prepare_route(original)?;
        let _decode_scope = self
            .reader_decode_scope()
            .map_err(|error| diagnostic.error(error))?;
        self.prepare_proof_owner()
            .map_err(|error| diagnostic.error(error))?;
        if redb::native_storage_active() {
            return self.with_db_handle_funded(original, f);
        }
        let scope =
            native_storage_admission::StorageAdmissionScope::writer_original_native(original)
                .map_err(crate::funded_error::OriginalError::native)?;
        scope.run_original(crate::funded_error::OriginalStoragePhase::Database, || {
            self.with_db_handle_funded(original, f)
        })
    }

    // Queue unwind custody is supplied by the event-bus frame OUTSIDE the
    // maintenance guard. Even an unwinding callback keeps its sticky cause and
    // actual Original alive until that frame's physical destruction.
    pub(super) fn with_db_queue_original<T>(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        cleanup: &mut crate::database::event_bus::OwnedQueueCommitCleanup,
        f: impl FnOnce(
            &redb::Database,
            &mut crate::database::event_bus::OwnedQueueCommitCleanup,
        ) -> crate::funded_error::OriginalResult<T>,
    ) -> crate::funded_error::OriginalResult<T> {
        let diagnostic = crate::funded_error::OriginalError::prepare_route(original)?;
        let _decode_scope = self
            .reader_decode_scope()
            .map_err(|error| diagnostic.error(error))?;
        self.prepare_proof_owner()
            .map_err(|error| diagnostic.error(error))?;
        // Preserve inherited-scope selection. The actual outer run_original
        // joins its own cause; no new scope, lookup or payer is fabricated here.
        if redb::native_storage_active() {
            return self.with_db_handle_funded(original, |db| f(db, cleanup));
        }
        let scope =
            native_storage_admission::StorageAdmissionScope::writer_original_native(original)
                .map_err(crate::funded_error::OriginalError::native)?;
        cleanup.storage_scope = Some(scope.retain_admission());
        scope.run_original(crate::funded_error::OriginalStoragePhase::Queue, || {
            self.with_db_handle_funded(original, |db| f(db, cleanup))
        })
    }

    pub(super) fn with_db_handle_funded<T>(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        f: impl FnOnce(&redb::Database) -> crate::funded_error::OriginalResult<T>,
    ) -> crate::funded_error::OriginalResult<T> {
        let diagnostic = crate::funded_error::OriginalError::prepare_route(original)?;
        #[cfg(test)]
        PERSISTENCE_OWNERSHIP_READS_FOR_TEST.with(|count| count.set(count.get() + 1));
        if let Some(error) = self.saved_export_abandonment_error.lock().as_ref() {
            // Inherited producer gap: its source/formatting has not been
            // preadmitted. Do not pretend the caller's grant paid these bytes.
            return Err(diagnostic.error(Error::Other(
                format!("saved generation abandonment failed: {error}").into(),
            )));
        }
        let companion = self
            .lock_file
            .read()
            .map_err(|error| diagnostic.storage(original, error))?;
        if companion.is_none() {
            return Err(diagnostic.error(Error::Other("database persistence is closed".into())));
        }
        let database = self
            .db
            .read()
            .map_err(|error| diagnostic.storage(original, error))?;
        let db = database.as_ref().ok_or_else(|| {
            diagnostic.error(Error::Other("database persistence is closed".into()))
        })?;
        f(db)
    }

    // Borrow the same open native handle for byte-only operations. No codec
    // owner is created here; the caller retains its actual read/page owner.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_db_handle<T>(
        &self,
        f: impl FnOnce(&redb::Database) -> Result<T>,
    ) -> Result<T> {
        self.with_db_handle_loan(|db| self.with_storage_admission(|| f(db)))
    }

    // The same open-handle loan, separated from native IO admission so a
    // byte guard may keep its own original receipts after IO completes.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_db_handle_loan<T>(
        &self,
        f: impl FnOnce(&redb::Database) -> Result<T>,
    ) -> Result<T> {
        #[cfg(test)]
        PERSISTENCE_OWNERSHIP_READS_FOR_TEST.with(|count| count.set(count.get() + 1));
        // Operations borrow shared handle ownership. A writer waiting inside
        // Redb must not hold an exclusive lock needed to begin an ordinary read.
        // Close/recycle still take both exclusive guards before replacing it.
        if let Some(error) = self.saved_export_abandonment_error.lock().as_ref() {
            return Err(Error::Other(
                (format!("saved generation abandonment failed: {error}")).into(),
            ));
        }
        let companion = self.lock_file.read().map_err(Self::storage_error)?;
        if companion.is_none() {
            return Err(Error::Other(
                ("database persistence is closed".to_string()).into(),
            ));
        }
        let database = self.db.read().map_err(Self::storage_error)?;
        let db = database
            .as_ref()
            .ok_or_else(|| Error::Other(("database persistence is closed".to_string()).into()))?;
        f(db)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_storage_admission<T>(
        &self,
        operation: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        // A caller's already installed capability owns native misses and any
        // pending map created here. Never replace its original payer.
        if redb::native_storage_active() {
            return operation();
        }
        // Decode-only read callers carry their operation child on the existing
        // image owner. Bridge that same child instead of selecting the writer.
        if let Ok(owner) = crate::read_image_memory::active_owner()
            && let Some(memory) = owner.reader_budget()
        {
            let storage_scope =
                native_storage_admission::StorageAdmissionScope::reader_operation(memory)
                    .map_err(native_storage_admission::original_storage_error)?;
            return storage_scope.run_original_unqualified(operation);
        }
        // Only unscoped internal maintenance/bootstrap uses the attach-time
        // writer handle. Owned publication installed its explicit original above.
        let original = self.proof_accountant.get().and_then(|slot| {
            let accountant = slot.accountant();
            slot.original_handle_for(&accountant)
                .filter(|handle| handle.is_writer())
        });
        if let Some(handle) = original {
            let original = crate::memory_accounting::reserve_storage_from_original_handle(&handle)?;
            let storage_scope =
                native_storage_admission::StorageAdmissionScope::writer_original(&original)?;
            storage_scope.run_original_unqualified(operation)
        } else {
            operation()
        }
    }
}
