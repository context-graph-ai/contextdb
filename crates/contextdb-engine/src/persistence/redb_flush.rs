use super::*;

impl RedbPersistence {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn replace_vector_partition_image_for_tables_in_write(
        &self,
        write_txn: &redb::WriteTransaction,
        image: &CompleteVectorPartitionImage,
        affected_tables: &HashSet<String>,
        accountant: crate::memory_accounting::AccountantHandle,
    ) -> Result<()> {
        self.prepare_vector_partition_image_replacement_in_write(
            write_txn,
            affected_tables,
            accountant,
        )?;
        if !image.memberships.is_empty() {
            let mut table = write_txn
                .open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE)
                .map_err(Self::storage_error)?;
            for record in &image.memberships {
                self.insert_vector_partition_membership(write_txn, &mut table, record)?;
            }
        }
        if !image.journal.is_empty() {
            let mut table = write_txn
                .open_table(VECTOR_PARTITION_JOURNAL_TABLE)
                .map_err(Self::storage_error)?;
            for record in &image.journal {
                self.append_vector_partition_journal(&mut table, record)?;
            }
        }
        if !image.tombstones.is_empty() {
            let mut table = write_txn
                .open_table(VECTOR_PARTITION_TOMBSTONE_TABLE)
                .map_err(Self::storage_error)?;
            for record in &image.tombstones {
                let key = Self::vector_partition_tombstone_key(record);
                self.insert_vector_partition_record(
                    &mut table,
                    &key,
                    VectorPartitionRecordKind::Tombstone,
                    record,
                )?;
            }
        }
        Self::ensure_open_vector_directory_in_write(write_txn)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn write_vector_partition_mutations_in_write(
        &self,
        write_txn: &redb::WriteTransaction,
        batch: &PreparedVectorPartitionMutationBatch,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let mut memberships = write_txn
            .open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE)
            .map_err(Self::storage_error)?;
        let mut journal = write_txn
            .open_table(VECTOR_PARTITION_JOURNAL_TABLE)
            .map_err(Self::storage_error)?;
        let mut tombstones = write_txn
            .open_table(VECTOR_PARTITION_TOMBSTONE_TABLE)
            .map_err(Self::storage_error)?;

        for (position, mutation) in batch.mutations.iter().enumerate() {
            let ordinal = u64::try_from(position)
                .ok()
                .and_then(|position| position.checked_mul(2))
                .ok_or_else(|| Error::Other("vector partition journal ordinal overflow".into()))?;
            match mutation {
                PreparedVectorPartitionMutation::Delete {
                    index,
                    before,
                    deleted_tx,
                } => {
                    let prepared = Self::membership_from_binding(index, before, None)?;
                    let tombstone = self.tombstone_vector_partition_membership(
                        write_txn,
                        &mut memberships,
                        &mut tombstones,
                        prepared,
                        *deleted_tx,
                        batch.commit_lsn,
                        VectorPartitionTombstoneCause::Delete,
                    )?;
                    self.append_vector_partition_journal(
                        &mut journal,
                        &VectorPartitionJournalRecord {
                            index: index.clone(),
                            partition_key: before.partition_key.clone(),
                            lsn: batch.commit_lsn,
                            ordinal,
                            change: VectorPartitionJournalChange::Tombstone(tombstone),
                        },
                    )?;
                }
                PreparedVectorPartitionMutation::Insert {
                    index,
                    after,
                    entry: _,
                } => {
                    let membership = Self::membership_from_binding(index, after, None)?;
                    self.insert_vector_partition_membership(
                        write_txn,
                        &mut memberships,
                        &membership,
                    )?;
                    self.append_vector_partition_journal(
                        &mut journal,
                        &VectorPartitionJournalRecord {
                            index: index.clone(),
                            partition_key: after.partition_key.clone(),
                            lsn: batch.commit_lsn,
                            ordinal,
                            change: VectorPartitionJournalChange::Upsert(membership),
                        },
                    )?;
                }
                PreparedVectorPartitionMutation::Move {
                    index,
                    before,
                    after,
                    moved_tx,
                } => {
                    let prepared = Self::membership_from_binding(index, before, None)?;
                    let tombstone = self.tombstone_vector_partition_membership(
                        write_txn,
                        &mut memberships,
                        &mut tombstones,
                        prepared,
                        *moved_tx,
                        batch.commit_lsn,
                        VectorPartitionTombstoneCause::Move,
                    )?;
                    self.append_vector_partition_journal(
                        &mut journal,
                        &VectorPartitionJournalRecord {
                            index: index.clone(),
                            partition_key: before.partition_key.clone(),
                            lsn: batch.commit_lsn,
                            ordinal,
                            change: VectorPartitionJournalChange::Tombstone(tombstone),
                        },
                    )?;

                    let membership = Self::membership_from_binding(index, after, None)?;
                    self.insert_vector_partition_membership(
                        write_txn,
                        &mut memberships,
                        &membership,
                    )?;
                    self.append_vector_partition_journal(
                        &mut journal,
                        &VectorPartitionJournalRecord {
                            index: index.clone(),
                            partition_key: after.partition_key.clone(),
                            lsn: batch.commit_lsn,
                            ordinal: ordinal + 1,
                            change: VectorPartitionJournalChange::Upsert(membership),
                        },
                    )?;
                }
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn flush_data(&self, ws: &WriteSet) -> Result<()> {
        self.flush_data_with_logs(ws, &[])
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn flush_data_with_logs(&self, ws: &WriteSet, change_log: &[ChangeLogEntry]) -> Result<()> {
        self.flush_data_with_logs_and_sink_events(
            ws,
            change_log,
            FlushDataOptions {
                local_erasure: None,
                sink_events: &[],
                trigger_audits: &[],
                schema_ddl: SchemaDdlPersistence {
                    event_bus: None,
                    trigger: None,
                },
                max_sink_queue_depth: usize::MAX,
                snapshots: FlushDataSnapshots::default(),
                received_schema: None,
                partition_mutations: None,
            },
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn flush_data_with_logs_and_sink_events(
        &self,
        ws: &WriteSet,
        change_log: &[ChangeLogEntry],
        options: FlushDataOptions<'_>,
    ) -> Result<()> {
        self.flush_data_with_precommit(ws, change_log, options, || Ok(()))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn flush_data_with_precommit(
        &self,
        ws: &WriteSet,
        change_log: &[ChangeLogEntry],
        options: FlushDataOptions<'_>,
        prepare_publication: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        let _decode_scope = self.writer_decode_scope()?;
        self.prepare_proof_owner()?;
        let deleted_rows_snapshot = options.snapshots.deleted_rows;
        let sink_events = options.sink_events;
        let trigger_audits = options.trigger_audits;
        let schema_ddl = options.schema_ddl;
        let max_sink_queue_depth = options.max_sink_queue_depth;
        let partition_mutations = options.partition_mutations;
        if let Some(received_schema) = options.received_schema {
            return self.flush_received_schema_stage(
                ws,
                change_log,
                received_schema,
                sink_events,
                trigger_audits,
                max_sink_queue_depth,
            );
        }
        let loaded_table_meta;
        let table_meta = match options.snapshots.table_meta {
            Some(table_meta) => table_meta,
            None => {
                loaded_table_meta = self.load_all_table_meta()?;
                &loaded_table_meta
            }
        };
        let write_set_has_vector_changes = !ws.vector_deletes.is_empty()
            || !ws.vector_inserts.is_empty()
            || !ws.vector_moves.is_empty();
        if write_set_has_vector_changes
            && partition_mutations.is_none_or(PreparedVectorPartitionMutationBatch::is_empty)
        {
            return Err(Error::Other(
                ("vector persistence requires a prepared partition mutation batch".to_string())
                    .into(),
            ));
        }
        let has_vector_changes = write_set_has_vector_changes
            || partition_mutations.is_some_and(|batch| !batch.is_empty());
        let vector_quantization = if has_vector_changes {
            Self::admitted_vector_quantization_map(table_meta, crate::read_image_memory::reserve)?
        } else {
            HashMap::new()
        };
        // Runtime rows deliberately retain no vector body, but the durable
        // F32 row value is the independent half of the direct-reader
        // divergence proof. Rejoin the staged vector with the exact row
        // occurrence while both are still inside this commit; quantized
        // columns continue to persist only their vector-store body.
        let f32_witness_owner = crate::read_image_memory::active_owner()?.row_owner()?;
        let f32_row_vectors =
            Self::f32_witness_index(ws, partition_mutations, &vector_quantization, |bytes| {
                f32_witness_owner.reserve(bytes)
            })?;
        let prepared_local_erasure_vector_generations = options
            .local_erasure
            .as_ref()
            .map(|erasure| self.prepare_authoritative_purge_vector_generations(erasure.projection))
            .transpose()?
            .unwrap_or_default();
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
#[cfg(any(test, feature = "test-seams"))]
let mut graph_publication_attempt = crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            let graph_commit_support=Self::capture_graph_commit_support(&write_txn,ws.commit_lsn)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut stage_started = std::time::Instant::now();
            // Physical row/index/log writes finish before proof construction.
            // Their transient working state leaves the stack here; the exact
            // manifests and mutation invoices still span proof publication.
            let (pending_manifests, identity_mutations_commitment_credits, identity_mutations) = (|| {
            let mut pending_manifests = relational_version_manifest::PendingManifestPublication::new(self.proof_admission());
            // Declared first, so dropped last: these charges outlive the mutations they pay for.
            crate::read_image_memory::reserve(std::mem::size_of::<historical_index_commitment::CommitmentCredit>())?;
            let mut identity_mutations_commitment_credits: Vec<historical_index_commitment::CommitmentCredit> = Vec::with_capacity(1);
            // These source entries are already known. Allocate their directory
            // once, rather than retaining a geometrically oversized directory
            // beside its full successor late in a large seed commit.
            let vector_identities = partition_mutations.map_or(0, |batch| {
                batch.mutations.iter().map(|mutation| match mutation {
                    PreparedVectorPartitionMutation::Delete { .. } => 1usize,
                    PreparedVectorPartitionMutation::Insert { .. } => 2,
                    PreparedVectorPartitionMutation::Move { .. } => 3,
                }).sum::<usize>()
            });
            let mut posting_identities = 0usize;
            let mut commitment_identities = 0usize;
            for (table, meta) in table_meta {
                let rows = ws.relational_inserts.iter().filter(|(name, _)| name == table).count()
                    .checked_add(ws.relational_deletes.iter().filter(|(name, ..)| name == table).count())
                    .ok_or_else(|| Error::Other("logical identity mutation count overflow".into()))?;
                if rows == 0 { continue; }
                // Declaration construction is temporary schema scratch, not
                // retained per-row state. Its owner dies after the Vec does.
                let scratch = crate::read_image_memory::active_owner()?.row_owner()?;
                scratch.reserve(meta.metadata_bytes().saturating_mul(4).saturating_add(4096))?;
                let declarations = Self::relational_index_declarations(meta);
                // The same changed rows also publish one historical commitment
                // per declared index. Those publications join this directory
                // after the postings, so include their slots before allocation.
                commitment_identities = commitment_identities.checked_add(declarations.len())
                    .ok_or_else(|| Error::Other("logical identity mutation count overflow".into()))?;
                posting_identities = rows.checked_mul(declarations.len())
                    .and_then(|count| posting_identities.checked_add(count))
                    .ok_or_else(|| Error::Other("logical identity mutation count overflow".into()))?;
            }
            let sync_identities = crate::composite_store::sync_source_lsn_update_cardinality(ws)
                .ok_or_else(|| Error::Other("logical identity mutation count overflow".into()))?;
            let identity_capacity = [
                ws.relational_deletes.len(), ws.relational_inserts.len(),
                ws.adj_deletes.len(), ws.adj_inserts.len(), vector_identities,
                posting_identities, sync_identities, commitment_identities, change_log.len(),
                // Each config write/delete emits one Configuration member, including
                // internal commit metadata appended to an otherwise row-only write.
                ws.config_writes.len(), ws.config_deletes.len(),
                usize::from(ws.commit_lsn.is_some() && Self::write_set_visibility_tx(ws).is_some()),
            ].into_iter().try_fold(0usize, |count, next| count.checked_add(next))
                .ok_or_else(|| Error::Other("logical identity mutation count overflow".into()))?;
            let mut identity_mutations = WriterMutationVector::with_capacity(identity_capacity)?;
            for (table, row_id, _) in &ws.relational_deletes {
                crate::read_image_memory::reserve(table.len().saturating_add(24))?;
                identity_mutations.push(content_identity::remove_row_mutation(table, *row_id))?;
            }
            for (table, row) in &ws.relational_inserts {
                identity_mutations.push(content_identity::admitted_row_mutation(
                    table,
                    row,
                    table_meta.get(table),
                )?)?;
            }
            for (source, edge_type, target, _) in &ws.adj_deletes {
                crate::read_image_memory::reserve(edge_type.len().saturating_add(56))?;
                identity_mutations.push(content_identity::remove_edge_mutation(*source, edge_type, *target))?;
            }
            for edge in &ws.adj_inserts {
                identity_mutations.push(content_identity::admitted_edge_mutation(edge)?)?;
            }
            if let Some(batch) = partition_mutations {
                for mutation in &batch.mutations {
                    match mutation {
                        PreparedVectorPartitionMutation::Delete { index, before, .. } => {
                            let quantization = vector_quantization
                                .get(index)
                                .copied()
                                .unwrap_or_default();
                            crate::read_image_memory::reserve(index.table.len().saturating_add(index.column.len()).saturating_add(32))?;
                            identity_mutations.push(content_identity::remove_vector_mutation(
                                index,
                                before.row.row_id,
                                quantization,
                            ))?;
                        }
                        PreparedVectorPartitionMutation::Insert { index, after, entry } => {
                            let entry = batch.resolve_insert(ws, *entry, index, after)?;
                            let quantization = vector_quantization
                                .get(index)
                                .copied()
                                .unwrap_or_default();
                            identity_mutations.extend(content_identity::admitted_vector_mutation_pair(entry, quantization)?)?;
                        }
                        PreparedVectorPartitionMutation::Move {
                            index,
                            before,
                            after,
                            moved_tx,
                        } => {
                            let quantization = vector_quantization
                                .get(index)
                                .copied()
                                .unwrap_or_default();
                            crate::read_image_memory::reserve(index.table.len().saturating_add(index.column.len()).saturating_add(32))?;
                            identity_mutations.push(content_identity::remove_vector_mutation(
                                index,
                                before.row.row_id,
                                quantization,
                            ))?;
                            let source_owner = crate::read_image_memory::active_owner()?.row_owner()?;
                            source_owner.reserve(index.table.len().saturating_add(index.column.len()).saturating_mul(2).saturating_add(32))?;
                            let mut moved = VectorEntry {
                                index: index.clone(),
                                row_id: after.row.row_id,
                                vector: Vec::new(),
                                created_tx: *moved_tx,
                                deleted_tx: None,
                                lsn: batch.commit_lsn,
                            };
                            if let PreparedVectorPartitionMutation::Move { before, .. } = mutation {
                                let vectors = content_size::open_vectors(&write_txn)
                                    .map_err(Self::storage_error)?;
                                let source_key = Self::vector_identity_key(
                                    index,
                                    before.row.row_id,
                                    before.vector_created_tx,
                                    before.vector_lsn,
                                );
                                let source = vectors
                                    .get(source_key.as_slice())
                                    .map_err(Self::storage_error)?
                                    .ok_or_else(|| {
                                        self.vector_partition_corruption(
                                            "a prepared move has no durable source vector",
                                        )
                                    })?;
                                moved.vector = source_owner.scope(|| Self::decode_vector_entry(source.value()))?.vector;
                            }
                            identity_mutations.extend(content_identity::admitted_vector_mutation_pair(&moved, quantization)?)?;
                        }
                    }
                }
            }

            // Exact durable F32 sources are held as identities and decoded row by row.

            // Postings and the row versions they project are committed to
            // separately and must agree: see `historical_index_commitment`.
            let mut commitments = historical_index_commitment::CommitmentBatch::for_writer()?;
            let mut relational_deletes_by_table = BTreeMap::<&str, Vec<(RowId, TxId)>>::new();
            for (table, row_id, deleted_tx) in &ws.relational_deletes {
                relational_deletes_by_table
                    .entry(table.as_str())
                    .or_default()
                    .push((*row_id, *deleted_tx));
            }
            for (table, deletes) in relational_deletes_by_table {
                let table_name = Self::rel_table_name(table);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut redb_table =
                    content_size::open_rows(&write_txn, table_def, table_meta.get(table))
                        .map_err(Self::storage_error)?;
                for (row_id, deleted_tx) in deletes {
                    let _row_scope = crate::read_image_memory::active_owner()?.row_owner()?.enter();
                    let mut encoded = Vec::new();
                    self.verify_relational_group_pending_in_write(&write_txn, table, row_id, &*redb_table, &pending_manifests.mutations)?;
                    if let Some(row) = deleted_rows_snapshot
                        .as_ref()
                        .and_then(|by_table| by_table.get(table))
                        .and_then(|by_row| by_row.get(&row_id))
                    {
                        if row.deleted_tx.is_some() {
                            continue;
                        }
                        let key = Self::rel_row_key(row);
                        let mut overlay = redb_table
                            .get(key.as_slice())
                            .map_err(Self::storage_error)?
                            .map(|stored| {
                                Self::decode_versioned_row(stored.value(), table_meta.get(table))
                            })
                            .transpose()?
                            .map(|stored| {
                                Self::f32_vectors_from_row(&stored, table_meta.get(table))
                            }).transpose()?
                            .unwrap_or_default();
                        self.overlay_f32_witnesses(&write_txn, &f32_row_vectors, table, row, &mut overlay, crate::read_image_memory::reserve, Self::decode_vector_entry)?;
                        Self::encode_versioned_row_with_f32_overlay_into(
                            row,
                            Some(deleted_tx),
                            table_meta.get(table),
                            Some(&overlay),
                            &mut encoded,
                        )?;
                        {
                            let predecessor = redb_table.get(key.as_slice()).map_err(Self::storage_error)?;
                            self.replace_relational_manifest_pending_in_write(&write_txn, table, key.as_slice(), predecessor.as_ref().map(|body| body.value()), Some(encoded.as_slice()), Some(&mut pending_manifests))?;
                        }
                        let keys = content_size::VersionLife::keys_of(redb_table.indexes(), row)?;
                        redb_table.insert_version(
                            &mut commitments,
                            key.as_slice(),
                            encoded.as_slice(),
                            &content_size::VersionLife {
                                keys: &keys,
                                created: row.created_tx,
                                deleted: Some(deleted_tx),
                            },
                        )?;
                        continue;
                    }
                    let (lower, upper) = Self::rel_row_key_range(row_id);
                    let latest_version = {
                        let mut range = redb_table
                            .range(lower.as_slice()..upper.as_slice())
                            .map_err(Self::storage_error)?;
                        range
                            .next_back()
                            .transpose()
                            .map_err(Self::storage_error)?
                            .map(|(key, value)| {
                                let row = Self::decode_versioned_row(
                                    value.value(),
                                    table_meta.get(table),
                                )?;
                                Ok::<_, Error>((key.value().to_vec(), row))
                            })
                            .transpose()?
                    };
                    let Some((key, mut row)) = latest_version else {
                        continue;
                    };
                    if row.row_id != row_id || row.deleted_tx.is_some() {
                        continue;
                    }
                    row.deleted_tx = Some(deleted_tx);
                    Self::encode_versioned_row_into(&row, table_meta.get(table), &mut encoded)?;
                    {
                        let predecessor = redb_table.get(key.as_slice()).map_err(Self::storage_error)?;
                        self.replace_relational_manifest_pending_in_write(&write_txn, table, key.as_slice(), predecessor.as_ref().map(|body| body.value()), Some(encoded.as_slice()), Some(&mut pending_manifests))?;
                    }
                    let keys = content_size::VersionLife::keys_of(redb_table.indexes(), &row)?;
                    redb_table.insert_version(
                        &mut commitments,
                        key.as_slice(),
                        encoded.as_slice(),
                        &content_size::VersionLife {
                            keys: &keys,
                            created: row.created_tx,
                            deleted: Some(deleted_tx),
                        },
                    )?;
                }
            }

            let _replacement_lookup_charge = self.proof_accountant().map(|accountant| {
                crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                    accountant, ws.relational_deletes.len().saturating_mul(128).saturating_add(64),
                    "writer_source", "replacement_lookup", "Raise MEMORY_LIMIT to prepare row replacement identities.",
                )
            }).transpose()?;
            let replaced_rows = ws.relational_deletes.iter()
                .map(|(table, row_id, _)| (table.as_str(), *row_id))
                .collect::<HashSet<_>>();
            let mut relational_inserts_by_table = BTreeMap::<&str, Vec<&VersionedRow>>::new();
            for (table, row) in &ws.relational_inserts {
                relational_inserts_by_table
                    .entry(table.as_str())
                    .or_default()
                    .push(row);
            }
            for (table, rows) in relational_inserts_by_table {
                let table_name = Self::rel_table_name(table);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut redb_table =
                    content_size::open_rows(&write_txn, table_def, table_meta.get(table))
                        .map_err(Self::storage_error)?;
                for row in rows {
                    let _row_scope = crate::read_image_memory::active_owner()?.row_owner()?.enter();
                    let mut encoded = Vec::new();
                    // An ordinary update reopens with body-free runtime rows
                    // and need not stage an unchanged vector again. Carry the
                    // independent F32 witness from the exact prior row id into
                    // its successor; explicitly staged vectors win when the
                    // update did change a vector.
                    let replaces_same_row = replaced_rows.contains(&(table, row.row_id));
                    let mut overlay = if replaces_same_row {
                        let (lower, upper) = Self::rel_row_key_range(row.row_id);
                        let mut range = redb_table
                            .range(lower.as_slice()..upper.as_slice())
                            .map_err(Self::storage_error)?;
                        range
                            .next_back()
                            .transpose()
                            .map_err(Self::storage_error)?
                            .map(|(_, stored)| {
                                Self::decode_versioned_row(
                                    stored.value(),
                                    table_meta.get(table),
                                )
                            })
                            .transpose()?
                            .map(|stored| {
                                Self::f32_vectors_from_row(&stored, table_meta.get(table))
                            }).transpose()?
                            .unwrap_or_default()
                    } else {
                        HashMap::new()
                    };
                    // A clear removes the prior body's witness from this
                    // successor. A replacement may stage a new body below.
                    overlay.retain(|column, _| {
                        !ws.vector_deletes.iter().any(|(index, deleted_row, _)| {
                            index.table == table
                                && *deleted_row == row.row_id
                                && index.column == *column
                        })
                    });
                    self.overlay_f32_witnesses(&write_txn, &f32_row_vectors, table, row, &mut overlay, crate::read_image_memory::reserve, Self::decode_vector_entry)?;
                    Self::encode_versioned_row_with_f32_overlay_into(
                        row,
                        row.deleted_tx,
                        table_meta.get(table),
                        Some(&overlay),
                        &mut encoded,
                    )?;
                    let key = Self::rel_row_key(row);
                    {
                        let predecessor = redb_table.get(key.as_slice()).map_err(Self::storage_error)?;
                        self.replace_relational_manifest_pending_in_write(&write_txn, table, key.as_slice(), predecessor.as_ref().map(|body| body.value()), Some(encoded.as_slice()), Some(&mut pending_manifests))?;
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

            identity_mutations.extend(Self::flush_relational_index_changes_in_write(
                &write_txn,
                ws,
                table_meta,
                deleted_rows_snapshot.as_ref(),
                &mut commitments,
            )?)?;
            let (published, credit) = commitments.finish(&write_txn)?.into_parts();
            identity_mutations_commitment_credits.push(credit);
            identity_mutations.extend(published)?;

            let (clear_source_lsns, set_source_lsns) = sync_source_lsn_updates(ws);
            if !clear_source_lsns.is_empty() || !set_source_lsns.is_empty() {
                let mut source_lsn_table = write_txn
                    .open_table(SYNC_ROW_SOURCE_LSN_TABLE)
                    .map_err(Self::storage_error)?;
                let mut source_kind_table = write_txn
                    .open_table(SYNC_ROW_SOURCE_KIND_TABLE)
                    .map_err(Self::storage_error)?;
                for (table, row_id) in clear_source_lsns {
                    let key = Self::sync_row_source_lsn_key(&table, row_id);
                    source_lsn_table
                        .remove(key.as_slice())
                        .map_err(Self::storage_error)?;
                    source_kind_table
                        .remove(key.as_slice())
                        .map_err(Self::storage_error)?;
                    identity_mutations.push(content_identity::ContentIdentityMutation::remove(
                        content_identity::LogicalFamily::Sync,
                        key,
                    ))?;
                }
                for (table, row_id, source_lsn, kind) in set_source_lsns {
                    let key = Self::sync_row_source_lsn_key(&table, row_id);
                    source_lsn_table
                        .insert(key.as_slice(), source_lsn.0)
                        .map_err(Self::storage_error)?;
                    source_kind_table
                        .insert(key.as_slice(), kind)
                        .map_err(Self::storage_error)?;
                    let mut value = source_lsn.0.to_le_bytes().to_vec();
                    value.push(kind);
                    identity_mutations.push(content_identity::ContentIdentityMutation::put(
                        content_identity::LogicalFamily::Sync,
                        key,
                        value,
                    ))?;
                }
            }

            // Retain only this write set's actually tombstoned predecessors,
            // independently of older imported tombstones or retired receipts.
            let selected_deletion_memory = crate::read_image_memory::ImageMemory::funded_for_original(
                self.require_decode_original()?, "writer_source", "graph_selected_deletions",
                "Raise MEMORY_LIMIT to retain this commit's selected graph versions.")?;
            type SelectedDeletion = Option<(Vec<u8>, TxId, (Lsn, TxId))>;
            selected_deletion_memory.reserve(
                ws.adj_deletes.len().saturating_mul(std::mem::size_of::<SelectedDeletion>()),
            )?;
            let mut selected_deletions: Vec<SelectedDeletion> =
                (0..ws.adj_deletes.len()).map(|_| None).collect();
            if !ws.adj_deletes.is_empty() || !ws.adj_inserts.is_empty() {
                let mut fwd_table = write_txn
                    .open_table(GRAPH_FWD_TABLE)
                    .map_err(Self::storage_error)?;
                let mut rev_table = write_txn
                    .open_table(GRAPH_REV_TABLE)
                    .map_err(Self::storage_error)?;

                let mut encoded = Vec::new();
                for (delete_index, (source, edge_type, target, deleted_tx)) in
                    ws.adj_deletes.iter().enumerate()
                {
                    let mut key_charge = self.proof_accountant().map(|accountant| {
                        crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                            accountant, (33 + edge_type.len()).saturating_mul(2), "writer_source",
                            "graph_delete_keys", "Raise MEMORY_LIMIT to prepare the selected edge identity.",
                        )
                    }).transpose()?;
                    let mut prefix = Vec::with_capacity(33 + edge_type.len());
                    prefix.extend_from_slice(source.as_bytes());
                    prefix.extend_from_slice(target.as_bytes());
                    prefix.extend_from_slice(edge_type.as_bytes());
                    prefix.push(0);
                    let upper = writer_source::prefix_end(&prefix);
                    let mut after = None::<Vec<u8>>;
                    let mut found = false;
                    loop {
                        let lower = after.as_deref().map_or(
                            std::ops::Bound::Included(prefix.as_slice()),
                            std::ops::Bound::Excluded,
                        );
                        let upper = upper.as_deref().map_or(
                            std::ops::Bound::Unbounded,
                            std::ops::Bound::Excluded,
                        );
                        // End the range borrow before changing either direction.
                        // Only this version's key/body is live in scratch.
                        let memory = self.proof_accountant().map(|accountant| {
                            crate::read_image_memory::ImageMemory::charging_for(accountant, "writer_source",
                                "graph_delete_window", "Raise MEMORY_LIMIT to decode the selected edge version.")
                        });
                        let next = {
                            let mut range = fwd_table.range::<&[u8]>((lower, upper))
                                .map_err(Self::storage_error)?.inspect(|_entry| { #[cfg(any(test, feature = "test-seams"))] if let Ok((key, _)) = _entry { crate::read_work_probe::graph_storage_key("graph_fwd", "iteration_step", key.value()); } });
                            range.next().transpose().map_err(Self::storage_error)?
                                .map(|(key, value)| {
                                    if let Some(memory) = &memory {
                                        memory.reserve(key.value().len().saturating_mul(2)
                                            .saturating_add(value.value().len().saturating_mul(2)).saturating_add(64))?;
                                    }
                                    #[cfg(any(test, feature = "test-seams"))]
                                    crate::writer_query_test_seams::note_graph_delete_decode();
                                    #[cfg(any(test, feature = "test-seams"))]
                                    crate::read_work_probe::note_graph("graph_payload", "payload_decode", key.value(), None, Some("ordinary_deletion"));
                                    let edge: AdjEntry = match &memory {
                                        Some(memory) => memory.scope(|| Self::decode(value.value())),
                                        None => Self::decode(value.value()),
                                    }?;
                                    Ok::<_, Error>((key.value().to_vec(), edge))
                                }).transpose()?
                        };
                        let Some((key, mut edge)) = next else { break; };
                        if edge.source != *source || edge.target != *target
                            || edge.edge_type != *edge_type {
                            return Err(crate::funded_error::corruption_text(self.require_decode_original().and_then(|original| original.read_budget()), self.path.display(), "graph posting is outside its edge identity".into()));
                        }
                        if edge.deleted_tx.is_none() {
                            found = true;
                            let order = (edge.lsn, edge.created_tx);
                            let selected = &mut selected_deletions[delete_index];
                            if selected.as_ref().is_none_or(|(_, _, prior)| order > *prior) {
                                if let Some((p, d, prior)) = selected {
                                    if p.len() != key.len() {
                                        return Err(crate::funded_error::corruption_text(self.require_decode_original().and_then(|original| original.read_budget()), self.path.display(), "selected graph predecessor width changed".into()));
                                    }
                                    p.copy_from_slice(&key);
                                    *d = *deleted_tx;
                                    *prior = order;
                                } else {
                                    selected_deletion_memory.reserve(key.len())?;
                                    *selected = Some((key.clone(), *deleted_tx, order));
                                }
                            }
                            edge.deleted_tx = Some(*deleted_tx);
                            let mut encoded = Vec::new();
                            Self::encode_into(&edge, &mut encoded)?;
                            let rev_key = Self::graph_rev_key(&edge);
                            fwd_table.insert(observed_graph_key!("graph_fwd", "insert", key.as_slice(), 5), encoded.as_slice())
                                .map_err(Self::storage_error)?;
                            rev_table.insert(observed_graph_key!("graph_rev", "insert", rev_key.as_slice(), 5), encoded.as_slice())
                                .map_err(Self::storage_error)?;
                            self.graph_writer_window(||Self::install_graph_creation_in_write(&write_txn, &edge, &encoded))?;
                        }
                        if let Some(charge) = &mut key_charge {
                            charge.try_grow_for(key.capacity(), "writer_source", "graph_delete_keys",
                                "Raise MEMORY_LIMIT to retain the selected edge resume key.")?;
                            if let Some(prior) = after.as_ref() { charge.try_shrink(prior.capacity())?; }
                        }
                        after = Some(key);
                    }
                    if !found {
                        return Err(Error::NotFound(format!(
                            "edge {source} -[{edge_type}]-> {target} in graph_fwd"
                        )));
                    }
                }

                for entry in &ws.adj_inserts {
                    Self::encode_into(entry, &mut encoded)?;
                    let fwd_key = Self::graph_fwd_key(entry);
                    let rev_key = Self::graph_rev_key(entry);
                    fwd_table
                        .insert(observed_graph_key!("graph_fwd", "insert", fwd_key.as_slice(), 5), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    self.graph_writer_window(||Self::install_graph_creation_in_write(&write_txn, entry, &encoded))?;
                    rev_table
                        .insert(observed_graph_key!("graph_rev", "insert", rev_key.as_slice(), 5), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            self.graph_writer_window(|| Self::apply_row_graph_in_write(&write_txn, ws))?;

            if has_vector_changes {
                let mut vectors_table = content_size::open_vectors(&write_txn)
                    .map_err(Self::storage_error)?;

                if let Some(batch) = partition_mutations {
                    for mutation in &batch.mutations {
                        let PreparedVectorPartitionMutation::Delete {
                            index,
                            before,
                            deleted_tx,
                        } = mutation
                        else {
                            continue;
                        };
                        let _vector_scope = crate::read_image_memory::active_owner()?.row_owner()?.enter();
                        let key = Self::vector_identity_key(
                            index,
                            before.row.row_id,
                            before.vector_created_tx,
                            before.vector_lsn,
                        );
                        let mut entry = {
                            let stored = vectors_table
                                .get(key.as_slice())
                                .map_err(Self::storage_error)?
                                .ok_or_else(|| {
                                    self.vector_partition_corruption(
                                        "a prepared delete has no durable source vector",
                                    )
                                })?;
                            Self::decode_vector_entry(stored.value())?
                        };
                        if entry.index != *index
                            || entry.row_id != before.row.row_id
                            || entry.created_tx != before.vector_created_tx
                            || entry.lsn != before.vector_lsn
                        {
                            return Err(self.vector_partition_corruption(
                                "a prepared delete source key does not match its vector payload",
                            ));
                        }
                        if let Some(existing_deleted_tx) = entry.deleted_tx
                            && existing_deleted_tx != *deleted_tx
                        {
                            return Err(self.vector_partition_corruption(
                                "a prepared delete source has a conflicting deletion transaction",
                            ));
                        }
                        entry.deleted_tx = Some(*deleted_tx);
                        let quantization = vector_quantization
                            .get(index)
                            .copied()
                            .unwrap_or_default();
                        let encoded = Self::encode_vector_entry_admitted(&entry, quantization)?;
                        vectors_table
                            .insert(key.as_slice(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }

                if let Some(batch) = partition_mutations {
                    for mutation in &batch.mutations {
                        let PreparedVectorPartitionMutation::Insert {
                            index,
                            after,
                            entry,
                        } = mutation
                        else {
                            continue;
                        };
                        let _vector_scope = crate::read_image_memory::active_owner()?.row_owner()?.enter();
                        let entry = batch.resolve_insert(ws, *entry, index, after)?;
                        if entry.index != *index
                            || entry.row_id != after.row.row_id
                            || entry.created_tx != after.vector_created_tx
                            || entry.lsn != after.vector_lsn
                            || entry.deleted_tx.is_some()
                        {
                            return Err(Error::Other((format!(
                                "prepared vector insert has inconsistent identity on {}.{}",
                                index.table, index.column
                            )).into()));
                        }
                        let quantization = vector_quantization
                            .get(index)
                            .copied()
                            .unwrap_or_default();
                        let encoded = Self::encode_vector_entry_admitted(entry, quantization)?;
                        let key = Self::vector_key(entry);
                        if let Some(existing) = vectors_table
                            .get(key.as_slice())
                            .map_err(Self::storage_error)?
                            && existing.value() != encoded.as_slice()
                        {
                            return Err(self.vector_partition_corruption(
                                "a prepared insert destination has conflicting vector bytes",
                            ));
                        }
                        vectors_table
                            .insert(key.as_slice(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }

                if let Some(batch) = partition_mutations {
                    for mutation in &batch.mutations {
                        let PreparedVectorPartitionMutation::Move {
                            index,
                            before,
                            after,
                            moved_tx,
                        } = mutation
                        else {
                            continue;
                        };
                        let _vector_scope = crate::read_image_memory::active_owner()?.row_owner()?.enter();
                        if after.vector_created_tx != *moved_tx
                            || after.vector_lsn != batch.commit_lsn
                        {
                            return Err(Error::Other((format!(
                                "prepared vector move has inconsistent destination identity on {}.{}",
                                index.table, index.column
                            )).into()));
                        }
                        let old_key = Self::vector_identity_key(
                            index,
                            before.row.row_id,
                            before.vector_created_tx,
                            before.vector_lsn,
                        );
                        let mut old_entry = {
                            let stored = vectors_table
                                .get(old_key.as_slice())
                                .map_err(Self::storage_error)?
                                .ok_or_else(|| {
                                    self.vector_partition_corruption(
                                        "a prepared move has no durable source vector",
                                    )
                                })?;
                            Self::decode_vector_entry(stored.value())?
                        };
                        if old_entry.index != *index
                            || old_entry.row_id != before.row.row_id
                            || old_entry.created_tx != before.vector_created_tx
                            || old_entry.lsn != before.vector_lsn
                        {
                            return Err(self.vector_partition_corruption(
                                "a prepared move source key does not match its vector payload",
                            ));
                        }
                        if let Some(existing_deleted_tx) = old_entry.deleted_tx
                            && existing_deleted_tx != *moved_tx
                        {
                            return Err(self.vector_partition_corruption(
                                "a prepared move source has a conflicting deletion transaction",
                            ));
                        }
                        old_entry.deleted_tx = Some(*moved_tx);
                        let quantization = vector_quantization
                            .get(index)
                            .copied()
                            .unwrap_or_default();
                        let old_encoded = Self::encode_vector_entry_admitted(&old_entry, quantization)?;
                        vectors_table
                            .insert(old_key.as_slice(), old_encoded.as_slice())
                            .map_err(Self::storage_error)?;

                        let mut new_entry = old_entry;
                        new_entry.row_id = after.row.row_id;
                        new_entry.created_tx = *moved_tx;
                        new_entry.deleted_tx = None;
                        new_entry.lsn = batch.commit_lsn;
                        let new_key = Self::vector_key(&new_entry);
                        let new_encoded = Self::encode_vector_entry_admitted(&new_entry, quantization)?;
                        if let Some(existing) = vectors_table
                            .get(new_key.as_slice())
                            .map_err(Self::storage_error)?
                            && existing.value() != new_encoded.as_slice()
                        {
                            return Err(self.vector_partition_corruption(
                                "a prepared move destination has conflicting vector bytes",
                            ));
                        }
                        vectors_table
                            .insert(new_key.as_slice(), new_encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }
            }

            if let Some(partition_mutations) = partition_mutations {
                self.write_vector_partition_mutations_in_write(
                    &write_txn,
                    partition_mutations,
                )?;
            }

            if let (Some(lsn), Some(tx)) = (ws.commit_lsn, Self::write_set_visibility_tx(ws)) {
                let mut table = write_txn
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?;
                table.insert(lsn.0, tx.0).map_err(Self::storage_error)?;
                identity_mutations.push(content_identity::commit_index_mutation(lsn, tx))?;
            }

            Self::stage_graph_commit_support(&write_txn,graph_commit_support)?;
            if !change_log.is_empty() {
                let mut table = write_txn
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                let mut encoded = Vec::new();
                let mut key = String::with_capacity(Self::change_log_entry_key_len());
                for (index, entry) in change_log.iter().enumerate() {
                    Self::write_change_log_entry_key(lsn, index, &mut key);
                    if let ChangeLogEntry::EdgeDelete { source, target, edge_type, .. } = entry {
                        let selected_index = ws.adj_deletes.iter().position(|(s, k, t, _)|
                            s == source && t == target && k == edge_type
                        ).ok_or_else(|| crate::funded_error::corruption_text(self.require_decode_original().and_then(|original| original.read_budget()), self.path.display(), "ordinary graph deletion has no selected write-set predecessor".into()))?;
                        let (p, d, _) = selected_deletions[selected_index].as_ref()
                            .ok_or_else(|| crate::funded_error::corruption_text(self.require_decode_original().and_then(|original| original.read_budget()), self.path.display(), "ordinary graph deletion selected predecessor is missing".into()))?;
                        self.encode_change_log_with_selected_predecessor_in_write(
                            &write_txn, entry, Some((p.as_slice(), *d)), &mut encoded,
                        )?;
                    } else {
                        self.encode_change_log_into_in_write(&write_txn, entry, &mut encoded)?;
                    }
                    table
                        .insert(observed_graph_key!("history", "insert", key.as_str(), 8), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    self.graph_writer_window(||Self::write_change_log_selectors_in_write(&write_txn, key.as_str(), encoded.as_slice()))?;
                    identity_mutations.push(Self::stored_change_log_mutation_in_write(&write_txn, &key)?)?;
                }
            }

            if !sink_events.is_empty() {
                let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                let mut overflow_audits = Vec::new();
                let mut by_sink: BTreeMap<&str, Vec<(usize, &SinkQueueEntry)>> = BTreeMap::new();
                for (index, (sink, entry)) in sink_events.iter().enumerate() {
                    by_sink
                        .entry(sink.as_str())
                        .or_default()
                        .push((index, entry));
                }
                for (sink, entries) in by_sink {
                    let table_name = Self::sink_queue_table_name(sink);
                    let table_def: TableDefinition<u64, &[u8]> =
                        TableDefinition::new(table_name.as_str());
                    let mut table = write_txn
                        .open_table(table_def)
                        .map_err(Self::storage_error)?;
                    let mut existing = table.len().map_err(Self::storage_error)? as usize;
                    for (index, entry) in entries {
                        if existing >= max_sink_queue_depth {
                            return Err(Error::Other((format!("sink queue is full: {sink}")).into()));
                        }
                        let overflow_id = if existing >= max_sink_queue_depth {
                            table
                                .first()
                                .map_err(Self::storage_error)?
                                .map(|(key, _)| key.value())
                        } else {
                            None
                        };
                        if let Some(id) = overflow_id {
                            table.remove(id).map_err(Self::storage_error)?;
                            existing = existing.saturating_sub(1);
                            overflow_audits.push((index, sink.to_string()));
                            let logical_key = format!("{}:{sink}:{id}", sink.len());
                            identity_mutations.push(content_identity::remove_status_mutation(
                                b"sink-queue",
                                logical_key.as_bytes(),
                            ))?;
                        }
                        let encoded = Self::encode(entry)?;
                        table
                            .insert(entry.id, encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        let logical_key = format!("{}:{sink}:{}", sink.len(), entry.id);
                        identity_mutations.push(content_identity::encoded_status_mutation(
                            b"sink-queue",
                            logical_key.as_bytes(),
                            entry,
                        )?)?;
                        existing = existing.saturating_add(1);
                    }
                }
                if !overflow_audits.is_empty() {
                    let mut audit_table = write_txn
                        .open_table(SINK_AUDIT_TABLE)
                        .map_err(Self::storage_error)?;
                    for (index, sink) in overflow_audits {
                        let key = Self::sink_audit_key(lsn, index, &sink);
                        let audit = SinkAuditEntry {
                            lsn,
                            kind: SinkAuditKind::QueueOverflow {
                                sink,
                                dropped_count: 1,
                            },
                        };
                        let encoded = Self::encode(&audit)?;
                        audit_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        identity_mutations.push(content_identity::status_mutation(
                            b"sink-audit",
                            key.as_bytes(),
                            &encoded,
                        ))?;
                    }
                }
            }

            if !trigger_audits.is_empty() {
                {
                    let mut audit_table = write_txn
                        .open_table(TRIGGER_AUDIT_TABLE)
                        .map_err(Self::storage_error)?;
                    let mut stamps = write_txn
                        .open_table(TRIGGER_AUDIT_STAMPS_TABLE)
                        .map_err(Self::storage_error)?;
                    let stamped_at = Wallclock::now().0;
                    for (index, entry) in trigger_audits {
                        let key = Self::trigger_audit_key(*index, &entry.trigger_name);
                        let encoded = Self::encode(entry)?;
                        audit_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        stamps
                            .insert(key.as_str(), stamped_at)
                            .map_err(Self::storage_error)?;
                        identity_mutations.push(content_identity::status_mutation(
                            b"trigger-audit",
                            key.as_bytes(),
                            &encoded,
                        ))?;
                        identity_mutations.push(content_identity::status_mutation(
                            b"trigger-stamp",
                            key.as_bytes(),
                            &stamped_at.to_le_bytes(),
                        ))?;
                    }
                }
                {
                    let mut config_table = write_txn
                        .open_table(CONFIG_TABLE)
                        .map_err(Self::storage_error)?;
                    let mut ring: Vec<TriggerAuditEntry> = config_table
                        .get(TRIGGER_AUDIT_RING_CONFIG_KEY)
                        .map_err(Self::storage_error)?
                        .map(|value| Self::decode(value.value()))
                        .transpose()?
                        .unwrap_or_default();
                    ring.extend(trigger_audits.iter().map(|(_, entry)| entry.clone()));
                    let overflow = ring
                        .len()
                        .saturating_sub(crate::database::trigger::TRIGGER_AUDIT_RING_CAPACITY);
                    if overflow > 0 {
                        ring.drain(0..overflow);
                    }
                    let current_next_index = config_table
                        .get(TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY)
                        .map_err(Self::storage_error)?
                        .map(|value| Self::decode::<u64>(value.value()))
                        .transpose()?
                        .unwrap_or(0);
                    let next_index = trigger_audits
                        .iter()
                        .map(|(index, _)| index.saturating_add(1))
                        .max()
                        .unwrap_or(0)
                        .max(current_next_index);
                    let encoded_ring = Self::encode(&ring)?;
                    config_table
                        .insert(TRIGGER_AUDIT_RING_CONFIG_KEY, encoded_ring.as_slice())
                        .map_err(Self::storage_error)?;
                    identity_mutations.extend(content_identity::config_mutations(
                        TRIGGER_AUDIT_RING_CONFIG_KEY,
                        Some(&encoded_ring),
                    ))?;
                    let encoded_next_index = Self::encode(&next_index)?;
                    config_table
                        .insert(
                            TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY,
                            encoded_next_index.as_slice(),
                        )
                        .map_err(Self::storage_error)?;
                    identity_mutations.extend(content_identity::config_mutations(
                        TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY,
                        Some(&encoded_next_index),
                    ))?;
                }
            }

            if schema_ddl.event_bus.is_some() || schema_ddl.trigger.is_some() {
                {
                    let mut config_table = write_txn
                        .open_table(CONFIG_TABLE)
                        .map_err(Self::storage_error)?;
                    if let Some(event_bus_ddl) = schema_ddl.event_bus {
                        for (key, encoded) in &event_bus_ddl.config_values {
                            config_table
                                .insert(key.as_str(), encoded.as_slice())
                                .map_err(Self::storage_error)?;
                            identity_mutations.extend(content_identity::config_mutations(
                                key,
                                Some(encoded),
                            ))?;
                        }
                    }
                    if let Some(trigger_ddl) = schema_ddl.trigger {
                        for (key, encoded) in &trigger_ddl.config_values {
                            config_table
                                .insert(key.as_str(), encoded.as_slice())
                                .map_err(Self::storage_error)?;
                            identity_mutations.extend(content_identity::config_mutations(
                                key,
                                Some(encoded),
                            ))?;
                        }
                    }
                }
                let ddl_entries = schema_ddl
                    .event_bus
                    .into_iter()
                    .flat_map(|commit| commit.ddl.iter())
                    .chain(
                        schema_ddl
                            .trigger
                            .into_iter()
                            .flat_map(|commit| commit.ddl.iter()),
                    )
                    .collect::<Vec<_>>();
                if !ddl_entries.is_empty() {
                    let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                    let mut ddl_table = write_txn
                        .open_table(DDL_LOG_TABLE)
                        .map_err(Self::storage_error)?;
                    for (index, change) in ddl_entries.iter().enumerate() {
                        let key = Self::ddl_log_key_for_index(lsn, index, ddl_entries.len());
                        self.append_ddl_summary_in_write(&write_txn, &key, change)?;
                        let encoded = Self::encode(change)?;
                        ddl_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        identity_mutations.push(content_identity::encoded_history_mutation(
                            &key, change,
                        )?)?;
                    }
                }
            }

            if !ws.config_writes.is_empty() || !ws.config_deletes.is_empty() {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for key in &ws.config_deletes {
                    Self::update_policy_hold_release_index(&write_txn, key, false)?;
                    config_table
                        .remove(key.as_str())
                        .map_err(Self::storage_error)?;
                    identity_mutations.extend(content_identity::config_mutations(key, None))?;
                }
                for (key, encoded) in &ws.config_writes {
                    Self::update_policy_hold_release_index(&write_txn, key, true)?;
                    let encoded = if ws.config_max_u64_keys.iter().any(|max_key| max_key == key) {
                        let incoming = Self::decode::<u64>(encoded)?;
                        let current = config_table
                            .get(key.as_str())
                            .map_err(Self::storage_error)?
                            .map(|value| Self::decode::<u64>(value.value()))
                            .transpose()?
                            .unwrap_or(0);
                        Self::encode(&current.max(incoming))?
                    } else {
                        encoded.clone()
                    };
                    config_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    identity_mutations.extend(content_identity::config_mutations(
                        key,
                        Some(&encoded),
                    ))?;
                }
            }

            if let Some(erasure) = options.local_erasure.as_ref() {
                self.apply_local_erasure(
                    &write_txn,
                    erasure.projection,
                    &prepared_local_erasure_vector_generations,
                    None,
                    erasure.accountant.clone(),
                    erasure.operation,
                    None,
                )?;
            }
            #[cfg(any(test, feature = "test-seams"))]
            {
                observe_append_stage(AppendStage::RowsIndexesAndLogs, &mut stage_started);
            }
            // Row persistence has consumed every F32 witness. Free its native
            // directory before returning its dedicated writer credit.
            drop(f32_row_vectors);
            drop(f32_witness_owner);
                Ok::<_, Error>((pending_manifests, identity_mutations_commitment_credits,
                    identity_mutations))
            })()?;
            Self::apply_content_identity_with_manifests_in_write(
                &write_txn,
                &identity_mutations,
                &pending_manifests.mutations,
                &mut self.proof_admission(),
            )?;
            drop(pending_manifests);
            // No proof workspace or source directory overlaps vector body copies.
            // Every fallible vector admission still precedes write_txn.commit.
            drop(identity_mutations);
            drop(identity_mutations_commitment_credits);
            prepare_publication()?;
            #[cfg(any(test, feature = "test-seams"))]
            {
                observe_append_stage(AppendStage::ContentIdentity, &mut stage_started);
            }
            // Failure is reached after the unit and its outcome are staged.
            #[cfg(any(test, feature = "test-seams"))]
            if ws
                .config_writes
                .iter()
                .any(|(key, _)| key.starts_with("delivery_hub_outcome.v1."))
            {
                crate::custody::durability::before_custody_commit(&write_txn)?;
            }
            #[cfg(any(test, feature = "test-seams"))]
            {
                stage_started = std::time::Instant::now();
            }
            self.preserve_graph_commit_support(&write_txn,graph_commit_support,ws.commit_lsn.zip(Self::write_set_visibility_tx(ws)))?;
            // The grouping that decides the retention clocks is admitted first.
            let _clock_grouping = self.proof_accountant().map(|accountant| {
                crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                    accountant, Self::retention_clock_grouping_bytes(ws),
                    "writer_source", "retention_clock_grouping", "Raise MEMORY_LIMIT to decide this commit's retention clocks.",
                )
            }).transpose()?;
            Self::write_writer_recovery_bounds_in_write(&write_txn, Some(ws))?;
            #[cfg(feature = "in-process-test-seams")]
            crate::writer_commit_test_seams::reached(crate::writer_commit_test_seams::CommitBoundary::BeforeRedb);
            { #[cfg(any(test, feature = "test-seams"))]
            graph_observation_checkpoint(10)?;
            let graph_commit_result = write_txn.commit();
#[cfg(any(test, feature = "test-seams"))]
if graph_commit_result.is_ok() { graph_publication_attempt.committed(); graph_observation_checkpoint(11)?; }
graph_commit_result }.map_err(Self::storage_error)?;
            #[cfg(feature = "in-process-test-seams")]
            crate::writer_commit_test_seams::reached(crate::writer_commit_test_seams::CommitBoundary::AfterRedb);
            #[cfg(any(test, feature = "test-seams"))]
            {
                observe_append_stage(AppendStage::Commit, &mut stage_started);
            }
            Ok(())
        })
    }

    /// Commit a complete received-schema projection in the transaction that
    /// also records its finalized WriteSet identity.  The normal incremental
    /// writer cannot be used here: a DROP or same-name recreation needs the
    /// durable image to become the prepared full replacement atomically.
    /// The replaced generation of every table a received schema unit
    /// rewrites, retired from the committed identity: its rows, their sync
    /// provenance, the current vectors and every stored vector version when
    /// vectors are replaced, the links of each replaced edge type, and -- for
    /// a table the unit drops -- its index postings, maintained counts,
    /// readiness and retention expiries. The replacement image is written
    /// afterwards and its members are added after these removals.
    ///
    /// Storage the flush does not otherwise rewrite is removed here too: an
    /// index the new schema no longer declares, and a dropped table's counts,
    /// readiness and retention expiries.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn retire_received_schema_generation_in_write(
        &self,
        write_txn: &redb::WriteTransaction,
        stage: &ReceivedSchemaPersistenceProjection,
        commitments: &mut historical_index_commitment::CommitmentBatch,
    ) -> Result<schema_projection::ReceivedRetirementLog> {
        let _decode_scope = self.writer_decode_scope()?;
        let mut mutations = schema_projection::ReceivedRetirementLog::new(self, write_txn)?;
        let mut affected = stage.affected_tables.iter().collect::<Vec<_>>();
        affected.sort();

        for table in &affected {
            // A newly declared table has no retired generation. Opening its
            // missing relation here would create a table only to delete it
            // before writing the replacement image.
            let exists = {
                let metadata = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                metadata
                    .get(Self::meta_key(table).as_str())
                    .map_err(Self::storage_error)?
                    .is_some()
            };
            if !exists {
                continue;
            }
            let table_name = Self::rel_table_name(table);
            let table_def: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(table_name.as_str());
            // Keep the established legacy mismatch handling. A
            // current relation is then walked against its complete version
            // witness; no population-sized row-id set is retained.
            match write_txn.open_table(table_def) {
                Ok(relation) => drop(relation),
                Err(redb::TableError::TableTypeMismatch { .. }) => continue,
                Err(error) => return Err(Self::storage_error(error)),
            }
            let mut previous_row_id = None;
            self.visit_relational_rows_in_write(write_txn, table, |row| {
                if previous_row_id == Some(row.row_id) {
                    return Ok(());
                }
                mutations.append(
                    self,
                    write_txn,
                    &[content_identity::remove_row_mutation(table, row.row_id)],
                )?;
                previous_row_id = Some(row.row_id);
                Ok(())
            })?;
        }

        for table in &affected {
            let Some(source_meta) = self.schema_projection_metadata_point(table)? else {
                continue;
            };
            let old = &source_meta.0;
            let replacement = stage.table_meta.get(*table);
            let kept = replacement
                .map(|meta| {
                    Self::relational_index_declarations(meta)
                        .into_iter()
                        .map(|declaration| declaration.name)
                        .collect::<HashSet<_>>()
                })
                .unwrap_or_default();
            for declaration in Self::relational_index_declarations(old) {
                if kept.contains(&declaration.name) {
                    // The rebuild of the surviving declaration retires and
                    // replaces its postings itself.
                    continue;
                }
                commitments.drop_index(table, &declaration.name)?;
                let name = Self::relational_index_table_name(table, &declaration.name);
                let definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                    TableDefinition::new(name.as_str());
                self.retire_index_posting_identities_in_write(
                    write_txn,
                    table,
                    &declaration,
                    &mut mutations,
                )?;
                match write_txn.delete_table(definition) {
                    Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(error) => return Err(Self::storage_error(error)),
                }
                let current_name =
                    Self::current_relational_index_table_name(table, &declaration.name);
                let current_definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                    TableDefinition::new(current_name.as_str());
                write_txn
                    .delete_table(current_definition)
                    .map_err(Self::storage_error)?;
            }
            if replacement.is_some() {
                continue;
            }
            // A dropped table keeps no maintained counts, no readiness and no
            // retention expiries.
            self.erase_table_index_counts_in_write(write_txn, table, &mut mutations)?;
            if write_txn
                .open_table(RELATIONAL_INDEX_READY_TABLE)
                .map_err(Self::storage_error)?
                .remove(table.as_str())
                .map_err(Self::storage_error)?
                .is_some()
            {
                mutations.append(
                    self,
                    write_txn,
                    &[content_identity::remove_status_mutation(
                        RELATIONAL_INDEX_COUNT_READY_IDENTITY_NAMESPACE,
                        table.as_bytes(),
                    )],
                )?;
            }
            self.retire_table_retention_expiries_in_write(write_txn, table, Some(&mut mutations))?;
        }

        for table in &affected {
            self.retire_table_source_provenance_in_write(
                write_txn,
                table,
                false,
                true,
                Some(&mut mutations),
            )?;
        }

        if stage.replace_vectors {
            match content_size::open_vectors(write_txn) {
                Ok(vectors) => {
                    for item in vectors.iter().map_err(Self::storage_error)? {
                        let (_, value) = item.map_err(Self::storage_error)?;
                        let mut window = self.proof_admission();
                        window
                            .admit(value.value().len().saturating_mul(16).saturating_add(1024))?;
                        let row_memory = crate::read_image_memory::ImageMemory::operation_writer(
                            self.require_decode_accountant()?,
                        )?;
                        row_memory.scope(|| {
                            let entry = Self::decode_persisted_vector_entry_exact(value.value())?;
                            if !stage.affected_tables.contains(&entry.index.table) {
                                return Ok(());
                            }
                            let quantization = entry.vector.quantization();
                            mutations.append(
                                self,
                                write_txn,
                                &[
                                    content_identity::remove_vector_version_mutation(
                                        &entry.index,
                                        entry.row_id,
                                        entry.created_tx,
                                        entry.lsn,
                                        quantization,
                                    ),
                                    content_identity::remove_vector_mutation(
                                        &entry.index,
                                        entry.row_id,
                                        quantization,
                                    ),
                                ],
                            )
                        })?;
                    }
                }
                Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(error) => return Err(Self::storage_error(error)),
            }
        }

        if stage.replace_graph {
            let forward = write_txn
                .open_table(GRAPH_FWD_TABLE)
                .map_err(Self::storage_error)?;
            for item in forward
                .iter()
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
                let (_key, value) = item.map_err(Self::storage_error)?;
                #[cfg(any(test, feature = "test-seams"))]
                crate::read_work_probe::note_graph(
                    "graph_payload",
                    "payload_decode",
                    _key.value(),
                    None,
                    Some("received_schema_identity_preparation"),
                );
                let mut window = self.proof_admission();
                window.admit(value.value().len().saturating_mul(16).saturating_add(1024))?;
                let row_memory = crate::read_image_memory::ImageMemory::operation_writer(
                    self.require_decode_accountant()?,
                )?;
                row_memory.scope(|| {
                    let edge = Self::decode::<AdjEntry>(value.value())?;
                    if stage.affected_graph_edge_types.contains(&edge.edge_type) {
                        mutations.append(
                            self,
                            write_txn,
                            &[content_identity::remove_edge_mutation(
                                edge.source,
                                &edge.edge_type,
                                edge.target,
                            )],
                        )?;
                    }
                    Ok::<(), Error>(())
                })?;
            }
        }
        Ok(mutations)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn flush_received_schema_stage(
        &self,
        ws: &WriteSet,
        change_log: &[ChangeLogEntry],
        stage: &ReceivedSchemaPersistenceProjection,
        sink_events: &[PreparedSinkEvent],
        trigger_audits: &[(u64, TriggerAuditEntry)],
        max_sink_queue_depth: usize,
    ) -> Result<()> {
        let _decode_scope = self.writer_decode_scope()?;
        self.prepare_proof_owner()?;
        if stage.schema_noop {
            return self.flush_received_schema_noop(ws, change_log, stage);
        }
        let partition_image = stage
            .replace_vectors
            .then(|| {
                self.prepare_complete_vector_partition_image(stage, ws.commit_lsn.unwrap_or(Lsn(0)))
            })
            .transpose()?;
        let vector_quantization = Self::vector_quantization_map(&stage.table_meta);
        let sink_queue_names = sink_events
            .iter()
            .map(|(sink, _)| sink.clone())
            .collect::<BTreeSet<_>>();
        let schema = self.prepare_writer_schema_change(
            stage.affected_tables.iter().map(String::as_str),
            stage
                .table_meta
                .iter()
                .map(|(name, meta)| (name.as_str(), meta)),
        )?;
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            let graph_commit_support =
                Self::capture_graph_commit_support(&write_txn, ws.commit_lsn)?;
            self.graph_writer_window(|| {
                Self::apply_received_row_graph_parts_in_write(
                    &write_txn,
                    &stage.row_adj_inserts,
                    &stage.row_adj_deletes,
                    &stage.dropped_row_graph,
                )?;
                self.retire_received_shape_losses_in_write(&write_txn, stage, None)
            })?;
            let indexes_to_rebuild = {
                let stored_meta = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                let ready = write_txn
                    .open_table(RELATIONAL_INDEX_READY_TABLE)
                    .map_err(Self::storage_error)?;
                let mut rebuild = HashSet::new();
                for (table, next) in &stage.table_meta {
                    let previous = stored_meta
                        .get(Self::meta_key(table).as_str())
                        .map_err(Self::storage_error)?
                        .map(|encoded| {
                            Self::decode_table_meta_versioned(encoded.value()).map(|(meta, _)| meta)
                        })
                        .transpose()?;
                    let unchanged_declarations = previous.as_ref().is_some_and(|previous| {
                        if previous.maintained_counts != next.maintained_counts {
                            return false;
                        }
                        let before = Self::relational_index_declarations(previous);
                        if before != Self::relational_index_declarations(next) {
                            return false;
                        }
                        let indexed_columns = before
                            .iter()
                            .flat_map(|index| index.columns.iter().map(|(name, _)| name.as_str()))
                            .collect::<HashSet<_>>();
                        previous
                            .columns
                            .iter()
                            .filter(|column| indexed_columns.contains(column.name.as_str()))
                            .eq(next
                                .columns
                                .iter()
                                .filter(|column| indexed_columns.contains(column.name.as_str())))
                    });
                    let ready_before = ready
                        .get(table.as_str())
                        .map_err(Self::storage_error)?
                        .is_some_and(|version| version.value() == RELATIONAL_INDEX_READY_VERSION);
                    if !unchanged_declarations
                        || !ready_before
                        || stage.row_changed_tables.contains(table)
                        || stage.structurally_dropped_tables.contains(table)
                    {
                        rebuild.insert(table.clone());
                    }
                }
                rebuild
            };

            // Read before anything is replaced: every member the replaced
            // generation stored for an affected table leaves the committed
            // identity in this transaction, ahead of the replacement's own
            // members, so the later mutation of one logical key wins.
            // Postings and the row versions they project are committed to
            // separately and must agree: see `historical_index_commitment`.
            let mut commitments = historical_index_commitment::CommitmentBatch::for_writer()?;
            let retired_identity = self.retire_received_schema_generation_in_write(
                &write_txn,
                stage,
                &mut commitments,
            )?;

            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                // Only retired names are removed first. A surviving name's
                // previous incarnation record must remain until its own encode.
                for table in &stage.affected_tables {
                    if !stage.table_meta.contains_key(table) {
                        let key = Self::meta_key(table);
                        meta_table
                            .remove(key.as_str())
                            .map_err(Self::storage_error)?;
                    }
                }
                for (table, meta) in &stage.table_meta {
                    // Retain one complete encoding owner, not a HashMap of
                    // every table's bytes and conservative scratch reservation.
                    // The owner stays alive through insertion and route setup;
                    // its bytes drop before its reservation at this iteration's end.
                    let encoded = self.schema_update_in_table(
                        &meta_table,
                        table,
                        meta,
                        stage.ddl_log.iter().map(|(_, change)| change),
                    )?;
                    let key = Self::meta_key(table);
                    meta_table
                        .insert(key.as_str(), encoded.bytes.as_slice())
                        .map_err(Self::storage_error)?;
                    vector_membership_pages::ensure_declared_routes(
                        &write_txn,
                        table,
                        meta,
                        self.proof_accountant(),
                    )?;
                }
            }

            self.retain_pending_authored_rows_in_write(&write_txn, &stage.authored_pending_rows)?;
            let mut relational_tables = stage.affected_tables.iter().cloned().collect::<Vec<_>>();
            relational_tables.sort();
            relational_tables.dedup();
            for table in relational_tables {
                #[cfg(any(test, feature = "test-seams"))]
                crate::database::record_received_schema_durable_table_rewrite_for_test(&table);
                let table_name = Self::rel_table_name(&table);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                self.retain_received_authored_rows_in_write(&write_txn, &table, stage)?;
                self.retire_relational_manifests_in_write(&write_txn, &table)?;
                content_size::reset_rows(&write_txn, &table)?;
                match write_txn.delete_table(table_def) {
                    Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(redb::TableError::TableTypeMismatch { .. }) => {}
                    Err(error) => return Err(Self::storage_error(error)),
                }
                let legacy_table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                match write_txn.delete_table(legacy_table_def) {
                    Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(redb::TableError::TableTypeMismatch { .. }) => {}
                    Err(error) => return Err(Self::storage_error(error)),
                }
                // The rows are written again below: the version side of
                // every index is folded afresh from them.
                if let Some(meta) = stage.table_meta.get(&table) {
                    // A retained empty relation still needs a physical family
                    // and its producer witness. No row callback recreates it.
                    drop(
                        content_size::open_rows(&write_txn, table_def, Some(meta))
                            .map_err(Self::storage_error)?,
                    );
                    for declaration in Self::relational_index_declarations(meta) {
                        commitments.reset(
                            &write_txn,
                            &table,
                            &declaration,
                            historical_index_commitment::Side::Versions,
                        )?;
                    }
                }
            }
            let paged_rows = if stage.row_records.is_some() {
                Some(stage.materialize_rows_compatibility()?)
            } else {
                None
            };
            let resident_rows = stage.rows_image();
            let compatibility_rows = paged_rows.as_ref().unwrap_or(&*resident_rows);
            for (table, rows) in compatibility_rows {
                let table_name = Self::rel_table_name(table);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut redb_table =
                    content_size::open_rows(&write_txn, table_def, stage.table_meta.get(table))
                        .map_err(Self::storage_error)?;
                for row in rows {
                    #[cfg(any(test, feature = "test-seams"))]
                    crate::database::record_received_schema_durable_row_rewrite_for_test();
                    let encoded = Self::encode_versioned_row(row, stage.table_meta.get(table))?;
                    let key = Self::rel_row_key(row);
                    {
                        let predecessor = redb_table
                            .get(key.as_slice())
                            .map_err(Self::storage_error)?;
                        self.replace_relational_manifest_in_write(
                            &write_txn,
                            table,
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
            for (table, meta) in &stage.table_meta {
                if !indexes_to_rebuild.contains(table) {
                    continue;
                }
                Self::rebuild_relational_indexes_in_write(
                    &write_txn,
                    &self.path,
                    table,
                    meta,
                    compatibility_rows.get(table).map(Vec::as_slice),
                    &mut self.proof_admission(),
                    Some(&mut commitments),
                )?;
            }
            // The replacement is complete here: its commitments are checked
            // and published before the operational rows fold their own.
            let (replacement_commitments, _replacement_commitment_credit) =
                commitments.finish(&write_txn)?.into_parts();
            let (operational_index_identity_mutations, _operational_commitment_credits) =
                self.flush_received_schema_operational_rows_in_write(&write_txn, stage)?;

            for table in &stage.affected_tables {
                self.retire_table_source_provenance_in_write(&write_txn, table, true, false, None)?;
            }
            {
                let mut lsn_table = write_txn
                    .open_table(SYNC_ROW_SOURCE_LSN_TABLE)
                    .map_err(Self::storage_error)?;
                let mut kind_table = write_txn
                    .open_table(SYNC_ROW_SOURCE_KIND_TABLE)
                    .map_err(Self::storage_error)?;
                stage.visit_sync_sources(|table, row_id, lsn, kind| {
                    let key = Self::sync_row_source_lsn_key(table, row_id);
                    lsn_table
                        .insert(key.as_slice(), lsn.0)
                        .map_err(Self::storage_error)?;
                    kind_table
                        .insert(key.as_slice(), kind)
                        .map_err(Self::storage_error)?;
                    Ok(())
                })?;
            }

            if stage.replace_graph {
                self.retire_graph_replacement_in_write(
                    &write_txn,
                    &stage.edges,
                    Some(&stage.affected_graph_edge_types),
                )?;
                self.erase_received_graph_adjacencies_in_write(
                    &write_txn,
                    &stage.affected_graph_edge_types,
                )?;
                let mut fwd = write_txn
                    .open_table(GRAPH_FWD_TABLE)
                    .map_err(Self::storage_error)?;
                let mut rev = write_txn
                    .open_table(GRAPH_REV_TABLE)
                    .map_err(Self::storage_error)?;
                for edge in &stage.edges {
                    let encoded = Self::encode(edge)?;
                    let fwd_key = Self::graph_fwd_key(edge);
                    let rev_key = Self::graph_rev_key(edge);
                    fwd.insert(
                        observed_graph_key!("graph_fwd", "insert", fwd_key.as_slice(), 5),
                        encoded.as_slice(),
                    )
                    .map_err(Self::storage_error)?;
                    self.graph_writer_window(|| {
                        Self::install_graph_creation_in_write(&write_txn, edge, &encoded)
                    })?;
                    rev.insert(
                        observed_graph_key!("graph_rev", "insert", rev_key.as_slice(), 5),
                        encoded.as_slice(),
                    )
                    .map_err(Self::storage_error)?;
                }
            }

            if stage.replace_vectors {
                // Authenticate the old fixed inputs while their raw bodies and
                // source companions still belong to the committed image.
                let previous = db.begin_read().map_err(Self::storage_error)?;
                self.reconcile_paged_builds_for_image_in_write(
                    &write_txn,
                    partition_image.as_ref().expect("prepared vector image"),
                    &previous,
                    stage,
                    None,
                )?;
                self.remove_vector_entries_matching_in_write(&write_txn, &|index| {
                    stage.affected_tables.contains(&index.table)
                })?;
                let mut vectors =
                    content_size::open_vectors(&write_txn).map_err(Self::storage_error)?;
                stage.visit_vector_bodies(|entry| {
                    let quantization = vector_quantization
                        .get(&entry.index)
                        .copied()
                        .unwrap_or_default();
                    let encoded = Self::encode_vector_entry_admitted(entry, quantization)?;
                    let key = Self::vector_key(entry);
                    vectors
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    Ok(())
                })?;
                self.replace_vector_partition_image_for_tables_in_write(
                    &write_txn,
                    partition_image.as_ref().expect("prepared vector image"),
                    &stage.affected_tables,
                    stage.vector_image_memory.accountant.clone(),
                )?;
                for (table, meta) in &stage.table_meta {
                    vector_membership_pages::ensure_declared_routes(
                        &write_txn,
                        table,
                        meta,
                        self.proof_accountant(),
                    )?;
                }
            }

            {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let mut grouped = BTreeMap::<Lsn, Vec<&DdlChange>>::new();
                for (lsn, ddl) in &stage.ddl_log {
                    grouped.entry(*lsn).or_default().push(ddl);
                }
                for (lsn, ddl) in grouped {
                    for (index, change) in ddl.iter().enumerate() {
                        let key = Self::ddl_log_key_for_index(lsn, index, ddl.len());
                        self.append_ddl_summary_in_write(&write_txn, &key, change)?;
                        let encoded = Self::encode(*change)?;
                        ddl_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }
            }

            if let (Some(lsn), Some(tx)) = (ws.commit_lsn, Self::write_set_visibility_tx(ws)) {
                let mut index = write_txn
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?;
                index.insert(lsn.0, tx.0).map_err(Self::storage_error)?;
            }
            let mut change_log_identity = Vec::new();
            if !stage.structurally_dropped_tables.is_empty() {
                // A dropped table's change-log records leave the store and
                // the committed identity together, in this received-schema
                // write transaction, so a later failure cannot leave a new
                // schema image paired with old-generation history. A commit
                // that also changed other tables keeps those records, packed
                // back into consecutive positions within its LSN -- the
                // layout every other history retirement leaves -- with the
                // identity moved to the positions they now occupy.
                let mut log = write_txn
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let mut affected_lsns = BTreeSet::new();
                for item in {
                    #[cfg(any(test, feature = "test-seams"))]
                    crate::read_work_probe::note_graph(
                        "history",
                        "population_walk",
                        &[],
                        None,
                        None,
                    );
                    log.iter()
                }
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
                }) {
                    let (key, value) = item.map_err(Self::storage_error)?;
                    let entry = Self::decode_stored_change_log(value.value())?.entry;
                    if Self::change_log_entry_references_any_table(
                        &entry,
                        &stage.structurally_dropped_tables,
                    ) {
                        let coordinate = Self::numeric_change_key(key.value())?;
                        affected_lsns.insert(Lsn(u64::from_be_bytes(
                            coordinate[..8]
                                .try_into()
                                .expect("fixed history coordinate"),
                        )));
                    }
                }
                let mut key_buf = String::with_capacity(Self::change_log_entry_key_len());
                for lsn in affected_lsns {
                    let prefix = format!("{:020}:", lsn.0);
                    let mut group = Vec::new();
                    for item in log
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
                        let (key, value) = item.map_err(Self::storage_error)?;
                        if !key.value().starts_with(prefix.as_str()) {
                            break;
                        }
                        group.push((key.value().to_string(), value.value().to_vec()));
                    }
                    let mut survivors = Vec::with_capacity(group.len());
                    for (key, bytes) in group {
                        let entry = Self::decode_stored_change_log(&bytes)?.entry;
                        log.remove(observed_graph_key!("history", "remove", key.as_str(), 8))
                            .map_err(Self::storage_error)?;
                        let carried = !Self::change_log_entry_references_any_table(
                            &entry,
                            &stage.structurally_dropped_tables,
                        );
                        Self::detach_change_log_selectors_in_write(
                            &write_txn,
                            key.as_str(),
                            carried,
                        )?;
                        change_log_identity.push(
                            content_identity::remove_encoded_history_mutation(&key, &entry)?,
                        );
                        if !Self::change_log_entry_references_any_table(
                            &entry,
                            &stage.structurally_dropped_tables,
                        ) {
                            survivors.push((bytes, entry));
                        }
                    }
                    for (index, (bytes, _entry)) in survivors.into_iter().enumerate() {
                        Self::write_change_log_entry_key(lsn, index, &mut key_buf);
                        log.insert(
                            observed_graph_key!("history", "insert", key_buf.as_str(), 8),
                            bytes.as_slice(),
                        )
                        .map_err(Self::storage_error)?;
                        self.graph_writer_window(|| {
                            Self::write_carried_change_log_selectors_in_write(
                                &write_txn,
                                key_buf.as_str(),
                                bytes.as_slice(),
                            )
                        })?;
                        change_log_identity
                            .push(Self::stored_change_log_mutation(&key_buf, &bytes)?);
                    }
                }
            }
            Self::stage_graph_commit_support(&write_txn, graph_commit_support)?;
            if !change_log.is_empty() {
                let mut log = write_txn
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                let mut key = String::with_capacity(Self::change_log_entry_key_len());
                let mut encoded = Vec::new();
                for (index, entry) in change_log.iter().enumerate() {
                    Self::write_change_log_entry_key(lsn, index, &mut key);
                    self.encode_change_log_into_in_write(&write_txn, entry, &mut encoded)?;
                    log.insert(
                        observed_graph_key!("history", "insert", key.as_str(), 8),
                        encoded.as_slice(),
                    )
                    .map_err(Self::storage_error)?;
                    self.graph_writer_window(|| {
                        Self::write_change_log_selectors_in_write(
                            &write_txn,
                            key.as_str(),
                            encoded.as_slice(),
                        )
                    })?;
                }
            }
            let mut committed_config_values = Vec::new();
            {
                let mut config = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for key in &ws.config_deletes {
                    Self::update_policy_hold_release_index(&write_txn, key, false)?;
                    config.remove(key.as_str()).map_err(Self::storage_error)?;
                }
                for (key, value) in stage.config_values.iter().chain(&ws.config_writes) {
                    Self::raise_config_identity_floors_in_write(&write_txn, key, value)?;
                    Self::update_policy_hold_release_index(&write_txn, key, true)?;
                    let value = if stage
                        .config_max_u64_keys
                        .iter()
                        .any(|max_key| max_key == key)
                    {
                        let incoming = Self::decode::<u64>(value)?;
                        let current = config
                            .get(key.as_str())
                            .map_err(Self::storage_error)?
                            .map(|value| Self::decode::<u64>(value.value()))
                            .transpose()?
                            .unwrap_or(0);
                        Self::encode(&current.max(incoming))?
                    } else {
                        value.clone()
                    };
                    config
                        .insert(key.as_str(), value.as_slice())
                        .map_err(Self::storage_error)?;
                    committed_config_values.push((key.clone(), value));
                }
            }
            // The complete received-schema core image now exists only inside
            // this live Redb transaction. Sink queues and trigger audits for
            // received rows belong immediately below this checkpoint and must
            // commit with that image, never in a later transaction.
            #[cfg(test)]
            if take_received_schema_side_effect_persistence_fault_for_test() {
                return Err(Error::Other(
                    ("injected received-schema side-effect persistence failure".to_string()).into(),
                ));
            }
            if !sink_events.is_empty() {
                let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                let mut overflow_audits = Vec::new();
                let mut by_sink: BTreeMap<&str, Vec<(usize, &SinkQueueEntry)>> = BTreeMap::new();
                for (index, (sink, entry)) in sink_events.iter().enumerate() {
                    by_sink
                        .entry(sink.as_str())
                        .or_default()
                        .push((index, entry));
                }
                for (sink, entries) in by_sink {
                    let table_name = Self::sink_queue_table_name(sink);
                    let table_def: TableDefinition<u64, &[u8]> =
                        TableDefinition::new(table_name.as_str());
                    let mut table = write_txn
                        .open_table(table_def)
                        .map_err(Self::storage_error)?;
                    let mut existing = table.len().map_err(Self::storage_error)? as usize;
                    for (index, entry) in entries {
                        if existing >= max_sink_queue_depth {
                            return Err(Error::Other(
                                (format!("sink queue is full: {sink}")).into(),
                            ));
                        }
                        let overflow_id = if existing >= max_sink_queue_depth {
                            table
                                .first()
                                .map_err(Self::storage_error)?
                                .map(|(key, _)| key.value())
                        } else {
                            None
                        };
                        if let Some(id) = overflow_id {
                            table.remove(id).map_err(Self::storage_error)?;
                            existing = existing.saturating_sub(1);
                            overflow_audits.push((index, sink.to_string()));
                        }
                        let encoded = Self::encode(entry)?;
                        table
                            .insert(entry.id, encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        existing = existing.saturating_add(1);
                    }
                }
                if !overflow_audits.is_empty() {
                    let mut audit_table = write_txn
                        .open_table(SINK_AUDIT_TABLE)
                        .map_err(Self::storage_error)?;
                    for (index, sink) in overflow_audits {
                        let key = Self::sink_audit_key(lsn, index, &sink);
                        let audit = SinkAuditEntry {
                            lsn,
                            kind: SinkAuditKind::QueueOverflow {
                                sink,
                                dropped_count: 1,
                            },
                        };
                        let encoded = Self::encode(&audit)?;
                        audit_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }
            }
            if !trigger_audits.is_empty() {
                {
                    let mut audit_table = write_txn
                        .open_table(TRIGGER_AUDIT_TABLE)
                        .map_err(Self::storage_error)?;
                    let mut stamps = write_txn
                        .open_table(TRIGGER_AUDIT_STAMPS_TABLE)
                        .map_err(Self::storage_error)?;
                    let stamped_at = Wallclock::now().0;
                    for (index, entry) in trigger_audits {
                        let key = Self::trigger_audit_key(*index, &entry.trigger_name);
                        let encoded = Self::encode(entry)?;
                        audit_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        stamps
                            .insert(key.as_str(), stamped_at)
                            .map_err(Self::storage_error)?;
                    }
                }
                {
                    let mut config_table = write_txn
                        .open_table(CONFIG_TABLE)
                        .map_err(Self::storage_error)?;
                    let mut ring: Vec<TriggerAuditEntry> = config_table
                        .get(TRIGGER_AUDIT_RING_CONFIG_KEY)
                        .map_err(Self::storage_error)?
                        .map(|value| Self::decode(value.value()))
                        .transpose()?
                        .unwrap_or_default();
                    ring.extend(trigger_audits.iter().map(|(_, entry)| entry.clone()));
                    let overflow = ring
                        .len()
                        .saturating_sub(crate::database::trigger::TRIGGER_AUDIT_RING_CAPACITY);
                    if overflow > 0 {
                        ring.drain(0..overflow);
                    }
                    let current_next_index = config_table
                        .get(TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY)
                        .map_err(Self::storage_error)?
                        .map(|value| Self::decode::<u64>(value.value()))
                        .transpose()?
                        .unwrap_or(0);
                    let next_index = trigger_audits
                        .iter()
                        .map(|(index, _)| index.saturating_add(1))
                        .max()
                        .unwrap_or(0)
                        .max(current_next_index);
                    let encoded_ring = Self::encode(&ring)?;
                    config_table
                        .insert(TRIGGER_AUDIT_RING_CONFIG_KEY, encoded_ring.as_slice())
                        .map_err(Self::storage_error)?;
                    committed_config_values
                        .push((TRIGGER_AUDIT_RING_CONFIG_KEY.to_owned(), encoded_ring));
                    let encoded_next_index = Self::encode(&next_index)?;
                    config_table
                        .insert(
                            TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY,
                            encoded_next_index.as_slice(),
                        )
                        .map_err(Self::storage_error)?;
                    committed_config_values.push((
                        TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY.to_owned(),
                        encoded_next_index,
                    ));
                }
            }
            retired_identity.apply(self, &write_txn)?;
            let mut identity_mutations =
                schema_projection::ReceivedRetirementLog::new(self, &write_txn)?;
            identity_mutations.append(self, &write_txn, &(replacement_commitments))?;
            drop(replacement_commitments);
            drop(_replacement_commitment_credit);
            identity_mutations.append(self, &write_txn, &(operational_index_identity_mutations))?;
            drop(operational_index_identity_mutations);
            drop(_operational_commitment_credits);
            for table in &stage.affected_tables {
                // The former batch skipped a schema removal superseded by its
                // replacement. Preserve that coalescing before replaying one
                // mutation at a time, or removal would clear content-size totals.
                if !stage.table_meta.contains_key(table) {
                    identity_mutations.append(
                        self,
                        &write_txn,
                        &(content_identity::remove_schema_mutations(table)),
                    )?;
                }
            }
            for (table, meta) in &stage.table_meta {
                identity_mutations.append(
                    self,
                    &write_txn,
                    &(content_identity::schema_mutations(table, meta)?),
                )?;
            }
            // The history retirement above, removals before the packed
            // survivors, so the physical store and the authenticated history
            // it proves never disagree about which records this installation
            // left behind or where they sit.
            identity_mutations.append(self, &write_txn, &(change_log_identity))?;
            drop(change_log_identity);
            for (table, rows) in compatibility_rows {
                let latest = schema_projection::ReceivedLatestRecords::new(&write_txn)?;
                for (index, row) in rows.iter().enumerate() {
                    latest.consider(
                        self,
                        &write_txn,
                        &row.row_id.0.to_be_bytes(),
                        row.lsn,
                        row.created_tx,
                        index,
                    )?;
                }
                latest.visit(self, &write_txn, |index| {
                    let row = &rows[index];
                    if row.deleted_tx.is_none() {
                        identity_mutations.append(
                            self,
                            &write_txn,
                            &[content_identity::row_mutation(
                                table,
                                row,
                                stage.table_meta.get(table),
                            )?],
                        )?;
                    }
                    Ok(())
                })?;
            }
            for (table, row_id, _) in &ws.relational_deletes {
                identity_mutations.append(
                    self,
                    &write_txn,
                    &[content_identity::remove_row_mutation(table, *row_id)],
                )?;
            }
            for (table, row) in &stage.operational_write_set.relational_inserts {
                identity_mutations.append(
                    self,
                    &write_txn,
                    &[content_identity::row_mutation(
                        table,
                        row,
                        stage.operational_table_meta.get(table),
                    )?],
                )?;
            }
            let latest = schema_projection::ReceivedLatestRecords::new(&write_txn)?;
            for (index, edge) in stage.edges.iter().enumerate() {
                let mut window = self.proof_admission();
                window.admit(edge.edge_type.len().saturating_mul(4).saturating_add(512))?;
                let key = Self::encode(&(&edge.edge_type, edge.source, edge.target))?;
                latest.consider(self, &write_txn, &key, edge.lsn, edge.created_tx, index)?;
            }
            latest.visit(self, &write_txn, |index| {
                let edge = &stage.edges[index];
                if edge.deleted_tx.is_none() {
                    identity_mutations.append(
                        self,
                        &write_txn,
                        &[content_identity::edge_mutation(edge)?],
                    )?;
                }
                Ok(())
            })?;
            let latest = schema_projection::ReceivedLatestRecords::new(&write_txn)?;
            // Compatibility callers may receive the same paged source;
            // their existing image owner admits this legacy gather explicitly.
            let mut identity_vectors = Vec::new();
            if stage.vector_records.is_some() {
                stage.visit_vector_bodies(|body| {
                    stage.vector_image_memory.reserve(
                        content_identity::original_record_work_bytes(body)?
                            .saturating_add(std::mem::size_of::<VectorEntry>()),
                    )?;
                    identity_vectors.reserve_exact(1);
                    identity_vectors.push(body.clone());
                    Ok(())
                })?;
            }
            let identity_vectors = if stage.vector_records.is_some() {
                &identity_vectors
            } else {
                &stage.vectors
            };
            for (index, vector) in identity_vectors.iter().enumerate() {
                let quantization = vector_quantization
                    .get(&vector.index)
                    .copied()
                    .unwrap_or_default();
                identity_mutations.append(
                    self,
                    &write_txn,
                    &[content_identity::vector_version_mutation(
                        vector,
                        quantization,
                    )],
                )?;
                let mut window = self.proof_admission();
                window.admit(
                    vector
                        .index
                        .table
                        .len()
                        .saturating_add(vector.index.column.len())
                        .saturating_mul(4)
                        .saturating_add(512),
                )?;
                let key =
                    Self::encode(&(&vector.index.table, &vector.index.column, vector.row_id))?;
                latest.consider(self, &write_txn, &key, vector.lsn, vector.created_tx, index)?;
            }
            latest.visit(self, &write_txn, |index| {
                let vector = &identity_vectors[index];
                if vector.deleted_tx.is_none() {
                    let quantization = vector_quantization
                        .get(&vector.index)
                        .copied()
                        .unwrap_or_default();
                    identity_mutations.append(
                        self,
                        &write_txn,
                        &[content_identity::vector_mutation(vector, quantization)],
                    )?;
                }
                Ok(())
            })?;
            stage.visit_sync_sources(|table, row_id, lsn, kind| {
                identity_mutations.append(
                    self,
                    &write_txn,
                    &[content_identity::sync_source_mutation(
                        table, row_id, lsn.0, kind,
                    )],
                )?;
                Ok(())
            })?;
            let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
            let mut change_key = String::with_capacity(Self::change_log_entry_key_len());
            for (index, _entry) in change_log.iter().enumerate() {
                Self::write_change_log_entry_key(lsn, index, &mut change_key);
                identity_mutations.append(
                    self,
                    &write_txn,
                    &[Self::stored_change_log_mutation_in_write(
                        &write_txn,
                        &change_key,
                    )?],
                )?;
            }
            let mut grouped_ddl = BTreeMap::<Lsn, Vec<&DdlChange>>::new();
            for (ddl_lsn, change) in &stage.ddl_log {
                grouped_ddl.entry(*ddl_lsn).or_default().push(change);
            }
            for (ddl_lsn, changes) in grouped_ddl {
                for (index, change) in changes.iter().enumerate() {
                    let key = Self::ddl_log_key_for_index(ddl_lsn, index, changes.len());
                    identity_mutations.append(
                        self,
                        &write_txn,
                        &[content_identity::encoded_history_mutation(&key, *change)?],
                    )?;
                }
            }
            // The committed history records the very commit-index entry this
            // flush stores above, with the same transaction id.
            if let (Some(commit_lsn), Some(tx)) = (ws.commit_lsn, Self::write_set_visibility_tx(ws))
            {
                identity_mutations.append(
                    self,
                    &write_txn,
                    &[content_identity::commit_index_mutation(commit_lsn, tx)],
                )?;
            }
            for key in &ws.config_deletes {
                identity_mutations.append(
                    self,
                    &write_txn,
                    &(content_identity::config_mutations(key, None)),
                )?;
            }
            for (key, value) in &committed_config_values {
                identity_mutations.append(
                    self,
                    &write_txn,
                    &(content_identity::config_mutations(key, Some(value))),
                )?;
            }
            if !sink_events.is_empty() {
                let table = write_txn
                    .open_table(SINK_AUDIT_TABLE)
                    .map_err(Self::storage_error)?;
                for item in table.iter().map_err(Self::storage_error)? {
                    let (key, value) = item.map_err(Self::storage_error)?;
                    identity_mutations.append(
                        self,
                        &write_txn,
                        &[content_identity::status_mutation(
                            b"sink-audit",
                            key.value().as_bytes(),
                            value.value(),
                        )],
                    )?;
                }
            }
            if !trigger_audits.is_empty() {
                let table = write_txn
                    .open_table(TRIGGER_AUDIT_TABLE)
                    .map_err(Self::storage_error)?;
                for item in table.iter().map_err(Self::storage_error)? {
                    let (key, value) = item.map_err(Self::storage_error)?;
                    identity_mutations.append(
                        self,
                        &write_txn,
                        &[content_identity::status_mutation(
                            b"trigger-audit",
                            key.value().as_bytes(),
                            value.value(),
                        )],
                    )?;
                }
                let table = write_txn
                    .open_table(TRIGGER_AUDIT_STAMPS_TABLE)
                    .map_err(Self::storage_error)?;
                for item in table.iter().map_err(Self::storage_error)? {
                    let (key, stamp) = item.map_err(Self::storage_error)?;
                    identity_mutations.append(
                        self,
                        &write_txn,
                        &[content_identity::status_mutation(
                            b"trigger-stamp",
                            key.value().as_bytes(),
                            &stamp.value().to_le_bytes(),
                        )],
                    )?;
                }
            }
            for sink in &sink_queue_names {
                let table_name = Self::sink_queue_table_name(sink);
                let table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                for item in table.iter().map_err(Self::storage_error)? {
                    let (id, value) = item.map_err(Self::storage_error)?;
                    let entry: SinkQueueEntry = Self::decode(value.value())?;
                    let key = format!("{}:{sink}:{}", sink.len(), id.value());
                    identity_mutations.append(
                        self,
                        &write_txn,
                        &[content_identity::encoded_status_mutation(
                            b"sink-queue",
                            key.as_bytes(),
                            &entry,
                        )?],
                    )?;
                }
            }
            identity_mutations.apply(self, &write_txn)?;
            if take_received_schema_pre_commit_fault_for_test() {
                return Err(Error::Other(
                    ("injected received-schema Redb pre-commit failure".to_string()).into(),
                ));
            }
            // Failure is reached after the unit and its outcome are staged.
            #[cfg(any(test, feature = "test-seams"))]
            if ws
                .config_writes
                .iter()
                .any(|(key, _)| key.starts_with("delivery_hub_outcome.v1."))
            {
                crate::custody::durability::before_custody_commit(&write_txn)?;
            }
            // The floors are maxima, so fold them over every received row and
            // vector and raise them in the commit's one write of the recovery
            // bounds.
            let mut floors: Option<(TxId, Lsn, RowId)> = None;
            let mut raise = |tx: TxId, lsn: Lsn, row: RowId| {
                floors = Some(floors.map_or((tx, lsn, row), |(t, l, r)| {
                    (t.max(tx), l.max(lsn), r.max(row))
                }));
            };
            for (table, rows) in compatibility_rows {
                for row in rows {
                    Self::raise_table_read_lsn_in_write(&write_txn, table, row.lsn)?;
                    raise(
                        row.created_tx.max(row.deleted_tx.unwrap_or(TxId(0))),
                        row.lsn,
                        row.row_id,
                    );
                }
            }
            stage.visit_vector_bodies(|entry| {
                Self::raise_table_read_lsn_in_write(&write_txn, &entry.index.table, entry.lsn)?;
                raise(
                    entry.created_tx.max(entry.deleted_tx.unwrap_or(TxId(0))),
                    entry.lsn,
                    entry.row_id,
                );
                Ok(())
            })?;
            self.preserve_graph_commit_support(
                &write_txn,
                graph_commit_support,
                ws.commit_lsn.zip(Self::write_set_visibility_tx(ws)),
            )?;
            // The grouping that decides the retention clocks is admitted first.
            let _clock_grouping = self
                .proof_accountant()
                .map(|accountant| {
                    crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                        accountant,
                        Self::retention_clock_grouping_bytes(ws),
                        "writer_source",
                        "retention_clock_grouping",
                        "Raise MEMORY_LIMIT to decide this commit's retention clocks.",
                    )
                })
                .transpose()?;
            Self::write_writer_recovery_bounds_with_floors_in_write(&write_txn, Some(ws), floors)?;
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
            self.complete_writer_schema_change(schema);
            Ok(())
        })
    }

    /// Commit authenticated replay identity and sender progress for a schema
    /// whose canonical structured projection is already live. These records
    /// share one Redb transaction while every logical data artifact remains
    /// byte-for-byte untouched.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn flush_received_schema_operational_rows_in_write(
        &self,
        write_txn: &redb::WriteTransaction,
        stage: &ReceivedSchemaPersistenceProjection,
    ) -> Result<(
        Vec<content_identity::ContentIdentityMutation>,
        Vec<historical_index_commitment::CommitmentCredit>,
    )> {
        let ws = &stage.operational_write_set;
        // Postings and the row versions they project are committed to
        // separately and must agree: see `historical_index_commitment`.
        let mut commitments = historical_index_commitment::CommitmentBatch::for_writer()?;
        for (table, row_id, deleted_tx) in &ws.relational_deletes {
            let old = stage
                .operational_deleted_rows
                .get(table)
                .and_then(|rows| rows.get(row_id))
                .ok_or_else(|| {
                    Error::Other(
                        (format!(
                            "received-schema operational update is missing row {} from {table}",
                            row_id.0
                        ))
                        .into(),
                    )
                })?;
            let mut deleted = old.clone();
            deleted.deleted_tx = Some(*deleted_tx);
            let table_name = Self::rel_table_name(table);
            let definition: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(table_name.as_str());
            let mut relation = content_size::open_rows(
                write_txn,
                definition,
                stage.operational_table_meta.get(table),
            )
            .map_err(Self::storage_error)?;
            self.verify_relational_group_in_write(write_txn, table, *row_id, &*relation)?;
            let key = Self::rel_row_key(&deleted);
            let encoded =
                Self::encode_versioned_row(&deleted, stage.operational_table_meta.get(table))?;
            {
                let predecessor = relation.get(key.as_slice()).map_err(Self::storage_error)?;
                self.replace_relational_manifest_in_write(
                    write_txn,
                    table,
                    key.as_slice(),
                    predecessor.as_ref().map(|body| body.value()),
                    Some(encoded.as_slice()),
                )?;
            }
            let keys = content_size::VersionLife::keys_of(relation.indexes(), &deleted)?;
            relation.insert_version(
                &mut commitments,
                key.as_slice(),
                encoded.as_slice(),
                &content_size::VersionLife {
                    keys: &keys,
                    created: deleted.created_tx,
                    deleted: deleted.deleted_tx,
                },
            )?;
        }
        for (table, row) in &ws.relational_inserts {
            let table_name = Self::rel_table_name(table);
            let definition: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(table_name.as_str());
            let mut relation = content_size::open_rows(
                write_txn,
                definition,
                stage.operational_table_meta.get(table),
            )
            .map_err(Self::storage_error)?;
            let key = Self::rel_row_key(row);
            let encoded = Self::encode_versioned_row(row, stage.operational_table_meta.get(table))?;
            {
                let predecessor = relation.get(key.as_slice()).map_err(Self::storage_error)?;
                self.replace_relational_manifest_in_write(
                    write_txn,
                    table,
                    key.as_slice(),
                    predecessor.as_ref().map(|body| body.value()),
                    Some(encoded.as_slice()),
                )?;
            }
            let keys = content_size::VersionLife::keys_of(relation.indexes(), row)?;
            relation.insert_version(
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
        // Declared first, so dropped last: these charges outlive the mutations they pay for.
        let mut mutations_commitment_credits: Vec<historical_index_commitment::CommitmentCredit> =
            Vec::new();
        let mut mutations = Self::flush_relational_index_changes_in_write(
            write_txn,
            ws,
            &stage.operational_table_meta,
            Some(&stage.operational_deleted_rows),
            &mut commitments,
        )?;
        let (published, credit) = commitments.finish(write_txn)?.into_parts();
        mutations_commitment_credits.push(credit);
        mutations.extend(published);
        // The credits travel with the mutations they pay for.
        Ok((mutations, mutations_commitment_credits))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn flush_received_schema_noop(
        &self,
        ws: &WriteSet,
        change_log: &[ChangeLogEntry],
        stage: &ReceivedSchemaPersistenceProjection,
    ) -> Result<()> {
        let _decode_scope = self.writer_decode_scope()?;
        self.prepare_proof_owner()?;
        #[cfg(any(test, feature = "test-seams"))]
        let flush_started = std::time::Instant::now();
        #[cfg(any(test, feature = "test-seams"))]
        {
            crate::database::record_received_schema_persistence_work_for_test(
                "noop-ddl-log-writes",
                stage.ddl_log.len() as u64,
            );
            crate::database::record_received_schema_persistence_work_for_test(
                "noop-config-writes",
                stage
                    .config_values
                    .len()
                    .saturating_add(ws.config_writes.len()) as u64,
            );
            crate::database::record_received_schema_persistence_work_for_test(
                "noop-config-deletes",
                ws.config_deletes.len() as u64,
            );
            crate::database::record_received_schema_persistence_work_for_test(
                "noop-operational-row-writes",
                stage
                    .operational_write_set
                    .relational_deletes
                    .len()
                    .saturating_add(stage.operational_write_set.relational_inserts.len())
                    as u64,
            );
        }
        let mut schema = if stage.table_meta.is_empty() {
            None
        } else {
            self.prepare_writer_schema_change(
                std::iter::empty(),
                stage
                    .table_meta
                    .iter()
                    .map(|(name, meta)| (name.as_str(), meta)),
            )?
        };
        self.with_db(|db| {
            #[cfg(any(test, feature = "test-seams"))]
            let begin_started = std::time::Instant::now();
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            let graph_commit_support =
                Self::capture_graph_commit_support(&write_txn, ws.commit_lsn)?;
            self.graph_writer_window(|| {
                Self::apply_received_row_graph_parts_in_write(
                    &write_txn,
                    &stage.row_adj_inserts,
                    &stage.row_adj_deletes,
                    &stage.dropped_row_graph,
                )?;
                self.retire_received_shape_losses_in_write(&write_txn, stage, None)
            })?;
            #[cfg(any(test, feature = "test-seams"))]
            crate::database::record_received_schema_persistence_profile_for_test(
                "noop-begin-write",
                begin_started,
            );
            let (operational_index_identity_mutations, _operational_commitment_credits) =
                self.flush_received_schema_operational_rows_in_write(&write_txn, stage)?;
            if !stage.table_meta.is_empty() {
                let mut metadata = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                for (table, meta) in &stage.table_meta {
                    let encoded = self.schema_update_in_table(
                        &metadata,
                        table,
                        meta,
                        stage.ddl_log.iter().map(|(_, ddl)| ddl),
                    )?;
                    metadata
                        .insert(Self::meta_key(table).as_str(), encoded.bytes.as_slice())
                        .map_err(Self::storage_error)?;
                    Self::raise_table_read_lsn_in_write(
                        &write_txn,
                        table,
                        ws.commit_lsn.unwrap_or(Lsn(0)),
                    )?;
                }
            }
            if !stage.ddl_log.is_empty() {
                let mut ddl = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let mut grouped = BTreeMap::<Lsn, Vec<&DdlChange>>::new();
                for (lsn, change) in &stage.ddl_log {
                    grouped.entry(*lsn).or_default().push(change);
                }
                for (lsn, changes) in grouped {
                    for (index, change) in changes.iter().enumerate() {
                        let key = Self::ddl_log_key_for_index(lsn, index, changes.len());
                        self.append_ddl_summary_in_write(&write_txn, &key, change)?;
                        let encoded = Self::encode(*change)?;
                        ddl.insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }
            }
            let mut committed_config_values = Vec::new();
            {
                let mut config = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for key in &ws.config_deletes {
                    Self::update_policy_hold_release_index(&write_txn, key, false)?;
                    config.remove(key.as_str()).map_err(Self::storage_error)?;
                }
                for (key, value) in stage.config_values.iter().chain(&ws.config_writes) {
                    Self::raise_config_identity_floors_in_write(&write_txn, key, value)?;
                    Self::update_policy_hold_release_index(&write_txn, key, true)?;
                    let value = if ws.config_max_u64_keys.iter().any(|max_key| max_key == key) {
                        let incoming = Self::decode::<u64>(value)?;
                        let current = config
                            .get(key.as_str())
                            .map_err(Self::storage_error)?
                            .map(|value| Self::decode::<u64>(value.value()))
                            .transpose()?
                            .unwrap_or(0);
                        Self::encode(&current.max(incoming))?
                    } else {
                        value.clone()
                    };
                    config
                        .insert(key.as_str(), value.as_slice())
                        .map_err(Self::storage_error)?;
                    committed_config_values.push((key.clone(), value));
                }
            }
            if let (Some(lsn), Some(tx)) = (ws.commit_lsn, ws.visibility_floor) {
                let mut index = write_txn
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?;
                index.insert(lsn.0, tx.0).map_err(Self::storage_error)?;
            }
            Self::stage_graph_commit_support(&write_txn, graph_commit_support)?;
            if !change_log.is_empty() {
                let mut log = write_txn
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                let mut key = String::with_capacity(Self::change_log_entry_key_len());
                let mut encoded = Vec::new();
                for (index, entry) in change_log.iter().enumerate() {
                    Self::write_change_log_entry_key(lsn, index, &mut key);
                    self.encode_change_log_into_in_write(&write_txn, entry, &mut encoded)?;
                    log.insert(
                        observed_graph_key!("history", "insert", key.as_str(), 8),
                        encoded.as_slice(),
                    )
                    .map_err(Self::storage_error)?;
                    self.graph_writer_window(|| {
                        Self::write_change_log_selectors_in_write(
                            &write_txn,
                            key.as_str(),
                            encoded.as_slice(),
                        )
                    })?;
                }
            }
            let mut identity_mutations = operational_index_identity_mutations;
            for (table, meta) in &stage.table_meta {
                identity_mutations.extend(content_identity::schema_mutations(table, meta)?);
            }
            for key in &ws.config_deletes {
                identity_mutations.extend(content_identity::config_mutations(key, None));
            }
            for (key, value) in &committed_config_values {
                identity_mutations.extend(content_identity::config_mutations(key, Some(value)));
            }
            let mut grouped_ddl = BTreeMap::<Lsn, Vec<&DdlChange>>::new();
            for (ddl_lsn, change) in &stage.ddl_log {
                grouped_ddl.entry(*ddl_lsn).or_default().push(change);
            }
            for (ddl_lsn, changes) in grouped_ddl {
                for (index, change) in changes.iter().enumerate() {
                    let key = Self::ddl_log_key_for_index(ddl_lsn, index, changes.len());
                    identity_mutations
                        .push(content_identity::encoded_history_mutation(&key, *change)?);
                }
            }
            for (table, row_id, _) in &stage.operational_write_set.relational_deletes {
                identity_mutations.push(content_identity::remove_row_mutation(table, *row_id));
            }
            for (table, row) in &stage.operational_write_set.relational_inserts {
                identity_mutations.push(content_identity::row_mutation(
                    table,
                    row,
                    stage.operational_table_meta.get(table),
                )?);
            }
            let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
            let mut change_key = String::with_capacity(Self::change_log_entry_key_len());
            for (index, _entry) in change_log.iter().enumerate() {
                Self::write_change_log_entry_key(lsn, index, &mut change_key);
                identity_mutations.push(Self::stored_change_log_mutation_in_write(
                    &write_txn,
                    &change_key,
                )?);
            }
            if let (Some(commit_lsn), Some(tx)) = (ws.commit_lsn, ws.visibility_floor) {
                identity_mutations.push(content_identity::commit_index_mutation(commit_lsn, tx));
            }
            if !identity_mutations.is_empty() {
                Self::apply_content_identity_mutations_in_write(
                    &write_txn,
                    &identity_mutations,
                    &mut self.proof_admission(),
                )?;
            }
            if take_received_schema_pre_commit_fault_for_test() {
                return Err(Error::Other(
                    ("injected received-schema Redb pre-commit failure".to_string()).into(),
                ));
            }
            #[cfg(any(test, feature = "test-seams"))]
            let durable_commit_started = std::time::Instant::now();
            self.preserve_graph_commit_support(
                &write_txn,
                graph_commit_support,
                ws.commit_lsn.zip(ws.visibility_floor),
            )?;
            // The grouping that decides the retention clocks is admitted first.
            let _clock_grouping = self
                .proof_accountant()
                .map(|accountant| {
                    crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                        accountant,
                        Self::retention_clock_grouping_bytes(ws),
                        "writer_source",
                        "retention_clock_grouping",
                        "Raise MEMORY_LIMIT to decide this commit's retention clocks.",
                    )
                })
                .transpose()?;
            Self::write_writer_recovery_bounds_in_write(&write_txn, Some(ws))?;
            {
                #[cfg(any(test, feature = "test-seams"))]
                graph_observation_checkpoint(10)?;
                let graph_commit_result = write_txn.commit();
                if graph_commit_result.is_ok() {
                    self.complete_writer_schema_change(schema.take());
                }
                #[cfg(any(test, feature = "test-seams"))]
                if graph_commit_result.is_ok() {
                    graph_publication_attempt.committed();
                    graph_observation_checkpoint(11)?;
                }
                graph_commit_result
            }
            .map_err(Self::storage_error)?;
            #[cfg(any(test, feature = "test-seams"))]
            crate::database::record_received_schema_persistence_profile_for_test(
                "noop-durable-commit",
                durable_commit_started,
            );
            Ok(())
        })?;
        #[cfg(any(test, feature = "test-seams"))]
        crate::database::record_received_schema_persistence_profile_for_test(
            "noop-flush-total",
            flush_started,
        );
        Ok(())
    }

    /// Persist a locally authored schema stage without replacing the received
    /// schema image. The caller has already prepared the exact affected table
    /// metadata and encoded sidecars; this transaction deliberately leaves
    /// all unrelated relation rows and durable state untouched.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn flush_local_schema_stage(
        &self,
        ws: &WriteSet,
        stage: &LocalSchemaPersistenceProjection,
    ) -> Result<()> {
        let _decode_scope = self.writer_decode_scope()?;
        self.prepare_proof_owner()?;
        let lsn = ws.commit_lsn.ok_or_else(|| {
            Error::Other(("local schema stage is missing its commit LSN".to_string()).into())
        })?;
        if stage.ddl.is_empty() {
            return Err(Error::Other(
                ("local schema stage contains no DDL".to_string()).into(),
            ));
        }
        let schema = self.prepare_writer_schema_change(
            std::iter::empty(),
            stage
                .affected_table_meta
                .iter()
                .map(|(name, meta)| (name.as_str(), meta)),
        )?;
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            let graph_commit_support = Self::capture_graph_commit_support(&write_txn, Some(lsn))?;

            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                for (table, meta) in &stage.affected_table_meta {
                    let key = Self::meta_key(table);
                    let encoded_schema =
                        self.schema_update_in_table(&meta_table, table, meta, &stage.ddl)?;
                    let encoded = &encoded_schema.bytes;
                    meta_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    vector_membership_pages::ensure_declared_routes(
                        &write_txn,
                        table,
                        meta,
                        self.proof_accountant(),
                    )?;
                }
            }

            // `open_table` is both the Redb table creation path and a no-op
            // for an existing relation. Do not delete, scan, or rewrite rows:
            // another committed local table must remain byte-for-byte intact.
            for table in stage.affected_table_meta.keys() {
                let table_name = Self::rel_table_name(table);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let _relation = content_size::open_rows(
                    &write_txn,
                    table_def,
                    stage.affected_table_meta.get(table),
                )
                .map_err(Self::storage_error)?;
            }
            for (table, meta) in &stage.affected_table_meta {
                Self::rebuild_relational_indexes_in_write(
                    &write_txn,
                    &self.path,
                    table,
                    meta,
                    None,
                    &mut self.proof_admission(),
                    None,
                )?;
            }

            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for key in &ws.config_deletes {
                    Self::update_policy_hold_release_index(&write_txn, key, false)?;
                    config_table
                        .remove(key.as_str())
                        .map_err(Self::storage_error)?;
                }
                for (key, encoded) in stage.config_values.iter().chain(&ws.config_writes) {
                    Self::raise_config_identity_floors_in_write(&write_txn, key, encoded)?;
                    Self::update_policy_hold_release_index(&write_txn, key, true)?;
                    config_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                for (index, change) in stage.ddl.iter().enumerate() {
                    let key = Self::ddl_log_key_for_index(lsn, index, stage.ddl.len());
                    self.append_ddl_summary_in_write(&write_txn, &key, change)?;
                    let encoded = Self::encode(change)?;
                    ddl_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            {
                let mut index = write_txn
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?;
                index
                    .insert(lsn.0, stage.commit_index_tx.0)
                    .map_err(Self::storage_error)?;
            }

            let mut identity_mutations = Vec::new();
            for (table, meta) in &stage.affected_table_meta {
                identity_mutations.extend(content_identity::schema_mutations(table, meta)?);
            }
            for key in &ws.config_deletes {
                identity_mutations.extend(content_identity::config_mutations(key, None));
            }
            for (key, encoded) in stage.config_values.iter().chain(&ws.config_writes) {
                Self::raise_config_identity_floors_in_write(&write_txn, key, encoded)?;
                identity_mutations.extend(content_identity::config_mutations(key, Some(encoded)));
            }
            for (index, change) in stage.ddl.iter().enumerate() {
                let key = Self::ddl_log_key_for_index(lsn, index, stage.ddl.len());
                identity_mutations.push(content_identity::encoded_history_mutation(&key, change)?);
            }
            identity_mutations.push(content_identity::commit_index_mutation(
                lsn,
                stage.commit_index_tx,
            ));
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &identity_mutations,
                &mut self.proof_admission(),
            )?;

            if take_received_schema_pre_commit_fault_for_test() {
                return Err(Error::Other(
                    ("injected received-schema Redb pre-commit failure".to_string()).into(),
                ));
            }
            self.preserve_graph_commit_support(
                &write_txn,
                graph_commit_support,
                Some((lsn, stage.commit_index_tx)),
            )?;
            // The grouping that decides the retention clocks is admitted first.
            let _clock_grouping = self
                .proof_accountant()
                .map(|accountant| {
                    crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                        accountant,
                        Self::retention_clock_grouping_bytes(ws),
                        "writer_source",
                        "retention_clock_grouping",
                        "Raise MEMORY_LIMIT to decide this commit's retention clocks.",
                    )
                })
                .transpose()?;
            Self::write_writer_recovery_bounds_in_write(&write_txn, Some(ws))?;
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
            self.complete_writer_schema_change(schema);
            Ok(())
        })
    }

    pub(super) fn change_log_entry_references_any_table(
        entry: &ChangeLogEntry,
        tables: &HashSet<String>,
    ) -> bool {
        match entry {
            ChangeLogEntry::RowInsert { table, .. } | ChangeLogEntry::RowDelete { table, .. } => {
                tables.contains(table)
            }
            ChangeLogEntry::VectorInsert { index, .. }
            | ChangeLogEntry::VectorDelete { index, .. } => tables.contains(&index.table),
            ChangeLogEntry::EdgeInsert { .. } | ChangeLogEntry::EdgeDelete { .. } => false,
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn flush_table_meta(&self, name: &str, meta: &TableMeta) -> Result<()> {
        let _decode_scope = self.writer_decode_scope()?;
        self.prepare_proof_owner()?;
        let schema =
            self.prepare_writer_schema_change(std::iter::empty(), std::iter::once((name, meta)))?;
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::meta_key(name);
                let encoded_schema = self.schema_update_in_table(&meta_table, name, meta, &[])?;
                let encoded = &encoded_schema.bytes;
                meta_table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
                vector_membership_pages::ensure_declared_routes(
                    &write_txn,
                    name,
                    meta,
                    self.proof_accountant(),
                )?;
            }
            Self::rebuild_relational_indexes_in_write(
                &write_txn,
                &self.path,
                name,
                meta,
                None,
                &mut self.proof_admission(),
                None,
            )?;
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &content_identity::schema_mutations(name, meta)?,
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
            self.complete_writer_schema_change(schema);
            Ok(())
        })
    }

    /// Durable CREATE TABLE foundation: the table declaration, its one
    /// authoritative schema generation, immutable DDL provenance, and the
    /// matching DDL log entry must become visible together. This prevents
    /// CREATE from retaining the former meta-then-log crash window.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn flush_table_meta_with_config_values_and_append_ddl_log(
        &self,
        name: &str,
        meta: &TableMeta,
        config_values: Vec<(&str, Vec<u8>)>,
        lsn: Lsn,
        ddl: &DdlChange,
    ) -> Result<()> {
        self.publish_table_metadata_and_ddl(
            name,
            meta,
            config_values,
            lsn,
            std::slice::from_ref(ddl),
            true,
            None,
            || {},
        )
    }

    /// The caller proved that only search breadth or consolidation differs. Existing rows,
    /// postings, counts, vector bodies and authored history remain valid.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn publish_vector_policy_metadata_and_ddl(
        &self,
        name: &str,
        meta: &TableMeta,
        config_values: Vec<(&str, Vec<u8>)>,
        lsn: Lsn,
        ddl: &[DdlChange],
        original: &crate::memory_accounting::OwnedMemoryReservation,
        after_commit: impl FnOnce(),
    ) -> Result<()> {
        let scope = native_storage_admission::StorageAdmissionScope::writer_original(original)?;
        scope.run_original_unqualified(|| {
            self.publish_table_metadata_and_ddl(
                name,
                meta,
                config_values,
                lsn,
                ddl,
                false,
                Some(original),
                after_commit,
            )
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn publish_table_metadata_and_ddl(
        &self,
        name: &str,
        meta: &TableMeta,
        config_values: Vec<(&str, Vec<u8>)>,
        lsn: Lsn,
        ddl: &[DdlChange],
        rebuild_indexes: bool,
        original: Option<&crate::memory_accounting::OwnedMemoryReservation>,
        after_commit: impl FnOnce(),
    ) -> Result<()> {
        let _decode_scope = self.writer_decode_scope()?;
        #[cfg(any(test, feature = "test-seams"))]
        let phase = crate::read_image_memory::MaintenancePhase::enter("policy_metadata");
        self.prepare_proof_owner()?;
        let mut schema = match original {
            Some(original) => self.prepare_writer_policy_schema_change(original, name, meta)?,
            None => self
                .prepare_writer_schema_change(std::iter::empty(), std::iter::once((name, meta)))?,
        };
        let mut after_commit = Some(after_commit);
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            let mut identity_mutations = content_identity::schema_mutations(name, meta)?;
            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::meta_key(name);
                let encoded_schema = self.schema_update_in_table(&meta_table, name, meta, ddl)?;
                let encoded = &encoded_schema.bytes;
                meta_table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
                vector_membership_pages::ensure_declared_routes(
                    &write_txn,
                    name,
                    meta,
                    self.proof_accountant(),
                )?;
            }
            if rebuild_indexes {
                Self::rebuild_relational_indexes_in_write(
                    &write_txn,
                    &self.path,
                    name,
                    meta,
                    None,
                    &mut self.proof_admission(),
                    None,
                )?;
            }
            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, encoded) in config_values {
                    Self::raise_config_identity_floors_in_write(&write_txn, key, &encoded)?;
                    Self::update_policy_hold_release_index(&write_txn, key, true)?;
                    config_table
                        .insert(key, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    identity_mutations
                        .extend(content_identity::config_mutations(key, Some(&encoded)));
                }
            }
            {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                for (ordinal, change) in ddl.iter().enumerate() {
                    let key = Self::ddl_log_key_for_index(lsn, ordinal, ddl.len());
                    self.append_ddl_summary_in_write(&write_txn, &key, change)?;
                    let encoded = Self::encode(change)?;
                    ddl_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    identity_mutations
                        .push(content_identity::encoded_history_mutation(&key, change)?);
                }
            }
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &identity_mutations,
                &mut self.proof_admission(),
            )?;
            if !rebuild_indexes {
                Self::raise_table_read_lsn_in_write(&write_txn, name, lsn)?;
            }
            Self::raise_noncanonical_writer_floors_in_write(
                &write_txn,
                TxId(0),
                Lsn(lsn.0),
                RowId(0),
            )?;
            Self::write_writer_recovery_bounds_in_write(&write_txn, None)?;
            {
                #[cfg(any(test, feature = "test-seams"))]
                if !rebuild_indexes {
                    policy_metadata_checkpoint_for_test(10)?;
                }
                #[cfg(any(test, feature = "test-seams"))]
                graph_observation_checkpoint(10)?;
                #[cfg(any(test, feature = "test-seams"))]
                phase.record("durable_commit_enter", 0, None);
                let graph_commit_result = write_txn.commit();
                #[cfg(any(test, feature = "test-seams"))]
                if graph_commit_result.is_ok() {
                    phase.record("durable_commit_exit", 0, None);
                }
                if !rebuild_indexes && graph_commit_result.is_ok() {
                    self.complete_writer_schema_change(schema.take());
                    after_commit.take().expect("prepared metadata publication")();
                }
                #[cfg(any(test, feature = "test-seams"))]
                if graph_commit_result.is_ok() {
                    graph_publication_attempt.committed();
                    graph_observation_checkpoint(11)?;
                    if !rebuild_indexes {
                        policy_metadata_checkpoint_for_test(11)?;
                    }
                }
                graph_commit_result
            }
            .map_err(Self::storage_error)?;
            if rebuild_indexes {
                self.complete_writer_schema_change(schema);
                after_commit.take().expect("prepared metadata publication")();
            }
            Ok(())
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn remove_table_meta(&self, name: &str) -> Result<()> {
        self.prepare_proof_owner()?;
        let schema =
            self.prepare_writer_schema_change(std::iter::once(name), std::iter::empty())?;
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::meta_key(name);
                meta_table
                    .remove(key.as_str())
                    .map_err(Self::storage_error)?;
            }
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &content_identity::remove_schema_mutations(name),
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
            self.complete_writer_schema_change(schema);
            Ok(())
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn flush_config_value<T: serde::Serialize>(&self, key: &str, value: &T) -> Result<()> {
        self.prepare_proof_owner()?;
        #[cfg(any(test, feature = "test-seams"))]
        if verified_ordered_run_test_seams::config_write_refused_for_test() {
            return Err(Error::Other(
                (format!("injected durable configuration write failure for {key}")).into(),
            ));
        }
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            Self::update_policy_hold_release_index(&write_txn, key, true)?;
            let encoded;
            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                encoded = Self::encode(value)?;
                config_table
                    .insert(key, encoded.as_slice())
                    .map_err(Self::storage_error)?;
            }
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &content_identity::config_mutations(key, Some(&encoded)),
                &mut self.proof_admission(),
            )?;
            Self::raise_config_identity_floors_in_write(&write_txn, key, &encoded)?;
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

    /// Commit the fixed control left by a schema memory refusal. Its proof is
    /// charged before allocation, without re-admitting the refused operation.
    /// Arbitrary configuration/data writes keep their ordinary admission path.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn flush_memory_schema_holdback_control(
        &self,
        key: &str,
        state: Option<&crate::sync_types::DurableSchemaSyncHoldback>,
    ) -> Result<()> {
        self.prepare_proof_owner()?;
        #[cfg(any(test, feature = "test-seams"))]
        if verified_ordered_run_test_seams::config_write_refused_for_test() {
            return Err(Error::Other(
                (format!("injected durable memory-hold configuration write failure for {key}"))
                    .into(),
            ));
        }
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            Self::update_policy_hold_release_index(&write_txn, key, state.is_some())?;
            let encoded = {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                match state {
                    Some(state) => {
                        let encoded = Self::encode(state)?;
                        config_table
                            .insert(key, encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        Some(encoded)
                    }
                    None => {
                        config_table.remove(key).map_err(Self::storage_error)?;
                        None
                    }
                }
            };
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &content_identity::config_mutations(key, encoded.as_deref()),
                &mut content_identity::ProofAdmission::fixed_refusal_control(
                    self.proof_accountant(),
                )
                .with_scratch_owner(self.proof_scratch_owner()),
            )?;
            if let Some(encoded) = &encoded {
                Self::raise_config_identity_floors_in_write(&write_txn, key, encoded)?;
            }
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

    /// Persist a sync transport cursor and its covered configuration identity
    /// in the same transaction. Configuration remains part of the committed
    /// image even when this cursor does not change user rows.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn flush_sync_progress_value<T: serde::Serialize>(
        &self,
        key: &str,
        value: &T,
    ) -> Result<()> {
        self.prepare_proof_owner()?;
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            Self::update_policy_hold_release_index(&write_txn, key, true)?;
            let encoded = Self::encode(value)?;
            {
                let mut config = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                config
                    .insert(key, encoded.as_slice())
                    .map_err(Self::storage_error)?;
            }
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &content_identity::config_mutations(key, Some(&encoded)),
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
    pub(crate) fn encode_config_value<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
        Self::encode(value)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn decode_config_value<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
        Self::decode(bytes)
    }

    /// Strict config decode for callers whose durable bytes are themselves an
    /// authority boundary. Unlike the compatibility decoder above, a valid
    /// prefix followed by stray bytes is corrupt rather than silently accepted.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn decode_config_value_exact<T: serde::de::DeserializeOwned>(
        bytes: &[u8],
    ) -> Result<T> {
        Self::decode_exact(bytes)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn flush_encoded_config_values_and_append_ddl_log(
        &self,
        config_values: Vec<(&str, Vec<u8>)>,
        lsn: Lsn,
        ddl: &[DdlChange],
    ) -> Result<()> {
        self.prepare_proof_owner()?;
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            let mut identity_mutations = Vec::new();
            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, encoded) in config_values {
                    Self::raise_config_identity_floors_in_write(&write_txn, key, &encoded)?;
                    Self::update_policy_hold_release_index(&write_txn, key, true)?;
                    config_table
                        .insert(key, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    identity_mutations
                        .extend(content_identity::config_mutations(key, Some(&encoded)));
                }
            }
            {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                for (index, change) in ddl.iter().enumerate() {
                    let key = Self::ddl_log_key_for_index(lsn, index, ddl.len());
                    self.append_ddl_summary_in_write(&write_txn, &key, change)?;
                    let encoded = Self::encode(change)?;
                    ddl_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    identity_mutations
                        .push(content_identity::encoded_history_mutation(&key, change)?);
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
                Lsn(lsn.0),
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
    pub fn flush_encoded_config_values(&self, config_values: Vec<(&str, Vec<u8>)>) -> Result<()> {
        self.prepare_proof_owner()?;
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            let mut identity_mutations = Vec::new();
            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, encoded) in config_values {
                    Self::raise_config_identity_floors_in_write(&write_txn, key, &encoded)?;
                    Self::update_policy_hold_release_index(&write_txn, key, true)?;
                    config_table
                        .insert(key, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    identity_mutations
                        .extend(content_identity::config_mutations(key, Some(&encoded)));
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

    pub(crate) fn policy_hold_release_marker(key: &str) -> Option<String> {
        key.strip_prefix("sync_policy_hold_release.v1.")
            .map(|digest| format!("sync_policy_hold_released.v1.{digest}"))
    }

    /// Release membership is derived alongside the authoritative config fact.
    /// Its absence can be checked without loading a neighboring hold payload.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn update_policy_hold_release_index(
        write_txn: &redb::WriteTransaction,
        key: &str,
        present: bool,
    ) -> Result<()> {
        let Some(marker) = Self::policy_hold_release_marker(key) else {
            return Ok(());
        };
        let mut indexes = write_txn
            .open_table(POLICY_HOLD_INDEX_TABLE)
            .map_err(Self::storage_error)?;
        if present {
            indexes
                .insert(marker.as_str(), &[1u8][..])
                .map_err(Self::storage_error)?;
        } else {
            indexes
                .remove(marker.as_str())
                .map_err(Self::storage_error)?;
        }
        Ok(())
    }

    /// Change a primary configuration record and its selection indexes atomically.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn update_indexed_config_values(
        &self,
        puts: &[(String, Vec<u8>)],
        removes: &[String],
    ) -> Result<()> {
        self.prepare_proof_owner()?;
        #[cfg(any(test, feature = "test-seams"))]
        if verified_ordered_run_test_seams::config_write_refused_for_test() {
            return Err(Error::Other(
                "injected durable configuration write failure".into(),
            ));
        }
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            let mut mutations = Vec::new();
            {
                let mut table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                let mut indexes = write_txn
                    .open_table(POLICY_HOLD_INDEX_TABLE)
                    .map_err(Self::storage_error)?;
                for key in removes {
                    if policy_hold_index_key(key) {
                        indexes.remove(key.as_str()).map_err(Self::storage_error)?;
                    } else {
                        table.remove(key.as_str()).map_err(Self::storage_error)?;
                        mutations.extend(content_identity::config_mutations(key, None));
                    }
                }
                for (key, bytes) in puts {
                    if policy_hold_index_key(key) {
                        indexes
                            .insert(key.as_str(), bytes.as_slice())
                            .map_err(Self::storage_error)?;
                    } else {
                        table
                            .insert(key.as_str(), bytes.as_slice())
                            .map_err(Self::storage_error)?;
                        mutations.extend(content_identity::config_mutations(key, Some(bytes)));
                    }
                }
            }
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
            .map_err(Self::storage_error)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn remove_config_value(&self, key: &str) -> Result<()> {
        self.prepare_proof_owner()?;
        #[cfg(any(test, feature = "test-seams"))]
        if verified_ordered_run_test_seams::config_write_refused_for_test() {
            return Err(Error::Other(
                (format!("injected durable configuration removal failure for {key}")).into(),
            ));
        }
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            Self::update_policy_hold_release_index(&write_txn, key, false)?;
            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                config_table.remove(key).map_err(Self::storage_error)?;
            }
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &content_identity::config_mutations(key, None),
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
    pub fn load_sink_queue<T: serde::de::DeserializeOwned>(&self, sink: &str) -> Result<Vec<T>> {
        let _decode_scope = self.reader_decode_scope()?;
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
                let (_, value) = entry.map_err(Self::storage_error)?;
                entries.push(Self::decode(value.value())?);
            }
            Ok(entries)
        })
    }

    /// One bounded read of a durable sink queue in a single read transaction:
    /// its depth and newest id from the table root and last key, then the
    /// entries with ids in `(after, upper]` in ascending id order (`after =
    /// None` starts at the lowest id, `upper = None` ends at the newest).
    /// Each entry is decoded under a fresh writer-funded owner that keeps its
    /// actual decoded heap charged; the read stops once `max_entries` entries
    /// or `max_bytes` decoded bytes are held (the first entry is always kept,
    /// so one entry wider than `max_bytes` is read alone). A memory refusal
    /// before any entry is held is returned as `refusal`, with depth and
    /// newest id still read; a refusal after that just ends the window.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_sink_queue_window<T: serde::de::DeserializeOwned>(
        &self,
        sink: &str,
        after: Option<u64>,
        upper: Option<u64>,
        max_entries: usize,
        max_bytes: usize,
        accountant: crate::memory_accounting::AccountantHandle,
    ) -> Result<SinkQueueWindowRead<T>> {
        let owner = match crate::read_image_memory::ImageMemory::operation_writer(accountant) {
            Ok(owner) => owner,
            Err(error) if crate::read_image_memory::is_memory_refusal(&error) => {
                let mut read = self.load_sink_queue_extent(sink)?;
                read.refusal = Some(error);
                return Ok(read);
            }
            Err(error) => return Err(error),
        };
        let _decode_scope = owner.enter();
        let mut read = self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table_name = Self::sink_queue_table_name(sink);
            let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new(table_name.as_str());
            let table = match read_txn.open_table(table_def) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => {
                    return Ok(SinkQueueWindowRead::empty(0, 0));
                }
                Err(err) => return Err(Self::storage_error(err)),
            };
            let depth = table.len().map_err(Self::storage_error)?;
            let newest_id = table
                .last()
                .map_err(Self::storage_error)?
                .map(|(key, _)| key.value())
                .unwrap_or(0);
            let mut read = SinkQueueWindowRead::empty(depth, newest_id);
            let start = match after {
                Some(after) => match after.checked_add(1) {
                    Some(start) => start,
                    None => return Ok(read),
                },
                None => 0,
            };
            let end = upper.unwrap_or(u64::MAX);
            if start > end {
                return Ok(read);
            }
            // The window's own buffers are funded before they are allocated.
            let capacity = max_entries.min(usize::try_from(depth).unwrap_or(usize::MAX));
            let buffers = capacity.saturating_mul(
                std::mem::size_of::<T>().saturating_add(std::mem::size_of::<usize>()),
            );
            if let Err(error) = owner.reserve(buffers) {
                if !crate::read_image_memory::is_memory_refusal(&error) {
                    return Err(error);
                }
                read.refusal = Some(error);
                read.exhausted = false;
                return Ok(read);
            }
            read.entries = Vec::with_capacity(capacity);
            read.credits = Vec::with_capacity(capacity);
            let mut held = 0usize;
            for entry in table.range(start..=end).map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                if read.entries.len() >= max_entries {
                    read.exhausted = false;
                    break;
                }
                let before = owner.held();
                let decoded = match Self::decode::<T>(value.value()) {
                    Ok(decoded) => decoded,
                    Err(error) if crate::read_image_memory::is_memory_refusal(&error) => {
                        if read.entries.is_empty() {
                            read.refusal = Some(error);
                        }
                        read.exhausted = false;
                        break;
                    }
                    Err(error) => return Err(error),
                };
                let credit = owner.held().saturating_sub(before);
                if !read.entries.is_empty() && held.saturating_add(credit) > max_bytes {
                    drop(decoded);
                    owner.release(credit);
                    read.exhausted = false;
                    break;
                }
                held = held.saturating_add(credit);
                let _ = key;
                read.entries.push(decoded);
                read.credits.push(credit);
            }
            Ok(read)
        })?;
        // An empty read keeps no owner; dropping it returns its buffer credit.
        if !read.entries.is_empty() {
            read.owner = Some(owner);
        }
        Ok(read)
    }

    /// Depth and newest id of one durable sink queue, without its entries.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn load_sink_queue_extent<T>(&self, sink: &str) -> Result<SinkQueueWindowRead<T>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table_name = Self::sink_queue_table_name(sink);
            let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new(table_name.as_str());
            let table = match read_txn.open_table(table_def) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => {
                    return Ok(SinkQueueWindowRead::empty(0, 0));
                }
                Err(err) => return Err(Self::storage_error(err)),
            };
            let depth = table.len().map_err(Self::storage_error)?;
            let newest_id = table
                .last()
                .map_err(Self::storage_error)?
                .map(|(key, _)| key.value())
                .unwrap_or(0);
            let mut read = SinkQueueWindowRead::empty(depth, newest_id);
            read.exhausted = depth == 0;
            Ok(read)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn update_sink_queue_entry<T: serde::Serialize>(
        &self,
        sink: &str,
        remove_id: Option<u64>,
        put: Option<(u64, &T)>,
        uncertain: bool,
        unconfirmed_delta: i8,
    ) -> Result<()> {
        let _decode_scope = self.writer_decode_scope()?;
        self.prepare_proof_owner()?;
        if remove_id.is_none() && put.is_none() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            let mut identity_mutations = Vec::new();
            {
                let table_name = Self::sink_queue_table_name(sink);
                let table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                if let Some(id) = remove_id {
                    table.remove(id).map_err(Self::storage_error)?;
                    let key = format!("{}:{sink}:{id}", sink.len());
                    identity_mutations.push(content_identity::remove_status_mutation(
                        b"sink-queue",
                        key.as_bytes(),
                    ));
                }
                if let Some((id, entry)) = put {
                    let encoded = Self::encode(entry)?;
                    table
                        .insert(id, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    let key = format!("{}:{sink}:{id}", sink.len());
                    identity_mutations.push(content_identity::encoded_status_mutation(
                        b"sink-queue",
                        key.as_bytes(),
                        entry,
                    )?);
                }
            }
            if uncertain {
                let key = format!(
                    "{}{}",
                    crate::database::event_bus::UNCERTAIN_COUNT_CONFIG_PREFIX,
                    sink
                );
                let mut table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                let previous: u64 = match table.get(key.as_str()).map_err(Self::storage_error)? {
                    Some(value) => Self::decode(value.value())?,
                    None => 0,
                };
                let encoded = Self::encode(&previous.saturating_add(1))?;
                table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
                identity_mutations.extend(content_identity::config_mutations(&key, Some(&encoded)));
            }
            if unconfirmed_delta != 0 {
                let key = format!(
                    "{}{}",
                    crate::database::event_bus::UNCONFIRMED_COUNT_CONFIG_PREFIX,
                    sink
                );
                let mut table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                let previous: u64 = match table.get(key.as_str()).map_err(Self::storage_error)? {
                    Some(value) => Self::decode(value.value())?,
                    None => 0,
                };
                let next = if unconfirmed_delta > 0 {
                    previous.checked_add(1)
                } else {
                    previous.checked_sub(1)
                }
                .ok_or_else(|| Error::Other(("sink attempt summary overflow".to_owned()).into()))?;
                let encoded = Self::encode(&next)?;
                table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
                identity_mutations.extend(content_identity::config_mutations(&key, Some(&encoded)));
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
    pub fn remove_sink_queue_entries(
        &self,
        sink: &str,
        remove_ids: &[u64],
        unconfirmed: u64,
    ) -> Result<()> {
        let _decode_scope = self.writer_decode_scope()?;
        self.prepare_proof_owner()?;
        if remove_ids.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            let mut identity_mutations = Vec::new();
            {
                let table_name = Self::sink_queue_table_name(sink);
                let table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                for id in remove_ids {
                    table.remove(*id).map_err(Self::storage_error)?;
                    let key = format!("{}:{sink}:{id}", sink.len());
                    identity_mutations.push(content_identity::remove_status_mutation(
                        b"sink-queue",
                        key.as_bytes(),
                    ));
                }
            }
            if unconfirmed > 0 {
                let key = format!(
                    "{}{}",
                    crate::database::event_bus::UNCONFIRMED_COUNT_CONFIG_PREFIX,
                    sink
                );
                let mut table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                let previous: u64 = match table.get(key.as_str()).map_err(Self::storage_error)? {
                    Some(value) => Self::decode(value.value())?,
                    None => 0,
                };
                let next = previous.checked_sub(unconfirmed).ok_or_else(|| {
                    Error::Other(("sink attempt summary underflow".to_owned()).into())
                })?;
                let encoded = Self::encode(&next)?;
                table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
                identity_mutations.extend(content_identity::config_mutations(&key, Some(&encoded)));
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
    pub fn append_change_log(&self, lsn: Lsn, entries: &[ChangeLogEntry]) -> Result<()> {
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
                entries.iter().enumerate().map(|(i, entry)| (lsn, i, entry)),
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
    pub fn append_ddl_log(&self, lsn: Lsn, change: &DdlChange) -> Result<()> {
        self.prepare_proof_owner()?;
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            let key = Self::ddl_log_key(lsn);
            self.append_ddl_summary_in_write(&write_txn, &key, change)?;
            let encoded = Self::encode(change)?;
            {
                let mut table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
            }
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &[content_identity::encoded_history_mutation(&key, change)?],
                &mut self.proof_admission(),
            )?;
            Self::raise_noncanonical_writer_floors_in_write(
                &write_txn,
                TxId(0),
                Lsn(lsn.0),
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
    pub fn append_trigger_audit(&self, index: u64, entry: &TriggerAuditEntry) -> Result<()> {
        let _decode_scope = self.writer_decode_scope()?;
        self.prepare_proof_owner()?;
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            let mut identity_mutations = Vec::new();
            {
                let mut table = write_txn
                    .open_table(TRIGGER_AUDIT_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::trigger_audit_key(index, &entry.trigger_name);
                let encoded = Self::encode(entry)?;
                table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
                let mut stamps = write_txn
                    .open_table(TRIGGER_AUDIT_STAMPS_TABLE)
                    .map_err(Self::storage_error)?;
                let stamped_at = Wallclock::now().0;
                stamps
                    .insert(key.as_str(), stamped_at)
                    .map_err(Self::storage_error)?;
                identity_mutations.push(content_identity::status_mutation(
                    b"trigger-audit",
                    key.as_bytes(),
                    &encoded,
                ));
                identity_mutations.push(content_identity::status_mutation(
                    b"trigger-stamp",
                    key.as_bytes(),
                    &stamped_at.to_le_bytes(),
                ));
            }
            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                let mut ring: Vec<TriggerAuditEntry> = config_table
                    .get(TRIGGER_AUDIT_RING_CONFIG_KEY)
                    .map_err(Self::storage_error)?
                    .map(|value| Self::decode(value.value()))
                    .transpose()?
                    .unwrap_or_default();
                ring.push(entry.clone());
                let overflow = ring
                    .len()
                    .saturating_sub(crate::database::trigger::TRIGGER_AUDIT_RING_CAPACITY);
                if overflow > 0 {
                    ring.drain(0..overflow);
                }
                let current_next_index = config_table
                    .get(TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY)
                    .map_err(Self::storage_error)?
                    .map(|value| Self::decode::<u64>(value.value()))
                    .transpose()?
                    .unwrap_or(0);
                let next_index = index.saturating_add(1).max(current_next_index);
                let encoded_ring = Self::encode(&ring)?;
                config_table
                    .insert(TRIGGER_AUDIT_RING_CONFIG_KEY, encoded_ring.as_slice())
                    .map_err(Self::storage_error)?;
                identity_mutations.extend(content_identity::config_mutations(
                    TRIGGER_AUDIT_RING_CONFIG_KEY,
                    Some(&encoded_ring),
                ));
                let encoded_next_index = Self::encode(&next_index)?;
                config_table
                    .insert(
                        TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY,
                        encoded_next_index.as_slice(),
                    )
                    .map_err(Self::storage_error)?;
                identity_mutations.extend(content_identity::config_mutations(
                    TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY,
                    Some(&encoded_next_index),
                ));
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
    pub fn rewrite_vectors(&self, vectors: &[VectorEntry]) -> Result<()> {
        let table_meta = self.load_all_table_meta()?;
        let vector_quantization = Self::vector_quantization_map(&table_meta);
        let previous_vectors = self.load_vectors()?;
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            content_size::reset_vector_bytes(&write_txn)?;
            write_txn
                .delete_table(VECTORS_TABLE)
                .map_err(Self::storage_error)?;
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
            Self::reset_content_identity_families_in_write(
                &write_txn,
                &[
                    content_identity::LogicalFamily::F32Vectors,
                    content_identity::LogicalFamily::Sq8Vectors,
                    content_identity::LogicalFamily::Sq4Vectors,
                ],
                &mut self.proof_admission(),
            )?;
            let mut mutations = previous_vectors
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
            for entry in vectors {
                let quantization = vector_quantization
                    .get(&entry.index)
                    .copied()
                    .unwrap_or_default();
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
            for entry in vectors {
                Self::raise_table_read_lsn_in_write(&write_txn, &entry.index.table, entry.lsn)?;
                Self::raise_noncanonical_writer_floors_in_write(
                    &write_txn,
                    entry.created_tx.max(entry.deleted_tx.unwrap_or(TxId(0))),
                    entry.lsn,
                    entry.row_id,
                )?;
            }
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
    pub fn rewrite_graph_edges(&self, edges: &[AdjEntry]) -> Result<()> {
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            self.retire_graph_replacement_in_write(&write_txn, edges, None)?;
            write_txn
                .delete_table(GRAPH_FWD_TABLE)
                .map_err(Self::storage_error)?;
            write_txn
                .delete_table(GRAPH_REV_TABLE)
                .map_err(Self::storage_error)?;
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
                &[content_identity::LogicalFamily::Graph],
                &mut self.proof_admission(),
            )?;
            let mut ordered = edges.iter().collect::<Vec<_>>();
            ordered.sort_by_key(|edge| (edge.lsn, edge.created_tx));
            let mutations = ordered
                .into_iter()
                .map(content_identity::edge_mutation)
                .collect::<Result<Vec<_>>>()?;
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &mutations,
                &mut self.proof_admission(),
            )?;
            for edge in edges {
                Self::raise_noncanonical_writer_floors_in_write(
                    &write_txn,
                    edge.created_tx.max(edge.deleted_tx.unwrap_or(TxId(0))),
                    edge.lsn,
                    RowId(0),
                )?;
            }
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

    /// How many distinct prefix values this store currently holds a maintained
    /// count for, over exactly these columns. Inspection only: it counts
    /// records, never rows.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn stored_maintained_count_values(
        &self,
        table: &str,
        columns: &[(String, contextdb_core::SortDirection)],
    ) -> Result<u64> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let counts = match read_txn.open_table(RELATIONAL_INDEX_COUNT_TABLE) {
                Ok(counts) => counts,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let mut stored = 0u64;
            for entry in counts.iter().map_err(Self::storage_error)? {
                let (key, _) = entry.map_err(Self::storage_error)?;
                let decoded: EncodedRelationalIndexCountKey = Self::decode(key.value())?;
                if decoded.table == table && decoded.columns == columns {
                    stored = stored.saturating_add(1);
                }
            }
            Ok(stored)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn load_all_table_meta(&self) -> Result<HashMap<String, TableMeta>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let meta_table = match read_txn.open_table(META_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut tables = HashMap::new();
            for entry in meta_table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let key = key.value();
                if let Some(name) = key.strip_prefix("table:") {
                    let (meta, via_legacy) = Self::decode_table_meta_versioned(value.value())?;
                    if via_legacy {
                        self.used_legacy_table_meta_layout
                            .store(true, Ordering::Relaxed);
                    }
                    tables.insert(name.to_string(), meta);
                }
            }
            Ok(tables)
        })
    }

    /// Every config value whose key starts with `prefix`, decoded, in key order.
    /// The config keys are sorted, so a range from `prefix` and a stop at the
    /// first key that no longer shares it collects exactly the contiguous run.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn load_config_values_with_prefix<T: serde::de::DeserializeOwned>(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, T)>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let config_table = match read_txn.open_table(CONFIG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let mut out = Vec::new();
            for entry in config_table.range(prefix..).map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let key = key.value();
                if !key.starts_with(prefix) {
                    break;
                }
                #[cfg(any(test, feature = "test-seams"))]
                crate::writer_query_test_seams::note_config_enumeration(key);
                #[cfg(any(test, feature = "test-seams"))]
                crate::writer_query_test_seams::note_config_decode(key);
                out.push((key.to_string(), Self::decode(value.value())?));
            }
            Ok(out)
        })
    }

    /// Decode only the requested page. The extra key is inspected solely to
    /// report a continuation; its value is never copied or decoded.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_config_page_with_prefix<T: serde::de::DeserializeOwned>(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
        accountant: crate::memory_accounting::AccountantHandle,
    ) -> Result<(Vec<(String, T)>, bool)> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let config_table = match read_txn.open_table(CONFIG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok((Vec::new(), false)),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let memory = crate::read_image_memory::ImageMemory::charging_for(
                accountant,
                "sync_policy",
                "read_refused_outcome_page",
                "Reduce the page size or raise MEMORY_LIMIT before reading refusal outcomes.",
            );
            let mut page = Vec::new();
            let mut more = false;
            for entry in config_table
                .range(cursor.unwrap_or(prefix)..)
                .map_err(Self::storage_error)?
            {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let key = key.value();
                if !key.starts_with(prefix) {
                    break;
                }
                if cursor.is_some_and(|cursor| key <= cursor) {
                    continue;
                }
                if page.len() == limit {
                    more = true;
                    break;
                }
                #[cfg(any(test, feature = "test-seams"))]
                crate::writer_query_test_seams::note_config_enumeration(key);
                let bytes = value.value();
                memory.reserve(key.len().saturating_add(bytes.len()))?;
                #[cfg(any(test, feature = "test-seams"))]
                crate::writer_query_test_seams::note_config_decode(key);
                let decoded = memory.scope(|| Self::decode(bytes))?;
                page.push((key.to_owned(), decoded));
            }
            Ok((page, more))
        })
    }

    /// Inspect a peer-scoped run one value at a time and retain only the next
    /// source-order candidate. The digest suffix on durable keys has no source
    /// ordering, so selecting one candidate requires a scan of that peer's run.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_next_config_value_by_source<T, F>(
        &self,
        prefix: &str,
        after: Option<(u64, &str)>,
        accountant: crate::memory_accounting::AccountantHandle,
        source_lsn: F,
    ) -> Result<Option<(String, T, crate::read_image_memory::ImageMemoryHandle)>>
    where
        T: serde::de::DeserializeOwned,
        F: Fn(&T) -> Option<u64>,
    {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let config_table = match read_txn.open_table(CONFIG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let mut candidate: Option<(String, T, crate::read_image_memory::ImageMemoryHandle)> =
                None;
            for entry in config_table.range(prefix..).map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let key = key.value();
                if !key.starts_with(prefix) {
                    break;
                }
                let memory = crate::read_image_memory::ImageMemory::charging_for(
                    accountant.clone(),
                    "sync_policy",
                    "read_held_replay",
                    "Raise MEMORY_LIMIT before replaying this held data unit.",
                );
                #[cfg(any(test, feature = "test-seams"))]
                crate::writer_query_test_seams::note_config_enumeration(key);
                let bytes = value.value();
                memory.reserve(key.len().saturating_add(bytes.len()))?;
                #[cfg(any(test, feature = "test-seams"))]
                crate::writer_query_test_seams::note_config_decode(key);
                let decoded: T = memory.scope(|| Self::decode(bytes))?;
                let Some(position) = source_lsn(&decoded) else {
                    continue;
                };
                if after.is_some_and(|after| (position, key) <= after) {
                    continue;
                }
                if candidate.as_ref().is_some_and(|(best_key, best, _)| {
                    (position, key)
                        >= (
                            source_lsn(best).expect("selected peer value has a source position"),
                            best_key.as_str(),
                        )
                }) {
                    continue;
                }
                candidate = Some((key.to_owned(), decoded, memory));
            }
            Ok(candidate)
        })
    }
}
