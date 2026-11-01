use super::*;

impl Database {
    /// Read a committed image's history from its pinned snapshot the first
    /// time anything asks for it, and publish it into the same structures a
    /// live handle keeps. Until then those structures are empty and nothing
    /// that reads them runs. The history is admitted as it is read and every
    /// record is proved against the committed identity; the deleted edges'
    /// tombstones are checked against the commit index here, as a complete
    /// open checks them.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from executor.rs"
        )
    )]
    pub(crate) fn ensure_image_history(&self) -> Result<()> {
        self.ensure_image_history_with_read_memory(&|| Ok(()), self.declared_reader_memory()?)
            .map_err(ImageHistoryError::into_engine_error)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn ensure_image_history_with_read_memory(
        &self,
        checkpoint: &dyn Fn() -> Result<()>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<(), ImageHistoryError> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let Some(history) = self.image_history.as_ref() else {
            return Ok(());
        };
        if !history.load_history {
            return Ok(checkpoint()?);
        }
        let mut loaded = history.loaded.lock();
        if loaded.is_some() {
            // Already loaded: this request still owns its own cancellation
            // and active-time budget, and the work still ahead of it --
            // projecting and encoding an answer from what is cached -- is
            // real work a cached history must not exempt from either check.
            return Ok(checkpoint()?);
        }
        let read = history.source.load_history(
            &self.finalization.accountant,
            checkpoint,
            read_memory.clone(),
        )?;
        checkpoint()?;
        // The aux indexes are admitted and built only once the history that
        // feeds them is itself admitted: a room too tight for both is
        // refused here, before either structure exists, and the history
        // charge just taken is released with `read` on the way out.
        let indexes = crate::read_image_memory::ImageMemory::charging_reader(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            read_memory,
        );
        let (table_index, lsn_refcounts) =
            build_admitted_change_log_aux_indexes(&read.change_log, &indexes)?;
        self.with_commit_lock(|| -> Result<()> {
            *self.change_log.write() = read.change_log;
            *self.change_log_table_index.write() = table_index;
            *self.change_log_lsn_refcounts.write() = lsn_refcounts;
            *self.ddl_log.write() = read.ddl_log;
            Ok(())
        })
        .map_err(ImageHistoryError::from)?;
        *loaded = Some(LoadedImageHistoryCharge {
            _history: read.charge,
            _indexes: indexes,
        });
        Ok(())
    }

    /// Test-introspection: how many commit-index entries are retained, and the
    /// lowest LSN among them (`Lsn(0)` when the index is empty). Production-dead
    /// — nothing but tests reads it.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn commit_index_census_for_test(&self) -> (usize, Lsn) {
        let _operation = self.assert_open_operation();
        self.tx_mgr
            .try_commit_index_census()
            .expect("authenticated test commit census")
    }

    /// Test-introspection: how many commit-index entries sit strictly below
    /// `lsn`. The anchor rule wants exactly one after a prune — fewer drops the
    /// oldest surviving delete, more is unreclaimed growth. Production-dead.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn commit_index_entries_below_for_test(&self, lsn: Lsn) -> usize {
        let _operation = self.assert_open_operation();
        self.commit_index_for_test()
            .iter()
            .filter(|(entry, _)| *entry < lsn)
            .count()
    }

    /// Test-introspection: a sorted copy of `change_log_table_index`'s own
    /// `(lsn, row_id)` coordinates for one table -- the per-table shadow a
    /// scoped cleanup/retention pass reads instead of scanning the whole
    /// change log. Sorted so a caller can compare it against a
    /// content-derived multiset without depending on internal ordering.
    /// Production-dead -- nothing but tests reads it.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __change_log_table_index_for_test(&self, table: &str) -> Vec<(Lsn, RowId)> {
        let _operation = self.assert_open_operation();
        let mut coords = self
            .change_log_table_index
            .read()
            .get(table)
            .cloned()
            .unwrap_or_default();
        coords.sort_by_key(|(lsn, row_id)| (*lsn, *row_id));
        coords
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn set_sync_row_source_lsn(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        source_lsn: Lsn,
    ) -> Result<()> {
        self.set_sync_row_source_lsn_kind(
            tx,
            table,
            row_id,
            source_lsn,
            contextdb_relational::store::SyncSourceKind::Pulled,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn set_sync_row_source_lsn_kind(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        source_lsn: Lsn,
        kind: contextdb_relational::store::SyncSourceKind,
    ) -> Result<()> {
        let kind = match kind {
            contextdb_relational::store::SyncSourceKind::Pulled => 0,
            contextdb_relational::store::SyncSourceKind::AcceptedLocal => 1,
            contextdb_relational::store::SyncSourceKind::AcceptedLocalPending => 2,
        };
        match &self.tx_mgr {
            DatabaseTransactionHost::Legacy(manager) => manager.with_write_set(tx, |ws| {
                ws.set_relational_insert_source_lsn_kind(
                    table.to_string(),
                    row_id,
                    source_lsn,
                    kind,
                );
            }),
            DatabaseTransactionHost::Owned(handle) => handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.with_engine(tx, |guard| {
                        // Preserve the row's existing admission while replacing
                        // its paired source LSN/kind through one prepared map.
                        let admitted =
                            guard.with_legacy(|ws| ws.admitted_bytes_for_staged_row(table, row_id));
                        let prepared = guard.prepare_sidecars(
                            table,
                            row_id,
                            Some((source_lsn, kind)),
                            admitted,
                        )?;
                        guard.publish_tx(prepared);
                        Ok(())
                    })
                })
            }),
        }
    }

    /// Record sync provenance for a row this transaction leaves exactly as it
    /// stands. The row is not restaged: an immutable table refuses a rewrite,
    /// and republishing correct values would put a mutation in the change log
    /// that no peer asked for.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn mark_sync_row_provenance(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        source_lsn: Lsn,
        kind: contextdb_relational::store::SyncSourceKind,
    ) -> Result<()> {
        let kind = match kind {
            contextdb_relational::store::SyncSourceKind::Pulled => 0,
            contextdb_relational::store::SyncSourceKind::AcceptedLocal => 1,
            contextdb_relational::store::SyncSourceKind::AcceptedLocalPending => 2,
        };
        match &self.tx_mgr {
            DatabaseTransactionHost::Legacy(manager) => manager.with_write_set(tx, |ws| {
                ws.set_sync_source_provenance_mark(table.to_string(), row_id, source_lsn, kind);
            }),
            DatabaseTransactionHost::Owned(handle) => {
                // Reserve before materializing the name and transfer that
                // exact debit with its original body into the lower facade.
                let mut admission = self.reserve_original_preparation_for(
                    table.len(),
                    "sync",
                    "source_provenance",
                    "Reduce staged provenance or raise MEMORY_LIMIT.",
                )?;
                let body = contextdb_tx::staged_source::AuxBody::Provenance((
                    table.to_owned(),
                    row_id,
                    source_lsn,
                    kind,
                ));
                handle.with_host(|host| {
                    let invoice = admission.into_source_aux_invoice_for(&host.funder)?;
                    host.transactions.with_transactions(|transactions| {
                        transactions.with_engine(tx, |guard| {
                            let batch = guard.prepare_owned_aux(body, invoice)?;
                            let prepared = guard.prepare_aux_splice(
                                guard.read().sync_source_provenance_marks.len(),
                                &[],
                                batch,
                            )?;
                            guard.publish_tx(prepared);
                            Ok(())
                        })
                    })
                })
            }
        }
    }

    /// Route each row cell through `coerce_value_for_column` for variant
    /// compatibility. The one concession to historical `insert_row` behavior
    /// is that `Value::Vector` payloads are accepted regardless of declared
    /// dimension — prior integration suites (e.g. the 3-component probe into
    /// a VECTOR(384) embedding column) depend on the library API NOT enforcing
    /// dim equality there. SQL execution (`exec_insert`/`exec_update`) still
    /// performs the full dim check because it always threads through the
    /// executor module's coercion helpers.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn coerce_row_for_insert(
        &self,
        table: &str,
        values: HashMap<ColName, Value>,
        current_tx_max: Option<TxId>,
        active_tx: Option<TxId>,
    ) -> Result<HashMap<ColName, Value>> {
        let meta = self.table_meta(table);
        let mut out: HashMap<ColName, Value> = HashMap::with_capacity(values.len());
        for (col, v) in values {
            // Vector + Value::Vector: pass straight through (dim check happens on SQL path).
            let is_vector_bypass = matches!(&v, Value::Vector(_))
                && meta
                    .as_ref()
                    .and_then(|m| m.columns.iter().find(|c| c.name == col))
                    .map(|c| matches!(c.column_type, contextdb_core::ColumnType::Vector(_)))
                    .unwrap_or(false);

            let coerced = if is_vector_bypass {
                v
            } else {
                crate::executor::coerce_into_column(
                    self,
                    table,
                    &col,
                    v,
                    current_tx_max,
                    active_tx,
                )?
            };
            out.insert(col, coerced);
        }
        Ok(out)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_primary_key_values_present(
        &self,
        table: &str,
        values: &HashMap<ColName, Value>,
    ) -> Result<()> {
        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
        for column in &meta.columns {
            let is_primary_key = column.primary_key
                || meta
                    .primary_key_columns
                    .iter()
                    .any(|name| name == &column.name);
            if is_primary_key && matches!(values.get(&column.name), None | Some(Value::Null)) {
                return Err(Error::ColumnNotNullable {
                    table: table.to_string(),
                    column: column.name.clone(),
                });
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn insert_row_with_unique_noop(
        &self,
        tx: TxId,
        table: &str,
        mut values: HashMap<ColName, Value>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<InsertRowResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.ensure_trigger_table_ready(table, "insert_row")?;
        self.complete_insert_access_values(table, &mut values)?;
        let allow_duplicate_unique_noop =
            !self.table_meta(table).is_some_and(|meta| meta.immutable);
        match self.check_row_constraints(
            tx,
            table,
            &values,
            None,
            allow_duplicate_unique_noop,
            read_memory.clone(),
        )? {
            RowConstraintCheck::Valid => {
                let row_id = self.relational_store.new_row_id();
                self.assert_row_write_allowed(
                    table,
                    row_id,
                    &values,
                    self.snapshot_for_read(),
                    read_memory.clone(),
                )?;
                let natural_key = self
                    .table_meta(table)
                    .and_then(|meta| natural_key_from_row_values(&meta, &values));
                let row_vectors_source = values.clone();
                let staged_before = self.staged_row_count(tx);
                let mut values = values;
                self.blank_quantized_vector_values(table, &mut values);
                let inserted = self.insert_relational_row_from_read(
                    tx,
                    table,
                    row_id,
                    values,
                    self.snapshot_for_read(),
                    None,
                    read_memory.clone(),
                )?;
                self.write_row_vectors_or_unstage(
                    tx,
                    table,
                    inserted,
                    &row_vectors_source,
                    staged_before,
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
                if let Some(natural_key) = natural_key {
                    self.stage_local_creation_lineage(
                        tx,
                        table,
                        natural_key,
                        inserted,
                        read_memory.clone(),
                    )?;
                }
                Ok(InsertRowResult::Inserted(inserted))
            }
            RowConstraintCheck::DuplicateUniqueNoOp => Ok(InsertRowResult::NoOp),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn upsert_row(
        &self,
        tx: TxId,
        table: &str,
        conflict_col: &str,
        values: HashMap<ColName, Value>,
    ) -> Result<UpsertResult> {
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.upsert_row_with_read_memory(
            tx,
            table,
            conflict_col,
            values,
            &mut vector_copy_owner,
            read_memory,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn upsert_row_with_read_memory(
        &self,
        tx: TxId,
        table: &str,
        conflict_col: &str,
        mut values: HashMap<ColName, Value>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<UpsertResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        (|| {
            let _operation = self.open_operation_after_write_control_wait(tx, "upsert_row")?;
            self.ensure_trigger_table_ready(table, "upsert_row")?;
            self.complete_insert_access_values(table, &mut values)?;
            let checkpoint = self.write_set_checkpoint(tx)?;
            let mut admission = None;
            let mut staged_row = None;
            let result = self.upsert_row_within_operation(
                tx,
                table,
                conflict_col,
                values,
                &mut admission,
                &mut staged_row,
                vector_copy_owner,
                read_memory.clone(),
            );
            if result.is_err() {
                let _ = self.restore_write_set_checkpoint(tx, checkpoint);
            } else {
                if let (Some(admission), Some(row)) = (admission.take(), staged_row) {
                    self.record_owned_row_admission(tx, table, row, admission);
                }
                checkpoint.settle();
            }
            result
        })()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn upsert_row_within_operation(
        &self,
        tx: TxId,
        table: &str,
        conflict_col: &str,
        values: HashMap<ColName, Value>,
        admission: &mut Option<OwnedMemoryReservation>,
        staged_row: &mut Option<RowId>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<UpsertResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let snapshot = self.snapshot_for_read();
        let existing_row = values
            .get(conflict_col)
            .map(|conflict_value| {
                self.point_lookup_in_tx(
                    tx,
                    table,
                    conflict_col,
                    conflict_value,
                    snapshot,
                    read_memory.clone(),
                )
            })
            .transpose()?
            .flatten();
        let existing_row_id = existing_row.as_ref().map(|row| row.row_id);
        // Diff-respecting column-level IMMUTABLE check: reject any upsert whose
        // flagged-column value differs from the existing local value. Idempotent
        // replay (same-value) succeeds; new rows (no existing match) apply normally.
        if let (Some(existing), Some(meta)) = (existing_row.as_ref(), self.table_meta(table)) {
            for col_def in meta.columns.iter().filter(|c| c.immutable) {
                let Some(incoming) = values.get(&col_def.name) else {
                    continue;
                };
                let existing_value = existing.values.get(&col_def.name);
                if existing_value != Some(incoming) {
                    return Err(Error::ImmutableColumn {
                        table: table.to_string(),
                        column: col_def.name.clone(),
                    });
                }
            }
        }
        self.validate_row_constraints(tx, table, &values, existing_row_id, read_memory.clone())?;
        let Some(existing) = existing_row.as_ref() else {
            let row_id = self.relational_store.new_row_id();
            self.assert_row_write_allowed(table, row_id, &values, snapshot, read_memory.clone())?;

            let row_uuid = values.get("id").and_then(Value::as_uuid).copied();
            let meta = self.table_meta(table);
            let new_state = meta
                .as_ref()
                .and_then(|m| m.state_machine.as_ref())
                .and_then(|sm| values.get(&sm.column))
                .and_then(Value::as_text)
                .map(std::borrow::ToOwned::to_owned);
            let natural_key = meta
                .as_ref()
                .and_then(|meta| natural_key_from_row_values(meta, &values));

            // The same ownership the other write doors use: the vectors are
            // taken before the row is staged, the quantized slots are emptied
            // so the row costs what the column declared, the vectors are
            // placed in the store the column reads from, and a placement that
            // cannot be made takes the row back out. Without this an upserted
            // vector is in no index at all -- a search never finds the row,
            // and the caller is told the row was inserted.
            let row_vectors_source = values.clone();
            let staged_before = self.staged_row_count(tx);
            let mut values = values;
            self.blank_quantized_vector_values(table, &mut values);
            let row_meta = meta
                .as_ref()
                .ok_or_else(|| Error::TableNotFound(table.into()))?;
            *admission = Some(self.admit_retained_row_owned(
                table,
                &row_vectors_source,
                row_meta,
                "relational",
                "staged_index_posting",
                "Reduce row size or raise MEMORY_LIMIT.",
            )?);
            let inserted = self.insert_relational_row_from_read(
                tx,
                table,
                row_id,
                values,
                snapshot,
                None,
                read_memory.clone(),
            )?;
            *staged_row = Some(inserted);
            if let Some(admission) = admission.take() {
                self.record_owned_row_admission(tx, table, inserted, admission);
            }
            self.write_row_vectors_or_unstage(
                tx,
                table,
                inserted,
                &row_vectors_source,
                staged_before,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            if let Some(natural_key) = natural_key {
                self.stage_local_creation_lineage(
                    tx,
                    table,
                    natural_key,
                    inserted,
                    read_memory.clone(),
                )?;
            }
            if let (Some(uuid), Some(state), Some(_meta)) =
                (row_uuid, new_state.as_deref(), meta.as_ref())
            {
                self.propagate_state_change_if_needed(
                    tx,
                    table,
                    Some(uuid),
                    Some(state),
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
            }
            return Ok(UpsertResult::Inserted);
        };
        {
            self.assert_row_write_allowed(
                table,
                existing.row_id,
                &existing.values,
                snapshot,
                read_memory.clone(),
            )?;
            self.assert_row_write_allowed(
                table,
                existing.row_id,
                &values,
                snapshot,
                read_memory.clone(),
            )?;
        }

        let row_uuid = values.get("id").and_then(Value::as_uuid).copied();
        let meta = self.table_meta(table);
        let new_state = meta
            .as_ref()
            .and_then(|m| m.state_machine.as_ref())
            .and_then(|sm| values.get(&sm.column))
            .and_then(Value::as_text)
            .map(std::borrow::ToOwned::to_owned);

        // Compare what the store would hold against what it holds. A
        // quantized column keeps an approximation and the row itself keeps
        // nothing, so a diff against the caller's own full-precision input
        // reports a change on every upsert of the same vector -- and each one
        // would rewrite the row and place the vector again.
        let stored_now =
            self.outbound_row_values(table, existing, vector_copy_owner, read_memory.clone())?;
        let changed = values.iter().any(|(column, value)| {
            stored_now.get(column) != Some(&self.stored_form_of_value(table, column, value))
        });
        if !changed {
            return Ok(UpsertResult::NoOp);
        }
        self.validate_commit_time_upsert_state_transition(table, existing, &values)?;
        // Placing the new vector is what retires the old one: the store
        // deletes the entry this row already had before it takes the new one,
        // so an updating upsert leaves one vector behind, not two.
        let row_meta = meta
            .as_ref()
            .ok_or_else(|| Error::TableNotFound(table.into()))?;
        *admission = Some(self.admit_retained_row_owned(
            table,
            &values,
            row_meta,
            "relational",
            "staged_index_posting",
            "Reduce row size or raise MEMORY_LIMIT.",
        )?);
        let row_vectors = self.row_vector_values(table, &values);
        let mut values = values;
        self.blank_quantized_vector_values(table, &mut values);
        let replaced = self.replace_row_after_update_validation(
            tx,
            table,
            existing.row_id,
            values,
            snapshot,
            read_memory.clone(),
        )?;
        *staged_row = Some(replaced);
        if let Some(admission) = admission.take() {
            self.record_owned_row_admission(tx, table, replaced, admission);
        }
        self.write_row_vectors(
            tx,
            replaced,
            row_vectors,
            vector_copy_owner,
            read_memory.clone(),
        )?;

        if let (Some(uuid), Some(state), Some(_meta)) =
            (row_uuid, new_state.as_deref(), meta.as_ref())
        {
            self.propagate_state_change_if_needed(
                tx,
                table,
                Some(uuid),
                Some(state),
                vector_copy_owner,
                read_memory.clone(),
            )?;
        }

        Ok(UpsertResult::Updated)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_row_constraints(
        &self,
        tx: TxId,
        table: &str,
        values: &HashMap<ColName, Value>,
        skip_row_id: Option<RowId>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        match self.check_row_constraints(
            tx,
            table,
            values,
            skip_row_id,
            false,
            read_memory.clone(),
        )? {
            RowConstraintCheck::Valid => Ok(()),
            RowConstraintCheck::DuplicateUniqueNoOp => {
                unreachable!("strict constraint validation cannot return no-op")
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn check_row_constraints(
        &self,
        tx: TxId,
        table: &str,
        values: &HashMap<ColName, Value>,
        skip_row_id: Option<RowId>,
        allow_duplicate_unique_noop: bool,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<RowConstraintCheck> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // Constraint probes MUST see the current committed watermark, not any
        // thread-local override. A PK/UNIQUE violation on a committed row must
        // be detected even if the caller pinned a pre-violation snapshot for
        // read visibility.
        let published = self.tx_mgr.capture_published_read();
        let snapshot = published.snapshot;
        let _writer_read = self.enter_writer_read(
            writer_read::durable_root(published.handle)?,
            read_memory.clone(),
        );
        // Validating a write's constraints is write working memory: the
        // writer budget funds every lookup it makes, never the statement's
        // read limit.
        self.check_row_constraints_at_snapshot(
            tx,
            table,
            values,
            skip_row_id,
            allow_duplicate_unique_noop,
            snapshot,
            self.finalization.accountant.read_budget()?,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn check_row_constraints_at_snapshot(
        &self,
        tx: TxId,
        table: &str,
        values: &HashMap<ColName, Value>,
        skip_row_id: Option<RowId>,
        allow_duplicate_unique_noop: bool,
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<RowConstraintCheck> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let metas = self.relational_store.table_meta.read();
        let meta = metas
            .get(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

        for column in &meta.columns {
            let is_primary_key = column.primary_key
                || meta
                    .primary_key_columns
                    .iter()
                    .any(|name| name == &column.name);
            if is_primary_key && matches!(values.get(&column.name), None | Some(Value::Null)) {
                return Err(Error::ColumnNotNullable {
                    table: table.to_string(),
                    column: column.name.clone(),
                });
            }
        }

        // Scan the whole table only when no index covers any PK / UNIQUE
        // column we need to probe. Pulled lazily so the fast path skips it.
        let mut visible_rows_cache: Option<Vec<VersionedRow>> = None;

        for column in meta.columns.iter().filter(|column| column.primary_key) {
            let Some(value) = values.get(&column.name) else {
                continue;
            };
            if *value == Value::Null {
                continue;
            }
            match self.probe_column_for_constraint(
                tx,
                table,
                &column.name,
                value,
                snapshot,
                skip_row_id,
                read_memory.clone(),
            )? {
                ConstraintProbe::Match(_) => {
                    return Err(Error::UniqueViolation {
                        table: table.to_string(),
                        column: column.name.clone(),
                    });
                }
                ConstraintProbe::NoMatch => {}
                ConstraintProbe::NoIndex => {
                    // Fallback to full scan for PK columns without an index.
                    if visible_rows_cache.is_none() {
                        visible_rows_cache = Some(self.relational_scan_filter_with_tx(
                            Some(tx),
                            table,
                            snapshot,
                            &|row| skip_row_id.is_none_or(|row_id| row.row_id != row_id),
                            read_memory.clone(),
                        )?);
                    }
                    let rows = visible_rows_cache.as_deref().unwrap();
                    if rows
                        .iter()
                        .any(|existing| existing.values.get(&column.name) == Some(value))
                    {
                        return Err(Error::UniqueViolation {
                            table: table.to_string(),
                            column: column.name.clone(),
                        });
                    }
                }
            }
        }

        let mut duplicate_unique_row_id = None;

        for column in meta
            .columns
            .iter()
            .filter(|column| column.unique && !column.primary_key)
        {
            let Some(value) = values.get(&column.name) else {
                continue;
            };
            if *value == Value::Null {
                continue;
            }
            let matching_row_ids: Vec<RowId> = match self.probe_column_for_constraint(
                tx,
                table,
                &column.name,
                value,
                snapshot,
                skip_row_id,
                read_memory.clone(),
            )? {
                ConstraintProbe::Match(rid) => vec![rid],
                ConstraintProbe::NoMatch => Vec::new(),
                ConstraintProbe::NoIndex => {
                    if visible_rows_cache.is_none() {
                        visible_rows_cache = Some(self.relational_scan_filter_with_tx(
                            Some(tx),
                            table,
                            snapshot,
                            &|row| skip_row_id.is_none_or(|row_id| row.row_id != row_id),
                            read_memory.clone(),
                        )?);
                    }
                    let rows = visible_rows_cache.as_deref().unwrap();
                    rows.iter()
                        .filter(|existing| existing.values.get(&column.name) == Some(value))
                        .map(|existing| existing.row_id)
                        .collect()
                }
            };
            self.merge_unique_conflict(
                tx,
                table,
                &column.name,
                &matching_row_ids,
                allow_duplicate_unique_noop,
                &mut duplicate_unique_row_id,
            )?;
        }

        // A multi-column UNIQUE violation is a loud constraint error on every
        // INSERT path, exactly like a composite PRIMARY KEY duplicate (never
        // the single-column no-op-on-plain-insert convention
        // `allow_duplicate_unique_noop` governs below) — standard SQL never
        // silently drops a row on a UNIQUE violation. Same probe, one no-op
        // policy for both composite forms.
        let composite_pk =
            (meta.primary_key_columns.len() >= 2).then_some(&meta.primary_key_columns);
        for unique_constraint in meta.unique_constraints.iter().chain(composite_pk) {
            let mut candidate_values = Vec::with_capacity(unique_constraint.len());
            let mut has_null = false;

            for column_name in unique_constraint {
                match values.get(column_name) {
                    Some(Value::Null) | None => {
                        has_null = true;
                        break;
                    }
                    Some(value) => candidate_values.push(value.clone()),
                }
            }

            if has_null {
                continue;
            }

            let matching_row_ids: Vec<RowId> = if let Some(rid) = self.probe_composite_unique(
                tx,
                table,
                unique_constraint,
                &candidate_values,
                snapshot,
                skip_row_id,
                read_memory.clone(),
            )? {
                vec![rid]
            } else if self.index_covers_composite(table, unique_constraint) {
                Vec::new()
            } else {
                if visible_rows_cache.is_none() {
                    visible_rows_cache = Some(self.relational_scan_filter_with_tx(
                        Some(tx),
                        table,
                        snapshot,
                        &|row| skip_row_id.is_none_or(|row_id| row.row_id != row_id),
                        read_memory.clone(),
                    )?);
                }
                let rows = visible_rows_cache.as_deref().unwrap();
                rows.iter()
                    .filter(|existing| {
                        unique_constraint.iter().zip(candidate_values.iter()).all(
                            |(column_name, value)| existing.values.get(column_name) == Some(value),
                        )
                    })
                    .map(|existing| existing.row_id)
                    .collect()
            };
            // Report a composite UNIQUE violation naming every column of the
            // constraint, not just the first — the caller needs the whole
            // tuple to know which declared constraint fired.
            let column_label = unique_constraint.join(", ");
            self.merge_unique_conflict(
                tx,
                table,
                &column_label,
                &matching_row_ids,
                false,
                &mut duplicate_unique_row_id,
            )?;
        }

        if duplicate_unique_row_id.is_some() {
            Ok(RowConstraintCheck::DuplicateUniqueNoOp)
        } else {
            Ok(RowConstraintCheck::Valid)
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn merge_unique_conflict(
        &self,
        tx: TxId,
        table: &str,
        column: &str,
        matching_row_ids: &[RowId],
        allow_duplicate_unique_noop: bool,
        duplicate_unique_row_id: &mut Option<RowId>,
    ) -> Result<()> {
        if matching_row_ids.is_empty() {
            return Ok(());
        }

        if !allow_duplicate_unique_noop || matching_row_ids.len() != 1 {
            return Err(Error::UniqueViolation {
                table: table.to_string(),
                column: column.to_string(),
            });
        }

        let matched_row_id = matching_row_ids[0];
        if self.row_id_is_staged_insert(tx, table, matched_row_id)? {
            return Err(Error::UniqueViolation {
                table: table.to_string(),
                column: column.to_string(),
            });
        }

        if let Some(existing_row_id) = duplicate_unique_row_id {
            if *existing_row_id != matched_row_id {
                return Err(Error::UniqueViolation {
                    table: table.to_string(),
                    column: column.to_string(),
                });
            }
        } else {
            *duplicate_unique_row_id = Some(matched_row_id);
        }

        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn row_id_is_staged_insert(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
    ) -> Result<bool> {
        self.tx_mgr.with_read(tx, |ws| {
            ws.relational_inserts
                .iter()
                .any(|(staged_table, staged_row)| {
                    staged_table == table && staged_row.row_id == row_id
                })
        })
    }

    /// Returns true if `table` has an index whose leading key covers `column`.
    pub(super) fn index_covers_column(&self, table: &str, column: &str) -> bool {
        let indexes = self.relational_store.indexes.read();
        indexes.get(table).is_some_and(|table_indexes| {
            table_indexes
                .values()
                .any(|idx| idx.columns.first().is_some_and(|(name, _)| name == column))
        })
    }

    /// Returns true if `table` has an index whose first-column prefix contains
    /// exactly the columns in `cols` (same order).
    pub(super) fn index_covers_composite(&self, table: &str, cols: &[String]) -> bool {
        let indexes = self.relational_store.indexes.read();
        indexes.get(table).is_some_and(|table_indexes| {
            table_indexes.values().any(|idx| {
                idx.columns.len() >= cols.len()
                    && idx
                        .columns
                        .iter()
                        .zip(cols.iter())
                        .all(|((c, _), want)| c == want)
            })
        })
    }

    /// Look up `(table, column) = value` using a single-column index when one
    /// exists, layered with the tx's staged inserts and deletes.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn probe_column_for_constraint(
        &self,
        tx: TxId,
        table: &str,
        column: &str,
        value: &Value,
        snapshot: SnapshotId,
        skip_row_id: Option<RowId>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ConstraintProbe> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if self.persistence.is_some() {
            let columns = [column.to_owned()];
            if let Some(row) = self.writer_constraint_staged_match(
                tx,
                table,
                &columns,
                std::slice::from_ref(value),
                skip_row_id,
                read_memory.clone(),
            )? {
                return Ok(ConstraintProbe::Match(row));
            }
            return Ok(
                match self.writer_indexed_row_with_overlay(
                    table,
                    &columns,
                    std::slice::from_ref(value),
                    snapshot,
                    &HashSet::new(),
                    None,
                    Some((tx, skip_row_id)),
                    read_memory.clone(),
                )? {
                    None => ConstraintProbe::NoIndex,
                    Some(None) => ConstraintProbe::NoMatch,
                    Some(Some(row)) => ConstraintProbe::Match(row),
                },
            );
        }
        use contextdb_core::{DirectedValue, TotalOrdAsc};
        let (tx_staged_deletes, tx_delete_predicates, staged_overlap) =
            self.tx_mgr.with_read(tx, |ws| {
                // Rows this tx has already staged for delete must not be treated as
                // obstructions by the constraint probe. The old index entry still
                // looks visible at the committed-watermark snapshot until commit.
                let deletes = if ws.relational_deletes.is_empty() {
                    std::collections::HashSet::new()
                } else {
                    ws.relational_deletes
                        .iter()
                        .filter(|(t, _, _)| t == table)
                        .map(|(_, row_id, _)| *row_id)
                        .collect()
                };
                let delete_predicates = ws
                    .relational_delete_predicates
                    .iter()
                    .filter(|predicate| predicate.table == table)
                    .cloned()
                    .collect::<Vec<_>>();
                let overlap = ws.relational_inserts.iter().find_map(|(t, row)| {
                    #[cfg(feature = "in-process-test-seams")]
                    crate::writer_query_test_seams::note_constraint_staged_row();
                    #[cfg(any(test, feature = "test-seams"))]
                    crate::writer_query_test_seams::note_target_entry(t);
                    if t != table {
                        return None;
                    }
                    if let Some(sid) = skip_row_id
                        && row.row_id == sid
                    {
                        return None;
                    }
                    if row.values.get(column) == Some(value) {
                        Some(row.row_id)
                    } else {
                        None
                    }
                });
                (deletes, delete_predicates, overlap)
            })?;
        let entries = {
            let indexes = self.relational_store.indexes.read();
            // Prefer conventional auto-index names when they still match the
            // requested column, then fall back to any exact single-column index.
            let pk_key = format!("__pk_{column}");
            let unique_key = format!("__unique_{column}");
            let storage = indexes.get(table).and_then(|table_indexes| {
                table_indexes
                    .get(&pk_key)
                    .filter(|idx| idx.columns.len() == 1 && idx.columns[0].0 == column)
                    .or_else(|| {
                        table_indexes
                            .get(&unique_key)
                            .filter(|idx| idx.columns.len() == 1 && idx.columns[0].0 == column)
                    })
                    .or_else(|| {
                        table_indexes
                            .values()
                            .find(|idx| idx.columns.len() == 1 && idx.columns[0].0 == column)
                    })
            });
            let Some(storage) = storage else {
                return Ok(ConstraintProbe::NoIndex);
            };
            let key = vec![DirectedValue::Asc(TotalOrdAsc(value.clone()))];
            storage
                .exact_postings(&key)
                .map(|entries| entries.to_vec())
                .unwrap_or_default()
        };
        for entry in entries {
            #[cfg(any(test, feature = "test-seams"))]
            crate::writer_query_test_seams::note_target_entry(table);
            #[cfg(feature = "in-process-test-seams")]
            crate::writer_query_test_seams::note_constraint_posting();
            if let Some(sid) = skip_row_id
                && entry.row_id == sid
            {
                continue;
            }
            if tx_staged_deletes.contains(&entry.row_id) {
                continue;
            }
            if !tx_delete_predicates.is_empty()
                && let Some(row) = self
                    .relational_store
                    .row_by_id(table, entry.row_id, snapshot)
                && row_matches_delete_predicates(&tx_delete_predicates, table, &row)
            {
                continue;
            }
            if entry.visible_at(snapshot) {
                return Ok(ConstraintProbe::Match(entry.row_id));
            }
        }
        Ok(match staged_overlap {
            Some(row_id) => ConstraintProbe::Match(row_id),
            None => ConstraintProbe::NoMatch,
        })
    }

    /// Probe a composite UNIQUE (a, b, ...) using the first index whose
    /// leading prefix matches `cols`. The probe walks the range for the full
    /// key prefix.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn probe_composite_unique(
        &self,
        tx: TxId,
        table: &str,
        cols: &[String],
        values: &[Value],
        snapshot: SnapshotId,
        skip_row_id: Option<RowId>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<RowId>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if cols.is_empty() || values.is_empty() || cols.len() != values.len() {
            return Ok(None);
        }
        if self.persistence.is_some() {
            if let Some(row) = self.writer_constraint_staged_match(
                tx,
                table,
                cols,
                values,
                skip_row_id,
                read_memory.clone(),
            )? {
                return Ok(Some(row));
            }
            return self
                .writer_indexed_row_with_overlay(
                    table,
                    cols,
                    values,
                    snapshot,
                    &HashSet::new(),
                    None,
                    Some((tx, skip_row_id)),
                    read_memory.clone(),
                )
                .map(Option::flatten);
        }
        // Rows this tx has already staged for delete are not obstructions.
        let (tx_staged_deletes, tx_delete_predicates): (
            std::collections::HashSet<RowId>,
            Vec<RelationalDeletePredicate>,
        ) = self.tx_mgr.with_read(tx, |ws| {
            let deletes = ws
                .relational_deletes
                .iter()
                .filter(|(t, _, _)| t == table)
                .map(|(_, row_id, _)| *row_id)
                .collect();
            let predicates = ws
                .relational_delete_predicates
                .iter()
                .filter(|predicate| predicate.table == table)
                .cloned()
                .collect();
            (deletes, predicates)
        })?;
        let index_name = self.relational_store.table_meta(table).and_then(|meta| {
            meta.indexes
                .iter()
                .find(|decl| {
                    decl.columns.len() >= cols.len()
                        && (decl.kind != IndexKind::Auto || decl.columns.len() == cols.len())
                        && decl
                            .columns
                            .iter()
                            .zip(cols.iter())
                            .all(|((c, _), w)| c == w)
                })
                .map(|decl| decl.name.clone())
        });
        let Some(index_name) = index_name else {
            return Ok(None);
        };
        let entries = {
            let indexes = self.relational_store.indexes.read();
            let Some(storage) = indexes
                .get(table)
                .and_then(|table_indexes| table_indexes.get(&index_name))
            else {
                return Ok(None);
            };
            let prefix = index_key_from_values(&storage.columns[..cols.len()], values);
            if storage.columns.len() == cols.len() {
                storage
                    .exact_postings(&prefix)
                    .map(|entries| entries.to_vec())
                    .unwrap_or_default()
            } else {
                storage
                    .tree
                    .range(prefix.clone()..)
                    .take_while(|(key, _)| {
                        key.len() >= prefix.len() && key[..prefix.len()] == prefix[..]
                    })
                    .flat_map(|(_, entries)| entries.iter().cloned())
                    .collect::<Vec<_>>()
            }
        };
        for entry in entries {
            #[cfg(any(test, feature = "test-seams"))]
            crate::writer_query_test_seams::note_target_entry(table);
            if let Some(sid) = skip_row_id
                && entry.row_id == sid
            {
                continue;
            }
            if tx_staged_deletes.contains(&entry.row_id) {
                continue;
            }
            if !tx_delete_predicates.is_empty()
                && let Some(row) = self
                    .relational_store
                    .row_by_id(table, entry.row_id, snapshot)
                && row_matches_delete_predicates(&tx_delete_predicates, table, &row)
            {
                continue;
            }
            if entry.visible_at(snapshot) {
                return Ok(Some(entry.row_id));
            }
        }
        // Tx-staged inserts.
        let overlap = self.tx_mgr.with_read(tx, |ws| {
            for (t, row) in ws.relational_inserts {
                #[cfg(any(test, feature = "test-seams"))]
                crate::writer_query_test_seams::note_target_entry(t);
                if t != table {
                    continue;
                }
                if let Some(sid) = skip_row_id
                    && row.row_id == sid
                {
                    continue;
                }
                let matches = cols
                    .iter()
                    .zip(values.iter())
                    .all(|(c, v)| row.values.get(c) == Some(v));
                if matches {
                    return Some(row.row_id);
                }
            }
            None
        })?;
        Ok(overlap)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn rewrite_txid_placeholders(
        &self,
        origin_tx: TxId,
        ws: &mut WriteSet,
    ) -> Result<()> {
        for (table, row) in &mut ws.relational_inserts {
            self.rewrite_txid_placeholders_in_values(
                table,
                origin_tx,
                row.created_tx,
                &mut row.values,
            );
        }
        Ok(())
    }

    pub(super) fn rewrite_txid_placeholders_for_rows_not_in(
        &self,
        origin_tx: TxId,
        ws: &mut WriteSet,
        baseline: &HashSet<(String, RowId)>,
    ) {
        for (table, row) in &mut ws.relational_inserts {
            if baseline.contains(&(table.clone(), row.row_id)) {
                continue;
            }
            self.rewrite_txid_placeholders_in_values(
                table,
                origin_tx,
                self.private_receiver_transaction(origin_tx)
                    .unwrap_or(row.created_tx),
                &mut row.values,
            );
        }
    }

    pub(super) fn rewrite_txid_placeholders_in_values(
        &self,
        table: &str,
        origin_tx: TxId,
        canonical_tx: TxId,
        values: &mut HashMap<ColName, Value>,
    ) {
        let Some(meta) = self.table_meta(table) else {
            return;
        };
        for column in meta
            .columns
            .iter()
            .filter(|column| matches!(column.column_type, ColumnType::TxId))
        {
            if matches!(values.get(&column.name), Some(Value::TxId(tx)) if *tx == origin_tx) {
                values.insert(column.name.clone(), Value::TxId(canonical_tx));
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn commit_validate(
        &self,
        origin_tx: TxId,
        ws: &mut WriteSet,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<CommitValidationOutcome> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // The guards below are one-shot: each describes work this pass is
        // about to validate and must not be re-applied. The recorded vector
        // schema epochs are NOT one-shot — they describe the whole
        // transaction, which an opted-in sync trigger can bring back here
        // more than once: dispatching a callback that writes into the applying
        // transaction re-enters commit validation for the same tx. Draining
        // the epochs with the guards left a later pass with nothing to compare
        // against, and an unchanged vector index was reported as concurrently
        // altered. They stay until the transaction itself ends, where every
        // commit and rollback path already clears this entry.
        let metadata = {
            let mut pending = self.pending_commit_metadata.lock();
            let entry = pending.entry(origin_tx).or_default();
            PendingCommitMetadata {
                discard_selections: entry.discard_selections.clone(),
                conditional_update_guards: std::mem::take(&mut entry.conditional_update_guards),
                sync_lineage_guards: std::mem::take(&mut entry.sync_lineage_guards),
                upsert_intents: std::mem::take(&mut entry.upsert_intents),
                vector_schema_epochs: entry.vector_schema_epochs.clone(),
                delivery_registrations: Vec::new(),
                lineage_generations: entry.lineage_generations.clone(),
                row_graph_undo: entry.row_graph_undo.clone(),
                local_schema_stage_discarded: entry.local_schema_stage_discarded,
            }
        };
        let published = self.tx_mgr.capture_published_read();
        let snapshot = published.snapshot;
        let root = writer_read::durable_root(published.handle)?;
        let _writer_read = self.enter_writer_read(root, read_memory.clone());
        let mut stats_guard = CommitStageStatsGuard::new(self);
        #[cfg(any(test, feature = "test-seams"))]
        let mut stage_timer = CommitStageTimer::new();
        self.validate_sync_lineage_guards(
            snapshot,
            &metadata.sync_lineage_guards,
            read_memory.clone(),
        )?;
        let validation = self.revalidate_conditional_updates(
            ws,
            snapshot,
            &metadata.conditional_update_guards,
            Some(stats_guard.stats_mut()),
            vector_copy_owner,
            read_memory.clone(),
        )?;
        #[cfg(any(test, feature = "test-seams"))]
        stage_timer.record(stats_guard.stats_mut(), 0);
        self.validate_vector_schema_epochs(ws, &metadata.vector_schema_epochs)?;
        self.validate_vector_write_set_schema(ws, vector_copy_owner, read_memory.clone())?;
        self.discard_deletes_of_rows_that_are_already_gone(ws, snapshot, read_memory.clone())?;
        let mut deleted_rows_by_table =
            self.deleted_row_snapshots_by_table(ws, snapshot, read_memory.clone())?;
        // Validating the write set's constraints is write working memory:
        // the writer budget funds these lookups, never the caller's read limit.
        let constraint_memory = self.finalization.accountant.read_budget()?;
        self.validate_unique_constraints_in_write_set(
            ws,
            snapshot,
            &metadata.upsert_intents,
            &mut deleted_rows_by_table,
            stats_guard.stats_mut(),
            vector_copy_owner,
            constraint_memory.clone(),
        )?;
        #[cfg(any(test, feature = "test-seams"))]
        stage_timer.record(stats_guard.stats_mut(), 1);
        let changes = self.accepted_commit_state_changes(
            DatabaseTransactionHost::legacy_read(ws),
            snapshot,
            read_memory.clone(),
        )?;
        changes.with_value(|changes| -> Result<()> {
            for (table, uuid, state, tx) in changes {
                let mut options = PreparedPropagationOptions {
                    tx: *tx,
                    snapshot,
                    lookup_delta: None,
                };
                self.propagate_state_change_in_prepared_write_set(
                    ws,
                    table,
                    Some(*uuid),
                    Some(state),
                    &mut options,
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
            }
            Ok(())
        })?;
        deleted_rows_by_table =
            self.deleted_row_snapshots_by_table(ws, snapshot, read_memory.clone())?;
        self.validate_unique_constraints_in_write_set(
            ws,
            snapshot,
            &[],
            &mut deleted_rows_by_table,
            stats_guard.stats_mut(),
            vector_copy_owner,
            constraint_memory.clone(),
        )?;
        self.canonicalize_row_graph_in_prepared_write_set(ws);
        self.validate_foreign_keys_in_write_set(
            ws,
            snapshot,
            &deleted_rows_by_table,
            stats_guard.stats_mut(),
            constraint_memory.clone(),
        )?;
        #[cfg(any(test, feature = "test-seams"))]
        stage_timer.record(stats_guard.stats_mut(), 2);
        self.validate_composite_foreign_keys_in_write_set(
            ws,
            snapshot,
            &deleted_rows_by_table,
            stats_guard.stats_mut(),
            constraint_memory,
        )?;
        #[cfg(any(test, feature = "test-seams"))]
        stage_timer.record(stats_guard.stats_mut(), 3);
        self.validate_vector_schema_epochs(ws, &metadata.vector_schema_epochs)?;
        self.validate_vector_write_set_schema(ws, vector_copy_owner, read_memory.clone())?;
        #[cfg(any(test, feature = "test-seams"))]
        stage_timer.record(stats_guard.stats_mut(), 4);
        stats_guard.flush();
        Ok(validation)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_vector_schema_epochs(
        &self,
        ws: &WriteSet,
        recorded_epochs: &HashMap<VectorIndexRef, u64>,
    ) -> Result<()> {
        for index in Self::write_set_vector_schema_refs(ws) {
            let Some(recorded_epoch) = recorded_epochs.get(&index) else {
                return Err(Self::stale_vector_schema_error(&index));
            };
            let current_epoch = self.vector_schema_gates.epoch_for(&index);
            if current_epoch != *recorded_epoch {
                return Err(Self::stale_vector_schema_error(&index));
            }
        }
        Ok(())
    }

    pub(super) fn stale_vector_schema_error(index: &VectorIndexRef) -> Error {
        Error::SchemaInvalid {
            reason: format!(
                "vector index {}.{} changed while transaction was open; retry transaction",
                index.table, index.column
            ),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_vector_write_set_schema(
        &self,
        ws: &WriteSet,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        _read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let _read_memory: contextdb_core::read_memory::ReadMemoryOwner = _read_memory.into();

        // A received unit that declares a table AND carries that table's
        // vectors commits both together atomically. At this
        // point the table exists only in the prepared stage, so reading the
        // receiver's CURRENT shape would refuse the unit for not yet having
        // installed the very schema it is installing. The detached worker has
        // already run this exact check against the arriving shape, with the
        // table and its declared layout present, which is what makes skipping
        // it here a re-check rather than a missing one.
        if ws
            .commit_lsn
            .is_some_and(|lsn| self.received_schema_stages.lock().contains_key(&lsn))
        {
            return Ok(());
        }
        let mut refs = ws
            .vector_inserts
            .iter()
            .map(|entry| entry.index.clone())
            .chain(ws.vector_deletes.iter().map(|(index, _, _)| index.clone()))
            .chain(ws.vector_moves.iter().map(|(index, _, _, _)| index.clone()))
            .collect::<Vec<_>>();
        refs.sort_by(|a, b| a.table.cmp(&b.table).then(a.column.cmp(&b.column)));
        refs.dedup();

        for index in refs {
            let Some(meta) = self.table_meta(&index.table) else {
                return Err(Error::UnknownVectorIndex { index });
            };
            let Some(column) = meta
                .columns
                .iter()
                .find(|column| column.name == index.column)
            else {
                return Err(Error::UnknownVectorIndex { index });
            };
            let ColumnType::Vector(expected) = column.column_type else {
                return Err(Error::UnknownVectorIndex { index });
            };
            let layout = contextdb_vector::scoped_index_layout(
                &self.vector_store,
                &index,
                vector_copy_owner,
            )?;
            if layout.dimension != expected {
                return Err(Error::VectorIndexDimensionMismatch {
                    index,
                    expected,
                    actual: layout.dimension,
                });
            }
        }

        for entry in &ws.vector_inserts {
            let expected = self
                .vector_store
                .with_index_layout(&entry.index, |layout| layout.dimension)?;
            if entry.vector.len() != expected {
                return Err(Error::VectorIndexDimensionMismatch {
                    index: entry.index.clone(),
                    expected,
                    actual: entry.vector.len(),
                });
            }
        }
        for (table, row) in &ws.relational_inserts {
            let Some(meta) = self.table_meta(table) else {
                continue;
            };
            for (column_name, value) in &row.values {
                let Value::Vector(vector) = value else {
                    continue;
                };
                let index = VectorIndexRef::new(table, column_name);
                let Some(column) = meta
                    .columns
                    .iter()
                    .find(|column| column.name == *column_name)
                else {
                    return Err(Error::UnknownVectorIndex { index });
                };
                let ColumnType::Vector(expected) = column.column_type else {
                    return Err(Error::UnknownVectorIndex { index });
                };
                if vector.len() != expected {
                    return Err(Error::VectorIndexDimensionMismatch {
                        index,
                        expected,
                        actual: vector.len(),
                    });
                }
            }
        }

        Ok(())
    }

    pub(super) fn take_conditional_update_guards_for_tx(
        &self,
        tx: TxId,
    ) -> Vec<PendingConditionalUpdateGuard> {
        self.pending_commit_metadata
            .lock()
            .get_mut(&tx)
            .map(|metadata| std::mem::take(&mut metadata.conditional_update_guards))
            .unwrap_or_default()
    }

    /// Retained writer owners released at close. File-backed history and
    /// commit directories live in native tables; memory stores retain their
    /// log and directory entries. The shared page cache remains charged.
    pub(super) fn durable_writer_retained_bytes(&self) -> usize {
        // A committed read image is not a writer. It never appends a
        // change-log entry and never adds a commit-index entry, and the image
        // it holds of both was admitted when it was decoded -- so it is
        // charged nothing at this door and has nothing to hand back at close
        // or drop.
        if self.read_image_memory.is_some() {
            return 0;
        }
        crate::composite_store::change_log_retained_bytes(&self.change_log.read()).saturating_add(
            if self.persistence.is_none() {
                self.tx_mgr
                    .commit_index_len()
                    .saturating_mul(crate::composite_store::COMMIT_INDEX_ENTRY_BYTES)
            } else {
                0
            },
        )
    }

    /// Writer-owned log, cache and captured source controls, for a proof
    /// that measures the separate retained cost of storing a row.
    ///
    /// A proof about what STORING A ROW costs reads the accountant net of
    /// this. The two are different retentions with different lifetimes: a
    /// row's charge comes back when the row does, while the log keeps the
    /// history of that row being stored and deleted until a maintenance
    /// cycle trims it -- which is a thing the store is still holding, and a
    /// declared limit has to see it.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn __durable_writer_charged_bytes_for_test(&self) -> usize {
        self.durable_writer_retained_bytes()
            .saturating_add(self.schema_history_retained_bytes())
            .saturating_add(self.persistence.as_ref().map_or(0, |p| p.cache_charge()))
            .saturating_add(self.commit_source_reference_charge)
            .saturating_add(self.persistence.as_ref().map_or(0, |persistence| {
                let (root_charge, schema_charge) = persistence.writer_snapshot_charge_for_test();
                root_charge.saturating_add(schema_charge)
            }))
    }

    /// Test-only forced log-gap seam. Durable store state remains intact, so
    /// the next extraction must exercise the same persisted-state fallback a
    /// peer encounters when its requested watermark predates available logs.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn __force_sync_log_gap_for_test(&self) {
        let _schema_publication = self.enter_schema_publication_gate(false);
        self.with_commit_lock(|| {
            self.forced_change_log_gap_through
                .store(self.current_lsn().0, Ordering::SeqCst);
            let mut change_log = self.change_log.write();
            // Captured-image history is funded by LoadedImageHistoryCharge's
            // reader owner, not by the writer's retained-log accountant.
            if self.read_image_memory.is_none() {
                self.finalization.accountant.release(
                    crate::composite_store::change_log_retained_bytes(&change_log),
                );
            }
            change_log.clear();
            drop(change_log);
            self.change_log_table_index.write().clear();
            self.change_log_lsn_refcounts.write().clear();
            self.clear_memory_schema_log();
        });
    }

    pub(super) fn pending_upsert_intents_for_tx(&self, tx: TxId) -> Vec<PendingUpsertIntent> {
        self.pending_commit_metadata
            .lock()
            .get(&tx)
            .map(|metadata| metadata.upsert_intents.clone())
            .unwrap_or_default()
    }

    pub(super) fn upsert_intent_indexes_by_row(
        upsert_intents: &[PendingUpsertIntent],
    ) -> HashMap<(TableName, RowId), Vec<usize>> {
        let mut indexes = HashMap::<(TableName, RowId), Vec<usize>>::new();
        for (index, intent) in upsert_intents.iter().enumerate() {
            indexes
                .entry((intent.table.clone(), intent.row_id))
                .or_default()
                .push(index);
        }
        indexes
    }

    pub(super) fn upsert_intent_index_by_conflict(
        upsert_intents: &[PendingUpsertIntent],
    ) -> HashMap<(TableName, RowId, Vec<ColName>), usize> {
        let mut indexes = HashMap::<(TableName, RowId, Vec<ColName>), usize>::new();
        for (index, intent) in upsert_intents.iter().enumerate() {
            indexes
                .entry((
                    intent.table.clone(),
                    intent.row_id,
                    intent.conflict_columns.clone(),
                ))
                .or_insert(index);
        }
        indexes
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn rewrite_commit_time_upserts_for_write_set(
        &self,
        ws: &mut WriteSet,
        snapshot: SnapshotId,
        upsert_intents: &[PendingUpsertIntent],
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if upsert_intents.is_empty() {
            return Ok(());
        }
        let upsert_lookup = CommitTimeUpsertLookup {
            intents: upsert_intents,
            by_row: Self::upsert_intent_indexes_by_row(upsert_intents),
        };
        let mut deleted_by_table = Self::deleted_row_ids_by_table(ws);
        let empty_deleted = HashSet::new();
        let mut index = 0;
        while index < ws.relational_inserts.len() {
            let table = ws.relational_inserts[index].0.clone();
            let skip_deleted = deleted_by_table.get(&table).unwrap_or(&empty_deleted);
            let prefix = Self::relational_insert_prefix_identity(ws, index);
            if let Some(rewrite) = self.apply_original_commit_time_upsert_if_needed(
                ws,
                index,
                skip_deleted,
                &upsert_lookup,
                snapshot,
                None,
                vector_copy_owner,
                read_memory.clone(),
            )? {
                deleted_by_table = Self::deleted_row_ids_by_table(ws);
                index =
                    Self::relational_insert_index_by_row(ws, &rewrite.table, rewrite.row_id, index)
                        .unwrap_or_else(|| {
                            if Self::relational_insert_prefix_unchanged(ws, &prefix) {
                                index.saturating_add(1)
                            } else {
                                index.min(ws.relational_inserts.len())
                            }
                        });
                continue;
            }
            index += 1;
        }
        Ok(())
    }

    pub(super) fn deleted_row_ids_for_table(ws: &WriteSet, table: &str) -> HashSet<RowId> {
        ws.relational_deletes
            .iter()
            .filter(|(t, _, _)| t == table)
            .map(|(_, row_id, _)| *row_id)
            .collect()
    }

    /// Drop a staged delete whose row is no longer there to delete.
    ///
    /// Another writer can delete the same row between the moment this
    /// transaction staged its `DELETE` and the moment it commits. The delete
    /// then has nothing to do, and a no-op has to be traceless: leaving it in
    /// the write set would write a second deletion record into history and
    /// raise an outbound deletion obligation for a row this node did not
    /// delete. A row this same transaction is inserting is not gone -- it has
    /// simply not been committed yet -- so its delete stays.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn discard_deletes_of_rows_that_are_already_gone(
        &self,
        ws: &mut WriteSet,
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if ws.relational_deletes.is_empty() {
            return Ok(());
        }
        let live = self.deleted_row_snapshots_by_table(ws, snapshot, read_memory.clone())?;
        let staged = ws
            .relational_inserts
            .iter()
            .map(|(table, row)| (table.clone(), row.row_id))
            .collect::<HashSet<_>>();
        let mut discarded = HashSet::new();
        ws.relational_deletes.retain(|(table, row_id, _)| {
            let keep = live
                .get(table)
                .is_some_and(|rows| rows.contains_key(row_id))
                || staged.contains(&(table.clone(), *row_id));
            if !keep {
                discarded.insert((table.clone(), *row_id));
            }
            keep
        });
        if discarded.is_empty() {
            return Ok(());
        }
        // The delete also staged this row's outgoing deletion obligation. That
        // obligation is the part a peer would see, so a traceless no-op has to
        // drop it too, or the next push carries a deletion this node never
        // performed.
        ws.config_writes.retain(|(key, encoded)| {
            if !key.starts_with(Self::DURABLE_LINEAGE_CONFIG_PREFIX) {
                return true;
            }
            let Ok(record) = RedbPersistence::decode_config_value::<DurableLineageRecord>(encoded)
            else {
                return true;
            };
            if record.delete_obligation != DurableDeleteObligation::Pending {
                return true;
            }
            let Some(row_id) = record.local_row_id else {
                return true;
            };
            !discarded.contains(&(record.table.clone(), row_id))
        });
        Ok(())
    }

    pub(super) fn deleted_row_ids_by_table(ws: &WriteSet) -> HashMap<String, HashSet<RowId>> {
        let mut deleted = HashMap::<String, HashSet<RowId>>::new();
        for (table, row_id, _) in &ws.relational_deletes {
            deleted.entry(table.clone()).or_default().insert(*row_id);
        }
        deleted
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn deleted_row_snapshots_by_table(
        &self,
        ws: &WriteSet,
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<HashMap<String, HashMap<RowId, VersionedRow>>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if ws.relational_deletes.is_empty() {
            return Ok(HashMap::new());
        }
        let mut seen = HashSet::new();
        let mut keys = Vec::with_capacity(ws.relational_deletes.len());
        for (table, row_id, _) in &ws.relational_deletes {
            if seen.insert((table.clone(), *row_id)) {
                keys.push((table.clone(), *row_id));
            }
        }
        if self.persistence.is_some() {
            // Consumers inspect only declared unique and foreign-key tuples.
            // Large unrelated payloads stay in the admitted source window.
            let columns = self.staged_tuple_key_columns_by_table();
            let mut rows = HashMap::<String, HashMap<RowId, VersionedRow>>::new();
            let mut output = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
            for (table, row_id) in keys {
                let selected = columns.get(&table).map_or(&[][..], Vec::as_slice);
                let row = self.constraint_row_at_admitted(
                    &table,
                    row_id,
                    snapshot,
                    selected,
                    &mut output,
                    read_memory.clone(),
                )?;
                if let Some(row) = row {
                    output.try_grow_for(
                        128,
                        "writer_source",
                        "deleted_row_output",
                        "Raise MEMORY_LIMIT to prepare these deletions.",
                    )?;
                    rows.entry(table).or_default().insert(row_id, row);
                }
            }
            self.retain_writer_output(output);
            return Ok(rows);
        }
        Ok(self.relational_store.live_rows_by_id(&keys))
    }

    pub(super) fn relational_insert_prefix_identity(
        ws: &WriteSet,
        end: usize,
    ) -> Vec<(String, RowId)> {
        ws.relational_inserts
            .iter()
            .take(end)
            .map(|(table, row)| (table.clone(), row.row_id))
            .collect()
    }

    pub(super) fn relational_insert_prefix_unchanged(
        ws: &WriteSet,
        expected: &[(String, RowId)],
    ) -> bool {
        ws.relational_inserts.len() >= expected.len()
            && ws.relational_inserts.iter().zip(expected.iter()).all(
                |((table, row), (expected_table, expected_row_id))| {
                    table == expected_table && row.row_id == *expected_row_id
                },
            )
    }

    pub(super) fn value_can_be_tuple_key(value: &Value) -> bool {
        !matches!(value, Value::Float64(_) | Value::Json(_) | Value::Vector(_))
    }

    pub(super) fn values_can_be_tuple_key(values: &[Value]) -> bool {
        values.iter().all(Self::value_can_be_tuple_key)
    }

    pub(super) fn add_staged_tuple_columns(
        columns_by_table: &mut HashMap<String, Vec<Vec<String>>>,
        table: &str,
        columns: Vec<String>,
    ) {
        if columns.is_empty() {
            return;
        }
        let table_columns = columns_by_table.entry(table.to_string()).or_default();
        if !table_columns.iter().any(|existing| existing == &columns) {
            table_columns.push(columns);
        }
    }

    pub(super) fn staged_tuple_key_columns_by_table(&self) -> HashMap<String, Vec<Vec<String>>> {
        let metas = self.relational_store.table_meta.read();
        let mut columns_by_table = HashMap::<String, Vec<Vec<String>>>::new();

        for (table, meta) in metas.iter() {
            for column in &meta.columns {
                if column.primary_key || column.unique {
                    Self::add_staged_tuple_columns(
                        &mut columns_by_table,
                        table,
                        vec![column.name.clone()],
                    );
                }
                if let Some(reference) = &column.references {
                    Self::add_staged_tuple_columns(
                        &mut columns_by_table,
                        table,
                        vec![column.name.clone()],
                    );
                    Self::add_staged_tuple_columns(
                        &mut columns_by_table,
                        &reference.table,
                        vec![reference.column.clone()],
                    );
                }
            }

            for unique_constraint in meta.enforced_unique_tuples() {
                Self::add_staged_tuple_columns(
                    &mut columns_by_table,
                    table,
                    unique_constraint.clone(),
                );
            }

            for fk in &meta.composite_foreign_keys {
                Self::add_staged_tuple_columns(
                    &mut columns_by_table,
                    table,
                    fk.child_columns.clone(),
                );
                Self::add_staged_tuple_columns(
                    &mut columns_by_table,
                    &fk.parent_table,
                    fk.parent_columns.clone(),
                );
            }
        }

        columns_by_table
    }

    pub(super) fn add_commit_stat(counter: &AtomicU64, delta: u64) {
        if delta > 0 {
            counter.fetch_add(delta, Ordering::SeqCst);
        }
    }

    pub(super) fn add_commit_stage_stats(&self, stats: &CommitStageStats) {
        Self::add_commit_stat(&self.commit_rows_validated, stats.rows_validated);
        Self::add_commit_stat(&self.commit_indexed_probes, stats.indexed_probes);
        Self::add_commit_stat(
            &self.commit_staged_vs_staged_comparisons,
            stats.staged_vs_staged_comparisons,
        );
        Self::add_commit_stat(&self.commit_scan_rows_touched, stats.scan_rows_touched);
        Self::add_commit_stat(
            &self.commit_index_maintenance_visits,
            stats.index_maintenance_visits,
        );
        #[cfg(any(test, feature = "test-seams"))]
        if let Some(stage_wall_nanos) = stats.stage_wall_nanos {
            for (counter, delta) in self.commit_stage_wall_nanos.iter().zip(stage_wall_nanos) {
                Self::add_commit_stat(counter, delta);
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn committed_unique_conflict_row_id(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
        stats: Option<&mut CommitStageStats>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<RowId>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut stats = stats;
        if columns.is_empty() || columns.len() != values.len() {
            return Ok(None);
        }

        if let Some(row_id) = self.indexed_visible_row_id_by_columns(
            table,
            columns,
            values,
            snapshot,
            skip_deleted,
            stats.as_deref_mut(),
            read_memory.clone(),
        )? {
            return Ok(row_id);
        }

        let before_scan_rows = stats
            .as_ref()
            .map(|_| self.relational_store.scan_rows_touched());
        let rows = self.relational_scan_filter_with_tx(
            None,
            table,
            snapshot,
            &|row| {
                !skip_deleted.contains(&row.row_id)
                    && columns
                        .iter()
                        .zip(values.iter())
                        .all(|(column, value)| row.values.get(column) == Some(value))
            },
            read_memory.clone(),
        )?;
        if let (Some(stats), Some(before_scan_rows)) = (stats, before_scan_rows) {
            stats.scan_rows_touched = stats.scan_rows_touched.saturating_add(
                self.relational_store
                    .scan_rows_touched()
                    .saturating_sub(before_scan_rows),
            );
        }
        Ok(rows.into_iter().next().map(|row| row.row_id))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn indexed_visible_row_id_by_columns(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
        stats: Option<&mut CommitStageStats>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<Option<RowId>>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if columns.is_empty() || columns.len() != values.len() {
            return Ok(Some(None));
        }

        if self.persistence.is_some() {
            return self.writer_indexed_row(
                table,
                columns,
                values,
                snapshot,
                skip_deleted,
                stats,
                read_memory.clone(),
            );
        }

        if self.retained_relational_source.is_some() {
            let meta = self
                .table_meta(table)
                .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
            let Some(index) = meta.indexes.iter().find(|index| {
                index.columns.len() >= columns.len()
                    && index
                        .columns
                        .iter()
                        .zip(columns)
                        .all(|((have, _), want)| have == want)
            }) else {
                return Ok(None);
            };
            // A committed image has no writer: its reader funds the native key
            // before it is built, and a second copy when a range probe clones
            // it for both bounds. The cursors admit their own windows.
            let key_copies = if index.columns.len() == columns.len() {
                1
            } else {
                2
            };
            let mut scratch = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
            scratch.try_grow_for(
                writer_read::constraint_key_bytes(values)
                    .saturating_mul(key_copies)
                    .saturating_add(256),
                "reader",
                "constraint_keys",
                "Raise the declared read memory limit to validate this key.",
            )?;
            let key = index_key_from_values(&index.columns[..columns.len()], values);
            let mut output = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
            if let Some(stats) = stats {
                stats.indexed_probes = stats.indexed_probes.saturating_add(1);
            }
            if index.columns.len() == columns.len() {
                let mut cursor =
                    contextdb_relational::mem::BoundedExactIndexCursor::for_keys([key]);
                cursor.select_identities_only();
                while let Some(row) = self.bounded_exact_index_row_next(
                    read_memory.clone(),
                    table,
                    &index.name,
                    snapshot,
                    &mut cursor,
                    || Ok::<(), Error>(()),
                    || Ok::<(), Error>(()),
                    |_| Ok(()),
                    |_| Ok(()),
                    |bytes| {
                        bytes.retain_into(
                            &mut output,
                            "reader",
                            "constraint_identity",
                            "Raise the declared read memory limit to validate this key.",
                        )
                    },
                )? {
                    if !skip_deleted.contains(&row.row_id) {
                        return Ok(Some(Some(row.row_id)));
                    }
                }
            } else {
                let mut cursor = contextdb_relational::mem::BoundedOrderedRowCursor::for_index(
                    index.name.clone(),
                    false,
                )
                .seeking_run(
                    std::ops::Bound::Included(key.clone()),
                    std::ops::Bound::Included(key),
                );
                cursor.select_identities_only();
                while let Some(row) = self.bounded_ordered_table_row_next(
                    read_memory.clone(),
                    table,
                    &index.columns[0].0,
                    index.columns[0].1,
                    snapshot,
                    &mut cursor,
                    || Ok::<(), Error>(()),
                    || Ok(()),
                    |bytes| {
                        bytes.retain_into(
                            &mut output,
                            "reader",
                            "constraint_identity",
                            "Raise the declared read memory limit to validate this key.",
                        )
                    },
                )? {
                    if !skip_deleted.contains(&row.row_id) {
                        return Ok(Some(Some(row.row_id)));
                    }
                }
            }
            return Ok(Some(None));
        }

        fn visible_posting(
            #[cfg(any(test, feature = "test-seams"))] table: &str,
            entries: &[contextdb_relational::IndexEntry],
            snapshot: SnapshotId,
            skip_deleted: &HashSet<RowId>,
        ) -> Option<RowId> {
            entries
                .iter()
                .find(|entry| {
                    #[cfg(any(test, feature = "test-seams"))]
                    crate::writer_query_test_seams::note_target_entry(table);
                    !skip_deleted.contains(&entry.row_id) && entry.visible_at(snapshot)
                })
                .map(|entry| entry.row_id)
        }

        let (index_checked, indexed_row_id) = {
            let indexes = self.relational_store.indexes.read();
            if columns.len() == 1 {
                let column = &columns[0];
                let pk_key = format!("__pk_{column}");
                let unique_key = format!("__unique_{column}");
                let storage = indexes.get(table).and_then(|table_indexes| {
                    table_indexes
                        .get(&pk_key)
                        .filter(|idx| idx.columns.len() == 1 && idx.columns[0].0 == *column)
                        .or_else(|| {
                            table_indexes
                                .get(&unique_key)
                                .filter(|idx| idx.columns.len() == 1 && idx.columns[0].0 == *column)
                        })
                        .or_else(|| {
                            table_indexes
                                .values()
                                .find(|idx| idx.columns.len() == 1 && idx.columns[0].0 == *column)
                        })
                });
                match storage {
                    Some(storage) => {
                        let key = index_key_from_values(&storage.columns[..1], values);
                        (
                            true,
                            storage.exact_postings(&key).and_then(|entries| {
                                visible_posting(
                                    #[cfg(any(test, feature = "test-seams"))]
                                    table,
                                    entries,
                                    snapshot,
                                    skip_deleted,
                                )
                            }),
                        )
                    }
                    None => (false, None),
                }
            } else {
                let storage = indexes.get(table).and_then(|table_indexes| {
                    table_indexes.values().find(|idx| {
                        idx.columns.len() >= columns.len()
                            && (!idx.exact_only() || idx.columns.len() == columns.len())
                            && idx
                                .columns
                                .iter()
                                .zip(columns.iter())
                                .all(|((have, _), want)| have == want)
                    })
                });
                match storage {
                    Some(storage) => {
                        let prefix =
                            index_key_from_values(&storage.columns[..columns.len()], values);
                        let row_id = if storage.columns.len() == columns.len() {
                            storage.exact_postings(&prefix).and_then(|entries| {
                                visible_posting(
                                    #[cfg(any(test, feature = "test-seams"))]
                                    table,
                                    entries,
                                    snapshot,
                                    skip_deleted,
                                )
                            })
                        } else {
                            storage
                                .tree
                                .range(prefix.clone()..)
                                .take_while(|(key, _)| {
                                    key.len() >= prefix.len() && key[..prefix.len()] == prefix[..]
                                })
                                .find_map(|(_, entries)| {
                                    visible_posting(
                                        #[cfg(any(test, feature = "test-seams"))]
                                        table,
                                        entries,
                                        snapshot,
                                        skip_deleted,
                                    )
                                })
                        };
                        (true, row_id)
                    }
                    None => (false, None),
                }
            }
        };

        if !index_checked {
            return Ok(None);
        }

        if let Some(stats) = stats {
            stats.indexed_probes = stats.indexed_probes.saturating_add(1);
        }
        Ok(Some(indexed_row_id))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn required_indexed_visible_row_by_columns(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let Some(row_id) = self.indexed_visible_row_id_by_columns(
            table,
            columns,
            values,
            snapshot,
            skip_deleted,
            None,
            read_memory.clone(),
        )?
        else {
            return Err(Error::Other(
                (format!(
                    "exact sync key probe on {table}({}) has no covering index",
                    columns.join(", ")
                ))
                .into(),
            ));
        };
        row_id
            .map(|row_id| {
                self.committed_indexed_point_row_at(table, row_id, snapshot, read_memory.clone())
            })
            .transpose()
            .map(Option::flatten)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn required_indexed_visible_row_by_column(
        &self,
        table: &str,
        column: &str,
        value: &Value,
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let columns = vec![column.to_string()];
        let values = vec![value.clone()];
        self.required_indexed_visible_row_by_columns(
            table,
            &columns,
            &values,
            snapshot,
            skip_deleted,
            read_memory.clone(),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn committed_unique_conflict(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
        stats: Option<&mut CommitStageStats>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let Some(row_id) = self.committed_unique_conflict_row_id(
            table,
            columns,
            values,
            snapshot,
            skip_deleted,
            stats,
            read_memory.clone(),
        )?
        else {
            return Ok(None);
        };
        self.committed_row_at(table, row_id, snapshot, read_memory.clone())
    }

    pub(super) fn committed_unique_probe_required_for_replacement(
        &self,
        row_id: RowId,
        columns: &[String],
        values: &[Value],
        skip_deleted: &HashSet<RowId>,
        deleted_rows: Option<&HashMap<RowId, VersionedRow>>,
    ) -> bool {
        if !skip_deleted.contains(&row_id) {
            return true;
        }
        !deleted_rows
            .and_then(|rows| rows.get(&row_id))
            .is_some_and(|committed| {
                columns
                    .iter()
                    .zip(values.iter())
                    .all(|(column, value)| committed.values.get(column) == Some(value))
            })
    }

    pub(super) fn staged_unique_conflict_in_write_set(
        ws: &WriteSet,
        insert_index: usize,
        table: &str,
        columns: &[String],
        values: &[Value],
        stats: Option<&mut CommitStageStats>,
    ) -> Option<RowId> {
        let mut stats = stats;
        for (other_index, (other_table, other_row)) in ws.relational_inserts.iter().enumerate() {
            if other_index == insert_index || other_table != table {
                continue;
            }
            if let Some(stats) = stats.as_deref_mut() {
                stats.staged_vs_staged_comparisons =
                    stats.staged_vs_staged_comparisons.saturating_add(1);
            }
            if ws.relational_inserts[insert_index].1.row_id == other_row.row_id {
                continue;
            }
            if columns
                .iter()
                .zip(values.iter())
                .all(|(column, value)| other_row.values.get(column) == Some(value))
            {
                return Some(other_row.row_id);
            }
        }
        None
    }

    pub(super) fn indexed_visible_row_exists(
        &self,
        table: &str,
        column: &str,
        value: &Value,
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
        stats: Option<&mut CommitStageStats>,
    ) -> Option<bool> {
        use contextdb_core::{DirectedValue, TotalOrdAsc, TotalOrdDesc};

        let indexes = self.relational_store.indexes.read();
        let pk_key = format!("__pk_{column}");
        let unique_key = format!("__unique_{column}");
        let storage = indexes.get(table).and_then(|table_indexes| {
            table_indexes
                .get(&pk_key)
                .filter(|idx| idx.columns.len() == 1 && idx.columns[0].0 == column)
                .or_else(|| {
                    table_indexes
                        .get(&unique_key)
                        .filter(|idx| idx.columns.len() == 1 && idx.columns[0].0 == column)
                })
                .or_else(|| {
                    table_indexes
                        .values()
                        .find(|idx| idx.columns.len() == 1 && idx.columns[0].0 == column)
                })
        })?;
        let key = match storage.columns.first().map(|(_, direction)| direction) {
            Some(SortDirection::Desc) => vec![DirectedValue::Desc(TotalOrdDesc(value.clone()))],
            _ => vec![DirectedValue::Asc(TotalOrdAsc(value.clone()))],
        };
        if let Some(stats) = stats {
            stats.indexed_probes = stats.indexed_probes.saturating_add(1);
        }
        Some(storage.exact_postings(&key).is_some_and(|entries| {
            entries.iter().any(|entry| {
                #[cfg(any(test, feature = "test-seams"))]
                crate::writer_query_test_seams::note_target_entry(table);
                !skip_deleted.contains(&entry.row_id) && entry.visible_at(snapshot)
            })
        }))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn visible_row_exists_by_column(
        &self,
        table: &str,
        column: &str,
        value: &Value,
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
        stats: Option<&mut CommitStageStats>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if self.persistence.is_some() || self.retained_relational_source.is_some() {
            if let Some(row) = self.indexed_visible_row_id_by_columns(
                table,
                &[column.to_owned()],
                std::slice::from_ref(value),
                snapshot,
                skip_deleted,
                stats,
                read_memory.clone(),
            )? {
                return Ok(row.is_some());
            }
            let rows = self.relational_scan_filter_with_tx(
                None,
                table,
                snapshot,
                &|row| !skip_deleted.contains(&row.row_id) && row.values.get(column) == Some(value),
                read_memory.clone(),
            )?;
            return Ok(!rows.is_empty());
        }
        let mut stats = stats;
        if let Some(exists) = self.indexed_visible_row_exists(
            table,
            column,
            value,
            snapshot,
            skip_deleted,
            stats.as_deref_mut(),
        ) {
            return Ok(exists);
        }

        let before_scan_rows = stats
            .as_ref()
            .map(|_| self.relational_store.scan_rows_touched());
        let rows = self.relational_scan_filter_with_tx(
            None,
            table,
            snapshot,
            &|row| !skip_deleted.contains(&row.row_id) && row.values.get(column) == Some(value),
            read_memory.clone(),
        )?;
        if let (Some(stats), Some(before_scan_rows)) = (stats, before_scan_rows) {
            stats.scan_rows_touched = stats.scan_rows_touched.saturating_add(
                self.relational_store
                    .scan_rows_touched()
                    .saturating_sub(before_scan_rows),
            );
        }
        Ok(!rows.is_empty())
    }

    /// The committed row carrying a whole sync identity — every key column
    /// matched through the covering index.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn visible_row_by_natural_key(
        &self,
        table: &str,
        natural_key: &NaturalKey,
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.required_indexed_visible_row_by_columns(
            table,
            &natural_key.key_columns(),
            &natural_key.key_values(),
            snapshot,
            skip_deleted,
            read_memory.clone(),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn indexed_visible_row_exists_by_columns(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
        stats: Option<&mut CommitStageStats>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if self.persistence.is_some() || self.retained_relational_source.is_some() {
            let row = self.indexed_visible_row_id_by_columns(
                table,
                columns,
                values,
                snapshot,
                skip_deleted,
                stats,
                read_memory.clone(),
            )?;
            let Some(row) = row else {
                self.fk_full_scan_fallbacks.fetch_add(1, Ordering::SeqCst);
                return Err(Error::Other(
                    (format!(
                        "composite foreign key probe on {table}({}) has no covering index",
                        columns.join(", ")
                    ))
                    .into(),
                ));
            };
            self.fk_indexed_tuple_probes.fetch_add(1, Ordering::SeqCst);
            return Ok(row.is_some());
        }
        if columns.is_empty() || columns.len() != values.len() {
            return Ok(false);
        }

        fn visible_posting(
            #[cfg(any(test, feature = "test-seams"))] table: &str,
            entries: &[contextdb_relational::IndexEntry],
            snapshot: SnapshotId,
            skip_deleted: &HashSet<RowId>,
        ) -> Option<RowId> {
            entries
                .iter()
                .find(|entry| {
                    #[cfg(any(test, feature = "test-seams"))]
                    crate::writer_query_test_seams::note_target_entry(table);
                    !skip_deleted.contains(&entry.row_id) && entry.visible_at(snapshot)
                })
                .map(|entry| entry.row_id)
        }

        let row_id = {
            let indexes = self.relational_store.indexes.read();
            let Some(storage) = indexes.get(table).and_then(|table_indexes| {
                table_indexes.values().find(|storage| {
                    storage.columns.len() >= columns.len()
                        && (!storage.exact_only() || storage.columns.len() == columns.len())
                        && storage
                            .columns
                            .iter()
                            .zip(columns.iter())
                            .all(|((indexed_column, _), wanted)| indexed_column == wanted)
                })
            }) else {
                self.fk_full_scan_fallbacks.fetch_add(1, Ordering::SeqCst);
                return Err(Error::Other(
                    (format!(
                        "composite foreign key probe on {table}({}) has no covering index",
                        columns.join(", ")
                    ))
                    .into(),
                ));
            };

            self.fk_indexed_tuple_probes.fetch_add(1, Ordering::SeqCst);
            if let Some(stats) = stats {
                stats.indexed_probes = stats.indexed_probes.saturating_add(1);
            }
            let prefix = index_key_from_values(&storage.columns[..columns.len()], values);
            if storage.columns.len() == columns.len() {
                storage.exact_postings(&prefix).and_then(|entries| {
                    visible_posting(
                        #[cfg(any(test, feature = "test-seams"))]
                        table,
                        entries,
                        snapshot,
                        skip_deleted,
                    )
                })
            } else {
                storage
                    .tree
                    .range(prefix.clone()..)
                    .take_while(|(key, _)| {
                        key.len() >= prefix.len() && key[..prefix.len()] == prefix[..]
                    })
                    .find_map(|(_, entries)| {
                        visible_posting(
                            #[cfg(any(test, feature = "test-seams"))]
                            table,
                            entries,
                            snapshot,
                            skip_deleted,
                        )
                    })
            }
        };
        Ok(row_id.is_some())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn apply_commit_time_upsert(
        &self,
        ws: &mut WriteSet,
        insert_index: usize,
        conflict_row: &VersionedRow,
        intent: &PendingUpsertIntent,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<CommitTimeUpsertRewrite>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if intent.table != ws.relational_inserts[insert_index].0
            || intent.row_id != ws.relational_inserts[insert_index].1.row_id
        {
            return Ok(None);
        }

        let (table, incoming) = ws.relational_inserts[insert_index].clone();
        let on_conflict = OnConflictPlan {
            columns: intent.conflict_columns.clone(),
            update_columns: intent.update_columns.clone(),
        };
        let mut original_insert_values = intent.insert_values.clone();
        self.rewrite_txid_placeholders_in_values(
            &table,
            intent.active_tx,
            incoming.created_tx,
            &mut original_insert_values,
        );
        let mut values = apply_on_conflict_updates(
            self,
            &table,
            original_insert_values.clone(),
            conflict_row,
            &on_conflict,
            &intent.params,
            Some(intent.active_tx),
        )?;
        self.rewrite_txid_placeholders_in_values(
            &table,
            intent.active_tx,
            incoming.created_tx,
            &mut values,
        );
        for (column, incoming_value) in &incoming.values {
            if original_insert_values.get(column) != Some(incoming_value) {
                values.insert(column.clone(), incoming_value.clone());
            }
        }
        self.validate_commit_time_upsert_replacement(
            &table,
            conflict_row,
            &values,
            snapshot,
            read_memory.clone(),
        )?;

        let incoming_row_bytes = self
            .table_meta(&table)
            .map(|meta| retained_row_bytes_for_meta(&table, &incoming.values, &meta, true))
            .unwrap_or_else(|| incoming.estimated_bytes());
        let replacement_row_bytes = self
            .table_meta(&table)
            .map(|meta| retained_row_bytes_for_meta(&table, &values, &meta, true))
            .unwrap_or_else(|| {
                let mut replacement = incoming.clone();
                replacement.values = values.clone();
                replacement.estimated_bytes()
            });
        let extra_row_bytes = replacement_row_bytes.saturating_sub(incoming_row_bytes);
        if extra_row_bytes > 0 {
            self.finalization.accountant.try_allocate_for(
                extra_row_bytes,
                "insert",
                "commit_time_upsert_row_rewrite",
                "Reduce row size or raise MEMORY_LIMIT before committing this UPSERT.",
            )?;
        }

        let mut replacement = incoming;
        let incoming_row_id = replacement.row_id;
        replacement.row_id = conflict_row.row_id;
        replacement.values = values;
        if let Err(err) = self.reconcile_commit_time_upsert_vectors(
            ws,
            &table,
            incoming_row_id,
            conflict_row.row_id,
            &replacement.values,
            replacement.created_tx,
            replacement.lsn,
            snapshot,
            vector_copy_owner,
        ) {
            if extra_row_bytes > 0 {
                self.finalization.accountant.release(extra_row_bytes);
            }
            return Err(err);
        }
        if incoming_row_bytes > replacement_row_bytes {
            self.finalization
                .accountant
                .release(incoming_row_bytes - replacement_row_bytes);
        }
        let row_uuid = replacement
            .values
            .get("id")
            .and_then(Value::as_uuid)
            .copied();
        let new_state = self
            .table_meta(&table)
            .and_then(|meta| meta.state_machine)
            .and_then(|sm| replacement.values.get(&sm.column))
            .and_then(Value::as_text)
            .map(std::borrow::ToOwned::to_owned);
        let changed = replacement
            .values
            .iter()
            .any(|(column, value)| conflict_row.values.get(column) != Some(value));
        let mut lookup_delta = StagedTupleLookupDelta::default();
        lookup_delta.remove(&table, &ws.relational_inserts[insert_index].1);
        lookup_delta.add(&table, &replacement);
        self.replace_row_graph_in_prepared_write_set(
            ws,
            &table,
            incoming_row_id,
            &replacement,
            read_memory.clone(),
        )?;
        ws.relational_inserts[insert_index] = (table.clone(), replacement);
        if !ws
            .relational_deletes
            .iter()
            .any(|(t, row_id, _)| t == &table && *row_id == conflict_row.row_id)
        {
            ws.relational_deletes.push((
                table.clone(),
                conflict_row.row_id,
                ws.relational_inserts[insert_index].1.created_tx,
            ));
        }
        if changed && let (Some(uuid), Some(state)) = (row_uuid, new_state.as_deref()) {
            let replacement_tx = ws.relational_inserts[insert_index].1.created_tx;
            let mut propagation_options = PreparedPropagationOptions {
                tx: replacement_tx,
                snapshot,
                lookup_delta: Some(&mut lookup_delta),
            };
            self.propagate_state_change_in_prepared_write_set(
                ws,
                &table,
                Some(uuid),
                Some(state),
                &mut propagation_options,
                vector_copy_owner,
                read_memory.clone(),
            )?;
        }
        Ok(Some(CommitTimeUpsertRewrite {
            table,
            row_id: conflict_row.row_id,
            lookup_delta,
        }))
    }

    pub(super) fn relational_insert_index_by_row(
        ws: &WriteSet,
        table: &str,
        row_id: RowId,
        preferred_index: usize,
    ) -> Option<usize> {
        if ws
            .relational_inserts
            .get(preferred_index)
            .is_some_and(|(candidate_table, row)| candidate_table == table && row.row_id == row_id)
        {
            return Some(preferred_index);
        }
        if let Some(index) = preferred_index.checked_sub(1)
            && ws
                .relational_inserts
                .get(index)
                .is_some_and(|(candidate_table, row)| {
                    candidate_table == table && row.row_id == row_id
                })
        {
            return Some(index);
        }
        ws.relational_inserts
            .iter()
            .position(|(candidate_table, row)| candidate_table == table && row.row_id == row_id)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_commit_time_upsert_replacement(
        &self,
        table: &str,
        conflict_row: &VersionedRow,
        values: &HashMap<ColName, Value>,
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if self.relational_store().is_immutable(table) {
            return Err(Error::ImmutableTable(table.to_string()));
        }
        self.assert_row_write_allowed(
            table,
            conflict_row.row_id,
            &conflict_row.values,
            snapshot,
            read_memory.clone(),
        )?;
        self.assert_row_write_allowed(
            table,
            conflict_row.row_id,
            values,
            snapshot,
            read_memory.clone(),
        )?;
        self.validate_commit_time_upsert_state_transition(table, conflict_row, values)?;
        self.validate_commit_time_upsert_vectors(table, values)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_commit_time_upsert_state_transition(
        &self,
        table: &str,
        conflict_row: &VersionedRow,
        values: &HashMap<ColName, Value>,
    ) -> Result<()> {
        let Some(meta) = self.table_meta(table) else {
            return Ok(());
        };
        let Some(state_machine) = meta.state_machine else {
            return Ok(());
        };

        let old_state = conflict_row
            .values
            .get(&state_machine.column)
            .and_then(Value::as_text);
        let new_state = values.get(&state_machine.column).and_then(Value::as_text);
        let (Some(old_state), Some(new_state)) = (old_state, new_state) else {
            return Ok(());
        };

        if self.relational_store().validate_state_transition(
            table,
            &state_machine.column,
            old_state,
            new_state,
        ) {
            return Ok(());
        }

        Err(Error::InvalidStateTransition(format!(
            "{old_state} -> {new_state}"
        )))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_commit_time_upsert_vectors(
        &self,
        table: &str,
        values: &HashMap<ColName, Value>,
    ) -> Result<()> {
        let Some(meta) = self.table_meta(table) else {
            return Ok(());
        };

        for column in &meta.columns {
            if let ColumnType::Vector(expected) = column.column_type
                && let Some(Value::Vector(vector)) = values.get(&column.name)
            {
                let got = vector.len();
                if got != expected {
                    return Err(self.direct_vector_dimension_error(
                        &VectorIndexRef::new(table, column.name.clone()),
                        expected,
                        got,
                    ));
                }
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reconcile_commit_time_upsert_vectors(
        &self,
        ws: &mut WriteSet,
        table: &str,
        incoming_row_id: RowId,
        conflict_row_id: RowId,
        replacement_values: &HashMap<ColName, Value>,
        replacement_tx: TxId,
        replacement_lsn: Lsn,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<()> {
        let Some(meta) = self.table_meta(table) else {
            return Ok(());
        };
        let vector_columns = meta
            .columns
            .iter()
            .filter(|column| matches!(column.column_type, ColumnType::Vector(_)))
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        if vector_columns.is_empty() {
            return Ok(());
        }

        let mut pending_by_column = HashMap::<ColName, VectorEntry>::new();
        let mut pos = 0;
        while pos < ws.vector_inserts.len() {
            let entry = &ws.vector_inserts[pos];
            if entry.index.table == table && entry.row_id == incoming_row_id {
                let entry = ws.vector_inserts.remove(pos);
                if let Some(replaced) = pending_by_column.insert(entry.index.column.clone(), entry)
                {
                    self.finalization
                        .accountant
                        .release(self.vector_insert_accounted_bytes(
                            &replaced.index,
                            replaced.vector.len(),
                            vector_copy_owner,
                        )?);
                }
            } else {
                pos += 1;
            }
        }

        for column in vector_columns {
            let index = VectorIndexRef::new(table, column.clone());
            let existing = self.vector_store.load_live_entry_for_row(
                &index,
                conflict_row_id,
                snapshot,
                vector_copy_owner,
            )?;
            let final_vector = match replacement_values.get(&column) {
                Some(Value::Vector(vector)) => Some(vector.clone()),
                _ => None,
            };
            let Some(final_vector) = final_vector else {
                if let Some(pending) = pending_by_column.remove(&column) {
                    self.finalization
                        .accountant
                        .release(self.vector_insert_accounted_bytes(
                            &pending.index,
                            pending.vector.len(),
                            vector_copy_owner,
                        )?);
                }
                if existing.is_some()
                    && !ws
                        .vector_deletes
                        .iter()
                        .any(|(pending_index, pending_row_id, _)| {
                            *pending_index == index && *pending_row_id == conflict_row_id
                        })
                {
                    ws.vector_deletes
                        .push((index.clone(), conflict_row_id, replacement_tx));
                }
                continue;
            };

            if existing
                .as_ref()
                .is_some_and(|entry| entry.vector == final_vector)
            {
                if let Some(pending) = pending_by_column.remove(&column) {
                    self.finalization
                        .accountant
                        .release(self.vector_insert_accounted_bytes(
                            &pending.index,
                            pending.vector.len(),
                            vector_copy_owner,
                        )?);
                }
                continue;
            }

            if existing.is_some()
                && !ws
                    .vector_deletes
                    .iter()
                    .any(|(pending_index, pending_row_id, _)| {
                        *pending_index == index && *pending_row_id == conflict_row_id
                    })
            {
                ws.vector_deletes
                    .push((index.clone(), conflict_row_id, replacement_tx));
            }

            let mut entry = match pending_by_column.remove(&column) {
                Some(mut pending) if pending.vector == final_vector => {
                    pending.row_id = conflict_row_id;
                    pending.created_tx = replacement_tx;
                    pending.deleted_tx = None;
                    pending.lsn = replacement_lsn;
                    pending
                }
                Some(pending) => {
                    self.finalization
                        .accountant
                        .release(self.vector_insert_accounted_bytes(
                            &pending.index,
                            pending.vector.len(),
                            vector_copy_owner,
                        )?);
                    let bytes = self.vector_insert_accounted_bytes(
                        &index,
                        final_vector.len(),
                        vector_copy_owner,
                    )?;
                    self.finalization.accountant.try_allocate_for_label(
                        bytes,
                        "insert",
                        self.finalization
                            .accountant
                            .prepare_operation_label(format_args!(
                                "vector_insert@{}.{}",
                                index.table, index.column
                            ))?,
                        "Reduce vector dimensionality, insert fewer rows, or raise MEMORY_LIMIT.",
                    )?;
                    VectorEntry {
                        index: index.clone(),
                        row_id: conflict_row_id,
                        vector: final_vector.clone(),
                        created_tx: replacement_tx,
                        deleted_tx: None,
                        lsn: replacement_lsn,
                    }
                }
                None => {
                    let bytes = self.vector_insert_accounted_bytes(
                        &index,
                        final_vector.len(),
                        vector_copy_owner,
                    )?;
                    self.finalization.accountant.try_allocate_for_label(
                        bytes,
                        "insert",
                        self.finalization
                            .accountant
                            .prepare_operation_label(format_args!(
                                "vector_insert@{}.{}",
                                index.table, index.column
                            ))?,
                        "Reduce vector dimensionality, insert fewer rows, or raise MEMORY_LIMIT.",
                    )?;
                    VectorEntry {
                        index: index.clone(),
                        row_id: conflict_row_id,
                        vector: final_vector.clone(),
                        created_tx: replacement_tx,
                        deleted_tx: None,
                        lsn: replacement_lsn,
                    }
                }
            };
            entry.index = index;

            let mut replaced_entries = Vec::new();
            let mut pos = 0;
            while pos < ws.vector_inserts.len() {
                if ws.vector_inserts[pos].index == entry.index
                    && ws.vector_inserts[pos].row_id == conflict_row_id
                {
                    replaced_entries.push(ws.vector_inserts.remove(pos));
                } else {
                    pos += 1;
                }
            }
            for replaced in replaced_entries {
                self.finalization
                    .accountant
                    .release(self.vector_insert_accounted_bytes(
                        &replaced.index,
                        replaced.vector.len(),
                        vector_copy_owner,
                    )?);
            }
            ws.vector_inserts.push(entry);
        }

        for pending in pending_by_column.into_values() {
            self.finalization
                .accountant
                .release(self.vector_insert_accounted_bytes(
                    &pending.index,
                    pending.vector.len(),
                    vector_copy_owner,
                )?);
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn apply_original_commit_time_upsert_if_needed(
        &self,
        ws: &mut WriteSet,
        insert_index: usize,
        skip_deleted: &HashSet<RowId>,
        upsert_lookup: &CommitTimeUpsertLookup<'_>,
        snapshot: SnapshotId,
        stats: Option<&mut CommitStageStats>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<CommitTimeUpsertRewrite>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut stats = stats;
        let (table, row) = ws.relational_inserts[insert_index].clone();
        let Some(intent_indexes) = upsert_lookup.by_row.get(&(table.clone(), row.row_id)) else {
            return Ok(None);
        };
        for intent_index in intent_indexes {
            let intent = &upsert_lookup.intents[*intent_index];
            if intent.conflict_columns.is_empty() {
                continue;
            }
            let mut insert_values = intent.insert_values.clone();
            self.rewrite_txid_placeholders_in_values(
                &table,
                intent.active_tx,
                row.created_tx,
                &mut insert_values,
            );
            let mut conflict_values = Vec::with_capacity(intent.conflict_columns.len());
            let mut has_null = false;
            for column in &intent.conflict_columns {
                match insert_values.get(column) {
                    Some(Value::Null) | None => {
                        has_null = true;
                        break;
                    }
                    Some(value) => conflict_values.push(value.clone()),
                }
            }
            if has_null {
                continue;
            }

            let Some(conflict) = self.committed_unique_conflict(
                &table,
                &intent.conflict_columns,
                &conflict_values,
                snapshot,
                skip_deleted,
                stats.as_deref_mut(),
                read_memory.clone(),
            )?
            else {
                continue;
            };
            if let Some(replacement) = self.apply_commit_time_upsert(
                ws,
                insert_index,
                &conflict,
                intent,
                snapshot,
                vector_copy_owner,
                read_memory.clone(),
            )? {
                return Ok(Some(replacement));
            }
        }

        Ok(None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_unique_constraints_in_write_set(
        &self,
        ws: &mut WriteSet,
        snapshot: SnapshotId,
        upsert_intents: &[PendingUpsertIntent],
        deleted_rows_by_table: &mut HashMap<String, HashMap<RowId, VersionedRow>>,
        stats: &mut CommitStageStats,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let upsert_lookup = CommitTimeUpsertLookup {
            intents: upsert_intents,
            by_row: Self::upsert_intent_indexes_by_row(upsert_intents),
        };
        let upsert_intent_by_conflict = Self::upsert_intent_index_by_conflict(upsert_intents);
        let staged_tuple_columns = self.staged_tuple_key_columns_by_table();
        let mut staged_lookup = StagedTupleLookup::from_write_set(&staged_tuple_columns, ws);
        let mut deleted_by_table = Self::deleted_row_ids_by_table(ws);
        let empty_deleted = HashSet::new();
        let mut index = 0;
        'validate: while index < ws.relational_inserts.len() {
            let (table, row) = ws.relational_inserts[index].clone();
            let meta = self
                .table_meta(&table)
                .ok_or_else(|| Error::TableNotFound(table.clone()))?;
            let skip_deleted = deleted_by_table.get(&table).unwrap_or(&empty_deleted);
            stats.rows_validated = stats.rows_validated.saturating_add(1);

            if let Some(rewrite) = self.apply_original_commit_time_upsert_if_needed(
                ws,
                index,
                skip_deleted,
                &upsert_lookup,
                snapshot,
                Some(stats),
                vector_copy_owner,
                read_memory.clone(),
            )? {
                staged_lookup.apply_delta(&staged_tuple_columns, &rewrite.lookup_delta);
                deleted_by_table = Self::deleted_row_ids_by_table(ws);
                *deleted_rows_by_table =
                    self.deleted_row_snapshots_by_table(ws, snapshot, read_memory.clone())?;
                index =
                    Self::relational_insert_index_by_row(ws, &rewrite.table, rewrite.row_id, index)
                        .unwrap_or_else(|| index.min(ws.relational_inserts.len()));
                continue 'validate;
            }

            for column in meta
                .columns
                .iter()
                .filter(|column| column.primary_key || column.unique)
            {
                let Some(value) = row.values.get(&column.name) else {
                    continue;
                };
                if *value == Value::Null {
                    continue;
                }
                let columns = vec![column.name.clone()];
                let values = vec![value.clone()];
                let staged_conflict = if Self::values_can_be_tuple_key(&values) {
                    staged_lookup
                        .staged_unique_conflict(&table, row.row_id, &columns, &values, stats)
                } else {
                    Self::staged_unique_conflict_in_write_set(
                        ws,
                        index,
                        &table,
                        &columns,
                        &values,
                        Some(stats),
                    )
                };
                if staged_conflict.is_some() {
                    return Err(Error::UniqueViolation {
                        table,
                        column: column.name.clone(),
                    });
                }
                let upsert_intent_index = upsert_intent_by_conflict
                    .get(&(table.clone(), row.row_id, columns.clone()))
                    .copied();
                let deleted_rows = deleted_rows_by_table.get(&table);
                let conflict = if self.committed_unique_probe_required_for_replacement(
                    row.row_id,
                    &columns,
                    &values,
                    skip_deleted,
                    deleted_rows,
                ) {
                    if upsert_intent_index.is_some() {
                        self.committed_unique_conflict(
                            &table,
                            &columns,
                            &values,
                            snapshot,
                            skip_deleted,
                            Some(stats),
                            read_memory.clone(),
                        )?
                    } else if self
                        .committed_unique_conflict_row_id(
                            &table,
                            &columns,
                            &values,
                            snapshot,
                            skip_deleted,
                            Some(stats),
                            read_memory.clone(),
                        )?
                        .is_some()
                    {
                        Some(VersionedRow {
                            row_id: RowId(0),
                            values: HashMap::new(),
                            created_tx: TxId(0),
                            deleted_tx: None,
                            lsn: Lsn(0),
                            created_at: None,
                        })
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some(conflict) = conflict {
                    if let Some(intent_index) = upsert_intent_index
                        && let Some(rewrite) = self.apply_commit_time_upsert(
                            ws,
                            index,
                            &conflict,
                            &upsert_intents[intent_index],
                            snapshot,
                            vector_copy_owner,
                            read_memory.clone(),
                        )?
                    {
                        staged_lookup.apply_delta(&staged_tuple_columns, &rewrite.lookup_delta);
                        deleted_by_table = Self::deleted_row_ids_by_table(ws);
                        *deleted_rows_by_table =
                            self.deleted_row_snapshots_by_table(ws, snapshot, read_memory.clone())?;
                        index = Self::relational_insert_index_by_row(
                            ws,
                            &rewrite.table,
                            rewrite.row_id,
                            index,
                        )
                        .unwrap_or_else(|| index.min(ws.relational_inserts.len()));
                        continue 'validate;
                    }
                    return Err(Error::UniqueViolation {
                        table,
                        column: column.name.clone(),
                    });
                }
            }

            for unique_constraint in meta.enforced_unique_tuples() {
                let mut values = Vec::with_capacity(unique_constraint.len());
                let mut has_null = false;
                for column in unique_constraint {
                    match row.values.get(column) {
                        Some(Value::Null) | None => {
                            has_null = true;
                            break;
                        }
                        Some(value) => values.push(value.clone()),
                    }
                }
                if has_null {
                    continue;
                }
                let staged_conflict = if Self::values_can_be_tuple_key(&values) {
                    staged_lookup.staged_unique_conflict(
                        &table,
                        row.row_id,
                        unique_constraint,
                        &values,
                        stats,
                    )
                } else {
                    Self::staged_unique_conflict_in_write_set(
                        ws,
                        index,
                        &table,
                        unique_constraint,
                        &values,
                        Some(stats),
                    )
                };
                if staged_conflict.is_some() {
                    return Err(Error::UniqueViolation {
                        table,
                        column: unique_constraint.first().cloned().unwrap_or_default(),
                    });
                }
                let upsert_intent_index = upsert_intent_by_conflict
                    .get(&(table.clone(), row.row_id, unique_constraint.clone()))
                    .copied();
                let deleted_rows = deleted_rows_by_table.get(&table);
                let conflict = if self.committed_unique_probe_required_for_replacement(
                    row.row_id,
                    unique_constraint,
                    &values,
                    skip_deleted,
                    deleted_rows,
                ) {
                    if upsert_intent_index.is_some() {
                        self.committed_unique_conflict(
                            &table,
                            unique_constraint,
                            &values,
                            snapshot,
                            skip_deleted,
                            Some(stats),
                            read_memory.clone(),
                        )?
                    } else if self
                        .committed_unique_conflict_row_id(
                            &table,
                            unique_constraint,
                            &values,
                            snapshot,
                            skip_deleted,
                            Some(stats),
                            read_memory.clone(),
                        )?
                        .is_some()
                    {
                        Some(VersionedRow {
                            row_id: RowId(0),
                            values: HashMap::new(),
                            created_tx: TxId(0),
                            deleted_tx: None,
                            lsn: Lsn(0),
                            created_at: None,
                        })
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some(conflict) = conflict {
                    if let Some(intent_index) = upsert_intent_index
                        && let Some(rewrite) = self.apply_commit_time_upsert(
                            ws,
                            index,
                            &conflict,
                            &upsert_intents[intent_index],
                            snapshot,
                            vector_copy_owner,
                            read_memory.clone(),
                        )?
                    {
                        staged_lookup.apply_delta(&staged_tuple_columns, &rewrite.lookup_delta);
                        deleted_by_table = Self::deleted_row_ids_by_table(ws);
                        *deleted_rows_by_table =
                            self.deleted_row_snapshots_by_table(ws, snapshot, read_memory.clone())?;
                        index = Self::relational_insert_index_by_row(
                            ws,
                            &rewrite.table,
                            rewrite.row_id,
                            index,
                        )
                        .unwrap_or_else(|| index.min(ws.relational_inserts.len()));
                        continue 'validate;
                    }
                    return Err(Error::UniqueViolation {
                        table,
                        column: unique_constraint.first().cloned().unwrap_or_default(),
                    });
                }
            }

            index += 1;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reject_if_incoming_lineage_matches_purged(
        record: &DurableLineageRecord,
        table: &str,
        natural_key: &NaturalKey,
        table_generation: u64,
        lineage_root: &str,
        author_node_id: &str,
        author_database_incarnation: &str,
        author_local_mutation_position: u64,
    ) -> Result<()> {
        let exact_purged_root = record.delete_obligation == DurableDeleteObligation::Purged
            && record.table == table
            && record.natural_key == *natural_key
            && record.table_generation == table_generation
            && record.lineage_root == lineage_root
            && record.author_node_id.as_deref() == Some(author_node_id)
            && record.author_database_incarnation.as_deref() == Some(author_database_incarnation)
            && record.author_local_mutation_position == author_local_mutation_position;
        if !exact_purged_root {
            return Ok(());
        }
        let Some(frontier) = record
            .purge_frontier
            .as_deref()
            .and_then(|frontier| frontier.parse::<u64>().ok())
        else {
            return Ok(());
        };
        Err(Error::PurgeCausalityFence {
            table: table.to_string(),
            key: natural_key.pairs(),
            lineage_root: lineage_root.to_string(),
            frontier: Lsn(frontier),
        })
    }

    /// A transaction that began before an authoritative purge can still carry
    /// an update of the destroyed row. Local work is fenced by its pinned
    /// RowId; authenticated sync work is fenced by the exact staged creator
    /// sidecar, so remote RowIds and delete-only writes never decide ancestry.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reject_purged_lineage_recreation(
        &self,
        ws: &WriteSet,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        for (table, row) in &ws.relational_inserts {
            let Some(meta) = self.table_meta(table) else {
                continue;
            };
            let Some(natural_key) = natural_key_from_row_values(&meta, &row.values) else {
                continue;
            };
            let generation = self.durable_lineage_table_generation(table, read_memory.clone())?;
            let purged_lives =
                self.purged_lineage_records_at_generation(table, &natural_key, generation)?;
            if purged_lives.is_empty() {
                continue;
            }
            let sidecar_key = Self::durable_row_lineage_config_key(table, &natural_key, generation);
            if let Some((_, encoded)) = ws.config_writes.iter().find(|(key, _)| key == &sidecar_key)
            {
                let incoming: DurableRowLineageSidecar =
                    RedbPersistence::decode_config_value(encoded)?;
                for record in &purged_lives {
                    Self::reject_if_incoming_lineage_matches_purged(
                        record,
                        table,
                        &natural_key,
                        incoming.table_generation,
                        &incoming.lineage_root,
                        &incoming.author_node_id,
                        &incoming.author_database_incarnation.to_hex(),
                        incoming.author_local_mutation_position.0,
                    )?;
                }
                // Authenticated ancestry is the discriminator for this staged
                // sync row. A different creator root is a fresh lineage even
                // if adjudication began while the old local RowId was visible.
                continue;
            }
            for record in purged_lives {
                if record.local_row_id != Some(row.row_id) {
                    continue;
                }
                if let Some(frontier) = record
                    .purge_frontier
                    .as_deref()
                    .and_then(|frontier| frontier.parse::<u64>().ok())
                {
                    return Err(Error::PurgeCausalityFence {
                        table: table.clone(),
                        key: natural_key.pairs(),
                        lineage_root: record.lineage_root,
                        frontier: Lsn(frontier),
                    });
                }
            }
        }
        for (key, encoded) in &ws.config_writes {
            if let Some(suffix) = key.strip_prefix("sync_row_lineage.v1.") {
                let incoming: DurableRowLineageSidecar =
                    RedbPersistence::decode_config_value(encoded)?;
                let purged_lives = {
                    let lineage_state = self.lineage_state_lock.lock();
                    self.purged_lineage_records_for_suffix(&lineage_state, suffix)?
                };
                for current in &purged_lives {
                    Self::reject_if_incoming_lineage_matches_purged(
                        current,
                        &current.table,
                        &current.natural_key,
                        incoming.table_generation,
                        &incoming.lineage_root,
                        &incoming.author_node_id,
                        &incoming.author_database_incarnation.to_hex(),
                        incoming.author_local_mutation_position.0,
                    )?;
                }
                continue;
            }
            if let Some(suffix) = key.strip_prefix(Self::DURABLE_LINEAGE_CONFIG_PREFIX) {
                let incoming: DurableLineageRecord = RedbPersistence::decode_config_value(encoded)?;
                let purged_lives = {
                    let lineage_state = self.lineage_state_lock.lock();
                    self.purged_lineage_records_for_suffix(&lineage_state, suffix)?
                };
                for current in &purged_lives {
                    Self::reject_if_incoming_lineage_matches_purged(
                        current,
                        &incoming.table,
                        &incoming.natural_key,
                        incoming.table_generation,
                        &incoming.lineage_root,
                        incoming.author_node_id.as_deref().unwrap_or_default(),
                        incoming
                            .author_database_incarnation
                            .as_deref()
                            .unwrap_or_default(),
                        incoming.author_local_mutation_position,
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
    pub(super) fn validate_foreign_keys_in_write_set(
        &self,
        ws: &WriteSet,
        snapshot: SnapshotId,
        deleted_rows_by_table: &HashMap<String, HashMap<RowId, VersionedRow>>,
        stats: &mut CommitStageStats,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let staged_lookup = StagedTupleLookup::rebuild(self, ws)?;
        let deleted_by_table = Self::deleted_row_ids_by_table(ws);
        let empty_deleted = HashSet::new();
        for (table, row) in &ws.relational_inserts {
            stats.rows_validated = stats.rows_validated.saturating_add(1);
            let meta = self
                .table_meta(table)
                .ok_or_else(|| Error::TableNotFound(table.clone()))?;
            for column in &meta.columns {
                let Some(reference) = &column.references else {
                    continue;
                };
                let Some(value) = row.values.get(&column.name) else {
                    continue;
                };
                if *value == Value::Null {
                    continue;
                }
                let staged_deletes = deleted_by_table
                    .get(&reference.table)
                    .unwrap_or(&empty_deleted);
                let parent_columns = vec![reference.column.clone()];
                let parent_values = std::slice::from_ref(value);
                let staged_match = if Self::values_can_be_tuple_key(parent_values) {
                    staged_lookup.contains_tuple(
                        &reference.table,
                        &parent_columns,
                        parent_values,
                        Some(stats),
                    )
                } else {
                    staged_tuple_exists(ws, &reference.table, &parent_columns, parent_values)
                };
                let committed_match = if staged_match {
                    true
                } else {
                    self.visible_row_exists_by_column(
                        &reference.table,
                        &reference.column,
                        value,
                        snapshot,
                        staged_deletes,
                        Some(stats),
                        read_memory.clone(),
                    )?
                };
                if !staged_match && !committed_match {
                    return Err(Error::ForeignKeyViolation {
                        child_table: table.clone(),
                        child_columns: vec![column.name.clone()],
                        parent_table: reference.table.clone(),
                        parent_columns: vec![reference.column.clone()],
                    });
                }
            }
        }

        let reverse_refs = {
            let metas = self.relational_store.table_meta.read();
            metas
                .iter()
                .flat_map(|(child_table, meta)| {
                    meta.columns.iter().filter_map(|column| {
                        column.references.as_ref().map(|reference| {
                            (
                                reference.table.clone(),
                                reference.column.clone(),
                                child_table.clone(),
                                column.name.clone(),
                            )
                        })
                    })
                })
                .collect::<Vec<_>>()
        };

        for (parent_table, parent_row_id, _) in &ws.relational_deletes {
            stats.rows_validated = stats.rows_validated.saturating_add(1);
            let Some(parent_row) = deleted_rows_by_table
                .get(parent_table)
                .and_then(|rows| rows.get(parent_row_id))
            else {
                continue;
            };
            for (ref_table, ref_column, child_table, child_column) in &reverse_refs {
                if ref_table != parent_table {
                    continue;
                }
                let Some(parent_value) = parent_row.values.get(ref_column) else {
                    continue;
                };
                if *parent_value == Value::Null {
                    continue;
                }
                let parent_columns = vec![ref_column.clone()];
                let parent_values = std::slice::from_ref(parent_value);
                let parent_replaced_with_same_key = if Self::values_can_be_tuple_key(parent_values)
                {
                    staged_lookup.contains_tuple(
                        parent_table,
                        &parent_columns,
                        parent_values,
                        Some(stats),
                    )
                } else {
                    staged_tuple_exists(ws, parent_table, &parent_columns, parent_values)
                };
                if parent_replaced_with_same_key {
                    continue;
                }
                let child_deletes = deleted_by_table.get(child_table).unwrap_or(&empty_deleted);
                if self.visible_row_exists_by_column(
                    child_table,
                    child_column,
                    parent_value,
                    snapshot,
                    child_deletes,
                    Some(stats),
                    read_memory.clone(),
                )? {
                    return Err(Error::ForeignKeyViolation {
                        child_table: child_table.clone(),
                        child_columns: vec![child_column.clone()],
                        parent_table: parent_table.clone(),
                        parent_columns: vec![ref_column.clone()],
                    });
                }
            }
        }

        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_composite_foreign_keys_in_write_set(
        &self,
        ws: &WriteSet,
        snapshot: SnapshotId,
        deleted_rows_by_table: &HashMap<String, HashMap<RowId, VersionedRow>>,
        stats: &mut CommitStageStats,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let staged_lookup = StagedTupleLookup::rebuild(self, ws)?;
        let deleted_by_table = Self::deleted_row_ids_by_table(ws);
        let empty_deleted = HashSet::new();
        for (table, row) in &ws.relational_inserts {
            stats.rows_validated = stats.rows_validated.saturating_add(1);
            let meta = self
                .table_meta(table)
                .ok_or_else(|| Error::TableNotFound(table.clone()))?;
            for fk in &meta.composite_foreign_keys {
                let Some(values) = tuple_values_for_row(row, &fk.child_columns) else {
                    continue;
                };
                let parent_deletes = deleted_by_table
                    .get(&fk.parent_table)
                    .unwrap_or(&empty_deleted);
                let staged_match = if Self::values_can_be_tuple_key(&values) {
                    staged_lookup.contains_tuple(
                        &fk.parent_table,
                        &fk.parent_columns,
                        &values,
                        Some(stats),
                    )
                } else {
                    staged_tuple_exists(ws, &fk.parent_table, &fk.parent_columns, &values)
                };
                let committed_match = if staged_match {
                    true
                } else {
                    self.indexed_visible_row_exists_by_columns(
                        &fk.parent_table,
                        &fk.parent_columns,
                        &values,
                        snapshot,
                        parent_deletes,
                        Some(stats),
                        read_memory.clone(),
                    )?
                };
                if !staged_match && !committed_match {
                    return Err(Error::ForeignKeyViolation {
                        child_table: table.clone(),
                        child_columns: fk.child_columns.clone(),
                        parent_table: fk.parent_table.clone(),
                        parent_columns: fk.parent_columns.clone(),
                    });
                }
            }
        }

        let reverse_refs = {
            let metas = self.relational_store.table_meta.read();
            metas
                .iter()
                .flat_map(|(child_table, meta)| {
                    meta.composite_foreign_keys.iter().map(|fk| {
                        (
                            fk.parent_table.clone(),
                            fk.parent_columns.clone(),
                            child_table.clone(),
                            fk.child_columns.clone(),
                        )
                    })
                })
                .collect::<Vec<_>>()
        };

        for (parent_table, parent_row_id, _) in &ws.relational_deletes {
            stats.rows_validated = stats.rows_validated.saturating_add(1);
            let Some(parent_row) = deleted_rows_by_table
                .get(parent_table)
                .and_then(|rows| rows.get(parent_row_id))
            else {
                continue;
            };
            for (ref_table, ref_columns, child_table, child_columns) in &reverse_refs {
                if ref_table != parent_table {
                    continue;
                }
                let Some(parent_values) = tuple_values_for_row(parent_row, ref_columns) else {
                    continue;
                };
                let parent_replaced_with_same_key = if Self::values_can_be_tuple_key(&parent_values)
                {
                    staged_lookup.contains_tuple(
                        parent_table,
                        ref_columns,
                        &parent_values,
                        Some(stats),
                    )
                } else {
                    staged_tuple_exists(ws, parent_table, ref_columns, &parent_values)
                };
                if parent_replaced_with_same_key {
                    continue;
                }

                let staged_child_match = if Self::values_can_be_tuple_key(&parent_values) {
                    staged_lookup.contains_tuple(
                        child_table,
                        child_columns,
                        &parent_values,
                        Some(stats),
                    )
                } else {
                    staged_tuple_exists(ws, child_table, child_columns, &parent_values)
                };
                if staged_child_match {
                    return Err(Error::ForeignKeyViolation {
                        child_table: child_table.clone(),
                        child_columns: child_columns.clone(),
                        parent_table: parent_table.clone(),
                        parent_columns: ref_columns.clone(),
                    });
                }

                let child_deletes = deleted_by_table.get(child_table).unwrap_or(&empty_deleted);
                if self.indexed_visible_row_exists_by_columns(
                    child_table,
                    child_columns,
                    &parent_values,
                    snapshot,
                    child_deletes,
                    Some(stats),
                    read_memory.clone(),
                )? {
                    return Err(Error::ForeignKeyViolation {
                        child_table: child_table.clone(),
                        child_columns: child_columns.clone(),
                        parent_table: parent_table.clone(),
                        parent_columns: ref_columns.clone(),
                    });
                }
            }
        }

        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn revalidate_conditional_updates(
        &self,
        ws: &mut WriteSet,
        snapshot: SnapshotId,
        guards: &[PendingConditionalUpdateGuard],
        mut stats: Option<&mut CommitStageStats>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<CommitValidationOutcome> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut outcome = CommitValidationOutcome::default();
        for guard in guards.iter().rev() {
            let mut read_credit = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
            if let Some(stats) = stats.as_deref_mut() {
                stats.rows_validated = stats.rows_validated.saturating_add(1);
            }
            let matches = match self.table_meta(&guard.table) {
                Some(meta) => {
                    let predicate_columns = guard
                        .predicates
                        .iter()
                        .map(|(column, _)| column.clone())
                        .collect::<Vec<_>>();
                    let vector_columns = meta
                        .columns
                        .iter()
                        .filter(|column| matches!(column.column_type, ColumnType::Vector(_)))
                        .map(|column| column.name.as_str())
                        .collect::<HashSet<_>>();
                    let resident = self.committed_row_at_admitted(
                        &guard.table,
                        guard.row_id,
                        snapshot,
                        &mut read_credit,
                        read_memory.clone(),
                    )?;
                    let needs_body = resident.as_ref().is_some_and(|row| {
                        predicate_columns.iter().any(|column| {
                            !vector_columns.contains(column.as_str())
                                && !row.values.contains_key(column)
                        })
                    });
                    let row = if needs_body {
                        self.committed_row_at_admitted(
                            &guard.table,
                            guard.row_id,
                            snapshot,
                            &mut read_credit,
                            read_memory.clone(),
                        )?
                    } else {
                        resident
                    };
                    match row {
                        Some(mut row) => {
                            self.supplement_selected_vector_columns(
                                &guard.table,
                                snapshot,
                                None,
                                Some(&predicate_columns),
                                std::slice::from_mut(&mut row),
                                vector_copy_owner,
                                read_memory.clone(),
                            )?;
                            row_conditions_match(&row, &meta, &guard.predicates)
                        }
                        None => false,
                    }
                }
                None => false,
            };
            if matches {
                continue;
            }

            self.release_insert_allocations_for_slice(
                ws,
                guard.before,
                guard.after,
                vector_copy_owner,
            )?;
            remove_write_set_slice(ws, guard.before, guard.after);
            if guard.fail_on_conflict {
                outcome.conditional_conflict_count =
                    outcome.conditional_conflict_count.saturating_add(1);
            } else {
                outcome.conditional_noop_count = outcome.conditional_noop_count.saturating_add(1);
            }
        }
        Ok(outcome)
    }

    /// A fresh creator delete is authorized for one exact committed row
    /// instance. If that row is replaced or edited before commit, fail the
    /// entire sync transaction so its receipt and lineage writes cannot be
    /// acknowledged without the delete.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_sync_lineage_guards(
        &self,
        snapshot: SnapshotId,
        guards: &[PendingSyncLineageGuard],

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        for guard in guards {
            let unchanged = self
                .committed_row_at(&guard.table, guard.row_id, snapshot, read_memory.clone())?
                .is_some_and(|row| row.values == guard.values);
            if !unchanged {
                return Err(Error::SyncError((format!(
                    "authenticated creator delete target {} row {} changed before commit; retry the sync unit",
                    guard.table, guard.row_id.0
                )).into()));
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn release_insert_allocations_for_slice(
        &self,
        ws: &WriteSet,
        before: WriteSetCounts,
        after: WriteSetCounts,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<()> {
        for (table, row) in ws
            .relational_inserts
            .iter()
            .skip(before.relational_inserts)
            .take(
                after
                    .relational_inserts
                    .saturating_sub(before.relational_inserts),
            )
        {
            let bytes = self.staged_row_release_bytes(ws, table, row, self.table_meta(table));
            self.finalization.accountant.release(bytes);
        }

        for edge in ws
            .adj_inserts
            .iter()
            .skip(before.adj_inserts)
            .take(after.adj_inserts.saturating_sub(before.adj_inserts))
        {
            self.finalization
                .accountant
                .release(standalone_edge_retained_bytes(edge));
        }

        for entry in ws
            .vector_inserts
            .iter()
            .skip(before.vector_inserts)
            .take(after.vector_inserts.saturating_sub(before.vector_inserts))
        {
            self.finalization
                .accountant
                .release(self.vector_insert_accounted_bytes(
                    &entry.index,
                    entry.vector.len(),
                    vector_copy_owner,
                )?);
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn propagate_state_change_if_needed(
        &self,
        tx: TxId,
        table: &str,
        row_uuid: Option<uuid::Uuid>,
        new_state: Option<&str>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let (Some(uuid), Some(state)) = (row_uuid, new_state) {
            if !self.propagation_rules_can_react(table, state) {
                return Ok(());
            }
            let already_propagating = self
                .tx_mgr
                .with_read(tx, |ws| *ws.propagation_in_progress)?;
            if !already_propagating {
                self.tx_mgr
                    .update_flags(tx, |flags| flags.propagation_in_progress = true)?;
                let propagate_result = self.propagate(
                    tx,
                    table,
                    uuid,
                    state,
                    vector_copy_owner,
                    read_memory.clone(),
                );
                self.tx_mgr
                    .update_flags(tx, |flags| flags.propagation_in_progress = false)?;
                propagate_result?;
            }
        }

        Ok(())
    }

    pub(crate) fn propagation_rules_can_react(&self, table: &str, state: &str) -> bool {
        let metas = self.relational_store().table_meta.read();
        if metas.get(table).is_some_and(|meta| {
            meta.propagation_rules.iter().any(|rule| match rule {
                PropagationRule::Edge { trigger_state, .. }
                | PropagationRule::VectorExclusion { trigger_state } => trigger_state == state,
                PropagationRule::ForeignKey { .. } => false,
            })
        }) {
            return true;
        }
        metas.values().any(|meta| {
            meta.propagation_rules.iter().any(|rule| {
                matches!(
                    rule,
                    PropagationRule::ForeignKey {
                        referenced_table,
                        trigger_state,
                        ..
                    } if referenced_table == table && trigger_state == state
                )
            })
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn propagate(
        &self,
        tx: TxId,
        table: &str,
        row_uuid: uuid::Uuid,
        new_state: &str,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let snapshot = self.snapshot_for_read();
        let metas = self.relational_store().table_meta.read().clone();
        let mut queue: VecDeque<PropagationQueueEntry> = VecDeque::new();
        let mut visited: HashSet<(String, uuid::Uuid)> = HashSet::new();
        let mut abort_violation: Option<Error> = None;
        let ctx = PropagationContext {
            tx,
            snapshot,
            metas: &metas,
        };
        let root = PropagationSource {
            table,
            uuid: row_uuid,
            state: new_state,
            depth: 0,
        };

        self.enqueue_fk_children(&ctx, &mut queue, root, read_memory.clone());
        self.enqueue_edge_children(&ctx, &mut queue, root, read_memory.clone())?;
        self.apply_vector_exclusions(&ctx, root, vector_copy_owner, read_memory.clone())?;

        while let Some(entry) = queue.pop_front() {
            if !visited.insert((entry.table.clone(), entry.uuid)) {
                continue;
            }

            let Some(meta) = metas.get(&entry.table) else {
                continue;
            };

            let Some(state_machine) = &meta.state_machine else {
                let msg = format!(
                    "warning: propagation target table {} has no state machine",
                    entry.table
                );
                eprintln!("{msg}");
                if entry.abort_on_failure && abort_violation.is_none() {
                    abort_violation = Some(Error::PropagationAborted {
                        table: entry.table.clone(),
                        column: String::new(),
                        from: String::new(),
                        to: entry.target_state.clone(),
                    });
                }
                continue;
            };

            let state_column = state_machine.column.clone();
            let Some(existing) = self.relational_point_with_tx(
                Some(tx),
                &entry.table,
                "id",
                &Value::Uuid(entry.uuid),
                snapshot,
                read_memory.clone(),
            )?
            else {
                continue;
            };

            let from_state = existing
                .values
                .get(&state_column)
                .and_then(Value::as_text)
                .unwrap_or("")
                .to_string();

            let mut next_values = existing.values.clone();
            next_values.insert(
                state_column.clone(),
                Value::Text(entry.target_state.clone()),
            );

            self.assert_row_write_allowed(
                &entry.table,
                existing.row_id,
                &existing.values,
                snapshot,
                read_memory.clone(),
            )?;
            self.assert_row_write_allowed(
                &entry.table,
                existing.row_id,
                &next_values,
                snapshot,
                read_memory.clone(),
            )?;

            let upsert_outcome = (|| {
                if existing.values == next_values {
                    return Ok(UpsertResult::NoOp);
                }
                self.validate_commit_time_upsert_state_transition(
                    &entry.table,
                    &existing,
                    &next_values,
                )?;
                let admission = self.admit_retained_row_owned(
                    &entry.table,
                    &next_values,
                    meta,
                    "relational",
                    "staged_index_posting",
                    "Reduce propagation growth or raise MEMORY_LIMIT.",
                )?;
                let row = self.replace_row_after_update_validation(
                    tx,
                    &entry.table,
                    existing.row_id,
                    next_values,
                    snapshot,
                    read_memory.clone(),
                )?;
                self.record_owned_row_admission(tx, &entry.table, row, admission);
                Ok(UpsertResult::Updated)
            })();

            let reached_state = match upsert_outcome {
                Ok(UpsertResult::Updated) => entry.target_state.as_str(),
                Ok(UpsertResult::NoOp) | Ok(UpsertResult::Inserted) => continue,
                Err(Error::InvalidStateTransition(_)) => {
                    eprintln!(
                        "warning: skipped invalid propagated transition {}.{} {} -> {}",
                        entry.table, state_column, from_state, entry.target_state
                    );
                    if entry.abort_on_failure && abort_violation.is_none() {
                        abort_violation = Some(Error::PropagationAborted {
                            table: entry.table.clone(),
                            column: state_column.clone(),
                            from: from_state,
                            to: entry.target_state.clone(),
                        });
                    }
                    continue;
                }
                Err(err) => return Err(err),
            };

            self.enqueue_edge_children(
                &ctx,
                &mut queue,
                PropagationSource {
                    table: &entry.table,
                    uuid: entry.uuid,
                    state: reached_state,
                    depth: entry.depth,
                },
                read_memory.clone(),
            )?;
            self.apply_vector_exclusions(
                &ctx,
                PropagationSource {
                    table: &entry.table,
                    uuid: entry.uuid,
                    state: reached_state,
                    depth: entry.depth,
                },
                vector_copy_owner,
                read_memory.clone(),
            )?;

            self.enqueue_fk_children(
                &ctx,
                &mut queue,
                PropagationSource {
                    table: &entry.table,
                    uuid: entry.uuid,
                    state: reached_state,
                    depth: entry.depth,
                },
                read_memory.clone(),
            );
        }

        if let Some(err) = abort_violation {
            return Err(err);
        }

        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn propagate_state_change_in_prepared_write_set(
        &self,
        ws: &mut WriteSet,
        table: &str,
        row_uuid: Option<uuid::Uuid>,
        new_state: Option<&str>,
        options: &mut PreparedPropagationOptions<'_>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let (Some(uuid), Some(state)) = (row_uuid, new_state) else {
            return Ok(());
        };
        if !self.propagation_rules_can_react(table, state) {
            return Ok(());
        }
        if ws.propagation_in_progress {
            return Ok(());
        }

        ws.propagation_in_progress = true;
        let result = self.propagate_in_prepared_write_set(
            ws,
            table,
            uuid,
            state,
            options,
            vector_copy_owner,
            read_memory.clone(),
        );
        ws.propagation_in_progress = false;
        result
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn propagate_in_prepared_write_set(
        &self,
        ws: &mut WriteSet,
        table: &str,
        row_uuid: uuid::Uuid,
        new_state: &str,
        options: &mut PreparedPropagationOptions<'_>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let metas = self.relational_store().table_meta.read().clone();
        let mut queue: VecDeque<PropagationQueueEntry> = VecDeque::new();
        let mut visited: HashSet<(String, uuid::Uuid)> = HashSet::new();
        let mut abort_violation: Option<Error> = None;
        let ctx = PropagationContext {
            tx: options.tx,
            snapshot: options.snapshot,
            metas: &metas,
        };
        let root = PropagationSource {
            table,
            uuid: row_uuid,
            state: new_state,
            depth: 0,
        };

        self.enqueue_fk_children_in_prepared_write_set(
            ws,
            &ctx,
            &mut queue,
            root,
            read_memory.clone(),
        )?;
        self.enqueue_edge_children_in_prepared_write_set(
            ws,
            &ctx,
            &mut queue,
            root,
            read_memory.clone(),
        )?;
        self.apply_vector_exclusions_in_prepared_write_set(
            ws,
            &ctx,
            root,
            vector_copy_owner,
            read_memory.clone(),
        )?;

        while let Some(entry) = queue.pop_front() {
            if !visited.insert((entry.table.clone(), entry.uuid)) {
                continue;
            }

            let Some(meta) = metas.get(&entry.table) else {
                continue;
            };

            let Some(state_machine) = &meta.state_machine else {
                let msg = format!(
                    "warning: propagation target table {} has no state machine",
                    entry.table
                );
                eprintln!("{msg}");
                if entry.abort_on_failure && abort_violation.is_none() {
                    abort_violation = Some(Error::PropagationAborted {
                        table: entry.table.clone(),
                        column: String::new(),
                        from: String::new(),
                        to: entry.target_state.clone(),
                    });
                }
                continue;
            };

            let state_column = state_machine.column.clone();
            let Some(existing) = self.point_lookup_in_prepared_write_set(
                ws,
                &entry.table,
                "id",
                &Value::Uuid(entry.uuid),
                options.snapshot,
                read_memory.clone(),
            )?
            else {
                continue;
            };

            let from_state = existing
                .values
                .get(&state_column)
                .and_then(Value::as_text)
                .unwrap_or("")
                .to_string();

            let mut next_values = existing.values.clone();
            next_values.insert(
                state_column.clone(),
                Value::Text(entry.target_state.clone()),
            );

            self.assert_row_write_allowed(
                &entry.table,
                existing.row_id,
                &existing.values,
                options.snapshot,
                read_memory.clone(),
            )?;
            self.assert_row_write_allowed(
                &entry.table,
                existing.row_id,
                &next_values,
                options.snapshot,
                read_memory.clone(),
            )?;

            let reached_state = match self.upsert_row_in_prepared_write_set(
                ws,
                &entry.table,
                &state_column,
                &existing,
                next_values,
                options,
                read_memory.clone(),
            ) {
                Ok(true) => entry.target_state.as_str(),
                Ok(false) => continue,
                Err(Error::InvalidStateTransition(_)) => {
                    eprintln!(
                        "warning: skipped invalid propagated transition {}.{} {} -> {}",
                        entry.table, state_column, from_state, entry.target_state
                    );
                    if entry.abort_on_failure && abort_violation.is_none() {
                        abort_violation = Some(Error::PropagationAborted {
                            table: entry.table.clone(),
                            column: state_column.clone(),
                            from: from_state,
                            to: entry.target_state.clone(),
                        });
                    }
                    continue;
                }
                Err(err) => return Err(err),
            };

            let propagated = PropagationSource {
                table: &entry.table,
                uuid: entry.uuid,
                state: reached_state,
                depth: entry.depth,
            };
            self.enqueue_edge_children_in_prepared_write_set(
                ws,
                &ctx,
                &mut queue,
                propagated,
                read_memory.clone(),
            )?;
            self.apply_vector_exclusions_in_prepared_write_set(
                ws,
                &ctx,
                propagated,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            self.enqueue_fk_children_in_prepared_write_set(
                ws,
                &ctx,
                &mut queue,
                propagated,
                read_memory.clone(),
            )?;
        }

        if let Some(err) = abort_violation {
            return Err(err);
        }

        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn enqueue_fk_children_in_prepared_write_set(
        &self,
        ws: &WriteSet,
        ctx: &PropagationContext<'_>,
        queue: &mut VecDeque<PropagationQueueEntry>,
        source: PropagationSource<'_>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        for (owner_table, owner_meta) in ctx.metas {
            for rule in &owner_meta.propagation_rules {
                let PropagationRule::ForeignKey {
                    fk_column,
                    referenced_table,
                    trigger_state,
                    target_state,
                    max_depth,
                    abort_on_failure,
                    ..
                } = rule
                else {
                    continue;
                };

                if referenced_table != source.table || trigger_state != source.state {
                    continue;
                }

                if source.depth >= *max_depth {
                    continue;
                }

                let rows = self.scan_filter_in_prepared_write_set(
                    ws,
                    owner_table,
                    ctx.snapshot,
                    &|row| row.values.get(fk_column) == Some(&Value::Uuid(source.uuid)),
                    read_memory.clone(),
                )?;

                for row in rows {
                    if let Some(id) = row.values.get("id").and_then(Value::as_uuid).copied() {
                        queue.push_back(PropagationQueueEntry {
                            table: owner_table.clone(),
                            uuid: id,
                            target_state: target_state.clone(),
                            depth: source.depth + 1,
                            abort_on_failure: *abort_on_failure,
                        });
                    }
                }
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn enqueue_edge_children_in_prepared_write_set(
        &self,
        ws: &WriteSet,
        ctx: &PropagationContext<'_>,
        queue: &mut VecDeque<PropagationQueueEntry>,
        source: PropagationSource<'_>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let Some(meta) = ctx.metas.get(source.table) else {
            return Ok(());
        };

        for rule in &meta.propagation_rules {
            let PropagationRule::Edge {
                edge_type,
                direction,
                trigger_state,
                target_state,
                max_depth,
                abort_on_failure,
            } = rule
            else {
                continue;
            };

            if trigger_state != source.state || source.depth >= *max_depth {
                continue;
            }

            let capture = self.graph_cursor_prepared_read_guard(ws, ctx.tx, read_memory.clone())?;
            let _captured = capture.enter();
            let (bfs, _, _bfs_memory) = self.graph_bfs_counted(
                Some(ctx.tx),
                source.uuid,
                Some(std::slice::from_ref(edge_type)),
                *direction,
                1..=1,
                ctx.snapshot,
                read_memory.clone(),
            )?;

            for node in bfs.nodes {
                if self
                    .point_lookup_in_prepared_write_set(
                        ws,
                        source.table,
                        "id",
                        &Value::Uuid(node.id),
                        ctx.snapshot,
                        read_memory.clone(),
                    )?
                    .is_some()
                {
                    queue.push_back(PropagationQueueEntry {
                        table: source.table.to_string(),
                        uuid: node.id,
                        target_state: target_state.clone(),
                        depth: source.depth + 1,
                        abort_on_failure: *abort_on_failure,
                    });
                }
            }
        }

        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn apply_vector_exclusions_in_prepared_write_set(
        &self,
        ws: &mut WriteSet,
        ctx: &PropagationContext<'_>,
        source: PropagationSource<'_>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let Some(meta) = ctx.metas.get(source.table) else {
            return Ok(());
        };

        for rule in &meta.propagation_rules {
            let PropagationRule::VectorExclusion { trigger_state } = rule else {
                continue;
            };
            if trigger_state != source.state {
                continue;
            }
            let Some(index) = self.table_meta(source.table).and_then(|meta| {
                meta.columns
                    .iter()
                    .find(|column| matches!(column.column_type, ColumnType::Vector(_)))
                    .map(|column| VectorIndexRef::new(source.table, column.name.clone()))
            }) else {
                continue;
            };
            for row_id in self.logical_row_ids_for_uuid_in_prepared_write_set(
                ws,
                source.table,
                source.uuid,
                ctx.snapshot,
                read_memory.clone(),
            )? {
                self.delete_vector_in_prepared_write_set(
                    ws,
                    ctx.tx,
                    index.clone(),
                    row_id,
                    ctx.snapshot,
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
            }
        }

        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn scan_in_prepared_write_set(
        &self,
        ws: &WriteSet,
        table: &str,
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut result = self.committed_rows_at(table, snapshot, read_memory.clone())?;

        let committed_row_ids: HashSet<RowId> = result.iter().map(|row| row.row_id).collect();
        let deleted_row_ids: HashSet<RowId> = ws
            .relational_deletes
            .iter()
            .filter(|(delete_table, _, _)| delete_table == table)
            .map(|(_, row_id, _)| *row_id)
            .collect();
        result.retain(|row| {
            !deleted_row_ids.contains(&row.row_id)
                && !row_matches_delete_predicates(&ws.relational_delete_predicates, table, row)
        });

        let mut seen_inserts = HashSet::new();
        let mut inserts = ws
            .relational_inserts
            .iter()
            .rev()
            .filter(|(insert_table, row)| {
                insert_table == table
                    && seen_inserts.insert(row.row_id)
                    && (!deleted_row_ids.contains(&row.row_id)
                        || committed_row_ids.contains(&row.row_id))
            })
            .map(|(_, row)| row.clone())
            .collect::<Vec<_>>();
        inserts.reverse();
        result.extend(inserts);

        Ok(result)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn scan_filter_in_prepared_write_set(
        &self,
        ws: &WriteSet,
        table: &str,
        snapshot: SnapshotId,
        predicate: &dyn Fn(&VersionedRow) -> bool,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        Ok(self
            .scan_in_prepared_write_set(ws, table, snapshot, read_memory.clone())?
            .into_iter()
            .filter(predicate)
            .collect())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn point_lookup_in_prepared_write_set(
        &self,
        ws: &WriteSet,
        table: &str,
        col: &str,
        value: &Value,
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        Ok(self
            .scan_in_prepared_write_set(ws, table, snapshot, read_memory.clone())?
            .into_iter()
            .find(|row| row.values.get(col) == Some(value)))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn row_by_id_in_prepared_write_set(
        &self,
        ws: &WriteSet,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        Ok(self
            .scan_in_prepared_write_set(ws, table, snapshot, read_memory.clone())?
            .into_iter()
            .find(|row| row.row_id == row_id))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn logical_row_ids_for_uuid_in_prepared_write_set(
        &self,
        ws: &WriteSet,
        table: &str,
        uuid: uuid::Uuid,
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<RowId>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        Ok(self
            .scan_in_prepared_write_set(ws, table, snapshot, read_memory.clone())?
            .into_iter()
            .filter(|row| row.values.get("id") == Some(&Value::Uuid(uuid)))
            .map(|row| row.row_id)
            .collect())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn upsert_row_in_prepared_write_set(
        &self,
        ws: &mut WriteSet,
        table: &str,
        state_column: &str,
        existing: &VersionedRow,
        next_values: HashMap<ColName, Value>,
        options: &mut PreparedPropagationOptions<'_>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if self.relational_store().is_immutable(table) {
            return Err(Error::ImmutableTable(table.to_string()));
        }

        let old_state = existing
            .values
            .get(state_column)
            .and_then(Value::as_text)
            .unwrap_or("");
        let new_state = next_values
            .get(state_column)
            .and_then(Value::as_text)
            .unwrap_or("");
        if old_state == new_state && next_values == existing.values {
            return Ok(false);
        }
        if !self.relational_store().validate_state_transition(
            table,
            state_column,
            old_state,
            new_state,
        ) {
            return Err(Error::InvalidStateTransition(format!(
                "{old_state} -> {new_state}"
            )));
        }

        let changed = next_values
            .iter()
            .any(|(column, value)| existing.values.get(column) != Some(value));
        if !changed {
            return Ok(false);
        }

        self.validate_primary_key_values_present(table, &next_values)?;

        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
        let row_bytes = self.admit_retained_row_bytes(
            table,
            &next_values,
            &meta,
            "update",
            "prepared_state_propagation_row_replace",
            "Reduce row growth or raise MEMORY_LIMIT before committing this propagated update.",
        )?;

        if let Err(err) = self.delete_row_in_prepared_write_set(
            ws,
            options.tx,
            table,
            existing.row_id,
            options.lookup_delta.as_deref_mut(),
            read_memory.clone(),
        ) {
            self.finalization.accountant.release(row_bytes);
            return Err(err);
        }

        let inserted = VersionedRow {
            row_id: existing.row_id,
            values: next_values,
            created_tx: options.tx,
            deleted_tx: None,
            lsn: ws.commit_lsn.unwrap_or(Lsn(0)),
            created_at: Some(current_wallclock()),
        };
        self.replace_row_graph_in_prepared_write_set(
            ws,
            table,
            existing.row_id,
            &inserted,
            read_memory.clone(),
        )?;
        if let Some(delta) = options.lookup_delta.as_deref_mut() {
            delta.add(table, &inserted);
        }
        if let Err(error) = self.relational.stage_ordered_row_from_descriptor(
            ws,
            table,
            &inserted,
            ws.relational_inserts.len(),
            &meta,
        ) {
            self.finalization.accountant.release(row_bytes);
            return Err(error);
        }
        ws.set_relational_insert_admitted_bytes(table, inserted.row_id, row_bytes);
        ws.relational_inserts.push((table.to_string(), inserted));
        Ok(true)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn delete_row_in_prepared_write_set(
        &self,
        ws: &mut WriteSet,
        tx: TxId,
        table: &str,
        row_id: RowId,
        mut lookup_delta: Option<&mut StagedTupleLookupDelta>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if !self.relational_store.table_meta.read().contains_key(table) {
            return Err(Error::TableNotFound(table.to_string()));
        }
        if self.relational_store().is_immutable(table) {
            return Err(Error::ImmutableTable(table.to_string()));
        }

        let mut removed_inserts = Vec::new();
        let mut pos = 0;
        while pos < ws.relational_inserts.len() {
            if ws.relational_inserts[pos].0 == table
                && ws.relational_inserts[pos].1.row_id == row_id
            {
                removed_inserts.push(ws.relational_inserts.remove(pos));
            } else {
                pos += 1;
            }
        }
        for (removed_table, row) in removed_inserts {
            if let Some(delta) = lookup_delta.as_deref_mut() {
                delta.remove(&removed_table, &row);
            }
            let bytes = self.staged_row_release_bytes(
                ws,
                &removed_table,
                &row,
                self.table_meta(&removed_table),
            );
            self.finalization.accountant.release(bytes);
            ws.forget_relational_insert_admitted_bytes(&removed_table, row.row_id);
        }

        let committed_row_exists = self
            .committed_row_at(table, row_id, self.snapshot_for_read(), read_memory.clone())?
            .is_some();
        if committed_row_exists
            && !ws
                .relational_deletes
                .iter()
                .any(|(delete_table, deleted_row_id, _)| {
                    delete_table == table && *deleted_row_id == row_id
                })
        {
            ws.relational_deletes.push((table.to_string(), row_id, tx));
        }

        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn delete_vector_in_prepared_write_set(
        &self,
        ws: &mut WriteSet,
        tx: TxId,
        index: VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.vector_store.with_index_layout(&index, |_| ())?;
        let Some(row) = self.row_by_id_in_prepared_write_set(
            ws,
            &index.table,
            row_id,
            snapshot,
            read_memory.clone(),
        )?
        else {
            return Err(Error::NotFound(format!(
                "row {row_id} in table {}",
                index.table
            )));
        };
        self.assert_row_write_allowed(
            &index.table,
            row.row_id,
            &row.values,
            snapshot,
            read_memory.clone(),
        )?;
        let existing_live = self.vector_store.has_live_entry_for_row(
            &index,
            row_id,
            snapshot,
            vector_copy_owner,
        )?;
        let mut canceled_inserts = Vec::new();
        let mut pos = 0;
        while pos < ws.vector_inserts.len() {
            if ws.vector_inserts[pos].index == index && ws.vector_inserts[pos].row_id == row_id {
                canceled_inserts.push(ws.vector_inserts.remove(pos));
            } else {
                pos += 1;
            }
        }

        let mut moved_sources = Vec::new();
        let mut pos = 0;
        while pos < ws.vector_moves.len() {
            let (move_index, old_row_id, new_row_id, _) = &ws.vector_moves[pos];
            if *move_index == index && *new_row_id == row_id {
                moved_sources.push(*old_row_id);
                ws.vector_moves.remove(pos);
            } else {
                pos += 1;
            }
        }
        let pending_move_from_row = ws
            .vector_moves
            .iter()
            .any(|(move_index, old_row_id, _, _)| *move_index == index && *old_row_id == row_id);
        for old_row_id in moved_sources {
            if !ws
                .vector_deletes
                .iter()
                .any(|(pending_index, pending_row_id, _)| {
                    *pending_index == index && *pending_row_id == old_row_id
                })
            {
                ws.vector_deletes.push((index.clone(), old_row_id, tx));
            }
        }

        let already_deleted = ws
            .vector_deletes
            .iter()
            .any(|(pending_index, pending_row_id, _)| {
                *pending_index == index && *pending_row_id == row_id
            });
        if !pending_move_from_row && existing_live && !already_deleted {
            ws.vector_deletes.push((index.clone(), row_id, tx));
        }
        for entry in canceled_inserts {
            let bytes = self.vector_insert_accounted_bytes(
                &entry.index,
                entry.vector.len(),
                vector_copy_owner,
            )?;
            // The canceled body and its owned index strings are destroyed
            // before returning their existing insertion invoice.
            drop(entry);
            self.finalization.accountant.release(bytes);
        }
        Ok(())
    }

    pub(super) fn enqueue_fk_children(
        &self,
        ctx: &PropagationContext<'_>,
        queue: &mut VecDeque<PropagationQueueEntry>,
        source: PropagationSource<'_>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        for (owner_table, owner_meta) in ctx.metas {
            for rule in &owner_meta.propagation_rules {
                let PropagationRule::ForeignKey {
                    fk_column,
                    referenced_table,
                    trigger_state,
                    target_state,
                    max_depth,
                    abort_on_failure,
                    ..
                } = rule
                else {
                    continue;
                };

                if referenced_table != source.table || trigger_state != source.state {
                    continue;
                }

                if source.depth >= *max_depth {
                    continue;
                }

                let rows = match self.relational_scan_filter_with_tx(
                    Some(ctx.tx),
                    owner_table,
                    ctx.snapshot,
                    &|row| row.values.get(fk_column) == Some(&Value::Uuid(source.uuid)),
                    read_memory.clone(),
                ) {
                    Ok(rows) => rows,
                    Err(err) => {
                        eprintln!(
                            "warning: propagation scan failed for {owner_table}.{fk_column}: {err}"
                        );
                        continue;
                    }
                };

                for row in rows {
                    if let Some(id) = row.values.get("id").and_then(Value::as_uuid).copied() {
                        queue.push_back(PropagationQueueEntry {
                            table: owner_table.clone(),
                            uuid: id,
                            target_state: target_state.clone(),
                            depth: source.depth + 1,
                            abort_on_failure: *abort_on_failure,
                        });
                    }
                }
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn enqueue_edge_children(
        &self,
        ctx: &PropagationContext<'_>,
        queue: &mut VecDeque<PropagationQueueEntry>,
        source: PropagationSource<'_>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let Some(meta) = ctx.metas.get(source.table) else {
            return Ok(());
        };

        for rule in &meta.propagation_rules {
            let PropagationRule::Edge {
                edge_type,
                direction,
                trigger_state,
                target_state,
                max_depth,
                abort_on_failure,
            } = rule
            else {
                continue;
            };

            if trigger_state != source.state || source.depth >= *max_depth {
                continue;
            }

            let bfs = self.query_bfs_with_read_memory(
                source.uuid,
                Some(std::slice::from_ref(edge_type)),
                *direction,
                1,
                ctx.snapshot,
                read_memory.clone(),
            )?;

            for node in bfs.nodes {
                if self
                    .relational_point_with_tx(
                        Some(ctx.tx),
                        source.table,
                        "id",
                        &Value::Uuid(node.id),
                        ctx.snapshot,
                        read_memory.clone(),
                    )?
                    .is_some()
                {
                    queue.push_back(PropagationQueueEntry {
                        table: source.table.to_string(),
                        uuid: node.id,
                        target_state: target_state.clone(),
                        depth: source.depth + 1,
                        abort_on_failure: *abort_on_failure,
                    });
                }
            }
        }

        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn apply_vector_exclusions(
        &self,
        ctx: &PropagationContext<'_>,
        source: PropagationSource<'_>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let Some(meta) = ctx.metas.get(source.table) else {
            return Ok(());
        };

        for rule in &meta.propagation_rules {
            let PropagationRule::VectorExclusion { trigger_state } = rule else {
                continue;
            };
            if trigger_state != source.state {
                continue;
            }
            for row_id in self.logical_row_ids_for_uuid(ctx.tx, source.table, source.uuid) {
                let index = self
                    .table_meta(source.table)
                    .and_then(|meta| {
                        meta.columns
                            .iter()
                            .find(|column| {
                                matches!(column.column_type, contextdb_core::ColumnType::Vector(_))
                            })
                            .map(|column| VectorIndexRef::new(source.table, column.name.clone()))
                    })
                    .unwrap_or_default();
                self.delete_vector_with_read_memory(
                    ctx.tx,
                    index,
                    row_id,
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
            }
        }

        Ok(())
    }

    /// Delete a row and everything the row owns.
    ///
    /// A vector column lives in its own index, not in the relational row, so
    /// removing the relational body alone leaves every vector behind: the row
    /// is gone from `SELECT` and still answered by nearest-neighbour search,
    /// and the store keeps paying for what it holds.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn delete_row(&self, tx: TxId, table: &str, row_id: RowId) -> Result<()> {
        let _operation = self.open_operation()?;
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.delete_row_with_read_memory(tx, table, row_id, &mut vector_copy_owner, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn delete_row_with_read_memory(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.delete_row_inner(
            tx,
            table,
            row_id,
            true,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    /// Remove only the relational body of a row whose vectors the caller is
    /// about to move or re-place itself, as row replacement does.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn delete_row_keeping_vectors(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.delete_row_inner(
            tx,
            table,
            row_id,
            false,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn delete_row_inner(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        release_vectors: bool,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.ensure_no_mixed_local_schema_dml(tx)?;
        let trigger_callback_bound = self.trigger_callback_tx_bound_matches(tx);
        let _operation = if trigger_callback_bound || self.sql_write_control_bypass_active(tx) {
            None
        } else {
            Some(self.open_operation_after_write_control_wait(tx, "delete_row")?)
        };
        let checkpoint = self.write_set_checkpoint(tx)?;
        let result = (|| {
            // ACL and tombstone probes borrow source rows only for this deletion.
            // Their copies drop before this scope releases its original credits.
            let _row_outputs = self.writer_output_scope();
            self.ensure_trigger_table_ready(table, "delete_row")?;
            self.assert_row_id_write_allowed(
                Some(tx),
                table,
                row_id,
                self.snapshot_for_read(),
                read_memory.clone(),
            )?;
            if let Some(row) =
                self.committed_row_at(table, row_id, self.snapshot_for_read(), read_memory.clone())?
                && let Some(meta) = self.table_meta(table)
                && let Some(natural_key) = natural_key_from_row_values(&meta, &row.values)
            {
                // A local delete replaces any older pulled tombstone for this key.
                // Sync apply records its own marker again only after its delete
                // commits, so a failed inbound delete never becomes suppressible.
                self.clear_sync_tombstone(table, &natural_key);
            }
            if release_vectors {
                // Before the relational body goes, so a failure leaves the row and
                // its vectors together rather than a row with no embeddings.
                for index in self.vector_indexes_of_table(table) {
                    let staged = self.tx_mgr.with_read(tx, |ws| {
                        ws.vector_inserts
                            .iter()
                            .any(|entry| entry.index == index && entry.row_id == row_id)
                            || ws
                                .vector_moves
                                .iter()
                                .any(|(have, _, to, _)| *have == index && *to == row_id)
                    })?;
                    if staged
                        || self.vector_store.has_live_entry_for_row(
                            &index,
                            row_id,
                            self.snapshot_for_read(),
                            vector_copy_owner,
                        )?
                    {
                        self.delete_vector_with_read_memory(
                            tx,
                            index,
                            row_id,
                            vector_copy_owner,
                            read_memory.clone(),
                        )?;
                    }
                }
            }
            self.delete_relational_row_releasing_staged(tx, table, row_id, read_memory.clone())
        })();
        if result.is_err() {
            let _ = self.restore_write_set_checkpoint(tx, checkpoint);
        } else {
            checkpoint.settle();
        }
        result
    }

    /// One index reference per vector column the table declares.
    pub(crate) fn vector_indexes_of_table(&self, table: &str) -> Vec<VectorIndexRef> {
        self.table_meta(table)
            .map(|meta| {
                meta.columns
                    .iter()
                    .filter(|column| {
                        matches!(column.column_type, contextdb_core::ColumnType::Vector(_))
                    })
                    .map(|column| VectorIndexRef::new(table, column.name.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn scan(&self, table: &str, snapshot: SnapshotId) -> Result<Vec<VersionedRow>> {
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.scan_with_read_memory(table, snapshot, &mut vector_copy_owner, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn scan_with_read_memory(
        &self,
        table: &str,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        #[cfg(any(test, feature = "test-seams"))]
        observe_read_execution_convergence(ReadExecutionConvergenceEvent::EagerRowSourceTouch);
        let rows = self.committed_rows_at(table, snapshot, read_memory.clone())?;
        let mut rows = self.filter_rows_for_read(table, rows, snapshot, read_memory.clone())?;
        self.supplement_quantized_vectors(
            table,
            snapshot,
            &mut rows,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        Ok(rows)
    }

    /// Clone a selected committed row under the executing read's retained
    /// credit. The shared bounded source kernel performs the actual read.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn constraint_row_at_admitted(
        &self,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,
        columns: &[Vec<String>],
        output: &mut contextdb_core::read_memory::ReadCredit,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let Some(root) = self.writer_read_root()? else {
            return self.committed_row_at_admitted(table, row_id, snapshot, output, read_memory);
        };
        let source = self.statement_relational_source(root, read_memory.clone())?;
        let window = contextdb_relational::source::visible_row(&source, table, row_id, snapshot)?;
        let Some(window) = window else {
            return Ok(None);
        };
        let row = window.value();
        let selected = |column: &String| columns.iter().any(|tuple| tuple.contains(column));
        // Keep the existing general materializer for complex key payloads.
        if row
            .values
            .iter()
            .any(|(column, value)| selected(column) && matches!(value, Value::Json(_)))
        {
            drop(window);
            return self.committed_row_at_admitted(table, row_id, snapshot, output, read_memory);
        }
        let count = row.values.keys().filter(|column| selected(column)).count();
        let bytes = row
            .values
            .iter()
            .filter(|(column, _)| selected(column))
            .fold(
                std::mem::size_of::<VersionedRow>()
                    .saturating_add(held::map::<String, Value>(count)),
                |bytes, (column, value)| {
                    bytes
                        .saturating_add(held::text(column))
                        .saturating_add(held::value(value))
                },
            );
        output.try_grow_for(
            bytes,
            "writer_source",
            "constraint_row_output",
            "Reduce the selected constraint keys or raise the declared read memory limit.",
        )?;
        let mut values = HashMap::with_capacity(count);
        for (column, value) in &row.values {
            if selected(column) {
                values.insert(column.clone(), value.clone());
            }
        }
        Ok(Some(VersionedRow {
            row_id: row.row_id,
            values,
            created_tx: row.created_tx,
            deleted_tx: row.deleted_tx,
            lsn: row.lsn,
            created_at: row.created_at,
        }))
    }
}
