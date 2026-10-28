use super::*;

/// Outcome of the actual maintenance pruning-mutex acquisition attempt.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenancePruningMutexEventForTest {
    Acquired,
    Contended,
}

#[cfg(any(test, feature = "test-seams"))]
type MaintenancePruningMutexObserverForTest =
    Arc<dyn Fn(MaintenancePruningMutexEventForTest) + Send + Sync>;

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static MAINTENANCE_PRUNING_MUTEX_OBSERVER: std::cell::RefCell<Option<MaintenancePruningMutexObserverForTest>> = const { std::cell::RefCell::new(None) };
}

impl Database {
    /// Observe this worker's maintenance pruning-mutex attempt. Restores the
    /// enclosing observer on return or unwind. Contention is reported before
    /// the ordinary blocking acquisition; Acquired means try_lock succeeded.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn observe_maintenance_pruning_mutex_for_test<T>(
        observer: Arc<dyn Fn(MaintenancePruningMutexEventForTest) + Send + Sync>,
        operation: impl FnOnce() -> T,
    ) -> T {
        struct Restore(Option<MaintenancePruningMutexObserverForTest>);
        impl Drop for Restore {
            fn drop(&mut self) {
                MAINTENANCE_PRUNING_MUTEX_OBSERVER.with(|slot| *slot.borrow_mut() = self.0.take());
            }
        }
        let _restore =
            Restore(MAINTENANCE_PRUNING_MUTEX_OBSERVER.with(|slot| slot.replace(Some(observer))));
        operation()
    }
}

impl Database {
    pub(super) fn vector_schema_refs_for_state_propagation_from_table(
        &self,
        table: &str,
    ) -> Vec<VectorIndexRef> {
        let metas = self.relational_store().table_meta.read().clone();
        let mut roots = HashSet::new();
        if let Some(meta) = metas.get(table) {
            for rule in &meta.propagation_rules {
                match rule {
                    PropagationRule::Edge { trigger_state, .. }
                    | PropagationRule::VectorExclusion { trigger_state } => {
                        roots.insert(trigger_state.clone());
                    }
                    PropagationRule::ForeignKey { .. } => {}
                }
            }
        }
        for meta in metas.values() {
            for rule in &meta.propagation_rules {
                if let PropagationRule::ForeignKey {
                    referenced_table,
                    trigger_state,
                    ..
                } = rule
                    && referenced_table == table
                {
                    roots.insert(trigger_state.clone());
                }
            }
        }

        let mut refs = Vec::new();
        let mut queue = roots
            .into_iter()
            .map(|state| (table.to_string(), state))
            .collect::<VecDeque<_>>();
        let mut visited = HashSet::new();
        while let Some((source_table, source_state)) = queue.pop_front() {
            if !visited.insert((source_table.clone(), source_state.clone())) {
                continue;
            }
            let Some(meta) = metas.get(&source_table) else {
                continue;
            };
            for rule in &meta.propagation_rules {
                match rule {
                    PropagationRule::VectorExclusion { trigger_state }
                        if trigger_state == &source_state =>
                    {
                        refs.extend(
                            meta.columns
                                .iter()
                                .filter(|column| {
                                    matches!(column.column_type, ColumnType::Vector(_))
                                })
                                .map(|column| {
                                    VectorIndexRef::new(&source_table, column.name.clone())
                                }),
                        );
                    }
                    PropagationRule::Edge {
                        trigger_state,
                        target_state,
                        ..
                    } if trigger_state == &source_state => {
                        queue.push_back((source_table.clone(), target_state.clone()));
                    }
                    _ => {}
                }
            }

            for (owner_table, owner_meta) in &metas {
                for rule in &owner_meta.propagation_rules {
                    if let PropagationRule::ForeignKey {
                        referenced_table,
                        trigger_state,
                        target_state,
                        ..
                    } = rule
                        && referenced_table == &source_table
                        && trigger_state == &source_state
                    {
                        queue.push_back((owner_table.clone(), target_state.clone()));
                    }
                }
            }
        }
        VectorSchemaGates::sorted_refs(refs)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn vector_store_schema_refs(
        &self,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<Vec<VectorIndexRef>> {
        Ok(self
            .vector_store
            .index_infos(vector_copy_owner)?
            .into_iter()
            .map(|info| info.index)
            .collect())
    }

    /// Return the row changes since `since`. Walks `change_log` for
    /// `RowInsert` / `RowDelete` entries whose LSN exceeds `since`, fetches
    /// the row values out of the live relational store, and emits a
    /// `RowChange` the receiver can replay. row_id order is preserved.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn change_log_rows_since(&self, since: Lsn) -> Result<Vec<RowChange>> {
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.change_log_rows_since_with_read_memory(since, &mut vector_copy_owner, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn change_log_rows_since_with_read_memory(
        &self,
        since: Lsn,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<RowChange>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        let changes =
            self.changes_since_with_read_memory(since, vector_copy_owner, read_memory.clone());
        if !self.access_is_admin() {
            return Ok(changes?.rows);
        }
        let entries = self.change_log_since_with_read_memory(since, read_memory.clone())?;
        let tables = self.relational_store.tables.read();
        let mut out = Vec::new();
        for e in entries {
            match e {
                ChangeLogEntry::RowInsert { table, row_id, lsn } => {
                    let Some(rows) = tables.get(&table) else {
                        continue;
                    };
                    let Some(row) = rows
                        .iter()
                        .rev()
                        .find(|r| r.row_id == row_id && r.lsn == lsn)
                        .or_else(|| rows.iter().rev().find(|r| r.row_id == row_id))
                    else {
                        continue;
                    };
                    let Some(natural_key) = self
                        .table_meta(&table)
                        .and_then(|meta| natural_key_from_row_values(&meta, &row.values))
                    else {
                        continue;
                    };
                    let values = self.outbound_row_values(
                        &table,
                        row,
                        vector_copy_owner,
                        read_memory.clone(),
                    )?;
                    out.push(RowChange {
                        table,
                        natural_key,
                        values,
                        deleted: row.deleted_tx.is_some(),
                        lsn,
                        created_at: row.created_at,
                    });
                }
                ChangeLogEntry::RowDelete {
                    table,
                    row_id: _,
                    natural_key,
                    lsn,
                } => {
                    out.push(RowChange {
                        table,
                        natural_key,
                        values: HashMap::new(),
                        deleted: true,
                        lsn,
                        created_at: None,
                    });
                }
                _ => {}
            }
        }
        Ok(out)
    }

    /// Count of base rows the executor touched during the most recent query.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __rows_examined(&self) -> u64 {
        let _operation = self.assert_open_operation();
        self.rows_examined.load(Ordering::SeqCst)
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __reset_rows_examined(&self) {
        let _operation = self.assert_open_operation();
        self.rows_examined.store(0, Ordering::SeqCst);
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __bump_rows_examined(&self, delta: u64) {
        let _operation = self.assert_open_operation();
        self.bump_active_query_rows_examined_scope(delta);
        self.rows_examined.fetch_add(delta, Ordering::SeqCst);
    }

    #[cfg(not(any(test, feature = "test-seams")))]
    pub(crate) fn __bump_rows_examined(&self, delta: u64) {
        let _operation = self.assert_open_operation();
        self.bump_active_query_rows_examined_scope(delta);
        self.rows_examined.fetch_add(delta, Ordering::SeqCst);
    }

    pub(super) fn bump_active_query_rows_examined_scope(&self, delta: u64) {
        let database_key = self.id;
        QUERY_ROWS_EXAMINED_SCOPES.with(|scopes| {
            if let Some(scope) = scopes
                .borrow_mut()
                .active
                .iter_mut()
                .rev()
                .find(|scope| scope.database_key == database_key)
            {
                scope.rows_examined = scope.rows_examined.saturating_add(delta);
            }
        });
    }

    /// Count of batch-level `indexes.write()` lock acquisitions since startup.
    /// `apply_changes` bumps this once per batch; per-row commits do not.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __index_write_lock_count(&self) -> u64 {
        let _operation = self.assert_open_operation();
        self.relational_store.index_write_lock_count()
    }

    /// Index slots the commit-time insert/delete maintenance loop iterated
    /// over for the most recent committed write set(s), summed across rows.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __index_maintenance_visits(&self) -> u64 {
        let _operation = self.assert_open_operation();
        self.relational_store.index_maintenance_visits()
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __reset_index_maintenance_visits(&self) {
        let _operation = self.assert_open_operation();
        self.relational_store.reset_index_maintenance_visits();
    }

    /// Rows touched by relational full-scan primitives since the last reset.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __relational_scan_rows_touched(&self) -> u64 {
        let _operation = self.assert_open_operation();
        self.relational_store.scan_rows_touched()
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __reset_relational_scan_rows_touched(&self) {
        let _operation = self.assert_open_operation();
        self.relational_store.reset_scan_rows_touched();
    }

    /// Index entries touched by ordered index walks since the last reset. It
    /// witnesses the index side of a read the way the scanned-row counter
    /// witnesses the table side.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __relational_index_entries_touched(&self) -> u64 {
        let _operation = self.assert_open_operation();
        self.relational_store.index_entries_touched()
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __reset_relational_index_entries_touched(&self) {
        let _operation = self.assert_open_operation();
        self.relational_store.reset_index_entries_touched();
    }

    /// Index slots iterated during file-load/reopen replay. Read after
    /// `Database::open(path)` returns.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __open_index_maintenance_visits(&self) -> u64 {
        let _operation = self.assert_open_operation();
        self.relational_store.open_index_maintenance_visits()
    }

    /// Total entries across every registered index's BTreeMap.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __introspect_indexes_total_entries(&self) -> u64 {
        let _operation = self.assert_open_operation();
        self.relational_store.introspect_indexes_total_entries()
    }

    /// Probe the constraint-check path for a specific table/column/value.
    /// Returns a QueryResult whose trace reflects whether the probe went
    /// through an index (IndexScan) or a full scan. Accepts either a
    /// single-column index or a composite leading-column match.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __probe_constraint_check(
        &self,
        table: &str,
        column: &str,
        value: Value,
    ) -> Result<QueryResult> {
        let _operation = self.open_operation()?;
        let covered = self.index_covers_column(table, column)
            || self
                .relational_store
                .indexes
                .read()
                .get(table)
                .is_some_and(|table_indexes| {
                    table_indexes
                        .values()
                        .any(|idx| idx.columns.first().is_some_and(|(c, _)| c == column))
                });
        let trace = if covered {
            QueryTrace {
                cache_resident_fraction: None,
                groups: Default::default(),
                physical_plan: "IndexScan",
                index_used: None,
                predicates_pushed: Default::default(),
                indexes_considered: Default::default(),
                sort_elided: false,
                query_vector_source: None,
                vector_search: None,
                rows_examined: 0,
                maintained_count: None,
            }
        } else {
            QueryTrace::scan()
        };
        let _ = value;
        let read_memory = self.declared_reader_memory()?;
        QueryResult::publish_with(read_memory, || {
            Ok(OwnedQueryResult::new(QueryResultData {
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
                trace,
                cascade: None,
            }))
        })
    }

    /// Run one pruning cycle and return the pruned row count or typed refusal.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn run_pruning_cycle(&self) -> Result<u64> {
        crate::memory_accounting::observe_inherited_process_ceiling();
        let read_memory = self.finalization.accountant.read_budget()?;
        self.run_pruning_cycle_with_read_memory(read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn run_pruning_cycle_with_read_memory(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<u64> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.run_pruning_cycle_checked_with_read_memory(read_memory.clone())
            .map(|report| report.pruned_rows)
    }

    /// Run one declared-retention pruning cycle and return its report or error.
    /// Reader-visible versions are deferred. Each selected row, change-log,
    /// vector, adjacency, and deferred-edge retry set commits together. Storage
    /// errors are returned to the caller. This runs the retention pass only;
    /// use [`Self::run_maintenance_cycle`] to also advance
    /// current-only history cleanup and maintained vector indexes.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn run_pruning_cycle_checked(&self) -> Result<PruningReport> {
        crate::memory_accounting::observe_inherited_process_ceiling();
        let read_memory = self.finalization.accountant.read_budget()?;
        self.run_pruning_cycle_checked_with_read_memory(read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn run_pruning_cycle_checked_with_read_memory(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<PruningReport> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.assert_open_operation();
        let _guard = MaintenanceReceiptGuard::lock(&self.pruning_guard);
        #[cfg(any(test, feature = "test-seams"))]
        let _held = crate::persistence::PruningGuardHeldForTest::enter();
        self.maintenance_context().run_pruning(read_memory.clone())
    }

    /// Run ONE maintenance cycle synchronously: generic retention, the
    /// engine's own durable trigger-audit retention, currency version
    /// compaction, and maintained vector-index progress — the exact work the
    /// engine-owned loop dispatches on a tick, through the same code path.
    /// Vector construction and repair advance every needy partition in the
    /// finite backlog sampled by this call, with never-built partitions first.
    /// Concurrent new work, a stale candidate, or a typed resource refusal can
    /// leave work for a later call. Existing snapshot-compatible routes keep
    /// serving while a replacement is prepared, and active readers defer
    /// reclamation.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn run_maintenance_cycle(&self) -> Result<MaintenanceReport> {
        #[cfg(any(test, feature = "test-seams"))]
        let _maintenance_phase = crate::read_image_memory::MaintenancePhase::enter("maintenance");
        crate::memory_accounting::observe_inherited_process_ceiling();
        let read_memory = self.finalization.accountant.read_budget()?;
        self.run_maintenance_cycle_with_read_memory(read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn run_maintenance_cycle_with_read_memory(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<MaintenanceReport> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut operation = self.assert_open_operation();
        operation.release_schema_publication_for_online_maintenance();
        #[cfg(any(test, feature = "test-seams"))]
        let observer = MAINTENANCE_PRUNING_MUTEX_OBSERVER.with(|slot| slot.borrow().clone());
        #[cfg(any(test, feature = "test-seams"))]
        let _guard = if let Some(observer) = observer {
            let identity = Arc::as_ptr(&self.pruning_guard) as usize;
            let guard = if let Some(guard) = self.pruning_guard.try_lock() {
                observer(MaintenancePruningMutexEventForTest::Acquired);
                guard
            } else {
                note_writer_receipt_contention(identity);
                observer(MaintenancePruningMutexEventForTest::Contended);
                self.pruning_guard.lock()
            };
            MaintenanceReceiptGuard::from_locked(&self.pruning_guard, guard)
        } else {
            MaintenanceReceiptGuard::lock(&self.pruning_guard)
        };
        #[cfg(not(any(test, feature = "test-seams")))]
        let _guard = MaintenanceReceiptGuard::lock(&self.pruning_guard);
        #[cfg(any(test, feature = "test-seams"))]
        let _held = crate::persistence::PruningGuardHeldForTest::enter();
        let result = self
            .maintenance_context()
            .run_cycle(CurrencyGate::Always, read_memory);
        // The operation root can retain decoded source windows until exit.
        // Retire those physical holders before the closing observation, while
        // this caller still holds maintenance exclusion.
        drop(operation);
        #[cfg(any(test, feature = "test-seams"))]
        let emission = result
            .is_ok()
            .then(|| self.prepare_writer_memory_ownership_receipt_for_test("maintenance"))
            .flatten();
        #[cfg(any(test, feature = "test-seams"))]
        drop(_held);
        drop(_guard);
        #[cfg(any(test, feature = "test-seams"))]
        if let Some(emission) = emission {
            emission.emit();
        }
        result
    }

    /// Production-dead: how many times `retire_snapshot_free_vector_partitions`
    /// actually began its guarded removal pass, as opposed to skipping it
    /// because a cheap pre-check found nothing retirable.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn vector_partition_retirement_pass_count_for_test(&self) -> usize {
        self.snapshot_free_retirement_pass_count
            .load(Ordering::SeqCst)
    }

    /// Production-dead observation that a full maintenance wake reached its
    /// closing stamp even when one partition reported a local refusal.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn maintenance_cycle_stamp_is_set_for_test(&self) -> bool {
        self.last_maintenance_cycle_at.lock().is_some()
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn clear_maintenance_cycle_stamp_for_test(&self) {
        *self.last_maintenance_cycle_at.lock() = None;
    }

    /// Production-dead: application pages the storage fragmentation
    /// statistics walk has visited on the calling thread, cumulatively, and
    /// how many of those it visited while holding the pruning guard. A
    /// caller-driven maintenance cycle runs on the calling thread, so the
    /// difference across one cycle is that cycle's statistics work.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn __storage_statistics_pages_walked_for_test(&self) -> (u64, u64) {
        crate::persistence::storage_statistics_pages_walked_for_test()
    }

    /// Production-dead: storage statistics snapshots captured on the calling
    /// thread, cumulatively.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn __storage_statistics_captures_for_test() -> u64 {
        crate::persistence::storage_statistics_captures_for_test()
    }

    /// Production-dead: allocator free-total rebuilds on the calling thread,
    /// cumulatively. A rebuild reads every allocator bitmap word.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn __allocator_free_total_recounts_for_test() -> u64 {
        redb::Database::free_total_recounts_for_test()
    }

    /// Production-dead: regions visited on the calling thread to compute the
    /// allocator's free-total geometry, cumulatively.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn __allocator_geometry_region_visits_for_test() -> u64 {
        redb::Database::free_total_geometry_region_visits_for_test()
    }

    /// Production-dead: drop trust in the allocator's free-page totals, as an
    /// interrupted allocator-state save leaves them until a recount.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __distrust_allocator_free_totals_for_test(&self) {
        let persistence = self.persistence.as_ref().expect("file fixture");
        persistence
            .with_db(|db| {
                db.distrust_allocator_free_totals_for_test();
                Ok(())
            })
            .expect("distrust allocator totals");
    }

    /// Production-dead: automatic compaction checks that read the free-page
    /// measurement on the calling thread, cumulatively.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn __automatic_compaction_checks_for_test() -> u64 {
        AUTOMATIC_COMPACTION_CHECKS.with(std::cell::Cell::get)
    }

    /// Production-dead: sweeps the last explicit compaction on the calling
    /// thread ran.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn __explicit_compaction_sweeps_last_for_test() -> u64 {
        EXPLICIT_COMPACTION_SWEEPS.with(std::cell::Cell::get)
    }

    /// Production-dead: run the calling thread's compaction clock `by`
    /// further ahead of the monotonic clock.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn __advance_compaction_clock_for_test(by: Duration) {
        COMPACTION_CLOCK_ADVANCE.with(|advance| advance.set(advance.get() + by));
    }

    /// Production-dead: the next explicit compaction on the calling thread
    /// fails its exact fragmentation measurement.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn __fail_next_exact_fragmentation_measurement_for_test() {
        FAIL_EXACT_FRAGMENTATION_MEASUREMENT.with(|fail| fail.set(true));
    }

    /// Production-dead: the calling thread publishes `batches` more storage
    /// compaction batches, then the next one is refused before it starts.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn __refuse_storage_compaction_batch_after_for_test(batches: u32) {
        STORAGE_COMPACTION_BATCHES_BEFORE_REFUSAL.with(|slot| slot.set(Some(batches)));
    }

    /// Production-dead: pause the next `SET STORAGE_COMPACTION` after it is
    /// durable and before it is live.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn __pause_after_storage_compaction_persist_for_test(&self) -> ApplyPhasePauseGuard {
        let _operation = self.assert_open_operation();
        let inner = self.storage_compaction_policy.lock().update_pause.clone();
        ApplyPhasePauseGuard::arm_for_test(inner)
    }

    /// Production-dead: `SET STORAGE_COMPACTION` calls that have entered.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn __storage_compaction_update_entries_for_test(&self) -> u64 {
        self.storage_compaction_policy.lock().update_probe.entries()
    }

    /// Production-dead: wait until `target` `SET STORAGE_COMPACTION` calls
    /// have entered.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn __wait_for_storage_compaction_update_entry_for_test(&self, target: u64) {
        let probe = self.storage_compaction_policy.lock().update_probe.clone();
        probe.wait_for(target);
    }

    /// Production-dead: whether the shared setting-update lock is held.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn __storage_compaction_update_lock_is_held_for_test(&self) -> bool {
        self.limit_update_lock.try_lock().is_none()
    }

    /// Production-dead: the allocator's maintained `(free pages, region
    /// pages, trailing free pages)`; `None` while untrusted.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __allocator_free_totals_for_test(&self) -> Result<Option<(u64, u64, u64)>> {
        let persistence = self.persistence.as_ref().expect("file fixture");
        persistence.with_db(|db| Ok(db.allocator_free_totals()))
    }

    /// Production-dead: per region `(length, free, trailing free, highest
    /// free order)`, read from every allocator bitmap. Diagnostics only.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __allocator_regions_for_test(&self) -> Result<Vec<(u32, u32, u32, Option<u8>)>> {
        let persistence = self.persistence.as_ref().expect("file fixture");
        persistence.with_db(|db| Ok(db.region_free_pages_for_test()))
    }

    /// Production-dead: whether the saved allocator-state table is still the
    /// lazy allocator source, and the last page it occupies.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __allocator_state_placement_for_test(&self) -> Result<(bool, Option<(u32, u32)>)> {
        let persistence = self.persistence.as_ref().expect("file fixture");
        persistence.with_db(|db| {
            db.allocator_state_placement_for_test()
                .map_err(crate::persistence::RedbPersistence::storage_error)
        })
    }

    /// Production-dead: the maintained allocator free-page ratio the
    /// maintenance decisions read, and the same ratio from an independent
    /// recount of every allocator bitmap (which does not count as a rebuild).
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __allocator_free_page_ratios_for_test(&self) -> Result<(Option<f64>, f64)> {
        let persistence = self.persistence.as_ref().expect("file fixture");
        let maintained = persistence.allocator_free_page_ratio()?;
        let (free, pages, trailing) = persistence.with_db(|db| {
            db.allocator_free_totals_recount_for_test()
                .map_err(crate::persistence::RedbPersistence::storage_error)
        })?;
        let recounted = crate::persistence::reclaimable_free_ratio(free, pages, trailing);
        Ok((maintained, recounted))
    }

    /// Production-dead: the exact on-demand fragmentation ratio, computed by
    /// the full statistics walk a diagnostic caller asks for.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __storage_fragmentation_ratio_for_test(&self) -> Result<Option<f64>> {
        self.persistence
            .as_ref()
            .map(|persistence| persistence.fragmentation_ratio())
            .transpose()
    }

    /// Explicit operator compaction drains finite relocation transactions,
    /// allowing recording and reads between batches. It bypasses the automatic
    /// fragmentation/interval gates and resets that interval on completion.
    /// It repeats whole sweeps until interior free pages are below the
    /// declared stop ratio (5% by default) or a further sweep stops shrinking
    /// the file, at most the declared number of sweeps (8 by default), so it
    /// ends with the file close to its data rather than with a first sweep's
    /// copies pinning the end.
    /// The storage handle is recycled only when no storage reader owns it;
    /// otherwise existing snapshots retain their handle and source pages.
    /// A reopen failure preserves the durable file and returns the existing
    /// `StoreHandleRecycleFailed` error. Memory databases have no file to compact.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn compact_now(&self) -> Result<CompactionReport> {
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        let mut operation = self.assert_open_operation();
        // Relocation captures each finite storage batch itself. The public
        // operation's opening read root would retain the pages it reclaims.
        // Keep lifecycle/schema guards and roots owned by actual readers.
        drop(operation._writer_read.take());
        let policy = self.storage_compaction_policy();
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(CompactionReport {
                policy,
                ..CompactionReport::default()
            });
        };
        // The same reclaimable share the automatic decision reads, and the
        // exact walk's in-page slack beside it under its own name. A failed
        // measurement is reported unknown; the sweep does not depend on it.
        let (fragmentation_before, page_slack_ratio) =
            if exact_fragmentation_measurement_fails_for_test() {
                (None, None)
            } else {
                (
                    persistence.allocator_free_page_ratio().ok().flatten(),
                    persistence.fragmentation_ratio().ok(),
                )
            };
        let bytes_before = self.disk_file_size();
        if !self.vector_store.has_durable_memberships() {
            let memberships = persistence.load_vector_partition_memberships()?;
            let raw_loaders = durable_raw_vector_partitions(
                &self.vector_store,
                DurableRawVectorSource::Writable(persistence.clone()),
                memberships,
                &self.finalization.accountant,
                &mut vector_copy_owner,
            )?;
            self.vector_store
                .attach_durable_raw_partition_loaders(raw_loaders)?;
        }
        let start = Instant::now();
        // One sweep copies each page before its old address is released, so
        // when the front of the file has no room yet its copies land behind
        // the old end and the space they vacate opens up inside the file
        // only after the sweep. A further sweep moves that tail into the
        // released interior. Sweep again while interior free pages remain
        // and the previous sweep still shrank the file (the first sweep
        // always gets a successor), up to a fixed number of sweeps.
        let mut recycled = None;
        let mut sweep_bytes_before = bytes_before;
        #[cfg(any(test, feature = "test-seams"))]
        EXPLICIT_COMPACTION_SWEEPS.with(|sweeps| sweeps.set(0));
        for sweep in 0..policy.max_sweeps() {
            #[cfg(any(test, feature = "test-seams"))]
            EXPLICIT_COMPACTION_SWEEPS.with(|sweeps| sweeps.set(sweeps.get() + 1));
            recycled = loop {
                let (progress, recycled) = publish_storage_compaction_batch(
                    &self.tx_mgr,
                    persistence,
                    &self.finalization.accountant,
                    &self.publication_interrupted,
                    fragmentation_before,
                )?;
                if progress.complete {
                    break recycled;
                }
                std::thread::yield_now();
            };
            let sweep_bytes_after = self.disk_file_size();
            let shrank = matches!(
                (sweep_bytes_before, sweep_bytes_after),
                (Some(before), Some(after)) if after < before
            );
            // Untrusted totals cannot say the file is tight: keep sweeping
            // while sweeps still shrink it.
            let tight = persistence
                .allocator_free_page_ratio()?
                .is_some_and(|interior_free| interior_free < policy.stop_ratio());
            if tight || (sweep > 0 && !shrank) {
                break;
            }
            sweep_bytes_before = sweep_bytes_after;
        }
        self.vector_store.evict_reloadable_raw_partitions();
        let duration_micros = start.elapsed().as_micros() as u64;
        *self.last_auto_compact_at.lock() = Some(compaction_clock_now());
        let bytes_after = self.disk_file_size();
        Ok(CompactionReport {
            ran: true,
            duration_micros,
            bytes_before,
            bytes_after,
            file_shrank: matches!((bytes_before, bytes_after), (Some(b), Some(a)) if a < b),
            fragmentation_before,
            page_slack_ratio,
            policy,
            handle_recycled: recycled.is_some(),
            handle_recycle_micros: recycled.unwrap_or(0),
        })
    }

    /// Test-only override of [`AUTO_COMPACT_MIN_INTERVAL`], the same
    /// pattern as `__set_currency_maintenance_interval`: production never
    /// calls this, so the shipped default always governs the real automatic
    /// path. Lets a test prove BOTH halves deterministically, with no
    /// sleeps — a huge interval proves suppression (a second qualifying
    /// cycle right after the first must not recompact), and a zero interval
    /// proves the gate fires again the moment it is next due.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __set_auto_compact_min_interval_for_test(&self, interval: Duration) {
        self.storage_compaction_policy.lock().min_interval_override = Some(interval);
    }

    /// Production-dead: the most one growth of the writer's file may add at
    /// `usable` bytes for a request of `required` bytes, under the growth step
    /// its handle carries (`None`: upstream's growth).
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __growth_step_bytes_for_test(&self, usable: u64, required: u64) -> Result<Option<u64>> {
        match self.persistence.as_ref() {
            Some(persistence) => persistence.growth_step_for_test(usable, required),
            None => Ok(None),
        }
    }

    /// Whether storage maintenance has more batches, followed by actual
    /// page visits during candidate discovery and relocated pages in its
    /// most recent batch.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __storage_compaction_progress_for_test(&self) -> (bool, usize, usize) {
        self.persistence
            .as_ref()
            .map_or((false, 0, 0), |p| p.storage_compaction_progress_for_test())
    }

    // Production-dead extension of the existing compaction observation for
    // reader-admission schedules: inspect the real cursor and interval.
    #[cfg(test)]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from owner_cursor_admission_tests.rs"
        )
    )]
    pub(crate) fn maintenance_publication_state_for_test(
        &self,
    ) -> (&RedbPersistence, Option<std::time::Instant>, bool) {
        (
            self.persistence.as_ref().unwrap(),
            *self.last_auto_compact_at.lock(),
            self.publication_interrupted.load(Ordering::SeqCst),
        )
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __reset_vector_membership_records_decoded_for_test(&self) {
        RedbPersistence::reset_vector_membership_records_decoded_for_test();
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __vector_membership_records_decoded_for_test(&self) -> u64 {
        RedbPersistence::vector_membership_records_decoded_for_test()
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __reset_relational_index_rebuild_rows_decoded_for_test(&self) {
        RedbPersistence::reset_relational_index_rebuild_rows_decoded_for_test();
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __relational_index_rebuild_rows_decoded_for_test(&self) -> u64 {
        RedbPersistence::relational_index_rebuild_rows_decoded_for_test()
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __rebuild_relational_indexes_from_durable_rows_for_test(
        &self,
        table: &str,
    ) -> Result<()> {
        let _operation = self.assert_open_operation();
        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::Other((format!("unknown table '{table}'")).into()))?;
        self.persistence
            .as_ref()
            .ok_or_else(|| Error::Other(("database has no file persistence".to_owned()).into()))?
            .flush_table_meta(table, &meta)
    }

    /// Exact physical allocation versions retained for authenticated external
    /// file snapshots. This reads redb's deferred-free authority directly.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __external_snapshot_retention_for_test(
        &self,
    ) -> Result<
        crate::persistence::read_persistence_test_scaffold::ExternalSnapshotRetentionObservation,
    > {
        self.persistence
            .as_ref()
            .ok_or_else(|| Error::Other(("database has no file persistence".to_owned()).into()))?
            .external_snapshot_retention_for_test()
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __external_snapshot_page_allocated_for_test(&self, page: [u8; 8]) -> Result<bool> {
        self.persistence
            .as_ref()
            .ok_or_else(|| Error::Other(("database has no file persistence".to_owned()).into()))?
            .external_snapshot_page_allocated_for_test(page)
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __deferred_free_backlog_for_test(&self) -> Result<[(usize, usize); 2]> {
        self.persistence
            .as_ref()
            .ok_or_else(|| Error::Other(("database has no file persistence".to_owned()).into()))?
            .deferred_free_backlog_for_test()
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __fail_next_close_drain_for_test(&self) -> Result<()> {
        self.persistence
            .as_ref()
            .ok_or_else(|| Error::Other(("database has no file persistence".to_owned()).into()))?
            .fail_next_close_drain_for_test()
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __fail_next_close_handoff_for_test(&self) -> Result<()> {
        self.persistence
            .as_ref()
            .ok_or_else(|| Error::Other(("database has no file persistence".to_owned()).into()))?
            .fail_next_handoff_for_test();
        Ok(())
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __commit_storage_transaction_for_test(&self) -> Result<()> {
        self.persistence
            .as_ref()
            .ok_or_else(|| Error::Other(("database has no file persistence".to_owned()).into()))?
            .commit_storage_transaction_for_test()
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __projected_disk_growth_bytes_for_test(&self) -> Result<u64> {
        self.persistence
            .as_ref()
            .ok_or_else(|| Error::Other(("database has no file persistence".to_owned()).into()))?
            .projected_disk_growth_bytes_for_test()
    }

    /// What the engine-owned maintenance loop is doing for this database.
    pub fn maintenance_status(&self) -> MaintenanceStatus {
        let _operation = self.assert_open_operation();
        let active_maintenance_loops = usize::from(self.pruning_runtime.lock().handle.is_some());
        MaintenanceStatus {
            running: active_maintenance_loops > 0,
            retention_enabled: self.has_retained_tables(),
            currency_compaction_enabled: self.has_version_cleanup_tables(),
            active_maintenance_loops,
            policy: self.maintenance_policy(),
        }
    }

    /// Whether this database's maintenance is engine-owned or handed to the
    /// caller. See [`MaintenancePolicy`].
    pub fn maintenance_policy(&self) -> MaintenancePolicy {
        if self.maintenance_caller_driven.load(Ordering::SeqCst) {
            MaintenancePolicy::CallerDriven
        } else {
            MaintenancePolicy::EngineOwned
        }
    }

    /// Declare who owns this database's maintenance schedule. Switching TO
    /// `CallerDriven` stops any thread currently running for this database;
    /// switching TO `EngineOwned` starts one if anything is declared. Engine
    /// open-config only — see [`MaintenancePolicy`]'s own doc comment.
    pub fn set_maintenance_policy(&self, policy: MaintenancePolicy) {
        let _operation = self.assert_open_operation();
        self.maintenance_caller_driven
            .store(policy == MaintenancePolicy::CallerDriven, Ordering::SeqCst);
        self.reconcile_maintenance_thread();
    }

    /// The database's persisted maintenance polling cadence. Time wakes the
    /// worker; declarations and per-column need thresholds decide whether
    /// vector consolidation actually runs.
    /// The serving page cache size this store declares, or `None` where the
    /// consumer has not declared one and the engine's default share applies.
    pub fn vector_serving_cache_bytes(&self) -> Option<usize> {
        declared_serving_cache_bytes(self.vector_serving_cache_bytes.load(Ordering::SeqCst))
    }

    /// Declare, persist and apply the serving page cache size. `None` returns
    /// the store to the engine's default share of the declared memory limit.
    /// It takes effect for partitions loaded after it, which is what a reader
    /// opening later sees; a generation already resident keeps the cache it
    /// was admitted for until it is reloaded.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn set_vector_serving_cache_bytes(&self, bytes: Option<usize>) -> Result<()> {
        let _operation = self.open_operation()?;
        if let Some(bytes) = bytes
            && bytes == 0
        {
            return Err(Error::Other(
                ("vector serving cache size must be at least one byte".to_string()).into(),
            ));
        }
        let stored = bytes.map_or(0, |bytes| u64::try_from(bytes).unwrap_or(u64::MAX));
        if let Some(persistence) = &self.persistence {
            match bytes {
                Some(_) => self.publish_direct_metadata(|| {
                    persistence.flush_config_value(VECTOR_SERVING_CACHE_CONFIG_KEY, &stored)
                })?,
                None => self.publish_direct_metadata(|| {
                    persistence.remove_config_value(VECTOR_SERVING_CACHE_CONFIG_KEY)
                })?,
            }
        }
        self.vector_serving_cache_bytes
            .store(stored, Ordering::SeqCst);
        Ok(())
    }

    /// The storage compaction policy this store declares; undeclared fields
    /// are `None` and use today's defaults.
    pub fn storage_compaction_policy(&self) -> StorageCompactionPolicy {
        self.storage_compaction_policy.lock().declared
    }

    /// Declare, persist and apply storage compaction settings
    /// (`SET STORAGE_COMPACTION`). Clauses a statement omits keep their
    /// value; `Default` returns every setting to its default and removes the
    /// persisted declaration, so defaults stay silent.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn set_storage_compaction(
        &self,
        setting: &contextdb_parser::StorageCompactionSetting,
    ) -> Result<()> {
        #[cfg(any(test, feature = "test-seams"))]
        let (update_pause, update_probe) = {
            let state = self.storage_compaction_policy.lock();
            (state.update_pause.clone(), state.update_probe.clone())
        };
        #[cfg(any(test, feature = "test-seams"))]
        update_probe.note_entered();
        let _operation = self.open_operation()?;
        // One declaration at a time from its read through its live
        // publication, so two declarations never drop each other's clauses
        // and the live policy is always the durable one.
        let _update = self.limit_update_lock.lock();
        let mut next = self.storage_compaction_policy();
        match setting {
            contextdb_parser::StorageCompactionSetting::Default => {
                next = StorageCompactionPolicy::default();
            }
            contextdb_parser::StorageCompactionSetting::Declare(clauses) => next.declare(*clauses),
        }
        next.validate()?;
        if let Some(persistence) = &self.persistence {
            if next == StorageCompactionPolicy::default() {
                self.publish_direct_metadata(|| {
                    persistence.remove_config_value(STORAGE_COMPACTION_CONFIG_KEY)
                })?;
            } else {
                self.publish_direct_metadata(|| {
                    persistence.flush_config_value(STORAGE_COMPACTION_CONFIG_KEY, &next)
                })?;
            }
        }
        #[cfg(any(test, feature = "test-seams"))]
        update_pause.maybe_pause();
        self.storage_compaction_policy.lock().declared = next;
        if let Some(persistence) = &self.persistence {
            persistence.set_growth_step_ratio(next.stop_ratio())?;
        }
        Ok(())
    }

    pub fn maintenance_poll_interval(&self) -> Duration {
        Duration::from_millis(
            self.maintenance_poll_interval_ms
                .load(Ordering::SeqCst)
                .max(1),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn apply_maintenance_poll_interval(&self, interval: Duration) -> Result<()> {
        let milliseconds = u64::try_from(interval.as_millis()).map_err(|_| {
            Error::Other(
                ("maintenance poll interval exceeds the supported range".to_string()).into(),
            )
        })?;
        if milliseconds == 0 {
            return Err(Error::Other(
                ("maintenance poll interval must be at least one millisecond".to_string()).into(),
            ));
        }
        if let Some(persistence) = &self.persistence {
            self.publish_direct_metadata(|| {
                persistence.flush_config_value(MAINTENANCE_POLL_INTERVAL_CONFIG_KEY, &milliseconds)
            })?;
        }
        #[cfg(any(test, feature = "test-seams"))]
        self.maintenance_poll_interval_update_pause.maybe_pause();
        self.maintenance_poll_interval_ms
            .store(milliseconds, Ordering::SeqCst);
        Ok(())
    }

    /// Persist and apply a positive maintenance polling cadence. An existing
    /// engine-owned worker samples the new value for its next wait without
    /// joining an active maintenance cycle.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn set_maintenance_poll_interval(&self, interval: Duration) -> Result<()> {
        #[cfg(any(test, feature = "test-seams"))]
        self.maintenance_poll_interval_update_probe.note_entered();
        let _operation = self.open_operation()?;
        let _update = self.limit_update_lock.lock();
        self.apply_maintenance_poll_interval(interval)?;
        let rescheduled = if self.maintenance_policy() == MaintenancePolicy::EngineOwned {
            let runtime = self.pruning_runtime.lock();
            if runtime.handle.is_some() {
                runtime.interval_ms.store(
                    self.maintenance_poll_interval_ms.load(Ordering::SeqCst),
                    Ordering::SeqCst,
                );
                runtime.worker_clock.reschedule();
                true
            } else {
                false
            }
        } else {
            false
        };
        if !rescheduled {
            self.reconcile_maintenance_thread();
        }
        Ok(())
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __pause_after_maintenance_poll_interval_persist_for_test(&self) -> ApplyPhasePauseGuard {
        let _operation = self.assert_open_operation();
        let inner = self.maintenance_poll_interval_update_pause.clone();
        let generation = inner.arm();
        ApplyPhasePauseGuard { inner, generation }
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __maintenance_poll_interval_update_entries_for_test(&self) -> u64 {
        let _operation = self.assert_open_operation();
        self.maintenance_poll_interval_update_probe.entries()
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __wait_for_maintenance_poll_interval_update_entry_for_test(&self, target: u64) {
        let _operation = self.assert_open_operation();
        self.maintenance_poll_interval_update_probe.wait_for(target);
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __maintenance_poll_interval_update_lock_is_held_for_test(&self) -> bool {
        let _operation = self.assert_open_operation();
        self.limit_update_lock.try_lock().is_none()
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __maintenance_worker_poll_interval_for_test(&self) -> Option<Duration> {
        let _operation = self.assert_open_operation();
        let runtime = self.pruning_runtime.lock();
        runtime.handle.as_ref()?;
        Some(Duration::from_millis(
            runtime.interval_ms.load(Ordering::SeqCst),
        ))
    }

    /// The retention window applied to the engine's OWN durable trigger-audit
    /// history: the operator's declared window if one was set via
    /// [`Database::set_trigger_audit_retention`] on THIS handle, else the
    /// shipped default. A consumer that configures nothing still gets a
    /// bounded table.
    ///
    /// This is engine OPEN-CONFIG, not a stored declaration: it lives only on
    /// this in-process handle, the same way a disk limit or memory ceiling
    /// does, and is never written to the redb config store, never a DDL
    /// surface, and never transported. A fresh [`Database`] opened on the
    /// same path with no call to the setter reports the shipped default --
    /// see `trigger_audit_retention_config_tests.rs` for the pinning test.
    ///
    /// The maintenance pass itself (`Database::maintenance_context`'s
    /// `trigger_audit_retention_secs` field, the same shared `Arc` this
    /// handle stores) re-reads exactly this value on every tick, so a
    /// declared override is honored by the real cycle -- including a
    /// cycle running on a loop already spawned before this setter was
    /// called -- not only by this getter.
    pub fn trigger_audit_retention(&self) -> Option<Duration> {
        let _operation = self.assert_open_operation();
        Some(Duration::from_secs(
            self.trigger_audit_retention_secs.load(Ordering::SeqCst),
        ))
    }

    /// Declare the retention window for the engine's OWN durable trigger-
    /// audit history, as installation-scope operator policy -- not a
    /// per-table DDL axis (there is no user table to attach `RETAIN` to).
    /// A runtime setter on this handle's open-config ONLY: it never writes to
    /// the redb config store, so it does not survive this handle closing and
    /// a fresh [`Database`] being opened on the same path. Read back by
    /// [`Database::trigger_audit_retention`].
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn set_trigger_audit_retention(&self, window: Duration) -> Result<()> {
        let _operation = self.open_operation()?;
        self.trigger_audit_retention_secs
            .store(window.as_secs(), Ordering::SeqCst);
        Ok(())
    }

    /// An honest size answer for one table: estimated live bytes, exact live
    /// row count, and the whole file's size alongside as itself.
    ///
    /// `None` when the table does not exist. Stored-size failures and read
    /// memory refusals reach the caller instead of hiding an incomplete answer.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn table_size_estimate(&self, table: &str) -> Result<Option<TableSizeEstimate>> {
        let read_memory = self.declared_reader_memory()?;
        self.table_size_estimate_with_read_memory(table, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn table_size_estimate_with_read_memory(
        &self,
        table: &str,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<TableSizeEstimate>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        let Some(meta) = self.table_meta(table) else {
            return Ok(None);
        };
        let whole_file_bytes = self.disk_file_size();
        self.table_size_estimate_for_meta(table, &meta, whole_file_bytes, read_memory.clone())
            .map(Some)
    }

    /// One answer per table in this database, each describing its own table.
    ///
    /// A table whose stored sizes cannot be read stops this with that failure
    /// rather than reporting a table that looks smaller than it is.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn table_size_estimates(&self) -> Result<Vec<TableSizeEstimate>> {
        let read_memory = self.declared_reader_memory()?;
        self.table_size_estimates_with_read_memory(read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn table_size_estimates_with_read_memory(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<TableSizeEstimate>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        let whole_file_bytes = self.disk_file_size();
        let metas = self.relational_store.table_meta.read().clone();
        let mut estimates = metas
            .iter()
            .map(|(table, meta)| {
                self.table_size_estimate_for_meta(
                    table,
                    meta,
                    whole_file_bytes,
                    read_memory.clone(),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        estimates.sort_by(|left, right| left.table.cmp(&right.table));
        Ok(estimates)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn table_size_estimate_for_meta(
        &self,
        table: &str,
        meta: &TableMeta,
        whole_file_bytes: Option<u64>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<TableSizeEstimate> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let disk_backed = self.relational_store.uses_disk_backed_committed_bodies();
        if disk_backed {
            let root = self
                .writer_read_root()?
                .ok_or_else(|| Error::Other("table sizing requires its captured source".into()))?;
            let meta = root
                .borrow_captured_table_meta(table)
                .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
            let mut cursor = root.cursor(
                crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                &RedbPersistence::rel_table_name(table),
                crate::persistence::writer_source::WriterCursorFamily::Bytes,
                std::ops::Bound::Unbounded,
                std::ops::Bound::Unbounded,
                true,
            )?;
            let mut selected = None;
            let mut row_count = 0u64;
            let mut estimated_live_bytes = 0u64;
            while let Some(window) = cursor.next_row()? {
                let row = &window.value;
                if selected == Some(row.row_id) || row.created_tx.0 > root.snapshot.0 {
                    continue;
                }
                selected = Some(row.row_id);
                if !row.visible_at(root.snapshot) {
                    continue;
                }
                let physical = root.row_body_bytes(
                    &crate::persistence::writer_source::SourceWindowOwner::Read(
                        read_memory.clone(),
                    ),
                    table,
                    row,
                )?;
                let resident = estimate_row_bytes_for_meta(&row.values, meta, true);
                row_count = row_count.saturating_add(1);
                estimated_live_bytes =
                    estimated_live_bytes.saturating_add(resident.max(physical) as u64);
            }
            return Ok(TableSizeEstimate {
                table: table.to_owned(),
                estimated_live_bytes,
                row_count,
                whole_file_bytes,
            });
        }
        let mut row_count = 0u64;
        let mut estimated_live_bytes = 0u64;
        let mut offset = 0usize;
        loop {
            // One bounded run at a time: a table of a million rows is sized
            // without ever holding a million keys and a million stored lengths
            // in memory, and the room one run needs is admitted before it is
            // taken.
            let run = {
                let tables = self.relational_store.tables.read();
                let Some(rows) = tables.get(table) else {
                    break;
                };
                if offset >= rows.len() {
                    break;
                }
                let end = rows.len().min(offset.saturating_add(TABLE_SIZE_RUN_ROWS));
                let reservation = contextdb_core::read_memory::ReadCredit::try_new(
                    read_memory.clone(),
                    (end - offset)
                        .saturating_mul(TABLE_SIZE_RUN_ROW_BYTES.saturating_add(table.len())),
                    "reader",
                    "table_size_estimate",
                    "Raise reader memory to inspect this table size.",
                )?;
                let mut run = Vec::with_capacity(end - offset);
                for row in &rows[offset..end] {
                    if row.deleted_tx.is_some() {
                        continue;
                    }
                    run.push((
                        (table.to_string(), row.row_id, row.created_tx, row.lsn),
                        estimate_row_bytes_for_meta(&row.values, meta, true),
                    ));
                }
                offset = end;
                (run, reservation)
            };
            let (run, _reservation) = run;
            // A committed body the in-memory directory has shed still occupies
            // its full room in the file, so the answer for a stored table comes
            // from what the store holds, not from the resident projection. A
            // stored length this cannot read is a failure, never a zero.
            let stored = if disk_backed && !run.is_empty() {
                match self.persistence.as_ref() {
                    Some(persistence) => {
                        let keys = run.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
                        persistence.stored_relational_row_bytes(&keys)?
                    }
                    None => HashMap::new(),
                }
            } else {
                HashMap::new()
            };
            row_count = row_count.saturating_add(run.len() as u64);
            for (key, resident) in &run {
                let physical = stored.get(key).copied().unwrap_or(0);
                estimated_live_bytes =
                    estimated_live_bytes.saturating_add((*resident).max(physical) as u64);
            }
        }
        Ok(TableSizeEstimate {
            table: table.to_string(),
            estimated_live_bytes,
            row_count,
            whole_file_bytes,
        })
    }

    sync_test_seam! {
    /// Record this database's ONE authoritative authenticated hub. Persisted
    /// metadata survives reboot, so a different ticket cannot claim pull,
    /// push, or purge authority. Re-registering the same peer is a reconnect;
    /// use `change_retention_sync_peer` to move the durable destination.
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    fn register_retention_sync_peer(&self, node_id: &str) -> Result<()> {
        self.register_retention_sync_peer_mode(node_id, false)
    }
    }

    /// Construction alone can preserve a mismatched durable binding so the
    /// operator can use the existing destination-change recovery operation.
    /// All other registration failures still propagate to the constructor.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn register_retention_sync_peer_for_client(&self, node_id: &str) -> Result<()> {
        self.register_retention_sync_peer_mode(node_id, true)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn register_retention_sync_peer_mode(
        &self,
        node_id: &str,
        construction_recovery: bool,
    ) -> Result<()> {
        let _operation = self.open_operation()?;
        let mut established = self.retention_sync_peer.lock();
        // Authority now gates every authenticated pull and push. Reload the
        // durable value under the same lock rather than treating a failed
        // open-time best-effort load as an unbound database that may be
        // silently claimed by whichever peer connects next.
        if let Some(persistence) = &self.persistence {
            let durable =
                persistence.load_config_value::<String>(RETENTION_SYNC_PEER_CONFIG_KEY)?;
            if durable != *established {
                *established = durable;
            }
        }
        if let Some(existing) = established.as_deref() {
            if existing == node_id {
                return Ok(());
            }
            if construction_recovery {
                // Keep the existing durable binding. The client can inspect
                // status or explicitly change its destination; push and pull
                // still repeat the strict binding check before I/O.
                return Ok(());
            }
            return Err(Error::SchemaInvalid {
                reason: format!(
                    "this database is durably bound to authoritative hub {existing} for pull, \
                     push, and PURGE authority; authenticated hub {node_id} is refused — use the \
                     existing destination-change operation to move the binding"
                ),
            });
        }
        if let Some(persistence) = &self.persistence {
            self.publish_direct_metadata(|| {
                persistence.flush_config_value(RETENTION_SYNC_PEER_CONFIG_KEY, &node_id.to_string())
            })?;
        }
        *established = Some(node_id.to_string());
        Ok(())
    }

    /// The established retention hub, read back from database metadata after a
    /// restart.
    pub fn retention_sync_peer(&self) -> Option<String> {
        let _operation = self.assert_open_operation();
        self.retention_sync_peer.lock().clone()
    }

    sync_test_seam! {
    /// Move this edge's retained-data destination to `new_node_id`, and in the
    /// SAME operation forget everything the PREVIOUS destination confirmed. A
    /// retained table is delivered to exactly one destination at a time (see
    /// [`Database::register_retention_sync_peer`], which refuses a silent
    /// second binding); this is the shipped way to change WHICH one — any
    /// number of times, in either direction — without editing the database by
    /// hand.
    ///
    /// The two halves must be one operation, and it must fail safe. The
    /// persisted push and pull watermarks (per tenant) and the engine
    /// deletion-gate watermark all record progress against the OLD destination's
    /// history. Left in place while the binding moves, the edge would skip
    /// re-uploading rows the new — possibly empty — destination never received,
    /// and would delete locally-expired rows the moment retention next runs,
    /// because the deletion gate still reads "already delivered".
    ///
    /// So the whole body runs under the pruning guard (no maintenance cycle can
    /// prune in the window), and the confirmation record is cleared BEFORE the
    /// new binding is made durable. The deletion-gate watermark is driven
    /// explicitly BACK to `Lsn(0)` — the forward-only
    /// `advance_engine_sync_watermark` path cannot express a reset, so this is
    /// the one place that regresses it. If any step fails part-way, the edge is
    /// left on the SAFE side: the old peer is still bound and the gate is low, so
    /// nothing local is deleted and a re-upload targets the OLD hub, which
    /// already holds the rows. After a clean change nothing local is deleted
    /// until the NEW destination confirms receipt, exactly as a first-ever
    /// upload.
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    fn change_retention_sync_peer(
        &self,
        tenant_id: &TenantId,
        new_node_id: &str,
    ) -> Result<()> {
        let _operation = self.open_operation()?;
        // Serialize the complete reset/rebind boundary with any
        // hub-authenticated progress write. An old-hub request may finish its
        // network exchange concurrently, but it cannot restore that hub's
        // frontier after this operation has made the new binding durable.
        let mut established = self.retention_sync_peer.lock();
        // Hold the pruning guard for the whole operation: a maintenance cycle
        // that ran between the gate reset and the rebind could otherwise prune
        // SYNC SAFE rows against a frontier that no longer applies. Neither the
        // watermark persists nor the config flush below take the pruning guard,
        // so this does not self-deadlock; `open_operation` is reentrant, so the
        // nested calls take no second operation lock.
        let _guard = MaintenanceReceiptGuard::lock(&self.pruning_guard);
        // Clear the confirmation record FIRST, so a mid-operation failure lands
        // on the safe side. The gate goes to `Lsn(0)` and both persisted
        // watermarks are cleared before the new binding is durable.
        self.sync_watermark.store(Lsn(0), Ordering::SeqCst);
        self.persist_sync_push_watermark(tenant_id, Lsn(0))?;
        self.persist_sync_pull_watermark(tenant_id, Lsn(0))?;
        self.persist_sync_pending_push_confirmation(tenant_id, None)?;
        if self.take_retention_peer_persist_fault_for_test() {
            return Err(Error::Other((
                "injected persistence failure mid destination change (test seam)".to_string()).into()));
        }
        // Only now make the new binding durable — last, so a failure above leaves
        // the OLD destination bound with the gate already low. The matching
        // re-upload target is committed beside the binding: after a restart the
        // edge still knows that rows inherited from the former hub must cross
        // this explicit move once, while an interrupted change can never expose
        // a target without its matching authoritative peer.
        let new_node_id = new_node_id.to_string();
        let reupload_key = tenant_id.config_key(DESTINATION_REUPLOAD_TARGET_CONFIG_NAME);
        let reupload = DestinationReuploadEpoch {
            epoch_id: uuid::Uuid::new_v4(),
            hub_node_id: new_node_id.clone(),
            held_through_lsn: self.current_lsn(),
            held_ddl: (*self
                .ddl_log)
                .read()
                .iter()
                .map(|(lsn, ddl)| {
                    rmp_serde::to_vec(ddl)
                        .map(|encoded| (*lsn, encoded))
                        .map_err(|err| {
                            Error::SyncError((format!(
                                "cannot capture held schema for destination move: {err}"
                            )).into())
                        })
                })
                .collect::<Result<Vec<_>>>()?,
        };
        if let Some(persistence) = &self.persistence {
            self.publish_direct_metadata(|| {
                persistence.flush_encoded_config_values(vec![
                (
                    RETENTION_SYNC_PEER_CONFIG_KEY,
                    RedbPersistence::encode_config_value(&new_node_id)?,
                ),
                (
                    reupload_key.as_str(),
                    RedbPersistence::encode_config_value(&reupload)?,
                ),
            ])
            })?;
        } else {
            self.in_memory_destination_reuploads
                .lock()
                .insert(reupload_key, reupload);
        }
        *established = Some(new_node_id.to_string());
        Ok(())
    }
    }

    sync_test_seam! {
    #[cfg(any(test, feature = "test-seams"))]
    /// Whether this authenticated destination still needs the one explicit
    /// re-upload created by [`Database::change_retention_sync_peer`]. The
    /// target is durable and names the destination, so a failed or concurrent
    /// move cannot disable ordinary echo suppression toward another hub.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn destination_reupload_frontier(
        &self,
        tenant_id: &TenantId,
        hub_node_id: &str,
    ) -> Result<Option<Lsn>> {
        let _operation = self.open_operation()?;
        let Some(persistence) = &self.persistence else {
            return Ok((*self
                .in_memory_destination_reuploads)
                .lock()
                .get(&tenant_id.config_key(DESTINATION_REUPLOAD_TARGET_CONFIG_NAME))
                .filter(|target| target.hub_node_id == hub_node_id)
                .map(|target| target.held_through_lsn));
        };
        let established = persistence
            .load_config_value::<String>(RETENTION_SYNC_PEER_CONFIG_KEY)?;
        if established.as_deref() != Some(hub_node_id) {
            return Ok(None);
        }
        let target = persistence.load_config_value::<DestinationReuploadEpoch>(
            &tenant_id.config_key(DESTINATION_REUPLOAD_TARGET_CONFIG_NAME),
        )?;
        Ok(target
            .filter(|target| target.hub_node_id == hub_node_id)
            .map(|target| target.held_through_lsn))
    }
    }

    /// Capture the complete target-bound re-upload epoch before a push begins.
    /// The opaque identity deliberately does not derive from the frontier: a
    /// move away and back to the same hub can have the same frontier while
    /// still requiring a new rebuild acknowledgement.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn destination_reupload_epoch(
        &self,
        tenant_id: &TenantId,
        hub_node_id: &str,
    ) -> Result<Option<(Lsn, uuid::Uuid)>> {
        let _operation = self.open_operation()?;
        let target = if let Some(persistence) = &self.persistence {
            let established =
                persistence.load_config_value::<String>(RETENTION_SYNC_PEER_CONFIG_KEY)?;
            if established.as_deref() != Some(hub_node_id) {
                return Ok(None);
            }
            persistence.load_config_value::<DestinationReuploadEpoch>(
                &tenant_id.config_key(DESTINATION_REUPLOAD_TARGET_CONFIG_NAME),
            )?
        } else {
            self.in_memory_destination_reuploads
                .lock()
                .get(&tenant_id.config_key(DESTINATION_REUPLOAD_TARGET_CONFIG_NAME))
                .cloned()
        };
        Ok(target
            .filter(|target| target.hub_node_id == hub_node_id)
            .map(|target| (target.held_through_lsn, target.epoch_id)))
    }

    sync_test_seam! {
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn destination_reupload_epoch_identity_for_test(
        &self,
        tenant_id: &TenantId,
        hub_node_id: &str,
    ) -> Result<Option<uuid::Uuid>> {
        self.destination_reupload_epoch(tenant_id, hub_node_id)
            .map(|epoch| epoch.map(|(_, identity)| identity))
    }
    }

    sync_test_seam! {
    /// Retire the explicit destination re-upload only after that destination
    /// has confirmed every selected source group. A stale client cannot clear
    /// a newer move because both the durable binding and target must still name
    /// the peer whose push just completed.
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    fn complete_destination_reupload(
        &self,
        tenant_id: &TenantId,
        hub_node_id: &str,
        epoch_id: uuid::Uuid,
    ) -> Result<()> {
        self.with_authoritative_hub_reply(hub_node_id, |db| {
            db.complete_destination_reupload_while_authoritative(tenant_id, hub_node_id, epoch_id)
        })
    }

    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    pub(crate) fn complete_destination_reupload_while_authoritative(
        &self,
        tenant_id: &TenantId,
        hub_node_id: &str,
        epoch_id: uuid::Uuid,
    ) -> Result<()> {
        let Some(persistence) = &self.persistence else {
            let key = tenant_id.config_key(DESTINATION_REUPLOAD_TARGET_CONFIG_NAME);
            let mut reuploads = self.in_memory_destination_reuploads.lock();
            if reuploads
                .get(&key)
                .is_some_and(|target| {
                    target.hub_node_id == hub_node_id && target.epoch_id == epoch_id
                })
            {
                reuploads.remove(&key);
            }
            return Ok(());
        };
        let key = tenant_id.config_key(DESTINATION_REUPLOAD_TARGET_CONFIG_NAME);
        let target = persistence.load_config_value::<DestinationReuploadEpoch>(&key)?;
        if target
                .as_ref()
                .is_some_and(|target| {
                    target.hub_node_id == hub_node_id && target.epoch_id == epoch_id
                })
        {
            self.publish_direct_metadata(|| persistence.remove_config_value(&key))?;
        }
        Ok(())
    }
    }

    /// Arm the one-shot test injection seam: the next
    /// `change_retention_sync_peer` on THIS thread fails after clearing the
    /// confirmation record. Production-dead — nothing but tests calls it.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __arm_retention_reclaim_crash_for_test(&self, after_commit: bool) {
        crate::persistence::arm_retention_reclaim_crash_for_test(if after_commit { 2 } else { 1 });
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __arm_retention_peer_persist_fault_for_test(&self) {
        RETENTION_PEER_PERSIST_FAULT.with(|f| f.set(true));
    }

    /// Arm the one-shot test injection seam: the NEXT version-cleanup pass
    /// (`compact_currency_versions` / a maintenance cycle's currency pass) on
    /// THIS thread fails before any durable or in-memory removal. Production-dead --
    /// nothing but tests calls it.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __arm_currency_compaction_persist_fault_for_test(&self) {
        CURRENCY_COMPACTION_PERSIST_FAULT.with(|f| f.set(true));
    }

    /// Arm the one-shot test injection seam: the NEXT `compact_now`/
    /// `MaintenanceContext::maybe_auto_compact` call's post-compact handle
    /// recycle (close the store's redb handle, then reopen the same
    /// already-compacted file) fails its reopen — the file-level compaction
    /// itself still runs and still succeeds; only the reopen fails. Lets a
    /// test assert the on-disk file stays intact and a fresh `Database::open`
    /// on the same path recovers every row. Production-dead — nothing but a
    /// test calls it.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __arm_handle_recycle_reopen_fault_for_test(&self) {
        crate::persistence::arm_handle_recycle_reopen_fault_for_test();
    }

    /// Arm the one-shot test race seam: the NEXT `execute` call on THIS
    /// thread closes its own database synchronously in the gap between the
    /// `already_closed` pre-check and the fallible snapshot registration —
    /// deterministically simulating a close landing in that exact window,
    /// on the SAME thread. Lets a test prove the registration's fallible
    /// form catches it instead of panicking, without needing a second
    /// thread to ever actually win a real race. Production-dead — nothing
    /// but a test calls it.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __arm_close_in_snapshot_registration_race_window_for_test(&self) {
        CLOSE_IN_SNAPSHOT_REGISTRATION_RACE_WINDOW.with(|f| f.set(true));
    }

    /// Consume the armed injection flag, if any. Reads a thread-local that is
    /// never set outside a test, so the production path takes the `false` arm.
    pub(super) fn take_retention_peer_persist_fault_for_test(&self) -> bool {
        RETENTION_PEER_PERSIST_FAULT.with(|f| f.replace(false))
    }

    /// Collapse every table that declares `HISTORY CURRENT ONLY` back to one
    /// live version per logical row, dropping superseded MVCC versions and
    /// their change-log references, associated vector copies, and eligible
    /// commit-index entries in one durable transaction. Active readers retain
    /// every version they can still see. This does not compact the storage file;
    /// file compaction is a separate maintenance operation. Sync replay continues
    /// to resolve the surviving current state across cleanup.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn compact_currency_versions(&self) -> Result<CurrencyCompactionReport> {
        let read_memory = self.declared_reader_memory()?;
        self.compact_currency_versions_with_read_memory(read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn compact_currency_versions_with_read_memory(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<CurrencyCompactionReport> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.assert_open_operation();
        let _guard = MaintenanceReceiptGuard::lock(&self.pruning_guard);
        self.maintenance_context()
            .run_currency(CurrencyGate::Always, read_memory.clone())
    }

    /// Test-only entry point mirroring `compact_currency_versions`, over a
    /// CALLER-SUPPLIED table list rather than the declared eligibility set.
    /// Production-dead -- nothing but tests calls it, and it runs the exact
    /// unmodified real mechanism (`compact_currency_versions_inner`); the
    /// production entry point above is untouched. Lets a test exercise the
    /// real pass against a table shape it has not declared `HISTORY CURRENT
    /// ONLY` on (yet) — for example a vector-bearing table.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __compact_currency_versions_for_tables_for_test(
        &self,
        tables: &[&str],
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<CurrencyCompactionReport> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.assert_open_operation();
        let _guard = MaintenanceReceiptGuard::lock(&self.pruning_guard);
        self.maintenance_context()
            .run_currency_for_tables(tables, read_memory.clone())
    }

    /// Test-introspection surface: a content fingerprint over the full
    /// vector and graph-adjacency populations AS PERSISTED ON DISK,
    /// independent of iteration order. Used to prove a maintenance pass that
    /// has no business touching these tables left their persisted contents
    /// byte-identical -- a moved byte here means a population was rewritten
    /// with no reason to be. Reads back the redb-backed `vector_entries`,
    /// `graph_fwd`, and `graph_rev` tables through
    /// [`RedbPersistence::load_vectors`] /
    /// [`RedbPersistence::load_forward_edges`] /
    /// [`RedbPersistence::load_reverse_edges`] -- the in-memory `VectorStore`
    /// / `GraphStore` populations are a cache in front of that persisted
    /// state, so hashing them instead would prove nothing about whether the
    /// FILE was rewritten, only about the cache. On an in-memory database
    /// (no persistence layer) this falls back to the in-memory populations,
    /// since there is no persisted state to read; that fallback earns no
    /// on-disk-bytes guarantee and this bench never exercises it.
    /// Production-dead: pure read-only observation, no behavior change.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __vector_and_graph_fingerprint_for_test(
        &self,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<(String, String)> {
        let Some(persistence) = self.persistence.as_ref() else {
            let mut vector_lines: Vec<String> = self
                .vector_store
                .all_entries(vector_copy_owner)?
                .iter()
                .map(|entry| format!("{entry:?}"))
                .collect();
            vector_lines.sort();
            let vector_hash = blake3::hash(vector_lines.join("\n").as_bytes())
                .to_hex()
                .to_string();
            let mut edge_lines: Vec<String> = self
                .graph_store
                .forward_adj
                .read()
                .values()
                .flat_map(|entries| entries.iter())
                .map(|entry| format!("{entry:?}"))
                .collect();
            edge_lines.sort();
            let edge_hash = blake3::hash(edge_lines.join("\n").as_bytes())
                .to_hex()
                .to_string();
            return Ok((vector_hash, edge_hash));
        };

        let mut vector_lines: Vec<String> = persistence
            .load_vectors()?
            .iter()
            .map(|entry| format!("{entry:?}"))
            .collect();
        vector_lines.sort();
        let vector_hash = blake3::hash(vector_lines.join("\n").as_bytes())
            .to_hex()
            .to_string();

        let mut edge_lines: Vec<String> = persistence
            .load_forward_edges()?
            .iter()
            .map(|entry| format!("fwd:{entry:?}"))
            .collect();
        edge_lines.extend(
            persistence
                .load_reverse_edges()?
                .iter()
                .map(|entry| format!("rev:{entry:?}")),
        );
        edge_lines.sort();
        let edge_hash = blake3::hash(edge_lines.join("\n").as_bytes())
            .to_hex()
            .to_string();
        Ok((vector_hash, edge_hash))
    }

    // Frozen checkpoint controls retain this former convenience call. Production
    // consumers use the bounded verification operation; no public API is restored.
    #[cfg(test)]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from historical_index_commitment.rs"
        )
    )]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(super) fn verify_historical_indexes(
        &self,
        table: &str,
    ) -> Result<Vec<crate::HistoricalIndexVerification>> {
        self.verify_historical_indexes_within(table, self.declared_reader_memory()?, &mut || Ok(()))
    }

    /// Verify historical indexes as one bounded read: `checkpoint` carries the
    /// request's cancellation and active-time limit and runs before every
    /// posting and row version, and the walk is charged to `memory`.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn verify_historical_indexes_within(
        &self,
        table: &str,
        memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<Vec<crate::HistoricalIndexVerification>> {
        let memory: contextdb_core::read_memory::ReadMemoryOwner = memory.into();

        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
        if let Some(image) = &self.committed_image_startup {
            return image
                .config_source
                .verify_historical_indexes(table, &meta, memory, checkpoint);
        }
        match &self.persistence {
            Some(persistence) => {
                persistence.verify_historical_indexes(table, &meta, memory, checkpoint)
            }
            None => Err(Error::ReadFailure(
                contextdb_core::read_contract::ReadFailure::owner_route_unsupported(
                    "a memory-only store keeps no historical index commitments to verify".into(),
                ),
            )),
        }
    }

    /// Production-dead count of physical stored row versions, including
    /// superseded keepers. File-backed writers count durable keys without
    /// decoding bodies; in-memory stores count their resident versions.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __physical_version_count_for_test(&self, table: &str) -> usize {
        if let Some(persistence) = &self.persistence {
            if self.table_meta(table).is_none() {
                return 0;
            }
            return persistence
                .durable_version_count_for_test(table)
                .expect("physical version observation requires the durable table");
        }
        self.relational_store
            .tables
            .read()
            .get(table)
            .map_or(0, Vec::len)
    }

    /// Test-introspection surface: the PHYSICAL vector-entry count across
    /// the whole database right now -- every superseded/tombstoned entry
    /// plus every live one, mirroring `__physical_version_count_for_test`
    /// for the vector population (`VectorStore::all_entries` returns every
    /// entry ever inserted, not just live ones -- verified by reading its
    /// source, which applies no `deleted_tx` filter). Production-dead:
    /// pure read-only observation, no behavior change.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __vector_entry_count_for_test(
        &self,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<usize> {
        Ok(self.vector_store.all_entries(vector_copy_owner)?.len())
    }

    /// True when any table on this database declares `HISTORY CURRENT ONLY`,
    /// whether it was created here, arrived from disk on reopen, or arrived
    /// over synced DDL — the mirror of `has_retained_tables` for the
    /// version-history axis. Maintenance is declaration-driven: a database
    /// that declares no `RETAIN` window, no `HISTORY CURRENT ONLY`, and holds
    /// no durable trigger-audit history starts no maintenance thread at all
    /// (see [`Self::reconcile_maintenance_thread`]); one that declares any
    /// of those and wants to own its own schedule calls `pause_maintenance`
    /// and drives `run_maintenance_cycle` itself.
    pub(super) fn has_version_cleanup_tables(&self) -> bool {
        self.relational_store
            .table_meta
            .read()
            .values()
            .any(|meta| meta.history_policy == Some(HistoryPolicy::CurrentOnly))
    }

    /// True when any table declares RETAIN, whether it was created here or
    /// arrived from disk on reopen or over synced DDL.
    pub(super) fn has_retained_tables(&self) -> bool {
        self.relational_store
            .table_meta
            .read()
            .values()
            .any(|meta| meta.default_ttl_seconds.is_some())
    }

    /// True when this database keeps a durable trigger-audit history that the
    /// shipped default retention has to bound.
    pub(super) fn has_durable_trigger_audit(&self) -> bool {
        !self.trigger.declarations.lock().is_empty()
    }

    /// True when schema metadata declares at least one vector index, including
    /// an empty index whose maintained graph will become eligible after data
    /// arrives.
    pub(super) fn has_vector_index_declarations(&self) -> bool {
        !self.vector_store.is_empty()
    }

    pub(super) fn maintenance_context(&self) -> MaintenanceContext {
        {
            let mut storage = std::mem::MaybeUninit::<MaintenanceContext>::uninit();
            let mut construction = MaintenanceCleanup {
                source: storage.as_mut_ptr(),
                initialized: [false; 27],
            };
            unsafe {
                {
                    std::ptr::addr_of_mut!((*construction.source).reader_memory_limit).write(
                        self.owner_read_config
                            .limits
                            .limits
                            .memory
                            .min(contextdb_core::read_contract::ReadLimits::SHIPPED_MEMORY),
                    );
                    construction.initialized[0] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).read_request_provider)
                        .write(self.declared_read_request_provider());
                    construction.initialized[1] = true;
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).writer_receipt_observer)
                        .write(self.writer_receipt_observer_for_maintenance());
                    construction.initialized[2] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).disk_limit)
                        .write(self.disk_limit.clone());
                    construction.initialized[3] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).vector_serving_cache_bytes)
                        .write(self.vector_serving_cache_bytes.clone());
                    construction.initialized[4] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).resource_closed)
                        .write(self.resource_closed.clone());
                    construction.initialized[5] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).publication_interrupted)
                        .write(self.publication_interrupted.clone());
                    construction.initialized[6] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).worker_stop).write(None);
                    construction.initialized[7] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).relational)
                        .write(self.relational_store.clone());
                    construction.initialized[8] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).graph)
                        .write(self.graph_store.clone());
                    construction.initialized[9] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).vector)
                        .write(self.vector_store.clone());
                    construction.initialized[10] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).finalization)
                        .write(Arc::clone(&self.finalization));
                    construction.initialized[25] = true;
                }
                {
                    std::ptr::addr_of_mut!((*construction.source).pending_local_schema_stages)
                        .write(self.pending_local_schema_stages.clone());
                    construction.initialized[26] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).persistence)
                        .write(self.persistence.clone());
                    construction.initialized[11] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).change_log)
                        .write(self.change_log.clone());
                    construction.initialized[12] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).change_log_table_index)
                        .write(self.change_log_table_index.clone());
                    construction.initialized[13] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).change_log_lsn_refcounts)
                        .write(self.change_log_lsn_refcounts.clone());
                    construction.initialized[14] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).trigger)
                        .write(self.trigger.clone());
                    construction.initialized[15] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).tx_mgr)
                        .write(self.tx_mgr.clone());
                    construction.initialized[16] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).snapshot_registry)
                        .write(self.snapshot_registry.clone());
                    construction.initialized[19] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).retention_deferred_edge_nodes)
                        .write(self.retention_deferred_edge_nodes.clone());
                    construction.initialized[20] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).last_maintenance_cycle_at)
                        .write(self.last_maintenance_cycle_at.clone());
                    construction.initialized[21] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).last_auto_compact_at)
                        .write(self.last_auto_compact_at.clone());
                    construction.initialized[22] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).storage_compaction_policy)
                        .write(self.storage_compaction_policy.clone());
                    construction.initialized[23] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).sync_watermark)
                        .write(self.sync_watermark.clone());
                    construction.initialized[17] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).trigger_audit_retention_secs)
                        .write(self.trigger_audit_retention_secs.clone());
                    construction.initialized[18] = true;
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).snapshot_free_retirement_pass_count
                    )
                    .write(self.snapshot_free_retirement_pass_count.clone());
                    construction.initialized[24] = true;
                }
            }
            construction.initialized.fill(false);
            unsafe { storage.assume_init() }
        }
    }

    /// Read the established retention hub back out of database metadata at
    /// open, so the multi-hub refusal survives a reboot.
    pub(crate) fn load_retention_sync_peer(&self) {
        let Some(persistence) = &self.persistence else {
            return;
        };
        match persistence.load_config_value::<String>(RETENTION_SYNC_PEER_CONFIG_KEY) {
            Ok(peer) => *self.retention_sync_peer.lock() = peer,
            Err(err) => {
                tracing::warn!(error = %err, "failed to load the established retention sync peer")
            }
        }
    }

    /// Start or stop the engine-owned maintenance thread so it runs exactly
    /// when this database has something to maintain — a retained table, a
    /// currency table, a durable trigger audit, or a declared vector index.
    /// Called at open (next to the cron tickler) and again whenever DDL lands,
    /// so a fresh install, a reopen, and a table that arrived over synced DDL
    /// all self-maintain with no consumer call. A database with nothing to
    /// maintain spawns no thread at all. Caller-driven mode never spawns; the
    /// host drives `run_maintenance_cycle` itself.
    pub(crate) fn reconcile_maintenance_thread(&self) {
        if self.maintenance_caller_driven.load(Ordering::SeqCst) {
            // CallerDriven: never spawn, however much is declared -- the
            // host owns the schedule and drives `run_maintenance_cycle`
            // itself. If a thread is somehow still running (the policy just
            // switched away from EngineOwned), stop it.
            if self.__maintenance_thread_running() {
                self.stop_pruning_thread();
            }
            return;
        }
        let eligible = self.has_version_cleanup_tables()
            || self.has_retained_tables()
            || self.has_durable_trigger_audit()
            || self.has_vector_index_declarations();
        let running = self.__maintenance_thread_running();
        if eligible && !running {
            self.spawn_maintenance(self.maintenance_poll_interval());
        } else if !eligible && running {
            // The last declaration left (an ALTER dropped the only RETAIN
            // window, the only HISTORY CURRENT ONLY table was dropped, ...):
            // stop the loop rather than leaving it ticking forever over
            // nothing to maintain.
            self.stop_pruning_thread();
        }
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __hold_engine_owned_vector_maintenance_clock_for_test(
        &self,
    ) -> Result<VectorMaintenanceWorkerClockGuardForTest> {
        let _operation = self.assert_open_operation();
        let runtime = self.pruning_runtime.lock();
        if runtime.handle.is_none() {
            return Err(Error::Other(
                ("engine-owned vector maintenance worker is not running".to_string()).into(),
            ));
        }
        runtime.worker_clock.hold();
        Ok(VectorMaintenanceWorkerClockGuardForTest {
            clock: runtime.worker_clock.clone(),
            shutdown: runtime.shutdown.clone(),
        })
    }

    /// Test-only: restart the currency-maintenance thread at a short interval so
    /// a test need not wait the production cadence. No shipped surface sets this.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __set_currency_maintenance_interval(&self, interval: Duration) {
        let _operation = self.assert_open_operation();
        self.spawn_maintenance(interval);
    }

    /// Test-only: whether a maintenance/pruning thread is currently running.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __maintenance_thread_running(&self) -> bool {
        self.pruning_runtime.lock().handle.is_some()
    }

    #[cfg(not(any(test, feature = "test-seams")))]
    pub(crate) fn __maintenance_thread_running(&self) -> bool {
        self.pruning_runtime.lock().handle.is_some()
    }

    /// Test-only: wake-cycles STARTED by the current currency-maintenance
    /// thread since spawn (counted on wake, before the pending gate).
    /// Test-build only — production carries neither the counter nor this
    /// reader; tests poll it as the liveness half of a state-based wait.
    #[cfg(test)]
    pub(crate) fn __maintenance_wakes(&self) -> u64 {
        self.pruning_runtime.lock().wakes.load(Ordering::SeqCst)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn spawn_maintenance(&self, interval: Duration) {
        self.stop_pruning_thread();

        let shutdown = Arc::new(AtomicBool::new(false));
        let interval_ms = Arc::new(AtomicU64::new(
            u64::try_from(interval.as_millis())
                .unwrap_or(u64::MAX)
                .max(1),
        ));
        #[cfg(test)]
        let wakes = Arc::new(AtomicU64::new(0));
        #[cfg(test)]
        let thread_wakes = wakes.clone();
        let mut context = self.maintenance_context();
        context.worker_stop = Some(shutdown.clone());
        let pruning_guard = self.pruning_guard.clone();
        let thread_shutdown = shutdown.clone();
        let worker_clock = Arc::new(MaintenanceWorkerClock::default());
        let thread_worker_clock = worker_clock.clone();
        let thread_interval_ms = interval_ms.clone();

        #[cfg(any(test, feature = "test-seams"))]
        crate::read_probe::note_maintenance_worker_start();
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_probe::note_thread_maintenance_worker_start();
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_probe::note_background_worker_start();
        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::SeqCst) {
                let controlled_serial = match thread_worker_clock
                    .wait_for_cycle(&thread_shutdown, &thread_interval_ms)
                {
                    Some(serial) => serial,
                    None => break,
                };
                if thread_shutdown.load(Ordering::SeqCst) {
                    break;
                }
                #[cfg(test)]
                thread_wakes.fetch_add(1, Ordering::SeqCst);
                let _guard = MaintenanceReceiptGuard::lock(&pruning_guard);
                #[cfg(any(test, feature = "test-seams"))]
                let _held = crate::persistence::PruningGuardHeldForTest::enter();
                // ONE loop, every job. Each pass carries its OWN cheap gate so a
                // quiet node does near-zero work per tick: the currency pass is
                // gated on superseded-version count, the retention pass bails
                // before the commit lock when no table declares RETAIN, and the
                // audit pass takes no write transaction unless something aged
                // out. No pass relies on another's gate.
                crate::memory_accounting::observe_inherited_process_ceiling();
                #[cfg(any(test, feature = "test-seams"))]
                let mut emission = None;
                match context
                    .finalization
                    .accountant
                    .read_budget()
                    .and_then(|read_memory| context.run_cycle(CurrencyGate::Scheduled, read_memory))
                {
                    Ok(report) => {
                        log_maintenance_cycle(&report);
                        #[cfg(any(test, feature = "test-seams"))]
                        if let Some(observer) = &context.writer_receipt_observer {
                            emission = observer
                                .prepare_writer_memory_ownership_receipt_for_test("maintenance");
                        }
                    }
                    Err(err) => log_pruning_error(&err),
                }
                #[cfg(any(test, feature = "test-seams"))]
                drop(_held);
                drop(_guard);
                #[cfg(any(test, feature = "test-seams"))]
                if let Some(emission) = emission {
                    emission.emit();
                }
                thread_worker_clock.complete(controlled_serial);
            }
            thread_worker_clock.mark_worker_exited();
        });

        let mut runtime = self.pruning_runtime.lock();
        runtime.shutdown = shutdown;
        runtime.handle = Some(handle);
        runtime.interval_ms = interval_ms;
        runtime.worker_clock = worker_clock;
        #[cfg(test)]
        {
            runtime.wakes = wakes;
        }
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub fn __fk_probe_stats(&self) -> FkProbeStats {
        FkProbeStats {
            indexed_tuple_probes: self.fk_indexed_tuple_probes.load(Ordering::SeqCst),
            full_scan_fallbacks: self.fk_full_scan_fallbacks.load(Ordering::SeqCst),
        }
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub fn __reset_fk_probe_stats(&self) {
        let _operation = self.assert_open_operation();
        self.fk_indexed_tuple_probes.store(0, Ordering::SeqCst);
        self.fk_full_scan_fallbacks.store(0, Ordering::SeqCst);
    }

    /// Per-stage commit work counters accumulated since the last reset.
    /// `#[doc(hidden)]` test-introspection surface.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __commit_stage_stats(&self) -> CommitStageStats {
        let _operation = self.assert_open_operation();
        #[cfg(any(test, feature = "test-seams"))]
        let stage_wall_nanos = {
            let timings = self
                .commit_stage_wall_nanos
                .each_ref()
                .map(|counter| counter.load(Ordering::SeqCst));
            timings.iter().any(|nanos| *nanos != 0).then_some(timings)
        };
        #[cfg(not(any(test, feature = "test-seams")))]
        let stage_wall_nanos = None;
        CommitStageStats {
            rows_validated: self.commit_rows_validated.load(Ordering::SeqCst),
            indexed_probes: self.commit_indexed_probes.load(Ordering::SeqCst),
            staged_vs_staged_comparisons: self
                .commit_staged_vs_staged_comparisons
                .load(Ordering::SeqCst),
            scan_rows_touched: self.commit_scan_rows_touched.load(Ordering::SeqCst),
            index_maintenance_visits: self.commit_index_maintenance_visits.load(Ordering::SeqCst),
            stage_wall_nanos,
        }
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __reset_commit_stage_stats(&self) {
        let _operation = self.assert_open_operation();
        self.commit_rows_validated.store(0, Ordering::SeqCst);
        self.commit_indexed_probes.store(0, Ordering::SeqCst);
        self.commit_staged_vs_staged_comparisons
            .store(0, Ordering::SeqCst);
        self.commit_scan_rows_touched.store(0, Ordering::SeqCst);
        self.commit_index_maintenance_visits
            .store(0, Ordering::SeqCst);
        #[cfg(any(test, feature = "test-seams"))]
        for counter in &self.commit_stage_wall_nanos {
            counter.store(0, Ordering::SeqCst);
        }
    }

    /// Set the maintenance loop interval so a test need not wait the
    /// production cadence. Restarts the same single loop; no shipped surface
    /// sets this.
    pub fn set_pruning_interval(&self, interval: Duration) {
        let _operation = self.assert_open_operation();
        self.spawn_maintenance(interval);
    }

    pub fn sync_watermark(&self) -> Lsn {
        let _operation = self.assert_open_operation();
        self.sync_watermark.load(Ordering::SeqCst)
    }

    sync_test_seam! {
    fn set_sync_watermark(&self, watermark: Lsn) {
        let _operation = self.assert_open_operation();
        self.sync_watermark.store(watermark, Ordering::SeqCst);
    }
    }

    pub fn instance_id(&self) -> uuid::Uuid {
        self.instance_id
    }

    sync_test_seam! {
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    #[cfg_attr(not(feature = "test-seams"), allow(dead_code, reason = "KEEP-SEAM: live only under the test-seams feature; used from schema_stage_registry.rs"))]
    fn open_memory_with_plugin_and_accountant(
        plugin: Arc<dyn DatabasePlugin>,
        accountant: Arc<MemoryAccountant>,
    ) -> Result<Self> {
        Self::open_memory_internal(plugin, WriterBudget::from_accountant(accountant)?, crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(OwnerReadConfig::default().limits.limits.memory)?)
    }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn open_memory_with_plugin(plugin: Arc<dyn DatabasePlugin>) -> Result<Self> {
        Self::open_with_plugin(MEMORY_DATABASE_PATH, plugin)
    }

    /// Exports a transactionally consistent snapshot of this database into a
    /// brand-new artifact file at `dest`. The source stays fully live: the
    /// commit mutex is held only for a bounded snapshot capture (never across
    /// the copy), and pruning is deferred for the export's duration so rows
    /// alive-but-expired at the snapshot land in the artifact.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn export_snapshot(&self, dest: impl AsRef<Path>) -> Result<ExportReport> {
        let read_memory = self.declared_reader_memory()?;
        self.export_snapshot_with_read_memory(dest, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn export_snapshot_with_read_memory(
        &self,
        dest: impl AsRef<Path>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ExportReport> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let image = crate::read_image_memory::ImageMemory::operation_reader(
            self.accountant.clone(),
            read_memory.clone(),
        )?;
        let _memory = image.enter();
        let mut retained_vector_copy = crate::vector_read_memory::admission(read_memory.clone());
        let vector_copy_owner = &mut retained_vector_copy;

        let _operation = self.open_operation()?;
        let dest = dest.as_ref();
        // Fast typed error before anything is created; the no-replace
        // hard-link publish below is the race-proof backstop.
        if dest.symlink_metadata().is_ok() {
            return Err(Error::ExportDestinationExists {
                path: dest.to_path_buf(),
            });
        }
        let _pruning_deferral = MaintenanceReceiptGuard::lock(&self.pruning_guard);

        // The unique segment sits before the temp extension so each attempt's
        // PID-lock path (`{dest}.{unique}.lock`) is unique too: concurrent
        // exports never contend on a lock path.
        let unique = uuid::Uuid::new_v4().simple().to_string();
        let mut temp_name = dest.as_os_str().to_os_string();
        temp_name.push(format!(".{unique}.tmpexport"));
        let temp_path = PathBuf::from(temp_name);
        let mut temp_lock_name = temp_path.as_os_str().to_os_string();
        temp_lock_name.push(".lock");
        let temp_lock_path = PathBuf::from(temp_lock_name);

        // Where this export's copy starts. A purge whose frontier lands after
        // it fences the export, whether the copy finished or the erasure
        // invalidated the storage it was copying from.
        let export_start_lsn = self.current_lsn();
        match self.write_export_artifact(dest, &temp_path, vector_copy_owner, read_memory.clone()) {
            Ok(report) => {
                // The purge frontier recheck and no-replace publish share
                // the commit mutex.  A purge therefore cannot land between
                // proving this captured snapshot safe and exposing an
                // artifact that still contains the destroyed lineage.
                #[cfg(any(test, feature = "test-seams"))]
                crate::writer_commit_test_seams::reached(
                    crate::writer_commit_test_seams::CommitBoundary::ExportBeforePublication,
                );
                let publish = self.with_commit_lock(|| -> Result<()> {
                    if let Some(record) = self.purge_frontier_after_snapshot(report.snapshot_lsn)? {
                        let frontier = record
                            .purge_frontier
                            .as_deref()
                            .and_then(|frontier| frontier.parse::<u64>().ok())
                            .expect("permanent purge frontier was checked");
                        return Err(Error::PurgeExportSnapshotFence {
                            table: record.table,
                            key: record.natural_key.pairs(),
                            lineage_root: record.lineage_root,
                            frontier: Lsn(frontier),
                            snapshot_lsn: report.snapshot_lsn,
                        });
                    }
                    // DISCARD deliberately has no Purged lifecycle record.
                    // Its canonical local frontier must fence publication too.
                    self.validate_export_erasure_frontier(
                        report.snapshot_lsn,
                        read_memory.clone(),
                    )?;
                    RedbPersistence::publish_export_artifact(&temp_path, dest).map_err(|err| {
                        if err.kind() == std::io::ErrorKind::AlreadyExists {
                            Error::ExportDestinationExists {
                                path: dest.to_path_buf(),
                            }
                        } else {
                            export_io_error(dest, &err)
                        }
                    })
                });
                let _ = std::fs::remove_file(&temp_path);
                let _ = std::fs::remove_file(&temp_lock_path);
                match publish {
                    Ok(()) => Ok(report),
                    Err(err) => Err(err),
                }
            }
            Err(err) => {
                // Remove every file this attempt created — the temp artifact
                // and its lock — and nothing else.
                let _ = std::fs::remove_file(&temp_path);
                let _ = std::fs::remove_file(&temp_lock_path);
                let erased = matches!(
                    &err,
                    Error::ReadFailure(failure)
                        if failure.kind() == contextdb_core::read_contract::ReadFailureKind::AuthoritativeErasure
                );
                if erased
                    && let Some(record) = self
                        .with_commit_lock(|| self.purge_frontier_after_snapshot(export_start_lsn))?
                {
                    let frontier = record
                        .purge_frontier
                        .as_deref()
                        .and_then(|frontier| frontier.parse::<u64>().ok())
                        .expect("permanent purge frontier was checked");
                    return Err(Error::PurgeExportSnapshotFence {
                        table: record.table,
                        key: record.natural_key.pairs(),
                        lineage_root: record.lineage_root,
                        frontier: Lsn(frontier),
                        snapshot_lsn: export_start_lsn,
                    });
                }
                Err(err)
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_export_erasure_frontier(
        &self,
        snapshot_lsn: Lsn,
        memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let memory: contextdb_core::read_memory::ReadMemoryOwner = memory.into();

        let frontier = match &self.persistence {
            Some(persistence) => persistence.global_erasure_frontier(memory.clone())?,
            None => self.lineage_state_lock.lock().erasure_frontiers.global(),
        };
        if frontier <= snapshot_lsn {
            return Ok(());
        }
        let reason = "authoritative erasure invalidated the exported snapshot";
        let _diagnostic = contextdb_core::read_memory::ReadCredit::try_new(
            memory,
            reason.len(),
            "reader",
            "export_snapshot",
            "Raise the reader memory limit to report authoritative erasure.",
        )?;
        Err(Error::ReadFailure(
            contextdb_core::read_contract::ReadFailure::new(
                contextdb_core::read_contract::ReadFailureKind::AuthoritativeErasure,
                contextdb_core::read_contract::ReadFailureDetail::Reason {
                    reason: reason.to_owned(),
                },
            )
            .expect("authoritative-erasure refusal carries ordinary reason detail"),
        ))
    }

    /// Find a permanent lineage frontier that was committed after an export's
    /// capture point.  Called only while the transaction commit mutex is held
    /// immediately before publication, never while the export is copying.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn purge_frontier_after_snapshot(
        &self,
        snapshot_lsn: Lsn,
    ) -> Result<Option<DurableLineageRecord>> {
        let state = self.lineage_state_lock.lock();
        Ok(self
            .list_lineage_records(&state)?
            .into_iter()
            .map(|(_, record)| record)
            .chain(self.list_purged_lineage_history(&state)?)
            .find(|record| {
                record.delete_obligation == DurableDeleteObligation::Purged
                    && record
                        .purge_frontier
                        .as_deref()
                        .and_then(|frontier| frontier.parse::<u64>().ok())
                        .is_some_and(|frontier| frontier > snapshot_lsn.0)
            }))
    }

    /// Hold the table-scoped erasure guard through the final transfer to the
    /// caller. Captured committed roots do not depend on later writer batches.
    pub(crate) fn publish_authoritative_read<T, E>(
        &self,
        scope: &crate::executor::ReadPublicationScope,
        map_error: impl Fn(Error) -> E,
        publish: impl FnOnce() -> std::result::Result<T, E>,
    ) -> std::result::Result<T, E> {
        let _publication = self
            .authorize_authoritative_read_publication(scope)
            .map_err(&map_error)?;
        publish()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn register_writer_cursor(
        &self,
        cursor: &Arc<crate::executor::BoundedCursorExecutionState>,
    ) -> Result<()> {
        let _publication = self
            .read_publication_gate
            .acquire_read(&cursor.publication)?;
        let mut state = self.read_publication_gate.state.lock();
        if state
            .writer_scope
            .as_ref()
            .is_some_and(|tables| publication_scopes_conflict(tables, &cursor.publication.tables))
        {
            drop(state);
            cursor.release_for_erasure();
            return Err(Error::ReadCancelled);
        }
        state.cursors.retain(|cursor| cursor.strong_count() != 0);
        state.cursors.push(Arc::downgrade(cursor));
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn authorize_authoritative_read_publication(
        &self,
        scope: &crate::executor::ReadPublicationScope,
    ) -> Result<AuthoritativeReadPublicationGuard> {
        // Exclude conflicting erasure through actual publication, without
        // serializing a captured answer behind unrelated writer construction.
        let publication = self.read_publication_gate.acquire_read(scope)?;
        scope.check_publication_budget()?;
        match &self.persistence {
            Some(persistence) => {
                let memory = scope
                    .read_memory
                    .clone()
                    .map(Ok)
                    .unwrap_or_else(|| self.declared_reader_memory())?;
                persistence.validate_erasure_frontiers(scope, memory)?;
            }
            None => self
                .lineage_state_lock
                .lock()
                .erasure_frontiers
                .validate(scope)?,
        }
        scope.check_publication_budget()?;
        Ok(publication)
    }

    pub(crate) fn capture_authoritative_read_publication(
        &self,
        tables: BTreeSet<String>,
        limits: ReadLimits,
        clock: Arc<dyn DeadlineClock>,
        cancellation: OwnerReadCancellation,
        started_ms: u64,
    ) -> crate::executor::ReadPublicationScope {
        crate::executor::ReadPublicationScope::bounded(
            self.tx_mgr.current_lsn(),
            tables,
            limits,
            clock,
            cancellation,
            started_ms,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn write_export_artifact(
        &self,
        dest: &Path,
        temp_path: &Path,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ExportReport> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // A missing destination directory fails here, before any file exists;
        // the directory is never created on the caller's behalf.
        // The destination maintains a whole committed proof of its own while
        // the snapshot is copied into it. That is this database's memory, so
        // the accountant is handed to the constructor: the format marker and
        // the initial content identity are the destination's FIRST artifact
        // writes, and attaching after they are on disk leaves that whole prefix
        // outside `MEMORY_LIMIT`.
        let artifact = RedbPersistence::create_with_accountant(
            temp_path,
            Some(crate::memory_accounting::AccountantHandle::clone(
                &self.finalization.accountant,
            )),
            crate::persistence::redb_writer_cache_bytes_for_limit(
                self.finalization.accountant.usage().limit,
            ),
        )
        .map_err(|err| export_io_error(dest, &err))?;
        let copied = self
            .copy_snapshot_into_artifact(&artifact, vector_copy_owner, read_memory.clone())
            .and_then(|report| {
                // Compact so `bytes_written` (and the artifact itself) reflect
                // content size, not allocator slack from the batched copy.
                artifact.compact(crate::memory_accounting::AccountantHandle::clone(
                    &self.finalization.accountant,
                ))?;
                Ok(report)
            });
        artifact.close();
        let mut report = copied?;
        let file = std::fs::File::open(temp_path).map_err(|err| export_io_error(dest, &err))?;
        file.sync_all().map_err(|err| export_io_error(dest, &err))?;
        report.bytes_written = file
            .metadata()
            .map_err(|err| export_io_error(dest, &err))?
            .len();
        Ok(report)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn copy_snapshot_into_artifact(
        &self,
        artifact: &RedbPersistence,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ExportReport> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // Single commit point: the committed watermark TxId, committed LSN,
        // and commit-index prefix are captured together under the commit
        // mutex. The mutex is released before any copying starts; commits
        // landing mid-copy carry visibility TxIds above the watermark and LSN
        // stamps above the snapshot LSN, so the filters below exclude them
        // entirely — a commit is in the artifact entirely or not at all.
        let (
            watermark,
            snapshot_lsn,
            commit_index,
            blob_snapshot,
            checkpoint_source,
            writer_root,
            recovery_floors,
            memory_erasure_frontiers,
        ) = self.with_commit_lock(|| -> Result<_> {
            let watermark = self.tx_mgr.current_tx_max();
            let snapshot_lsn = self.tx_mgr.current_lsn();
            let writer_root =
                writer_read::durable_root(self.tx_mgr.capture_published_read().handle)?;
            let recovery_floors = match &writer_root {
                Some(root) => RedbPersistence::writer_recovery_floors_in_read(root)?,
                None => crate::persistence::WriterRecoveryBounds {
                    identity_tx_floor: watermark.0,
                    source_lsn_floor: snapshot_lsn.0,
                    external_row_id_floor: self.relational_store.next_row_id().0.saturating_sub(1),
                    commit_count: 0,
                    graph_retirement_frontier: 0,
                    graph_occurrence_id_floor: 0,
                },
            };
            let commit_index = if writer_root.is_none() {
                Some(self.tx_mgr.read_commit_directory_cursor(
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Included(snapshot_lsn),
                    &contextdb_tx::ReadCommitContext::new(read_memory.clone(), &|| Ok(())),
                )?)
            } else {
                None
            };
            let blob_snapshot = self.blob_repository.begin_export_snapshot()?;
            let checkpoint_source = self
                .persistence
                .as_ref()
                .map(|persistence| {
                    persistence.checkpoint_read_source_with_read_memory(read_memory.clone())
                })
                .transpose()?;
            Ok((
                watermark,
                snapshot_lsn,
                commit_index,
                blob_snapshot,
                checkpoint_source,
                writer_root,
                recovery_floors,
                self.lineage_state_lock.lock().erasure_frontiers.clone(),
            ))
        })?;
        #[cfg(any(test, feature = "test-seams"))]
        self.export_after_capture_pause.maybe_pause();

        let _writer_read = self.enter_writer_read(writer_root.clone(), read_memory.clone());
        let snapshot = SnapshotId(watermark.0);
        let _intent = self.vector_historical_read_intent(snapshot, read_memory.clone());
        let _vector_capture =
            self.vector_read_scope(snapshot, read_memory.clone(), vector_copy_owner)?;
        let copied = self.with_vector_read_admission(vector_copy_owner, |vector_copy_owner| (|| -> Result<ExportReport> {
            let table_meta = match &writer_root {
                Some(root) => root
                    .captured_table_metadata()
                    .map(|(name, meta)| (name.to_owned(), meta.clone()))
                    .collect(),
                None => self.relational_store.table_meta.read().clone(),
            };
            for (name, meta) in &table_meta {
                artifact.flush_table_meta(name, meta)?;
            }
            if let Some(root) = &writer_root {
                root.copy_schema_into(
                    crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                    artifact,
                )?;
            }

            // Rows: streaming storage-layer copy, batched by position. Positions
            // are stable for the copy's duration: commits only append versions or
            // set tombstones in place, and pruning (the only remover) is deferred.
            let mut row_count = 0u64;
            let mut table_names: Vec<&String> = table_meta.keys().collect();
            table_names.sort();
            for name in table_names {
                let name = name.as_str();
                let meta = table_meta.get(name);
                if let Some(source) = &checkpoint_source {
                    source.verify_relational_version_manifests(
                        name, read_memory.clone(), &mut || {
                            if self.closed.load(Ordering::SeqCst)
                                || self.resource_closed.load(Ordering::SeqCst) {
                                return Err(closed_database_error());
                            }
                            self.refuse_after_interrupted_publication()
                        },
                    )?;
                }
                if let (Some(root), Some(source), Some(meta)) =
                    (&writer_root, &checkpoint_source, meta)
                {
                    use std::ops::Bound;
                    let mut cursor = root.cursor(
                        crate::persistence::writer_source::SourceWindowOwner::Read(
                            read_memory.clone(),
                        ),
                        &RedbPersistence::rel_table_name(name),
                        crate::persistence::writer_source::WriterCursorFamily::Bytes,
                        Bound::Unbounded,
                        Bound::Unbounded,
                        false,
                    )?;
                    while let Some(window) = cursor.next_row()? {
                        let _row_copy = contextdb_core::read_memory::ReadCredit::try_new(
                            read_memory.clone(),
                            window.value.estimated_bytes(),
                            "storage",
                            "export_row",
                            "Raise MEMORY_LIMIT to export this row.",
                        )?;
                        let Some(mut row) = export_row_at_snapshot(&window.value, watermark) else {
                            continue;
                        };
                        let (_, admission) = source.load_export_row_values_with_read_memory(
                            name,
                            meta,
                            std::slice::from_mut(&mut row),
                            read_memory.clone(),
                        )?;
                        let copied_row = (|| -> Result<()> {
                            source.overlay_export_row_f32_projections_with_admission(
                                name,
                                meta,
                                std::slice::from_mut(&mut row),
                                &admission,
                                true,
                            )?;
                            artifact.append_table_rows_batch(
                                name,
                                Some(meta),
                                std::slice::from_ref(&row),
                            )?;
                            if row.deleted_tx.is_none() {
                                row_count += 1;
                                if let Some((lsn, kind)) = root.sync_source(
                                    crate::persistence::writer_source::SourceWindowOwner::Read(
                                        read_memory.clone(),
                                    ),
                                    name,
                                    row.row_id,
                                )? {
                                    artifact.append_sync_source_provenance_batch(&[(
                                        name.to_owned(),
                                        row.row_id,
                                        lsn,
                                        kind,
                                    )])?;
                                }
                            }
                            Ok(())
                        })();
                        row.values.clear();
                        drop(admission);
                        copied_row?;
                    }
                    artifact.rebuild_relational_indexes_for_current_table(name, meta)?;
                    continue;
                }
                let mut offset = 0usize;
                loop {
                    let (mut batch, source_provenance) = {
                        let tables = self.relational_store.tables.read();
                        let Some(stored) = tables.get(name) else {
                            break;
                        };
                        if offset >= stored.len() {
                            break;
                        }
                        let end = stored.len().min(offset + EXPORT_BATCH_SIZE);
                        let mut batch = Vec::with_capacity(end - offset);
                        let mut source_provenance = Vec::new();
                        for row in &stored[offset..end] {
                            let Some(row) = export_row_at_snapshot(row, watermark) else {
                                continue;
                            };
                            if row.deleted_tx.is_none() {
                                row_count += 1;
                                if let Some(lsn) =
                                    self.relational_store.sync_source_lsn(name, row.row_id)
                                {
                                    let kind = match self
                                    .relational_store
                                    .sync_source_kind(name, row.row_id)
                                {
                                    Some(
                                        contextdb_relational::store::SyncSourceKind::AcceptedLocal,
                                    ) => 1,
                                    Some(
                                        contextdb_relational::store::SyncSourceKind::AcceptedLocalPending,
                                    ) => 2,
                                    _ => 0,
                                };
                                    source_provenance.push((
                                        name.to_string(),
                                        row.row_id,
                                        lsn,
                                        kind,
                                    ));
                                }
                            }
                            batch.push(row);
                        }
                        offset = end;
                        (batch, source_provenance)
                    };
                    let mut overlay_admission = None;
                    let mut appended = false;
                    if let (Some(source), Some(meta)) = (&checkpoint_source, meta) {
                        // The pinned source is the one place an exported body may
                        // come from: it is the snapshot this artifact claims. The
                        // stored values come back first, then the projection
                        // overlay authenticates them against that same root.
                        if self.relational_store.uses_disk_backed_committed_bodies()
                            && !batch.is_empty()
                        {
                            // The bodies come back in runs the memory budget
                            // admits. Each run is written to the artifact and let
                            // go before the next is loaded, so exporting a table
                            // of large bodies never asks the process to hold the
                            // whole batch of them at once.
                            let mut written = 0usize;
                            while written < batch.len() {
                                let (filled, admitted) = source.load_export_row_values_with_read_memory(
                                    name,
                                    meta,
                                    &mut batch[written..],
                                    read_memory.clone(),
                                )?;
                                if filled == 0 {
                                    return Err(Error::Other((
                                    "checkpoint export admitted no row body and could not                                      make progress"
                                        .to_string()).into()));
                                }
                                let end = written + filled;
                                let copied_run = (|| -> Result<()> {
                                    source.overlay_export_row_f32_projections_with_admission(
                                        name,
                                        meta,
                                        &mut batch[written..end],
                                        &admitted,
                                        true,
                                    )?;
                                    artifact.append_table_rows_batch(
                                        name,
                                        Some(meta),
                                        &batch[written..end],
                                    )?;
                                    Ok(())
                                })();
                                // The artifact holds them now, so the copies and
                                // the admission that covered them both go.
                                for row in &mut batch[written..end] {
                                    row.values = HashMap::new();
                                }
                                drop(admitted);
                                copied_run?;
                                written = end;
                            }
                            appended = true;
                        } else {
                            overlay_admission = Some(source.overlay_export_row_f32_projections_with_read_memory(name, meta, &mut batch, read_memory.clone())?);
                        }
                    }
                    let copied_batch = (|| -> Result<()> {
                        if !appended {
                            artifact.append_table_rows_batch(name, meta, &batch)?;
                        }
                        artifact.append_sync_source_provenance_batch(&source_provenance)?;
                        Ok(())
                    })();
                    for row in &mut batch {
                        row.values.clear();
                    }
                    drop(overlay_admission);
                    copied_batch?;
                }
                if let Some(meta) = meta {
                    artifact.rebuild_relational_indexes_for_current_table(name, meta)?;
                }
            }

            // Graph history validates each contribution against its commit.
            // Install the captured directory before copying those records.
            if let Some(root) = &writer_root {
                let mut cursor = root.commit_cursor(
                    crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                    Lsn(0),
                    snapshot_lsn,
                    false,
                )?;
                let mut batch = BTreeMap::new();
                while let Some((lsn, tx)) = cursor.next()? {
                    batch.insert(lsn, tx);
                    if batch.len() == EXPORT_BATCH_SIZE {
                        artifact.flush_commit_index_entries(&batch)?;
                        batch.clear();
                    }
                }
                artifact.flush_commit_index_entries(&batch)?;
            } else if let Some(mut cursor) = commit_index {
                let context = contextdb_tx::ReadCommitContext::new(read_memory.clone(), &|| Ok(()));
                let mut credit = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
                let mut batch = BTreeMap::new();
                while let Some((lsn, tx)) = cursor.next_entry(&context)? {
                    // Conservative per-insertion node funding covers internal
                    // splits as well as the retained pair and allocator header.
                    let node_bytes = 11 * std::mem::size_of::<(Lsn, TxId)>()
                        + 12 * std::mem::size_of::<usize>()
                        + 4 * std::mem::size_of::<usize>()
                        + crate::sync_types::held::ALLOCATION_BYTES;
                    credit.try_grow_for(2 * node_bytes, "reader", "export_commit_output",
                        "Reduce the export batch or raise the declared reader memory limit.")?;
                    batch.insert(lsn, tx);
                    if batch.len() == EXPORT_BATCH_SIZE {
                        artifact.flush_commit_index_entries(&batch)?;
                        batch.clear();
                        credit.try_shrink(credit.bytes())?;
                    }
                }
                artifact.flush_commit_index_entries(&batch)?;
                drop(batch);
                drop(credit);
            }

            // The same captured graph root serves image and writer exports.
            let mut edge_count = 0u64;
            let graph_root = match &writer_root {
                Some(root) => Some(crate::persistence::graph_read::EngineGraphReadRoot::Writer(
                    root.clone(),
                )),
                None => self.graph_read_root()?,
            };
            self.visit_graph_adjacency_at_root(
                graph_root.clone(),
                false,
                |entry| {
                    let _edge_copy = contextdb_core::read_memory::ReadCredit::try_new(
                        read_memory.clone(),
                        entry.estimated_bytes(),
                        "storage",
                        "export_edge",
                        "Raise MEMORY_LIMIT to export this edge.",
                    )?;
                    if let Some(edge) = export_edge_at_snapshot(entry, watermark) {
                        if edge.deleted_tx.is_none() {
                            edge_count += 1;
                        }
                        artifact.append_graph_edges_batch(std::slice::from_ref(&edge))?;
                    }
                    Ok(())
                },
                read_memory.clone(),
            )?;

            self.visit_row_graph_adjacency_at_root(graph_root, |entry, owner| {
                let _copy = contextdb_core::read_memory::ReadCredit::try_new(read_memory.clone(),
                    entry.estimated_bytes().saturating_add(owner.estimated_bytes()),
                    "storage", "export_row_edge", "Raise MEMORY_LIMIT to export this row witness.")?;
                if let Some(edge) = export_edge_at_snapshot(entry, watermark) {
                    if edge.deleted_tx.is_none() { edge_count += 1; }
                    artifact.append_row_graph_edge(&contextdb_tx::OwnedAdjEntry { owner: owner.clone(), entry: edge })?;
                }
                Ok(())
            }, read_memory.clone())?;

            // Vectors: complete per-column copy with the column's declared
            // quantization, so relational vector values re-hydrate on open and
            // HNSW rebuilds deterministically from the same stored data.
            let quantization = RedbPersistence::vector_quantization_map(&table_meta);
            let mut vector_count = 0u64;
            let mut vector_batch = Vec::new();
            let mut membership_batch = Vec::new();
            if let Some(root) = &writer_root {
                use std::ops::Bound;
                let mut memberships = root.cursor(
                    crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                    redb::TableHandle::name(&crate::persistence::VECTOR_PARTITION_MEMBERSHIP_TABLE),
                    crate::persistence::writer_source::WriterCursorFamily::Bytes,
                    Bound::Unbounded,
                    Bound::Unbounded,
                    false,
                )?;
                while let Some(placement) = memberships.next_membership()? {
                    // The source placement is authoritative. A shared body's
                    // stamps cannot reconstruct several disjoint intervals.
                    if placement.value.visible_from.0 > watermark.0 {
                        continue;
                    }
                    let body = root.export_vector_body(
                        crate::persistence::writer_source::SourceWindowOwner::Read(
                            read_memory.clone(),
                        ),
                        &placement.value,
                    )?;
                    if body.value.created_tx.0 > watermark.0 {
                        continue;
                    }
                    if placement.value.deleted_tx.is_none() {
                        vector_count += 1;
                    }
                    artifact.append_vector_entries_batch(
                        std::slice::from_ref(&body.value),
                        &quantization,
                    )?;
                    artifact.append_vector_partition_memberships_batch(std::slice::from_ref(
                        &placement.value,
                    ))?;
                }
            } else {
                let (entries, vector_image_owner) = self
                    .vector_store
                    .try_resident_partitioned_entries_with_membership(vector_copy_owner.scratch())?
                    .into_parts();
                for partitioned in entries {
                    let membership = partitioned.membership;
                    if membership.visible_from.0 > watermark.0
                        || partitioned.entry.created_tx.0 > watermark.0
                    {
                        continue;
                    }
                    let deleted_tx = membership.deleted_tx.filter(|tx| tx.0 <= watermark.0);
                    if deleted_tx.is_none() {
                        vector_count += 1;
                    }
                    membership_batch.push(VectorPartitionMembershipRecord {
                        index: partitioned.entry.index.clone(),
                        partition_key: partitioned.partition_key,
                        row_id: partitioned.entry.row_id,
                        row_created_tx: membership.owning_row_tx,
                        row_lsn: membership.row_lsn,
                        vector_created_tx: partitioned.entry.created_tx,
                        vector_lsn: partitioned.entry.lsn,
                        visible_from: membership.visible_from,
                        deleted_tx,
                    });
                    let mut canonical_body = partitioned.entry;
                    canonical_body.deleted_tx = partitioned.canonical_deleted_tx;
                    vector_batch.push(canonical_body);
                    if vector_batch.len() >= EXPORT_BATCH_SIZE {
                        artifact.append_vector_entries_batch(&vector_batch, &quantization)?;
                        artifact.append_vector_partition_memberships_batch(&membership_batch)?;
                        vector_batch.clear();
                        membership_batch.clear();
                    }
                }
                artifact.append_vector_entries_batch(&vector_batch, &quantization)?;
                artifact.append_vector_partition_memberships_batch(&membership_batch)?;
                drop(vector_batch);
                drop(membership_batch);
                drop(vector_image_owner);
            }

            // Change log: entries at or below the snapshot LSN, with per-LSN key
            // indices tracked across batches so multi-entry commits never collide.
            let mut offset = 0usize;
            let mut lsn_run: Option<(Lsn, usize)> = None;
            if let Some(root) = &writer_root {
                let mut cursor = root.change_log_cursor(
                    crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                    Lsn(0),
                    snapshot_lsn,
                )?;
                while let Some(window) = cursor.next_change()? {
                    let ordinal = usize::try_from(u64::from_be_bytes(
                        window.position()[8..16]
                            .try_into()
                            .expect("numeric occurrence key"),
                    ))
                    .map_err(|_| Error::Other("export change ordinal exceeds usize".into()))?;
                    artifact.append_change_log_entries_batch(&[(ordinal, window.value.clone())])?;
                }
            }
            loop {
                if writer_root.is_some() {
                    break;
                }
                let batch: Vec<(usize, ChangeLogEntry)> = {
                    let log = self.change_log.read();
                    if offset >= log.len() {
                        break;
                    }
                    let end = log.len().min(offset + EXPORT_BATCH_SIZE);
                    let mut batch = Vec::new();
                    for entry in &log[offset..end] {
                        let lsn = entry.lsn();
                        if lsn > snapshot_lsn {
                            continue;
                        }
                        let index = match lsn_run {
                            Some((run_lsn, next)) if run_lsn == lsn => next,
                            _ => 0,
                        };
                        lsn_run = Some((lsn, index + 1));
                        batch.push((index, entry.clone()));
                    }
                    offset = end;
                    batch
                };
                artifact.append_change_log_entries_batch(&batch)?;
            }

            if writer_root.is_none() {
                self.relational_store.visit_authored_history(|table, meta, row| {
                    if row.lsn <= snapshot_lsn { artifact.append_authored_row(table, meta, row)?; }
                    Ok(())
                })?;
            }
            if let Some(root) = &writer_root {
                root.copy_ddl_into(
                    crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                    artifact,
                    snapshot_lsn,
                )?;
                root.copy_authored_into(
                    crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()), artifact, snapshot_lsn,
                )?;
            } else {
                let ddl_entries: Vec<(Lsn, DdlChange)> = self
                    .ddl_log
                    .read()
                    .iter()
                    .filter(|(lsn, _)| *lsn <= snapshot_lsn)
                    .cloned()
                    .collect();
                artifact.append_ddl_log_entries(&ddl_entries)?;
            }

            // The owning Redb read transaction was captured with the relational
            // snapshot tuple. Copy only its canonical manifest-owned blob roles;
            // staging and orphan chunks are never backup-addressable.
            blob_snapshot.copy_canonical_into(artifact)?;

            match &self.persistence {
                Some(persistence) => {
                    // File-backed source: raw copy of the whole config table
                    // (tenant watermark keys are dynamic), the durable sink
                    // queues, and the audit tables.
                    let root = writer_root.as_ref().ok_or_else(|| {
                        Error::Other("file export requires its captured writer root".into())
                    })?;
                    root.copy_erasure_frontiers_into(artifact, read_memory.clone())?;
                    root.copy_config_into(
                        crate::persistence::writer_source::SourceWindowOwner::Read(
                            read_memory.clone(),
                        ),
                        artifact,
                    )?;
                    for sink in persistence.sink_queue_names()? {
                        let entries = persistence.dump_sink_queue_raw(&sink)?;
                        for chunk in entries.chunks(EXPORT_BATCH_SIZE) {
                            artifact.append_sink_queue_raw(&sink, chunk)?;
                        }
                    }
                    let trigger_audits = persistence.dump_trigger_audit_raw()?;
                    for chunk in trigger_audits.chunks(EXPORT_BATCH_SIZE) {
                        artifact.append_trigger_audit_raw(chunk)?;
                    }
                    let trigger_audit_stamps = persistence.dump_trigger_audit_stamps_raw()?;
                    for chunk in trigger_audit_stamps.chunks(EXPORT_BATCH_SIZE) {
                        artifact.append_trigger_audit_stamps_raw(chunk)?;
                    }
                    let sink_audits = persistence.dump_sink_audit_raw()?;
                    for chunk in sink_audits.chunks(EXPORT_BATCH_SIZE) {
                        artifact.append_sink_audit_raw(chunk)?;
                    }
                }
                None => {
                    // In-memory source: persisted-only operational config does
                    // not exist here and is not exported, but DDL-declared state
                    // is database state — cron schedules, event-bus definitions,
                    // and trigger declarations live only in memory and export
                    // exactly as file-backed DDL persistence would have written
                    // them.
                    memory_erasure_frontiers.copy_into(artifact, read_memory.clone())?;
                    let mut config_values = self.export_cron_config_values()?;
                    config_values.extend(self.export_event_bus_config_values()?);
                    config_values.extend(self.export_trigger_config_values()?);
                    artifact.flush_encoded_config_values(config_values)?;
                    let custody = self
                        .custody_metadata
                        .lock()
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect::<Vec<_>>();
                    artifact.flush_encoded_config_values(
                        custody
                            .iter()
                            .map(|(k, v)| (k.as_str(), v.clone()))
                            .collect(),
                    )?;
                }
            }

            artifact.flush_writer_recovery_floors(recovery_floors)?;
            Ok(ExportReport {
                snapshot_lsn,
                rows: row_count,
                edges: edge_count,
                vectors: vector_count,
                bytes_written: 0,
            })
        })());
        match copied {
            Ok(report) => Ok(report),
            Err(error) => self.with_commit_lock(|| {
                if let Some(record) = self.purge_frontier_after_snapshot(snapshot_lsn)? {
                    let frontier = record
                        .purge_frontier
                        .as_deref()
                        .and_then(|frontier| frontier.parse::<u64>().ok())
                        .expect("permanent purge frontier was checked");
                    return Err(Error::PurgeExportSnapshotFence {
                        table: record.table,
                        key: record.natural_key.pairs(),
                        lineage_root: record.lineage_root,
                        frontier: Lsn(frontier),
                        snapshot_lsn,
                    });
                }
                Err(error)
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn close(&self) -> Result<()> {
        let current_thread = crate::owned_worker::current_thread_id();
        let mut progress = self.close_progress.lock();
        loop {
            match *progress {
                CloseProgress::Complete => return Ok(()),
                CloseProgress::Running(owner) => {
                    // The owner may be joining this dispatcher. Waiting here
                    // would make the join wait for itself, so the callback
                    // accepts the already-running shutdown and exits.
                    if owner == current_thread || self.event_bus.callback_active_on_current_thread()
                    {
                        return Ok(());
                    }
                    #[cfg(any(test, feature = "test-seams"))]
                    if let Some(observer) = self.close_follower_observer.lock().clone() {
                        observer();
                    }
                    self.close_changed.wait(&mut progress);
                }
                CloseProgress::Idle => {
                    *progress = CloseProgress::Running(current_thread);
                    break;
                }
            }
        }
        drop(progress);
        let mut owner = CloseOwnerGuard {
            progress: &self.close_progress,
            changed: &self.close_changed,
            complete: false,
            deferred: false,
        };
        let result = self.close_as_owner();
        owner.deferred = self.attach_deferred_close_completion(result.is_ok());
        if result.is_ok() {
            owner.complete();
        }
        result
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn __set_close_follower_observer_for_test(
        &self,
        observer: Option<Arc<dyn Fn() + Send + Sync>>,
    ) {
        *self.close_follower_observer.lock() = observer;
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn close_as_owner(&self) -> Result<()> {
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        let db_id = self.id;
        let _close_waiter = if Self::callback_active_process_wide() {
            Some(TriggerCloseWaiterGuard::new(&self.trigger))
        } else {
            None
        };
        if Self::callback_active_process_wide() {
            self.assert_callback_reentry_allowed()?;
        }
        if DB_OPERATION_STACK.with(|stack| stack.borrow().contains(&db_id)) {
            return Err(Error::Other(
                ("cannot close database from inside an active operation".to_string()).into(),
            ));
        }
        self.assert_public_tx_control_cross_thread_allowed_for_surface("close")?;
        let already_closed = {
            let _operation_barrier = self.operation_gate.write();
            let already_closed = self.closed.swap(true, Ordering::SeqCst);
            if !already_closed
                && self.resource_owner
                && matches!(&self.tx_mgr, DatabaseTransactionHost::Legacy(_))
            {
                self.pending_local_schema_stages.lock().clear();
                self.local_schema_stages.lock().clear();
            }
            already_closed
        };
        if already_closed && self.resource_closed.load(Ordering::SeqCst) {
            return Ok(());
        }
        if !already_closed {
            let tx = self.session_tx.lock().take();
            if let Some(tx) = tx {
                if let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr {
                    match handle.with_host(|host| host.rollback_terminal(tx)) {
                        Ok(terminal) => {
                            self.remove_original_terminal_stages(terminal.identity().original_tx);
                            drop(terminal);
                        }
                        Err(Error::TxNotFound(_)) => self.remove_original_terminal_stages(tx),
                        Err(error) => return Err(error),
                    }
                } else {
                    if self.private_received_rows_hold_delete_invoices() {
                        if let Ok(ws) = self.tx_mgr.rollback_write_set(tx) {
                            self.dispose_private_abandoned_write_set(ws, &mut vector_copy_owner)?;
                        }
                    } else if let Ok(ws) = self.tx_mgr.cloned_write_set(tx) {
                        self.release_insert_allocations(&ws, &mut vector_copy_owner)?;
                    }
                    self.pending_event_bus_ddl.lock().remove(&tx);
                    self.pending_local_schema_stages.lock().remove(&tx);
                    self.pending_commit_metadata.lock().remove(&tx);
                    let _ = self.tx_mgr.rollback(tx);
                }
            }
            // A resource-owner close can also follow direct `begin()` /
            // `execute_in_tx()` use, whose TxId is not stored in `session_tx`.
            // No public operation can still be committing past the close barrier,
            // so discard every private staged image owned by this shared database
            // -- and hand back what each abandoned transaction's inserts were
            // holding. Without this a transaction opened with `begin()` and never
            // committed left its whole admission charged against a limit that
            // nothing was holding memory for any more.
            if self.resource_owner {
                if matches!(&self.tx_mgr, DatabaseTransactionHost::Legacy(_)) {
                    self.pending_local_schema_stages.lock().clear();
                    self.local_schema_stages.lock().clear();
                }
                if let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr {
                    for terminal in handle.with_host(EngineDatabaseTransactions::drain_terminals) {
                        self.remove_original_terminal_stages(terminal.identity().original_tx);
                        drop(terminal);
                    }
                    self.pending_local_schema_stages.lock().clear();
                    self.local_schema_stages.lock().clear();
                } else {
                    for write_set in self.tx_mgr.drain_active_write_sets() {
                        if self.private_received_rows_hold_delete_invoices() {
                            self.dispose_private_abandoned_write_set(
                                write_set,
                                &mut vector_copy_owner,
                            )?;
                        } else {
                            self.release_insert_allocations(&write_set, &mut vector_copy_owner)?;
                        }
                    }
                }
                // The change log and the commit index go with the store. They are
                // charged as they grow, and a reopen charges what it rehydrates,
                // so a close that did not hand them back charged the same log
                // twice against an accountant an embedder reuses across opens.
                self.finalization
                    .accountant
                    .release(self.durable_writer_retained_bytes());
            }
            self.stop_cron_tickler();
            self.stop_pruning_thread();
        }
        let event_bus_shutdown = self.stop_event_bus_threads();
        #[cfg(any(test, feature = "test-seams"))]
        self.observe_quiescent_stop_for_test(
            crate::read_session::QuiescentStopPhase::WritesStopped,
        );
        // Settle eligible deferred frees while writes are still legal. Keep a
        // failure to report after releasing the owner resources; handoff must
        // still publish the final committed root and stop later writes.
        if !already_closed {
            // A prior attempt has already crossed the handoff boundary; a
            // retry may only finish draining accepted owner reads. Retain a
            // failed drain across that wait and any deadline refusal.
            if let Err(error) = self.drain_storage_before_handoff() {
                *self.close_error.lock() = Some(error);
            }
        }
        // No writer or reclaimer can move the committed root after this
        // publication. Fresh readers can select the direct file while the
        // accepted owner requests finish against their retained image.
        let handoff_result = self.publish_quiescent_owner();
        let quiescent = handoff_result.is_ok();
        #[cfg(any(test, feature = "test-seams"))]
        if handoff_result.is_ok() {
            self.observe_quiescent_stop_for_test(
                crate::read_session::QuiescentStopPhase::Published,
            );
        }
        if let Err(error) = handoff_result {
            // Cleanup has precedence when both phases fail. Either cause
            // remains owned through a drain timeout and its later retry.
            self.close_error.lock().get_or_insert(error);
        }
        self.drain_owner_reads()?;
        #[cfg(any(test, feature = "test-seams"))]
        self.observe_quiescent_stop_for_test(
            crate::read_session::QuiescentStopPhase::AcceptedDrained,
        );
        // A quiescent, drained owner closes as the last reader: it still holds
        // its data file and companion, so no writer can purge while it removes
        // the reader registries nothing registers in any more.
        if quiescent
            && self.resource_owner
            && let Some(persistence) = &self.persistence
        {
            persistence.remove_idle_reader_registries();
        }
        if self.resource_owner {
            // Sample the final logical root after accepted reads drain, while
            // the durable commit directory and its owner are still available.
            #[cfg(any(test, feature = "test-seams"))]
            self.emit_writer_memory_ownership_receipt_for_test("close");
            self.resource_closed.store(true, Ordering::SeqCst);
            self.tx_mgr.release_publication_handle();
            self.blob_repository.close();
            self.subscriptions.lock().subscribers.clear();
            if !event_bus_shutdown.deferred_resource_cleanup() {
                if let Some(persistence) = &self.persistence {
                    persistence.close();
                }
                self.release_open_registry();
            }
        }
        #[cfg(feature = "test-seams")]
        let _released_for_test =
            (self.resource_owner && !event_bus_shutdown.deferred_resource_cleanup()).then(|| {
                ResourceReleaseCompletionForTest(Arc::clone(
                    &self.finalization.release_completion_for_test,
                ))
            });
        let plugin_result = if self.resource_owner {
            self.plugin.on_close()
        } else {
            Ok(())
        };
        let close_result = self.close_error.lock().take().map_or(Ok(()), Err);
        close_result.and(plugin_result)
    }

    pub(super) fn assert_open_operation(&self) -> DatabaseOperationGuard<'_> {
        match self.open_operation() {
            Ok(guard) => guard,
            Err(error) => panic!("database handle refuses service: {error}"),
        }
    }

    /// Prepared memory state publishes infallibly after the commit, so an unwind between the
    /// durable commit and the end of publication is an engine bug with no
    /// correct partial publication to patch back to. The handle refuses every
    /// later operation; reopening recovers the complete committed state from
    /// the durable image.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn refuse_after_interrupted_publication(&self) -> Result<()> {
        if self.publication_interrupted.load(Ordering::SeqCst)
            || self.tx_mgr.store().publication_interrupted()
        {
            return Err(Error::PublicationInterrupted);
        }
        Ok(())
    }

    pub(crate) fn capture_read_schema(&self) -> SchemaPublicationGuard<'_> {
        self.enter_schema_publication_gate(false)
    }

    /// Select the read or write side for COMMIT without racing a concurrent
    /// final schema-staging statement on the same transaction. Once the read
    /// side is held, no schema statement can publish a new pending stage; if
    /// one landed between the first observation and lock acquisition, drop
    /// the read side and retry as a writer instead of upgrading in place.
    pub(super) fn enter_commit_schema_publication_gate(
        &self,
        tx: TxId,
    ) -> SchemaPublicationGuard<'_> {
        loop {
            let publishes_local_schema = self.pending_local_schema_stages.lock().contains_key(&tx);
            let guard = self.enter_schema_publication_gate(publishes_local_schema);
            if publishes_local_schema || !self.pending_local_schema_stages.lock().contains_key(&tx)
            {
                return guard;
            }
            drop(guard);
        }
    }

    /// Take the received-schema write side without parking behind a callback
    /// that may be waiting for this very `apply_changes` call to return.
    /// Timed lock acquisition is only a wake-up cadence: callback state, not
    /// elapsed time, decides the result.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn enter_apply_changes_schema_publication_gate(
        &self,
        surface: &'static str,
    ) -> Result<SchemaPublicationGuard<'_>> {
        // Preserve the public operation contract before changing lock order:
        // a closed handle wins over callback state, cron contention refuses
        // immediately, and same-database trigger contention waits (including
        // its deterministic deadlock guard and telemetry). Drop the read-side
        // operation lease before requesting the schema write side so received
        // DDL never attempts a read-to-write upgrade.
        drop(self.open_operation_after_public_tx_control_wait(surface)?);
        let gate_id = Arc::as_ptr(&self.schema_publication_gate) as usize;
        loop {
            if self.closed.load(Ordering::SeqCst) || self.resource_closed.load(Ordering::SeqCst) {
                return Err(closed_database_error());
            }
            self.refuse_after_interrupted_publication()?;
            self.assert_callback_reentry_allowed()?;
            if self.cron.callback_active_on_other_thread() {
                return Err(Error::CallbackActiveCrossThread {
                    kind: CallbackKind::Cron,
                });
            }
            if matches!(
                self.trigger.callback_contention_for_tx_control(
                    self.owner_thread,
                    self.cron.has_schedules(),
                ),
                TriggerContention::SameDb
            ) {
                self.wait_for_same_db_trigger_callback_idle(surface)?;
                continue;
            }
            if let Some(write) = self
                .schema_publication_gate
                .try_write_for(Duration::from_millis(1))
            {
                SCHEMA_PUBLICATION_STACK.with(|stack| stack.borrow_mut().push(gate_id));
                return Ok(SchemaPublicationGuard {
                    gate_id,
                    _read: None,
                    _write: Some(write),
                });
            }
        }
    }

    /// Hold the existing schema-publication read side while outbound sync
    /// prepares extraction-derived evidence. The orchestration layer releases
    /// this lease before serialization and all network awaits.
    #[cfg(feature = "sync-orchestration")]
    pub(crate) fn enter_outbound_sync_schema_read(&self) -> OutboundSyncSchemaReadGuard<'_> {
        OutboundSyncSchemaReadGuard {
            _inner: self.enter_schema_publication_gate(false),
        }
    }

    pub(super) fn statement_changes_table_schema(stmt: &Statement) -> bool {
        matches!(
            stmt,
            Statement::CreateTable(_)
                | Statement::DropTable(_)
                | Statement::AlterTable(_)
                | Statement::CreateIndex(_)
                | Statement::DropIndex(_)
                | Statement::CreateTrigger { .. }
                | Statement::DropTrigger { .. }
                | Statement::CreateEventType { .. }
                | Statement::CreateSink { .. }
                | Statement::CreateRoute { .. }
                | Statement::DropRoute { .. }
        )
    }

    pub(super) fn release_open_registry(&self) {
        if let Some(path) = self.open_registry_path.lock().take() {
            release_open_registry_path(&path);
        }
    }

    /// File-backed database with custom plugin.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn open_with_plugin(
        path: impl AsRef<Path>,
        plugin: Arc<dyn DatabasePlugin>,
    ) -> Result<Self> {
        Self::open_with_options(
            path,
            DatabaseOpenOptions {
                plugin,
                ..DatabaseOpenOptions::default()
            },
        )
    }

    sync_test_seam! {
    /// Full constructor with an internal accountant. Test-only outside this
    /// crate; production callers use the numeric startup-limit functions.
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    #[cfg_attr(not(feature = "test-seams"), allow(dead_code, reason = "KEEP-SEAM: live only under the test-seams feature; used from declared_memory_limit_bounds_a_writer.rs"))]
    fn open_with_config(
        path: impl AsRef<Path>,
        plugin: Arc<dyn DatabasePlugin>,
        accountant: Arc<MemoryAccountant>,
    ) -> Result<Self> {
        Self::open_with_config_and_disk_limit(path, plugin, accountant, None)
    }
    }

    sync_test_seam! {
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    #[cfg_attr(not(feature = "test-seams"), allow(dead_code, reason = "KEEP-SEAM: live only under the test-seams feature; used from durable_public_api_surface_tests.rs"))]
    fn open_with_config_and_disk_limit(
        path: impl AsRef<Path>,
        plugin: Arc<dyn DatabasePlugin>,
        accountant: Arc<MemoryAccountant>,
        startup_disk_limit: Option<u64>,
    ) -> Result<Self> {
        let path = path.as_ref();
        if path.as_os_str() == ":memory:" {
            return Self::open_memory_with_plugin_and_accountant(plugin, accountant);
        }
        let declared_read_memory = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(
            OwnerReadConfig::default().limits.limits.memory,
        )?;
        let db = Self::open_loaded(
            path,
            plugin,
            WriterBudget::from_accountant(accountant)?,
            startup_disk_limit,
            OpenDisposition::CreateIfMissing,
            None,
            declared_read_memory.clone(),
            declared_read_memory.child()?,
            DatabaseHostConstruction::Legacy,
        )?;
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_probe::note_plugin_start();
        db.plugin.on_open()?;
        db.start_cron_tickler_if_schedules_present()?;
        db.load_retention_sync_peer();
        db.reconcile_maintenance_thread();
        Ok(db)
    }
    }

    sync_test_seam! {
    #[cfg(any(test, feature = "test-seams"))]
    /// In-memory database with budget.
    fn open_memory_with_accountant(accountant: Arc<MemoryAccountant>) -> Self {
        Self::open_memory_internal(Arc::new(CorePlugin), WriterBudget::from_accountant(accountant).expect("writer budget registration failed"), crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(OwnerReadConfig::default().limits.limits.memory).expect("reader root construction failed"))
            .expect("failed to open in-memory database with accountant")
    }
    }

    sync_test_seam! {
    /// Access the memory accountant.
    fn accountant(&self) -> &MemoryAccountant {
        &self.finalization.accountant
    }
    }

    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from stable_funder_identity_proof_tests.rs"
        )
    )]
    pub(super) fn visit_source_copy_credits(&self, visit: &mut dyn FnMut(usize, usize, bool)) {
        self.vector_store
            .visit_retained_copy_credits(self.finalization.vector_accountant.as_ref(), visit);
        self.visit_writer_output_copy_credits(visit);
    }

    /// Sample at a quiescent boundary. Identity scratch belongs to this
    /// reporting operation's declared reader child and never to its writer.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from content_size_contract_tests.rs"
        )
    )]
    pub(crate) fn vector_memory_ownership_receipt(&self) -> Result<VectorMemoryOwnershipReceipt> {
        let sampled = self
            .vector_store
            .memory_ownership_snapshot_for_funder(self.finalization.vector_accountant.as_ref());
        let (source_metadata, reader_source_metadata) = {
            let mut count = 0usize;
            self.visit_source_copy_credits(&mut |_, _, _| count += 1);
            let bytes = count
                .checked_mul(std::mem::size_of::<(usize, usize, bool)>())
                .ok_or_else(|| Error::Other("memory report identity scratch overflow".into()))?;
            let _credit = contextdb_core::read_memory::ReadCredit::try_new(
                self.declared_reader_memory()?,
                bytes,
                "memory_report",
                "source_metadata_identities",
                "Reduce report metadata or raise the declared reader memory limit.",
            )?;
            // Exact fixed slice: no capacity growth, Arc clone, or persistent registry.
            // Declaring the credit first makes the slice drop before its reservation.
            let mut identities = vec![(0usize, 0usize, false); count].into_boxed_slice();
            let mut filled = 0usize;
            let mut changed = false;
            self.visit_source_copy_credits(&mut |identity, bytes, funded_by_caller| {
                if let Some(slot) = identities.get_mut(filled) {
                    *slot = (identity, bytes, funded_by_caller);
                    filled += 1;
                } else {
                    changed = true;
                }
            });
            if changed {
                return Err(Error::Other(
                    "memory report metadata changed during observation".into(),
                ));
            }
            let identities = &mut identities[..filled];
            identities.sort_unstable_by_key(|&(identity, _, _)| identity);
            let mut previous = None;
            let mut source_metadata = 0usize;
            let mut reader_source_metadata = 0usize;
            for &(identity, bytes, funded_by_caller) in identities.iter() {
                if previous != Some(identity) {
                    if funded_by_caller {
                        source_metadata = source_metadata.saturating_add(bytes);
                    } else {
                        reader_source_metadata = reader_source_metadata.saturating_add(bytes);
                    }
                    previous = Some(identity);
                }
            }
            (source_metadata, reader_source_metadata)
        };
        // The reporting child and its exact scratch credit have dropped before
        // sampling standing controls, so this report does not count itself.
        let (reader_child_controls, reader_parent_total) =
            self.declared_read_memory.standing_control_bytes();
        let reader_last_query_trace = self
            .last_vector_search_trace
            .read()
            .as_ref()
            .map_or(0, |trace| trace.retained_credit_bytes_for_test());
        #[cfg(any(test, feature = "test-seams"))]
        {
            let hook = WRITER_COLLECTION_HOOK.with(|slot| {
                let identity = Arc::as_ptr(&self.pruning_guard) as usize;
                let mut slot = slot.borrow_mut();
                if slot.as_ref().is_some_and(|(owner, _)| *owner == identity) {
                    slot.take().map(|(_, hook)| hook)
                } else {
                    None
                }
            });
            if let Some(hook) = hook {
                hook();
            }
        }
        let (accountant_total, process_native_bytes, combined_writer_envelope) =
            self.finalization.accountant.writer_envelope_usage()?;
        let process_native_owner_identity = if process_native_bytes == 0 {
            0
        } else {
            crate::memory_accounting::resident_insertion_runtime_snapshot()
                .map_or(0, |(_, _, identity)| identity)
        };
        let retained_total = sampled
            .raw_vector_bodies
            .saturating_add(sampled.raw_reverse_directory)
            .saturating_add(sampled.partition_vectors)
            .saturating_add(sampled.vector_partition_controls)
            .saturating_add(sampled.base_graph)
            .saturating_add(sampled.change_graph)
            .saturating_add(sampled.mutable_tail)
            .saturating_add(sampled.typed_partition_keys)
            .saturating_add(sampled.retired_pinned_graph_bytes)
            .saturating_add(source_metadata);
        let non_vector_total = accountant_total
            .saturating_sub(retained_total)
            .saturating_sub(sampled.temporary_workspace);
        Ok(VectorMemoryOwnershipReceipt {
            source_metadata,
            reader_source_metadata,
            reader_last_query_trace,
            reader_child_controls: reader_child_controls as usize,
            reader_root_control: self.declared_read_memory.root_control_bytes(),
            reader_child_payloads: self.declared_read_memory.child_payload_bytes() as usize,
            reader_acl_cache: self.declared_read_memory.acl_cache_bytes() as usize,
            reader_parent_total: reader_parent_total as usize,
            raw_vector_bodies: sampled.raw_vector_bodies,
            raw_reverse_directory: sampled.raw_reverse_directory,
            partition_vectors: sampled.partition_vectors,
            vector_partition_controls: sampled.vector_partition_controls,
            base_graph: sampled.base_graph,
            change_graph: sampled.change_graph,
            mutable_tail: sampled.mutable_tail,
            typed_partition_keys: sampled.typed_partition_keys,
            retired_pinned_graph_bytes: sampled.retired_pinned_graph_bytes,
            temporary_workspace: sampled.temporary_workspace,
            retained_total,
            non_vector_total,
            accountant_total,
            process_native_bytes,
            process_native_owner_identity,
            combined_writer_envelope,
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn vector_memory_ownership_receipt_for_test(&self) -> VectorMemoryOwnershipReceiptForTest {
        let receipt = self
            .vector_memory_ownership_receipt()
            .expect("declared reader memory must admit the ownership report");
        VectorMemoryOwnershipReceiptForTest {
            source_metadata: receipt.source_metadata,
            reader_source_metadata: receipt.reader_source_metadata,
            reader_last_query_trace: receipt.reader_last_query_trace,
            reader_child_controls: receipt.reader_child_controls,
            reader_root_control: receipt.reader_root_control,
            reader_child_payloads: receipt.reader_child_payloads,
            reader_acl_cache: receipt.reader_acl_cache,
            reader_parent_total: receipt.reader_parent_total,
            raw_vector_bodies: receipt.raw_vector_bodies,
            raw_reverse_directory: receipt.raw_reverse_directory,
            partition_vectors: receipt.partition_vectors,
            vector_partition_controls: receipt.vector_partition_controls,
            base_graph: receipt.base_graph,
            change_graph: receipt.change_graph,
            mutable_tail: receipt.mutable_tail,
            typed_partition_keys: receipt.typed_partition_keys,
            retired_pinned_graph_bytes: receipt.retired_pinned_graph_bytes,
            temporary_workspace: receipt.temporary_workspace,
            retained_total: receipt.retained_total,
            non_vector_total: receipt.non_vector_total,
            accountant_total: receipt.accountant_total,
            process_native_bytes: receipt.process_native_bytes,
            process_native_owner_identity: receipt.process_native_owner_identity,
            combined_writer_envelope: receipt.combined_writer_envelope,
        }
    }

    /// Observe retained allocations and their charges at one commit boundary.
    /// Reporting scratch uses a declared reader child; no writer admission,
    /// persistence read, or residency mutation occurs here.
    /// Signal once when a real maintenance lock acquisition has to wait.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn with_writer_memory_ownership_contention_observer_for_test<R>(
        &self,
        observer: Arc<dyn Fn() + Send + Sync>,
        operation: impl FnOnce() -> R,
    ) -> R {
        struct Remove {
            identity: usize,
            observer: Arc<dyn Fn() + Send + Sync>,
        }
        impl Drop for Remove {
            fn drop(&mut self) {
                WRITER_RECEIPT_CONTENTION_OBSERVERS
                    .lock()
                    .observers
                    .retain(|entry| {
                        !(entry.identity == self.identity
                            && Arc::ptr_eq(&entry.observer, &self.observer))
                    });
            }
        }
        let identity = Arc::as_ptr(&self.pruning_guard) as usize;
        // Wrap each registration to give nested registrations distinct identities,
        // even when the caller reuses its Arc callback.
        let observer: Arc<dyn Fn() + Send + Sync> = Arc::new(move || observer());
        let _remove = Remove {
            identity,
            observer: observer.clone(),
        };
        WRITER_RECEIPT_CONTENTION_OBSERVERS.lock().observers.push(
            WriterReceiptContentionObserver {
                identity,
                observer,
                fired: false,
            },
        );
        operation()
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn with_writer_memory_ownership_collection_hook_for_test<R>(
        &self,
        hook: impl FnOnce() + 'static,
        sample: impl FnOnce() -> R,
    ) -> R {
        struct Restore(Option<(usize, Box<dyn FnOnce()>)>);
        impl Drop for Restore {
            fn drop(&mut self) {
                let displaced = WRITER_COLLECTION_HOOK.with(|slot| slot.replace(self.0.take()));
                drop(displaced);
            }
        }
        let _restore = Restore(WRITER_COLLECTION_HOOK.with(|slot| {
            slot.replace(Some((
                Arc::as_ptr(&self.pruning_guard) as usize,
                Box::new(hook),
            )))
        }));
        sample()
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn writer_memory_ownership_receipt_for_test(&self) -> WriterMemoryOwnershipReceiptForTest {
        self.with_atomic_writer_memory_ownership_receipt_for_test(|receipt| receipt)
    }

    #[cfg(any(test, feature = "test-seams"))]
    /// Sample every named owner, its accountant, schema history and published
    /// LSN under one commit and maintenance boundary. The callback remains
    /// inside that boundary. A maintenance emitter reuses its actual held lock.
    #[doc(hidden)]
    pub fn with_atomic_writer_memory_ownership_receipt_for_test<R>(
        &self,
        observe: impl FnOnce(WriterMemoryOwnershipReceiptForTest) -> R,
    ) -> R {
        let _maintenance = (!MaintenanceReceiptGuard::held_for(&self.pruning_guard))
            .then(|| MaintenanceReceiptGuard::lock(&self.pruning_guard));
        let _sample_held =
            WriterReceiptSampleHeld::enter(Arc::as_ptr(&self.pruning_guard) as usize);
        self.with_commit_lock(|| {
            let mut receipt = WriterMemoryOwnershipReceiptForTest {
                vector: self.vector_memory_ownership_receipt_for_test(),
                relational_inventory: self.relational_store.retained_structure_bytes(),
                commit_source_reference_charge: self.commit_source_reference_charge,
                snapshot_pin_capacity_charge: self
                    .snapshot_registry
                    .pin_slots
                    .lock()
                    .as_ref()
                    .map_or(0, |pool| pool.slots.saturating_mul(pool.slot_bytes)),
                ..WriterMemoryOwnershipReceiptForTest::default()
            };
            let metadata = self.relational_store.table_meta.read();
            receipt.table_metadata = metadata
                .values()
                .map(|meta| retained_table_metadata_bytes(&self.relational_store, meta))
                .sum();
            for (table, rows) in self.relational_store.tables.read().iter() {
                let meta = metadata.get(table);
                for row in rows {
                    if row.deleted_tx.is_some() {
                        receipt.historical_row_versions += 1;
                        receipt.historical_row_priced_bytes +=
                            retained_row_bytes_with_optional_meta(table, row, meta);
                    } else {
                        receipt.current_row_versions += 1;
                        let by_table = receipt
                            .current_rows_by_table
                            .entry(table.clone())
                            .or_default();
                        by_table.versions += 1;
                        if let Some(meta) = meta {
                            let values = estimate_row_bytes_for_meta(&row.values, meta, false)
                                .saturating_mul(ROW_VALUE_RETENTIONS);
                            let side_maps = contextdb_core::retained_row_side_map_bytes(table);
                            let postings = contextdb_core::retained_index_posting_bytes_for_meta(
                                &row.values,
                                meta,
                            );
                            let projections = f32_projection_payload_bytes(&row.values, meta);
                            receipt.current_row_values += values;
                            receipt.current_row_side_maps += side_maps;
                            receipt.current_row_index_postings += postings;
                            receipt.current_row_f32_projections += projections;
                            by_table.values += values;
                            by_table.side_maps += side_maps;
                            by_table.index_postings += postings;
                            by_table.f32_projections += projections;
                        } else {
                            let values = fallback_retained_row_bytes(table, row);
                            receipt.current_row_values += values;
                            by_table.values += values;
                        }
                    }
                }
            }
            self.tx_mgr.with_active_write_sets(|active| {
                receipt.active_transactions = active.len();
                for ws in active.values() {
                    receipt.staged_row_versions += ws.relational_inserts.len();
                    receipt.staged_vector_owner_controls +=
                        ws.ordered_staged_rows.vector_capability_retained_bytes();
                    receipt.staged_index_postings += ws.ordered_staged_rows.retained_bytes();
                    for (table, row) in &ws.relational_inserts {
                        receipt.staged_row_admissions += ws
                            .admitted_bytes_for_staged_row(table, row.row_id)
                            .unwrap_or_else(|| {
                                retained_row_bytes_with_optional_meta(
                                    table,
                                    row,
                                    metadata.get(table),
                                )
                            });
                    }
                    for vector in &ws.vector_inserts {
                        receipt.staged_vector_admissions += self
                            .vector_store
                            .index_quantization_storage_bytes_for_test(
                                &vector.index,
                                vector.vector.len(),
                            )
                            .expect("staged vector index retains its declared layout");
                    }
                }
            });
            drop(metadata);
            receipt.graph_entries = self
                .graph_store
                .forward_adj
                .read()
                .values()
                .flatten()
                .map(|entry| entry.estimated_bytes())
                .sum();
            for entry in self.change_log.read().iter() {
                let kind = match entry {
                    ChangeLogEntry::RowInsert { .. } => 0,
                    ChangeLogEntry::RowDelete { .. } => 1,
                    ChangeLogEntry::VectorInsert { .. } => 2,
                    ChangeLogEntry::VectorDelete { .. } => 3,
                    ChangeLogEntry::EdgeInsert { .. } => 4,
                    ChangeLogEntry::EdgeDelete { .. } => 5,
                };
                receipt.change_log_entries[kind] += 1;
                receipt.change_log_charges[kind] +=
                    crate::composite_store::change_log_entry_retained_bytes(entry);
            }
            receipt.commit_index_entries = self
                .tx_mgr
                .try_commit_index_census()
                .expect("authenticated test commit census")
                .0;
            receipt.commit_index_charge = if self.persistence.is_none() {
                receipt
                    .commit_index_entries
                    .saturating_mul(crate::composite_store::COMMIT_INDEX_ENTRY_BYTES)
            } else {
                0
            };
            receipt.persistence_cache = self.persistence.as_ref().map_or(0, |p| {
                let cache = p.cache_charge();
                #[cfg(feature = "test-seams")]
                let cache = cache
                    + p.reader_registry_charge_for_test(
                        self.finalization.accountant.as_ptr() as usize
                    );
                cache
            });
            receipt.custody_companion_memo = self
                .custody_cache
                .lock()
                .as_ref()
                .map_or(0, crate::custody::store::Store::companion_memo_charge);
            receipt.custody_tenant_mirror = self
                .custody_cache
                .lock()
                .as_ref()
                .map_or(0, crate::custody::store::Store::tenant_mirror_charge);
            (
                receipt.current_membership_charge,
                receipt.captured_membership_charge,
            ) = self
                .relational_store
                .membership_held_charge_bytes_for_test();
            receipt.current_membership_by_table = self
                .relational_store
                .membership_held_charge_by_table_for_test();
            let ddl_log = self.ddl_log.read();
            receipt.ddl_log_entries = ddl_log.len();
            receipt.ddl_log_estimated_bytes = ddl_log
                .iter()
                .map(|(_, ddl)| std::mem::size_of::<(Lsn, DdlChange)>() + ddl.estimated_bytes())
                .sum();
            drop(ddl_log);
            let pending_metadata = self.pending_commit_metadata.lock();
            receipt.pending_commit_metadata = pending_metadata.len();
            for metadata in pending_metadata.values() {
                if let Some(memo) = &metadata.lineage_generations {
                    receipt.pending_lineage_generation_charge += memo._control.bytes();
                    let entries = memo.entries.lock();
                    let mut entry = entries.as_deref();
                    while let Some(current) = entry {
                        receipt.pending_lineage_generation_charge += current._memory.bytes();
                        entry = current.next.as_deref();
                    }
                }
            }
            drop(pending_metadata);
            receipt.pending_schema_stages = self.pending_local_schema_stages.lock().len()
                + self.local_schema_stages.lock().len()
                + self.received_schema_stages.lock().len();
            receipt.published_root_charge = self
                .tx_mgr
                .capture_published_read()
                .handle
                .and_then(|root| crate::persistence::WriterSnapshotHandle::from_capture(root).ok())
                .map_or(0, |root| root.descriptor_control_bytes());
            let (root_charge, schema_charge) =
                self.persistence.as_ref().map_or((0, 0), |persistence| {
                    persistence.writer_snapshot_charge_for_test()
                });
            receipt.writer_schema_charge = schema_charge;
            receipt.captured_root_charge =
                root_charge.saturating_sub(receipt.published_root_charge);
            receipt.sink_window_charge = self.event_bus.sink_window_charge();
            receipt.storage_maintenance_control_charge =
                self.persistence.as_ref().map_or(0, |persistence| {
                    persistence.storage_compaction_control_charge()
                });
            let (registration, backing, adapters) =
                self.finalization.accountant.writer_fixed_ownership();
            receipt.writer_registration = registration;
            receipt.engine_accountant_backing = backing;
            receipt.writer_adapter_controls = adapters;
            receipt.schema_history_retained_bytes = self.schema_history_retained_bytes();
            receipt.published_lsn = self.tx_mgr.capture_published_read().lsn;
            receipt.named_owner_total = registration
                + backing
                + adapters
                + receipt.pending_lineage_generation_charge
                + receipt.storage_maintenance_control_charge
                + receipt.commit_source_reference_charge
                + receipt.writer_schema_charge
                + receipt.published_root_charge
                + receipt.captured_root_charge
                + receipt.staged_index_postings
                + receipt.vector.retained_total
                + receipt.vector.temporary_workspace
                + receipt.current_row_values
                + receipt.current_row_side_maps
                + receipt.current_row_index_postings
                + receipt.current_row_f32_projections
                + receipt.staged_row_admissions
                + receipt.staged_vector_admissions
                + receipt.table_metadata
                + receipt.graph_entries
                + receipt.change_log_charges.iter().sum::<usize>()
                + receipt.commit_index_charge
                + receipt.persistence_cache
                + receipt.custody_companion_memo
                + receipt.custody_tenant_mirror
                + receipt.current_membership_charge
                + receipt.captured_membership_charge
                + receipt.sink_window_charge
                + receipt.schema_history_retained_bytes;
            receipt.reconciliation_gap =
                receipt.vector.accountant_total as i64 - receipt.named_owner_total as i64;
            observe(receipt)
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn writer_memory_underflow_count_for_test(&self) -> usize {
        self.finalization.accountant.underflow_count_for_test()
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn arm_vector_current_identity_admission_failure_for_test(&self) {
        self.finalization
            .accountant
            .arm_current_identity_admission_failure_for_test();
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn vector_current_identity_admission_failure_consumed_for_test(&self) -> bool {
        self.finalization
            .accountant
            .current_identity_admission_failure_consumed_for_test()
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn inspect_captured_vector_suffix_for_test(
        &self,
        partition: &contextdb_vector::VectorPartitionRef,
        snapshot: SnapshotId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<Option<contextdb_vector::store::CapturedVectorSuffixInspectionForTest>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        let _schema = self.vector_schema_read(&partition.index);
        let _source = self.writer_read_scope(read_memory.clone())?;
        let mut owner = crate::vector_read_memory::admission(read_memory);
        self.vector_store
            .inspect_captured_vector_suffix_for_test(partition, snapshot, &mut owner, checkpoint)
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn arm_vector_tail_admission_failure_for_test(&self) {
        self.finalization
            .accountant
            .arm_tail_admission_failure_for_test();
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn vector_tail_admission_failure_consumed_for_test(&self) -> bool {
        self.finalization
            .accountant
            .tail_admission_failure_consumed_for_test()
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn arm_received_vector_image_prepublication_failure_for_test(&self) {
        RECEIVED_VECTOR_IMAGE_FAILURE_CONSUMED.with(|consumed| consumed.set(false));
        RECEIVED_VECTOR_IMAGE_FAILURE_ARMED.with(|armed| armed.set(true));
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn received_vector_image_prepublication_failure_consumed_for_test(&self) -> bool {
        RECEIVED_VECTOR_IMAGE_FAILURE_CONSUMED.with(std::cell::Cell::get)
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn arm_dormant_vector_decode_failure_for_test(&self) {
        contextdb_vector::test_seam::arm_dormant_vector_decode_failure_for_test();
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn dormant_vector_decode_failure_consumed_for_test(&self) -> bool {
        contextdb_vector::test_seam::dormant_vector_decode_failure_consumed_for_test()
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn arm_vector_post_load_reconciliation_failure_for_test(&self) {
        contextdb_vector::test_seam::arm_vector_post_load_reconciliation_failure_for_test();
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn vector_post_load_reconciliation_failure_consumed_for_test(&self) -> bool {
        contextdb_vector::test_seam::vector_post_load_reconciliation_failure_consumed_for_test()
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn __arm_one_shot_vector_memory_workspace_pause_for_test(
        &self,
        phase: VectorMemoryWorkspacePhaseForTest,
    ) -> VectorLifecyclePauseHandle {
        match phase {
            VectorMemoryWorkspacePhaseForTest::CompactionPreparedBeforePublication => {
                self.vector_store.arm_memory_workspace_pause_for_test()
            }
        }
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn arm_vector_durable_purge_failure_for_test(&self) {
        arm_vector_memory_fault(
            &VECTOR_DURABLE_PURGE_FAILURE_ARMED,
            &VECTOR_DURABLE_PURGE_FAILURE_CONSUMED,
        );
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn vector_durable_purge_failure_consumed_for_test(&self) -> bool {
        VECTOR_DURABLE_PURGE_FAILURE_CONSUMED.load(Ordering::SeqCst)
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn arm_vector_durable_compaction_failure_for_test(&self) {
        let store = Arc::as_ptr(&self.vector_store) as usize;
        let _ = VECTOR_DURABLE_COMPACTION_FAILURE_CONSUMED_BY.compare_exchange(
            store,
            0,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        VECTOR_DURABLE_COMPACTION_FAILURE_ARMED_FOR.store(store, Ordering::SeqCst);
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn vector_durable_compaction_failure_consumed_for_test(&self) -> bool {
        VECTOR_DURABLE_COMPACTION_FAILURE_CONSUMED_BY.load(Ordering::SeqCst)
            == Arc::as_ptr(&self.vector_store) as usize
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn register_vector_index_for_column(
        &self,
        table: &str,
        column: &ColumnDef,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.register_vector_index_for_column_with_identity(
            table,
            &column.name,
            column,
            vector_copy_owner,
            read_memory,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn register_vector_index_for_column_with_identity(
        &self,
        table: &str,
        index_column: &str,
        column: &ColumnDef,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        _read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let _read_memory: contextdb_core::read_memory::ReadMemoryOwner = _read_memory.into();

        if let Some(layout) = vector_index_layout_from_column(column) {
            self.validate_native_ivf_declaration(
                &VectorIndexRef::new(table, index_column),
                column,
            )?;
            self.vector_store.register_index_with_layout(
                VectorIndexRef::new(table, index_column),
                layout,
                contextdb_vector::MemoryBudgetOwner::clone(&self.finalization.vector_accountant),
                vector_copy_owner,
            )?;
            self.reconcile_maintenance_thread();
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn validate_vector_layout_policy_for_ddl(
        &self,
        index: &VectorIndexRef,
        column: &ColumnDef,
        _vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        _read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let _read_memory: contextdb_core::read_memory::ReadMemoryOwner = _read_memory.into();

        self.validate_native_ivf_declaration(index, column)?;
        self.vector_store.with_index_layout(index, |_| ())?;
        if let Some(max_partitions) = column.effective_max_partitions()
            && self.vector_store.live_or_retained_partition_count(index)? > max_partitions as usize
        {
            return Err(Error::VectorPartitionLimitExceeded {
                index: index.clone(),
                max_partitions,
            });
        }
        Ok(())
    }

    pub(crate) fn deregister_vector_index(&self, table: &str, column: &str) {
        let index = VectorIndexRef::new(table, column);
        let receipt = self
            .vector_store
            .teardown_index_without_copy(&index, self.accountant());
        self.finalization
            .accountant
            .release(receipt.legacy_live_raw_bytes);
        self.reconcile_maintenance_thread();
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn prepare_vector_index_rename<'a>(
        &'a self,
        old: &VectorIndexRef,
        new: &VectorIndexRef,
        owner: contextdb_vector::VectorCopyAdmission,
    ) -> Result<contextdb_vector::store::PreparedVectorIndexRename<'a>> {
        self.vector_store.prepare_index_rename(old, new, owner)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn prepare_vector_index_replacement_rename<'a>(
        &'a self,
        old: &VectorIndexRef,
        new: &VectorIndexRef,
        replaced: &VectorIndexRef,
        owner: contextdb_vector::VectorCopyAdmission,
    ) -> Result<contextdb_vector::store::PreparedVectorIndexRename<'a>> {
        self.vector_store.prepare_index_replacement_rename(
            old,
            new,
            replaced,
            contextdb_vector::MemoryBudgetOwner::clone(&self.finalization.vector_accountant),
            owner,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn validate_vector_index_rename_for_ddl(
        &self,
        table: &str,
        from: &str,
        to: &str,
        _vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        _read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let _read_memory: contextdb_core::read_memory::ReadMemoryOwner = _read_memory.into();

        let old = VectorIndexRef::new(table, from);
        let new = VectorIndexRef::new(table, to);
        if self.vector_store.contains_index(&new) {
            return Err(Error::Other(
                (format!("vector index already exists: {}.{}", new.table, new.column)).into(),
            ));
        }
        self.vector_store.with_index_layout(&old, |_| ())
    }

    pub(crate) fn vector_store_deregister_table(&self, table: &str) {
        let receipt = self
            .vector_store
            .teardown_table_without_copy(table, self.accountant());
        self.finalization
            .accountant
            .release(receipt.legacy_live_raw_bytes);
        self.reconcile_maintenance_thread();
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn drain_vector_index_maintenance_for_ddl(
        &self,
        index: &VectorIndexRef,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<()> {
        let mut layout_copy = vector_copy_owner.local_scratch();
        let layout = self
            .vector_store
            .index_layout(index, layout_copy.as_mut().unwrap_or(vector_copy_owner))?;
        #[cfg(any(test, feature = "test-seams"))]
        // Marks entry to the DDL drain; tests then assert schema mutation waits
        // for the subsequent same-ref maintenance drain to complete.
        self.pause_vector_ddl_for_test(index);
        // Reconfiguration with the current shape is a no-op, but it acquires
        // the affected ref's maintenance lock before SQL metadata changes.
        self.vector_store.register_index_with_layout(
            index.clone(),
            layout,
            contextdb_vector::MemoryBudgetOwner::clone(&self.finalization.vector_accountant),
            vector_copy_owner,
        )?;
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn drain_vector_table_maintenance_for_ddl(
        &self,
        table: &str,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<()> {
        #[cfg(any(test, feature = "test-seams"))]
        // Same entry marker as the per-ref drain, keyed by table.* for DROP TABLE.
        self.pause_vector_ddl_for_test(&VectorIndexRef::new(table, "*"));
        let indexes = self
            .vector_store
            .index_infos(vector_copy_owner)?
            .into_iter()
            .filter(|info| info.index.table == table)
            .map(|info| info.index)
            .collect::<Vec<_>>();
        for index in indexes {
            self.drain_vector_index_maintenance_for_ddl(&index, vector_copy_owner)?;
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn pause_vector_ddl_for_test(&self, index: &VectorIndexRef) {
        self.vector_store.maybe_pause_ddl_for_test(index);
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_inspection_infos(
        &self,
        target: Option<&VectorIndexRef>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        _read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<
        Vec<(
            ColumnDef,
            contextdb_vector::store::VectorIndexLayoutInfo,
            Vec<contextdb_vector::store::VectorPartitionInfo>,
        )>,
    > {
        let _read_memory: contextdb_core::read_memory::ReadMemoryOwner = _read_memory.into();

        if !self.access_is_admin() {
            return Err(Error::VectorWholeIndexInspectionDenied);
        }

        let refs = match target {
            Some(index) => vec![index.clone()],
            None => self.vector_store_schema_refs(vector_copy_owner)?,
        };
        let _vector_schema = self.vector_schema_read_many(refs.clone());
        refs.into_iter()
            .map(|index| {
                let declaration = self
                    .table_meta(&index.table)
                    .and_then(|meta| {
                        meta.columns
                            .into_iter()
                            .find(|column| column.name == index.column)
                    })
                    .ok_or_else(|| Error::UnknownVectorIndex {
                        index: index.clone(),
                    })?;
                let mut partitions = self.vector_store.partition_infos(&index, vector_copy_owner)?;
                if self.persistence.as_ref().is_some_and(|p| p.repair_orphan_audit_blocked()) {
                    let details = contextdb_vector::store::VectorMaintenanceFailureDetails::from_error(
                        &Error::Other("vector page ownership reclamation is stalled; inspect unauthenticated retained references".into()),
                    );
                    for info in &mut partitions {
                        if info.maintenance_failure_details.is_none() {
                            info.maintenance_failure = Some(details.failure);
                            info.maintenance_failure_details = Some(details);
                        }
                    }
                }
                Ok((
                    declaration,
                    self.vector_store
                        .index_layout_info(&index, vector_copy_owner)?,
                    partitions,
                ))
            })
            .collect()
    }

    /// Where this handle reads the definitions, queues, and audit rings it
    /// loads once at open, or nothing when it holds no durable state at all.
    pub(crate) fn startup_state(&self) -> Option<StartupState<'_>> {
        if let Some(persistence) = self.persistence.as_deref() {
            return Some(StartupState::Durable(persistence));
        }
        self.committed_image_startup
            .as_deref()
            .map(StartupState::Image)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn account_loaded_state(&self) -> Result<()> {
        if let Some(startup) = self.startup_state() {
            let mut identities = Vec::new();
            for (table, rows) in self.relational_store.tables.read().iter() {
                for row in rows.iter().filter(|row| row.deleted_tx.is_some()) {
                    let key = crate::composite_store::retention_expiry_key(
                        table,
                        row.row_id,
                        row.created_tx,
                    );
                    if let Some(identity) =
                        startup.config_value::<(String, RowId, TxId, Lsn)>(&key)?
                    {
                        identities.push(identity);
                    }
                }
            }
            let prepared = self.relational_store.prepare_retention_expiries(
                identities,
                self.finalization.accountant.assembly_read_budget()?,
            )?;
            self.relational_store.publish_retention_expiries(prepared);
        }
        let metadata_bytes =
            self.relational_store
                .table_meta
                .read()
                .values()
                .fold(0usize, |acc, meta| {
                    acc.saturating_add(retained_table_metadata_bytes(&self.relational_store, meta))
                });
        // Charges taken so far are returned if a later one is refused: until
        // the whole set is charged, the retained-state owner refunds nothing.
        struct LoadedCharges<'a>(&'a MemoryAccountant, usize);
        impl Drop for LoadedCharges<'_> {
            fn drop(&mut self) {
                self.0.release(self.1);
            }
        }
        let mut charged = LoadedCharges(self.finalization.accountant.as_ref(), 0);
        self.finalization.accountant.try_allocate_for(
            metadata_bytes,
            "open",
            "load_table_metadata",
            "Open the database with a larger MEMORY_LIMIT or reduce stored schema metadata.",
        )?;
        charged.1 = metadata_bytes;

        let row_bytes =
            self.relational_store
                .tables
                .read()
                .iter()
                .fold(0usize, |acc, (table, rows)| {
                    let meta = self.table_meta(table);
                    // A version carrying `deleted_tx` was superseded or
                    // deleted before this store was written to disk, and the
                    // reclaim passes hand back only versions that still hold a
                    // charge. Charging one here would strand bytes no pass can
                    // return, so the row fold skips tombstoned versions
                    // exactly as the edge fold below does.
                    acc.saturating_add(
                        rows.iter()
                            .filter(|row| {
                                row.deleted_tx.is_none()
                                    || self.relational_store.is_retention_expired(
                                        table,
                                        row.row_id,
                                        row.created_tx,
                                    )
                            })
                            .fold(0usize, |inner, row| {
                                inner.saturating_add(retained_row_bytes_with_optional_meta(
                                    table,
                                    row,
                                    meta.as_ref(),
                                ))
                            }),
                    )
                });
        self.finalization.accountant.try_allocate_for(
            row_bytes,
            "open",
            "load_rows",
            "Open the database with a larger MEMORY_LIMIT or prune retained rows first.",
        )?;
        charged.1 = charged.1.saturating_add(row_bytes);

        let edge_bytes = self
            .graph_store
            .forward_adj
            .read()
            .values()
            .flatten()
            .fold(0usize, |acc, edge| {
                acc.saturating_add(edge.estimated_bytes())
            });
        self.finalization.accountant.try_allocate_for(
            edge_bytes,
            "open",
            "load_edges",
            "Open the database with a larger MEMORY_LIMIT or reduce graph edge volume.",
        )?;
        charged.1 = charged.1.saturating_add(edge_bytes);

        // The vector store attaches the exact open-time reservations to the
        // directory/raw objects that own them. A later DROP or eviction can
        // therefore return the original charge once without reconstructing
        // it from a different cache state.
        self.vector_store
            .account_loaded_state(contextdb_vector::MemoryBudgetOwner::clone(
                &self.finalization.vector_accountant,
            ))?;
        charged.1 = 0;
        self.finalization
            .install_loaded_state_owner(RetainedNonVectorMemoryOwner::new(
                self.relational_store.clone(),
                self.graph_store.clone(),
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            ));

        Ok(())
    }

    /// Admit one row version's retained bytes: the non-vector figure under
    /// the caller's own operation name, then each F32 vector column's
    /// relational projection under `vector_insert@<table>.<column>`, so a
    /// budget refusal names the vector column that caused it. The whole
    /// amount is one charge owned by the row version -- exactly
    /// `retained_row_bytes_for_meta(values, meta, true)`, which is what every
    /// release of the version returns -- and is returned here so the caller
    /// can hand it back if its statement fails afterwards. A refusal leaves
    /// nothing charged.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn admit_retained_row_bytes(
        &self,
        table: &str,
        values: &HashMap<ColName, Value>,
        meta: &TableMeta,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> Result<usize> {
        if crate::persistence::received_working_source::WorkingReadSource::current_for(
            self.tx_mgr.identity(),
        )?
        .is_some()
        {
            let admission =
                self.admit_retained_row_owned(table, values, meta, subsystem, operation, hint)?;
            let bytes = admission.bytes();
            admission.transfer_to_staged();
            return Ok(bytes);
        }
        let non_vector_bytes = retained_row_bytes_for_meta(table, values, meta, false);
        self.finalization.accountant.try_allocate_for(
            non_vector_bytes,
            subsystem,
            operation,
            hint,
        )?;
        let mut admitted = non_vector_bytes;
        for (column, projection_bytes) in f32_projection_payload_bytes_by_column(values, meta) {
            if let Err(err) = (|| {
                let subsystem = self
                    .finalization
                    .accountant
                    .prepare_operation_label(subsystem)?;
                let operation = self
                    .finalization
                    .accountant
                    .prepare_operation_label(format_args!("vector_insert@{table}.{column}"))?;
                self.finalization.accountant.try_allocate_for_labels(
                    projection_bytes,
                    subsystem,
                    operation,
                    "Reduce vector dimensionality, insert fewer rows, or raise MEMORY_LIMIT."
                        .into(),
                )
            })() {
                self.finalization.accountant.release(admitted);
                return Err(err);
            }
            admitted = admitted.saturating_add(projection_bytes);
        }
        debug_assert_eq!(
            admitted,
            retained_row_bytes_for_meta(table, values, meta, true)
        );
        Ok(admitted)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn admit_retained_row_owned(
        &self,
        table: &str,
        values: &HashMap<ColName, Value>,
        meta: &TableMeta,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> Result<OwnedMemoryReservation> {
        if matches!(&self.tx_mgr, DatabaseTransactionHost::Owned(_))
            || crate::persistence::received_working_source::WorkingReadSource::current_for(
                self.tx_mgr.identity(),
            )?
            .is_some()
        {
            let mut admission = self.reserve_original_preparation_for(
                retained_row_bytes_for_meta(table, values, meta, false),
                subsystem,
                operation,
                hint,
            )?;
            for (column, bytes) in f32_projection_payload_bytes_by_column(values, meta) {
                crate::memory_accounting::grow_preparation_with_operation_label(
                    &mut admission,
                    bytes,
                    subsystem,
                    format_args!("vector_insert@{table}.{column}"),
                    "Reduce vector dimensionality, insert fewer rows, or raise MEMORY_LIMIT.",
                )?;
            }
            return Ok(admission);
        }
        let bytes =
            self.admit_retained_row_bytes(table, values, meta, subsystem, operation, hint)?;
        Ok(OwnedMemoryReservation::from_admitted(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            bytes,
        ))
    }
    pub(crate) fn record_owned_row_admission(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        admission: OwnedMemoryReservation,
    ) {
        assert!(
            matches!(&self.tx_mgr, DatabaseTransactionHost::Legacy(_)),
            "owned rows must consume their original invoice before publication"
        );

        self.record_staged_row_admission(tx, table, row_id, admission.bytes());
        admission.transfer_to_staged();
    }

    /// Record what admission charged for a row version that is now staged.
    ///
    /// A staged version is released by exactly this figure. Without the
    /// record a release re-prices the row under whatever index set the table
    /// has AT THE RELEASE, and a change to that set while the row sat staged
    /// makes the two ends different numbers -- which is the accountant
    /// drifting, and a declared `MEMORY_LIMIT` bounding the wrong thing.
    pub(crate) fn record_staged_row_admission(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        bytes: usize,
    ) {
        let _ = self.tx_mgr.with_write_set(tx, |ws| {
            ws.set_relational_insert_admitted_bytes(table, row_id, bytes);
            ws.ordered_staged_rows
                .record_installed_admission(table, row_id, bytes);
        });
    }

    /// What one staged row version's release owes.
    ///
    /// The record its admission left, and the table's current shape only for
    /// a version that has no record -- which means nothing has re-priced it
    /// since it was staged, so the current shape still answers what admission
    /// charged. `meta` is how the caller reaches the table's shape: the close
    /// path reads the store directly because its handle is already closed.
    pub(super) fn staged_row_release_bytes(
        &self,
        ws: &contextdb_tx::WriteSet,
        table: &str,
        row: &VersionedRow,
        meta: Option<TableMeta>,
    ) -> usize {
        ws.admitted_bytes_for_staged_row(table, row.row_id)
            .unwrap_or_else(|| retained_row_bytes_with_optional_meta(table, row, meta.as_ref()))
    }

    /// Give back what an abandoned transaction's inserts were holding.
    ///
    /// This runs on the CLOSE path, after the handle has already been marked
    /// closed, so it reads the table's shape from the store directly instead
    /// of through the public `table_meta`. Asking a closed handle for a public
    /// operation is a programming error everywhere else and rightly panics;
    /// here the handle is closed BY DESIGN and the work is the engine giving
    /// its own accountant its bytes back. Going through the public door made
    /// a `--write` session that ended with an inserting transaction still open
    /// abort at teardown instead of rolling that transaction back.
    /// Own the private abandoned payload ahead of its existing refund. Both
    /// normal errors and unwinding destroy every write-set carrier first.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn dispose_private_abandoned_write_set(
        &self,
        ws: WriteSet,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<()> {
        struct Refund<'a> {
            accountant: &'a crate::memory_accounting::AccountantHandle,
            bytes: usize,
        }
        impl Drop for Refund<'_> {
            fn drop(&mut self) {
                self.accountant.release(self.bytes);
            }
        }
        struct Abandoned<'a> {
            payload: WriteSet,
            refund: Refund<'a>,
        }
        let mut abandoned = Abandoned {
            payload: ws,
            refund: Refund {
                accountant: &self.finalization.accountant,
                bytes: 0,
            },
        };
        for (table, row) in &abandoned.payload.relational_inserts {
            abandoned.refund.bytes =
                abandoned
                    .refund
                    .bytes
                    .saturating_add(self.staged_row_release_bytes(
                        &abandoned.payload,
                        table,
                        row,
                        self.relational_store.table_meta(table),
                    ));
        }
        for edge in &abandoned.payload.adj_inserts {
            abandoned.refund.bytes = abandoned
                .refund
                .bytes
                .saturating_add(edge.estimated_bytes());
        }
        for entry in &abandoned.payload.vector_inserts {
            abandoned.refund.bytes =
                abandoned
                    .refund
                    .bytes
                    .saturating_add(self.vector_insert_accounted_bytes(
                        &entry.index,
                        entry.vector.len(),
                        vector_copy_owner,
                    )?);
        }
        // Field order, rather than a success-only drop call, owns custody.
        drop(abandoned);
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn release_insert_allocations(
        &self,
        ws: &contextdb_tx::WriteSet,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<()> {
        for (table, row) in &ws.relational_inserts {
            let bytes = self.staged_row_release_bytes(
                ws,
                table,
                row,
                self.relational_store.table_meta(table),
            );
            self.finalization.accountant.release(bytes);
        }

        for (edge, _) in &ws.row_adj_deletes {
            self.finalization.accountant.release(edge.estimated_bytes());
        }
        for edge in &ws.row_adj_inserts {
            self.finalization.accountant.release(edge.estimated_bytes());
        }
        for edge in &ws.adj_inserts {
            self.finalization
                .accountant
                .release(standalone_edge_retained_bytes(edge));
        }

        for entry in &ws.vector_inserts {
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

    /// File-backed commits retain rows and postings only in the durable source.
    /// The transaction payload admission retires after durable publication;
    /// source pages, schema and root controls have their separate owners.
    pub(super) fn release_committed_row_body_allocations(&self, ws: &contextdb_tx::WriteSet) {
        if matches!(&self.tx_mgr, DatabaseTransactionHost::Legacy(_)) {
            for (edge, _) in &ws.row_adj_deletes {
                self.finalization.accountant.release(edge.estimated_bytes());
            }
            if self.graph_store.is_source_backed() {
                for edge in &ws.row_adj_inserts {
                    self.finalization.accountant.release(edge.estimated_bytes());
                }
            }
        }

        if self.persistence.is_none() || !self.relational_store.uses_disk_backed_committed_bodies()
        {
            return;
        }
        for (table, row) in &ws.relational_inserts {
            let admitted = self.staged_row_release_bytes(
                ws,
                table,
                row,
                self.relational_store.table_meta(table),
            );
            self.finalization.accountant.release(admitted);
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn delete_release_bytes_for_write_set(
        &self,
        ws: &contextdb_tx::WriteSet,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<DeleteReleaseBytes> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut bytes = DeleteReleaseBytes::default();
        // The whole charge the version took when it was admitted -- its
        // non-vector values and, for an F32 vector column, its relational
        // projection (`retained_row_bytes_for_meta(.., true)`, the same
        // figure `admit_retained_row_bytes` charged). Returned at this
        // commit; the pass that later removes the tombstoned version
        // physically credits nothing for it.
        for (table, row_id, _) in &ws.relational_deletes {
            if let Some(row) =
                self.relational_store
                    .row_by_id(table, *row_id, SnapshotId::from_raw_wire(u64::MAX))
            {
                bytes.relational.push(
                    self.table_meta(table)
                        .map(|meta| retained_row_bytes_with_optional_meta(table, &row, Some(&meta)))
                        .unwrap_or_else(|| {
                            retained_row_bytes_with_optional_meta(table, &row, None)
                        }),
                );
            }
        }

        // Adjacency is deliberately absent. Deleting an edge stamps the
        // entry and leaves it whole in both adjacency maps, so its bytes are
        // still held; handing them back here told an operator the store had
        // released memory it was still holding, and the limit stopped
        // covering what the process held. The retention pass that physically
        // removes the entry is the one release it gets.

        // A raw body whose partition state carries the accountant is owned by
        // that state from its load or apply until the prune, eviction, or
        // removal that physically drops it, and the state returns it from
        // its own record at that moment. Tombstoning the body here changes
        // nothing about who holds the bytes, so this commit returns nothing
        // for it. A body a partition that is not yet resident would fault in
        // later is charged by that load, never here. Only a resident body in
        // a state without an accountant still carries the per-insert charge
        // this commit must settle.
        for (index, row_id, _) in &ws.vector_deletes {
            if self
                .vector_store
                .raw_entry_charge_owned_by_state(index, *row_id)
            {
                continue;
            }
            if let Some(vector) = self.find_vector_by_index_and_row(
                index,
                *row_id,
                vector_copy_owner,
                read_memory.clone(),
            )? {
                bytes.vectors.push(self.vector_insert_accounted_bytes(
                    index,
                    vector.vector.len(),
                    vector_copy_owner,
                )?);
            }
        }
        Ok(bytes)
    }

    /// The unpublished received working image owns all historical bodies,
    /// including tombstones. Its existing standing owner retains their same
    /// insertion/copy invoices; public databases keep their established route.
    pub(crate) fn private_received_rows_hold_delete_invoices(&self) -> bool {
        self.finalization
            .non_vector_memory
            .lock()
            .as_ref()
            .is_some_and(|owner| owner.defer_private_row_refunds)
    }
    pub(crate) fn release_delete_allocations_from_bytes(&self, bytes: &DeleteReleaseBytes) {
        if !self.private_received_rows_hold_delete_invoices() {
            for bytes in &bytes.relational {
                self.finalization.accountant.release(*bytes);
            }
        }
        for bytes in &bytes.vectors {
            self.finalization.accountant.release(*bytes);
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn find_vector_by_index_and_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VectorEntry>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.vector_store_live_entry_for_row(
            index,
            row_id,
            SnapshotId::from_raw_wire(u64::MAX),
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn write_set_checkpoint(&self, tx: TxId) -> Result<WriteSetCheckpoint<'_>> {
        if let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr {
            return handle.with_host(|host| self.write_set_checkpoint_owned(host, tx));
        }

        let metadata = self.pending_commit_metadata.lock();
        let prior = metadata.get(&tx);
        let bytes = prior.map_or(0, |m| {
            m.vector_schema_epochs
                .keys()
                .map(|index| {
                    std::mem::size_of::<VectorIndexRef>()
                        .saturating_add(index.table.len())
                        .saturating_add(index.column.len())
                })
                .sum()
        });
        let charge = crate::memory_accounting::OwnedMemoryReservation::try_new_for(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            bytes,
            "relational",
            "mutation_checkpoint",
            "Reduce staged metadata or raise MEMORY_LIMIT.",
        )?;
        let metadata_counts = prior.map_or([0; 5], |m| {
            [
                m.discard_selections.len(),
                m.delivery_registrations.len(),
                m.conditional_update_guards.len(),
                m.sync_lineage_guards.len(),
                m.upsert_intents.len(),
            ]
        });
        let vector_epochs = prior.map_or_else(Vec::new, |m| {
            let mut epochs = Vec::with_capacity(m.vector_schema_epochs.len());
            for index in m.vector_schema_epochs.keys() {
                epochs.push(index.clone());
            }
            epochs
        });
        let metadata_existed = prior.is_some();
        drop(metadata);
        let graph_checkpoint = self.checkpoint_private_graph_statement(tx)?;
        let (counts, marker, requires_commit_lsn, propagation_in_progress) =
            self.tx_mgr.with_write_set(tx, |ws| {
                (
                    [
                        ws.relational_inserts.len(),
                        ws.relational_deletes.len(),
                        ws.adj_inserts.len(),
                        ws.vector_inserts.len(),
                        ws.vector_deletes.len(),
                        ws.vector_moves.len(),
                        ws.config_writes.len(),
                        ws.adj_deletes.len(),
                        ws.relational_delete_predicates.len(),
                        ws.sync_source_provenance_marks.len(),
                        ws.config_deletes.len(),
                        ws.config_max_u64_keys.len(),
                        usize::from(ws.requires_commit_lsn),
                        ws.row_adj_inserts.len(),
                        ws.row_adj_deletes.len(),
                    ],
                    ws.ordered_staged_rows.begin_checkpoint(),
                    ws.requires_commit_lsn,
                    ws.propagation_in_progress,
                )
            })?;
        Ok(WriteSetCheckpoint {
            branch: Some(WriteSetCheckpointBranch::Legacy(LegacyWriteSetCheckpoint {
                database: self,
                tx,
                counts,
                row_graph_marker: self.legacy_row_graph_marker(tx),
                requires_commit_lsn,
                propagation_in_progress,
                marker,
                metadata_counts,
                metadata_existed,
                vector_epochs,
                graph_checkpoint,
                _charge: charge,
                active: true,
            })),
        })
    }

    /// Both frames prepare before either installs. Metadata installs inside
    /// the lower preparation peer while the active-map lock still excludes a
    /// competing lower operation; no user callback runs in that peer.
    /// No Database open constructs this host. The existing operation read gate
    /// prevents close, not concurrent same-tx access: protected paired close
    /// now retains both locks; activation still needs complete producer migrations.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn write_set_checkpoint_owned<'a>(
        &'a self,
        host: &'a EngineDatabaseTransactions,
        tx: TxId,
    ) -> Result<WriteSetCheckpoint<'a>> {
        let (transaction, metadata) = host.transactions.with_transactions(|transactions| {
            transactions.checkpoint_with_peer(tx, |lower| {
                let prepared = host
                    .metadata
                    .prepare_metadata_checkpoint(lower.identity())?;
                Ok(host.metadata.begin_metadata_checkpoint(prepared))
            })
        })?;
        let checkpoint = WriteSetCheckpoint {
            branch: Some(WriteSetCheckpointBranch::Owned(OwnedWriteSetCheckpoint {
                pair: Some(OwnedCheckpointPair {
                    transaction: std::mem::ManuallyDrop::new(transaction),
                    metadata: metadata.checkpoint,
                }),
            })),
        };
        drop(metadata.records);
        Ok(checkpoint)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn write_set_counts(&self, tx: TxId) -> Result<WriteSetCounts> {
        self.tx_mgr.with_read(tx, |ws| WriteSetCounts {
            relational_inserts: ws.relational_inserts.len(),
            relational_deletes: ws.relational_deletes.len(),
            adj_inserts: ws.adj_inserts.len(),
            adj_deletes: ws.adj_deletes.len(),
            row_adj_inserts: ws.row_adj_inserts.len(),
            row_adj_deletes: ws.row_adj_deletes.len(),
            vector_inserts: ws.vector_inserts.len(),
            vector_deletes: ws.vector_deletes.len(),
            vector_moves: ws.vector_moves.len(),
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn publish_owned_metadata(
        &self,
        tx: TxId,
        prepare: impl FnOnce(&EngineMetadataJournal) -> Result<PreparedMetadataMutation>,
    ) -> Result<()> {
        let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr else {
            return Err(DatabaseTransactionHost::producer_gap(
                "owned metadata publication",
            ));
        };
        let checkpoint = self.write_set_checkpoint(tx)?;
        handle.with_host(|host| {
            host.transactions.with_transactions(|transactions| {
                transactions.with_engine(tx, |_guard| {
                    let prepared = prepare(&host.metadata)?;
                    host.metadata.publish_metadata(prepared);
                    Ok(())
                })
            })
        })?;
        checkpoint.settle();
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn append_owned_metadata(
        &self,
        tx: TxId,
        batch: AdmittedMetadataBatch,
    ) -> Result<()> {
        self.publish_owned_metadata(tx, |journal| journal.prepare_metadata_append(tx, batch))
    }

    // 8th parameter `fail_on_conflict` is load-bearing: it differentiates
    // stale-conditional-UPDATE silent no-ops from explicit row-guard atomic
    // rollbacks. Internal pub(crate) helper; struct refactor is overkill.
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn record_conditional_update_guard(
        &self,
        tx: TxId,
        table: TableName,
        row_id: RowId,
        predicates: Vec<(ColName, Value)>,
        before: WriteSetCounts,
        after: WriteSetCounts,
        fail_on_conflict: bool,
    ) -> Result<()> {
        self.tx_mgr.with_read(tx, |_| ())?;
        if matches!(&self.tx_mgr, DatabaseTransactionHost::Owned(_)) {
            let batch = self.with_preparation_original(|original| {
                let bytes = table
                    .len()
                    .checked_add(crate::memory_accounting::preparation_predicates_copy_bytes(
                        &predicates,
                    )?)
                    .ok_or_else(EngineMetadataJournal::identity_error)?;
                let invoice = crate::memory_accounting::reserve_preparation_from_original(
                    original,
                    bytes,
                    "relational",
                    "conditional_update_guard",
                    "Reduce staged metadata or raise MEMORY_LIMIT.",
                )?;
                let body = PendingConditionalUpdateGuard {
                    table: crate::memory_accounting::preparation_copy_string(&table)?,
                    row_id,
                    predicates: crate::memory_accounting::preparation_copy_predicates(&predicates)?,
                    before,
                    after,
                    fail_on_conflict,
                };
                Ok(AdmittedMetadataBatch::Conditional(
                    MetadataBuffer::prepare_one(body, invoice, original)?,
                ))
            })?;
            return self.append_owned_metadata(tx, batch);
        }

        self.pending_commit_metadata
            .lock()
            .entry(tx)
            .or_default()
            .conditional_update_guards
            .push(PendingConditionalUpdateGuard {
                table,
                row_id,
                predicates,
                before,
                after,
                fail_on_conflict,
            });
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn record_sync_lineage_guard(
        &self,
        tx: TxId,
        permission: &AuthenticatedFreshCreatorDelete,
    ) -> Result<()> {
        self.tx_mgr.with_read(tx, |_| ())?;
        if matches!(&self.tx_mgr, DatabaseTransactionHost::Owned(_)) {
            let batch = self.with_preparation_original(|original| {
                let bytes = permission
                    .table
                    .len()
                    .checked_add(crate::memory_accounting::preparation_values_copy_bytes(
                        &permission.values,
                    )?)
                    .ok_or_else(EngineMetadataJournal::identity_error)?;
                let invoice = crate::memory_accounting::reserve_preparation_from_original(
                    original,
                    bytes,
                    "sync",
                    "lineage_guard",
                    "Reduce staged metadata or raise MEMORY_LIMIT.",
                )?;
                let body = PendingSyncLineageGuard {
                    table: crate::memory_accounting::preparation_copy_string(&permission.table)?,
                    row_id: permission.row_id,
                    values: crate::memory_accounting::preparation_copy_values(&permission.values)?,
                };
                Ok(AdmittedMetadataBatch::SyncLineage(
                    MetadataBuffer::prepare_one(body, invoice, original)?,
                ))
            })?;
            return self.append_owned_metadata(tx, batch);
        }

        self.pending_commit_metadata
            .lock()
            .entry(tx)
            .or_default()
            .sync_lineage_guards
            .push(PendingSyncLineageGuard {
                table: permission.table.clone(),
                row_id: permission.row_id,
                values: permission.values.clone(),
            });
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn record_upsert_intent(
        &self,
        tx: TxId,
        table: TableName,
        row_id: RowId,
        details: UpsertIntentDetails,
    ) -> Result<()> {
        let mut vector_schema_refs = self.vector_schema_refs_for_table(&table);
        if self.table_meta(&table).is_some_and(|meta| {
            meta.state_machine.is_some_and(|state_machine| {
                details
                    .update_columns
                    .iter()
                    .any(|(column_name, _)| *column_name == state_machine.column)
            })
        }) {
            vector_schema_refs
                .extend(self.vector_schema_refs_for_state_propagation_from_table(&table));
        }
        // Couple the epoch registration and its upsert body: admission of
        // either can refuse without leaving half of the metadata operation.
        let owned_checkpoint = if matches!(&self.tx_mgr, DatabaseTransactionHost::Owned(_)) {
            Some(self.write_set_checkpoint(tx)?)
        } else {
            None
        };
        self.record_vector_schema_epochs(tx, vector_schema_refs)?;
        self.tx_mgr.with_read(tx, |_| ())?;
        if let Some(checkpoint) = owned_checkpoint {
            let batch = self.with_preparation_original(|original| {
                let values_bytes = crate::memory_accounting::preparation_values_copy_bytes(
                    &details.insert_values,
                )?;
                let names_bytes = crate::memory_accounting::preparation_names_copy_bytes(
                    &details.conflict_columns,
                )?;
                let assignments_bytes =
                    crate::memory_accounting::preparation_assignments_copy_bytes(
                        &details.update_columns,
                    )?;
                let params_bytes =
                    crate::memory_accounting::preparation_values_copy_bytes(&details.params)?;
                let bytes = [values_bytes, names_bytes, assignments_bytes, params_bytes]
                    .into_iter()
                    .try_fold(table.len(), |bytes, additional| {
                        bytes
                            .checked_add(additional)
                            .ok_or_else(EngineMetadataJournal::identity_error)
                    })?;
                let invoice = crate::memory_accounting::reserve_preparation_from_original(
                    original,
                    bytes,
                    "relational",
                    "upsert_intent",
                    "Reduce staged metadata or raise MEMORY_LIMIT.",
                )?;
                let body = PendingUpsertIntent {
                    table: crate::memory_accounting::preparation_copy_string(&table)?,
                    row_id,
                    active_tx: tx,
                    insert_values: crate::memory_accounting::preparation_copy_values(
                        &details.insert_values,
                    )?,
                    conflict_columns: crate::memory_accounting::preparation_copy_names(
                        &details.conflict_columns,
                    )?,
                    update_columns: crate::memory_accounting::preparation_copy_assignments(
                        &details.update_columns,
                    )?,
                    params: crate::memory_accounting::preparation_copy_values(&details.params)?,
                };
                Ok(AdmittedMetadataBatch::Upsert(MetadataBuffer::prepare_one(
                    body, invoice, original,
                )?))
            })?;
            self.append_owned_metadata(tx, batch)?;
            checkpoint.settle();
            return Ok(());
        }
        self.pending_commit_metadata
            .lock()
            .entry(tx)
            .or_default()
            .upsert_intents
            .push(PendingUpsertIntent {
                table,
                row_id,
                active_tx: tx,
                insert_values: details.insert_values,
                conflict_columns: details.conflict_columns,
                update_columns: details.update_columns,
                params: details.params,
            });
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn restore_write_set_checkpoint(
        &self,
        tx: TxId,
        mut checkpoint: WriteSetCheckpoint<'_>,
    ) -> Result<()> {
        match checkpoint.branch.take().expect("active coupled checkpoint") {
            WriteSetCheckpointBranch::Legacy(legacy) => {
                self.restore_legacy_write_set_checkpoint(tx, legacy)
            }
            WriteSetCheckpointBranch::Owned(owned) => {
                owned.restore_for_tx(tx);
                Ok(())
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn restore_legacy_write_set_checkpoint(
        &self,
        tx: TxId,
        mut checkpoint: LegacyWriteSetCheckpoint<'_>,
    ) -> Result<()> {
        let row_graph_journal = self
            .pending_commit_metadata
            .lock()
            .get(&tx)
            .and_then(|m| m.row_graph_undo.clone());
        self.tx_mgr.with_write_set(tx, |ws| {
            self.restore_legacy_row_graph_journal(
                row_graph_journal.as_ref(),
                checkpoint.row_graph_marker,
                ws,
            );
            ws.ordered_staged_rows.restore_checkpoint(
                checkpoint.marker,
                &mut ws.relational_inserts,
                &mut ws.relational_insert_admitted_bytes,
                |bytes| self.finalization.accountant.release(bytes),
            );
            ws.ordered_staged_rows.restore_vector_checkpoint(
                checkpoint.marker,
                &mut ws.vector_inserts,
                &mut ws.vector_moves,
                &mut ws.vector_deletes,
                |bytes| self.finalization.accountant.release(bytes),
            );
            ws.ordered_staged_rows.settle_checkpoint();
            ws.relational_inserts.truncate(checkpoint.counts[0]);
            ws.relational_deletes.truncate(checkpoint.counts[1]);
            if checkpoint.graph_checkpoint.is_none() {
                for edge in ws.adj_inserts.drain(checkpoint.counts[2]..) {
                    let bytes = standalone_edge_retained_bytes(&edge);
                    drop(edge);
                    self.finalization.accountant.release(bytes);
                }
            }
            ws.vector_inserts.truncate(checkpoint.counts[3]);
            ws.vector_deletes.truncate(checkpoint.counts[4]);
            ws.vector_moves.truncate(checkpoint.counts[5]);
            ws.config_writes.truncate(checkpoint.counts[6]);
            if checkpoint.graph_checkpoint.is_none() {
                ws.adj_deletes.truncate(checkpoint.counts[7]);
            }
            ws.relational_delete_predicates
                .truncate(checkpoint.counts[8]);
            ws.sync_source_provenance_marks
                .truncate(checkpoint.counts[9]);
            ws.config_deletes.truncate(checkpoint.counts[10]);
            ws.config_max_u64_keys.truncate(checkpoint.counts[11]);
            for (edge, _) in ws.row_adj_deletes.drain(checkpoint.counts[14]..) {
                self.finalization.accountant.release(edge.estimated_bytes());
            }
            ws.requires_commit_lsn = checkpoint.requires_commit_lsn;
            ws.propagation_in_progress = checkpoint.propagation_in_progress;
            ws.forget_admitted_bytes_for_unstaged_rows();
        })?;
        if let Some(graph) = checkpoint.graph_checkpoint.as_mut() {
            self.restore_private_graph_statement(tx, graph)?;
        }
        checkpoint.active = false;
        let mut metadata = self.pending_commit_metadata.lock();
        if !checkpoint.metadata_existed {
            metadata.remove(&tx);
        } else if let Some(prior) = metadata.get_mut(&tx) {
            // A rolled-back schema stage must never reuse a generation memo.
            prior.lineage_generations = None;
            prior
                .discard_selections
                .truncate(checkpoint.metadata_counts[0]);
            prior
                .delivery_registrations
                .truncate(checkpoint.metadata_counts[1]);
            prior
                .conditional_update_guards
                .truncate(checkpoint.metadata_counts[2]);
            prior
                .sync_lineage_guards
                .truncate(checkpoint.metadata_counts[3]);
            prior.upsert_intents.truncate(checkpoint.metadata_counts[4]);
            prior
                .vector_schema_epochs
                .retain(|index, _| checkpoint.vector_epochs.contains(index));
        }
        Ok(())
    }

    sync_test_seam! {
    #[cfg(any(test, feature = "test-seams"))]
    /// Get a clone of the current conflict policies. The declared per-table
    /// policy lives on each table's meta (`sync_conflict_policy_for_table`
    /// resolves against it); this carries the deployment default, which callers
    /// pass to a `SyncServer` as its baseline.
    fn conflict_policies(&self) -> ConflictPolicies {
        let _operation = self.assert_open_operation();
        self.conflict_policies.read().clone()
    }
    }

    pub fn plugin(&self) -> &dyn DatabasePlugin {
        assert!(
            !self.closed.load(Ordering::SeqCst),
            "database handle is closed"
        );
        self.plugin.as_ref()
    }

    /// A child of this store's already declared reader limit. Cloned views
    /// share the parent total, while each operation has separate charges.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn declared_reader_memory(&self) -> Result<contextdb_core::read_memory::ReadMemoryOwner> {
        self.declared_read_memory.child()
    }

    /// Hold `bytes` of this store's writer budget until the guard drops, to
    /// put an operation under transient memory pressure.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn hold_writer_memory_for_test(&self, bytes: usize) -> Result<WriterMemoryHoldForTest> {
        Ok(WriterMemoryHoldForTest(
            crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                bytes,
                "test",
                "hold_writer_memory",
                "Hold less writer memory.",
            )?,
        ))
    }

    /// This store's writer accountant, for engine sync work that holds a
    /// reservation rather than a read budget.
    pub(crate) fn writer_accountant_handle(&self) -> crate::memory_accounting::AccountantHandle {
        crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant)
    }

    /// Whether this handle is a sealed read image (opened from a committed
    /// image), which never serves as a sync hub.
    pub(crate) fn is_sealed_read_image(&self) -> bool {
        self.image_history.is_some()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn operation_read_owner(
        &self,
        read_memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<crate::read_image_memory::ImageMemoryHandle> {
        crate::read_image_memory::ImageMemory::operation_reader(
            self.accountant.clone(),
            read_memory,
        )
    }

    /// Memory for sync work the engine does on its own store, such as reading
    /// a pending unit back to push it. It is engine working memory, so the
    /// writer budget (MEMORY_LIMIT) funds it, never a caller's statement
    /// read limit.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn engine_sync_read_memory(
        &self,
    ) -> Result<contextdb_core::read_memory::ReadMemoryOwner> {
        self.finalization.accountant.read_budget()
    }

    /// Measurement only: this store's writer accountant, whose high-water mark
    /// shows the peak of engine sync work such as serving a pull page.
    #[cfg(test)]
    pub(crate) fn writer_accountant_for_test(&self) -> &crate::memory_accounting::AccountantHandle {
        &self.finalization.accountant
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __visit_committed_change_and_ddl_members_for_test(
        &self,
        tx: contextdb_core::TxId,
        tables: &[&str],
        visitor: impl FnMut(&[u8]) -> contextdb_core::Result<()>,
    ) -> contextdb_core::Result<()> {
        let _operation = self.assert_open_operation();
        let memory = self.declared_reader_memory()?;
        let persistence = self.persistence.as_ref().ok_or_else(|| {
            contextdb_core::Error::Other("committed member visitor requires persistence".into())
        })?;
        persistence.visit_committed_change_and_ddl_members_for_test(
            tx,
            tables,
            memory,
            self.accountant.clone(),
            visitor,
        )
    }

    /// Borrow the actual serving cache without retaining the persistence handle lock.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating. "
    )]
    pub(crate) fn read_cache_observation(&self) -> Result<Option<redb::ReadCacheObservation>> {
        self.persistence
            .as_ref()
            .map_or(Ok(None), |persistence| persistence.read_cache_observation())
    }

    /// Bytes the declared reader memory currently holds for reads.
    pub(crate) fn declared_read_charged_bytes(&self) -> u64 {
        self.declared_read_memory.charged_bytes()
    }

    /// Observe the reader parent that funds operation and cursor owners.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn declared_read_memory_for_test(&self) -> crate::vector_read_memory::DeclaredReadRoot {
        self.declared_read_memory.clone()
    }

    /// Private query acquisition borrows the existing supplier. Constructing a
    /// wrapper Arc before the operation exists would itself be unadmitted.
    pub(crate) fn query_request_provider(&self) -> &crate::vector_read_memory::DeclaredReadRoot {
        &self.declared_read_memory
    }

    pub fn declared_read_request_provider(&self) -> crate::vector_read_memory::ReadRequestProvider {
        crate::vector_read_memory::ReadRequestProvider::declared(self.declared_read_memory.clone())
    }

    pub fn owner_read_config(&self) -> &OwnerReadConfig {
        let _operation = self.assert_open_operation();
        self.owner_read_config.as_ref()
    }

    /// Actual declared-reader parent bytes across this operation, including
    /// metadata, child controls, cursor scratch/windows and other same-parent
    /// admissions on worker threads. Concurrent same-parent work is included.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn with_reader_memory_high_water_for_test<R>(
        &self,
        operation: impl FnOnce() -> R,
    ) -> (R, crate::vector_read_memory::ReaderMemoryHighWaterForTest) {
        self.declared_read_memory
            .observe_high_water_for_test(operation)
    }

    /// Peak successfully admitted writer bytes for journey observation.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn write_memory_high_water_for_test(&self) -> usize {
        self.finalization.accountant.high_water_for_test()
    }

    /// One tagged original reservation of `bytes` on this writer, plain or
    /// native: `(outcome, writer used before, after, process aggregate before,
    /// after)`. A refusal must leave both counters where they were.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn writer_original_reservation_for_test(
        &self,
        bytes: usize,
        native: bool,
    ) -> (std::result::Result<(), String>, usize, usize, usize, usize) {
        let handle = crate::memory_accounting::StoreAccountantHandle::new(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
        );
        let used_before = self.finalization.accountant.usage().used;
        let aggregate_before = crate::memory_accounting::process_aggregate_snapshot().0;
        let outcome = if native {
            handle
                .try_allocate_for_original_native(bytes, "test", "original_reservation", "test")
                .map_err(|refusal| format!("{refusal:?}"))
        } else {
            handle
                .try_allocate_for_original(bytes, "test", "original_reservation", "test")
                .map_err(|error| format!("{error:?}"))
        };
        (
            outcome,
            used_before,
            self.finalization.accountant.usage().used,
            aggregate_before,
            crate::memory_accounting::process_aggregate_snapshot().0,
        )
    }

    /// Release `bytes` a successful `writer_original_reservation_for_test` took.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn writer_release_for_test(&self, bytes: usize) {
        self.finalization.accountant.release(bytes);
    }

    /// `(configured, writer, process, capacity, reserve)` bytes of this
    /// writer's page cache: the capacity policy gives it, what the writer's
    /// accountant holds for it, what the process aggregate holds for it as
    /// open residue, and the capacity and whole reserve the native cache was
    /// last actually settled to.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn writer_cache_for_test(&self) -> (usize, usize, usize, usize, usize) {
        (
            crate::persistence::redb_writer_cache_bytes_for_limit(
                self.finalization.accountant.usage().limit,
            ),
            self.persistence
                .as_ref()
                .map_or(0, |persistence| persistence.cache_charge()),
            self.persistence
                .as_ref()
                .map_or(0, |persistence| persistence.cache_stripe_residue_bytes()),
            self.persistence
                .as_ref()
                .map_or(0, |persistence| persistence.cache_installation_for_test().0),
            self.persistence
                .as_ref()
                .map_or(0, |persistence| persistence.cache_installation_for_test().1),
        )
    }

    pub fn memory_limit(&self) -> Option<usize> {
        let _operation = self.assert_open_operation();
        self.finalization.accountant.usage().limit
    }

    pub fn contexts(&self) -> Option<&BTreeSet<ContextId>> {
        let _operation = self.assert_open_operation();
        self.access.contexts.as_ref()
    }

    pub fn scope_labels(&self) -> Option<&BTreeSet<ScopeLabel>> {
        let _operation = self.assert_open_operation();
        self.access.scope_labels.as_ref()
    }

    pub fn principal(&self) -> Option<&Principal> {
        let _operation = self.assert_open_operation();
        self.access.principal.as_ref()
    }

    pub fn plugin_health(&self) -> PluginHealth {
        let _operation = self.assert_open_operation();
        self.plugin.health()
    }

    pub fn plugin_describe(&self) -> serde_json::Value {
        let _operation = self.assert_open_operation();
        self.plugin.describe()
    }

    pub(crate) fn relational_store(&self) -> &Arc<RelationalStore> {
        &self.relational_store
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn allocate_ddl_lsn<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(Lsn) -> Result<R>,
    {
        #[cfg(any(test, feature = "test-seams"))]
        crate::writer_commit_test_seams::reached(
            crate::writer_commit_test_seams::CommitBoundary::BeforeDdlLsnAllocation,
        );
        let committed = self.tx_mgr.allocate_ddl_lsn(f);
        #[cfg(any(test, feature = "test-seams"))]
        if committed.is_ok() {
            self.emit_writer_memory_ownership_receipt_for_test("commit");
        }
        committed
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn allocate_ddl_lsn_maybe<F, R>(&self, f: F) -> Result<Option<R>>
    where
        F: FnOnce(Lsn) -> Result<Option<R>>,
    {
        #[cfg(any(test, feature = "test-seams"))]
        crate::writer_commit_test_seams::reached(
            crate::writer_commit_test_seams::CommitBoundary::BeforeDdlLsnAllocation,
        );
        let committed = self.tx_mgr.allocate_ddl_lsn_maybe(f);
        #[cfg(any(test, feature = "test-seams"))]
        if matches!(&committed, Ok(Some(_))) {
            self.emit_writer_memory_ownership_receipt_for_test("commit");
        }
        committed
    }

    /// Standalone durable metadata writes publish their successor before the
    /// ordinary commit guard ends. A previously captured reader root remains
    /// valid; a later scope captures this newly committed root.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn publish_direct_metadata<R>(
        &self,
        write: impl FnOnce() -> Result<R>,
    ) -> Result<R> {
        publish_direct_metadata(&self.tx_mgr, write)
    }

    pub(crate) fn with_commit_lock<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        self.tx_mgr.with_commit_lock(f)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn log_alter_table_ddl(
        &self,
        name: &str,
        meta: &TableMeta,
        lsn: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let change = alter_table_ddl_change(name, meta, Vec::new());
        self.append_ddl_change(lsn, change, read_memory.clone())
    }

    pub(crate) fn alter_table_ddl_for_meta(
        &self,
        name: &str,
        meta: &TableMeta,
        extra_constraints: Vec<String>,
    ) -> DdlChange {
        alter_table_ddl_change(name, meta, extra_constraints)
    }

    /// Persist a replayed ALTER's result and log it with the author's own
    /// rename and drop markers, so the relayed DDL says what the author did.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn persist_table_meta_rows_vectors_and_log_alter_table_ddl_with_markers(
        &self,
        name: &str,
        meta: &TableMeta,
        markers: Vec<String>,
        lsn: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.persist_table_meta_rows_vectors_and_append_alter_table_ddl(
            name,
            meta,
            lsn,
            markers,
            Vec::new(),
            read_memory.clone(),
        )
    }

    /// What the versions already committed into `table` cost under one
    /// shape. Public databases price the established live set; an unpublished
    /// received working image also prices historical versions whose original
    /// invoices it retains through physical disposal.
    pub(super) fn committed_rows_priced_under(&self, table: &str, meta: &TableMeta) -> usize {
        let retain_deleted = self.private_received_rows_hold_delete_invoices();
        let tables = self.relational_store.tables.read();
        tables
            .get(table)
            .map(Vec::as_slice)
            .unwrap_or_default()
            .iter()
            .filter(|row| retain_deleted || row.deleted_tx.is_none())
            .fold(0usize, |bytes, row| {
                let values = self.relational_store.committed_projection_values(meta, row);
                bytes.saturating_add(retained_row_bytes_for_meta(table, &values, meta, true))
            })
    }

    /// Price what every OPEN transaction has staged into `table` under the
    /// shape it was admitted with and under the shape that is arriving.
    ///
    /// A staged row version lives in its write set until commit, so nothing
    /// that reads committed versions can see it -- and it is charged under
    /// the index set the table had when its statement ran, then released
    /// under the set the table has when the transaction ends. After a CREATE
    /// INDEX lands mid-transaction that release is one posting per row too
    /// large; after a DROP INDEX it is one too small. Both are the accountant
    /// drifting away from what the store holds.
    ///
    /// Returns the staged total on each side, for the caller to fold into the
    /// same swap it takes for the committed rows, and the new figure per
    /// staged row, which the swap records only once the shape is live.
    pub(super) fn staged_rows_priced_across_shape_change(
        &self,
        table: &str,
        old_meta: &TableMeta,
        new_meta: &TableMeta,
    ) -> (usize, usize, HashMap<TxId, Vec<(String, RowId, usize)>>) {
        if matches!(&self.tx_mgr, DatabaseTransactionHost::Owned(_)) {
            // Native source rows retain their real body/posting invoices.
            // Current-declaration replacement is independently prepared at
            // their next commit; no Legacy logical estimate is transferred.
            return (0, 0, HashMap::new());
        }
        self.tx_mgr.with_active_write_sets(|active| {
            let mut old_bytes = 0usize;
            let mut new_bytes = 0usize;
            let mut per_tx: HashMap<TxId, Vec<(String, RowId, usize)>> = HashMap::new();
            for (tx, ws) in active.iter() {
                for (staged_table, row) in &ws.relational_inserts {
                    if staged_table != table {
                        continue;
                    }
                    let admitted = ws
                        .admitted_bytes_for_staged_row(staged_table, row.row_id)
                        .unwrap_or_else(|| {
                            retained_row_bytes_for_meta(table, &row.values, old_meta, true)
                        });
                    let arriving = retained_row_bytes_for_meta(table, &row.values, new_meta, true);
                    old_bytes = old_bytes.saturating_add(admitted);
                    new_bytes = new_bytes.saturating_add(arriving);
                    per_tx
                        .entry(*tx)
                        .or_default()
                        .push((table.to_string(), row.row_id, arriving));
                }
            }
            (old_bytes, new_bytes, per_tx)
        })
    }

    /// Reserve the retained-memory delta for one local table replacement
    /// before its durable half runs. A larger projection owns its reservation
    /// after publication; a smaller projection releases only after the old
    /// in-memory image has actually been swapped out.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn prepare_local_table_projection_memory(
        &self,
        name: &str,
        old_meta: &TableMeta,
        new_meta: &TableMeta,
        new_rows: &[VersionedRow],
    ) -> Result<PreparedMemorySwap> {
        let retain_deleted = self.private_received_rows_hold_delete_invoices();
        // Only live versions own a charge on either side of the swap: a
        // tombstoned version handed its bytes back at the commit that
        // stamped it, so counting it here would release it a second time.
        let retained_rows = |rows: &[VersionedRow], meta: &TableMeta| {
            // The file-backed publication discards the projected bodies and
            // postings. Their source windows already own the temporary copy;
            // the swap retains only metadata and transaction-local rows.
            if self.relational_store.uses_disk_backed_committed_bodies() {
                return 0;
            }
            rows.iter()
                .filter(|row| retain_deleted || row.deleted_tx.is_none())
                .fold(0usize, |bytes, row| {
                    let values = self.relational_store.committed_projection_values(meta, row);
                    bytes.saturating_add(retained_row_bytes_for_meta(name, &values, meta, true))
                })
        };
        let old_bytes = {
            let tables = self.relational_store.tables.read();
            retained_table_metadata_bytes(&self.relational_store, old_meta).saturating_add(
                retained_rows(
                    tables.get(name).map(Vec::as_slice).unwrap_or_default(),
                    old_meta,
                ),
            )
        };
        let new_bytes = retained_table_metadata_bytes(&self.relational_store, new_meta)
            .saturating_add(retained_rows(new_rows, new_meta));
        // Rows a still-open transaction has staged into this table are
        // charged under the shape it had when their statement ran and
        // released under the shape it has when that transaction ends, so the
        // settlement has to cover them alongside the committed ones.
        let (staged_old, staged_new, staged_repricing) =
            self.staged_rows_priced_across_shape_change(name, old_meta, new_meta);
        PreparedMemorySwap::prepare_for(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            old_bytes.saturating_add(staged_old),
            new_bytes.saturating_add(staged_new),
            0,
            "ddl",
            "prepare_local_table_projection",
            "Reduce schema or indexed row size, or raise MEMORY_LIMIT before altering the table.",
        )
        .map(|swap| {
            swap.carrying_staged_admissions(StagedAdmissionRepricing {
                tx_mgr: self.tx_mgr.clone(),
                per_tx: staged_repricing,
            })
        })
        .and_then(|swap| swap.prepare_schema_generation(self, std::iter::once(name)))
    }

    pub(crate) fn allows_local_vector_policy_metadata_publication(&self) -> bool {
        self.finalization
            .local_policy_metadata_enabled
            .load(Ordering::SeqCst)
            && !self.capture_detached_sync_write_set.load(Ordering::SeqCst)
            && !self.private_received_rows_hold_delete_invoices()
    }

    /// Publish an already validated, local-autocommit search-breadth or consolidation declaration.
    /// No row, posting or historical body changes ownership on this path.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn publish_local_vector_policy_metadata(
        &self,
        name: &str,
        old_meta: &TableMeta,
        meta: &TableMeta,
        index: &VectorIndexRef,
        lsn: Lsn,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        consolidation_only: bool,
        committed: &std::cell::Cell<bool>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let column = meta
            .columns
            .iter()
            .find(|column| column.name == index.column)
            .ok_or_else(|| Error::UnknownVectorIndex {
                index: index.clone(),
            })?;
        // Admit the replacement live declaration before cloning it. The row
        // and posting invoices are unchanged, including transaction-local rows.
        let mut metadata_memory = self.reserve_original_preparation_for(
            retained_table_metadata_bytes(&self.relational_store, meta),
            "ddl",
            if consolidation_only {
                "prepare_consolidation_metadata"
            } else {
                "prepare_ef_search_metadata"
            },
            "Reduce schema metadata or raise MEMORY_LIMIT.",
        )?;
        let mut memory_swap = PreparedMemorySwap::from_original_preparation(
            &mut metadata_memory,
            retained_table_metadata_bytes(&self.relational_store, old_meta),
            retained_table_metadata_bytes(&self.relational_store, meta),
            0,
        )?;
        let replacement = meta.clone();
        // Use the existing schema-staging envelope for DDL rendering and its
        // transient encodings; the retained occurrence gets its own invoice.
        let _scratch = self.reserve_original_preparation_for(
            Self::staged_table_ddl_temporary_bytes("")
                .saturating_add(meta.metadata_bytes().saturating_mul(32))
                .saturating_add(name.len().saturating_mul(32)),
            "ddl",
            if consolidation_only {
                "prepare_consolidation_history"
            } else {
                "prepare_ef_search_history"
            },
            "Reduce schema metadata or raise MEMORY_LIMIT.",
        )?;
        vector_copy_owner.reserve(std::mem::size_of::<VectorIndexLayout>().saturating_add(
            column.partition_key_columns.as_ref().map_or(0, |columns| {
                columns
                    .len()
                    .saturating_mul(std::mem::size_of::<String>())
                    .saturating_add(columns.iter().map(String::len).sum::<usize>())
            }),
        ))?;
        let layout =
            vector_index_layout_from_column(column).ok_or_else(|| Error::UnknownVectorIndex {
                index: index.clone(),
            })?;
        let prepared_policy =
            self.vector_store
                .prepare_policy_layout_update(index, layout, vector_copy_owner)?;
        let ddl = [self.alter_table_ddl_for_meta(name, meta, Vec::new())];
        let memory_generation = if self.persistence.is_none() {
            let sidecars = self.ddl_generation_sidecars(&ddl, read_memory.clone())?;
            let sidecar = &sidecars[0];
            Some((sidecar.table.clone(), sidecar.table_generation))
        } else {
            None
        };
        let values = self.ddl_generation_sidecar_values(lsn, &ddl, 0, read_memory)?;
        let memory_history = if let Some((table, generation)) = &memory_generation {
            let sidecar = DurableDdlGenerationSidecar {
                table: table.clone(),
                table_generation: *generation,
            };
            self.prepare_memory_schema_history(lsn, 0, &ddl, std::slice::from_ref(&sidecar))?
        } else {
            None
        };
        // Every fallible preparation, including the captured writer schema,
        // precedes durability. Move the successor payloads into an infallible
        // callback that runs before the postcommit observation boundary.
        let mut published_policy = None;
        let publish = || {
            let old = {
                let mut metadata = self.relational_store.table_meta.write();
                let current = metadata
                    .get_mut(name)
                    .expect("validated local policy table");
                std::mem::replace(current, replacement)
            };
            drop(old);
            self.publish_memory_schema_history(memory_history);
            memory_swap.commit_after_swap();
            if let Some(prepared) = prepared_policy {
                // Apply the already admitted layout after durable success.
                // A consolidation recheck belongs outside this committed
                // write's sticky admission scope, never inside publication.
                published_policy = self.vector_store.publish_policy_layout_update(prepared);
            }
        };
        let result = if let Some(persistence) = &self.persistence {
            // The durable occurrence is an admitted temporary encoding input,
            // not a resident history entry. Persistence keeps the occurrence
            // and its summary/configuration in the same durable transaction.
            let durable_ddl = [self.alter_table_ddl_for_meta(name, meta, Vec::new())];
            publish_direct_metadata_after_commit(&self.tx_mgr, committed, || {
                persistence.publish_vector_policy_metadata_and_ddl(
                    name,
                    meta,
                    values
                        .iter()
                        .map(|(key, value)| (key.as_str(), value.clone()))
                        .collect(),
                    lsn,
                    &durable_ddl,
                    &metadata_memory,
                    || {
                        publish();
                        committed.set(true);
                    },
                )
            })
        } else {
            publish();
            committed.set(true);
            Ok(())
        };
        result?;
        if consolidation_only && let Some(published) = published_policy {
            // Match ordinary local/received policy publication: a refused
            // inspection-only recheck keeps its failure mark until the
            // next maintenance pass. Durable metadata remains authoritative.
            if let Ok(read_memory) = self.declared_reader_memory() {
                let mut owner = crate::vector_read_memory::admission(read_memory);
                let _ = self
                    .vector_store
                    .recheck_policy_maintenance_failures(&published, &mut owner);
            }
        }
        Ok(())
    }

    /// Settle what a change to `table`'s INDEX SET costs -- or gives back --
    /// for the rows that are already in it.
    ///
    /// A row is charged one posting per DECLARED index, so adding an index to
    /// a table with rows in it adds one posting to every one of them and
    /// removing an index takes one away. A local CREATE INDEX / DROP INDEX
    /// settles that through `prepare_local_table_projection_memory`, which
    /// rebuilds the whole table projection. A change that ARRIVES -- a
    /// CREATE/DROP INDEX over sync, or an ALTER TABLE whose constraints
    /// refresh the automatic indexes -- mutates the table in place instead,
    /// and took no settlement at all: those rows were charged N postings when
    /// they were stored and released at N+1 when they were deleted, so the
    /// accountant drifted DOWN and the declared limit stopped bounding the
    /// store; a received DROP INDEX drifted it the other way, into refusing
    /// work the store had room for.
    ///
    /// Reserve BEFORE the change and `commit_after_swap` after it, so a
    /// refusal is the ordinary typed one and leaves the table untouched.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn prepare_index_set_change_memory(
        &self,
        table: &str,
        old_meta: &TableMeta,
        new_meta: &TableMeta,
    ) -> Result<PreparedMemorySwap> {
        let retain_deleted = self.private_received_rows_hold_delete_invoices();
        let (old_rows, new_rows) = {
            let tables = self.relational_store.tables.read();
            let rows = tables.get(table).map(Vec::as_slice).unwrap_or_default();
            // Only a live version owns a charge on either side: a tombstoned
            // version handed its bytes back at the commit that stamped it.
            let retained = |meta: &TableMeta| {
                rows.iter()
                    .filter(|row| retain_deleted || row.deleted_tx.is_none())
                    .fold(0usize, |bytes, row| {
                        let values = self.relational_store.committed_projection_values(meta, row);
                        bytes
                            .saturating_add(retained_row_bytes_for_meta(table, &values, meta, true))
                    })
            };
            (retained(old_meta), retained(new_meta))
        };
        // The committed rows are read above under `tables.read()`; the staged
        // ones are read below with that guard already dropped, so this door
        // never holds the table image and the transaction table at once.
        let (staged_old, staged_new, staged_repricing) =
            self.staged_rows_priced_across_shape_change(table, old_meta, new_meta);
        PreparedMemorySwap::prepare_for(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            retained_table_metadata_bytes(&self.relational_store, old_meta)
                .saturating_add(old_rows)
                .saturating_add(staged_old),
            retained_table_metadata_bytes(&self.relational_store, new_meta)
                .saturating_add(new_rows)
                .saturating_add(staged_new),
            0,
            "ddl",
            "index_set_change",
            "Reduce the number of indexes on this table or raise MEMORY_LIMIT before changing \
             its schema.",
        )
        .map(|swap| {
            swap.carrying_staged_admissions(StagedAdmissionRepricing {
                tx_mgr: self.tx_mgr.clone(),
                per_tx: staged_repricing,
            })
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn prepare_local_index_projection(
        &self,
        meta: &TableMeta,
        rows: &[VersionedRow],
    ) -> Result<HashMap<String, contextdb_relational::IndexStorage>> {
        RelationalStore::projected_index_storage_with_budget(
            meta,
            rows,
            self.finalization.accountant.read_budget()?,
        )
    }

    /// Publish the already-durable local table projection.  This is the sole
    /// executor path that changes in-memory table metadata/rows/indexes after
    /// the matching DDL and provenance have committed.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn publish_local_table_projection_and_ddl(
        &self,
        name: &str,
        meta: TableMeta,
        rows: Vec<VersionedRow>,
        _lsn: Lsn,
        _ddl: &[DdlChange],
        projected_indexes: HashMap<String, contextdb_relational::IndexStorage>,
        memory_swap: &mut PreparedMemorySwap,
        memory_history: Option<PreparedSchemaHistory>,
    ) -> Result<()> {
        let store = self.relational_store();
        if !store.publish_table_projection_with_generation(
            name,
            meta,
            rows,
            projected_indexes,
            memory_swap.history.take(),
        ) {
            return Err(Error::TableNotFound(name.to_string()));
        }
        self.publish_memory_schema_history(memory_history);
        Ok(())
    }

    pub(crate) fn project_table_meta_with_auto_indexes(&self, mut meta: TableMeta) -> TableMeta {
        let user_indexes = meta
            .indexes
            .iter()
            .filter(|index| index.kind == IndexKind::UserDeclared)
            .cloned()
            .collect::<Vec<_>>();
        meta.indexes = crate::executor::auto_indexes_for_table_meta(&meta);
        meta.indexes.extend(user_indexes);
        meta
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn persist_table_meta_rows_vectors_and_append_alter_table_ddl(
        &self,
        name: &str,
        meta: &TableMeta,
        lsn: Lsn,
        extra_constraints: Vec<String>,
        mut prior_ddl: Vec<DdlChange>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let change = alter_table_ddl_change(name, meta, extra_constraints);
        prior_ddl.push(change);
        let memory_history =
            self.prepare_memory_ddl_append(lsn, &prior_ddl, read_memory.clone())?;
        if let Some(source) =
            crate::persistence::received_working_source::WorkingReadSource::current_for(
                self.tx_mgr.identity(),
            )?
        {
            source.project_schema(name, Some(meta), &prior_ddl)?;
        }
        if let Some(persistence) = &self.persistence {
            // Replayed rename/drop and policy changes use the same unpublished
            // record cursor as local SQL. A file-backed store has no resident
            // row image: gathering its retained history here would defeat the
            // bounded projection even though durability can consume the source
            // directly. Storage-shape replacements retain their separate path.
            let streamed = self.relational_store.uses_disk_backed_committed_bodies()
                && persistence.supports_streamed_schema_projection(name, meta)?;
            let rows = if streamed {
                Vec::new()
            } else {
                self.table_versions_for_projection(name)?
            };
            let values =
                self.ddl_generation_sidecar_values(lsn, &prior_ddl, 0, read_memory.clone())?;
            self.publish_direct_metadata(|| {
                if streamed {
                    let values = values
                        .iter()
                        .map(|(key, value)| (key.as_str(), value.clone()))
                        .collect::<Vec<_>>();
                    if !persistence
                        .stream_scalar_schema_projection(name, meta, lsn, &prior_ddl, &values)?
                    {
                        return Err(Error::Other(
                            "the prepared schema cursor changed before durability".into(),
                        ));
                    }
                    return Ok(());
                }
                persistence.rewrite_table_meta_rows_and_append_ddl_log(
                    name,
                    meta,
                    &rows,
                    lsn,
                    &prior_ddl,
                    values
                        .iter()
                        .map(|(key, value)| (key.as_str(), value.clone()))
                        .collect(),
                )
            })?;
        }
        self.publish_memory_schema_history(memory_history);
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn prepare_memory_ddl_append(
        &self,
        lsn: Lsn,
        ddl: &[DdlChange],
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<PreparedSchemaHistory>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if self.persistence.is_some() {
            return Ok(None);
        }
        let start = self
            .ddl_log
            .read()
            .iter()
            .filter(|(at, _)| *at == lsn)
            .count() as u32;
        let sidecars = self.ddl_generation_sidecars(ddl, read_memory)?;
        self.prepare_memory_schema_history(lsn, start, ddl, &sidecars)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn append_ddl_change(
        &self,
        lsn: Lsn,
        change: DdlChange,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let memory_history = self.prepare_memory_ddl_append(
            lsn,
            std::slice::from_ref(&change),
            read_memory.clone(),
        )?;
        if let Some(source) =
            crate::persistence::received_working_source::WorkingReadSource::current_for(
                self.tx_mgr.identity(),
            )?
        {
            source.apply_ddl(&change)?;
        }
        if let Some(persistence) = &self.persistence {
            let values = self.ddl_generation_sidecar_values(
                lsn,
                std::slice::from_ref(&change),
                0,
                read_memory.clone(),
            )?;
            self.publish_direct_metadata(|| {
                persistence.flush_encoded_config_values_and_append_ddl_log(
                    values
                        .iter()
                        .map(|(key, value)| (key.as_str(), value.clone()))
                        .collect(),
                    lsn,
                    std::slice::from_ref(&change),
                )
            })?;
        }
        self.publish_memory_schema_history(memory_history);
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn log_create_index_ddl(
        &self,
        table: &str,
        name: &str,
        columns: &[(String, contextdb_core::SortDirection)],
        lsn: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let change = DdlChange::CreateIndex {
            table: table.to_string(),
            name: name.to_string(),
            columns: columns.to_vec(),
        };
        self.append_ddl_change(lsn, change, read_memory.clone())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn log_drop_index_ddl(
        &self,
        table: &str,
        name: &str,
        lsn: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let change = DdlChange::DropIndex {
            table: table.to_string(),
            name: name.to_string(),
        };
        self.append_ddl_change(lsn, change, read_memory.clone())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn persist_table_meta(&self, name: &str, meta: &TableMeta) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            self.publish_direct_metadata(|| persistence.flush_table_meta(name, meta))?;
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn set_memory_limit(&self, limit: Option<usize>) -> Result<()> {
        let _operation = self.open_operation()?;
        let _update = self.limit_update_lock.lock();
        let usage = self.finalization.accountant.usage();
        if let Some(ceiling) = usage.startup_ceiling {
            match limit {
                Some(bytes) if bytes > ceiling => {
                    return Err(Error::InvalidSetting {
                        name: "MEMORY_LIMIT",
                        value: bytes.to_string(),
                        allowed: format!(
                            "at most {} (startup ceiling)",
                            memory_ceiling_description(ceiling)
                        ),
                    });
                }
                None => {
                    return Err(Error::InvalidSetting {
                        name: "MEMORY_LIMIT",
                        value: "NONE".to_owned(),
                        allowed: format!(
                            "cannot remove memory limit when a startup ceiling is set ({})",
                            memory_ceiling_description(ceiling)
                        ),
                    });
                }
                _ => {}
            }
        }

        // The write that records a limit is itself charged to the budget, so
        // the two halves of a transition have to be ordered without ever
        // publishing capacity the durable value has not committed to.
        //
        // The rule: while the transition is in flight the PUBLISHED budget is
        // the smaller of where it started and where it is going, so no
        // concurrent operation can consume capacity that may never commit and
        // none keeps allocating above a smaller value already on disk. The
        // control write itself runs under thread-private headroom up to the
        // larger of the two, which no other thread and no other operation can
        // see, so a limit too small to write under is still recoverable. The
        // pair stays transactional: a failed durable write leaves the runtime
        // budget back where the durable value still is, and the target is
        // published only once it has committed.
        let previous = usage.limit;
        let Some(persistence) = &self.persistence else {
            return self.finalization.accountant.set_budget(limit);
        };
        let persist = |persistence: &RedbPersistence| match limit {
            Some(limit) => persistence.flush_config_value("memory_limit", &limit),
            None => persistence.remove_config_value("memory_limit"),
        };
        let (published_during, headroom) = match (previous, limit) {
            (Some(previous), Some(target)) => (
                Some(previous.min(target)),
                previous.max(target) - previous.min(target),
            ),
            // Moving to no limit at all: the durable write runs with the
            // capacity it is moving to, privately, and the old ceiling stays
            // published until that write commits.
            (Some(previous), None) => (Some(previous), usize::MAX - previous),
            // Coming from no limit: the new ceiling is published first, and the
            // write runs under the unbounded capacity it is leaving.
            (None, Some(target)) => (Some(target), usize::MAX - target),
            (None, None) => (None, 0),
        };
        if published_during != previous {
            self.finalization.accountant.set_budget(published_during)?;
        }
        #[cfg(any(test, feature = "test-seams"))]
        memory_limit_transition_seam::observe(
            memory_limit_transition_seam::TransitionPhase::BeforeDurableWrite,
        );
        let outcome = self
            .finalization
            .accountant
            .with_transition_headroom(headroom, || {
                self.publish_direct_metadata(|| persist(persistence))
            });
        #[cfg(any(test, feature = "test-seams"))]
        memory_limit_transition_seam::observe(
            memory_limit_transition_seam::TransitionPhase::AfterDurableWrite,
        );
        if let Err(error) = outcome {
            // Nothing was published that the durable value does not still say,
            // so recovery is simply putting the published budget back.
            if published_during != previous {
                let _ = self.finalization.accountant.set_budget(previous);
            }
            return Err(error);
        }
        self.finalization.accountant.set_budget(limit)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn set_disk_limit(&self, limit: Option<u64>) -> Result<()> {
        let _operation = self.open_operation()?;
        let _update = self.limit_update_lock.lock();
        if self.persistence.is_none() {
            self.disk_limit.store(0, Ordering::SeqCst);
            return Ok(());
        }

        let ceiling = self.disk_limit_startup_ceiling();
        if let Some(ceiling) = ceiling {
            match limit {
                Some(bytes) if bytes > ceiling => {
                    return Err(Error::Other(
                        (format!("disk limit {bytes} exceeds startup ceiling {ceiling}")).into(),
                    ));
                }
                None => {
                    return Err(Error::Other(
                        ("cannot remove disk limit when a startup ceiling is set".to_string())
                            .into(),
                    ));
                }
                _ => {}
            }
        }

        if let Some(persistence) = &self.persistence {
            let previous = self.disk_limit();
            let raises_limit = match (previous, limit) {
                (_, None) => true,
                (Some(previous), Some(next)) => next > previous,
                (None, Some(_)) => false,
            };
            if raises_limit {
                persistence.set_disk_limit(limit)?;
            }
            let persisted = self.publish_direct_metadata(|| match limit {
                Some(limit) => persistence.flush_config_value("disk_limit", &limit),
                None => persistence.remove_config_value("disk_limit"),
            });
            if let Err(error) = persisted {
                if raises_limit {
                    persistence.set_disk_limit(previous)?;
                }
                return Err(error);
            }
            if !raises_limit {
                persistence.set_disk_limit(limit)?;
            }
        }
        self.disk_limit.store(limit.unwrap_or(0), Ordering::SeqCst);
        Ok(())
    }

    pub fn disk_limit(&self) -> Option<u64> {
        let _operation = self.assert_open_operation();
        match self.disk_limit.load(Ordering::SeqCst) {
            0 => None,
            bytes => Some(bytes),
        }
    }

    pub fn disk_limit_startup_ceiling(&self) -> Option<u64> {
        let _operation = self.assert_open_operation();
        match self.disk_limit_startup_ceiling.load(Ordering::SeqCst) {
            0 => None,
            bytes => Some(bytes),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn content_size_totals(
        &self,
        memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        before_table: &mut dyn FnMut() -> Result<()>,
    ) -> Result<crate::persistence::content_size::ContentSizeTotals> {
        let memory: contextdb_core::read_memory::ReadMemoryOwner = memory.into();

        if let Some(image) = &self.committed_image_startup {
            return image
                .config_source
                .content_size_totals(memory, before_table);
        }
        if let Some(totals) = self.writer_content_size_totals(memory.clone(), before_table)? {
            return Ok(totals);
        }
        match &self.persistence {
            None => Err(Error::ReadFailure(
                contextdb_core::read_contract::ReadFailure::owner_route_unsupported(
                    "SHOW CONTENT_SIZE has no persisted committed values in a memory-only store"
                        .into(),
                ),
            )),
            Some(persistence) => Err(crate::funded_error::corruption_text(
                Ok(memory.clone()),
                persistence.path().display(),
                "content-size inspection has no captured committed root".into(),
            )),
        }
    }

    pub fn disk_file_size(&self) -> Option<u64> {
        let _operation = self.assert_open_operation();
        // A handle reading a committed image holds no persistence, but it was
        // read OUT OF a file and that file's length came with it. Answering
        // `None` there made the same question about the same store answer
        // with a number over a channel and with nothing at all against the
        // file, when the file is the very thing the number measures.
        if self.persistence.is_none() {
            return self.image_store_file_bytes;
        }
        self.persistence
            .as_ref()
            .map(|persistence| std::fs::metadata(persistence.path()).map(|meta| meta.len()))
            .transpose()
            .ok()
            .flatten()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn check_disk_budget(&self, operation: &str) -> Result<()> {
        let _operation = self.open_operation()?;
        let Some(limit) = self.disk_limit() else {
            return Ok(());
        };
        let Some(current_bytes) = self.disk_file_size() else {
            return Ok(());
        };
        if current_bytes.saturating_add(MIN_DISK_WRITE_HEADROOM_BYTES) <= limit {
            return Ok(());
        }
        Err(Error::DiskBudgetExceeded {
            operation: operation.to_string(),
            current_bytes,
            budget_limit_bytes: limit,
            hint: "Reduce retained file-backed data or raise DISK_LIMIT before writing more data."
                .to_string(),
        })
    }

    pub(super) const OUTBOUND_SCHEMA_HOLDBACK_PREFIX: &'static str =
        "sync_schema_holdback_outbound.v1.";
    pub(super) const INBOUND_SCHEMA_HOLDBACK_PREFIX: &'static str =
        "sync_schema_holdback_inbound.v1.";

    /// The RECEIVER's own holdback: tables whose arriving schema unit did not
    /// fit this node's declared `MEMORY_LIMIT`. Its own key so a node that is
    /// both a hub and an edge never overwrites one role's record with the
    /// other's, exactly as the inbound and outbound keys are already separate.
    pub(super) const MEMORY_SCHEMA_HOLDBACK_PREFIX: &'static str =
        "sync_schema_holdback_memory.v1.";

    pub(super) fn schema_holdback_config_key(
        prefix: &str,
        tenant_id: &TenantId,
        peer_node_id: &str,
    ) -> String {
        tenant_id.config_key(&format!("{prefix}{peer_node_id}"))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn persisted_memory_schema_sync_holdback(
        &self,
        tenant_id: &TenantId,
        peer_node_id: &str,
    ) -> Result<Option<DurableSchemaSyncHoldback>> {
        let key = Self::schema_holdback_config_key(
            Self::MEMORY_SCHEMA_HOLDBACK_PREFIX,
            tenant_id,
            peer_node_id,
        );
        if let Some(persistence) = &self.persistence {
            return persistence.load_config_value(&key);
        }
        Ok(self
            .in_memory_sync_progress
            .lock()
            .schema_holdbacks
            .get(&key)
            .cloned())
    }

    /// Make every write of the receiver's durable memory-hold record fail on
    /// this thread, the way a full disk or a store already at its declared
    /// limit does.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn refuse_memory_schema_holdback_writes_for_test(refuse: bool) {
        REFUSE_MEMORY_HOLDBACK_WRITE.with(|flag| flag.set(refuse));
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn persist_memory_schema_sync_holdback(
        &self,
        tenant_id: &TenantId,
        peer_node_id: &str,
        state: Option<&DurableSchemaSyncHoldback>,
    ) -> Result<()> {
        #[cfg(any(test, feature = "test-seams"))]
        if REFUSE_MEMORY_HOLDBACK_WRITE.with(std::cell::Cell::get) {
            return Err(Error::Other(
                ("injected durable memory-hold write failure".to_string()).into(),
            ));
        }
        let key = Self::schema_holdback_config_key(
            Self::MEMORY_SCHEMA_HOLDBACK_PREFIX,
            tenant_id,
            peer_node_id,
        );
        if let Some(persistence) = &self.persistence {
            return self.publish_direct_metadata(|| {
                persistence.flush_memory_schema_holdback_control(&key, state)
            });
        }
        let mut progress = self.in_memory_sync_progress.lock();
        match state {
            Some(state) => {
                progress.schema_holdbacks.insert(key, state.clone());
            }
            None => {
                progress.schema_holdbacks.remove(&key);
            }
        }
        Ok(())
    }

    /// Every table this receiver is holding an arriving schema unit back on for
    /// memory, per peer. Empty is the ordinary state; an entry means the
    /// operator's declared limit could not hold a whole arriving unit and that
    /// table's schema is behind its author until the limit allows it.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn memory_held_received_schema_units(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Vec<MemoryHeldReceivedSchemaUnit>> {
        let _operation = self.open_operation()?;
        let tenant_suffix = format!(":{}", tenant_id.as_str());
        let states = if let Some(persistence) = &self.persistence {
            persistence
                .load_config_values_with_prefix::<DurableSchemaSyncHoldback>(
                    Self::MEMORY_SCHEMA_HOLDBACK_PREFIX,
                )?
                .into_iter()
                .filter_map(|(key, state)| key.ends_with(&tenant_suffix).then_some(state))
                .collect::<Vec<_>>()
        } else {
            self.in_memory_sync_progress
                .lock()
                .schema_holdbacks
                .iter()
                .filter(|(key, _)| {
                    key.starts_with(Self::MEMORY_SCHEMA_HOLDBACK_PREFIX)
                        && key.ends_with(&tenant_suffix)
                })
                .map(|(_, state)| state.clone())
                .collect::<Vec<_>>()
        };
        let mut held = states
            .into_iter()
            .flat_map(|state| {
                let peer_node_id = state.peer_node_id.clone();
                state
                    .memory_held_tables
                    .into_iter()
                    .map(move |(table, hold)| MemoryHeldReceivedSchemaUnit {
                        peer_node_id: peer_node_id.clone(),
                        table,
                        held_at: hold.held_at,
                        required_bytes: hold.required_bytes,
                    })
            })
            .collect::<Vec<_>>();
        held.sort();
        Ok(held)
    }

    /// Every table one arriving DDL unit reshapes.
    pub(super) fn sync_ddl_unit_tables(ddl: &[DdlChange]) -> BTreeSet<String> {
        ddl.iter()
            .filter_map(|change| match change {
                DdlChange::CreateTable { name, .. }
                | DdlChange::AlterTable { name, .. }
                | DdlChange::DropTable { name } => Some(name.clone()),
                DdlChange::CreateIndex { table, .. } | DdlChange::DropIndex { table, .. } => {
                    Some(table.clone())
                }
                _ => None,
            })
            .collect()
    }

    /// Who a memory hold is recorded against, and where in that source's order
    /// the unit stands.
    ///
    /// A signed unit is held against its author, at the earliest DDL LSN the
    /// author gave it -- the frontier this receiver has not passed and the one a
    /// retry starts from. A unit with no provenance has no author to be held
    /// against, but it still has a table it is waiting on and an order it
    /// arrived in, and a later unit for that table must still wait behind it.
    /// So it is held against one reserved no-provenance source, at the earliest
    /// DDL LSN it carries.
    pub(super) fn received_unit_hold_identity(
        ddl_lsn: &[Lsn],
        received: Option<&crate::protocol::ReceivedDdlContext>,
    ) -> ReceivedUnitHoldIdentity {
        match received {
            Some(received) => ReceivedUnitHoldIdentity {
                tenant_id: received.tenant_id.clone(),
                source_node_id: received.source_node_id.clone(),
                position: received
                    .entries
                    .iter()
                    .map(|entry| entry.source_ddl_lsn)
                    .min()
                    .unwrap_or(Lsn(0)),
            },
            None => ReceivedUnitHoldIdentity {
                tenant_id: TenantId::from(UNATTESTED_RECEIVED_UNIT_HOLD_SOURCE),
                source_node_id: UNATTESTED_RECEIVED_UNIT_HOLD_SOURCE.to_string(),
                position: ddl_lsn.iter().copied().min().unwrap_or(Lsn(0)),
            },
        }
    }

    /// Record, durably and per table, that a whole arriving schema unit did not
    /// fit this node's declared limit.
    ///
    /// Nothing of the unit was applied and nothing was logged -- the refusal
    /// came from its price or from the private copy of the tables it reshapes,
    /// before anything was published -- so the author is never told it landed: the pull cursor does not advance and the unit is
    /// re-delivered. The hold is what makes that state visible and durable, and
    /// what keeps LATER units for the SAME table from installing out of author
    /// order while this one waits. Units for every other table are untouched.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn hold_back_received_unit_for_memory(
        &self,
        ddl: &[DdlChange],
        holder: &ReceivedUnitHoldIdentity,
        refusal: &Error,
    ) -> Result<()> {
        let Error::MemoryBudgetExceeded {
            requested_bytes,
            operation,
            ..
        } = refusal
        else {
            return Ok(());
        };
        // Refused FOR a hold that already exists: the unit waiting there is the
        // one the hold names, and this later one simply queues behind it.
        if operation == RECEIVED_SCHEMA_MEMORY_HOLDBACK_OPERATION {
            return Ok(());
        }
        let tables = Self::sync_ddl_unit_tables(ddl);
        if tables.is_empty() {
            return Ok(());
        }
        let held_at = holder.position;
        let mut state = self
            .persisted_memory_schema_sync_holdback(&holder.tenant_id, &holder.source_node_id)?
            .unwrap_or_else(|| DurableSchemaSyncHoldback {
                peer_node_id: holder.source_node_id.clone(),
                ..Default::default()
            });
        state.peer_node_id = holder.source_node_id.clone();
        for table in tables {
            state.memory_held_tables.entry(table).or_insert(
                crate::sync_types::SchemaSyncMemoryHold {
                    held_at,
                    required_bytes: *requested_bytes,
                },
            );
        }
        state.held_through = state.held_through.max(held_at);
        self.persist_memory_schema_sync_holdback(
            &holder.tenant_id,
            &holder.source_node_id,
            Some(&state),
        )
    }

    /// Refuse a unit that arrives for a table already held back for memory at
    /// an EARLIER author position, so the held table's schema cannot skip ahead
    /// of the unit that is waiting. The unit AT the held position is the retry
    /// and goes through.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn refuse_received_unit_behind_a_memory_holdback(
        &self,
        ddl: &[DdlChange],
        holder: &ReceivedUnitHoldIdentity,
    ) -> Result<()> {
        if ddl.is_empty() {
            return Ok(());
        }
        let Some(state) =
            self.persisted_memory_schema_sync_holdback(&holder.tenant_id, &holder.source_node_id)?
        else {
            return Ok(());
        };
        if state.memory_held_tables.is_empty() {
            return Ok(());
        }
        let arriving_at = holder.position;
        for table in Self::sync_ddl_unit_tables(ddl) {
            if let Some(hold) = state.memory_held_tables.get(&table)
                && hold.held_at < arriving_at
            {
                let usage = self.finalization.accountant.usage();
                return Err(Error::MemoryBudgetExceeded {
                    subsystem: ("sync".to_string()).into(),
                    operation: (RECEIVED_SCHEMA_MEMORY_HOLDBACK_OPERATION.to_string()).into(),
                    requested_bytes: hold.required_bytes,
                    available_bytes: usage.available.unwrap_or(0),
                    budget_limit_bytes: usage.limit.unwrap_or(0),
                    hint: (format!(
                        "table '{table}' is waiting for memory: the schema unit at position {} \
                         from node '{}' needed {} bytes and did not fit, so later units for that \
                         table queue behind it. Raise MEMORY_LIMIT and sync resumes \
                         automatically; tables not listed keep moving.",
                        hold.held_at.0, holder.source_node_id, hold.required_bytes,
                    ))
                    .into(),
                });
            }
        }
        Ok(())
    }

    /// Drop the memory hold on every table a unit has now installed.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn release_memory_schema_holdback_for_applied_unit(
        &self,
        tables: &BTreeSet<String>,
        holder: &ReceivedUnitHoldIdentity,
    ) -> Result<()> {
        if tables.is_empty() {
            return Ok(());
        }
        let Some(mut state) =
            self.persisted_memory_schema_sync_holdback(&holder.tenant_id, &holder.source_node_id)?
        else {
            return Ok(());
        };
        let before = state.memory_held_tables.len();
        state
            .memory_held_tables
            .retain(|table, _| !tables.contains(table));
        if state.memory_held_tables.len() == before {
            return Ok(());
        }
        let remaining = (!state.memory_held_tables.is_empty()).then_some(state);
        self.persist_memory_schema_sync_holdback(
            &holder.tenant_id,
            &holder.source_node_id,
            remaining.as_ref(),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn persisted_outbound_schema_sync_holdback(
        &self,
        tenant_id: &TenantId,
        peer_node_id: &str,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<DurableSchemaSyncHoldback>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        let _writer_read = self.writer_read_scope(read_memory.clone())?;
        let key = Self::schema_holdback_config_key(
            Self::OUTBOUND_SCHEMA_HOLDBACK_PREFIX,
            tenant_id,
            peer_node_id,
        );
        if let Some(_persistence) = &self.persistence {
            return self.read_config_value(&key, read_memory.clone());
        }
        Ok(self
            .in_memory_sync_progress
            .lock()
            .schema_holdbacks
            .get(&key)
            .cloned())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn persist_outbound_schema_sync_holdback_while_authoritative(
        &self,
        tenant_id: &TenantId,
        peer_node_id: &str,
        state: Option<&DurableSchemaSyncHoldback>,
    ) -> Result<()> {
        let key = Self::schema_holdback_config_key(
            Self::OUTBOUND_SCHEMA_HOLDBACK_PREFIX,
            tenant_id,
            peer_node_id,
        );
        if let Some(persistence) = &self.persistence {
            match state {
                Some(state) => {
                    self.publish_direct_metadata(|| persistence.flush_config_value(&key, state))?
                }
                None => self.publish_direct_metadata(|| persistence.remove_config_value(&key))?,
            }
            return Ok(());
        }
        let mut progress = self.in_memory_sync_progress.lock();
        match state {
            Some(state) => {
                progress.schema_holdbacks.insert(key, state.clone());
            }
            None => {
                progress.schema_holdbacks.remove(&key);
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn persisted_inbound_schema_sync_holdbacks(
        &self,
        tenant_id: &TenantId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<DurableSchemaSyncHoldback>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        let _writer_read = self.writer_read_scope(read_memory.clone())?;
        let tenant_suffix = format!(":{}", tenant_id.as_str());
        let mut states = if let Some(_persistence) = &self.persistence {
            self.read_config_values_with_prefix::<DurableSchemaSyncHoldback>(
                Self::INBOUND_SCHEMA_HOLDBACK_PREFIX,
                read_memory.clone(),
            )?
            .into_iter()
            .filter_map(|(key, state)| key.ends_with(&tenant_suffix).then_some(state))
            .collect::<Vec<_>>()
        } else {
            self.in_memory_sync_progress
                .lock()
                .schema_holdbacks
                .iter()
                .filter(|(key, _)| {
                    key.starts_with(Self::INBOUND_SCHEMA_HOLDBACK_PREFIX)
                        && key.ends_with(&tenant_suffix)
                })
                .map(|(_, state)| state.clone())
                .collect::<Vec<_>>()
        };
        states.sort_by(|left, right| left.peer_node_id.cmp(&right.peer_node_id));
        Ok(states)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn persist_inbound_schema_sync_holdback(
        &self,
        tenant_id: &TenantId,
        peer_node_id: &str,
        state: Option<&DurableSchemaSyncHoldback>,
    ) -> Result<()> {
        let _operation = self.open_operation()?;
        let key = Self::schema_holdback_config_key(
            Self::INBOUND_SCHEMA_HOLDBACK_PREFIX,
            tenant_id,
            peer_node_id,
        );
        if let Some(persistence) = &self.persistence {
            match state {
                Some(state) => {
                    self.publish_direct_metadata(|| persistence.flush_config_value(&key, state))?
                }
                None => self.publish_direct_metadata(|| persistence.remove_config_value(&key))?,
            }
            return Ok(());
        }
        let mut progress = self.in_memory_sync_progress.lock();
        match state {
            Some(state) => {
                progress.schema_holdbacks.insert(key, state.clone());
            }
            None => {
                progress.schema_holdbacks.remove(&key);
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn persisted_sync_watermarks(&self, tenant_id: &TenantId) -> Result<(Lsn, Lsn)> {
        let read_memory = self.declared_reader_memory()?;
        self.persisted_sync_watermarks_with_read_memory(tenant_id, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn persisted_sync_watermarks_with_read_memory(
        &self,
        tenant_id: &TenantId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(Lsn, Lsn)> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        let _writer_read = self.writer_read_scope(read_memory.clone())?;
        let Some(_persistence) = &self.persistence else {
            let progress = self.in_memory_sync_progress.lock();
            let progress = progress.tenants.get(tenant_id.as_str());
            return Ok((
                progress
                    .map(|progress| progress.push_watermark)
                    .unwrap_or_default(),
                progress
                    .map(|progress| progress.pull_watermark)
                    .unwrap_or_default(),
            ));
        };
        let push = self
            .read_config_value::<u64>(
                &tenant_id.config_key("sync_push_watermark"),
                read_memory.clone(),
            )?
            .map(Lsn)
            .unwrap_or(Lsn(0));
        let pull = self
            .read_config_value::<u64>(
                &tenant_id.config_key("sync_pull_watermark"),
                read_memory.clone(),
            )?
            .map(Lsn)
            .unwrap_or(Lsn(0));
        Ok((push, pull))
    }

    sync_test_seam! {
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    fn persist_sync_push_watermark(&self, tenant_id: &TenantId, watermark: Lsn) -> Result<()> {
        let _operation = self.open_operation()?;
        if let Some(persistence) = &self.persistence {
            self.publish_direct_metadata(|| persistence.flush_sync_progress_value(
                &tenant_id.config_key("sync_push_watermark"),
                &watermark.0,
            ))?;
        } else {
            self.in_memory_sync_progress
                .lock()
                .tenants
                .entry(tenant_id.as_str().to_string())
                .or_default()
                .push_watermark = watermark;
        }
        Ok(())
    }
    }

    sync_test_seam! {
    #[cfg(any(test, feature = "test-seams"))]
    /// Persist source progress only while the same authenticated hub remains
    /// authoritative. The peer lock is shared with destination change, so an
    /// old-hub acknowledgement can land either wholly before the reset or be
    /// refused wholly after it, never restore a stale frontier in between.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn persist_sync_push_watermark_for_hub(
        &self,
        tenant_id: &TenantId,
        watermark: Lsn,
        hub_node_id: &str,
    ) -> Result<()> {
        self.with_authoritative_hub_reply(hub_node_id, |db| {
            db.persist_sync_push_watermark_while_authoritative(tenant_id, watermark)
        })
    }

    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    pub(crate) fn persist_sync_push_watermark_while_authoritative(
        &self,
        tenant_id: &TenantId,
        watermark: Lsn,
    ) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            self.publish_direct_metadata(|| persistence.flush_sync_progress_value(
                &tenant_id.config_key("sync_push_watermark"),
                &watermark.0,
            ))?;
        } else {
            self.in_memory_sync_progress
                .lock()
                .tenants
                .entry(tenant_id.as_str().to_string())
                .or_default()
                .push_watermark = watermark;
        }
        Ok(())
    }
    }

    /// A hub status probe proved that this edge's push reached the hub, but the
    /// edge has not yet pulled the hub's exact cursor events that establish
    /// their provenance. Redb retains this marker across database reopen and
    /// process restart; memory retains it only for reconstructed or scoped
    /// clients sharing this database life, so they cannot resend the same
    /// stampless rows before that pull completes.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn persisted_sync_pending_push_confirmation(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Option<Lsn>> {
        let read_memory = self.declared_reader_memory()?;
        self.persisted_sync_pending_push_confirmation_with_read_memory(tenant_id, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn persisted_sync_pending_push_confirmation_with_read_memory(
        &self,
        tenant_id: &TenantId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<Lsn>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        let _writer_read = self.writer_read_scope(read_memory.clone())?;
        let Some(_persistence) = &self.persistence else {
            return Ok(self
                .in_memory_sync_progress
                .lock()
                .tenants
                .get(tenant_id.as_str())
                .and_then(|progress| progress.pending_push_confirmation));
        };
        self.read_config_value::<u64>(
            &tenant_id.config_key("sync_pending_push_confirmation"),
            read_memory.clone(),
        )
        .map(|value| value.map(Lsn))
    }

    sync_test_seam! {
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    fn persist_sync_pending_push_confirmation(
        &self,
        tenant_id: &TenantId,
        watermark: Option<Lsn>,
    ) -> Result<()> {
        let _operation = self.open_operation()?;
        if let Some(persistence) = &self.persistence {
            let key = tenant_id.config_key("sync_pending_push_confirmation");
            match watermark {
                Some(watermark) => self.publish_direct_metadata(|| persistence.flush_config_value(&key, &watermark.0))?,
                None => {
                    self.publish_direct_metadata(|| persistence.commit_sync_reply_effects(&[], &[], &[
                        key, tenant_id.config_key("sync_pending_push_result"),
                    ]))?;
                },
            }
        } else {
            let mut progress = self.in_memory_sync_progress.lock();
            match watermark {
                Some(watermark) => {
                    progress
                        .tenants
                        .entry(tenant_id.as_str().to_string())
                        .or_default()
                        .pending_push_confirmation = Some(watermark);
                }
                None => {
                    if let Some(tenant) = progress.tenants.get_mut(tenant_id.as_str()) {
                        tenant.pending_push_confirmation = None;
                        tenant.pending_push_result = None;
                    }
                }
            }
        }
        Ok(())
    }
    }

    sync_test_seam! {
    #[cfg(any(test, feature = "test-seams"))]
    /// Hub-bound form of the lost-ack marker writer. Destination change holds
    /// the same peer lock while clearing the old marker and rebinding, so an
    /// old response cannot recreate its reconciliation obligation afterward.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn persist_sync_pending_push_confirmation_for_hub(
        &self,
        tenant_id: &TenantId,
        watermark: Option<Lsn>,
        hub_node_id: &str,
    ) -> Result<()> {
        self.with_authoritative_hub_reply(hub_node_id, |db| {
            db.persist_sync_pending_push_confirmation_while_authoritative(tenant_id, watermark)
        })
    }

    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    pub(crate) fn persist_sync_pending_push_confirmation_while_authoritative(
        &self,
        tenant_id: &TenantId,
        watermark: Option<Lsn>,
    ) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            let key = tenant_id.config_key("sync_pending_push_confirmation");
            match watermark {
                Some(watermark) => self.publish_direct_metadata(|| persistence.flush_config_value(&key, &watermark.0))?,
                None => {
                    self.publish_direct_metadata(|| persistence.commit_sync_reply_effects(&[], &[], &[
                        key, tenant_id.config_key("sync_pending_push_result"),
                    ]))?;
                },
            }
        } else {
            let mut progress = self.in_memory_sync_progress.lock();
            match watermark {
                Some(watermark) => {
                    progress
                        .tenants
                        .entry(tenant_id.as_str().to_string())
                        .or_default()
                        .pending_push_confirmation = Some(watermark);
                }
                None => {
                    if let Some(tenant) = progress.tenants.get_mut(tenant_id.as_str()) {
                        tenant.pending_push_confirmation = None;
                        tenant.pending_push_result = None;
                    }
                }
            }
        }
        Ok(())
    }
    }

    sync_test_seam! {
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    fn persist_sync_pull_watermark(&self, tenant_id: &TenantId, watermark: Lsn) -> Result<()> {
        let _operation = self.open_operation()?;
        if let Some(persistence) = &self.persistence {
            self.publish_direct_metadata(|| persistence
                .flush_config_value(&tenant_id.config_key("sync_pull_watermark"), &watermark.0))?;
        } else {
            self.in_memory_sync_progress
                .lock()
                .tenants
                .entry(tenant_id.as_str().to_string())
                .or_default()
                .pull_watermark = watermark;
        }
        Ok(())
    }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn persist_sync_pull_watermark_while_authoritative(
        &self,
        tenant_id: &TenantId,
        watermark: Lsn,
    ) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            self.publish_direct_metadata(|| {
                persistence
                    .flush_config_value(&tenant_id.config_key("sync_pull_watermark"), &watermark.0)
            })?;
        } else {
            self.in_memory_sync_progress
                .lock()
                .tenants
                .entry(tenant_id.as_str().to_string())
                .or_default()
                .pull_watermark = watermark;
        }
        Ok(())
    }
}

fn memory_ceiling_description(bytes: usize) -> String {
    const MIB: usize = 1024 * 1024;
    if bytes.is_multiple_of(MIB) {
        format!("{} MiB ({bytes} bytes)", bytes / MIB)
    } else {
        format!("{bytes} bytes")
    }
}
