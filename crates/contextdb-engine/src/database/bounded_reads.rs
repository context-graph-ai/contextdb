use super::*;

impl Database {

    /// Whether `table` has an index whose leading key covers `column`. A
    /// declared `PRIMARY KEY`/`UNIQUE` column is auto-indexed at
    /// `CREATE`/`ALTER` time and always answers `true`; a bare `id`-named
    /// column with neither declaration is NOT auto-indexed, so this can
    /// answer `false` for it. Exposed so a caller resolving sync eligibility
    /// on the `id`-column fallback (see `natural_key_columns_for_meta`) can
    /// require the SAME covering index the apply side's exact-key probe
    /// already hard-errors without, rather than trusting the column's name
    /// alone.
    pub fn column_has_covering_index(&self, table: &str, column: &str) -> bool {
        let _operation = self.assert_open_operation();
        self.index_covers_column(table, column)
    }

    /// Execute a query at a specific snapshot.
    ///
    /// Relational reads and `GRAPH_TABLE` traversal inside the query see state
    /// at or before `snapshot`. Under constrained handles, explicit anchor
    /// reads at the pinned snapshot return typed visibility errors for rows
    /// hidden by context, scope-label, or ACL gates at that snapshot.
    ///
    /// This ONE call is protected against version cleanup for its own
    /// duration: `execute` registers `snapshot` in the engine's read-snapshot
    /// registry for exactly this call, so a superseded version this call
    /// still needs cannot vanish while it runs. That protection ends when
    /// this call returns. Reusing the SAME `snapshot` value in a LATER call
    /// is not automatically protected — see [`Database::pin_snapshot`] if you
    /// need to hold a snapshot across separate calls on a table declaring
    /// `RETAIN` or `HISTORY CURRENT ONLY`.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn execute_at_snapshot(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        snapshot: SnapshotId,
    ) -> Result<QueryResult> {
        #[cfg(any(test, feature = "test-seams"))]
        let _maintenance_phase =
            crate::read_image_memory::MaintenancePhase::enter("execute_at_snapshot");
        let child = self.ordinary_read_child()?;
        crate::executor::bounded::with_native_read_memory(
            child,
            |read_memory, native_trace_refund| {
                QueryResult::publish_with(read_memory.clone(), || {
                    self.execute_at_snapshot_with_native_trace_refund(
                        sql,
                        params,
                        snapshot,
                        read_memory,
                        Some(&native_trace_refund),
                    )
                })
            },
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn execute_at_snapshot_with_read_memory(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        snapshot: SnapshotId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<OwnedQueryResult> {
        self.execute_at_snapshot_with_native_trace_refund(sql, params, snapshot, read_memory, None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "The existing inline native refusal is preserved without allocating an error."
    )]
    pub(crate) fn execute_at_snapshot_with_native_trace_refund(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        snapshot: SnapshotId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        native_trace_refund: Option<&crate::executor::bounded::NativeTraceRefund>,
    ) -> Result<OwnedQueryResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _intent = self.vector_historical_read_intent(snapshot, read_memory.clone());
        self.with_snapshot_override(snapshot, || {
            self.execute_owned_with_native_trace_refund(
                sql,
                params,
                read_memory.clone(),
                native_trace_refund,
            )
        })
    }

    /// Hold `snapshot` across separate calls: while the returned guard lives,
    /// retention and version cleanup defer any version still visible at
    /// `snapshot` to a later cycle rather than reclaiming it. Drop the guard
    /// when you are done — a version deferred only because of a pin is
    /// reclaimed on the next cycle after the pin releases.
    ///
    /// Needed only for a table declaring `RETAIN` or `HISTORY CURRENT ONLY`:
    /// a table that declares neither keeps every version, so a snapshot over
    /// it needs no pin. Without a pin, calling
    /// [`Database::execute_at_snapshot`] twice with the same `snapshot` and a
    /// cleanup cycle in between may silently return fewer rows the second
    /// time — not a wrong value, not an error, just physical cleanup that ran
    /// because nothing told the engine this snapshot was still wanted.
    ///
    /// Protection begins when this call RETURNS: if a removal pass is
    /// already mid-flight for versions older than `snapshot` when this is
    /// called, this call blocks until that pass finishes (bounded by one
    /// pass duration), and only then registers -- so the NEXT cleanup cycle
    /// honors the pin. Versions already reclaimed BEFORE this call runs
    /// cannot come back — that is the pre-existing, documented unpinned-
    /// held-snapshot boundary above, unchanged. A pin whose `snapshot` is
    /// not older than any in-flight pass's own floor never blocks.
    /// The first `ReadLimits::SHIPPED_SNAPSHOT_PIN_SLOTS` registrations use
    /// prepaid capacity. Further pins reserve their exact registration bytes
    /// from the store's declared reader memory until unpin; insufficient
    /// memory returns a typed `snapshot_pin` refusal.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn pin_snapshot(&self, snapshot: SnapshotId) -> contextdb_core::Result<SnapshotPin<'_>> {
        let read_memory = self.declared_reader_memory()?;
        let slot = self
            .snapshot_registry
            .acquire_pin_slot(read_memory.clone())?;
        // An explicit pin starts a new reader even inside an older read scope.
        // Retain the current physical publication and the requested logical
        // visibility independently, without consulting another reader's pin.
        let root = writer_read::durable_root(self.tx_mgr.capture_published_read().handle)?;
        let registration = if root.is_some() {
            self.snapshot_registry.admit(
                self.snapshot_registry.state.lock(),
                snapshot,
                SnapshotRegistrationClass::ReaderHeld,
                None,
            )
        } else {
            self.snapshot_registry.register(snapshot)
        };
        Ok(SnapshotPin {
            _database: self,
            _registration: registration,
            _slot: slot,
            _root: root,
            _read_memory: (read_memory),
        })
    }

    /// Whether committing `tx`'s write set could leave a partitioned vector
    /// partition with no live vector -- the one outcome the commit-time
    /// snapshot-removal pass exists for: an emptied partition's graph and
    /// live-plus-retained identity may be released at commit only when no
    /// registered snapshot still sees an entry in it, and that proof needs
    /// the pass. The pass holds every read arriving at a snapshot at or
    /// before this commit's watermark until the whole apply is over, so a
    /// commit that cannot empty a partition -- an in-place vector or
    /// attribute update that keeps each row in its partition, or any write
    /// on a table without a partitioned vector column -- must never begin
    /// one: a read opened before a commit finishes is owed its old complete
    /// view at once, not after the writer's apply.
    ///
    /// Conservative: answers `true` whenever emptiness cannot be ruled out
    /// cheaply (a key move, a vector removed without a same-row
    /// replacement, a row deleted from a partitioned vector table, or a
    /// replacement whose partition key cannot be shown unchanged).
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn write_set_could_empty_a_vector_partition(&self, tx: TxId) -> Result<bool> {
        struct WriteSetShape {
            tables: Vec<TableName>,
            has_moves: bool,
            has_unreplaced_vector_delete: bool,
            unreplaced_row_deletes: Vec<TableName>,
        }
        let shape = self.tx_mgr.with_write_set(tx, |ws| {
            let replaced_vectors: HashSet<(&VectorIndexRef, RowId)> = ws
                .vector_inserts
                .iter()
                .map(|entry| (&entry.index, entry.row_id))
                .collect();
            let replaced_rows: HashSet<(&str, RowId)> = ws
                .relational_inserts
                .iter()
                .map(|(table, row)| (table.as_str(), row.row_id))
                .collect();
            let mut tables: Vec<TableName> = ws
                .relational_inserts
                .iter()
                .map(|(table, _)| table.clone())
                .chain(
                    ws.relational_deletes
                        .iter()
                        .map(|(table, _, _)| table.clone()),
                )
                .collect();
            tables.sort_unstable();
            tables.dedup();
            WriteSetShape {
                tables,
                has_moves: !ws.vector_moves.is_empty(),
                has_unreplaced_vector_delete: ws
                    .vector_deletes
                    .iter()
                    .any(|(index, row_id, _)| !replaced_vectors.contains(&(index, *row_id))),
                unreplaced_row_deletes: ws
                    .relational_deletes
                    .iter()
                    .filter(|(table, row_id, _)| {
                        !replaced_rows.contains(&(table.as_str(), *row_id))
                    })
                    .map(|(table, _, _)| table.clone())
                    .collect(),
            }
        })?;
        if shape.has_moves || shape.has_unreplaced_vector_delete {
            return Ok(true);
        }
        // Only a PARTITIONED vector column has partition state to release;
        // an unpartitioned column is never reclaimed at commit.
        let mut partitioned_columns: HashMap<TableName, Vec<ColumnDef>> = HashMap::new();
        for table in &shape.tables {
            let Some(meta) = self.relational_store.table_meta(table) else {
                continue;
            };
            let columns: Vec<ColumnDef> = meta
                .columns
                .iter()
                .filter(|column| {
                    matches!(column.column_type, ColumnType::Vector(_))
                        && column.partition_key_columns.is_some()
                })
                .cloned()
                .collect();
            if !columns.is_empty() {
                partitioned_columns.insert(table.clone(), columns);
            }
        }
        if partitioned_columns.is_empty() {
            return Ok(false);
        }
        if shape
            .unreplaced_row_deletes
            .iter()
            .any(|table| partitioned_columns.contains_key(table))
        {
            return Ok(true);
        }
        // A replacement (or received upsert) on a partitioned table empties
        // its source partition only if a declared key changed. Rows are
        // copied out first so no store lock is taken under the write-set
        // lock; the commit lock is held, so the live row is the pre-commit row.
        let replacements: Vec<(TableName, VersionedRow)> =
            self.tx_mgr.with_write_set(tx, |ws| {
                ws.relational_inserts
                    .iter()
                    .filter(|(table, _)| partitioned_columns.contains_key(table))
                    .cloned()
                    .collect()
            })?;
        for (table, new_row) in &replacements {
            let Some(old_row) = self.relational_store.live_row_by_id(table, new_row.row_id) else {
                // A brand-new row leaves no partition.
                continue;
            };
            let Some(meta) = self.relational_store.table_meta(table) else {
                continue;
            };
            for declaration in &partitioned_columns[table] {
                let index = VectorIndexRef::new(table.clone(), declaration.name.clone());
                let old_key = crate::composite_store::vector_partition_key_for_row(
                    &index,
                    &meta,
                    declaration,
                    &old_row,
                );
                let new_key = crate::composite_store::vector_partition_key_for_row(
                    &index,
                    &meta,
                    declaration,
                    new_row,
                );
                match (old_key, new_key) {
                    (Ok(old_key), Ok(new_key)) if old_key == new_key => {}
                    _ => return Ok(true),
                }
            }
        }
        Ok(false)
    }

    /// Run the terminal commit for the statement currently on top of this
    /// thread. Only that exact registration token is omitted from the
    /// commit-time removal sample. Nested commits temporarily replace the
    /// token and restore the outer statement's protection on return.
    pub(super) fn with_current_statement_snapshot_excluded_from_terminal_commit<T>(
        &self,
        commit: impl FnOnce() -> T,
    ) -> T {
        let token =
            STATEMENT_SNAPSHOT_REGISTRATION_STACK.with(|stack| stack.borrow().last().copied());
        debug_assert!(
            token.is_some(),
            "terminal SQL commit has a statement snapshot"
        );
        COMMITTING_STATEMENT_SNAPSHOT_REGISTRATION.with(|cell| {
            struct RestoreCommitRegistration<'a> {
                cell: &'a std::cell::Cell<Option<u64>>,
                prior: Option<u64>,
            }

            impl Drop for RestoreCommitRegistration<'_> {
                fn drop(&mut self) {
                    self.cell.set(self.prior);
                }
            }

            let prior = cell.replace(token);
            let _restore = RestoreCommitRegistration { cell, prior };
            commit()
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn with_snapshot_override<T>(
        &self,
        snapshot: SnapshotId,
        f: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        SNAPSHOT_OVERRIDE.with(|cell| {
            struct SnapshotOverrideGuard<'a> {
                cell: &'a std::cell::RefCell<Option<SnapshotId>>,
                prior: Option<SnapshotId>,
            }

            impl Drop for SnapshotOverrideGuard<'_> {
                fn drop(&mut self) {
                    self.cell.replace(self.prior);
                }
            }

            let prior = cell.replace(Some(snapshot));
            let _guard = SnapshotOverrideGuard { cell, prior };
            f()
        })
    }

    /// Run a test-only reader through the same snapshot override used by the
    /// ordinary historical-read entrypoint. This lets the bounded door prove
    /// the identical retained-generation selection without adding a second
    /// production snapshot API.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __with_snapshot_override_for_test<T>(
        &self,
        snapshot: SnapshotId,
        operation: impl FnOnce() -> T,
    ) -> T {
        self.with_snapshot_override(snapshot, || Ok(operation()))
            .expect("installing a test snapshot override is infallible")
    }

    pub(crate) fn snapshot_for_read(&self) -> SnapshotId {
        SNAPSHOT_OVERRIDE.with(|cell| {
            cell.borrow().unwrap_or_else(|| {
                self.scoped_writer_root()
                    .map_or_else(|| self.snapshot(), |root| root.snapshot)
            })
        })
    }

    /// Fallible sibling of `snapshot_for_read`, for a caller that must not
    /// panic on a closed handle: an override snapshot is always returned (it
    /// requires no live operation to read), otherwise `None` if the handle
    /// is closed right now, or `Some` from a direct, lock-free
    /// `tx_mgr.snapshot()` call — deliberately bypassing `snapshot`'s own
    /// `assert_open_operation` (which panics, and which takes
    /// `operation_gate`'s read lock; see `already_closed`'s own doc comment
    /// for why a lock-holding check cannot sit ahead of a park/wait here).
    /// `execute`/`execute_in_tx` re-check with THIS, not `already_closed`
    /// again, immediately before registering a read snapshot: a close that
    /// lands in the narrow window between their own earlier `already_closed`
    /// check and this call must still surface as the typed closed-handle
    /// error, never a panic.
    pub(crate) fn try_snapshot_for_read(&self) -> Option<SnapshotId> {
        SNAPSHOT_OVERRIDE.with(|cell| {
            if let Some(snapshot) = *cell.borrow() {
                return Some(snapshot);
            }
            if self.already_closed() {
                return None;
            }
            Some(self.tx_mgr.snapshot())
        })
    }

    /// Capture the snapshot and detached registration owned by a bounded
    /// pull operation. The boxed guard is lifetime-free because the registry
    /// itself is reference counted; a cursor can therefore retain it together
    /// with its owning `Arc<Database>`.
    /// `None` means the caller withdrew during admission. A memory-backed
    /// registration may park behind an in-flight removal pass; a durable one
    /// registers while holding the commit boundary.
    /// For a durable source, snapshot capture and registration share the
    /// commit boundary. Cleanup cannot sample an unprotected captured root.
    /// The transaction this handle has open, for a read that has to see it.
    ///
    /// Same answer `execute` uses to decide whether a statement runs inside a
    /// transaction, so a bounded read over this handle and an uncapped one
    /// agree about which transaction they are inside.
    pub(crate) fn active_read_transaction(&self) -> Option<TxId> {
        self.active_session_tx().or_else(|| {
            self.origin_session_tx
                .as_ref()
                .and_then(|slot| *slot.lock())
        })
    }

    /// What an open transaction changes about what a read of one table sees.
    ///
    /// This is the cheap half of the merge the uncapped path does in
    /// `scan_with_tx`: which committed rows the transaction hides, and where
    /// its own rows sit in its write set. The rows themselves stay where they
    /// are until a read asks for one, so a bounded read charges each of them
    /// as it touches it rather than paying for the whole transaction up front.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn transaction_table_overlay(
        &self,
        tx: TxId,
        table: &str,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<crate::executor::TransactionTableOverlay> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(captured) = self
            .captured_graph_decisions()
            .filter(|capture| capture.tx == tx)
        {
            captured.check()?;
            let bytes = captured
                .decisions
                .relational_deletes
                .iter()
                .filter(|(name, _, _)| name == table)
                .count()
                .saturating_mul(96)
                .saturating_add(
                    captured
                        .decisions
                        .relational_delete_predicates
                        .iter()
                        .map(|p| {
                            p.table.len().saturating_add(128).saturating_add(
                                p.predicates
                                    .iter()
                                    .map(|(name, value)| {
                                        name.len()
                                            .saturating_add(value.estimated_bytes())
                                            .saturating_add(96)
                                    })
                                    .sum::<usize>(),
                            )
                        })
                        .sum::<usize>(),
                )
                .saturating_add(256);
            let memory = contextdb_core::read_memory::ReadCredit::try_new(
                read_memory,
                bytes,
                "graph",
                "captured_overlay",
                "Raise READ_MEMORY_LIMIT.",
            )?;
            return Ok(crate::executor::TransactionTableOverlay {
                deleted: captured
                    .decisions
                    .relational_deletes
                    .iter()
                    .filter(|(name, _, _)| name == table)
                    .map(|(_, id, _)| *id)
                    .collect(),
                delete_predicates: captured
                    .decisions
                    .relational_delete_predicates
                    .iter()
                    .filter(|p| p.table == table)
                    .cloned()
                    .collect(),
                staged_rows: captured.rows(table),
                bytes: 0,
                _memory: Some(memory),
            });
        }
        let size = |ws: &WriteSet| {
            let deleted = ws
                .relational_deletes
                .iter()
                .filter(|(name, _, _)| name == table)
                .count();
            let predicates = ws
                .relational_delete_predicates
                .iter()
                .filter(|p| p.table == table)
                .count();
            let bytes = ws
                .relational_deletes
                .iter()
                .filter(|(name, _, _)| name == table)
                .count()
                .saturating_mul(96)
                .saturating_add(
                    ws.relational_delete_predicates
                        .iter()
                        .filter(|p| p.table == table)
                        .map(|p| {
                            p.table.len().saturating_add(128).saturating_add(
                                p.predicates
                                    .iter()
                                    .map(|(name, value)| {
                                        name.len()
                                            .saturating_add(value.estimated_bytes())
                                            .saturating_add(96)
                                    })
                                    .sum::<usize>(),
                            )
                        })
                        .sum::<usize>(),
                )
                .saturating_add(128);
            (bytes, deleted, predicates)
        };
        loop {
            let (bytes, deleted_capacity, predicate_capacity) =
                self.tx_mgr.with_ordinary_read(tx, |ws| size(ws))?;
            let memory = contextdb_core::read_memory::ReadCredit::try_new(
                read_memory.clone(),
                bytes,
                "reader",
                "statement_overlay",
                "Reduce staged rows or raise the reader memory limit.",
            )?;
            let mut deleted = HashSet::with_capacity(deleted_capacity);
            let mut delete_predicates = Vec::with_capacity(predicate_capacity);
            let staged = self.tx_mgr.with_ordinary_read(tx, |ws| {
                let (needed, deletes, predicates) = size(ws);
                if needed > bytes || deletes > deleted_capacity || predicates > predicate_capacity {
                    return Err(());
                }
                deleted.extend(
                    ws.relational_deletes
                        .iter()
                        .filter(|(name, _, _)| name == table)
                        .map(|(_, id, _)| *id),
                );
                delete_predicates.extend(
                    ws.relational_delete_predicates
                        .iter()
                        .filter(|p| p.table == table)
                        .cloned(),
                );
                Ok(ws.ordered_staged_rows.statement_rows(table))
            })?;
            let Ok(staged_rows) = staged else {
                continue;
            };
            return Ok(crate::executor::TransactionTableOverlay {
                deleted,
                delete_predicates,
                staged_rows,
                bytes: 0,
                _memory: Some(memory),
            });
        }
    }

    /// What an open transaction changes about the vectors one index can be
    /// searched over.
    ///
    /// The same five facts the established door merges by hand
    /// (`query_vector_strict_with_write_set_exact`), read once and carried as
    /// decisions rather than as a copy of the index: what this transaction
    /// took out, what it re-filed elsewhere, and the immutable vectors it owns.
    pub(super) fn capture_vector_write_set<E: From<Error>>(
        &self,
        tx: TxId,
        index: &VectorIndexRef,
        retain_rows: bool,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<CapturedVectorWriteSet>, E> {
        let sizes = |ws: &WriteSet| {
            let deletes = ws
                .relational_deletes
                .iter()
                .filter(|(table, _, _)| table == &index.table)
                .count();
            (
                ws.vector_deletes
                    .iter()
                    .filter(|(have, _, _)| have == index)
                    .count()
                    .saturating_add(deletes),
                ws.vector_moves
                    .iter()
                    .filter(|(have, _, _, _)| have == index)
                    .count(),
                ws.vector_inserts
                    .iter()
                    .filter(|entry| entry.index == *index && entry.deleted_tx.is_none())
                    .count(),
                if retain_rows { deletes } else { 0 },
                if retain_rows {
                    ws.relational_inserts
                        .iter()
                        .filter(|(table, _)| table == &index.table)
                        .count()
                } else {
                    0
                },
            )
        };
        loop {
            let (
                removed_capacity,
                moved_capacity,
                staged_capacity,
                row_delete_capacity,
                anchor_capacity,
            ) = self
                .tx_mgr
                .with_write_set(tx, |ws| sizes(ws))
                .map_err(E::from)?;
            let bytes = std::mem::size_of::<CapturedVectorWriteSet>()
                .saturating_add(held::map::<RowId, ()>(removed_capacity))
                .saturating_add(held::map::<RowId, RowId>(moved_capacity))
                .saturating_add(held::map::<RowId, ()>(staged_capacity))
                .saturating_add(held::map::<RowId, ()>(row_delete_capacity))
                .saturating_add(staged_capacity.saturating_mul(std::mem::size_of::<
                    Arc<contextdb_tx::ordered_staged::StagedVectorCapability>,
                >()))
                .saturating_add(if staged_capacity == 0 {
                    0
                } else {
                    held::ALLOCATION_BYTES
                });
            let scratch_bytes = held::map::<RowId, ()>(anchor_capacity);
            before_retain(bytes.saturating_add(scratch_bytes))?;
            let mut seen_rows = HashSet::with_capacity(anchor_capacity);
            let mut removed = HashSet::with_capacity(removed_capacity);
            let mut moved = HashMap::with_capacity(moved_capacity);
            let mut staged_identities = HashSet::with_capacity(staged_capacity);
            let mut staged_vectors = Vec::with_capacity(staged_capacity);
            let mut deleted_rows = HashSet::with_capacity(row_delete_capacity);
            let captured = self
                .tx_mgr
                .with_write_set(tx, |ws| {
                    let now = sizes(ws);
                    if now.0 > removed_capacity
                        || now.1 > moved_capacity
                        || now.2 > staged_capacity
                        || now.3 > row_delete_capacity
                        || now.4 > anchor_capacity
                    {
                        return Err(());
                    }
                    let rows = retain_rows
                        .then(|| ws.ordered_staged_rows.statement_rows(&index.table))
                        .flatten();
                    if retain_rows {
                        for (table, row) in ws.relational_inserts.iter().rev() {
                            if table == &index.table
                                && seen_rows.insert(row.row_id)
                                && rows.as_ref().and_then(|rows| rows.row(row.row_id)) != Some(row)
                            {
                                return Ok(None);
                            }
                        }
                    }
                    for (have, row, _) in &ws.vector_deletes {
                        if have == index {
                            removed.insert(*row);
                        }
                    }
                    for (table, row_id, _) in &ws.relational_deletes {
                        if table == &index.table {
                            if retain_rows {
                                deleted_rows.insert(*row_id);
                            }
                            let replaced = if retain_rows {
                                ws.relational_inserts.iter().any(|(table, row)| {
                                    table == &index.table && row.row_id == *row_id
                                })
                            } else {
                                ws.ordered_staged_rows.contains_row(table, *row_id)
                            };
                            if !replaced {
                                removed.insert(*row_id);
                            }
                        }
                    }
                    for (have, from, to, _) in &ws.vector_moves {
                        if have == index {
                            moved.insert(*from, *to);
                        }
                    }
                    for entry in ws.vector_inserts.iter().rev() {
                        if entry.index == *index
                            && entry.deleted_tx.is_none()
                            && staged_identities.insert(entry.row_id)
                        {
                            let Some(capability) = ws
                                .ordered_staged_rows
                                .vector_capability(index, entry.row_id)
                            else {
                                return Ok(None);
                            };
                            if retain_rows && capability.vector() != entry.vector.as_slice() {
                                return Ok(None);
                            }
                            staged_vectors.push(capability);
                        }
                    }
                    Ok(Some((
                        rows,
                        now.2,
                        write_set_touches_vector_search(ws, index),
                    )))
                })
                .map_err(E::from)?;
            let Ok(captured) = captured else {
                continue;
            };
            let Some((rows, staged_entries, touches)) = captured else {
                return Ok(None);
            };
            drop(seen_rows);
            staged_vectors.reverse();
            let searchable_entry_count = removed
                .len()
                .saturating_add(moved.len())
                .saturating_add(staged_vectors.len());
            return Ok(Some(CapturedVectorWriteSet {
                vector: crate::executor::TransactionVectorOverlay {
                    removed,
                    moved,
                    staged_identities,
                    staged_vectors,
                    searchable_entry_count,
                    _memory: None,
                },
                rows,
                deleted_rows,
                staged_entries,
                touches,
            }));
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn transaction_vector_overlay(
        &self,
        tx: TxId,
        index: &VectorIndexRef,
        before_retain: &mut dyn FnMut(
            usize,
        ) -> std::result::Result<
            (),
            crate::executor::BoundedExecutionError,
        >,
    ) -> std::result::Result<
        crate::executor::TransactionVectorOverlay,
        crate::executor::BoundedExecutionError,
    > {
        self.capture_vector_write_set(tx, index, false, before_retain)?
            .map(|captured| captured.vector)
            .ok_or_else(|| {
                Error::Other("staged vector has no admitted immutable owner".into()).into()
            })
    }

    /// One vector the transaction has staged, cloned under the caller's
    /// charge.
    ///
    /// The identity is charged before it is read and the payload before it is
    /// copied, so a vector this handle staged costs a bounded read what a
    /// committed entry of the same width costs.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn transaction_staged_vector(
        &self,
        _tx: TxId,
        capability: &contextdb_tx::ordered_staged::StagedVectorCapability,
        before_touch: &mut dyn FnMut() -> std::result::Result<
            (),
            crate::executor::BoundedExecutionError,
        >,
        before_clone: &mut dyn FnMut(
            usize,
        ) -> std::result::Result<
            (),
            crate::executor::BoundedExecutionError,
        >,
    ) -> std::result::Result<Option<(RowId, Vec<f32>)>, crate::executor::BoundedExecutionError>
    {
        // The capability retains both identity and payload through admission.
        // Later mutations never require another lookup in the WriteSet.
        let values = capability.vector();
        let bytes = values
            .len()
            .saturating_mul(std::mem::size_of::<f32>())
            .saturating_add(std::mem::size_of::<Vec<f32>>());
        before_touch()?;
        before_clone(bytes)?;
        Ok(Some((capability.row_id(), values.to_vec())))
    }

    #[cfg(test)]
    pub(crate) fn bounded_read_snapshot_registration(
        &self,
        budget: &ReadRegistrationBudget,
        withdrawn: &OwnerReadCancellation,
        read_memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<Option<(SnapshotId, Lsn, Box<dyn Send + Sync>)>> {
        Ok(self
            .bounded_read_snapshot_registration_with_root(budget, withdrawn, read_memory.clone())?
            .map(|(snapshot, lsn, registration, root)| {
                (
                    snapshot,
                    lsn,
                    Box::new((registration, root)) as Box<dyn Send + Sync>,
                )
            }))
    }
    #[cfg(test)]
    pub(crate) fn bounded_cursor_snapshot_registration(
        &self,
        clock: Arc<dyn DeadlineClock>,
        idle_ms: u64,
        budget: &ReadRegistrationBudget,
        withdrawn: &OwnerReadCancellation,
        read_memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<
        Option<(
            SnapshotId,
            Lsn,
            Arc<CursorIdleWindow>,
            Option<crate::persistence::OwnerCursorStoragePin>,
            Box<dyn Send + Sync>,
        )>,
    > {
        Ok(self
            .bounded_cursor_snapshot_registration_with_root(
                clock,
                idle_ms,
                budget,
                withdrawn,
                read_memory.clone(),
            )?
            .map(|(snapshot, lsn, window, storage, registration, root)| {
                (
                    snapshot,
                    lsn,
                    window,
                    storage,
                    Box::new((registration, root)) as Box<dyn Send + Sync>,
                )
            }))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn bounded_read_snapshot_registration_with_root(
        &self,
        budget: &ReadRegistrationBudget,
        withdrawn: &OwnerReadCancellation,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<crate::executor::BoundedReadSnapshotPin>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if withdrawn.is_cancelled() {
            return Ok(None);
        }
        if self.persistence.is_some() {
            budget.remaining()?;
            let (snapshot, root) = self.capture_writer_transaction_statement(
                self.active_read_transaction(),
                read_memory.clone(),
            )?;
            let root = root.ok_or_else(closed_database_error)?;

            #[cfg(test)]
            self.snapshot_registry
                .durable_admission_test_pause
                .maybe_pause();
            let Some(registration) = self
                .snapshot_registry
                .register_pinned_root_with_budget(snapshot, None, budget, withdrawn)?
            else {
                return Ok(None);
            };
            #[cfg(feature = "in-process-test-seams")]
            crate::writer_query_test_seams::note_read_snapshot(snapshot);
            return Ok(Some((
                snapshot,
                root.lsn,
                Box::new(registration),
                Some(root),
            )));
        }
        let Some((snapshot, publication_lsn, registration)) =
            self.snapshot_registry.register_published_with_budget(
                || {
                    if self.already_closed() {
                        return Err(closed_database_error());
                    }
                    let (published_snapshot, lsn) = self.tx_mgr.published_read_snapshot();
                    let snapshot =
                        SNAPSHOT_OVERRIDE.with(|cell| cell.borrow().unwrap_or(published_snapshot));
                    Ok((snapshot, lsn))
                },
                None,
                budget,
                withdrawn,
            )?
        else {
            return Ok(None);
        };
        #[cfg(feature = "in-process-test-seams")]
        crate::writer_query_test_seams::note_read_snapshot(snapshot);
        Ok(Some((
            snapshot,
            publication_lsn,
            Box::new(registration) as Box<dyn Send + Sync>,
            None,
        )))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn owner_cursor_physical_identity(&self) -> Result<Option<(u64, u64)>> {
        self.persistence
            .as_ref()
            .map(|p| p.physical_identity())
            .transpose()
    }

    /// The same capture for a SUSPENDED cursor, which keeps its registration
    /// alive between calls. It publishes the idle window it is judged by, so
    /// removal defers to it only while it is still entitled to another page:
    /// a caller that opens a cursor, reads a page and never comes back stops
    /// holding rows back once that window passes. The returned window is the
    /// cursor's to stamp on every page it hands out. `None` carries the same
    /// meaning as on the read above: the caller withdrew during admission.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn bounded_cursor_snapshot_registration_with_root(
        &self,
        clock: Arc<dyn DeadlineClock>,
        idle_ms: u64,
        budget: &ReadRegistrationBudget,
        withdrawn: &OwnerReadCancellation,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<crate::executor::BoundedCursorSnapshotPin>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if withdrawn.is_cancelled() {
            return Ok(None);
        }
        if self.persistence.is_some() {
            budget.remaining()?;
            let (snapshot, root) = self.capture_writer_transaction_statement(
                self.active_read_transaction(),
                read_memory.clone(),
            )?;
            let root = root.ok_or_else(closed_database_error)?;
            let Some((snapshot, root, storage)) = self.writer_cursor_storage_pin(
                snapshot,
                root,
                budget,
                withdrawn,
                read_memory.clone(),
            )?
            else {
                return Ok(None);
            };

            #[cfg(test)]
            self.snapshot_registry
                .durable_admission_test_pause
                .maybe_pause();
            let window = Arc::new(CursorIdleWindow::new(
                Arc::clone(&clock),
                idle_ms,
                clock.now_ms(),
            ));
            let Some(registration) = self.snapshot_registry.register_pinned_root_with_budget(
                snapshot,
                Some(Arc::clone(&window)),
                budget,
                withdrawn,
            )?
            else {
                return Ok(None);
            };
            return Ok(Some((
                snapshot,
                root.lsn,
                window,
                Some(storage),
                Box::new(registration),
                Some(root),
            )));
        }
        let window = Arc::new(CursorIdleWindow::new(
            Arc::clone(&clock),
            idle_ms,
            clock.now_ms(),
        ));
        let Some((snapshot, publication_lsn, registration)) =
            self.snapshot_registry.register_published_with_budget(
                || {
                    if self.already_closed() {
                        return Err(closed_database_error());
                    }
                    let (published_snapshot, lsn) = self.tx_mgr.published_read_snapshot();
                    let snapshot =
                        SNAPSHOT_OVERRIDE.with(|cell| cell.borrow().unwrap_or(published_snapshot));
                    Ok((snapshot, lsn))
                },
                Some(Arc::clone(&window)),
                budget,
                withdrawn,
            )?
        else {
            return Ok(None);
        };
        Ok(Some((
            snapshot,
            publication_lsn,
            window,
            None,
            Box::new(registration),
            None,
        )))
    }

    /// Clone the shared accountant for an owned, suspendable request charge.
    /// What the relational store is holding right now, counted from its own
    /// structures rather than from the accountant's ledger or the process's
    /// resident set. A declared `MEMORY_LIMIT` is only effective if the
    /// accountant's total covers this number.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn retained_structure_bytes_for_test(
        &self,
    ) -> contextdb_relational::store::RetainedStructureBytes {
        self.relational_store.retained_structure_bytes()
    }

    /// Observe the currently retained native controls without allocation or cloning.
    /// Addresses identify live controls for allocator observation only; they grant
    /// no pointer dereference or lifetime authority and expire with their owners.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn __native_writer_backing_for_test(&self) -> (bool, [(usize, std::alloc::Layout); 3]) {
        self.finalization.accountant.native_backing_for_test()
    }

    #[cfg(test)]
    pub(crate) fn __accountant_handle_for_test(
        &self,
    ) -> crate::memory_accounting::AccountantHandle {
        crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant)
    }

    pub(crate) fn bounded_read_accountant(&self) -> crate::memory_accounting::AccountantHandle {
        crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant)
    }

    pub(crate) fn uses_retained_relational_source(&self) -> bool {
        self.retained_relational_source.is_some() || self.persistence.is_some()
    }

    pub(crate) fn release_statement_proof_reuse(&self) {
        if let Some(source) = &self.retained_relational_source {
            source.clear_statement_proofs();
        }
        if let Some(request) = redb::ReadCacheRequest::retiring()
            && let Some(persistence) = &self.persistence
        {
            persistence.retire_read_cache_request(request);
        }
    }

    /// How many distinct prefix values this store currently holds a
    /// maintained count for over exactly these columns. Inspection only.
    ///
    /// A store this cannot read is an error, never the number zero: reporting
    /// "holds 0" for an unreadable store would read as "this declaration
    /// costs nothing", which is the opposite of what an operator asked.
    /// A store with no persistence at all genuinely holds no records.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn stored_maintained_count_values(
        &self,
        table: &str,
        columns: &[(String, contextdb_core::SortDirection)],
    ) -> Result<u64> {
        if let Some(source) = &self.retained_relational_source {
            return source.stored_maintained_count_values(table, columns);
        }
        match &self.persistence {
            Some(persistence) => persistence.stored_maintained_count_values(table, columns),
            None => Ok(0),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn authenticated_relational_index_prefix_count(
        &self,
        table: &str,
        columns: &[(String, contextdb_core::SortDirection)],
        prefix: &contextdb_core::IndexKey,
        snapshot: SnapshotId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<u64>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if crate::persistence::received_working_source::WorkingReadSource::current_for(
            self.tx_mgr.identity(),
        )?
        .is_some()
        {
            return Ok(None);
        }
        if let Some(source) = &self.retained_relational_source {
            let name_memory = crate::read_image_memory::ImageMemory::charging_reader(
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                read_memory.clone(),
            );
            let index = {
                let metas = self.relational_store.table_meta.read();
                let meta = metas
                    .get(table)
                    .ok_or_else(|| Error::TableNotFound(table.into()))?;
                let selected = meta
                    .indexes
                    .iter()
                    .enumerate()
                    .filter(|(position, index)| {
                        !meta.indexes[..*position]
                            .iter()
                            .any(|earlier| earlier.name == index.name)
                    })
                    .map(|(_, index)| index)
                    .filter(|index| {
                        index.columns.len() >= columns.len()
                            && index.columns[..columns.len()] == *columns
                    })
                    .min_by(|left, right| left.name.cmp(&right.name));
                let Some(selected) = selected else {
                    return Ok(None);
                };
                name_memory.reserve(selected.name.len())?;
                selected.name.clone()
            };
            return source.authenticated_relational_index_prefix_count(
                table,
                &index,
                columns,
                prefix,
                read_memory,
            );
        }
        // A scalar exact-key count uses the same captured maintained records
        // as grouped counts. No postings are needed to answer this key.
        if prefix.len() != columns.len() {
            return Ok(None);
        }
        let Some(root) = self.scoped_writer_root() else {
            return Ok(None);
        };
        // The record is the root's; it answers only a read at the root's own
        // snapshot. An ordinary statement, inside a transaction or not, reads
        // at the current root's snapshot; a statement whose snapshot was
        // overridden (a historical read) falls back to the index range.
        if snapshot != root.snapshot {
            return Ok(None);
        }
        let mut count = 0;
        let ready = root.maintained_count_groups::<Error>(
            read_memory,
            table,
            columns,
            prefix,
            &mut || Ok(()),
            &mut |_key, value| {
                count = value;
                Ok(())
            },
        )?;
        Ok(ready.then_some(count))
    }

    /// Visit the maintained counts for a column list under one leading prefix.
    /// A captured writer root reads one maintained record per group. A committed
    /// image uses authenticated first-posting seeks and exact count lookups,
    /// skipping each group's encoded leading prefix rather than its postings.
    /// `Ok(false)` means nothing was visited: no captured writer root, a snapshot
    /// other than the captured root or image's own, counts not ready, or no
    /// committed-image index leading with the requested column list.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn maintained_count_groups<E: From<Error>>(
        &self,
        table: &str,
        columns: &[(String, contextdb_core::SortDirection)],
        prefix: &[contextdb_core::DirectedValue],
        snapshot: SnapshotId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), E>,
        visit: &mut dyn FnMut(contextdb_core::IndexKey, u64) -> std::result::Result<(), E>,
    ) -> std::result::Result<bool, E> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if crate::persistence::received_working_source::WorkingReadSource::current_for(
            self.tx_mgr.identity(),
        )
        .map_err(E::from)?
        .is_some()
        {
            return Ok(false);
        }
        if self.retained_relational_source.is_some() {
            return self.committed_maintained_count_groups(
                table,
                columns,
                prefix,
                snapshot,
                read_memory,
                before_touch,
                visit,
            );
        }
        let Some(root) = self.scoped_writer_root() else {
            return Ok(false);
        };
        if root.snapshot != snapshot {
            return Ok(false);
        }
        root.maintained_count_groups(read_memory, table, columns, prefix, before_touch, visit)
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn committed_maintained_count_groups<E: From<Error>>(
        &self,
        table: &str,
        columns: &[(String, contextdb_core::SortDirection)],
        prefix: &[contextdb_core::DirectedValue],
        snapshot: SnapshotId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), E>,
        visit: &mut dyn FnMut(contextdb_core::IndexKey, u64) -> std::result::Result<(), E>,
    ) -> std::result::Result<bool, E> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let Some(source) = &self.retained_relational_source else {
            return Ok(false);
        };
        // Borrow normalized metadata, deduplicating names before filtering
        // exactly as count_bearing_index_declarations does. Only the chosen
        // name is copied, under the requesting operation's exact byte charge.
        let name_memory = crate::read_image_memory::ImageMemory::charging_reader_for(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            read_memory.clone(),
            "committed_count",
            "index_name",
            "Raise the declared reader memory limit.",
        );
        let mut admitted_name_bytes = 0;
        let index = loop {
            let needed = {
                let metas = self.relational_store.table_meta.read();
                let meta = metas
                    .get(table)
                    .ok_or_else(|| E::from(Error::TableNotFound(table.into())))?;
                let selected = meta
                    .indexes
                    .iter()
                    .enumerate()
                    .filter(|(position, index)| {
                        !meta.indexes[..*position]
                            .iter()
                            .any(|earlier| earlier.name == index.name)
                    })
                    .map(|(_, index)| index)
                    .filter(|index| {
                        index.columns.len() >= columns.len()
                            && index.columns[..columns.len()] == *columns
                    })
                    .min_by(|left, right| left.name.cmp(&right.name));
                let Some(selected) = selected else {
                    return Ok(false);
                };
                if selected.name.len() <= admitted_name_bytes {
                    break selected.name.clone();
                }
                selected.name.len()
            };
            name_memory
                .reserve(needed - admitted_name_bytes)
                .map_err(E::from)?;
            admitted_name_bytes = needed;
        };
        // No budget callback or storage seek runs under the metadata lock.
        source.committed_maintained_count_groups(
            table,
            &index,
            columns,
            prefix,
            snapshot,
            read_memory.clone(),
            before_touch,
            &mut |position| {
                struct Validation<'a> {
                    db: &'a Database,
                    table: &'a str,
                    index: &'a str,
                    position: &'a crate::persistence::RelationalIndexPosition,
                }
                impl std::fmt::Display for Validation<'_> {
                    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        let metas = self.db.relational_store.table_meta.read();
                        let Some(meta) = metas.get(self.table) else {
                            return f.write_str("count table metadata absent");
                        };
                        let Some(declaration) = meta.indexes.iter().find(|d| d.name == self.index)
                        else {
                            return f.write_str("count index declaration absent");
                        };
                        match RedbPersistence::validated_index_position(
                            declaration,
                            meta,
                            self.position,
                        ) {
                            Ok(()) => Ok(()),
                            Err(reason) => std::fmt::Display::fmt(&reason, f),
                        }
                    }
                }
                let valid = {
                    let metas = self.relational_store.table_meta.read();
                    metas
                        .get(table)
                        .and_then(|meta| {
                            meta.indexes.iter().find(|d| d.name == index).map(|d| {
                                RedbPersistence::validated_index_position(d, meta, position).is_ok()
                            })
                        })
                        .unwrap_or(false)
                };
                if valid {
                    return Ok(());
                }
                // Each Display call drops its guard before try_format admits storage.
                Err(crate::funded_error::corruption_display(
                    Ok(read_memory.clone()),
                    source.database_path().display(),
                    format_args!(
                        "loaded content does not authenticate against the committed image root: {}",
                        Validation {
                            db: self,
                            table,
                            index: &index,
                            position
                        }
                    ),
                ))
            },
            visit,
        )
    }

    /// Pre-resolve and statically validate an already-parsed bounded read
    /// through the same planner validation functions used by ordinary
    /// execution. The bounded entrance keeps parsing before `on_query`, just
    /// like `Database::execute`, and hands this function that one AST rather
    /// than parsing the statement a second time.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn prepare_bounded_read_plan(
        &self,
        parsed: &Statement,
        params: &HashMap<String, Value>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(Statement, PhysicalPlan)> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let resolved = self.pre_resolve_cte_subqueries(
            parsed,
            params,
            None,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        let plan = contextdb_planner::plan(&resolved)?;
        validate_plan_columns(self, &plan)?;
        Ok((resolved, plan))
    }

    /// One row named by its identity, charged the way a scanned row is.
    ///
    /// The version index answers which version of this row the snapshot sees,
    /// so the cost is the row asked for rather than the table's whole write
    /// history. A caller that already knows which rows it wants -- a vector
    /// search holding the ids it scored -- asks here instead of walking the
    /// table and discarding everything it did not ask for.
    pub(crate) fn bounded_row_by_identity<E>(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        tx: Option<TxId>,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,
        before_touch: &mut (dyn FnMut() -> std::result::Result<(), E> + '_),
        before_clone: &mut (dyn FnMut(usize) -> std::result::Result<(), E> + '_),
    ) -> std::result::Result<Option<VersionedRow>, E>
    where
        E: From<Error>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // Existing byte callbacks own independently admitted copies. Preserve
        // that contract by making an actual copy while source credit is live.
        let mut source_credit = None;
        let row = self.bounded_row_by_identity_admitted(
            read_memory,
            tx,
            table,
            row_id,
            snapshot,
            before_touch,
            &mut |admission| match admission {
                crate::read_image_memory::RowReadAdmission::Copy(bytes) => before_clone(bytes),
                crate::read_image_memory::RowReadAdmission::Move(memory) => {
                    source_credit = Some(
                        memory
                            .take_retained_credit(memory.retained_payload_bytes())
                            .map_err(E::from)?,
                    );
                    Ok(())
                }
            },
        )?;
        if source_credit.is_none() {
            return Ok(row);
        }
        let copied = if let Some(row) = row.as_ref() {
            before_clone(row.estimated_bytes())?;
            Some(row.clone())
        } else {
            None
        };
        drop(row);
        drop(source_credit);
        Ok(copied)
    }

    pub(crate) fn visit_bounded_row_by_identity_admitted<E: From<Error>>(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        tx: Option<TxId>,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), E>,
        before_clone: &mut dyn FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), E>,
        visit: &mut dyn FnMut(Option<&VersionedRow>) -> std::result::Result<bool, E>,
    ) -> std::result::Result<bool, E> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // A scalar consumer can borrow the same funded, root-pinned window.
        // Transaction overlays, captured decisions and retained-image routes
        // retain their established resolution and admission behavior.
        if tx.is_none()
            && self.captured_graph_decisions().is_none()
            && self.retained_relational_source.is_none()
            && self.relational_store.uses_disk_backed_committed_bodies()
        {
            before_touch()?;
            if let Some(root) = self.writer_read_root().map_err(E::from)? {
                let source = self
                    .statement_relational_source(root, read_memory)
                    .map_err(E::from)?;
                let window = writer_read::visible_row_with_work(
                    &source,
                    table,
                    row_id,
                    snapshot,
                    before_touch,
                )?;
                return visit(window.as_ref().map(|window| window.value()));
            }
            return visit(None);
        }
        let row = self.bounded_row_by_identity_admitted(
            read_memory,
            tx,
            table,
            row_id,
            snapshot,
            before_touch,
            before_clone,
        )?;
        visit(row.as_ref())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn bounded_row_by_identity_admitted<E>(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        tx: Option<TxId>,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,
        before_touch: &mut (dyn FnMut() -> std::result::Result<(), E> + '_),
        before_clone: &mut (
                 dyn FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), E>
                     + '_
             ),
    ) -> std::result::Result<Option<VersionedRow>, E>
    where
        E: From<Error>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let captured = self
            .captured_graph_decisions()
            .filter(|capture| Some(capture.tx) == tx);
        if let Some(capture) = &captured {
            capture.check().map_err(E::from)?;
            before_touch()?;
            capture.check_table(table).map_err(E::from)?;
            if let Some(image) = capture
                .sealed_rows(table, read_memory.clone())
                .map_err(E::from)?
            {
                let bytes = image
                    .with_row(row_id, |row| Ok(row.map(VersionedRow::estimated_bytes)))
                    .map_err(E::from)?;
                if let Some(bytes) = bytes {
                    before_clone(crate::read_image_memory::RowReadAdmission::Copy(bytes))?;
                    return image
                        .with_row(row_id, |row| Ok(row.cloned()))
                        .map_err(E::from);
                }
            }
            if let Some(image) = capture.rows(table)
                && let Some(row) = image.row(row_id)
            {
                before_clone(crate::read_image_memory::RowReadAdmission::Copy(
                    row.estimated_bytes(),
                ))?;
                return Ok(Some(row.clone()));
            }
            if capture
                .decisions
                .relational_deletes
                .iter()
                .any(|(name, id, _)| name == table && *id == row_id)
            {
                return Ok(None);
            }
        }
        let tx = if captured.is_some() { None } else { tx };
        if let Some(source) = &self.retained_relational_source {
            if tx.is_some() {
                return Err(E::from(Error::Other(
                    ("a committed-image relational source cannot carry a write transaction"
                        .to_owned())
                    .into(),
                )));
            }
            let meta = self
                .table_meta(table)
                .ok_or_else(|| E::from(Error::TableNotFound(table.to_owned())))?;
            return source.bounded_relational_row_by_identity_admitted(
                table,
                &meta,
                row_id,
                snapshot,
                before_touch,
                before_clone,
                read_memory.clone(),
            );
        }
        before_touch()?;
        // A staged replacement/deletion does not certify its durable base.
        // Validate that addressed group before the unchanged overlay return.
        if tx.is_some()
            && let Some(root) = self.writer_read_root().map_err(E::from)?
            && snapshot.0 < root.snapshot.0
        {
            let mut refusal = None;
            let result = root.visit_historical_rows(
                table,
                Some(row_id),
                snapshot,
                read_memory.clone(),
                &mut || {
                    before_touch().map_err(|error| {
                        refusal = Some(error);
                        Error::ReadCancelled
                    })
                },
                &mut |_| Ok(()),
            );
            if let Some(error) = refusal {
                return Err(error);
            }
            result.map_err(E::from)?;
        }
        if let Some(tx) = tx {
            // The reader's own transaction answers first: a row it staged is
            // the row this read is about, and a row it deleted is not there.
            let staged = self
                .tx_mgr
                .with_write_set(tx, |ws| {
                    if let Some((_, row)) = ws
                        .relational_inserts
                        .iter()
                        .rev()
                        .find(|(insert_table, row)| insert_table == table && row.row_id == row_id)
                    {
                        return Some(Some(row.estimated_bytes()));
                    }
                    if ws
                        .relational_deletes
                        .iter()
                        .any(|(delete_table, deleted, _)| {
                            delete_table == table && *deleted == row_id
                        })
                    {
                        return Some(None);
                    }
                    None
                })
                .map_err(E::from)?;
            match staged {
                Some(Some(bytes)) => {
                    before_clone(crate::read_image_memory::RowReadAdmission::Copy(bytes))?;
                    return self
                        .tx_mgr
                        .with_write_set(tx, |ws| {
                            ws.relational_inserts
                                .iter()
                                .rev()
                                .find(|(insert_table, row)| {
                                    insert_table == table && row.row_id == row_id
                                })
                                .map(|(_, row)| row.clone())
                        })
                        .map_err(E::from);
                }
                Some(None) => return Ok(None),
                None => {}
            }
        }
        let resident = if self.relational_store.uses_disk_backed_committed_bodies() {
            None
        } else {
            let tables = self.relational_store.tables.read();
            let row = tables.get(table).and_then(|rows| {
                self.relational_store
                    .visible_version_position(table, rows, row_id, snapshot)
                    .and_then(|position| rows.get(position))
            });
            match row {
                Some(row) => {
                    // Charge while the only owned body is still in the store.
                    before_clone(crate::read_image_memory::RowReadAdmission::Copy(
                        row.estimated_bytes(),
                    ))?;
                    Some(row.clone())
                }
                None => None,
            }
        };
        if let Some(row) = resident {
            return Ok(Some(row));
        }
        let Some(root) = self.writer_read_root().map_err(E::from)? else {
            return Ok(None);
        };
        let source = self
            .statement_relational_source(root, read_memory.clone())
            .map_err(E::from)?;
        let window =
            writer_read::visible_row_with_work(&source, table, row_id, snapshot, before_touch)?;
        if let Some(window) = window {
            before_clone(crate::read_image_memory::RowReadAdmission::Copy(
                window.value().estimated_bytes(),
            ))?;
            Ok(Some(window.value().clone()))
        } else {
            Ok(None)
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn bounded_physical_table_cursor(
        &self,
        table: &str,
    ) -> Result<contextdb_relational::mem::BoundedPhysicalCursor> {
        if self.retained_relational_source.is_some() {
            self.table_meta(table)
                .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
            return Ok(contextdb_relational::mem::BoundedPhysicalCursor::default());
        }
        if self.persistence.is_some() {
            self.table_meta(table)
                .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
            return Ok(contextdb_relational::mem::BoundedPhysicalCursor::default());
        }
        self.relational.bounded_physical_cursor(table)
    }

    pub(crate) fn bounded_physical_table_row_next<E>(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        table: &str,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedPhysicalCursor,
        before_touch: impl FnMut() -> std::result::Result<(), E>,
        mut before_clone: impl FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<VersionedRow>, E>
    where
        E: From<Error>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut source_credit = None;
        let row = self.bounded_physical_table_row_next_admitted(
            read_memory,
            table,
            snapshot,
            cursor,
            before_touch,
            &mut |_| Ok(()),
            |admission| match admission {
                crate::read_image_memory::RowReadAdmission::Copy(bytes) => before_clone(bytes),
                crate::read_image_memory::RowReadAdmission::Move(memory) => {
                    source_credit = Some(
                        memory
                            .take_retained_credit(memory.retained_payload_bytes())
                            .map_err(E::from)?,
                    );
                    Ok(())
                }
            },
        )?;
        if source_credit.is_none() {
            return Ok(row);
        }
        let copied = if let Some(row) = row.as_ref() {
            before_clone(row.estimated_bytes())?;
            Some(row.clone())
        } else {
            None
        };
        drop(row);
        drop(source_credit);
        Ok(copied)
    }

    pub(crate) fn bounded_physical_table_row_next_admitted<E>(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        table: &str,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedPhysicalCursor,
        mut before_touch: impl FnMut() -> std::result::Result<(), E>,
        observed: &mut dyn FnMut(&VersionedRow) -> std::result::Result<(), E>,
        mut before_clone: impl FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<VersionedRow>, E>
    where
        E: From<Error>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(source) = &self.retained_relational_source {
            let meta = self
                .table_meta(table)
                .ok_or_else(|| E::from(Error::TableNotFound(table.to_owned())))?;
            return source.bounded_relational_physical_next_admitted(
                table,
                &meta,
                snapshot,
                cursor,
                &mut before_touch,
                observed,
                &mut before_clone,
                read_memory.clone(),
            );
        }
        if let Some(root) = self.writer_read_root().map_err(E::from)? {
            let source = self
                .statement_relational_source_at_snapshot(root, read_memory.clone(), snapshot)
                .map_err(E::from)?;
            let row = self.writer_physical_next(&source, table, cursor, before_touch, |bytes| {
                before_clone(crate::read_image_memory::RowReadAdmission::Copy(bytes))
            })?;
            if let Some(row) = &row {
                observed(row)?;
            }
            return Ok(row);
        }
        let row =
            self.relational
                .bounded_physical_row_next(table, cursor, before_touch, |bytes| {
                    before_clone(crate::read_image_memory::RowReadAdmission::Copy(bytes))
                })?;
        if let Some(row) = &row {
            observed(row)?;
        }
        Ok(row)
    }

    #[allow(dead_code)]
    pub(crate) fn bounded_index_next<E>(
        &self,
        table: &str,
        index: &str,
        cursor: &mut contextdb_relational::mem::BoundedIndexCursor,
        before_touch: impl FnMut() -> std::result::Result<(), E>,
        before_clone: impl FnOnce(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<(IndexKey, contextdb_relational::IndexEntry)>, E>
    where
        E: From<Error>,
    {
        self.relational
            .bounded_index_next(table, index, cursor, before_touch, before_clone)
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn ordered_prefix_snapshot_is_current(
        &self,
        snapshot: SnapshotId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(source) = &self.retained_relational_source {
            return Ok(source.ordered_prefix_snapshot_is_current(snapshot));
        }
        if let Some(root) = self.scoped_writer_root() {
            use contextdb_relational::source::RelationalReadSource;
            let source = self.statement_relational_source(root, read_memory)?;
            return Ok(source.ordered_prefix_snapshot_is_current(snapshot));
        }
        Ok(false)
    }

    pub(crate) fn bounded_ordered_table_row_next<E>(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        table: &str,
        column: &str,
        direction: SortDirection,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedOrderedRowCursor,
        check_boundary: impl FnMut() -> std::result::Result<(), E>,
        before_touch: impl FnMut() -> std::result::Result<(), E>,
        before_clone: impl FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<VersionedRow>, E>
    where
        E: From<Error>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.bounded_ordered_table_row_next_observed(
            read_memory,
            table,
            column,
            direction,
            snapshot,
            cursor,
            check_boundary,
            before_touch,
            |_, _| Ok(()),
            before_clone,
        )
    }

    pub(crate) fn bounded_ordered_table_row_next_observed<E>(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        table: &str,
        column: &str,
        direction: SortDirection,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedOrderedRowCursor,
        mut check_boundary: impl FnMut() -> std::result::Result<(), E>,
        mut before_touch: impl FnMut() -> std::result::Result<(), E>,
        mut observed: impl FnMut(&IndexKey, RowId) -> std::result::Result<(), E>,
        mut before_clone: impl FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<VersionedRow>, E>
    where
        E: From<Error>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(source) = &self.retained_relational_source {
            let meta = self
                .table_meta(table)
                .ok_or_else(|| E::from(Error::TableNotFound(table.to_owned())))?;
            let index_name = cursor.index_name().ok_or_else(|| {
                E::from(Error::Other(
                    ("bounded ordered cursor requires a selected index".to_owned()).into(),
                ))
            })?;
            let declaration = meta
                .indexes
                .iter()
                .find(|candidate| candidate.name == index_name)
                .ok_or_else(|| {
                    E::from(Error::IndexNotFound {
                        table: table.to_owned(),
                        index: index_name.to_owned(),
                    })
                })?;
            let Some((leading, index_direction)) = declaration.columns.first() else {
                return Err(E::from(Error::Other(
                    (format!("index {index_name} has no columns")).into(),
                )));
            };
            if leading != column || cursor.source_reverse() != (*index_direction != direction) {
                return Err(E::from(Error::Other(
                    (format!(
                        "index {index_name} cannot provide the requested order for {table}.{column}"
                    ))
                    .into(),
                )));
            }
            cursor.source_configure(declaration.columns.len());
            return source.bounded_relational_ordered_next_observed(
                table,
                &meta,
                declaration,
                snapshot,
                cursor,
                &mut check_boundary,
                &mut before_touch,
                &mut observed,
                &mut before_clone,
                read_memory.clone(),
            );
        }
        if let Some(root) = self.writer_read_root().map_err(E::from)? {
            let meta = root
                .borrow_captured_table_meta(table)
                .ok_or_else(|| E::from(Error::TableNotFound(table.to_owned())))?;
            let index_name = cursor.index_name().ok_or_else(|| {
                E::from(Error::Other(
                    ("bounded ordered cursor requires a selected index".to_owned()).into(),
                ))
            })?;
            let declaration = meta
                .indexes
                .iter()
                .find(|candidate| candidate.name == index_name)
                .ok_or_else(|| {
                    E::from(Error::IndexNotFound {
                        table: table.to_owned(),
                        index: index_name.to_owned(),
                    })
                })?;
            let Some((leading, index_direction)) = declaration.columns.first() else {
                return Err(E::from(Error::Other(
                    (format!("index {index_name} has no columns")).into(),
                )));
            };
            if leading != column || cursor.source_reverse() != (*index_direction != direction) {
                return Err(E::from(Error::Other(
                    (format!(
                        "index {index_name} cannot provide the requested order for {table}.{column}"
                    ))
                    .into(),
                )));
            }
            cursor.source_configure(declaration.columns.len());
            let index_components = declaration.columns.len();
            let source = self
                .statement_relational_source(root, read_memory.clone())
                .map_err(E::from)?;
            return self.writer_ordered_next(
                &source,
                table,
                snapshot,
                index_components,
                read_memory.clone(),
                cursor,
                check_boundary,
                before_touch,
                observed,
                |bytes| before_clone(crate::read_image_memory::RowReadAdmission::Copy(bytes)),
            );
        }
        self.relational.bounded_ordered_row_next_observed(
            table,
            column,
            direction,
            snapshot,
            cursor,
            before_touch,
            observed,
            |bytes| before_clone(crate::read_image_memory::RowReadAdmission::Copy(bytes)),
        )
    }

    /// How this store names itself in a damage report. A store with no file
    /// behind it says so rather than inventing a path.
    pub(crate) fn bounded_store_path(&self) -> String {
        self.persistence
            .as_ref()
            .map(|persistence| persistence.path().display().to_string())
            .unwrap_or_else(|| "<in-memory store>".to_owned())
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn __damage_first_saved_row_for_test(&self, table: &str, swap_body: bool) -> Result<()> {
        let persistence = self.persistence.as_ref().ok_or_else(|| {
            Error::Other(("saved-row damage requires a writer".to_owned()).into())
        })?;
        self.tx_mgr.with_maintenance_publication(|publication| {
            publication.reserve()?;
            persistence.damage_first_saved_row_for_test(table, swap_body)?;
            publication.complete();
            Ok(())
        })
    }

    pub(crate) fn bounded_visit_verified_ordered_identities<E>(
        &self,
        table: &str,
        index: &str,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedOrderedRowCursor,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), E>,
        visit: &mut dyn FnMut(RowId) -> std::result::Result<(), E>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<bool, E>
    where
        E: From<Error>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let Some(source) = &self.retained_relational_source else {
            return Ok(false);
        };
        source.bounded_visit_verified_ordered_identities(
            table,
            index,
            snapshot,
            cursor,
            before_touch,
            visit,
            read_memory,
        )
    }

    /// Whether the committed index proves no posting under any of
    /// `prefixes`. `None` when this store has no authenticated index to ask,
    /// or the proof did not fit: either way the caller holds no proof of
    /// emptiness.
    pub(crate) fn bounded_verified_index_prefixes_are_empty<E>(
        &self,
        table: &str,
        index: &str,
        snapshot: SnapshotId,
        prefixes: &[contextdb_core::IndexKey],
        check_boundary: &mut dyn FnMut() -> std::result::Result<(), E>,
        before_probe: &mut dyn FnMut() -> std::result::Result<(), E>,
        before_native_touch: &mut dyn FnMut() -> std::result::Result<(), E>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<Option<bool>, E>
    where
        E: From<Error>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let Some(source) = &self.retained_relational_source else {
            return Ok(None);
        };
        if !source.ordered_prefix_snapshot_is_current(snapshot) {
            return Ok(None);
        }
        let meta = self
            .table_meta(table)
            .ok_or_else(|| E::from(Error::TableNotFound(table.to_owned())))?;
        let Some(declaration) = meta
            .indexes
            .iter()
            .find(|candidate| candidate.name == index)
        else {
            return Ok(None);
        };
        source.verified_index_prefixes_are_empty(
            table,
            declaration,
            prefixes,
            check_boundary,
            before_probe,
            before_native_touch,
            read_memory,
        )
    }

    /// Charging is passed in rather than assumed, so the caller that owns the
    /// ceiling stays the one that decides what a posting and a row cost.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn bounded_exact_index_row_next<E>(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        table: &str,
        index: &str,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedExactIndexCursor,
        mut before_storage_step: impl FnMut() -> std::result::Result<(), E>,
        mut before_touch: impl FnMut() -> std::result::Result<(), E>,
        before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: impl FnMut(usize) -> std::result::Result<(), E>,
        mut before_clone: impl FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<VersionedRow>, E>
    where
        E: From<Error>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(source) = &self.retained_relational_source {
            let meta = self
                .table_meta(table)
                .ok_or_else(|| E::from(Error::TableNotFound(table.to_owned())))?;
            let declaration = meta
                .indexes
                .iter()
                .find(|candidate| candidate.name == index)
                .ok_or_else(|| {
                    E::from(Error::IndexNotFound {
                        table: table.to_owned(),
                        index: index.to_owned(),
                    })
                })?;
            let _ = (before_retain, release_retained);
            return source.bounded_relational_exact_next(
                table,
                &meta,
                declaration,
                snapshot,
                cursor,
                &mut before_storage_step,
                &mut before_touch,
                &mut before_clone,
                read_memory.clone(),
            );
        }
        if let Some(root) = self.writer_read_root().map_err(E::from)? {
            let source = self
                .statement_relational_source(root, read_memory.clone())
                .map_err(E::from)?;
            return self.writer_exact_next(
                &source,
                table,
                index,
                snapshot,
                cursor,
                before_storage_step,
                before_touch,
                before_retain,
                release_retained,
                |bytes| before_clone(crate::read_image_memory::RowReadAdmission::Copy(bytes)),
            );
        }
        self.relational.bounded_exact_index_row_next(
            table,
            index,
            snapshot,
            cursor,
            before_touch,
            before_retain,
            release_retained,
            |bytes| before_clone(crate::read_image_memory::RowReadAdmission::Copy(bytes)),
        )
    }

    /// A bounded read's schema claim is held by the read's own state, which a
    /// retained cursor keeps alive between requests and hands between service
    /// threads, so it takes the detached form rather than the statement-scoped
    /// one.
    pub(crate) fn bounded_vector_schema_read_many(
        &self,
        indexes: impl IntoIterator<Item = VectorIndexRef>,
    ) -> VectorSchemaReadGuard {
        self.vector_schema_read_many_detached(indexes)
    }

    /// What this transaction has staged for one row's vector, or `None` when it
    /// has not touched that row and the committed value still stands. The inner
    /// `None` is a row this transaction removed.
    ///
    /// The staged vector is MEASURED before it is copied, so the charge
    /// precedes the allocation it pays for -- the same order a committed row is
    /// charged in. Immutable ownership retains the measured body through the
    /// admission callback, so a later mutation cannot change the selected value.
    pub(crate) fn staged_row_vector_for_source<E>(
        &self,
        tx: TxId,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        before_copy: &mut dyn FnMut(usize) -> std::result::Result<(), E>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        _read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<Option<Option<Vec<f32>>>, E>
    where
        E: From<Error>,
    {
        let _read_memory: contextdb_core::read_memory::ReadMemoryOwner = _read_memory.into();

        self.with_vector_read_admission(vector_copy_owner, |vector_copy_owner| {
            enum Captured {
                Vector(Arc<contextdb_tx::ordered_staged::StagedVectorCapability>),
                Deleted,
                Moved { from: RowId, to: RowId },
            }
            let captured = self
                .tx_mgr
                .with_write_set(tx, |ws| {
                    if let Some(owner) = ws.ordered_staged_rows.vector_capability(index, row_id) {
                        return Some(Captured::Vector(owner));
                    }
                    if ws
                        .vector_deletes
                        .iter()
                        .rev()
                        .any(|(have, row, _)| have == index && *row == row_id)
                    {
                        return Some(Captured::Deleted);
                    }
                    ws.vector_moves
                        .iter()
                        .rev()
                        .find(|(have, from, to, _)| {
                            have == index && (*from == row_id || *to == row_id)
                        })
                        .map(|(_, from, to, _)| Captured::Moved {
                            from: *from,
                            to: *to,
                        })
                })
                .map_err(E::from)?;
            let Some(captured) = captured else {
                return Ok(None);
            };
            match captured {
                Captured::Vector(owner) => {
                    before_copy(0)?;
                    vector_copy_owner
                        .reserve(
                            owner
                                .vector()
                                .len()
                                .saturating_mul(std::mem::size_of::<f32>()),
                        )
                        .map_err(E::from)?;
                    Ok(Some(Some(owner.vector().to_vec())))
                }
                Captured::Deleted => Ok(Some(None)),
                Captured::Moved { from, to } => {
                    if from == row_id && from != to {
                        return Ok(Some(None));
                    }
                    // The stable placement was captured once. Loading its committed
                    // source never consults a later transaction mutation.
                    before_copy(0)?;
                    let entry = self
                        .vector_store
                        .load_live_entry_for_row(index, from, snapshot, vector_copy_owner)
                        .map_err(E::from)?;
                    Ok(Some(entry.map(|entry| entry.vector)))
                }
            }
        })
    }

    /// Resolve one ROW_VECTOR source through the relational row and vector
    /// overlays at the same snapshot. Ordinary and bounded SQL readers share
    /// this accessor so neither can pair a committed row with a
    /// transaction-local vector, or the reverse.
    pub(crate) fn with_row_vector_source<E, T>(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        tx: Option<TxId>,
        mut before_access_row: impl FnMut(usize) -> std::result::Result<(), E>,
        use_vector: impl FnOnce(Vec<f32>) -> std::result::Result<T, E>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<Option<T>, E>
    where
        E: From<Error>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let meta = self
            .table_meta(&index.table)
            .ok_or_else(|| E::from(Error::TableNotFound(index.table.clone())))?;
        let row = self
            .find_row_by_id_in_tx(tx, &index.table, row_id, snapshot, read_memory.clone())
            .map_err(E::from)?;
        let tables = self.relational_store.tables.read();
        // The read guard taken above is still live, so access evaluation
        // borrows it rather than acquiring it a second time. A row the reader
        // is not entitled to read is refused as the access decision it is;
        // reporting it as missing tells the caller the store does not hold
        // what it holds, and hides the refusal a caller branches on.
        if let Some(denial) = self.bounded_read_denial_for_row_in_tables(
            tx,
            &tables,
            &index.table,
            &meta,
            &row,
            snapshot,
            &mut before_access_row,
            read_memory.clone(),
        )? {
            return Err(E::from(denial));
        }
        // A reader inside its own transaction scores what that transaction
        // staged: the vector this read is about may have been rewritten a
        // moment ago by the same session, and answering from the committed
        // value would name a different nearest neighbour while looking
        // entirely correct doing it.
        if let Some(tx) = tx
            && let Some(staged) = self.staged_row_vector_for_source(
                tx,
                index,
                row_id,
                snapshot,
                &mut before_access_row,
                vector_copy_owner,
                read_memory.clone(),
            )?
        {
            return match staged {
                Some(vector) => use_vector(vector).map(Some),
                None => Ok(None),
            };
        }
        drop(tables);
        match self
            .vector_entry_for_row_in_tx(
                tx,
                index,
                row_id,
                snapshot,
                vector_copy_owner,
                read_memory.clone(),
            )
            .map_err(E::from)?
        {
            Some(entry) => use_vector(entry.vector).map(Some),
            None => Ok(None),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn bounded_vector_candidate_groups<E>(
        &self,
        index: &VectorIndexRef,
        candidates: Option<&[u64]>,
        snapshot: SnapshotId,
        before_checkpoint: impl FnMut() -> std::result::Result<(), E>,
        before_placement: impl FnMut() -> std::result::Result<(), E>,
        before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: impl FnMut(usize),
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        _read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<contextdb_vector::mem::BoundedVectorCandidateGroups, E>
    where
        E: From<Error>,
    {
        let _read_memory: contextdb_core::read_memory::ReadMemoryOwner = _read_memory.into();

        self.with_vector_read_admission(vector_copy_owner, |vector_copy_owner| {
            self.vector.bounded_candidate_groups(
                index,
                candidates,
                snapshot,
                before_checkpoint,
                before_placement,
                before_retain,
                release_retained,
                vector_copy_owner,
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn bounded_brute_force_vector_cursor<E>(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        groups: contextdb_vector::mem::BoundedVectorCandidateGroups,
        before_source_entry: impl FnMut() -> std::result::Result<(), E>,
        before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: impl FnMut(usize),
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        _read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<contextdb_vector::mem::BoundedBruteForceCursor, E>
    where
        E: From<Error>,
    {
        let _read_memory: contextdb_core::read_memory::ReadMemoryOwner = _read_memory.into();

        self.with_vector_read_admission(vector_copy_owner, |vector_copy_owner| {
            self.vector.bounded_brute_force_cursor_grouped(
                index,
                query,
                k,
                groups,
                before_source_entry,
                before_retain,
                release_retained,
                vector_copy_owner,
            )
        })
    }

    pub(crate) fn bounded_brute_force_vector_step<E>(
        &self,
        cursor: &mut contextdb_vector::mem::BoundedBruteForceCursor,
        query: &[f32],
        admit_row: Option<&mut dyn FnMut(RowId) -> std::result::Result<bool, E>>,
        before_source_entry: impl FnMut() -> std::result::Result<(), E>,
        before_precharged_candidate: impl FnMut() -> std::result::Result<(), E>,
        before_exhaustive_candidate: impl FnMut() -> std::result::Result<(), E>,
        before_distance: impl FnMut() -> std::result::Result<(), E>,
        before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: impl FnMut(usize),
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        _read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<contextdb_vector::mem::BoundedVectorStep, E>
    where
        E: From<Error>,
    {
        let _read_memory: contextdb_core::read_memory::ReadMemoryOwner = _read_memory.into();

        self.with_vector_read_admission(vector_copy_owner, |vector_copy_owner| {
            self.vector.bounded_brute_force_step_with_query(
                cursor,
                query,
                admit_row,
                before_source_entry,
                before_precharged_candidate,
                before_exhaustive_candidate,
                before_distance,
                before_retain,
                release_retained,
                vector_copy_owner,
            )
        })
    }

    /// Run the bounded HNSW source and return the admitted rows with the bytes
    /// still charged for them. The debug trace is moved into the last-search
    /// slot rather than copied: its row-id vectors and index names were
    /// admitted through the request's memory callbacks, so a copy would
    /// duplicate that payload outside the accounting that allowed it. Because
    /// the move hands the payload to a database-lifetime slot, the trace's
    /// bytes are released at the moment of publication and the returned count
    /// is the row payload the caller still owes. `None` means the exact
    /// Build this index's approximate graph if this snapshot is entitled to
    /// one, and answer whether a graph is now there to search.
    ///
    /// The store decides and the store builds. A refusal is not this read's to
    /// report: a search that could not have a graph still has an answer --
    /// it scores the entries instead, which is exactly what the established
    /// door does with the same refusal.
    /// The standing visited ceiling a traversal keeps when the caller declared
    /// no budget of its own.
    ///
    /// A read that names its own ceilings is governed by them; that is what a
    /// bounded read is for, and the standing ceiling steps aside. A read that
    /// names none keeps the ceiling it always had -- the same number, and the
    /// same typed refusal, on both doors.
    pub(crate) fn bounded_legacy_visited_cap(&self, declares_no_ceilings: bool) -> Option<usize> {
        if !declares_no_ceilings {
            return None;
        }
        Some(if self.access_is_admin() {
            contextdb_graph::mem::MAX_VISITED
        } else {
            GATED_BFS_VISITED_CEILING
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn __with_vector_pending_summary_read_failure_for_test<R>(
        &self,
        action: impl FnOnce() -> R,
    ) -> R {
        crate::persistence::vector_membership_source::with_pending_summary_failure_for_test(
            self.persistence
                .as_ref()
                .map(|source| Arc::as_ptr(source) as usize),
            action,
        )
    }

    pub(crate) fn bounded_vector_visit_visible_ids<E>(
        &self,
        index: &VectorIndexRef,
        snapshot: SnapshotId,
        before_entry: impl FnMut() -> std::result::Result<(), E>,
        before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: impl FnMut(usize),
        visit: impl FnMut(RowId) -> std::result::Result<(), E>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        _read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<(), E>
    where
        E: From<Error>,
    {
        let _read_memory: contextdb_core::read_memory::ReadMemoryOwner = _read_memory.into();

        self.with_vector_read_admission(vector_copy_owner, |vector_copy_owner| {
            self.vector.bounded_visit_visible_ids(
                index,
                snapshot,
                before_entry,
                before_retain,
                release_retained,
                visit,
                vector_copy_owner,
            )
        })
    }

    #[allow(clippy::type_complexity)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn bounded_hnsw_vector_search<E>(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        groups: &contextdb_vector::mem::BoundedVectorCandidateGroups,
        mode: VectorSearchMode,
        is_reader_memory_refusal: impl Fn(&E) -> bool,
        allowed_search_limits: contextdb_vector::hnsw::HnswAllowedSearchLimits,
        before_source_entry: impl FnMut() -> std::result::Result<(), E>,
        before_exact_candidate: impl FnMut() -> std::result::Result<(), E>,
        before_distance: impl FnMut() -> std::result::Result<(), E>,
        before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize),
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        _read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        load_transfer: crate::memory_accounting::VectorLoadReservationTransfer,
        row_check: Option<contextdb_vector::mem::RowCheck<'_, E>>,
    ) -> std::result::Result<crate::executor::BoundedVectorSearchOutcome, E>
    where
        E: From<Error>,
    {
        let _read_memory: contextdb_core::read_memory::ReadMemoryOwner = _read_memory.into();

        self.with_vector_read_admission(vector_copy_owner, |vector_copy_owner| {
            self.clear_last_vector_search_trace();
            let _inline_phase = contextdb_core::find_attribution::inline_phases::enter(
                contextdb_core::find_attribution::inline_phases::Phase::Adapter,
            );
            let _attribution = contextdb_core::find_attribution::stage("vector_search");
            let result = crate::memory_accounting::with_vector_load_reservation_transfer(
                &self.finalization.accountant,
                load_transfer,
                || {
                    self.vector
                        .bounded_hnsw_search_grouped_with_reader_memory_refusal(
                            index,
                            query,
                            k,
                            groups,
                            mode,
                            &is_reader_memory_refusal,
                            allowed_search_limits,
                            before_source_entry,
                            before_exact_candidate,
                            before_distance,
                            before_retain,
                            &mut release_retained,
                            vector_copy_owner,
                            row_check,
                        )
                },
            )?;
            let contextdb_vector::mem::BoundedHnswOutcome::Complete(result) = result else {
                return Ok(match result {
                    contextdb_vector::mem::BoundedHnswOutcome::CompleteEmpty => {
                        crate::executor::BoundedVectorSearchOutcome::CompleteEmpty
                    }
                    contextdb_vector::mem::BoundedHnswOutcome::ExactRequired { trace, .. } => {
                        crate::executor::BoundedVectorSearchOutcome::ExactRequired(trace)
                    }
                    contextdb_vector::mem::BoundedHnswOutcome::Unavailable { .. } => {
                        crate::executor::BoundedVectorSearchOutcome::Unavailable
                    }
                    contextdb_vector::mem::BoundedHnswOutcome::Incomplete { .. } => {
                        crate::executor::BoundedVectorSearchOutcome::Incomplete
                    }
                    contextdb_vector::mem::BoundedHnswOutcome::Disconnected { partition } => {
                        crate::executor::BoundedVectorSearchOutcome::Disconnected(partition)
                    }
                    contextdb_vector::mem::BoundedHnswOutcome::Complete(_) => unreachable!(
                        "the complete bounded vector outcome was matched before disposition mapping"
                    ),
                });
            };
            let contextdb_vector::mem::BoundedHnswResult {
                rows,
                trace,
                retained_bytes,
            } = result;
            // Keep the admitted trace beside the rows until the executor has built
            // disclosure from it. The executor then moves it to the debug slot and
            // releases exactly this trace charge.
            let trace_bytes = trace.retained_bytes();
            let Some(row_bytes) = retained_bytes.checked_sub(trace_bytes) else {
                release_retained(retained_bytes);
                return Err(E::from(Error::Other(
                    ("bounded vector trace charge exceeds the search charge".to_string()).into(),
                )));
            };
            Ok(crate::executor::BoundedVectorSearchOutcome::Complete(
                (rows, row_bytes),
                trace,
            ))
        })
    }

    /// Refuse a bounded vector read raised on a trigger or cron callback
    /// thread.
    ///
    /// A bounded read's schema claim is detached: the retained cursor keeps it
    /// between requests. On a callback thread the claim is deliberately EMPTY
    /// -- those callbacks already run inside the commit mutex that vector DDL
    /// metadata mutation also takes, so claiming the schema gate underneath
    /// one would order commit-mutex before schema-gate and deadlock. An empty
    /// claim is survivable for a statement that ends inside the callback, but
    /// a cursor outlives it, so it would escape holding no schema protection
    /// at all and could read an index concurrent DDL is rewriting.
    ///
    /// Neither available outcome is acceptable, so the read is refused rather
    /// than served unprotected. Statement-scoped reads on a callback thread
    /// are unchanged.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn assert_bounded_vector_read_is_schema_protected(&self) -> Result<()> {
        if self.callback_active_on_current_thread_for_this_db() {
            return Err(Error::InvalidStateTransition(
                "a bounded vector read cannot be opened inside a trigger or cron callback: its \
                 schema claim would outlive the callback with no protection, and claiming the \
                 schema gate under the commit mutex the callback already holds would deadlock -- \
                 run the bounded read from the calling session instead"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    /// The row checks a statement may spend checking its filter row by row:
    /// the walk budget its declared breadth gives it for `k`.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn bounded_vector_row_check_allowance(
        &self,
        index: &VectorIndexRef,
        k: usize,
        groups: &contextdb_vector::mem::BoundedVectorCandidateGroups,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<usize> {
        self.with_vector_read_admission(vector_copy_owner, |vector_copy_owner| {
            self.vector
                .bounded_row_check_allowance(index, k, groups, vector_copy_owner)
        })
    }

    pub(crate) fn bounded_vector_candidate_k<E>(
        &self,
        index: &VectorIndexRef,
        requested: usize,
        sort_key: Option<&str>,
        groups: &contextdb_vector::mem::BoundedVectorCandidateGroups,
        before_source: impl FnMut() -> std::result::Result<(), E>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        _read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<usize, E>
    where
        E: From<Error>,
    {
        let _read_memory: contextdb_core::read_memory::ReadMemoryOwner = _read_memory.into();

        self.with_vector_read_admission(vector_copy_owner, |vector_copy_owner| {
            self.assert_bounded_vector_read_is_schema_protected()?;
            let base_candidate_k = if let Some(sort_key) = sort_key {
                let meta = self
                    .table_meta(&index.table)
                    .ok_or_else(|| Error::TableNotFound(index.table.clone()))?;
                let column = meta
                    .columns
                    .iter()
                    .find(|column| column.name == index.column)
                    .ok_or_else(|| Error::UnknownVectorIndex {
                        index: index.clone(),
                    })?;
                let Some(policy) = column.rank_policy.as_ref() else {
                    return Err(E::from(Error::RankPolicyNotFound {
                        index: rank_index_name(&index.table, &index.column),
                        sort_key: sort_key.to_string(),
                    }));
                };
                if policy.sort_key != sort_key {
                    return Err(E::from(Error::RankPolicyNotFound {
                        index: rank_index_name(&index.table, &index.column),
                        sort_key: sort_key.to_string(),
                    }));
                }
                let _formula = self.rank_formula(&index.table, &index.column)?;
                // The indexed route's bounded pool. The bounded kernel resolves the
                // route when it runs and widens this to the whole visible set the
                // moment the exact route is taken, so the pool always carries the
                // meaning of the route that answered.
                self.vector.bounded_rank_candidate_k_grouped(
                    index,
                    requested,
                    groups,
                    before_source,
                    vector_copy_owner,
                )?
            } else {
                requested
            };

            Ok(base_candidate_k)
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn bounded_rank_candidate<E>(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        index: &VectorIndexRef,
        sort_key: &str,
        anchor_row_id: RowId,
        anchor_values: &HashMap<String, Value>,
        vector_score: f32,
        snapshot: SnapshotId,
        tx: Option<TxId>,
        mut before_candidate: impl FnMut() -> std::result::Result<(), E>,
        before_access: impl FnMut(usize) -> std::result::Result<(), E>,
        before_joined_clone: impl FnMut(usize) -> std::result::Result<(), E>,
        release_joined_clone: impl FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<(Option<f32>, usize), E>
    where
        E: From<Error>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        before_candidate()?;
        let meta = self
            .table_meta(&index.table)
            .ok_or_else(|| E::from(Error::TableNotFound(index.table.clone())))?;
        let column = meta
            .columns
            .iter()
            .find(|column| column.name == index.column)
            .ok_or_else(|| {
                E::from(Error::UnknownVectorIndex {
                    index: index.clone(),
                })
            })?;
        let policy = column.rank_policy.as_ref().ok_or_else(|| {
            E::from(Error::RankPolicyNotFound {
                index: rank_index_name(&index.table, &index.column),
                sort_key: sort_key.to_string(),
            })
        })?;
        if policy.sort_key != sort_key {
            return Err(E::from(Error::RankPolicyNotFound {
                index: rank_index_name(&index.table, &index.column),
                sort_key: sort_key.to_string(),
            }));
        }
        let formula = self
            .rank_formula(&index.table, &index.column)
            .map_err(E::from)?;
        let joined = self.bounded_joined_row_for_rank_policy(
            read_memory.clone(),
            policy,
            anchor_values,
            snapshot,
            tx,
            &mut before_candidate,
            before_access,
            before_joined_clone,
            release_joined_clone,
        )?;
        let joined_row = joined.as_ref().map(|(row, _)| row);
        let joined_bytes = joined.as_ref().map_or(0, |(_, bytes)| *bytes);
        self.rank_policy_eval_count.fetch_add(1, Ordering::SeqCst);
        let evaluated = formula.eval_with_resolver(vector_score, |formula_column| {
            if let Some(value) = anchor_values.get(formula_column) {
                return rank_value_to_number(value, formula_column);
            }
            let Some(joined) = joined_row else {
                return Ok(None);
            };
            if self.corrupt_joined_values.read().contains(&(
                policy.joined_table.clone(),
                joined.row_id,
                formula_column.to_string(),
            )) {
                return Err(FormulaEvalError::CorruptJoinedColumn {
                    column: formula_column.to_string(),
                });
            }
            rank_value_to_number(
                joined
                    .values
                    .get(formula_column)
                    .map_or(&Value::Null, |value| value),
                formula_column,
            )
        });
        match evaluated {
            Ok(Some(rank)) => Ok((Some(rank), joined_bytes)),
            Ok(None) => Ok((Some(f32::NAN), joined_bytes)),
            Err(error) => {
                let error_row_id = if matches!(error, FormulaEvalError::CorruptJoinedColumn { .. })
                {
                    joined_row
                        .map(|row| row.row_id)
                        .map_or(anchor_row_id, |row_id| row_id)
                } else {
                    anchor_row_id
                };
                self.warn_rank_eval_error(&index.table, &index.column, error_row_id, &error);
                Ok((None, joined_bytes))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn bounded_joined_row_for_rank_policy<E>(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        policy: &RankPolicy,
        anchor_values: &HashMap<String, Value>,
        snapshot: SnapshotId,
        tx: Option<TxId>,
        before_candidate: &mut impl FnMut() -> std::result::Result<(), E>,
        mut before_access: impl FnMut(usize) -> std::result::Result<(), E>,
        mut before_clone: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_clone: impl FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<(VersionedRow, usize)>, E>
    where
        E: From<Error>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.preflight_historical_index(
            &policy.joined_table,
            &policy.protected_index,
            snapshot,
            read_memory.clone(),
            before_candidate,
        )?;
        if policy.anchor_column.is_empty() {
            return Err(E::from(Error::Other(
                (format!(
                    "rank policy on index {}.{} has no resolved anchor join column",
                    policy.joined_table, policy.joined_column
                ))
                .into(),
            )));
        }
        let Some(join_value) = anchor_values.get(&policy.anchor_column) else {
            return Ok(None);
        };
        if *join_value == Value::Null {
            return Ok(None);
        }
        let key_bytes = join_value
            .estimated_bytes()
            .saturating_mul(8)
            .saturating_add(256);
        before_clone(key_bytes)?;
        let result = (|| {
            let source = self
                .writer_read_root()
                .map_err(E::from)?
                .map(|root| self.statement_relational_source(root, read_memory.clone()))
                .transpose()
                .map_err(E::from)?;

            let indexes = self.relational_store.indexes.read();
            let storage = indexes
                .get(&policy.joined_table)
                .and_then(|table_indexes| table_indexes.get(&policy.protected_index))
                .ok_or_else(|| {
                    E::from(Error::Other(
                        (format!(
                            "rank policy protected index `{}` missing on table `{}`",
                            policy.protected_index, policy.joined_table
                        ))
                        .into(),
                    ))
                })?;
            let Some((first_column, direction)) = storage.columns.first() else {
                return Err(E::from(Error::Other(
                    (format!(
                        "rank policy protected index `{}` on `{}` has no columns",
                        policy.protected_index, policy.joined_table
                    ))
                    .into(),
                )));
            };
            if first_column != &policy.joined_column {
                return Err(E::from(Error::Other(
                    (format!(
                        "rank policy protected index `{}` on `{}` no longer leads with `{}`",
                        policy.protected_index, policy.joined_table, policy.joined_column
                    ))
                    .into(),
                )));
            }
            let key_component = match direction {
                SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(join_value.clone())),
                SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(join_value.clone())),
            };
            let mut best_row_id = None;
            if let Some(source) = source.as_ref() {
                use contextdb_relational::source::{RelationalIndexRange, RelationalReadSource};
                use std::ops::Bound;
                let key = vec![key_component.clone()];
                let current = snapshot == source.snapshot();
                let columns_len = storage.columns.len();
                drop(indexes);
                let mut cursor = if columns_len == 1 {
                    source.exact_index_cursor(
                        &policy.joined_table,
                        &policy.protected_index,
                        current,
                        &key,
                    )
                } else {
                    let endpoint = source
                        .index_position_key(current, &key, RowId(0), TxId(0))
                        .map_err(E::from)?;
                    source.index_cursor(
                        &policy.joined_table,
                        &policy.protected_index,
                        RelationalIndexRange {
                            current,
                            lower: Bound::Included(endpoint.as_bytes()),
                            upper: Bound::Unbounded,
                            reverse: false,
                        },
                    )
                }
                .map_err(E::from)?;
                while let Some(window) =
                    writer_read::next_window_with_work(&mut cursor, before_candidate)?
                {
                    let posting = window.value();
                    if posting.key.first() != Some(&key_component) {
                        break;
                    }
                    if posting.entry.visible_at(snapshot)
                        && best_row_id.is_none_or(|current| current < posting.entry.row_id)
                    {
                        best_row_id = Some(posting.entry.row_id);
                    }
                }
            } else if self.retained_relational_source.is_some() {
                // A store read from its committed image: the protected index
                // is on the retained source, not in memory. Ask it exactly as
                // every other indexed read on this route does, so the policy
                // join answers the same as on the writer.
                let columns_len = storage.columns.len();
                let (first_column, first_direction) = (first_column.clone(), *direction);
                drop(indexes);
                let key = vec![key_component.clone()];
                // One visited identity is held at a time: each is charged as it
                // arrives and returned once its row id has been compared, so a
                // long run of matches costs one identity, not one per match.
                let output = std::cell::RefCell::new(contextdb_core::read_memory::ReadCredit::new(
                    read_memory.clone(),
                ));
                let mut retain = |bytes: crate::read_image_memory::RowReadAdmission<'_>| {
                    bytes
                        .retain_into(
                            &mut output.borrow_mut(),
                            "reader",
                            "rank_policy_join_identity",
                            "Raise the declared read memory limit to rank by this policy.",
                        )
                        .map_err(E::from)
                };
                let release_identity = || {
                    let mut credit = output.borrow_mut();
                    let held = credit.bytes();
                    credit.try_shrink(held).map_err(E::from)
                };
                if columns_len == 1 {
                    let mut cursor =
                        contextdb_relational::mem::BoundedExactIndexCursor::for_keys([key]);
                    cursor.select_identities_only();
                    while let Some(row) = self.bounded_exact_index_row_next(
                        read_memory.clone(),
                        &policy.joined_table,
                        &policy.protected_index,
                        snapshot,
                        &mut cursor,
                        &mut *before_candidate,
                        || Ok(()),
                        |_| Ok(()),
                        |_| Ok(()),
                        &mut retain,
                    )? {
                        if best_row_id.is_none_or(|current| current < row.row_id) {
                            best_row_id = Some(row.row_id);
                        }
                        drop(row);
                        release_identity()?;
                    }
                } else {
                    let mut cursor = contextdb_relational::mem::BoundedOrderedRowCursor::for_index(
                        policy.protected_index.clone(),
                        false,
                    )
                    .seeking_run(
                        std::ops::Bound::Included(key.clone()),
                        std::ops::Bound::Included(key),
                    );
                    // Rows, not identities: on this route an identities-only
                    // ordered walk proves and keeps verified runs, so its memory
                    // grows with the run. A row walk holds one row at a time,
                    // released below once its id has been compared.
                    while let Some(row) = self.bounded_ordered_table_row_next(
                        read_memory.clone(),
                        &policy.joined_table,
                        &first_column,
                        first_direction,
                        snapshot,
                        &mut cursor,
                        &mut *before_candidate,
                        || Ok(()),
                        &mut retain,
                    )? {
                        if best_row_id.is_none_or(|current| current < row.row_id) {
                            best_row_id = Some(row.row_id);
                        }
                        drop(row);
                        release_identity()?;
                    }
                }
            } else {
                let mut consider = |entries: &[contextdb_relational::IndexEntry]| {
                    for entry in entries {
                        before_candidate()?;
                        if entry.visible_at(snapshot)
                            && best_row_id.is_none_or(|current| current < entry.row_id)
                        {
                            best_row_id = Some(entry.row_id);
                        }
                    }
                    Ok::<(), E>(())
                };
                if storage.columns.len() == 1 {
                    if let Some(entries) = storage.exact_postings(&vec![key_component.clone()]) {
                        consider(entries)?;
                    }
                } else if !storage.exact_only() {
                    for (key, entries) in storage.tree.range(vec![key_component.clone()]..) {
                        if key.first() != Some(&key_component) {
                            break;
                        }
                        consider(entries)?;
                    }
                }
                drop(indexes);
            }
            // What this transaction has done to the joined table. A row it staged
            // is in no index, so the walk above cannot see it; a row it deleted or
            // rewrote must not win on the strength of the committed version the
            // index still points at.
            let overlay = match tx {
                Some(tx) => Some(self.transaction_table_overlay(
                    tx,
                    &policy.joined_table,
                    read_memory.clone(),
                )?),
                None => None,
            };
            let best_row_id = best_row_id.filter(|row_id| {
                overlay.as_ref().is_none_or(|overlay| {
                    !overlay.deleted.contains(row_id)
                        && !overlay
                            .staged_rows
                            .as_ref()
                            .is_some_and(|rows| rows.contains(row_id))
                })
            });
            let committed = 'committed: {
                let Some(row_id) = best_row_id else {
                    break 'committed None;
                };
                self.bounded_committed_rank_join_row(
                    tx,
                    source.as_ref().map(|source| {
                        source as &dyn contextdb_relational::source::RelationalReadSource
                    }),
                    policy,
                    row_id,
                    join_value,
                    snapshot,
                    overlay.as_ref(),
                    before_candidate,
                    &mut before_access,
                    &mut before_clone,
                    &mut release_clone,
                    read_memory.clone(),
                )?
            };
            let staged = match (tx, overlay.as_ref()) {
                (Some(tx), Some(overlay)) => self.bounded_staged_rank_join_row(
                    tx,
                    policy,
                    join_value,
                    snapshot,
                    overlay,
                    before_candidate,
                    &mut before_access,
                    &mut before_clone,
                    &mut release_clone,
                    read_memory.clone(),
                )?,
                _ => None,
            };
            // Newest version wins, by the same rule the established merge applies
            // over its own image of the table; the loser's copy is handed back
            // where it was charged.
            match (committed, staged) {
                (Some(committed), Some(staged)) => {
                    if staged.0.row_id > committed.0.row_id {
                        release_clone(committed.1)?;
                        Ok(Some(staged))
                    } else {
                        release_clone(staged.1)?;
                        Ok(Some(committed))
                    }
                }
                (Some(committed), None) => Ok(Some(committed)),
                (None, Some(staged)) => Ok(Some(staged)),
                (None, None) => Ok(None),
            }
        })();
        release_clone(key_bytes)?;
        result
    }

    /// The committed row the join resolves to, read by the identity the index
    /// named.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn bounded_committed_rank_join_row<E>(
        &self,
        tx: Option<TxId>,
        source: Option<&dyn contextdb_relational::source::RelationalReadSource>,
        policy: &RankPolicy,
        row_id: RowId,
        join_value: &Value,
        snapshot: SnapshotId,
        overlay: Option<&crate::executor::TransactionTableOverlay>,
        before_candidate: &mut impl FnMut() -> std::result::Result<(), E>,
        before_access: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        before_clone: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_clone: &mut impl FnMut(usize) -> std::result::Result<(), E>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<Option<(VersionedRow, usize)>, E>
    where
        E: From<Error>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // The posting and body share one immutable statement source. A
        // durable window retains its own decode admission while access and
        // caller-copy admission borrow the selected row.
        let window = if let Some(source) = source {
            writer_read::visible_row_with_work(
                source,
                &policy.joined_table,
                row_id,
                snapshot,
                before_candidate,
            )?
        } else {
            None
        };
        // A store read from its committed image keeps its rows on the
        // retained source, not in memory: load the selected row from there.
        let retained = source.is_none() && self.retained_relational_source.is_some();
        // The loaded body is charged to this evaluation alone: its credit
        // lives exactly as long as the row and is returned when this
        // candidate's join is done, never parked with the request's output.
        let mut retained_credit = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        let retained_row = if retained {
            before_candidate()?;
            let Some(row) = self
                .committed_row_at_admitted(
                    &policy.joined_table,
                    row_id,
                    snapshot,
                    &mut retained_credit,
                    read_memory.clone(),
                )
                .map_err(E::from)?
            else {
                return Ok(None);
            };
            Some(row)
        } else {
            None
        };
        let tables = (source.is_none() && !retained).then(|| self.relational_store.tables.read());
        let row = if source.is_some() {
            let Some(window) = window.as_ref() else {
                return Ok(None);
            };
            window.value()
        } else if let Some(row) = retained_row.as_ref() {
            row
        } else {
            let Some(rows) = tables
                .as_ref()
                .and_then(|tables| tables.get(&policy.joined_table))
            else {
                return Ok(None);
            };
            before_candidate()?;
            let Some(row) = self
                .relational_store
                .visible_version_position(&policy.joined_table, rows, row_id, snapshot)
                .and_then(|position| rows.get(position))
            else {
                return Ok(None);
            };
            row
        };
        if row
            .values
            .get(&policy.joined_column)
            .is_none_or(|value| !values_equal_for_rank_join(value, join_value))
        {
            return Ok(None);
        }
        // A row this transaction deleted by value rather than by identity is
        // gone for this reader too, and the index above cannot know it.
        if let Some(overlay) = overlay
            && contextdb_tx::row_matches_delete_predicates(
                &overlay.delete_predicates,
                &policy.joined_table,
                row,
            )
        {
            return Ok(None);
        }
        let meta = self
            .table_meta(&policy.joined_table)
            .ok_or_else(|| E::from(Error::TableNotFound(policy.joined_table.clone())))?;
        let allowed = if let Some(tables) = tables.as_ref() {
            self.bounded_read_allowed_for_row_in_tables(
                tx,
                tables,
                &policy.joined_table,
                &meta,
                row,
                snapshot,
                &mut *before_access,
                read_memory.clone(),
            )?
        } else {
            self.bounded_read_allowed_for_row(
                tx,
                &policy.joined_table,
                &meta,
                row,
                snapshot,
                &mut *before_access,
                read_memory.clone(),
            )?
        };
        if !allowed {
            return Ok(None);
        }
        let planned = bounded_rank_row_clone_planned_bytes(row).map_err(E::from)?;
        before_clone(planned)?;
        let cloned = row.clone();
        let actual = match bounded_rank_row_retained_bytes(&cloned) {
            Ok(actual) => actual,
            Err(error) => {
                release_clone(planned)?;
                return Err(E::from(error));
            }
        };
        if actual > planned {
            if let Err(error) = before_clone(actual - planned) {
                release_clone(planned)?;
                return Err(error);
            }
        } else if planned > actual {
            release_clone(planned - actual)?;
        }
        Ok(Some((cloned, actual)))
    }

    /// The best row this transaction has staged in the joined table for this
    /// join value.
    ///
    /// A staged row is in no index, so it is walked one row at a time out of
    /// the transaction's own write set -- the identity charged before it is
    /// read and the values before they are copied, exactly as a committed row
    /// is. The established door answers this by scanning the whole joined
    /// table into memory, which is the unbounded read a ceiling exists to
    /// prevent, so it is not reused here.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn bounded_staged_rank_join_row<E>(
        &self,
        tx: TxId,
        policy: &RankPolicy,
        join_value: &Value,
        snapshot: SnapshotId,
        overlay: &crate::executor::TransactionTableOverlay,
        before_candidate: &mut impl FnMut() -> std::result::Result<(), E>,
        before_access: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        before_clone: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_clone: &mut impl FnMut(usize) -> std::result::Result<(), E>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<Option<(VersionedRow, usize)>, E>
    where
        E: From<Error>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let meta = self
            .table_meta(&policy.joined_table)
            .ok_or_else(|| E::from(Error::TableNotFound(policy.joined_table.clone())))?;
        let mut best: Option<(VersionedRow, usize)> = None;
        for staged in overlay.staged_rows.iter().flat_map(|rows| rows.rows()) {
            before_candidate()?;
            let row_id = staged.row_id;
            let matches = staged
                .values
                .get(&policy.joined_column)
                .is_some_and(|value| values_equal_for_rank_join(value, join_value));
            if !matches || best.as_ref().is_some_and(|(best, _)| best.row_id >= row_id) {
                continue;
            }
            let planned = bounded_rank_row_clone_planned_bytes(staged).map_err(E::from)?;
            before_clone(planned)?;
            let cloned = staged.clone();
            let actual = match bounded_rank_row_retained_bytes(&cloned) {
                Ok(actual) => actual,
                Err(error) => {
                    release_clone(planned)?;
                    return Err(E::from(error));
                }
            };
            if actual > planned {
                if let Err(error) = before_clone(actual - planned) {
                    release_clone(planned)?;
                    return Err(error);
                }
            } else if planned > actual {
                release_clone(planned - actual)?;
            }
            // Judged after the copy because the gate reads the row, and a
            // staged row exists nowhere else to be read from.
            let tables = self.relational_store.tables.read();
            let allowed = self.bounded_read_allowed_for_row_in_tables(
                Some(tx),
                &tables,
                &policy.joined_table,
                &meta,
                &cloned,
                snapshot,
                &mut *before_access,
                read_memory.clone(),
            )?;
            drop(tables);
            if !allowed {
                release_clone(actual)?;
                continue;
            }
            if let Some((_, previous)) = best.replace((cloned, actual)) {
                release_clone(previous)?;
            }
        }
        Ok(best)
    }

    pub(crate) fn vector_schema_read(&self, index: &VectorIndexRef) -> VectorSchemaReadGuard {
        self.vector_schema_read_many([index.clone()])
    }

    pub(crate) fn vector_schema_read_table(&self, table: &str) -> Option<VectorSchemaReadGuard> {
        let refs = self.vector_schema_refs_for_table(table);
        (!refs.is_empty()).then(|| self.vector_schema_read_many(refs))
    }

    /// Take one read handle on an index's schema gate.
    ///
    /// A thread that already holds a claim on this gate takes the handle
    /// recursively: queueing behind a waiting schema change while holding a
    /// claim that same change waits for would leave neither able to finish. A
    /// thread that holds nothing queues fairly, so a waiting schema change is
    /// never barged past and always reaches the front.
    pub(super) fn vector_schema_gate_read_handle(
        &self,
        db_id: usize,
        index: &VectorIndexRef,
        statement_claims: &[(usize, VectorIndexRef)],
    ) -> ArcRwLockReadGuard<parking_lot::RawRwLock, ()> {
        let gate = self.vector_schema_gates.gate_for(index);
        let key = (db_id, index.clone());
        if statement_claims.contains(&key) || detached_vector_schema_claim_held(db_id, index) {
            gate.read_arc_recursive()
        } else {
            gate.read_arc()
        }
    }

    pub(crate) fn vector_schema_read_many(
        &self,
        refs: impl IntoIterator<Item = VectorIndexRef>,
    ) -> VectorSchemaReadGuard {
        let db_id = self.vector_schema_gate_id();
        // Same-thread trigger/cron callbacks already run inside the commit
        // mutex. Vector DDL metadata mutation also takes that mutex, so taking
        // the schema gate here would create commit-mutex -> schema-gate order.
        let callback_thread = self.callback_active_on_current_thread_for_this_db();
        let refs = VectorSchemaGates::sorted_refs(refs);
        let guards = if callback_thread {
            Vec::new()
        } else {
            let held = VECTOR_SCHEMA_READ_STACK.with(|stack| stack.borrow().clone());
            refs.iter()
                .filter(|index| !held.contains(&(db_id, (*index).clone())))
                .map(|index| self.vector_schema_gate_read_handle(db_id, index, &held))
                .collect()
        };
        VectorSchemaReadGuard::new(db_id, refs, guards)
    }

    /// Claim the same gates for state that outlives the statement that opened
    /// it: a retained bounded cursor keeps its claim between requests and can
    /// be released on a thread other than the one that opened it.
    ///
    /// Such a guard never reuses a statement's claim: doing so would leave the
    /// cursor holding nothing the moment that statement ends. It always takes
    /// handles of its own, fairly, so two sessions opening overlapping cursors
    /// on one index cannot keep a waiting schema change from ever running.
    /// Only where the opening thread already holds a claim on the same index —
    /// its own statement's, or an earlier cursor's — is the handle taken
    /// recursively, because a thread cannot queue behind a change that waits
    /// for what it is holding.
    ///
    /// The claim is recorded against the opening thread, so a schema change
    /// that thread runs later is answered with a refusal naming the cursor
    /// instead of waiting for a cursor only it can close.
    ///
    /// The same-thread callback arm carries over unchanged: trigger and cron
    /// callbacks already run inside the commit mutex that vector DDL metadata
    /// mutation also takes, so claiming the schema gate underneath one would
    /// order commit-mutex before schema-gate.
    pub(crate) fn vector_schema_read_many_detached(
        &self,
        refs: impl IntoIterator<Item = VectorIndexRef>,
    ) -> VectorSchemaReadGuard {
        let db_id = self.vector_schema_gate_id();
        let callback_thread = self.callback_active_on_current_thread_for_this_db();
        let refs = VectorSchemaGates::sorted_refs(refs);
        let guards = if callback_thread {
            Vec::new()
        } else {
            let held = VECTOR_SCHEMA_READ_STACK.with(|stack| stack.borrow().clone());
            refs.iter()
                .map(|index| self.vector_schema_gate_read_handle(db_id, index, &held))
                .collect()
        };
        VectorSchemaReadGuard::new_detached(db_id, refs, guards)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_schema_write(
        &self,
        index: &VectorIndexRef,
    ) -> Result<VectorSchemaWriteGuard> {
        self.vector_schema_write_many([index.clone()])
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_schema_write_table(
        &self,
        table: &str,
    ) -> Result<Option<VectorSchemaWriteGuard>> {
        let refs = self.vector_schema_refs_for_table(table);
        if refs.is_empty() {
            return Ok(None);
        }
        self.vector_schema_write_many(refs).map(Some)
    }

    /// Claim every affected index's schema gate for a change to its shape.
    ///
    /// A session reading one of those indexes through a cursor it has not
    /// closed is answered rather than parked: the wait it would otherwise
    /// enter can only end when it runs a statement it can no longer reach.
    /// Cursors held by other sessions are waited for, and inspection that
    /// starts after this point queues behind the change.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_schema_write_many(
        &self,
        refs: impl IntoIterator<Item = VectorIndexRef>,
    ) -> Result<VectorSchemaWriteGuard> {
        let refs = VectorSchemaGates::sorted_refs(refs);
        self.refuse_schema_change_under_own_open_cursor(&refs)?;
        let guards = refs
            .iter()
            .map(|index| self.vector_schema_gates.gate_for(index).write_arc())
            .collect();
        self.vector_schema_gates.bump_epochs(&refs);
        Ok(VectorSchemaWriteGuard { _guards: guards })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn refuse_schema_change_under_own_open_cursor(&self, refs: &[VectorIndexRef]) -> Result<()> {
        let db_id = self.vector_schema_gate_id();
        let Some(index) = refs
            .iter()
            .find(|index| detached_vector_schema_claim_held(db_id, index))
        else {
            return Ok(());
        };
        Err(Error::ReadFailure(ReadFailure::cursor_already_open(
            format!(
                "this session is reading {}.{} through a cursor it has not closed, and this \
                 change would have to wait for that cursor; close it and run the change again",
                index.table, index.column
            ),
        )))
    }

    pub(crate) fn vector_schema_refs_for_table(&self, table: &str) -> Vec<VectorIndexRef> {
        self.table_meta(table)
            .map(|meta| {
                meta.columns
                    .into_iter()
                    .filter(|column| matches!(column.column_type, ColumnType::Vector(_)))
                    .map(|column| VectorIndexRef::new(table, column.name))
                    .collect()
            })
            .unwrap_or_default()
    }
}
