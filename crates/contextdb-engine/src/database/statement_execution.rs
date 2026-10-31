use super::*;

impl Database {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn commit_returning_txid_with_read_memory(
        &self,
        tx: TxId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<TxId>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // Direct API commits can run trigger callbacks that execute nested
        // reads; give them the same statement-level schema view as SQL
        // `COMMIT` (which enters this gate through `execute`). A transaction
        // with privately staged local schema must retain the write side
        // through Redb durability and the complete after-apply publication;
        // ordinary DML-only commits remain readers.
        let _schema_publication = self.enter_commit_schema_publication_gate(tx);
        let _operation =
            self.open_operation_after_public_tx_control_wait_for_tx("commit", Some(tx))?;
        let _user_commit = self.enter_user_commit_callback_scope();
        let result = self.commit_with_source_returning_txid(
            tx,
            CommitSource::User,
            vector_copy_owner,
            read_memory.clone(),
        );
        if result.is_ok() {
            self.maybe_warn_caller_driven_backlog();
        }
        result
    }

    /// `CallerDriven` maintenance means the engine spawns no thread of its
    /// own, so nothing else ever notices a caller who declared retention or
    /// version-history cleanup and then never drives a cycle. Rather than
    /// fail silently, the NEXT commit after a full tick interval has elapsed
    /// with no cycle call logs one warning -- throttled to at most once per
    /// interval, so a caller that never drives a cycle is not warned on
    /// every single commit.
    pub(super) fn maybe_warn_caller_driven_backlog(&self) {
        if !self.maintenance_caller_driven.load(Ordering::SeqCst) {
            return;
        }
        if !(self.has_version_cleanup_tables()
            || self.has_retained_tables()
            || self.has_durable_trigger_audit()
            || self.has_vector_index_declarations())
        {
            return;
        }
        let now = std::time::Instant::now();
        let poll_interval = self.maintenance_poll_interval();
        let overdue = match *self.last_maintenance_cycle_at.lock() {
            Some(at) => now.duration_since(at) >= poll_interval,
            None => true,
        };
        if !overdue {
            return;
        }
        let mut warned_at = self.caller_driven_backlog_warned_at.lock();
        if warned_at.is_some_and(|at| now.duration_since(at) < poll_interval) {
            return;
        }
        *warned_at = Some(now);
        drop(warned_at);
        tracing::warn!(
            name: "caller_driven_maintenance_backlog",
            target: "maintenance",
            "this database declares retention, HISTORY CURRENT ONLY, a durable trigger audit, \
             or a vector index, and its maintenance policy is CallerDriven, but no \
             run_maintenance_cycle call has landed in over a tick interval -- declared \
             maintenance is not being advanced"
        );
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn rollback(&self, tx: TxId) -> Result<()> {
        let _operation =
            self.open_operation_after_public_tx_control_wait_for_tx("rollback", Some(tx))?;
        self.rollback_without_callback_tx_control(tx)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn rollback_without_callback_tx_control(&self, tx: TxId) -> Result<()> {
        if let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr {
            let terminal = handle.with_host(|host| host.rollback_terminal(tx))?;
            self.remove_original_terminal_stages(terminal.identity().original_tx);
            // Original source invoices retire by physical owner destruction.
            // Legacy logical release would refund the same body a second time.
            drop(terminal);
            return Ok(());
        }
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        let ws = self.tx_mgr.rollback_write_set(tx)?;
        self.pending_event_bus_ddl.lock().remove(&tx);
        self.pending_local_schema_stages.lock().remove(&tx);
        self.pending_commit_metadata.lock().remove(&tx);
        if self.private_received_rows_hold_delete_invoices() {
            self.dispose_private_abandoned_write_set(ws, &mut vector_copy_owner)?;
        } else {
            self.release_insert_allocations(&ws, &mut vector_copy_owner)?;
        }
        Ok(())
    }

    pub(super) fn remove_original_terminal_stages(&self, original_tx: TxId) {
        self.pending_event_bus_ddl.lock().remove(&original_tx);
        self.pending_local_schema_stages.lock().remove(&original_tx);
        self.pending_commit_metadata.lock().remove(&original_tx);
    }

    /// Admit preparation payload against this database's original writer.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn reserve_owned_preparation(&self, bytes: usize) -> Result<OwnedMemoryReservation> {
        self.reserve_original_preparation_for(
            bytes,
            "delivery",
            "prepare_owned_records",
            "Reduce delivery metadata or raise MEMORY_LIMIT.",
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_preparation_original<R>(
        &self,
        prepare: impl for<'o> FnOnce(&'o OwnedMemoryReservation) -> Result<R>,
    ) -> Result<R> {
        if let Some(source) =
            crate::persistence::received_working_source::WorkingReadSource::current_for(
                self.tx_mgr.identity(),
            )?
        {
            return source.with_original(prepare);
        }
        self.with_writer_preparation_original(prepare)
    }

    /// Explicit writer-source loan for preparation that has no received source.
    /// Received callers with an explicit source lend that source's Original instead.
    /// This seam performs no ambient source selection or registration reconstruction.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_writer_preparation_original<R>(
        &self,
        prepare: impl for<'o> FnOnce(&'o OwnedMemoryReservation) -> Result<R>,
    ) -> Result<R> {
        match &self.tx_mgr {
            DatabaseTransactionHost::Owned(handle) => {
                handle.with_host(|host| host.events.with_owned_persistence_original(prepare))
            }
            DatabaseTransactionHost::Legacy(_) => self
                .finalization
                .with_writer_original(prepare)
                .map_err(|refusal| match refusal {
                contextdb_core::read_memory::NativeRefusal::Other(reason) => {
                    Error::Other(reason.into())
                }
                _ => unreachable!("writer loan only validates source custody"),
            })?,
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reserve_original_preparation_for(
        &self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> Result<OwnedMemoryReservation> {
        if let Some(source) =
            crate::persistence::received_working_source::WorkingReadSource::current_for(
                self.tx_mgr.identity(),
            )?
        {
            return source.with_original(|original| {
                crate::memory_accounting::reserve_preparation_from_original(
                    original, bytes, subsystem, operation, hint,
                )
            });
        }
        match &self.tx_mgr {
            DatabaseTransactionHost::Owned(handle) => handle.with_host(|host| {
                host.events.with_owned_persistence_original(|original| {
                    crate::memory_accounting::reserve_preparation_from_original(
                        original, bytes, subsystem, operation, hint,
                    )
                })
            }),
            DatabaseTransactionHost::Legacy(_) => {
                self.with_writer_preparation_original(|original| {
                    crate::memory_accounting::reserve_preparation_from_original(
                        original, bytes, subsystem, operation, hint,
                    )
                })
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn with_owned_delivery_meta<R>(
        &self,
        table: &str,
        f: impl for<'m> FnOnce(&'m TableMeta) -> Result<R>,
    ) -> Result<R> {
        let _operation = self.assert_open_operation();
        if let Some(root) = self.scoped_writer_root() {
            let meta = root
                .borrow_captured_table_meta(table)
                .ok_or_else(crate::custody::canonical::invalid)?;
            f(meta)
        } else {
            let catalog = self.relational_store.table_meta.read();
            let meta = catalog
                .get(table)
                .ok_or_else(crate::custody::canonical::invalid)?;
            f(meta)
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn ensure_no_mixed_local_schema_dml(&self, tx: TxId) -> Result<()> {
        self.ensure_local_schema_stage_not_discarded(tx)?;
        if self.pending_local_schema_stages.lock().contains_key(&tx) {
            return Err(Error::Other((
                "mixed DML and transactional local schema DDL is not supported; commit or roll back before writing rows"
                    .to_string()).into()));
        }
        Ok(())
    }

    /// Whether an earlier statement of `tx` discarded its local schema stage
    /// after a failure past execution. Such a transaction must ROLLBACK.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn ensure_local_schema_stage_not_discarded(&self, tx: TxId) -> Result<()> {
        if self
            .pending_commit_metadata
            .lock()
            .get(&tx)
            .is_some_and(|metadata| metadata.local_schema_stage_discarded)
        {
            return Err(local_schema_stage_requires_rollback());
        }
        Ok(())
    }

    /// Mark `tx` as requiring ROLLBACK: its local schema stage was discarded.
    pub(super) fn mark_local_schema_stage_discarded(&self, tx: TxId) {
        self.pending_commit_metadata
            .lock()
            .entry(tx)
            .or_default()
            .local_schema_stage_discarded = true;
    }

    /// H, the copy hold of a new local schema stage: the copies the stage
    /// keeps outside the accountant. `base_tables` clones the live metadata;
    /// `base_triggers` and the image's seeded trigger declarations are two
    /// trigger copies; `base_event_bus` and the image's seeded projection are
    /// two event-bus copies.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn local_schema_stage_hold_bytes(&self) -> Result<usize> {
        let metadata =
            self.relational_store
                .table_meta
                .read()
                .values()
                .fold(0usize, |bytes, meta| {
                    bytes
                        .saturating_add(retained_table_metadata_bytes(&self.relational_store, meta))
                });
        let triggers = self.local_schema_trigger_bytes();
        let event_bus = self.received_event_bus_projection_bytes()?;
        metadata
            .checked_add(
                triggers
                    .checked_mul(2)
                    .ok_or_else(EngineMetadataJournal::identity_error)?,
            )
            .and_then(|bytes| bytes.checked_add(event_bus.checked_mul(2)?))
            .ok_or_else(EngineMetadataJournal::identity_error)
    }

    fn local_schema_trigger_bytes(&self) -> usize {
        self.trigger
            .declarations
            .lock()
            .values()
            .fold(0usize, |bytes, declaration| {
                bytes
                    .saturating_add(64)
                    .saturating_add(declaration.name.len())
                    .saturating_add(declaration.table.len())
                    .saturating_add(declaration.on_events.len().saturating_mul(8))
            })
    }

    pub(super) fn local_schema_pending_baseline_bytes(&self) -> usize {
        let metadata = self
            .relational_store
            .table_meta
            .read()
            .values()
            .fold(0usize, |bytes, meta| {
                bytes.saturating_add(meta.estimated_bytes())
            });
        let triggers =
            self.trigger
                .declarations
                .lock()
                .values()
                .fold(0usize, |bytes, declaration| {
                    bytes
                        .saturating_add(64)
                        .saturating_add(declaration.name.len())
                        .saturating_add(declaration.table.len())
                        .saturating_add(declaration.on_events.len().saturating_mul(8))
                });
        // `base_tables` and the schema-only worker each own metadata; the
        // worker also prebuilds physical index declarations. Trigger state is
        // retained in both the baseline and worker projections.
        metadata
            .saturating_mul(3)
            .saturating_add(triggers.saturating_mul(2))
            .saturating_add(4096)
    }

    /// Admit the copies a new stage holds outside the accountant, then build
    /// its private image on the live accountant.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn new_pending_local_schema_stage(&self) -> Result<PendingLocalSchemaStage> {
        let temporary_memory = TemporaryMemoryReservation::reserve_for_database(
            self,
            self.local_schema_stage_hold_bytes()?,
            "stage_transactional_schema",
        )?;
        let working = self.detached_local_schema_working_database()?;
        let base_event_bus = self.received_event_bus_projection();
        working.seed_received_event_bus_projection(base_event_bus.clone());
        *working.trigger.declarations.lock() = self.trigger.declarations.lock().clone();
        let base_triggers = working.trigger.declarations.lock().clone();
        Ok(PendingLocalSchemaStage {
            base_event_bus,
            base_triggers,
            base_table_generations: HashMap::new(),
            working,
            base_tables: self.relational_store.table_meta.read().clone(),
            temporary_memory,
        })
    }

    pub(super) fn staged_table_ddl_temporary_bytes(sql: &str) -> usize {
        sql.len().saturating_mul(32).saturating_add(4096)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn capture_local_schema_base_generation(
        &self,
        pending: &mut PendingLocalSchemaStage,
        ddl: &DdlChange,
    ) -> Result<()> {
        let table = Self::ddl_affected_table(ddl)
            .map(str::to_string)
            .or_else(|| match ddl {
                DdlChange::DropTrigger { name } => pending
                    .base_triggers
                    .get(name)
                    .map(|declaration| declaration.table.clone()),
                _ => None,
            });
        let Some(table) = table else {
            return Ok(());
        };
        let generation = self.authoritative_table_generation_counter(&table)?;
        pending
            .base_table_generations
            .entry(table.clone())
            .or_insert(generation);
        pending
            .working
            .in_memory_table_generations
            .lock()
            .entry(table)
            .or_insert(generation);
        Ok(())
    }

    /// Run the requested local schema change against a private clone.  The
    /// clone deliberately uses the ordinary executor, so sequential DDL gets
    /// its established validation without publishing a half-transaction
    /// schema to the live database.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stage_local_schema_statement(
        &self,
        tx: TxId,
        stmt: &Statement,
        sql: &str,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<OwnedQueryResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        match stmt {
            Statement::CreateTable(_) if self.has_context_or_principal_constraints() => {
                return Err(Error::Other(
                    ("DDL requires an admin database handle".to_string()).into(),
                ));
            }
            Statement::AlterTable(_) if self.has_access_constraints_for_query() => {
                return Err(Error::Other(
                    ("DDL requires an admin database handle".to_string()).into(),
                ));
            }
            Statement::CreateTable(_) => self.check_disk_budget("CREATE TABLE")?,
            Statement::AlterTable(_) => self.check_disk_budget("ALTER TABLE")?,
            Statement::CreateIndex(_) if self.has_access_constraints_for_query() => {
                return Err(Error::Other(
                    ("DDL requires an admin database handle".to_string()).into(),
                ));
            }
            Statement::CreateEventType { .. }
            | Statement::CreateSink { .. }
            | Statement::CreateRoute { .. } => self.require_admin_event_bus_ddl()?,
            Statement::CreateTrigger { .. } => self.require_admin_trigger_ddl("CREATE TRIGGER")?,
            // A trigger callback, or a handle without admin rights, is refused
            // with the typed trigger error before the transaction-shape check,
            // exactly as outside a transaction.
            Statement::DropTrigger { .. } => self.require_admin_trigger_ddl("DROP TRIGGER")?,
            _ => {}
        }
        self.ensure_local_schema_stage_not_discarded(tx)?;
        let detached = schema_stage_registry::PendingToken::detach_existing(
            &self.pending_local_schema_stages,
            tx,
        )?;
        let (token, existing) = match detached {
            Some((token, stage)) => (Some(token), Some(stage)),
            None => (None, None),
        };
        if existing.is_none() && !self.tx_mgr.with_read(tx, |read| read.is_empty())? {
            return Err(Error::Other((
                    "mixed DML and transactional local schema DDL is not supported; commit or roll back before changing schema"
                        .to_string()).into()));
        }
        let token = match token {
            Some(token) => token,
            None => {
                schema_stage_registry::PendingToken::reserve(&self.pending_local_schema_stages, tx)?
            }
        };
        let had_pending = existing.is_some();
        let mut pending = match existing {
            Some(pending) => pending,
            None => self.new_pending_local_schema_stage()?,
        };
        let change = match pending.working.ddl_change_for_statement(stmt, None) {
            Some(change) => Ok(change),
            None => contextdb_planner::plan(stmt).and_then(|plan| match plan {
                PhysicalPlan::CreateIndex(plan) => Ok(DdlChange::CreateIndex {
                    table: plan.table,
                    name: plan.name,
                    columns: plan.columns,
                }),
                _ => Err(Error::Other(
                    ("transactional local schema statement has no DDL representation".to_string())
                        .into(),
                )),
            }),
        };
        let change = match change {
            Ok(change) => change,
            Err(error) => {
                if had_pending {
                    token.publish(pending);
                }
                return Err(error);
            }
        };
        if let Err(error) = self.capture_local_schema_base_generation(&mut pending, &change) {
            if had_pending {
                token.publish(pending);
            }
            return Err(error);
        }
        // A plugin refusal changes nothing; the stage is republished as it was.
        if let Err(error) = self.plugin.on_ddl(&change) {
            if had_pending {
                token.publish(pending);
            }
            return Err(error);
        }
        // The guard owns the stage while the statement runs on the private
        // image: an unwind discards it and the transaction must roll back.
        let guard = LocalSchemaStatementGuard {
            database: self,
            tx,
            pending: Some(pending),
        };
        let execution = guard
            .pending
            .as_ref()
            .expect("guard holds the stage")
            .working
            .execute_owned_with_read_memory(sql, &HashMap::new(), read_memory.clone());
        #[cfg(test)]
        let past_execution = match &execution {
            Ok(_) => local_schema_test_seams::after_execute(),
            Err(_) => Ok(()),
        };
        #[cfg(not(test))]
        let past_execution: Result<()> = Ok(());
        let pending = guard.disarm();
        match (execution, past_execution) {
            // A failed statement undoes only its own work: every staged DDL
            // path admits what it needs before it publishes, so the image and
            // the earlier statements stand and the transaction can go on.
            (Err(error), _) => {
                if had_pending {
                    token.publish(pending);
                }
                Err(error)
            }
            (Ok(_), Err(error)) => {
                self.discard_local_schema_stage(tx, pending);
                Err(error)
            }
            (Ok(result), Ok(())) => {
                if let Err(error) = self.tx_mgr.update_flags(tx, |flags| {
                    flags.requires_commit_lsn = true;
                }) {
                    self.discard_local_schema_stage(tx, pending);
                    return Err(error);
                }
                token.publish(pending);
                Ok(result)
            }
        }
    }

    /// Discard a pending local schema stage after a failure past execution:
    /// the image drops first, then the stage reservation. The transaction is
    /// marked so only ROLLBACK may follow.
    fn discard_local_schema_stage(&self, tx: TxId, pending: PendingLocalSchemaStage) {
        let PendingLocalSchemaStage {
            working,
            temporary_memory,
            ..
        } = pending;
        drop(working);
        drop(temporary_memory);
        self.mark_local_schema_stage_discarded(tx);
    }

    pub fn snapshot(&self) -> SnapshotId {
        let _operation = self.assert_open_operation();
        self.tx_mgr.snapshot()
    }

    pub fn snapshot_at(&self, lsn: Lsn) -> SnapshotId {
        let _operation = self.assert_open_operation();
        let memory = self
            .declared_reader_memory()
            .expect("snapshot coordinate admission failed");
        let lsn = self
            .connected_history_end(lsn, memory)
            .expect("connected snapshot coordinate lookup failed");
        self.tx_mgr.snapshot_at_lsn(lsn)
    }

    pub(super) fn enter_sync_apply_trigger_gate_bypass(&self) -> SyncApplyTriggerGateGuard {
        SYNC_APPLY_TRIGGER_GATE_BYPASS_DEPTH.with(|depth| {
            depth.set(depth.get().saturating_add(1));
        });
        SyncApplyTriggerGateGuard
    }

    pub(super) fn sync_apply_trigger_gate_bypass_active() -> bool {
        SYNC_APPLY_TRIGGER_GATE_BYPASS_DEPTH.with(|depth| depth.get() > 0)
    }

    pub(super) fn enter_sync_apply_local_schema_bypass(
        &self,
        tx: TxId,
    ) -> SyncApplyLocalSchemaBypassGuard {
        let db_id = self.id;
        SYNC_APPLY_LOCAL_SCHEMA_BYPASS_STACK.with(|stack| {
            stack.borrow_mut().push((db_id, tx));
        });
        SyncApplyLocalSchemaBypassGuard { db_id, tx }
    }

    pub(super) fn sync_apply_local_schema_bypass_active(&self, tx: TxId) -> bool {
        let db_id = self.id;
        SYNC_APPLY_LOCAL_SCHEMA_BYPASS_STACK.with(|stack| stack.borrow().contains(&(db_id, tx)))
    }

    pub(super) fn enter_user_commit_callback_scope(&self) -> UserCommitCallbackGuard {
        let prior_active = USER_COMMIT_ACTIVE.with(|slot| slot.replace(true));
        let prior_reentry = USER_COMMIT_TRIGGER_REENTRY.with(|slot| slot.replace(false));
        UserCommitCallbackGuard {
            prior_active,
            prior_reentry,
        }
    }

    pub(super) fn record_user_commit_trigger_reentry(&self) {
        if self.access_is_admin() && USER_COMMIT_ACTIVE.with(|slot| slot.get()) {
            USER_COMMIT_TRIGGER_REENTRY.with(|slot| slot.set(true));
        }
    }

    pub(super) fn take_user_commit_trigger_reentry(&self) -> bool {
        USER_COMMIT_TRIGGER_REENTRY.with(|slot| slot.replace(false))
    }

    /// Acquire the built-in ordinary reader before any answer or vector setup.
    /// Scalar refusal needs no diagnostic allocation before an owner exists.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn ordinary_read_child(
        &self,
    ) -> Result<crate::vector_read_memory::NativeReadChildReceipt> {
        use contextdb_core::read_memory::NativeRefusal;
        self.declared_read_memory
            .native_child()
            .map_err(|refusal| match refusal {
                NativeRefusal::MemoryBudgetExceeded {
                    subsystem,
                    operation,
                    requested_bytes,
                    available_bytes,
                    budget_limit_bytes,
                    hint,
                } => Error::MemoryBudgetExceeded {
                    subsystem: subsystem.into(),
                    operation: operation.into(),
                    requested_bytes,
                    available_bytes,
                    budget_limit_bytes,
                    hint: hint.into(),
                },
                NativeRefusal::ProcessMemoryBudgetExceeded {
                    operation,
                    requested_bytes,
                    available_bytes,
                    budget_limit_bytes,
                    hint,
                } => Error::MemoryBudgetExceeded {
                    subsystem: "process_native_ceiling".into(),
                    operation: operation.into(),
                    requested_bytes,
                    available_bytes,
                    budget_limit_bytes,
                    hint: hint.into(),
                },
                NativeRefusal::Other(reason) => Error::Other(reason.into()),
                allocator @ NativeRefusal::AllocatorFailed { .. } => {
                    Error::NativeEngineOther(allocator)
                }
                NativeRefusal::NativeUnsupported { .. } => {
                    Error::Other("native reader admission unsupported: reader/request_owner".into())
                }
            })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn execute_with_read_memory(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<QueryResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        QueryResult::publish_with(read_memory.clone(), || {
            self.execute_owned_with_read_memory(sql, params, read_memory)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn execute_owned_with_read_memory(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<OwnedQueryResult> {
        self.execute_owned_with_native_trace_refund(sql, params, read_memory, None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "The existing inline native refusal is preserved without allocating an error."
    )]
    pub(crate) fn execute_owned_with_native_trace_refund(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        native_trace_refund: Option<&crate::executor::bounded::NativeTraceRefund>,
    ) -> Result<OwnedQueryResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        #[cfg(test)]
        let _purge_enclosing_producer = crate::persistence::content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::site(
            "Database::execute_owned_with_read_memory");
        let answer_memory = crate::read_image_memory::ImageMemory::query_reader_for(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            read_memory.clone(),
        )?;
        // Native trace suppliers retain the original pre-execution image
        // custody. An explicitly supplied owner needs no native trace keeper.
        let transient_answer = native_trace_refund.is_none()
            && answer_memory.value.scope(|| -> Result<bool> {
                Ok(match self.cached_statement(sql) {
                    Some(cached) => matches!(cached.stmt, Statement::Select(_)),
                    None => matches!(contextdb_parser::parse(sql)?, Statement::Select(_)),
                })
            })?;
        let execute = |image: &crate::read_image_memory::ImageMemoryHandle| {
            let _decode_scope = image.enter();
            let mut answer_copy = image.vector_copy(0)?;
            #[cfg(any(test, feature = "test-seams"))]
            observe_read_execution_convergence(ReadExecutionConvergenceEvent::PublicExecuteEntered);
            let scope = QueryRowsExaminedScope::enter(self);
            let mut result = self.execute_with_rows_examined_scope(
                sql,
                params,
                &mut answer_copy.owner,
                read_memory.clone(),
                native_trace_refund,
            );
            scope.finish(&mut result);
            if result.is_ok() {
                answer_copy.into_image();
            }
            result
        };
        if transient_answer {
            crate::read_image_memory::QueryAnswerCharge::execute_with_transient_image(
                answer_memory,
                execute,
            )
        } else {
            crate::read_image_memory::QueryAnswerCharge::execute_with_image(answer_memory, execute)
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn execute_with_rows_examined_scope(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        native_trace_refund: Option<&crate::executor::bounded::NativeTraceRefund>,
    ) -> Result<OwnedQueryResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // Checked BEFORE the snapshot registration below: `snapshot_for_read`
        // resolves through the public `Database::snapshot`, which panics on a
        // closed handle (`assert_open_operation`) by design for a caller that
        // asks for a snapshot directly. `execute` is a `Result`-returning
        // surface, never a panicking one -- a scoped view whose owner has
        // closed must see the same typed closed-handle error every other
        // `execute` path already returns, not a panic. Deliberately a
        // LOCK-FREE check (`already_closed`, not `open_operation`): this
        // function's own later logic can legitimately PARK a caller waiting
        // on an in-flight trigger callback (e.g. `BEGIN` under contention),
        // and `close()` needs `operation_gate`'s write lock to ever
        // complete -- holding its read lock here across that wait would
        // deadlock close() against the parked caller. This check narrows,
        // but does not by itself close, the race: a close landing between
        // THIS check and the snapshot registration just below is caught
        // there instead, by that call's own fallible form
        // (`try_snapshot_for_read`), not by anything later in this
        // function -- the registration precedes every one of this
        // function's own `open_operation()?` calls, so those cannot be
        // what catches a close in this specific window.
        if self.already_closed() {
            return Err(closed_database_error());
        }
        self.refuse_after_interrupted_publication()?;
        // Test-only race seam: deterministically simulates a close landing
        // in the window this comment block describes, on this same thread.
        // See `CLOSE_IN_SNAPSHOT_REGISTRATION_RACE_WINDOW`'s own doc
        // comment; production-dead, reads a thread-local that is never set.
        if take_close_in_snapshot_registration_race_window_for_test() {
            self.close()
                .expect("test-only synchronous close in the race window must succeed");
        }
        // Registered for exactly this call's duration -- see
        // `SnapshotFloorRegistry`'s doc comment. Covers `execute_at_snapshot`
        // too, since it runs through this same function under an override.
        // `try_snapshot_for_read`, not the panicking `snapshot_for_read`:
        // a close landing between the `already_closed` check above and this
        // line must still surface as the typed closed-handle error.
        let cached = self.cached_statement(sql);
        let parsed_stmt;
        let (stmt, _cached_plan) = if let Some(cached) = cached.as_ref() {
            (&cached.stmt, Some(&cached.plan))
        } else {
            parsed_stmt = contextdb_parser::parse(sql)?;
            (&parsed_stmt, None)
        };
        if Self::callback_active_process_wide() {
            self.assert_statement_allowed_for_callbacks_for_surface(stmt, "execute")?;
        }
        // This is the outer parsed-statement boundary: planning, subqueries,
        // trigger/callback reads, execution and autocommit all stay on one
        // side of a local table-schema publication.
        let session_schema_commit = if matches!(stmt, Statement::Commit) {
            *self.session_tx.lock()
        } else {
            None
        };
        let _schema_publication = if let Some(tx) = session_schema_commit {
            self.enter_commit_schema_publication_gate(tx)
        } else {
            self.enter_schema_publication_gate(Self::statement_changes_table_schema(stmt))
        };

        // DDL may have invalidated a plan while this call acquired the guard.
        // Fetch the plan again under the same guard as the durable root.
        let cached = self.cached_statement(sql);
        let parsed_stmt;
        let (stmt, cached_plan) = if let Some(cached) = cached.as_ref() {
            (&cached.stmt, Some(&cached.plan))
        } else {
            parsed_stmt = contextdb_parser::parse(sql)?;
            (&parsed_stmt, None)
        };
        let _membership_capture = self.relational_store.begin_membership_capture();
        let (read_snapshot, root) = if self.persistence.is_some()
            && Self::statement_changes_table_schema(stmt)
        {
            self.capture_writer_schema_statement()?
        } else {
            self.capture_writer_transaction_statement(*self.session_tx.lock(), read_memory.clone())?
        };
        let has_root = root.is_some();
        let _writer_read = self.enter_writer_read(root, read_memory.clone());
        let _snapshot_registration = self
            .snapshot_registry
            .register_statement_root(read_snapshot, has_root);

        match stmt {
            Statement::Begin => {
                if self.session_tx.lock().is_none() {
                    if Self::callback_active_process_wide() {
                        self.assert_callback_reentry_allowed()?;
                        self.assert_public_tx_control_cross_thread_allowed_for_surface("begin")?;
                    }
                    let _operation = self.open_operation()?;
                    let mut session = self.session_tx.lock();
                    if session.is_none() {
                        *session = Some(self.tx_mgr.try_begin()?);
                    }
                }
                return Ok(OwnedQueryResult::empty());
            }
            Statement::Commit => {
                let tx = *self.session_tx.lock();
                if let Some(tx) = tx {
                    self.ensure_local_schema_stage_not_discarded(tx)?;
                    match self.with_current_statement_snapshot_excluded_from_terminal_commit(|| {
                        self.commit_with_read_memory(tx, vector_copy_owner, read_memory.clone())
                    }) {
                        Ok(()) => {
                            let mut session = self.session_tx.lock();
                            if *session == Some(tx) {
                                *session = None;
                            }
                        }
                        Err(err) => {
                            if self.tx_mgr.with_read(tx, |_| ()).is_err() {
                                let mut session = self.session_tx.lock();
                                if *session == Some(tx) {
                                    *session = None;
                                }
                            }
                            return Err(err);
                        }
                    }
                }
                return Ok(OwnedQueryResult::empty());
            }
            Statement::Rollback => {
                let tx = *self.session_tx.lock();
                if let Some(tx) = tx {
                    self.rollback(tx)?;
                    let mut session = self.session_tx.lock();
                    if *session == Some(tx) {
                        *session = None;
                    }
                }
                return Ok(OwnedQueryResult::empty());
            }
            _ => {}
        }

        let active_tx = self.active_session_tx();
        let preopened_autocommit_tx =
            if active_tx.is_none() && Self::statement_uses_public_autocommit_write(stmt) {
                Some(self.begin_for_public_autocommit_write()?)
            } else {
                None
            };

        let operation_result = if preopened_autocommit_tx.is_some() {
            self.open_operation()
        } else {
            self.open_operation_after_statement_callback_wait(stmt, "execute")
        };
        let _operation = match operation_result {
            Ok(operation) => operation,
            Err(error) => {
                self.rollback_preopened_autocommit_tx(preopened_autocommit_tx);
                return Err(error);
            }
        };

        match stmt {
            Statement::CreateSchedule {
                name,
                every,
                callback,
                missed_tick_policy,
                catch_up_within_seconds,
            } => {
                self.create_cron_schedule(
                    name,
                    every,
                    callback,
                    missed_tick_policy.as_deref(),
                    *catch_up_within_seconds,
                )?;
                return Ok(OwnedQueryResult::empty());
            }
            Statement::DropSchedule { name } => {
                self.drop_cron_schedule(name)?;
                return Ok(OwnedQueryResult::empty());
            }
            Statement::CreateTrigger { .. } | Statement::DropTrigger { .. } => {
                let ddl = self
                    .ddl_change_for_statement(stmt, self.active_session_tx())
                    .expect("trigger statement has DDL change");
                if let Some(tx) = self.active_session_tx() {
                    return self.stage_local_schema_statement(tx, stmt, sql, read_memory.clone());
                } else {
                    self.apply_trigger_ddl_from_user(ddl, read_memory.clone())?;
                }
                return Ok(OwnedQueryResult::empty());
            }
            _ => {}
        }

        let result = self.with_snapshot_override(read_snapshot, || {
            self.execute_statement_with_plan(
                stmt,
                sql,
                params,
                active_tx,
                cached_plan,
                preopened_autocommit_tx,
                vector_copy_owner,
                read_memory.clone(),
                native_trace_refund,
            )
        });

        #[cfg(test)]
        if matches!(stmt, Statement::Select(_)) {
            maybe_pause_before_query_trace_rows_examined_capture_for_test();
        }

        result
    }

    pub(super) fn active_session_tx(&self) -> Option<TxId> {
        if CRON_CALLBACK_ACTIVE.with(|active| active.get()) {
            let this_db = self.id;
            if CRON_CALLBACK_DB.with(|slot| slot.get()) == Some(this_db) {
                return CRON_CALLBACK_TX.with(|slot| slot.get());
            }
        }
        if let Some(tx) = self.active_trigger_tx_for_this_handle() {
            return Some(tx);
        }
        *self.session_tx.lock()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn assert_statement_allowed_inside_cron_callback(
        &self,
        stmt: &Statement,
        forbidden_in_callback: bool,
        requires_callback_tx: bool,
    ) -> Result<()> {
        if CRON_CALLBACK_ACTIVE.with(|active| active.get()) && forbidden_in_callback {
            return Err(Error::CallbackReentry {
                kind: CallbackKind::Cron,
            });
        }
        if CRON_CALLBACK_ACTIVE.with(|active| active.get()) && requires_callback_tx {
            let active_tx = self.active_session_tx();
            let cron_tx = CRON_CALLBACK_TX.with(|slot| slot.get());
            let cron_db = CRON_CALLBACK_DB.with(|slot| slot.get());
            let this_db = self.id;
            if active_tx.is_none() || active_tx != cron_tx || cron_db != Some(this_db) {
                return Err(Error::CallbackReentry {
                    kind: CallbackKind::Cron,
                });
            }
        }
        if TRIGGER_CALLBACK_ACTIVE.with(|active| active.get()) && forbidden_in_callback {
            // Self-drop of the currently firing trigger has a long-standing
            // admin-error contract: block the DDL locally, preserve the active
            // trigger, and let the firing cascade continue if the callback
            // handles the error.
            if let Statement::DropTrigger { name } = stmt {
                let drops_active_trigger = TRIGGER_CALLBACK_NAME
                    .with(|active_name| active_name.borrow().as_deref() == Some(name));
                let drops_from_active_trigger_handle =
                    TRIGGER_CALLBACK_DB.with(|active_db| active_db.get() == Some(self.id));
                if drops_active_trigger && drops_from_active_trigger_handle {
                    return Ok(());
                }
            }
            self.record_user_commit_trigger_reentry();
            return Err(Error::CallbackReentry {
                kind: CallbackKind::Trigger,
            });
        }
        if TRIGGER_CALLBACK_ACTIVE.with(|active| active.get()) && requires_callback_tx {
            let active_tx = self.active_session_tx();
            let trigger_tx = TRIGGER_CALLBACK_TX.with(|slot| slot.get());
            let trigger_db = TRIGGER_CALLBACK_DB.with(|slot| slot.get());
            let this_db = self.id;
            if active_tx.is_none() || active_tx != trigger_tx || trigger_db != Some(this_db) {
                self.record_user_commit_trigger_reentry();
                return Err(Error::CallbackReentry {
                    kind: CallbackKind::Trigger,
                });
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn assert_statement_allowed_during_cross_thread_callback(
        &self,
        forbidden_in_callback: bool,
        requires_callback_tx: bool,
        surface: &'static str,
    ) -> Result<()> {
        if !forbidden_in_callback && !requires_callback_tx {
            return Ok(());
        }
        if self.cron.callback_active_on_other_thread() {
            return Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Cron,
            });
        }
        if self
            .trigger
            .callback_active_on_other_thread(self.owner_thread)
        {
            return self.wait_for_same_db_trigger_callback_idle(surface);
        }
        if requires_callback_tx {
            match self
                .trigger
                .callback_contention_for_tx_control(self.owner_thread, false)
            {
                TriggerContention::None => {}
                TriggerContention::SameDb => {
                    return self.wait_for_same_db_trigger_callback_idle(surface);
                }
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn assert_statement_allowed_for_callbacks_for_surface(
        &self,
        stmt: &Statement,
        surface: &'static str,
    ) -> Result<()> {
        let forbidden_in_callback = Self::statement_forbidden_inside_cron_callback(stmt);
        let requires_callback_tx = Self::statement_requires_cron_bound_handle(stmt);
        if !forbidden_in_callback && !requires_callback_tx {
            return Ok(());
        }
        if !Self::callback_active_process_wide() {
            return Ok(());
        }
        self.assert_statement_allowed_inside_cron_callback(
            stmt,
            forbidden_in_callback,
            requires_callback_tx,
        )?;
        self.assert_statement_allowed_during_cross_thread_callback(
            forbidden_in_callback,
            requires_callback_tx,
            surface,
        )
    }

    pub(super) fn cached_statement(&self, sql: &str) -> Option<Arc<CachedStatement>> {
        self.statement_cache.read().get(sql).cloned()
    }

    pub(super) fn cache_statement_if_eligible(
        &self,
        sql: &str,
        stmt: &Statement,
        plan: &PhysicalPlan,
    ) {
        if !Self::is_statement_cache_eligible(stmt, plan) {
            return;
        }

        let mut cache = self.statement_cache.write();
        if cache.contains_key(sql) {
            return;
        }
        if cache.len() >= MAX_STATEMENT_CACHE_ENTRIES {
            return;
        }
        cache.insert(
            sql.to_string(),
            Arc::new(CachedStatement {
                stmt: stmt.clone(),
                plan: plan.clone(),
            }),
        );
    }

    pub(super) fn is_statement_cache_eligible(stmt: &Statement, plan: &PhysicalPlan) -> bool {
        match (stmt, plan) {
            (Statement::Insert(ins), PhysicalPlan::Insert(_)) => {
                !ins.table.eq_ignore_ascii_case("GRAPH")
                    && !ins.table.eq_ignore_ascii_case("__edges")
            }
            (Statement::Update(_), PhysicalPlan::Update(_))
            | (Statement::Delete(_), PhysicalPlan::Delete(_)) => true,
            _ => false,
        }
    }

    pub(super) fn statement_forbidden_inside_cron_callback(stmt: &Statement) -> bool {
        matches!(
            stmt,
            Statement::Begin
                | Statement::Commit
                | Statement::Rollback
                | Statement::CreateTable(_)
                | Statement::AlterTable(_)
                | Statement::DropTable(_)
                | Statement::CreateIndex(_)
                | Statement::DropIndex(_)
                | Statement::CreateSchedule { .. }
                | Statement::DropSchedule { .. }
                | Statement::CreateTrigger { .. }
                | Statement::DropTrigger { .. }
                | Statement::CreateEventType { .. }
                | Statement::CreateSink { .. }
                | Statement::CreateRoute { .. }
                | Statement::DropRoute { .. }
                | Statement::SetMemoryLimit(_)
                | Statement::SetDiskLimit(_)
                | Statement::Purge(_)
        )
    }

    pub(super) fn statement_requires_cron_bound_handle(stmt: &Statement) -> bool {
        matches!(
            stmt,
            Statement::Insert(_) | Statement::Delete(_) | Statement::Update(_)
        )
    }

    pub(super) fn statement_uses_public_autocommit_write(stmt: &Statement) -> bool {
        matches!(
            stmt,
            Statement::Insert(_) | Statement::Delete(_) | Statement::Update(_)
        )
    }

    pub(super) fn plan_vector_schema_refs(&self, plan: &PhysicalPlan) -> Vec<VectorIndexRef> {
        match plan {
            PhysicalPlan::Insert(plan) => self.vector_schema_refs_for_table(&plan.table),
            PhysicalPlan::Delete(plan) => self.vector_schema_refs_for_table(&plan.table),
            PhysicalPlan::Update(plan) => self.vector_schema_refs_for_table(&plan.table),
            _ => Vec::new(),
        }
    }

    pub(crate) fn write_set_vector_schema_refs(ws: &WriteSet) -> Vec<VectorIndexRef> {
        VectorSchemaGates::sorted_refs(
            ws.vector_inserts
                .iter()
                .map(|entry| entry.index.clone())
                .chain(ws.vector_deletes.iter().map(|(index, _, _)| index.clone()))
                .chain(ws.vector_moves.iter().map(|(index, _, _, _)| index.clone())),
        )
    }

    pub(crate) fn write_set_touches_vector_schema(ws: &WriteSet) -> bool {
        !Self::write_set_vector_schema_refs(ws).is_empty()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn record_vector_schema_epoch(
        &self,
        tx: TxId,
        index: &VectorIndexRef,
    ) -> Result<()> {
        self.record_vector_schema_epochs(tx, [index.clone()])
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn record_vector_schema_epochs(
        &self,
        tx: TxId,
        refs: impl IntoIterator<Item = VectorIndexRef>,
    ) -> Result<()> {
        if matches!(&self.tx_mgr, DatabaseTransactionHost::Owned(_)) {
            let mut refs = refs.into_iter();
            let Some(first) = refs.next() else {
                return Ok(());
            };
            self.tx_mgr.with_read(tx, |_| ())?;
            let mut incoming = self.with_preparation_original(|original| {
                let mut incoming = OriginalEpochInputs::new(original)?;
                for index in std::iter::once(first).chain(refs) {
                    incoming.push_index(original, &index)?;
                }
                incoming.sort_dedup()?;
                Ok(incoming)
            })?;
            // Observe epochs in the established sorted order, before taking
            // any active-map or metadata guard.
            for (index, epoch) in &mut incoming.values {
                *epoch = self.vector_schema_gates.epoch_for(index);
            }
            return self.publish_owned_metadata(tx, |journal| {
                self.with_preparation_original(|original| {
                    let records = journal.records.lock();
                    let record = EngineMetadataJournal::record(&records, tx)
                        .ok_or_else(EngineMetadataJournal::identity_error)?;
                    let previous = match record.metadata.vector_schema_epochs.as_ref() {
                        None => None,
                        Some(entry) if entry.invoice.is_some() => Some(
                            entry
                                .body
                                .as_ref()
                                .ok_or_else(EngineMetadataJournal::identity_error)?,
                        ),
                        Some(_) => return Err(EngineMetadataJournal::identity_error()),
                    };
                    let capacity = previous
                        .map_or(0, HashMap::len)
                        .checked_add(incoming.values.len())
                        .ok_or_else(EngineMetadataJournal::identity_error)?;
                    struct EpochMap {
                        values: HashMap<VectorIndexRef, u64>,
                        owner: OwnedMemoryReservation,
                    }
                    let (map, invoice) = crate::memory_accounting::new_owned_native_map::<
                        VectorIndexRef,
                        u64,
                    >(original, capacity)?;
                    let mut mapped = EpochMap {
                        values: map,
                        owner: invoice,
                    };
                    if let Some(previous) = previous {
                        for (index, epoch) in previous {
                            let bytes = index
                                .table
                                .len()
                                .checked_add(index.column.len())
                                .ok_or_else(EngineMetadataJournal::identity_error)?;
                            crate::memory_accounting::grow_preparation_from_original(
                                &mut mapped.owner,
                                bytes,
                                "relational",
                                "vector_schema_epochs",
                                "Reduce staged metadata or raise MEMORY_LIMIT.",
                            )?;
                            let index = VectorIndexRef {
                                table: crate::memory_accounting::preparation_copy_string(
                                    &index.table,
                                )?,
                                column: crate::memory_accounting::preparation_copy_string(
                                    &index.column,
                                )?,
                            };
                            mapped.values.insert(index, *epoch);
                        }
                    }
                    drop(records);
                    for (index, epoch) in incoming.values.drain(..) {
                        let bytes = index
                            .table
                            .len()
                            .checked_add(index.column.len())
                            .ok_or_else(EngineMetadataJournal::identity_error)?;
                        if mapped.values.contains_key(&index) {
                            drop(index);
                            crate::memory_accounting::release_preparation_original(
                                &mut incoming.owner,
                                bytes,
                            );
                        } else {
                            let credit = crate::memory_accounting::detach_preparation_original(
                                &mut incoming.owner,
                                bytes,
                            )?;
                            let mut selected = AdmittedMetadataValue::new(index, credit);
                            crate::memory_accounting::merge_preparation_original(
                                &mut mapped.owner,
                                selected.invoice.as_mut().expect("selected index invoice"),
                            )?;
                            mapped
                                .values
                                .insert(selected.body.take().expect("selected index body"), epoch);
                        }
                    }
                    journal.prepare_metadata_epochs(tx, mapped.values, mapped.owner)
                })
            });
        }
        let refs = VectorSchemaGates::sorted_refs(refs);
        if refs.is_empty() {
            return Ok(());
        }
        self.tx_mgr.with_read(tx, |_| ())?;
        let observed = refs
            .into_iter()
            .map(|index| {
                let epoch = self.vector_schema_gates.epoch_for(&index);
                (index, epoch)
            })
            .collect::<Vec<_>>();
        let mut metadata = self.pending_commit_metadata.lock();
        let metadata = metadata.entry(tx).or_default();
        for (index, epoch) in observed {
            metadata.vector_schema_epochs.entry(index).or_insert(epoch);
        }
        Ok(())
    }

    pub(crate) fn clear_statement_cache(&self) {
        self.statement_cache.write().clear();
    }

    pub(super) fn clear_trigger_insert_state_machine_cache(db_key: DatabaseId) {
        TRIGGER_INSERT_STATE_MACHINE_CACHE.with(|cache| {
            cache.borrow_mut().remove(&db_key);
        });
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn trigger_insert_table_has_state_machine(&self, table: &str) -> Result<bool> {
        let db_key = self.id;
        if let Some(cached) = TRIGGER_INSERT_STATE_MACHINE_CACHE.with(|cache| {
            cache
                .borrow()
                .get(&db_key)
                .and_then(|by_table| by_table.get(table).copied())
        }) {
            return Ok(cached);
        }

        let has_state_machine = self
            .relational_store
            .table_has_state_machine(table)
            .ok_or_else(|| self.missing_table_error(table))?;
        TRIGGER_INSERT_STATE_MACHINE_CACHE.with(|cache| {
            cache
                .borrow_mut()
                .entry(db_key)
                .or_default()
                .insert(table.to_string(), has_state_machine);
        });
        Ok(has_state_machine)
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __statement_cache_len(&self) -> usize {
        let _operation = self.assert_open_operation();
        self.statement_cache.read().len()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn execute_autocommit(
        &self,
        plan: &PhysicalPlan,
        params: &HashMap<String, Value>,
        preopened_autocommit_tx: Option<TxId>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        native_trace_refund: Option<&crate::executor::bounded::NativeTraceRefund>,
    ) -> Result<OwnedQueryResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        match plan {
            PhysicalPlan::Insert(_)
            | PhysicalPlan::Delete(_)
            | PhysicalPlan::Update(_)
            | PhysicalPlan::Discard(_) => {
                let tx = match preopened_autocommit_tx {
                    Some(tx) => tx,
                    None => self.begin_for_public_autocommit_write()?,
                };
                let vector_schema_refs = self.plan_vector_schema_refs(plan);
                let _vector_schema = (!vector_schema_refs.is_empty())
                    .then(|| self.vector_schema_read_many(vector_schema_refs));
                let result = {
                    let _write_gate = self.enter_sql_write_control_bypass(tx);
                    let snapshot = self.snapshot_for_read();
                    self.with_snapshot_override(snapshot, || {
                        crate::executor::execute_plan_with_native_trace_refund(
                            self,
                            plan,
                            params,
                            Some(tx),
                            vector_copy_owner,
                            read_memory.clone(),
                            native_trace_refund,
                        )
                    })
                };
                match result {
                    Ok(mut qr) => {
                        let event_bus_ddl = self.take_pending_event_bus_ddl(tx);
                        let validation = self
                            .with_current_statement_snapshot_excluded_from_terminal_commit(
                                || {
                                    self.commit_with_source_and_event_bus_ddl(
                                        tx,
                                        CommitSource::AutoCommit,
                                        &event_bus_ddl,
                                        vector_copy_owner,
                                        read_memory.clone(),
                                    )
                                },
                            )?;
                        qr.rows_affected = qr
                            .rows_affected
                            .saturating_sub(validation.conditional_noop_count);
                        Ok(qr)
                    }
                    Err(e) => {
                        let _ = self.rollback_without_callback_tx_control(tx);
                        Err(e)
                    }
                }
            }
            _ => {
                if preopened_autocommit_tx.is_some() {
                    self.rollback_preopened_autocommit_tx(preopened_autocommit_tx);
                    return Err(Error::Other(
                        ("internal autocommit transaction reserved for non-DML statement"
                            .to_string())
                        .into(),
                    ));
                }
                let snapshot = self.snapshot_for_read();
                self.run_read_plan(
                    plan,
                    params,
                    None,
                    snapshot,
                    vector_copy_owner,
                    read_memory.clone(),
                    native_trace_refund,
                )
            }
        }
    }

    /// Answer a read the way the reading surface answers it.
    ///
    /// The plan is already chosen, so nothing is planned again: it is drained
    /// once through the same kernel a bounded read draws from, so one statement
    /// asked at either door reads the same sources, in the same order, and
    /// describes itself with the same words. This door is uncapped -- the
    /// established embedding API promises an answer, not a ceiling -- and a
    /// plan the kernel has no source for is run by the executor exactly as it
    /// always was.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn run_read_plan(
        &self,
        plan: &PhysicalPlan,
        params: &HashMap<String, Value>,
        tx: Option<TxId>,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        native_trace_refund: Option<&crate::executor::bounded::NativeTraceRefund>,
    ) -> Result<OwnedQueryResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if !crate::executor::bounded::kernel_can_answer(plan) {
            return crate::read_image_memory::QueryAnswerCharge::execute_legacy_with_active_image(
                || {
                    crate::executor::execute_plan_with_native_trace_refund(
                        self,
                        plan,
                        params,
                        tx,
                        vector_copy_owner,
                        read_memory.clone(),
                        native_trace_refund,
                    )
                },
            );
        }
        let clock: Arc<dyn contextdb_core::read_contract::DeadlineClock> =
            Arc::new(crate::local_transport::MonotonicDeadlineClock::new());
        // What THIS statement has already been charged with before the drain.
        // Sources the kernel shares with the eager path bump it as they read,
        // so the drain contributes part of its own figure on the way through.
        let examined_before = self.this_statements_rows_examined();
        let drained = crate::executor::bounded::drain_chosen_plan_with_native_trace_refund(
            self,
            plan,
            params,
            // The caller's own transaction, carried in rather than asked for:
            // an explicit handle is not the session's transaction, so a read
            // that asks the store which one is active is told none and answers
            // from committed state alone.
            tx,
            snapshot,
            clock,
            #[cfg(any(test, feature = "test-seams"))]
            Self::read_execution_kernel_probe_for_test()
                .map(crate::executor::bounded_read_test_support::kernel_probe),
            read_memory.clone(),
            native_trace_refund,
        );
        match drained {
            Ok(result) => {
                // The kernel counts what it looked at on its own request
                // context; the statement's scope is what the caller reads back
                // in the trace. So the route tops that scope up to the kernel's
                // figure -- only the part the shared sources did not already
                // bump, because adding the whole figure counts a resolution
                // scan twice, once through the source and once through the
                // trace.
                let contributed = match (self.this_statements_rows_examined(), examined_before) {
                    (Some(after), Some(before)) => after.saturating_sub(before),
                    // No scope of its own: nothing it reads is attributed to a
                    // statement, so there is nothing to subtract.
                    _ => 0,
                };
                self.__bump_rows_examined(result.trace.rows_examined.saturating_sub(contributed));
                Ok(result)
            }
            Err(crate::executor::BoundedExecutionError::Engine(error)) => Err(error),
            // Source capabilities and store admission can refuse even when
            // this door declares no request ceilings. Preserve their shared
            // typed detail so callers can distinguish erasure and limits.
            Err(crate::executor::BoundedExecutionError::Refused(failure)) => {
                Err(Error::ReadFailure(failure))
            }
            Err(other) => Err(Error::Other(
                (format!("uncapped read could not be answered: {other:?}")).into(),
            )),
        }
    }

    /// What the statement now running has been charged with, or `None` when
    /// nothing it reads is attributed to a statement at all.
    ///
    /// NOT the store-wide counter. That counter is stored back to zero by
    /// whichever statement most recently entered a scope on this database and
    /// is added to by every statement running against it, so a difference
    /// taken across it belongs to no statement in particular: two callers
    /// reading the same database at the same time would each fold part of the
    /// other's reading into their own trace, and the number an operator uses
    /// to judge a query would depend on what else happened to be running. The
    /// scope stack is per thread and the statement's own entry is the innermost
    /// one for this database, so reading it either side of the drain measures
    /// this statement's shared sources and nobody else's.
    pub(super) fn this_statements_rows_examined(&self) -> Option<u64> {
        let database_key = self.id;
        QUERY_ROWS_EXAMINED_SCOPES.with(|scopes| {
            scopes
                .borrow()
                .active
                .iter()
                .rev()
                .find(|scope| scope.database_key == database_key)
                .map(|scope| scope.rows_examined)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn explain(&self, sql: &str) -> Result<String> {
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.explain_with_read_memory(sql, &mut vector_copy_owner, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn explain_with_read_memory(
        &self,
        sql: &str,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<String> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let explained =
            self.explain_output_with_read_memory(sql, vector_copy_owner, read_memory.clone())?;
        let mut output = crate::cli_render::render_explain_output(&explained);
        if output.ends_with('\n') {
            output.pop();
        }
        if let Some(disclosure) = explained.vector_search.as_ref() {
            output.push_str(&format!(
                "\nvector_policy requested_mode: {} resolved_mode: {} \
                 aggregate_allowed_vectors: {} effective_auto_index_at: {} \
                 auto_index_at_source: {}",
                disclosure.requested_mode,
                disclosure.resolved_mode,
                render_optional_count(disclosure.aggregate_allowed_vectors),
                disclosure.effective_auto_index_at,
                disclosure.auto_index_at_source,
            ));
        }
        Ok(output)
    }

    /// Plan and disclose a statement without executing it. Vector facts come
    /// from passive catalog, authorization, and policy reads only.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn explain_output(&self, sql: &str) -> Result<ExplainOutput> {
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.explain_output_with_read_memory(sql, &mut vector_copy_owner, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn explain_output_with_read_memory(
        &self,
        sql: &str,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ExplainOutput> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.explain_output_with_params(
            sql,
            &HashMap::new(),
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    /// Parameter-aware passive planning used by the CLI rendering helper.
    /// Values inform route selection only; no statement is executed.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn explain_output_with_params(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ExplainOutput> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let stmt = contextdb_parser::parse(sql)?;
        let _schema_publication = self.enter_schema_publication_gate(false);
        let _operation = self.open_operation()?;
        let plan = contextdb_planner::plan(&stmt)?;
        // Refuse a statically-invalid query here too: rendering a plan for a
        // query that can never run reads as a green light for it.
        validate_plan_columns(self, &plan)?;
        let vector_shape = vector_search_shape_from_plan(&plan);
        let range_memo = crate::executor::IndexRangeMemo::default();
        let ranges = crate::executor::IndexRangeSizes::new(
            self,
            self.snapshot_for_read(),
            read_memory.clone(),
            &range_memo,
            &crate::executor::UnpacedIndexProbe,
        );
        let relational_route = if vector_shape.is_none() {
            analyze_passive_relational_route(self, &plan, params, &ranges)
        } else {
            None
        };
        range_memo.check_refusal()?;
        let _vector_schema = vector_shape
            .as_ref()
            .map(|shape| self.vector_schema_read(&shape.index));
        let snapshot = self.snapshot();
        let mut physical_plan = relational_route
            .as_ref()
            .map(|route| route.physical_plan.clone())
            .unwrap_or_else(|| plan.explain());
        let vector_inspection = vector_shape
            .as_ref()
            .map(|shape| {
                self.vector_search_disclosure(
                    shape,
                    snapshot,
                    params,
                    None,
                    vector_copy_owner,
                    read_memory.clone(),
                )
            })
            .transpose()?;
        let uses_indexed = vector_inspection
            .as_ref()
            .is_some_and(|(disclosure, indexed_available)| {
                matches!(
                    disclosure.route,
                    Some(VectorSearchRoute::Indexed | VectorSearchRoute::FilteredIndexed)
                ) && disclosure.refusal.is_none()
                    && *indexed_available
            });
        let vector_disclosure = vector_inspection.map(|(disclosure, _)| disclosure);
        if uses_indexed {
            physical_plan = annotate_vector_search_strategy(physical_plan, "Indexed");
            physical_plan =
                physical_plan.replace("VectorSearch {", "VectorSearch { strategy: Indexed,");
        } else {
            physical_plan = annotate_vector_search_strategy(physical_plan, "BruteForce");
            physical_plan =
                physical_plan.replace("VectorSearch {", "VectorSearch { strategy: BruteForce,");
        }
        Ok(ExplainOutput {
            physical_plan,
            index_used: relational_route
                .as_ref()
                .and_then(|route| route.index_used.clone()),
            predicates_pushed: relational_route
                .as_ref()
                .map(|route| route.predicates_pushed.clone())
                .unwrap_or_default(),
            indexes_considered: relational_route
                .as_ref()
                .map(|route| route.indexes_considered.clone())
                .unwrap_or_default(),
            sort_elided: relational_route
                .as_ref()
                .is_some_and(|route| route.sort_elided),
            vector_search: vector_disclosure,
        })
    }

    /// Read authorization from resident access metadata. This never invokes a
    /// table scan or loads a vector or graph.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn vector_metadata_authorized_candidates(
        &self,
        index: &VectorIndexRef,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<MetadataAuthorizedCandidates> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut memory = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        if !self.bounded_read_requires_candidate_filter(&index.table)? {
            return Ok(MetadataAuthorizedCandidates {
                ids: None,
                _memory: memory,
            });
        }
        let mut ids = RoaringTreemap::new();
        self.visit_authorized_row_metadata(
            &index.table,
            snapshot,
            |row| {
                if !ids.contains(row.row_id.0)
                    && self.vector_store.has_live_entry_for_row(
                        index,
                        row.row_id,
                        snapshot,
                        vector_copy_owner,
                    )?
                {
                    // Only an authorized identity can request memory. Neither a
                    // hidden population nor its row sizes affect this reservation.
                    let bytes = if ids.is_empty() { 544 } else { 32 };
                    let held = memory.bytes();
                    memory
                        .try_grow_for(
                            bytes,
                            "vector_search",
                            "explain_authorization",
                            "Narrow the inspected scope or raise MEMORY_LIMIT.",
                        )
                        .map_err(|error| match error {
                            Error::MemoryBudgetExceeded {
                                subsystem,
                                operation,
                                requested_bytes,
                                available_bytes,
                                hint,
                                ..
                            } => Error::MemoryBudgetExceeded {
                                subsystem,
                                operation,
                                requested_bytes,
                                available_bytes,
                                // The constrained caller sees this operation's budget,
                                // never the store's hidden resident allocation.
                                budget_limit_bytes: available_bytes.saturating_add(held),
                                hint,
                            },
                            other => other,
                        })?;
                    ids.insert(row.row_id.0);
                }
                Ok(())
            },
            read_memory.clone(),
        )?;
        Ok(MetadataAuthorizedCandidates {
            ids: Some(ids),
            _memory: memory,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn vector_policy_facts_for_explain(
        &self,
        shape: &VectorExplainShape<'_>,
        snapshot: SnapshotId,
        params: &HashMap<String, Value>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(VectorPolicyExplainFacts, MetadataAuthorizedCandidates)> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let requested_mode = self.resolved_vector_search_mode(
            &shape.index,
            shape
                .search_mode
                .map(crate::executor::map_vector_search_mode),
        )?;
        let layout = contextdb_vector::scoped_index_layout(
            &self.vector_store,
            &shape.index,
            vector_copy_owner,
        )?;
        let mut authorized = self.vector_metadata_authorized_candidates(
            &shape.index,
            snapshot,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        let candidate_route =
            crate::executor::vector_candidate_route(self, &shape.index, shape.candidates, params)?;
        // Count the rows the residual filter admits, from the same bitmap
        // execution builds, so the disclosed aggregate and the predicted route
        // are the executed ones. Only a fully bound, indexed or
        // directory-covered filter is read here; an unindexed one is compared
        // exactly either way, and an unbound one names no rows yet.
        if candidate_route.complete
            && crate::executor::bind_vector_search_parameters(
                shape.query_expr,
                shape.candidates,
                params,
            )
            .is_ok()
            && let (Some(admitted), _) = crate::executor::vector_candidate_bitmap(
                self,
                None,
                &shape.index,
                snapshot,
                shape.candidates,
                candidate_route.scope.as_ref(),
                candidate_route.candidates.as_ref(),
                candidate_route.access_filter,
                candidate_route.directory_route,
                params,
                vector_copy_owner,
                read_memory.clone(),
                // Admitted before the bitmap grows: its worst case, and the
                // same again for the intersection built while both inputs
                // are alive. Held with the authorization credit until the
                // narrowed set is dropped.
                &mut |max_ids| {
                    authorized._memory.try_grow_for(
                        max_ids
                            .saturating_mul(
                                contextdb_vector::mem::ROARING_TREEMAP_BYTES_PER_ID_UPPER_BOUND,
                            )
                            .saturating_mul(2),
                        "vector_search",
                        "explain_candidate_filter",
                        "Narrow the inspected scope or raise MEMORY_LIMIT.",
                    )
                },
            )?
        {
            authorized.ids = Some(match authorized.ids.take() {
                Some(ids) => ids & admitted,
                None => admitted,
            });
        }
        let partition_scope = &candidate_route.scope;
        let mut count = || {
            self.vector.authorized_visible_count_without_build(
                &shape.index,
                authorized.as_ref(),
                snapshot,
                vector_copy_owner,
            )
        };
        // Partition scope resolves after parameters are bound. A predicate
        // whose key identity is still an unbound placeholder names a scope
        // nobody has resolved yet, so there is no count to report: never the
        // whole column's aggregate, which belongs to a scope the caller did
        // not name and which a restricted handle must not learn.
        let aggregate_allowed_vectors = match partition_scope.as_ref() {
            Some(scope) => match scope.resolved_prefixes() {
                Some(prefixes) => Some(contextdb_vector::mem::with_selected_partition_prefixes(
                    &shape.index,
                    prefixes.to_vec(),
                    count,
                )?),
                None => None,
            },
            None => Some(count()?),
        };
        let policy = layout.resolve_policy();
        let filtered_exact =
            requested_mode == contextdb_core::VectorSearchMode::Auto && !candidate_route.complete;
        let resolved_mode = match (requested_mode, aggregate_allowed_vectors) {
            (contextdb_core::VectorSearchMode::Auto, Some(_)) if filtered_exact => {
                contextdb_core::VectorSearchMode::Exact
            }
            // AUTO is unresolved until the key binds: there is no trustworthy
            // count to derive a route from.
            (contextdb_core::VectorSearchMode::Auto, None) => {
                contextdb_core::VectorSearchMode::Auto
            }
            (contextdb_core::VectorSearchMode::Auto, Some(count))
                if count < policy.auto_index_at =>
            {
                contextdb_core::VectorSearchMode::Exact
            }
            (contextdb_core::VectorSearchMode::Auto, Some(_)) => {
                contextdb_core::VectorSearchMode::Indexed
            }
            (mode, _) => mode,
        };
        Ok((
            VectorPolicyExplainFacts {
                requested_mode,
                resolved_mode,
                aggregate_allowed_vectors,
                policy,
            },
            authorized,
        ))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn vector_search_disclosure(
        &self,
        shape: &VectorExplainShape<'_>,
        snapshot: SnapshotId,
        params: &HashMap<String, Value>,
        actual_trace: Option<&VectorSearchDebugTrace>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(VectorSearchDisclosure, bool)> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let actual_indexed = actual_trace.map(|trace| {
            trace
                .partition_routes
                .iter()
                .any(|partition| partition.route == VectorPartitionSearchRoute::Indexed)
        });
        let (mut facts, authorized) = self.vector_policy_facts_for_explain(
            shape,
            snapshot,
            params,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        let layout = self
            .vector_store
            .index_layout(&shape.index, vector_copy_owner)?;
        let candidate_route =
            crate::executor::vector_candidate_route(self, &shape.index, shape.candidates, params)?;
        let partition_scope = &candidate_route.scope;
        let native_routes = if self.has_native_ivf_source()
            && facts.aggregate_allowed_vectors.is_some()
        {
            if let Some(trace) = actual_trace {
                vector_copy_owner.reserve(
                    trace
                        .partition_routes
                        .len()
                        .checked_mul(std::mem::size_of::<VectorPartitionSearchTrace>())
                        .ok_or_else(|| Error::Other("native trace route bytes overflow".into()))?,
                )?;
                Some(trace.partition_routes.clone())
            } else {
                let breadth = crate::database::vector_ivf::policy::QueryBreadth::resolve(
                    &shape.index.table,
                    &shape.index.column,
                    shape.query_policy,
                )?;
                let mut predict = || {
                    self.native_ivf_predict_routes(
                        &shape.index,
                        shape.k,
                        facts.requested_mode,
                        breadth,
                        authorized.as_ref(),
                        snapshot,
                        read_memory.clone(),
                        vector_copy_owner,
                    )
                };
                Some(
                    if let Some(prefixes) = partition_scope
                        .as_ref()
                        .and_then(crate::executor::VectorPartitionScope::resolved_prefixes)
                    {
                        contextdb_vector::mem::with_selected_partition_prefixes(
                            &shape.index,
                            prefixes.to_vec(),
                            predict,
                        )?
                    } else {
                        predict()?
                    },
                )
            }
        } else {
            None
        };
        let indexed_available = if let Some(routes) = &native_routes {
            let indexed = routes
                .iter()
                .any(|route| route.route == VectorPartitionSearchRoute::Indexed);
            if facts.requested_mode == VectorSearchMode::Auto {
                facts.resolved_mode = if indexed {
                    VectorSearchMode::Indexed
                } else {
                    VectorSearchMode::Exact
                };
            }
            indexed
        } else {
            false
        };

        // An AUTO route over an unbound key is unresolved until the key binds.
        // Only the passive explain path can reach this state: every executing
        // reader binds its parameters before any scope or route is judged.
        let unresolved_until_bind = facts.resolved_mode == contextdb_core::VectorSearchMode::Auto;
        let scope = if unresolved_until_bind {
            // One redacted state, whatever number of unbound branches the
            // predicate happens to name.
            VectorSearchScopeShape::One
        } else {
            match partition_scope
                .as_ref()
                .map(crate::executor::VectorPartitionScope::selected_partitions)
            {
                Some(1) => VectorSearchScopeShape::One,
                Some(count) if count > 1 => VectorSearchScopeShape::Few,
                _ => VectorSearchScopeShape::All,
            }
        };
        let candidate_residual = vector_search_candidate_residual(shape, &candidate_route);
        let filtered_exact = facts.requested_mode == contextdb_core::VectorSearchMode::Auto
            && !candidate_route.complete;
        let mut partitions = self.vector_partition_disclosure(
            &shape.index,
            authorized.as_ref(),
            snapshot,
            partition_scope.as_ref(),
            None,
            facts.requested_mode,
            facts.resolved_mode == contextdb_core::VectorSearchMode::Exact,
            candidate_residual != VectorSearchResidual::None,
            actual_trace.map(|trace| trace.partition_routes.as_slice()),
            vector_copy_owner,
            read_memory.clone(),
        )?;
        if let Some(routes) = &native_routes {
            for (partition, route) in partitions.iter_mut().zip(routes) {
                partition.route = Some(if route.route == VectorPartitionSearchRoute::Indexed {
                    VectorSearchRoute::Indexed
                } else {
                    VectorSearchRoute::Exact
                });
                partition.reason = Some(route.reason.to_owned());
            }
        }
        let every_partition_uses_policy_exact = actual_trace.map_or_else(
            || {
                !partitions.is_empty()
                    && partitions
                        .iter()
                        .all(|partition| partition.route == Some(VectorSearchRoute::Exact))
            },
            |trace| {
                !trace.partition_routes.is_empty()
                    && trace
                        .partition_routes
                        .iter()
                        .all(|partition| partition.route == VectorPartitionSearchRoute::Exact)
            },
        );
        let exact_route = !unresolved_until_bind
            && (facts.resolved_mode == contextdb_core::VectorSearchMode::Exact
                || (facts.requested_mode == contextdb_core::VectorSearchMode::Auto
                    && (every_partition_uses_policy_exact
                        || actual_indexed == Some(false)
                        || (actual_indexed.is_none() && !indexed_available))));
        if exact_route && !every_partition_uses_policy_exact {
            for partition in &mut partitions {
                partition.route = Some(VectorSearchRoute::Exact);
            }
        }
        let resolved_mode = facts.resolved_mode;
        let relational_route = if exact_route {
            VectorSearchResidual::None
        } else {
            candidate_residual
        };
        let route = if unresolved_until_bind {
            None
        } else if exact_route {
            Some(VectorSearchRoute::Exact)
        } else if candidate_residual != VectorSearchResidual::None {
            Some(VectorSearchRoute::FilteredIndexed)
        } else {
            Some(VectorSearchRoute::Indexed)
        };
        let (refusal, recovery) = if !exact_route
            && facts.resolved_mode == contextdb_core::VectorSearchMode::Indexed
            && relational_route == VectorSearchResidual::Unsupported
        {
            (
                Some("filtered_route_unavailable".to_string()),
                vector_filter_index_recovery(
                    shape
                        .candidates
                        .filter(|_| shape.restricted_candidates)
                        .or(candidate_route.candidates.as_ref()),
                    &shape.index.table,
                ),
            )
        } else if !exact_route
            && facts.resolved_mode == contextdb_core::VectorSearchMode::Indexed
            && actual_indexed != Some(true)
            && !indexed_available
        {
            (
                Some("indexed_route_unavailable".to_string()),
                Some("run vector maintenance and retry".to_string()),
            )
        } else {
            (None, None)
        };
        let fallback = if unresolved_until_bind {
            Some("partition_scope_unresolved_until_bind".to_string())
        } else if let Some(trace) = actual_trace
            && facts.requested_mode == contextdb_core::VectorSearchMode::Auto
        {
            let reason = executed_auto_fallback(
                &trace.partition_routes,
                trace.fallback_reason,
                filtered_exact,
            );
            vector_copy_owner.reserve(reason.map_or(0, str::len))?;
            reason.map(str::to_owned)
        } else if filtered_exact {
            Some("filtered_route_exact".to_string())
        } else if facts.requested_mode == contextdb_core::VectorSearchMode::Auto
            && facts.resolved_mode == contextdb_core::VectorSearchMode::Exact
        {
            Some("aggregate_below_auto_index_at".to_string())
        } else if facts.requested_mode == contextdb_core::VectorSearchMode::Auto
            && every_partition_uses_policy_exact
        {
            Some(common_partition_reason(&partitions))
        } else if exact_route && facts.requested_mode == contextdb_core::VectorSearchMode::Auto {
            Some("maintained_index_unavailable".to_string())
        } else {
            None
        };
        // The clustered index is one base generation; it has no separate change layer.
        let base = indexed_available;
        let change = false;
        let tail = self.vector_pending_tail_without_load(
            &shape.index,
            authorized.as_ref(),
            snapshot,
            partition_scope
                .as_ref()
                .and_then(crate::executor::VectorPartitionScope::resolved_prefixes),
            None,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        vector_copy_owner.reserve(
            facts.policy.auto_index_at_source.len()
                + actual_trace
                    .and_then(|trace| trace.budget_receipt)
                    .map_or(0, |receipt| receipt.budget_terminal.len()),
        )?;
        Ok((
            VectorSearchDisclosure {
                route_evidence: if actual_trace.is_some() {
                    VectorSearchRouteEvidence::Executed
                } else {
                    VectorSearchRouteEvidence::PassivePrediction
                },
                requested_mode: facts.requested_mode,
                resolved_mode,
                aggregate_allowed_vectors: facts.aggregate_allowed_vectors,
                effective_auto_index_at: facts.policy.auto_index_at,
                auto_index_at_source: facts.policy.auto_index_at_source.to_owned(),
                partition_key_columns: layout.partition_key_columns,
                scope,
                route,
                base: if base {
                    VectorSearchLayerPresence::Present
                } else {
                    VectorSearchLayerPresence::Absent
                },
                change: if change {
                    VectorSearchLayerPresence::Present
                } else {
                    VectorSearchLayerPresence::Absent
                },
                tail: if tail {
                    VectorSearchTailState::Present
                } else {
                    VectorSearchTailState::Empty
                },
                residual: relational_route,
                fallback,
                refusal,
                recovery,
                query_source: match shape.query_expr {
                    Expr::RowVectorSource { .. } => VectorQuerySourceDisclosure::RedactedRowKey,
                    Expr::Literal(_) | Expr::Parameter(_) => VectorQuerySourceDisclosure::Vector,
                    _ => VectorQuerySourceDisclosure::Unknown,
                },
                partitions,
                global_candidate_budget: actual_trace.and_then(|trace| {
                    trace
                        .budget_receipt
                        .map(|receipt| receipt.global_candidate_budget)
                }),
                admission_evaluations: actual_trace.and_then(|trace| {
                    trace
                        .budget_receipt
                        .map(|receipt| receipt.admission_evaluations)
                }),
                expansion_evaluations: actual_trace.and_then(|trace| {
                    trace
                        .budget_receipt
                        .map(|receipt| receipt.expansion_evaluations)
                }),
                rescore_evaluations: actual_trace.and_then(|trace| {
                    trace
                        .budget_receipt
                        .map(|receipt| receipt.rescore_evaluations)
                }),
                layers_admitted: actual_trace
                    .and_then(|trace| trace.budget_receipt.map(|receipt| receipt.layers_admitted)),
                budget_terminal: actual_trace.and_then(|trace| {
                    trace
                        .budget_receipt
                        .map(|receipt| receipt.budget_terminal.to_owned())
                }),
            },
            indexed_available,
        ))
    }

    /// Read partition inspection facts only for the keys this statement
    /// selected. A complete key can be resolved directly; computing every
    /// partition's pending/tombstone counts first would walk every unrelated
    /// raw directory merely to discard those facts afterward.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn vector_partition_infos_for_prefixes(
        &self,
        index: &VectorIndexRef,
        prefixes: Option<&[VectorPartitionKey]>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        _read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<contextdb_vector::store::VectorPartitionInfo>> {
        let _read_memory: contextdb_core::read_memory::ReadMemoryOwner = _read_memory.into();

        let Some(prefixes) = prefixes else {
            return self.vector_store.partition_infos(index, vector_copy_owner);
        };
        let key_columns = self
            .vector_store
            .with_index_layout(index, |layout| layout.partition_key_columns.len())?;
        let mut key_copy = vector_copy_owner.local_scratch();
        let mut keys = if prefixes
            .iter()
            .all(|prefix| prefix.components().len() == key_columns)
        {
            prefixes.to_vec()
        } else {
            self.vector_store
                .partition_keys(index, key_copy.as_mut().unwrap_or(vector_copy_owner))?
                .into_iter()
                .filter(|key| {
                    prefixes
                        .iter()
                        .any(|prefix| key.components().starts_with(prefix.components()))
                })
                .collect()
        };
        keys.sort();
        keys.dedup();
        keys.into_iter()
            .map(|key| {
                self.vector_store
                    .try_partition_info(index, &key, vector_copy_owner)
            })
            .collect::<Result<Vec<_>>>()
            .map(|infos| infos.into_iter().flatten().collect())
    }

    /// One entry per selected partition with visible vectors, keys redacted.
    ///
    /// A key predicate whose identity is still an unbound placeholder names a
    /// scope nobody has resolved yet. The list then collapses to one redacted
    /// state: enumerating one entry per authorized partition would hand back,
    /// as the length of the list, the very count `aggregate_allowed_vectors`
    /// withholds for that same unnamed scope. A bound key, or no key
    /// predicate at all, reports each partition the caller actually named.
    ///
    /// Each partition carries the index members its column declares, with
    /// provenance. A member the declaration leaves to the engine is absent:
    /// it is chosen per query and no single value is true for the partition.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn vector_partition_disclosure(
        &self,
        index: &VectorIndexRef,
        authorized: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        scope: Option<&crate::executor::VectorPartitionScope>,
        groups: Option<&contextdb_vector::mem::BoundedVectorCandidateGroups>,
        requested_mode: contextdb_core::VectorSearchMode,
        force_exact: bool,
        filtered: bool,
        actual_routes: Option<&[VectorPartitionSearchTrace]>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<VectorPartitionDisclosure>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // Declared members can describe the complete partition population's
        // configuration; constrained callers get no per-partition entries.
        if self.bounded_read_requires_candidate_filter(&index.table)? {
            return Ok(Vec::new());
        }
        let layout =
            contextdb_vector::scoped_index_layout(&self.vector_store, index, vector_copy_owner)?;
        let revision = layout.resolve_policy().policy_revision;
        let declared = self.declared_ivf_members(index)?;
        let prefixes = match scope {
            None => None,
            Some(scope) => match scope.resolved_prefixes() {
                Some(prefixes) => Some(prefixes),
                None => {
                    return Ok(vec![declared.entry(
                        "<redacted:1>".to_owned(),
                        None,
                        None,
                        revision,
                    )]);
                }
            },
        };
        let route_of = |route: VectorPartitionSearchRoute| match route {
            VectorPartitionSearchRoute::Exact => VectorSearchRoute::Exact,
            VectorPartitionSearchRoute::Indexed if filtered => VectorSearchRoute::FilteredIndexed,
            VectorPartitionSearchRoute::Indexed => VectorSearchRoute::Indexed,
        };
        if let (Some(groups), Some(routes)) = (groups, actual_routes) {
            // Completed execution already chose every route. Stream its pinned
            // groups directly.
            let mut count = 0usize;
            groups.visit_nonempty_partitions(|_, _| {
                count += 1;
                Ok::<(), Error>(())
            })?;
            if count != routes.len() {
                return Err(Error::Other(
                    "executed vector route receipt does not match its groups".into(),
                ));
            }
            let reasons = routes.iter().map(|route| route.reason.len()).sum::<usize>();
            vector_copy_owner.reserve_for(
                contextdb_vector::mem::admission_bytes(
                    "explain_partition_disclosure",
                    count
                        .saturating_mul(EXPLAIN_PARTITION_DISCLOSURE_BYTES)
                        .saturating_add(reasons),
                ),
                "vector_search",
                "explain_partition_disclosure",
                "Narrow the inspected scope or raise MEMORY_LIMIT.",
            )?;
            return Ok(routes
                .iter()
                .enumerate()
                .map(|(position, route)| {
                    declared.entry(
                        format!("<redacted:{}>", position + 1),
                        Some(route_of(route.route)),
                        Some(route.reason.to_owned()),
                        revision,
                    )
                })
                .collect());
        }
        let mut disclosure = Vec::new();
        let mut actual_position = 0usize;
        let mut partitions = contextdb_vector::ScopedVectorCopy::new(vector_copy_owner, |scratch| {
            self.vector_partition_infos_for_prefixes(index, prefixes, scratch, read_memory.clone())
        })?;
        partitions.sort_by(|left, right| {
            left.partition
                .partition_key
                .cmp(&right.partition.partition_key)
        });
        for partition in partitions.into_iter() {
            let visible = contextdb_vector::mem::with_selected_partition_prefixes(
                index,
                vec![partition.partition.partition_key.clone()],
                || {
                    self.vector.authorized_visible_count_without_build(
                        index,
                        authorized,
                        snapshot,
                        vector_copy_owner,
                    )
                },
            )?;
            if visible == 0 {
                continue;
            }
            vector_copy_owner.reserve_for(
                contextdb_vector::mem::admission_bytes(
                    "explain_partition_disclosure",
                    EXPLAIN_PARTITION_DISCLOSURE_BYTES,
                ),
                "vector_search",
                "explain_partition_disclosure",
                "Narrow the inspected scope or raise MEMORY_LIMIT.",
            )?;
            let (route, reason) = if let Some(actual_routes) = actual_routes {
                let actual = actual_routes.get(actual_position).ok_or_else(|| {
                    Error::Other(
                        "executed vector partition route receipt ended before disclosure".into(),
                    )
                })?;
                actual_position += 1;
                (route_of(actual.route), Some(actual.reason.to_owned()))
            } else if requested_mode == contextdb_core::VectorSearchMode::Exact {
                (VectorSearchRoute::Exact, Some("exact_requested".to_owned()))
            } else if force_exact {
                (
                    VectorSearchRoute::Exact,
                    Some("predicted_exact_route".to_owned()),
                )
            } else if filtered {
                (
                    VectorSearchRoute::FilteredIndexed,
                    Some("predicted_indexed_route".to_owned()),
                )
            } else {
                (
                    VectorSearchRoute::Indexed,
                    Some("predicted_indexed_route".to_owned()),
                )
            };
            disclosure.push(declared.entry(
                format!("<redacted:{}>", disclosure.len() + 1),
                Some(route),
                reason,
                revision,
            ));
        }
        if let Some(actual_routes) = actual_routes
            && actual_position != actual_routes.len()
        {
            return Err(Error::Other(
                "executed vector partition route receipt outlived disclosure scope".into(),
            ));
        }
        Ok(disclosure)
    }

    /// The IVF members this column declares, read from its authored
    /// declaration and nothing else.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn declared_ivf_members(&self, index: &VectorIndexRef) -> Result<DeclaredIvfMembers> {
        let column = self
            .table_meta(&index.table)
            .and_then(|meta| {
                meta.columns
                    .into_iter()
                    .find(|column| column.name == index.column)
            })
            .ok_or_else(|| Error::UnknownVectorIndex {
                index: index.clone(),
            })?;
        Ok(DeclaredIvfMembers {
            lists: column.ivf_lists,
            probes: column.ivf_probes,
            refine_factor_hundredths: column.ivf_refine_factor_hundredths,
        })
    }

    /// Whether the selected authorized route has committed work newer than
    /// its sealed generations. Partition inspection already computes this
    /// from the raw directory and generation frontier, so explain can report
    /// the fresh exact tail without loading or replaying that tail.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn vector_pending_tail_without_load(
        &self,
        index: &VectorIndexRef,
        authorized: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        prefixes: Option<&[VectorPartitionKey]>,
        groups: Option<&contextdb_vector::mem::BoundedVectorCandidateGroups>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut local_copy_admission = vector_copy_owner.local_scratch();
        let vector_copy_owner = local_copy_admission.as_mut().unwrap_or(vector_copy_owner);
        for partition in self.vector_partition_infos_for_prefixes(
            index,
            prefixes,
            vector_copy_owner,
            read_memory.clone(),
        )? {
            if partition.pending_inserts == 0 && partition.tombstones == 0 {
                continue;
            }
            let visible = if let Some(groups) = groups {
                groups.partition_allowed_count(&partition.partition.partition_key)
            } else {
                contextdb_vector::mem::with_selected_partition_prefixes(
                    index,
                    vec![partition.partition.partition_key],
                    || {
                        self.vector.authorized_visible_count_without_build(
                            index,
                            authorized,
                            snapshot,
                            vector_copy_owner,
                        )
                    },
                )?
            };
            if visible > 0 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_search_disclosure_for_runtime(
        &self,
        index: VectorIndexRef,
        k: usize,
        restricted_candidates: bool,
        candidates: Option<&PhysicalPlan>,
        search_mode: Option<contextdb_parser::ast::VectorSearchMode>,
        query_expr: &Expr,
        snapshot: SnapshotId,
        params: &HashMap<String, Value>,
        trace: &VectorSearchDebugTrace,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Box<VectorSearchDisclosure>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // The enclosing answer copy owner retains this invoice through every
        // internal consumer and settles it at the existing public handoff.
        vector_copy_owner.reserve(std::mem::size_of::<VectorSearchDisclosure>())?;
        self.vector_search_disclosure(
            &VectorExplainShape {
                index,
                k,
                restricted_candidates,
                candidates,
                search_mode,
                query_policy: None,
                query_expr,
            },
            snapshot,
            params,
            Some(trace),
            vector_copy_owner,
            read_memory.clone(),
        )
        .map(|(disclosure, _)| Box::new(disclosure))
    }

    /// Describe a vector answer from the same admitted partition groups that
    /// execution consumed, without repeating candidate membership work.
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn completed_vector_search_disclosure(
        &self,
        index: VectorIndexRef,
        k: usize,
        restricted_candidates: bool,
        candidates: Option<&PhysicalPlan>,
        search_mode: Option<contextdb_parser::ast::VectorSearchMode>,
        query_expr: &Expr,
        snapshot: SnapshotId,
        params: &HashMap<String, Value>,
        aggregate_allowed_vectors: usize,
        groups: &contextdb_vector::mem::BoundedVectorCandidateGroups,
        partition_routes: &[VectorPartitionSearchTrace],
        fallback_reason: Option<&'static str>,
        budget_receipt: Option<contextdb_vector::mem::VectorSearchBudgetReceipt>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<VectorSearchDisclosure> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let shape = VectorExplainShape {
            index,
            k,
            restricted_candidates,
            candidates,
            search_mode,
            query_policy: None,
            query_expr,
        };
        let requested_mode = self.resolved_vector_search_mode(
            &shape.index,
            shape
                .search_mode
                .map(crate::executor::map_vector_search_mode),
        )?;
        let layout = self
            .vector_store
            .index_layout(&shape.index, vector_copy_owner)?;
        let policy = layout.resolve_policy();
        let candidate_route =
            crate::executor::vector_candidate_route(self, &shape.index, shape.candidates, params)?;
        let partition_scope = &candidate_route.scope;
        let filtered_exact =
            requested_mode == contextdb_core::VectorSearchMode::Auto && !candidate_route.complete;
        let native_indexed = self.has_native_ivf_source()
            && partition_routes
                .iter()
                .any(|route| route.route == VectorPartitionSearchRoute::Indexed);
        let resolved_mode = if self.has_native_ivf_source()
            && requested_mode == VectorSearchMode::Auto
        {
            if native_indexed {
                VectorSearchMode::Indexed
            } else {
                VectorSearchMode::Exact
            }
        } else {
            match requested_mode {
                contextdb_core::VectorSearchMode::Auto
                    if filtered_exact || aggregate_allowed_vectors < policy.auto_index_at =>
                {
                    contextdb_core::VectorSearchMode::Exact
                }
                contextdb_core::VectorSearchMode::Auto => contextdb_core::VectorSearchMode::Indexed,
                mode => mode,
            }
        };
        let scope = match partition_scope
            .as_ref()
            .map(crate::executor::VectorPartitionScope::selected_partitions)
        {
            Some(1) => VectorSearchScopeShape::One,
            Some(count) if count > 1 => VectorSearchScopeShape::Few,
            _ => VectorSearchScopeShape::All,
        };
        let candidate_residual = vector_search_candidate_residual(&shape, &candidate_route);
        let force_policy_exact = resolved_mode == VectorSearchMode::Exact;
        // Inspect captured pending work before retaining the output disclosure.
        // The groups already own the authorized source set and its original root.
        let base = native_indexed;
        let change = false;
        let tail = groups.has_pending_tail_without_load(vector_copy_owner)?;
        let partitions = self.vector_partition_disclosure(
            &shape.index,
            None,
            snapshot,
            partition_scope.as_ref(),
            Some(groups),
            requested_mode,
            force_policy_exact,
            groups.is_filtered() || candidate_residual != VectorSearchResidual::None,
            Some(partition_routes),
            vector_copy_owner,
            read_memory.clone(),
        )?;
        let route = if resolved_mode == contextdb_core::VectorSearchMode::Exact
            || (!native_indexed && requested_mode == contextdb_core::VectorSearchMode::Auto)
        {
            VectorSearchRoute::Exact
        } else if candidate_residual != VectorSearchResidual::None {
            VectorSearchRoute::FilteredIndexed
        } else {
            VectorSearchRoute::Indexed
        };
        let residual = if route == VectorSearchRoute::Exact {
            VectorSearchResidual::None
        } else {
            candidate_residual
        };
        let fallback = if requested_mode == contextdb_core::VectorSearchMode::Auto {
            let reason = executed_auto_fallback(partition_routes, fallback_reason, filtered_exact);
            vector_copy_owner.reserve(reason.map_or(0, str::len))?;
            reason.map(str::to_owned)
        } else {
            None
        };
        vector_copy_owner.reserve(
            policy.auto_index_at_source.len()
                + budget_receipt.map_or(0, |receipt| receipt.budget_terminal.len()),
        )?;
        Ok(VectorSearchDisclosure {
            route_evidence: VectorSearchRouteEvidence::Executed,
            requested_mode,
            resolved_mode,
            aggregate_allowed_vectors: Some(aggregate_allowed_vectors),
            effective_auto_index_at: policy.auto_index_at,
            auto_index_at_source: policy.auto_index_at_source.to_owned(),
            partition_key_columns: layout.partition_key_columns,
            scope,
            route: Some(route),
            base: if base {
                VectorSearchLayerPresence::Present
            } else {
                VectorSearchLayerPresence::Absent
            },
            change: if change {
                VectorSearchLayerPresence::Present
            } else {
                VectorSearchLayerPresence::Absent
            },
            tail: if tail {
                VectorSearchTailState::Present
            } else {
                VectorSearchTailState::Empty
            },
            residual,
            fallback,
            refusal: None,
            recovery: None,
            query_source: match shape.query_expr {
                Expr::RowVectorSource { .. } => VectorQuerySourceDisclosure::RedactedRowKey,
                Expr::Literal(_) | Expr::Parameter(_) => VectorQuerySourceDisclosure::Vector,
                _ => VectorQuerySourceDisclosure::Unknown,
            },
            partitions,
            global_candidate_budget: budget_receipt.map(|receipt| receipt.global_candidate_budget),
            admission_evaluations: budget_receipt.map(|receipt| receipt.admission_evaluations),
            expansion_evaluations: budget_receipt.map(|receipt| receipt.expansion_evaluations),
            rescore_evaluations: budget_receipt.map(|receipt| receipt.rescore_evaluations),
            layers_admitted: budget_receipt.map(|receipt| receipt.layers_admitted),
            budget_terminal: budget_receipt.map(|receipt| receipt.budget_terminal.to_owned()),
        })
    }

    pub(crate) fn publish_vector_search_trace(
        &self,
        trace: contextdb_vector::mem::OwnedVectorSearchTrace,
    ) {
        #[cfg(feature = "test-seams")]
        crate::vector_read_memory::statement_child_proof::trace_published(
            &trace,
            self.last_vector_search_trace.read().as_ref(),
        );
        let prior = self.last_vector_search_trace.write().replace(trace);
        drop(prior);
    }

    pub(crate) fn clear_last_vector_search_trace(&self) {
        #[cfg(feature = "test-seams")]
        crate::vector_read_memory::statement_child_proof::trace_cleared(
            self.last_vector_search_trace.read().as_ref(),
        );
        let prior = self.last_vector_search_trace.write().take();
        drop(prior);
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn execute_in_tx(
        &self,
        tx: TxId,
        sql: &str,
        params: &HashMap<String, Value>,
    ) -> Result<QueryResult> {
        let child = self.ordinary_read_child()?;
        crate::executor::bounded::with_native_read_memory(
            child,
            |read_memory, native_trace_refund| {
                QueryResult::publish_with(read_memory.clone(), || {
                    self.execute_in_tx_with_native_trace_refund(
                        tx,
                        sql,
                        params,
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
    pub(crate) fn execute_in_tx_with_read_memory(
        &self,
        tx: TxId,
        sql: &str,
        params: &HashMap<String, Value>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<OwnedQueryResult> {
        self.execute_in_tx_with_native_trace_refund(tx, sql, params, read_memory, None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "The existing inline native refusal is preserved without allocating an error."
    )]
    pub(crate) fn execute_in_tx_with_native_trace_refund(
        &self,
        tx: TxId,
        sql: &str,
        params: &HashMap<String, Value>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        native_trace_refund: Option<&crate::executor::bounded::NativeTraceRefund>,
    ) -> Result<OwnedQueryResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let answer_memory = crate::read_image_memory::ImageMemory::query_reader_for(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            read_memory.clone(),
        )?;
        // Native trace suppliers retain the original pre-execution image
        // custody. An explicitly supplied owner needs no native trace keeper.
        let transient_answer = native_trace_refund.is_none()
            && answer_memory.value.scope(|| -> Result<bool> {
                Ok(match self.cached_statement(sql) {
                    Some(cached) => matches!(cached.stmt, Statement::Select(_)),
                    None => matches!(contextdb_parser::parse(sql)?, Statement::Select(_)),
                })
            })?;
        let execute = |image: &crate::read_image_memory::ImageMemoryHandle| {
            let _decode_scope = image.enter();
            let mut answer_copy = image.vector_copy(0)?;
            let scope = QueryRowsExaminedScope::enter(self);
            let mut result = self.execute_in_tx_with_rows_examined_scope(
                tx,
                sql,
                params,
                &mut answer_copy.owner,
                read_memory.clone(),
                native_trace_refund,
            );
            scope.finish(&mut result);
            if result.is_ok() {
                answer_copy.into_image();
            }
            result
        };
        if transient_answer {
            crate::read_image_memory::QueryAnswerCharge::execute_with_transient_image(
                answer_memory,
                execute,
            )
        } else {
            crate::read_image_memory::QueryAnswerCharge::execute_with_image(answer_memory, execute)
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn run_callback_dml_plan(
        &self,
        tx: TxId,
        plan: &PhysicalPlan,
        params: &HashMap<String, Value>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<OwnedQueryResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let checkpoint = self.write_set_checkpoint(tx)?;
        let result = execute_plan(
            self,
            plan,
            params,
            Some(tx),
            vector_copy_owner,
            read_memory.clone(),
        );
        if result.is_err() {
            let _ = self.restore_write_set_checkpoint(tx, checkpoint);
        } else {
            checkpoint.settle();
        }
        result
    }

    /// Register a commit-time guard that requires the visible row selected by
    /// `key_column = key_value` to keep the supplied column values until this
    /// transaction commits. Returns `Ok(false)` when the row is not visible or
    /// does not currently match the predicates.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn guard_row_conditions_in_tx(
        &self,
        tx: TxId,
        table: &str,
        key_column: &str,
        key_value: &Value,
        predicates: &[(String, Value)],
    ) -> Result<bool> {
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.guard_row_conditions_in_tx_with_read_memory(
            tx,
            table,
            key_column,
            key_value,
            predicates,
            &mut vector_copy_owner,
            read_memory,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn guard_row_conditions_in_tx_with_read_memory(
        &self,
        tx: TxId,
        table: &str,
        key_column: &str,
        key_value: &Value,
        predicates: &[(String, Value)],
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation_after_public_tx_control_wait_for_tx(
            "guard_row_conditions_in_tx",
            Some(tx),
        )?;
        self.tx_mgr.with_read(tx, |_| ())?;
        self.assert_table_read_allowed(table)?;
        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
        if !meta.columns.iter().any(|column| column.name == key_column) {
            return Err(Error::ColumnNotFound {
                table: table.to_string(),
                column: key_column.to_string(),
            });
        }
        for (column, _) in predicates {
            if !meta
                .columns
                .iter()
                .any(|definition| definition.name == *column)
            {
                return Err(Error::ColumnNotFound {
                    table: table.to_string(),
                    column: column.clone(),
                });
            }
        }

        let snapshot = self.snapshot();
        let Some(mut row) = self.relational_point_with_tx(
            Some(tx),
            table,
            key_column,
            key_value,
            snapshot,
            read_memory.clone(),
        )?
        else {
            return Ok(false);
        };
        let staged = self.tx_mgr.with_read(tx, |ws| {
            ws.relational_inserts
                .iter()
                .rev()
                .any(|(staged_table, staged)| staged_table == table && staged.row_id == row.row_id)
        })?;
        let vector_columns = meta
            .columns
            .iter()
            .filter(|column| matches!(column.column_type, ColumnType::Vector(_)))
            .map(|column| column.name.as_str())
            .collect::<HashSet<_>>();
        let needs_body = !staged
            && predicates.iter().any(|(column, _)| {
                !vector_columns.contains(column.as_str()) && !row.values.contains_key(column)
            });
        if needs_body {
            let Some(committed) =
                self.committed_row_at(table, row.row_id, snapshot, read_memory.clone())?
            else {
                return Ok(false);
            };
            row = committed;
        }
        if !self.read_allowed_for_row(table, &meta, &row, snapshot, read_memory.clone())? {
            return Ok(false);
        }
        // A quantized vector column is held in its index, not in the row. Load
        // only vectors this guard actually compares; unrelated embeddings do
        // not participate in either the initial claim or commit validation.
        let mut guarded = [row];
        let predicate_columns = predicates
            .iter()
            .map(|(column, _)| column.clone())
            .collect::<Vec<_>>();
        self.supplement_selected_vector_columns(
            table,
            snapshot,
            Some(tx),
            Some(&predicate_columns),
            &mut guarded,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        let [row] = guarded;
        if !row_conditions_match(&row, &meta, predicates) {
            return Ok(false);
        }
        let counts = self.write_set_counts(tx)?;
        self.record_conditional_update_guard(
            tx,
            table.to_string(),
            row.row_id,
            predicates.to_vec(),
            counts,
            counts,
            true,
        )?;
        Ok(true)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn commit_with_source(
        &self,
        tx: TxId,
        source: CommitSource,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.commit_with_source_returning_txid(tx, source, vector_copy_owner, read_memory)
            .map(|_| ())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn commit_with_source_returning_txid(
        &self,
        tx: TxId,
        source: CommitSource,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<TxId>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let event_bus_ddl = self.take_pending_event_bus_ddl(tx);
        if self.pending_local_schema_stages.lock().contains_key(&tx) && !event_bus_ddl.is_empty() {
            self.pending_event_bus_ddl
                .lock()
                .entry(tx)
                .or_default()
                .extend(event_bus_ddl);
            return Err(Error::Other((
                "event-bus DDL and transactional local table schema DDL cannot be combined yet; commit or roll back before changing the other schema surface"
                    .to_string()).into()));
        }
        self.commit_with_source_and_event_bus_ddl(
            tx,
            source,
            &event_bus_ddl,
            vector_copy_owner,
            read_memory.clone(),
        )
        .map(|outcome| outcome.committed_tx)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_and_register_local_schema_stage(
        &self,
        tx: TxId,
        visibility_tx: TxId,
        lsn: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<schema_stage_registry::LocalRegistration>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.ensure_local_schema_stage_not_discarded(tx)?;
        let Some(pending) = self.pending_local_schema_stages.lock().remove(&tx) else {
            return Ok(None);
        };
        // Statement routing fences SQL row DML and the public row helpers,
        // but graph/vector and future mutation entrypoints share the generic
        // WriteSet.  The commit boundary is the final authority: schema
        // staging is DDL-only until a full mixed-image implementation exists.
        if !self.tx_mgr.with_read(tx, |read| {
            let no_requires = false;
            contextdb_tx::manager::EngineRead {
                requires_commit_lsn: &no_requires,
                ..read
            }
            .is_empty()
        })? {
            return Err(Error::Other((
                "mixed DML and transactional local schema DDL is not supported; commit or roll back before changing schema"
                    .to_string()).into()));
        }
        if *self.relational_store.table_meta.read() != pending.base_tables {
            return Err(Error::Other((
                "concurrent schema change invalidated this transactional local DDL; retry the transaction"
                    .to_string()).into()));
        }
        if self.received_event_bus_projection() != pending.base_event_bus {
            return Err(Error::Other((
                "concurrent event-bus change invalidated this transactional local DDL; retry the transaction"
                    .to_string()).into()));
        }
        if *self.trigger.declarations.lock() != pending.base_triggers {
            return Err(Error::Other((
                "concurrent trigger change invalidated this transactional local DDL; retry the transaction"
                    .to_string()).into()));
        }
        for (table, expected_generation) in &pending.base_table_generations {
            if self.authoritative_table_generation_counter(table)? != *expected_generation {
                return Err(Error::Other((
                    "concurrent table generation change invalidated this transactional local DDL; retry the transaction"
                        .to_string()).into()));
            }
        }
        let early_memory = if matches!(self.tx_mgr, DatabaseTransactionHost::Owned(_)) {
            Some(TemporaryMemoryReservation::reserve_for_database(
                self,
                pending
                    .working
                    .local_schema_pending_baseline_bytes()
                    .checked_add(
                        pending
                            .working
                            .received_event_bus_projection_bytes()?
                            .checked_mul(3)
                            .ok_or_else(EngineMetadataJournal::identity_error)?,
                    )
                    .and_then(|bytes| {
                        bytes.checked_add(
                            pending
                                .working
                                .ddl_log
                                .read()
                                .iter()
                                .map(|(_, change)| change.estimated_bytes())
                                .fold(0usize, usize::saturating_add)
                                .saturating_mul(3),
                        )
                    })
                    .ok_or_else(EngineMetadataJournal::identity_error)?,
                "prepare_transactional_schema_metadata",
            )?)
        } else {
            None
        };
        let event_bus_changed =
            pending.working.received_event_bus_projection() != pending.base_event_bus;
        let working = pending.working;
        let staged_ddl = working.ddl_log.read().clone();
        if staged_ddl.is_empty() {
            return Err(Error::Other(
                ("transactional local schema stage contains no DDL".to_string()).into(),
            ));
        }
        let staged_changes = staged_ddl
            .iter()
            .map(|(_, change)| change.clone())
            .collect::<Vec<_>>();
        let tables = working.relational_store.table_meta.read().clone();
        let projected_tables = staged_changes
            .iter()
            .filter_map(|change| match change {
                DdlChange::CreateTable { name, .. }
                | DdlChange::AlterTable { name, .. }
                | DdlChange::CreateIndex { table: name, .. } => Some(name.as_str()),
                _ => None,
            })
            .collect::<HashSet<_>>();

        // Prepare only tables whose metadata changes. The supported shapes do
        // not mutate stored row values, graph edges, or vector payloads, so a
        // schema commit never builds a whole-database replacement. Reserve
        // the affected projection's peak before cloning any live rows.
        let trigger_projection_bytes =
            working
                .trigger
                .declarations
                .lock()
                .values()
                .fold(0usize, |bytes, declaration| {
                    bytes
                        .saturating_add(64)
                        .saturating_add(declaration.name.len())
                        .saturating_add(declaration.table.len())
                        .saturating_add(declaration.on_events.len().saturating_mul(8))
                });
        let projection_peak_bytes = {
            let live_rows = self.relational_store.tables.read();
            projected_tables.iter().fold(
                trigger_projection_bytes
                    .saturating_mul(6)
                    .saturating_add(4096),
                |bytes, table| {
                    let Some(meta) = tables.get(*table) else {
                        return bytes;
                    };
                    let rows = live_rows.get(*table).map(Vec::as_slice).unwrap_or_default();
                    let row_bytes = rows.iter().fold(0usize, |row_bytes, row| {
                        row_bytes.saturating_add(retained_row_bytes_for_meta(
                            table,
                            &row.values,
                            meta,
                            true,
                        ))
                    });
                    // The projection copies the DECLARATION while it builds,
                    // and copies nothing of the index storage the declaration
                    // allocates: an exact index's 64 KiB probe bitmap is
                    // allocated once and kept, so multiplying it turned a
                    // six-table startup schema into more than a megabyte of
                    // reservation against a declared limit and refused work
                    // the machine had room for.
                    bytes
                        .saturating_add(meta.metadata_bytes().saturating_mul(4))
                        .saturating_add(meta.exact_probe_filter_bytes())
                        .saturating_add(row_bytes.saturating_mul(4))
                },
            )
        };
        let mut temporary_memory = TemporaryMemoryReservation::reserve_for_database(
            self,
            projection_peak_bytes,
            "commit_transactional_schema",
        )?;
        let relational = {
            let live_rows = self.relational_store.tables.read();
            projected_tables
                .iter()
                .map(|table| {
                    let meta = tables.get(*table).cloned().ok_or_else(|| {
                        Error::SyncError(
                            (format!(
                                "transactional schema projection lost affected table {table}"
                            ))
                            .into(),
                        )
                    })?;
                    let mut projection = RelationalStore::table_projection(
                        (*table).to_string(),
                        meta,
                        live_rows.get(*table).cloned().unwrap_or_default(),
                    );
                    projection
                        .admit_current_memberships(self.finalization.accountant.read_budget()?)?;
                    Ok(projection)
                })
                .collect::<Result<Vec<_>>>()?
        };
        let event_bus_projection = working.received_event_bus_projection();
        let native_events = matches!(self.tx_mgr, DatabaseTransactionHost::Owned(_));
        let (queue_mutation_lease, event_bus, native_event_bus) =
            if event_bus_changed && native_events {
                let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr else {
                    unreachable!("selected owning schema host");
                };
                let publication = handle.with_host(|host| {
                    host.events.with_owned_persistence_original(|original| {
                        let projection = event_bus::OwnedDefinitionsProjection::try_copy_filtered(
                            original,
                            &event_bus_projection,
                            None,
                        )?;
                        host.events.prepare_owned_definition_publication(
                            projection,
                            event_bus::OwnedDefinitionMode::Replace,
                            self.event_bus.begin_queue_mutation(),
                        )
                    })
                })?;
                (None, None, Some(publication))
            } else if event_bus_changed {
                (
                    Some(self.event_bus.begin_queue_mutation()),
                    Some(self.prepare_received_event_bus_publication_from_projection(
                        event_bus_projection,
                    )?),
                    None,
                )
            } else {
                (None, None, None)
            };
        let declarations = working.trigger.declarations.lock().clone();
        let (trigger, native_trigger) = if matches!(self.tx_mgr, DatabaseTransactionHost::Owned(_))
        {
            let publication = self.with_preparation_original(|original| {
                self.prepare_original_trigger_publication(original, &declarations)
            })?;
            (
                trigger::PreparedTriggerPublication::empty_placeholder(),
                Some(publication),
            )
        } else {
            (
                self.prepare_received_trigger_publication_from_projection(declarations)?,
                None,
            )
        };
        // `BEGIN; CREATE INDEX ...; COMMIT` builds one posting for every row
        // already in the table -- `table_projection` below assembles the
        // publication under the WORKING meta, which is the one carrying the
        // new index. The staging reservation is temporary and `Drop` returns
        // it in full, so without a row term here the only DURABLE charge this
        // door takes is the metadata swap, and the store keeps postings the
        // accountant never saw. The statement cannot have charged them
        // either: transactional DDL runs against a detached working database
        // with its own accountant, so the whole row term is new at the
        // commit.
        let old_metas = {
            let live_meta = self.relational_store.table_meta.read();
            projected_tables
                .iter()
                .filter_map(|table| {
                    live_meta
                        .get(*table)
                        .map(|meta| ((*table).to_string(), meta.clone()))
                })
                .collect::<Vec<_>>()
        };
        let old_bytes = old_metas.iter().fold(0usize, |bytes, (table, meta)| {
            bytes
                .saturating_add(retained_table_metadata_bytes(&self.relational_store, meta))
                .saturating_add(self.committed_rows_priced_under(table, meta))
        });
        let new_bytes = projected_tables.iter().fold(0usize, |bytes, table| {
            let Some(meta) = tables.get(*table) else {
                return bytes;
            };
            bytes
                .saturating_add(retained_table_metadata_bytes(&self.relational_store, meta))
                .saturating_add(self.committed_rows_priced_under(table, meta))
        });
        // A transaction that another caller holds open over one of these
        // tables staged its rows under the shape this commit is replacing.
        let mut staged_old = 0usize;
        let mut staged_new = 0usize;
        let mut staged_repricing: HashMap<TxId, Vec<(String, RowId, usize)>> = HashMap::new();
        for (table, old_meta) in &old_metas {
            let Some(new_meta) = tables.get(table.as_str()) else {
                continue;
            };
            let (old, new, per_tx) =
                self.staged_rows_priced_across_shape_change(table, old_meta, new_meta);
            staged_old = staged_old.saturating_add(old);
            staged_new = staged_new.saturating_add(new);
            for (tx, rows) in per_tx {
                staged_repricing.entry(tx).or_default().extend(rows);
            }
        }
        let old_bytes = old_bytes.saturating_add(staged_old);
        let new_bytes = new_bytes.saturating_add(staged_new);
        let affected_tables = projected_tables
            .iter()
            .map(|table| (*table).to_string())
            .collect::<HashSet<_>>();
        let dag_edge_types = MemGraphExecutor::<DynStore>::prepare_dag_edge_types(
            tables
                .values()
                .flat_map(|meta| meta.dag_edge_types.iter().cloned()),
        );
        let rank_formulas = Self::prepare_rank_formula_cache(&tables)?;
        let vector_layouts = vector_layouts_from_meta(&tables)
            .into_iter()
            .filter(|(index, _)| affected_tables.contains(&index.table))
            .collect::<Vec<_>>();
        // A scalar column or relational-index declaration does not replace
        // vector storage. Keep those exact column states (including retained
        // generations) without opening any canonical-body history cursor.
        // Only an actually changed vector declaration needs a replacement.
        vector_copy_owner.reserve(
            pending
                .base_tables
                .iter()
                .flat_map(|(table, meta)| meta.columns.iter().map(move |column| (table, column)))
                .filter(|(_, column)| matches!(column.column_type, ColumnType::Vector(_)))
                .map(|(table, column)| {
                    table
                        .len()
                        .saturating_add(column.name.len())
                        .saturating_add(std::mem::size_of::<(VectorIndexRef, VectorIndexLayout)>())
                        .saturating_add(column.partition_key_columns.as_ref().map_or(
                            0,
                            |columns| {
                                columns
                                    .iter()
                                    .map(|name| {
                                        std::mem::size_of::<String>().saturating_add(name.len())
                                    })
                                    .sum::<usize>()
                            },
                        ))
                        .saturating_mul(4)
                        .saturating_add(256)
                })
                .sum::<usize>()
                .saturating_add(
                    affected_tables
                        .iter()
                        .map(|table| table.len().saturating_add(128))
                        .sum::<usize>(),
                ),
        )?;
        let previous_layouts = vector_layouts_from_meta(&pending.base_tables);
        let native_vector_stage = matches!(&self.tx_mgr, DatabaseTransactionHost::Owned(_));
        let recreated_vector_table = |table: &str| {
            staged_changes
                .iter()
                .any(|ddl| matches!(ddl, DdlChange::DropTable { name } if name == table))
        };
        let changed_vector_tables = affected_tables
            .iter()
            .filter(|table| {
                previous_layouts
                    .iter()
                    .filter(|(index, _)| index.table.as_str() == table.as_str())
                    .any(|before| !vector_layouts.contains(before))
                    || vector_layouts
                        .iter()
                        .filter(|(index, _)| index.table.as_str() == table.as_str())
                        .any(|after| !previous_layouts.contains(after))
                    || (native_vector_stage && recreated_vector_table(table))
            })
            .cloned()
            .collect::<HashSet<_>>();
        let native_structural_vectors = native_vector_stage
            && vector_layouts.iter().all(|(index, layout)| {
                recreated_vector_table(&index.table)
                    || previous_layouts
                        .iter()
                        .find(|(old, _)| old == index)
                        .is_none_or(|(_, previous)| {
                            previous.dimension == layout.dimension
                                && previous.quantization == layout.quantization
                                && previous.partition_key_columns == layout.partition_key_columns
                        })
            })
            && changed_vector_tables.iter().all(|table| {
                recreated_vector_table(table)
                    || !(previous_layouts.iter().any(|(index, _)| {
                        index.table == *table
                            && !vector_layouts.iter().any(|(after, _)| after == index)
                    }) && vector_layouts.iter().any(|(index, _)| {
                        index.table == *table
                            && !previous_layouts.iter().any(|(before, _)| before == index)
                    }))
            });
        let vector = if changed_vector_tables.is_empty() {
            None
        } else if native_structural_vectors {
            let original = temporary_memory
                .original
                .as_ref()
                .ok_or_else(EngineMetadataJournal::identity_error)?;
            let declared = crate::memory_accounting::persistence_original_projection(original)?
                .vector_budget()?;
            let preparing = contextdb_vector::VectorCopyAdmission::new(declared.clone());
            Some(
                self.vector_store
                    .prepare_received_schema_declarations_publication(
                        vector_layouts
                            .into_iter()
                            .filter(|(index, _)| changed_vector_tables.contains(&index.table))
                            .collect(),
                        &changed_vector_tables,
                        recreated_vector_table,
                        declared,
                        preparing,
                    )?,
            )
        } else {
            let (vector_entries, vector_image_owner) = self
                .copy_partitioned_vector_membership_image(
                    |index| changed_vector_tables.contains(&index.table),
                    vector_copy_owner.scratch(),
                    read_memory.clone(),
                )?;
            let publication = self
                .vector_store
                .prepare_partitioned_received_schema_publication(
                    vector_layouts
                        .into_iter()
                        .filter(|(index, _)| changed_vector_tables.contains(&index.table))
                        .collect(),
                    vector_entries,
                    contextdb_vector::MemoryBudgetOwner::clone(
                        &self.finalization.vector_accountant,
                    ),
                    vector_image_owner,
                )?;
            Some(
                self.vector_store
                    .prepare_received_schema_tables_publication(
                        publication,
                        &changed_vector_tables,
                        &mut vector_copy_owner,
                    )?,
            )
        };
        let memory_swap = match temporary_memory.original.as_mut() {
            Some(original) => {
                PreparedMemorySwap::from_original_preparation(original, old_bytes, new_bytes, 0)?
            }
            None => PreparedMemorySwap::prepare(
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                old_bytes,
                new_bytes,
                0,
            )?,
        }
        .carrying_staged_admissions(StagedAdmissionRepricing {
            tx_mgr: self.tx_mgr.clone(),
            per_tx: staged_repricing,
        });

        let memory_swap = memory_swap.prepare_schema_generation(self, affected_tables.iter())?;
        let working_table_generations = working.in_memory_table_generations.lock().clone();
        let generation_affected_tables = staged_changes
            .iter()
            .filter_map(Self::ddl_affected_table)
            .collect::<HashSet<_>>();
        let table_generation_updates = generation_affected_tables
            .iter()
            .map(|table| {
                working_table_generations
                    .get(*table)
                    .copied()
                    .ok_or_else(|| {
                        Error::SyncError(
                            (format!("transactional DDL has no projected generation for {table}"))
                                .into(),
                        )
                    })
                    .map(|generation| ((*table).to_string(), generation))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        let mut config_values = generation_affected_tables
            .iter()
            .filter_map(|table| {
                table_generation_updates
                    .get(*table)
                    .map(|generation| (*table, *generation))
            })
            .map(|(table, generation)| {
                Ok((
                    Self::durable_lineage_table_generation_key(table),
                    RedbPersistence::encode_config_value(&generation)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut ddl_generations = Vec::with_capacity(staged_changes.len());
        for (ordinal, change) in staged_changes.iter().enumerate() {
            let table = Self::ddl_affected_table(change).map(str::to_string);
            let generation = table
                .as_deref()
                .map(|table| {
                    table_generation_updates.get(table).copied().ok_or_else(|| {
                        Error::SyncError(
                            (format!("transactional DDL has no projected generation for {table}"))
                                .into(),
                        )
                    })
                })
                .transpose()?;
            let sidecar = DurableDdlGenerationSidecar {
                table: table.clone(),
                table_generation: generation,
            };
            config_values.push((
                Self::durable_ddl_generation_sidecar_key(lsn, ordinal as u32),
                RedbPersistence::encode_config_value(&sidecar)?,
            ));
            ddl_generations.push(((lsn, ordinal as u32), (table, generation)));
        }
        if let Some(event_bus) = &event_bus {
            config_values.extend(event_bus.config_values.iter().cloned());
        }
        let trigger_config = native_trigger
            .as_ref()
            .map_or(&trigger.config_values, |native| {
                &native.publication.config_values
            });
        if native_trigger.is_some() {
            let bytes = crate::custody::preparation::preparation_array_bytes::<(String, Vec<u8>)>(
                config_values.len().saturating_add(trigger_config.len()),
            )?
            .checked_add(
                trigger_config
                    .iter()
                    .map(|(key, value)| key.len().saturating_add(value.len()))
                    .fold(0usize, usize::saturating_add),
            )
            .ok_or_else(EngineMetadataJournal::identity_error)?;
            temporary_memory.absorb(TemporaryMemoryReservation::reserve_for_database(
                self,
                bytes,
                "transactional_trigger_config_copy",
            )?)?;
            config_values.reserve_exact(trigger_config.len());
        }
        config_values.extend(trigger_config.iter().cloned());
        let ddl_log = staged_changes
            .iter()
            .cloned()
            .map(|change| (lsn, change))
            .collect::<Vec<_>>();
        let durable_projection =
            LocalSchemaPersistenceProjection {
                affected_table_meta: projected_tables
                    .iter()
                    .map(|table| {
                        tables
                            .get(*table)
                            .cloned()
                            .map(|meta| ((*table).to_string(), meta))
                            .ok_or_else(|| {
                                Error::SyncError((format!(
                                "transactional schema persistence lost affected table {table}"
                            )).into())
                            })
                    })
                    .collect::<Result<HashMap<_, _>>>()?,
                ddl: staged_changes,
                config_values,
                commit_index_tx: visibility_tx,
            };
        // Move the early declaration fee only after all fallible local builders
        // have finished. Until here it outlives their declaration-order drops.
        if let Some(early_memory) = early_memory {
            temporary_memory.absorb(early_memory)?;
        }
        let memory_history = if self.persistence.is_none() {
            Some(
                self.prepare_memory_history_entries(
                    ddl_log.iter().map(|(lsn, change)| (*lsn, change)),
                    ddl_generations
                        .iter()
                        .map(|(key, (table, generation))| (*key, (table, *generation))),
                )?,
            )
        } else {
            None
        };
        self.local_schema_stages.lock().insert(
            lsn,
            LocalSchemaStage {
                memory_history,
                relational,
                affected_tables,
                dag_edge_types,
                rank_formulas,
                vector,
                event_bus,
                native_event_bus: std::cell::RefCell::new(native_event_bus),
                _queue_mutation_lease: queue_mutation_lease,
                trigger,
                native_trigger,
                table_generations: table_generation_updates.into_iter().collect(),
                ddl_generations,
                ddl_log,
                memory_swap,
                _temporary_memory: temporary_memory,
                durable_projection,
                _vector_copy_owner: vector_copy_owner,
            },
        )?;
        Ok(Some(schema_stage_registry::LocalRegistration::new(
            &self.local_schema_stages,
            lsn,
        )))
    }
}

/// Owns a pending local schema stage while the statement runs on the private
/// image. Dropped while still armed (an unwind from inside the statement), it
/// discards the stage and marks the transaction for ROLLBACK.
struct LocalSchemaStatementGuard<'d> {
    database: &'d Database,
    tx: TxId,
    pending: Option<PendingLocalSchemaStage>,
}

impl LocalSchemaStatementGuard<'_> {
    fn disarm(mut self) -> PendingLocalSchemaStage {
        self.pending.take().expect("guard holds the stage")
    }
}

impl Drop for LocalSchemaStatementGuard<'_> {
    fn drop(&mut self) {
        if let Some(pending) = self.pending.take() {
            self.database.discard_local_schema_stage(self.tx, pending);
        }
    }
}

/// The typed refusal for a statement after the transaction's local schema
/// stage was discarded.
pub(super) fn local_schema_stage_requires_rollback() -> Error {
    Error::Other(
        "this transaction's schema stage was discarded after a failed statement; ROLLBACK is required"
            .into(),
    )
}

/// Unit-test fault points of the local schema stage. Each is armed by a test
/// and fires once.
#[cfg(test)]
pub(super) mod local_schema_test_seams {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum LocalSchemaFaultKind {
        Error,
        Unwind,
    }

    /// Where a fault inside `detached_local_schema_working_database` fires.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum PreparationFault {
        /// After the metadata fold is admitted, before the tables hold it.
        AfterFoldBeforeTables,
        /// After the fold and every registration.
        AfterRegistration,
    }

    static STATEMENT_FAULT: std::sync::Mutex<Option<LocalSchemaFaultKind>> =
        std::sync::Mutex::new(None);
    static PREPARATION_FAULT: std::sync::Mutex<Option<PreparationFault>> =
        std::sync::Mutex::new(None);

    pub(crate) fn arm_statement_fault(kind: LocalSchemaFaultKind) {
        *STATEMENT_FAULT.lock().expect("fault lock") = Some(kind);
    }

    pub(crate) fn arm_preparation_fault(at: PreparationFault) {
        *PREPARATION_FAULT.lock().expect("fault lock") = Some(at);
    }

    pub(super) fn after_execute() -> Result<()> {
        // Taken first: an injected unwind must not poison the fault lock.
        let fault = STATEMENT_FAULT.lock().expect("fault lock").take();
        match fault {
            Some(LocalSchemaFaultKind::Error) => Err(Error::Other(
                "injected local schema statement fault after execution".into(),
            )),
            Some(LocalSchemaFaultKind::Unwind) => {
                panic!("injected local schema statement unwind after execution")
            }
            None => Ok(()),
        }
    }

    pub(in crate::database) fn preparation_fault_at(at: PreparationFault) -> Result<()> {
        let mut armed = PREPARATION_FAULT.lock().expect("fault lock");
        if *armed != Some(at) {
            return Ok(());
        }
        *armed = None;
        Err(Error::Other(
            "injected local schema preparation fault".into(),
        ))
    }
}

/// The IVF members a vector column declares; an undeclared member is
/// chosen by the engine and has no single disclosed value.
#[derive(Clone, Copy)]
struct DeclaredIvfMembers {
    lists: Option<u32>,
    probes: Option<u32>,
    refine_factor_hundredths: Option<u32>,
}

impl DeclaredIvfMembers {
    fn entry(
        self,
        partition: String,
        route: Option<VectorSearchRoute>,
        reason: Option<String>,
        policy_revision: u64,
    ) -> VectorPartitionDisclosure {
        let source = |member: Option<u32>| member.map(|_| "declared".to_owned());
        VectorPartitionDisclosure {
            partition,
            route,
            reason,
            lists: self.lists,
            lists_source: source(self.lists),
            probes: self.probes,
            probes_source: source(self.probes),
            refine_factor_hundredths: self.refine_factor_hundredths,
            refine_factor_source: source(self.refine_factor_hundredths),
            policy_revision,
        }
    }
}

/// The one reason every disclosed partition shares, or the mixed marker.
fn common_partition_reason(partitions: &[VectorPartitionDisclosure]) -> String {
    let mut reasons = partitions.iter().filter_map(|partition| partition.reason.as_deref());
    match reasons.next() {
        Some(first) if reasons.all(|reason| reason == first) => first.to_owned(),
        Some(_) => "mixed_partition_routes".to_owned(),
        None => "none".to_owned(),
    }
}
