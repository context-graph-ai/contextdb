use super::*;

impl Database {
    sync_test_seam! {
    /// Marks this handle as a sync relay hub.
    ///
    /// Relay handles file incoming sync batches from other machines without
    /// requiring local trigger callbacks for trigger-attached tables. The flag
    /// affects only sync-apply trigger callback readiness; trigger execution
    /// remains disabled on apply and all normal data/schema validation still
    /// runs.
    fn enable_sync_relay_mode(&self) {
        self.sync_relay_mode.store(true, Ordering::SeqCst);
    }
    }

    pub(crate) fn sync_relay_mode_enabled(&self) -> bool {
        self.sync_relay_mode.load(Ordering::SeqCst)
    }

    /// Registers a trigger handler that prepares writes inside the firing
    /// transaction. Callback-created records face final commit validation
    /// with the observed row, and a failed commit rolls them all back.
    /// The handler must not perform external actions: a later constraint or
    /// plugin can still reject the transaction. For external delivery, insert
    /// a routed event row here and register its sink separately.
    ///
    /// # Concurrency contract
    ///
    /// While the callback is active on Thread A, other threads writing to
    /// the same `Database` (or an internal handle/`Arc` clone that shares
    /// the same trigger state) wait until the callback's transaction
    /// completes, then proceed as `Ok`. Unrelated cross-DB writers proceed
    /// independently; same-thread reentry returns [`Error::CallbackReentry`];
    /// callback tx-bound handles remain isolated to the runner thread; cron
    /// same-DB contention remains immediate; and a bounded deadlock-guard
    /// timeout returns the typed callback-active error plus one
    /// `tracing::warn!`.
    ///
    /// # Example
    ///
    /// Two writer threads + one parked callback. Both commits land.
    ///
    /// ```rust
    /// use contextdb_core::Value;
    /// use contextdb_engine::Database;
    /// use std::collections::HashMap;
    /// use std::sync::{Arc, Barrier, mpsc};
    /// use std::thread;
    /// use uuid::Uuid;
    ///
    /// let db = Arc::new(Database::open_memory());
    /// db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &HashMap::new()).unwrap();
    /// db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &HashMap::new()).unwrap();
    ///
    /// let entered = Arc::new(Barrier::new(2));
    /// let done = Arc::new(Barrier::new(2));
    /// let entered_cb = entered.clone();
    /// let done_cb = done.clone();
    /// db.register_transactional_trigger_callback("tr", move |_db, _ctx| {
    ///     entered_cb.wait();
    ///     done_cb.wait();
    ///     Ok(())
    /// }).unwrap();
    /// db.complete_initialization().unwrap();
    ///
    /// // Thread A fires the trigger (its callback parks at entered.wait()).
    /// let db_a = db.clone();
    /// let fire_a = thread::spawn(move || {
    ///     let tx = db_a.begin().expect("a begin");
    ///     db_a.execute_in_tx(
    ///         tx,
    ///         "INSERT INTO t (id) VALUES ($id)",
    ///         &HashMap::from([("id".to_string(), Value::Uuid(Uuid::from_u128(1)))]),
    ///     ).expect("a insert");
    ///     db_a.commit(tx).expect("a commit")
    /// });
    /// entered.wait();  // callback is now parked
    ///
    /// // Thread B writes against the same DB while A's callback is parked.
    /// // Same-DB contention waits and proceeds.
    /// let db_b = db.clone();
    /// let (started_tx, started_rx) = mpsc::channel();
    /// let writer_b = thread::spawn(move || {
    ///     started_tx.send(()).unwrap();
    ///     let tx = db_b.begin().expect("b begin must wait then succeed");
    ///     db_b.commit(tx).expect("b commit")
    /// });
    /// started_rx.recv().unwrap();
    /// assert!(!writer_b.is_finished(), "writer B remains fenced behind A");
    ///
    /// // Release A's callback. Both commits land.
    /// done.wait();
    /// fire_a.join().unwrap();
    /// writer_b.join().unwrap();
    /// ```
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn register_transactional_trigger_callback<F>(&self, name: &str, callback: F) -> Result<()>
    where
        F: Fn(&Database, &TriggerContext) -> Result<()> + Send + Sync + 'static,
    {
        let _operation = self.open_operation()?;
        if let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr {
            return handle.with_host(|host| {
                host.events.with_owned_persistence_original(|original| {
                    self.register_original_trigger_callback(host, original, name, callback)
                })
            });
        }
        if !self.trigger.declarations.lock().contains_key(name) {
            return Err(Error::TriggerNotDeclared {
                trigger_name: name.to_string(),
            });
        }
        let mut callbacks = self.trigger.callbacks.write();
        if callbacks.contains_key(name) {
            return Err(Error::TriggerAlreadyRegistered {
                trigger_name: name.to_string(),
            });
        }
        callbacks.insert(name.to_string(), trigger::TriggerCallback::legacy(callback));
        Ok(())
    }

    /// Compatibility spelling for [`Self::register_transactional_trigger_callback`].
    /// This callback runs before durable commit and is never an external
    /// notification hook. Same-database writers wait while it runs; same-thread
    /// reentry is refused and the bounded wait can return a callback-active error.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn register_trigger_callback<F>(&self, name: &str, callback: F) -> Result<()>
    where
        F: Fn(&Database, &TriggerContext) -> Result<()> + Send + Sync + 'static,
    {
        self.register_transactional_trigger_callback(name, callback)
    }

    /// Injects one failure immediately before the file-backed write-set apply.
    /// The pending sink entry has already been prepared, so this checks that a
    /// failed durable commit publishes neither records nor delivery.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn fail_next_local_durable_commit_for_test(&self) {
        assert!(
            self.persistence.is_some(),
            "requires a file-backed database"
        );
        crate::persistent_store::fail_next_local_apply_for_test();
    }

    /// Lists declared triggers.
    ///
    /// See [`Error::CallbackActiveCrossThread`] for the trigger concurrency
    /// contract that applies once callbacks are registered and initialization
    /// is complete.
    pub fn list_triggers(&self) -> Vec<TriggerDeclaration> {
        let _operation = self.assert_open_operation();
        self.persisted_trigger_declarations()
    }

    /// Lists registered host callback names.
    ///
    /// See [`Error::CallbackActiveCrossThread`] for the same-DB wait,
    /// unrelated-DB independence, Class A reentry, cron B1, and deadlock-guard
    /// behavior that applies to active callbacks.
    pub fn registered_trigger_callbacks(&self) -> Vec<String> {
        let _operation = self.assert_open_operation();
        let mut names = self
            .trigger
            .callbacks
            .read()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    pub fn trigger_cascade_depth_cap(&self) -> u32 {
        let _operation = self.assert_open_operation();
        trigger::TRIGGER_CASCADE_DEPTH_CAP
    }

    pub fn trigger_audit_ring_capacity(&self) -> usize {
        let _operation = self.assert_open_operation();
        trigger::TRIGGER_AUDIT_RING_CAPACITY
    }

    /// Returns the in-memory trigger audit ring.
    ///
    /// Deadlock-guard timeouts are operator tracing events, not durable trigger
    /// audit rows; see [`Error::CallbackActiveCrossThread`] for the timeout
    /// contract.
    pub fn trigger_audit_log(&self) -> Vec<TriggerAuditEntry> {
        let _operation = self.assert_open_operation();
        self.original_audit_ring_snapshot()
            .unwrap_or_else(|| self.trigger.audit_ring.lock().iter().cloned().collect())
    }

    /// Returns persisted trigger audit history filtered by trigger name/status.
    ///
    /// Deadlock-guard timeout diagnostics are emitted via `tracing::warn!`
    /// rather than this audit history. See [`Error::CallbackActiveCrossThread`].
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn trigger_audit_history(
        &self,
        filter: TriggerAuditFilter,
    ) -> Result<Vec<TriggerAuditEntry>> {
        let _operation = self.open_operation()?;
        let read_memory = self.declared_reader_memory()?;
        let (history, credit) = if let Some(persistence) = &self.persistence {
            // A read: the admitting declared reader owns every decode before
            // persistence is entered, never the writer's fallback. The
            // volatile history below decodes nothing.
            let decode = crate::read_image_memory::ImageMemory::operation_reader(
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                read_memory.clone(),
            )?;
            let _decode = decode.enter();
            let (history, credit) =
                persistence.load_trigger_audit_history_with_read_memory(Some(read_memory))?;
            (history, credit)
        } else {
            let mut credit = contextdb_core::read_memory::ReadCredit::new(read_memory);
            let history = self.trigger.volatile_audit_history.lock();
            let mut entries = Vec::new();
            for (_, entry) in history.iter() {
                let reason_bytes = match &entry.status {
                    TriggerAuditStatus::RolledBack { reason } => reason.len(),
                    _ => 0,
                };
                credit.try_grow_for(
                    std::mem::size_of::<TriggerAuditEntry>()
                        .saturating_add(entry.trigger_name.len())
                        .saturating_add(reason_bytes)
                        .saturating_add(96),
                    "reader",
                    "trigger_audit_history",
                    "Reduce the selected audit history or raise the declared read memory limit.",
                )?;
                entries.push(entry.clone());
            }
            drop(history);
            self.append_original_audit_history_to_read(&mut entries, &mut credit)?;
            (entries, Some(credit))
        };
        if let Some(credit) = credit {
            self.retain_writer_output(credit);
        }
        Ok(history
            .into_iter()
            .filter(|entry| {
                filter
                    .trigger_name
                    .as_ref()
                    .is_none_or(|name| entry.trigger_name == *name)
                    && filter
                        .status
                        .is_none_or(|status| entry.status.matches_filter(status))
            })
            .collect())
    }

    sync_test_seam! {
    /// Engine-internal apply primitive. Authenticated orchestration is
    /// co-located in this crate so no downstream feature can reopen it.
    ///
    /// It carries no apply role, so it never adopts: a caller that has not
    /// said which leg it is serving must not be handed the pull leg's
    /// hub-adoption, which would let a pushed value displace the value already
    /// held. Every production pull states its role through the sync client's
    /// own entry point.
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    #[cfg_attr(not(feature = "test-seams"), allow(dead_code, reason = "KEEP-SEAM: live only under the test-seams feature"))]
    fn apply_changes(
        &self,
        changes: ChangeSet,
        policies: &ConflictPolicies,
    ) -> Result<ApplyResult> {
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.apply_changes_with_read_memory(changes, policies, &mut vector_copy_owner, read_memory)
    }

    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    #[cfg_attr(not(feature = "test-seams"), allow(dead_code, reason = "KEEP-SEAM: live only under the test-seams feature; used from resident_history_test_seams.rs"))]
    pub(crate) fn apply_changes_with_read_memory(
        &self,
        changes: ChangeSet,
        policies: &ConflictPolicies,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ApplyResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.apply_changes_impl(
            changes,
            policies,
            &HashMap::new(),
            SyncAdoption::Continuing,
            SyncApplyRole::HubPushApply,
            None,
            false,
            None,
            None,
            &[],
            None,
            None,
            false,
            None,
            None,
         vector_copy_owner, read_memory.clone())
    }
    }

    /// Drive the authenticated, dependency-complete received-schema kernel
    /// from an integration test without substituting the raw apply path. The
    /// source-generated provenance is validated by the same wire decoder used
    /// by sync before the private received context is created.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn apply_received_schema_for_test(
        &self,
        changes: ChangeSet,
        ddl_provenance: Vec<crate::protocol::WireDdlProvenance>,
        tenant_id: TenantId,
        source_node_id: &str,
        source_incarnation: Incarnation,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ApplyResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if !changes.rows.is_empty() || !changes.edges.is_empty() || !changes.vectors.is_empty() {
            return Err(Error::SyncError(
                ("received-schema test bridge accepts schema-only units".to_string()).into(),
            ));
        }
        let mut wire = crate::protocol::WireChangeSet::from(changes);
        wire.ddl_provenance = ddl_provenance;
        let received = crate::protocol::received_ddl_context(
            &wire,
            &tenant_id,
            source_node_id,
            source_incarnation,
        )
        .map_err(crate::protocol::received_ddl_error)?
        .ok_or_else(|| {
            Error::SyncError(("received-schema unit contains no DDL".to_string()).into())
        })?;
        let changes = ChangeSet::try_from(wire)
            .map_err(|error| Error::SyncError((error.to_string()).into()))?;
        self.apply_changes_impl(
            changes,
            &Self::declared_sync_policies(true),
            &HashMap::new(),
            SyncAdoption::Continuing,
            SyncApplyRole::HubPushApply,
            None,
            true,
            None,
            None,
            &[],
            Some(&tenant_id),
            Some(&received),
            false,
            None,
            None,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    sync_test_seam! {
    #[cfg(any(test, feature = "test-seams"))]
    /// Apply a synced changeset carrying explicit per-row arrival positions —
    /// each row's ordering position on the node that ACCEPTED it, keyed by the
    /// row's own `.lsn` (see [`Self::resolve_incoming_arrival`]). This is the
    /// entry point the sync transport (`contextdb-server`) calls; every other
    /// caller keeps using [`Self::apply_changes`], which is unaffected (an
    /// empty `arrivals` map is the legacy behavior: each row's own `.lsn` IS
    /// its arrival, exactly as before this concept existed).
    ///
    /// `adoption` distinguishes a continuing pull from the full re-fetch a
    /// client issues right after its cursor's source changed — see
    /// [`SyncAdoption`] for why the two arbitrate differently under
    /// `ConflictPolicy::LatestWins`.
    /// Engine-internal synced apply primitive. The public sync façade has no
    /// way to supply either policy maps or progress state.
    fn apply_synced_changes(
        &self,
        changes: ChangeSet,
        policies: &ConflictPolicies,
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        adoption: SyncAdoption,
    ) -> Result<ApplyResult> {
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.apply_synced_changes_with_read_memory(
            changes,
            policies,
            arrivals,
            adoption,
            &mut vector_copy_owner,
            read_memory,
        )
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn apply_synced_changes_with_read_memory(
        &self,
        changes: ChangeSet,
        policies: &ConflictPolicies,
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        adoption: SyncAdoption,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ApplyResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.apply_changes_impl(
            changes,
            policies,
            arrivals,
            adoption,
            SyncApplyRole::HubPushApply,
            None,
            false,
            None,
            None,
            &[],
            None,
            None,
            false,
            None,
            None,
         vector_copy_owner, read_memory.clone())
    }
    }

    /// The hub applying a pushed unit it cannot receipt — an unidentified peer
    /// carries no node identity to receipt against. It is still a PUSH: the
    /// value already here is the first-accepted one, so KEEP FIRST keeps it and
    /// the pushed value is refused, exactly as a receipted push would be.
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn apply_authenticated_received_changes_with_lineages_as_hub_push(
        &self,
        changes: ChangeSet,
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        adoption: SyncAdoption,
        terminal_refusal_context: Option<&TerminalRefusalPullContext>,
        tenant_id: &TenantId,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        received_ddl: Option<&crate::protocol::ReceivedDdlContext>,
        dependency_complete: bool,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ApplyResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.apply_authenticated_received_changes_with_lineages_impl(
            changes,
            arrivals,
            adoption,
            SyncApplyRole::HubPushApply,
            terminal_refusal_context,
            tenant_id,
            lineages,
            received_ddl,
            dependency_complete,
            false,
            None,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    /// Authenticated wire DDL carries a transport-validated source context.
    /// Keep that context private to the orchestration path: raw/test apply
    /// calls never gain authority to manufacture received schema provenance.
    ///
    /// The pull-leg role of the authenticated apply, exercised directly by
    /// this crate's test harness; production pulls travel the sync client's
    /// ordinary apply path.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn apply_authenticated_received_changes_with_lineages(
        &self,
        changes: ChangeSet,
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        adoption: SyncAdoption,
        terminal_refusal_context: Option<&TerminalRefusalPullContext>,
        tenant_id: &TenantId,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        received_ddl: Option<&crate::protocol::ReceivedDdlContext>,
        dependency_complete: bool,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<ApplyResult> {
        self.apply_authenticated_received_changes_with_lineages_impl(
            changes,
            arrivals,
            adoption,
            SyncApplyRole::PullLeg,
            terminal_refusal_context,
            tenant_id,
            lineages,
            received_ddl,
            dependency_complete,
            false,
            None,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn apply_authenticated_received_changes_with_lineages_while_schema_publication_held(
        &self,
        changes: ChangeSet,
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        adoption: SyncAdoption,
        terminal_refusal_context: Option<&TerminalRefusalPullContext>,
        tenant_id: &TenantId,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        received_ddl: Option<&crate::protocol::ReceivedDdlContext>,
        dependency_complete: bool,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ApplyResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.apply_authenticated_received_changes_with_lineages_impl(
            changes,
            arrivals,
            adoption,
            SyncApplyRole::PullLeg,
            terminal_refusal_context,
            tenant_id,
            lineages,
            received_ddl,
            dependency_complete,
            true,
            None,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn apply_authenticated_received_changes_with_lineages_impl(
        &self,
        changes: ChangeSet,
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        adoption: SyncAdoption,
        apply_role: SyncApplyRole,
        terminal_refusal_context: Option<&TerminalRefusalPullContext>,
        tenant_id: &TenantId,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        received_ddl: Option<&crate::protocol::ReceivedDdlContext>,
        dependency_complete: bool,
        schema_publication_held: bool,
        push_outcome_key: Option<&SyncPushOutcomeContext>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ApplyResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.apply_changes_impl(
            changes,
            &Self::declared_sync_policies(false),
            arrivals,
            adoption,
            apply_role,
            None,
            dependency_complete,
            terminal_refusal_context,
            None,
            lineages,
            Some(tenant_id),
            received_ddl,
            schema_publication_held,
            None,
            push_outcome_key,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    sync_test_seam! {
    #[cfg(any(test, feature = "test-seams"))]
    /// Engine-internal authenticated push apply primitive.
    fn apply_synced_changes_with_receipt(
        &self,
        changes: ChangeSet,
        policies: &ConflictPolicies,
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        adoption: SyncAdoption,
        receipt: SyncApplyReceipt,
    ) -> Result<ApplyResult> {
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.apply_synced_changes_with_receipt_with_read_memory(
            changes,
            policies,
            arrivals,
            adoption,
            receipt,
            &mut vector_copy_owner,
            read_memory,
        )
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn apply_synced_changes_with_receipt_with_read_memory(
        &self,
        changes: ChangeSet,
        policies: &ConflictPolicies,
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        adoption: SyncAdoption,
        receipt: SyncApplyReceipt,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ApplyResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let result =
            self.apply_changes_impl(
                changes,
                policies,
                arrivals,
                adoption,
                SyncApplyRole::HubPushApply,
                Some(receipt.clone()),
                receipt.dependency_complete,
                None,
                None,
                &[],
                None,
                None,
                false,
                None,
                None,
             vector_copy_owner, read_memory.clone())?;
        if self.persistence.is_none() {
            let edge_key = Self::applied_push_watermark_node_incarnation_key(
                &receipt.tenant_id,
                &receipt.node_id,
                receipt.incarnation,
            );
            let mut watermarks = self.in_memory_applied_push_watermarks.lock();
            for key in [edge_key, receipt.tenant_id.as_str().to_string()] {
                watermarks
                    .entry(key)
                    .and_modify(|current| *current = (*current).max(receipt.source_lsn))
                    .or_insert(receipt.source_lsn);
            }
        }
        Ok(result)
    }
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn apply_authenticated_received_changes_with_receipt_and_lineages(
        &self,
        changes: ChangeSet,
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        adoption: SyncAdoption,
        receipt: SyncApplyReceipt,
        hub_local_author: Option<&str>,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        received_ddl: Option<&crate::protocol::ReceivedDdlContext>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<ApplyResult> {
        self.apply_authenticated_received_changes_with_outcome(
            changes,
            arrivals,
            adoption,
            receipt,
            hub_local_author,
            lineages,
            received_ddl,
            None,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn apply_authenticated_received_changes_with_outcome(
        &self,
        changes: ChangeSet,
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        adoption: SyncAdoption,
        receipt: SyncApplyReceipt,
        hub_local_author: Option<&str>,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        received_ddl: Option<&crate::protocol::ReceivedDdlContext>,
        push_outcome_key: Option<&SyncPushOutcomeKey>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ApplyResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let push_lock = push_outcome_key.map(|key| self.sync_push_apply_lock_for_key(key));
        let _push_apply = push_lock.as_ref().map(|lock| lock.lock());
        if let Some(key) = push_outcome_key
            && let Some(result) = self.replay_sync_push_outcome(key)?
        {
            return Ok(result);
        }
        // The last-request slot can belong to a later push. A data-empty
        // schema occurrence already has its own durable manifest result, so
        // redelivery needs no slot rebind or new receiver commit. A later
        // frontier or an explicit contact update still follows the receipt path.
        if changes.rows.is_empty()
            && changes.edges.is_empty()
            && changes.vectors.is_empty()
            && receipt.hub_contact_ms.is_none()
            && received_ddl.is_some()
        {
            let _manifest_lock = self.received_schema_manifest_lock.lock();
            if let ReceivedSchemaManifestClassification::Replay(result) =
                self.classify_received_schema_manifest(received_ddl)?
                && self
                    .persisted_sync_applied_push_watermark_for_node_incarnation_with_read_memory(
                        &receipt.tenant_id,
                        &receipt.node_id,
                        receipt.incarnation,
                        read_memory.clone(),
                    )?
                    .is_some_and(|watermark| watermark >= receipt.source_lsn)
            {
                return Ok(result.into());
            }
        }
        let original_changes = changes.clone();
        let PurgedLineageRefusal {
            changes,
            refused_rows,
            conflicts,
        } = self.classify_purged_lineage_descendants(changes, lineages)?;
        if receipt.dependency_complete
            && !refused_rows.is_empty()
            && !Self::changeset_is_wholly_empty(&changes)
        {
            return Err(Error::SyncError(
                ("dependency-complete push mixes purged-lineage refusal with nonterminal members"
                    .to_string())
                .into(),
            ));
        }
        let outcome = push_outcome_key.map(|key| SyncPushOutcomeContext {
            key: key.clone(),
            committed_lsn: None,
            refused_rows: refused_rows.len(),
            conflicts: conflicts.clone(),
        });
        // A request this node's declared policy refuses whole is answered with
        // its typed refusals: the apply records the distinct schema outcome,
        // while this door must not write an acceptance receipt or progress.
        let mut policy_plan = self.received_policy_plan_for_apply(
            &changes,
            received_ddl,
            Some(&receipt),
            SyncApplyRole::HubPushApply,
            hub_local_author,
            Some(&receipt.node_id),
            read_memory.clone(),
        )?;
        Self::hold_complete_policy_data(&mut policy_plan, &changes, receipt.dependency_complete);
        let wholly_refused_by_policy = Self::policy_plan_covers_all(&policy_plan, &changes);
        let applied = self.apply_changes_impl(
            changes,
            &Self::declared_sync_policies(true),
            arrivals,
            adoption,
            SyncApplyRole::HubPushApply,
            Some(receipt.clone()),
            receipt.dependency_complete,
            None,
            hub_local_author,
            lineages,
            Some(&receipt.tenant_id),
            received_ddl,
            false,
            None,
            outcome.as_ref(),
            vector_copy_owner,
            read_memory.clone(),
        );
        let result = match applied {
            Ok(mut result) => {
                result.skipped_rows += refused_rows.len();
                result.conflicts.extend(conflicts);
                result
            }
            Err(error @ Error::PurgeCausalityFence { .. }) => {
                let PurgedLineageRefusal {
                    changes,
                    refused_rows,
                    conflicts,
                } = self.classify_purged_lineage_descendants(original_changes, lineages)?;
                if refused_rows.is_empty() || !Self::changeset_is_wholly_empty(&changes) {
                    return Err(error);
                }
                let result = ApplyResult {
                    applied_rows: 0,
                    skipped_rows: refused_rows.len(),
                    conflicts,
                    new_lsn: self.current_lsn(),
                };
                let context = push_outcome_key.map(|key| SyncPushOutcomeContext {
                    key: key.clone(),
                    committed_lsn: None,
                    refused_rows: 0,
                    conflicts: Vec::new(),
                });
                self.commit_sync_apply_outcome_only(
                    &receipt,
                    context.as_ref(),
                    &result,
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
                result
            }
            Err(error) => return Err(error),
        };
        if wholly_refused_by_policy {
            return Ok(result);
        }
        if self.persistence.is_none() {
            let edge_key = Self::applied_push_watermark_node_incarnation_key(
                &receipt.tenant_id,
                &receipt.node_id,
                receipt.incarnation,
            );
            let mut watermarks = self.in_memory_applied_push_watermarks.lock();
            for key in [edge_key, receipt.tenant_id.as_str().to_string()] {
                watermarks
                    .entry(key)
                    .and_modify(|current| *current = (*current).max(receipt.source_lsn))
                    .or_insert(receipt.source_lsn);
            }
        }
        if let Some(key) = push_outcome_key {
            return self.replay_sync_push_outcome(key)?.ok_or_else(|| {
                Error::SyncError(
                    ("committed push omitted its durable apply outcome".to_string()).into(),
                )
            });
        }
        Ok(result)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn apply_delivery_prepared(
        &self,
        changes: ChangeSet,
        mut receipt: SyncApplyReceipt,
        hub: &str,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        received_ddl: Option<&crate::protocol::ReceivedDdlContext>,
        prepare: &mut dyn crate::custody::preparation::CommitPreparation,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ApplyResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // Preserve normal authenticated apply metadata, but
        // hold progress until the custody batch has adjudicated every sibling.
        receipt.source_lsn = self
            .persisted_sync_applied_push_watermark_for_node_incarnation_with_read_memory(
                &receipt.tenant_id,
                &receipt.node_id,
                receipt.incarnation,
                read_memory.clone(),
            )?
            .unwrap_or(Lsn(0));
        let result = self.apply_changes_impl(
            changes,
            &Self::declared_sync_policies(true),
            &HashMap::new(),
            SyncAdoption::Continuing,
            SyncApplyRole::HubPushApply,
            Some(receipt.clone()),
            true,
            None,
            Some(hub),
            lineages,
            Some(&receipt.tenant_id),
            received_ddl,
            false,
            Some(prepare),
            None,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        Ok(result)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_authenticated_received_plugin_output(
        expected: &ChangeSet,
        actual: &ChangeSet,
        context: &crate::protocol::ReceivedDdlContext,
    ) -> Result<()> {
        if actual.ddl != expected.ddl || actual.ddl_lsn != expected.ddl_lsn {
            return Err(Error::SyncError(
                ("authenticated received DDL is immutable after transport validation".to_string())
                    .into(),
            ));
        }
        if actual.edges != expected.edges || actual.vectors != expected.vectors {
            return Err(Error::SyncError(
                ("sync plugin may filter authenticated rows only; edges and vectors are immutable"
                    .to_string())
                .into(),
            ));
        }
        let mut source_rows = expected.rows.iter();
        for row in &actual.rows {
            if !source_rows.by_ref().any(|candidate| candidate == row) {
                return Err(Error::SyncError((
                    "sync plugin may filter authenticated rows only; it cannot rewrite, manufacture, re-key, or reorder a row"
                        .to_string()).into()));
            }
        }
        if context.entries.len() != expected.ddl.len() {
            return Err(Error::SyncError((
                "authenticated received DDL context does not cover the exact source schema vector"
                    .to_string()).into()));
        }
        Ok(())
    }

    // Use authenticated arrival provenance to distinguish this
    // node's own declaration from policy it merely adopted and relays.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn locally_declared_custody_policy(
        &self,
        table: &str,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(root) = self.writer_read_root()? {
            return self.durable_locally_declared_custody_policy(&root, table, read_memory.clone());
        }
        let mut ordinals = HashMap::<Lsn, u32>::new();
        let history = self.checked_ddl_entries_since(Lsn(0), read_memory.clone())?;
        for (lsn, change) in &history {
            let ordinal = ordinals.entry(*lsn).or_default();
            let received = self.with_received_ddl_arrival(
                (*lsn, *ordinal),
                |arrival| Ok(arrival.is_some()),
                read_memory.clone(),
            )?;
            *ordinal += 1;
            if received {
                continue;
            }
            let (
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            ) = match change {
                DdlChange::CreateTable {
                    name,
                    columns,
                    constraints,
                    foreign_keys,
                    composite_foreign_keys,
                    composite_unique,
                }
                | DdlChange::AlterTable {
                    name,
                    columns,
                    constraints,
                    foreign_keys,
                    composite_foreign_keys,
                    composite_unique,
                } => (
                    name,
                    columns,
                    constraints,
                    foreign_keys,
                    composite_foreign_keys,
                    composite_unique,
                ),
                _ => continue,
            };
            if name != table {
                continue;
            }
            let m = rough_sync_table_meta(
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            );
            if m.sync_direction.is_some()
                || m.conflict_policy.is_some()
                || m.immutable
                || m.default_ttl_seconds.is_some()
                || m.history_policy.is_some()
                || m.delivery_manifest_tables.is_some()
                || m.edge_discard.is_some()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// A durable writer selects this table's declaration occurrences from the
    /// small per-occurrence generation records (each names its table) and
    /// decodes only those occurrences' log positions. Unrelated declarations
    /// -- other tables, event routes with large filters -- are never read.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn durable_locally_declared_custody_policy(
        &self,
        root: &crate::persistence::WriterSnapshotHandle,
        table: &str,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        const PREFIX: &str = "sync_ddl_generation.v1.";
        let mut sidecars = root.config_cursor(
            crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
            PREFIX,
        )?;
        while let Some(window) = sidecars.next(|_, bytes| {
            RedbPersistence::decode_config_value_exact::<DurableDdlGenerationSidecar>(bytes)
        })? {
            if window.value.table.as_deref() != Some(table) {
                continue;
            }
            let (lsn, ordinal) = std::str::from_utf8(window.position())
                .ok()
                .and_then(|key| key.strip_prefix(PREFIX))
                .and_then(|rest| rest.split_once('.'))
                .and_then(|(lsn, ordinal)| {
                    Some((Lsn(lsn.parse().ok()?), ordinal.parse::<u32>().ok()?))
                })
                .ok_or_else(|| Error::StoreCorrupted {
                    path: "config".into(),
                    reason: "declaration generation key does not name one occurrence".into(),
                })?;
            if self.with_received_ddl_arrival(
                (lsn, ordinal),
                |arrival| Ok(arrival.is_some()),
                read_memory.clone(),
            )? {
                continue;
            }
            let mut occurrences = root.ddl_history_cursor(
                crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                Lsn(lsn.0.saturating_sub(1)),
            )?;
            let mut position = 0u32;
            let mut local = None;
            while let Some(entry) = occurrences.next_ddl_history()? {
                if entry.value.0 != lsn {
                    break;
                }
                if position == ordinal {
                    local = Some(entry);
                    break;
                }
                position += 1;
            }
            let Some(local) = local else {
                continue;
            };
            let (DdlChange::CreateTable {
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            }
            | DdlChange::AlterTable {
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            }) = &local.value.1
            else {
                continue;
            };
            if name != table {
                continue;
            }
            let m = rough_sync_table_meta(
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            );
            if m.sync_direction.is_some()
                || m.conflict_policy.is_some()
                || m.immutable
                || m.default_ttl_seconds.is_some()
                || m.history_policy.is_some()
                || m.delivery_manifest_tables.is_some()
                || m.edge_discard.is_some()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// A conflicting CREATE cannot authorize data against a different local
    /// shape. Judge it before source units split, so the existing durable
    /// hold also covers dependent rows on later pages and after restart.
    fn arriving_create_table_shape_refusal(
        &self,
        name: &str,
        columns: &[(String, String)],
        constraints: &[String],
        foreign_keys: &[SingleColumnForeignKey],
        composite_foreign_keys: &[CompositeForeignKey],
        composite_unique: &[Vec<String>],
    ) -> Option<Error> {
        let local = self.table_meta(name)?;
        if !sync_table_shape_matches(
            &local,
            columns,
            constraints,
            foreign_keys,
            composite_foreign_keys,
            composite_unique,
        ) {
            return Some(Error::SchemaInvalid {
                reason: format!("schema mismatch for table {name}: structured constraints differ"),
            });
        }
        if !sync_table_columns_match(&local, columns) {
            let (local_columns, remote_columns) = sync_table_column_lists(&local, columns);
            return Some(Error::SchemaInvalid {
                reason: format!(
                    "schema mismatch for table {name}: local columns {local_columns:?} differ from remote {remote_columns:?}"
                ),
            });
        }
        None
    }

    /// Whether this node's declared or bound policy refuses one arriving
    /// table statement, and with what typed refusal. The caller refuses the
    /// statement's WHOLE authored transaction on a refusal: an authenticated
    /// transaction is never installed in part.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn arriving_statement_refusal(
        &self,
        change: &DdlChange,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<Error>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let (name, columns, constraints, foreign_keys, composite_foreign_keys, composite_unique) =
            match change {
                DdlChange::CreateTable {
                    name,
                    columns,
                    constraints,
                    foreign_keys,
                    composite_foreign_keys,
                    composite_unique,
                }
                | DdlChange::AlterTable {
                    name,
                    columns,
                    constraints,
                    foreign_keys,
                    composite_foreign_keys,
                    composite_unique,
                } => (
                    name,
                    columns,
                    constraints,
                    foreign_keys,
                    composite_foreign_keys,
                    composite_unique,
                ),
                _ => return Ok(None),
            };
        if matches!(change, DdlChange::CreateTable { .. })
            && let Some(refusal) = self.arriving_create_table_shape_refusal(
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            )
        {
            return Ok(Some(refusal));
        }
        let incoming = rough_sync_table_meta(
            columns,
            constraints,
            foreign_keys,
            composite_foreign_keys,
            composite_unique,
        );
        if let Err(error) =
            crate::custody::policy::check_binding(self, name, &incoming, read_memory.clone())
                .and_then(|()| {
                    crate::custody::policy::check_declared(
                        self,
                        name,
                        &incoming,
                        read_memory.clone(),
                    )
                })
        {
            return Ok(Some(error));
        }
        // Preserve installer-owned DDL guards and silent-clause merge after
        // binding/declaration admission, using the existing owners.
        let installer_owned = crate::executor::ENGINE_OWNED_LEDGER_TABLES.contains(&name.as_str())
            || crate::executor::is_hub_refereed_sync_conflict_table(name);
        // Compare the live policies first: only a statement that restates a
        // clause differently needs to know whether this node declared it,
        // and that question reads the declaration history.
        if !installer_owned && let Some(local) = self.table_meta(name) {
            let local_policy = crate::custody::policy::from_meta(&local);
            let arriving_policy = crate::custody::policy::from_meta(&incoming);
            if let Some(clause) =
                crate::custody::policy::differing_clause(&local_policy, &arriving_policy)
                && self.locally_declared_custody_policy(name, read_memory.clone())?
            {
                // The existing invalid-policy preflights must still refuse an
                // impossible promise before this transaction is refused.
                let projected = HashMap::from([(name.clone(), local)]);
                refuse_undeliverable_promise_sync_ddl(&projected, change)?;
                refuse_keyless_sync_safe_sync_ddl(&projected, change)?;
                refuse_reclaimed_history_sync_ddl(change)?;
                return Ok(Some(crate::custody::policy::declared_policy_refusal(
                    name,
                    clause,
                    &local_policy,
                    &arriving_policy,
                )));
            }
        }
        Ok(None)
    }

    /// Which authored transactions of a received batch this node's declared
    /// policy refuses, and what the per-table holdback holds behind them.
    ///
    /// The unit of refusal is the authored transaction: every statement its
    /// author signed under one source position (or, with no signature, the
    /// statements sharing one source LSN). A transaction any statement of
    /// which restates a declared or bound table differently is refused WHOLE
    /// -- none of its statements install, so the receiver never serves a
    /// proper subset of an authenticated migration.
    /// An identical restatement is an ordinary statement and refuses nothing.
    ///
    /// Everything else is judged on its own, with one rule: data, or a later
    /// transaction, that depends on a table a refused transaction named is
    /// held behind that refusal -- it cannot install against a schema its
    /// author never had accepted here, and it cannot fail the rest of the
    /// batch. The hold is durable (`DurablePolicyHold`), so it survives the
    /// call, later pages and a restart:
    /// - a table the refused transaction would have CREATED and that does not
    ///   exist here holds every later delivery for it, whoever sends it, until
    ///   a transaction that creates it installs;
    /// - a table that EXISTS here holds the later deliveries from the peer
    ///   that sent the refused transaction (`source`), until a transaction
    ///   from that peer restates the table in a form this node accepts.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn received_policy_plan(
        &self,
        changes: &ChangeSet,
        received: Option<&crate::protocol::ReceivedDdlContext>,
        source: Option<&str>,
        identity: Option<(&TenantId, Incarnation)>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ReceivedPolicyPlan> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.received_policy_plan_with_refusals(
            changes,
            received,
            source,
            identity,
            &HashMap::new(),
            read_memory.clone(),
        )
    }

    /// Apply attempts also hold schema units refused while reading policy
    /// state, before the staged door is reached. Keep policy previews pure
    /// and preserve non-memory refusals and successful planning unchanged.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn received_policy_plan_for_apply(
        &self,
        changes: &ChangeSet,
        received: Option<&crate::protocol::ReceivedDdlContext>,
        receipt: Option<&SyncApplyReceipt>,
        role: SyncApplyRole,
        hub: Option<&str>,
        source: Option<&str>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ReceivedPolicyPlan> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let planned = if role == SyncApplyRole::HubPushApply
            && let (Some(receipt), Some(hub)) = (receipt, hub)
        {
            self.received_policy_plan_for_push(changes, received, receipt, hub, read_memory.clone())
        } else {
            self.received_policy_plan(
                changes,
                received,
                source,
                receipt.map(|receipt| (&receipt.tenant_id, receipt.incarnation)),
                read_memory.clone(),
            )
        };
        if let Err(refusal @ Error::MemoryBudgetExceeded { .. }) = &planned
            && self.access_is_admin()
            && !changes.ddl.is_empty()
        {
            let holder = Self::received_unit_hold_identity(&changes.ddl_lsn, received);
            // An earlier unit remains the reason a later one cannot apply,
            // even when the policy lookup itself no longer fits the limit.
            self.refuse_received_unit_behind_a_memory_holdback(&changes.ddl, &holder)?;
            // Lookup reservations have already dropped. As at the staged
            // door, a failed hold write must not replace the original refusal.
            let _ = self.hold_back_received_unit_for_memory(&changes.ddl, &holder, refusal);
        }
        planned
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn received_policy_plan_for_push(
        &self,
        changes: &ChangeSet,
        received: Option<&crate::protocol::ReceivedDdlContext>,
        receipt: &SyncApplyReceipt,
        hub: &str,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ReceivedPolicyPlan> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut refusals = HashMap::new();
        if let Some(received) = received {
            if receipt.tenant_id != received.tenant_id
                || receipt.node_id != received.source_node_id
                || receipt.incarnation != received.source_incarnation
            {
                return Err(Error::SyncError(
                    "schema refusal identity differs from authenticated push".into(),
                ));
            }
            let mut units = BTreeMap::new();
            for author in received
                .entries
                .iter()
                .filter_map(|entry| entry.author.as_ref())
            {
                units
                    .entry((
                        author.node_id.clone(),
                        author.incarnation,
                        author.source_ddl_lsn,
                    ))
                    .or_insert_with(Vec::new)
                    .push(author);
            }
            let prefix = Self::refused_schema_scope(
                &receipt.tenant_id,
                &receipt.node_id,
                receipt.incarnation,
                hub,
                self.sync_incarnation_with_read_memory(&receipt.tenant_id, read_memory.clone())?,
            );
            for authors in units.values() {
                let digest = crate::protocol::authored_schema_unit_digest(authors)
                    .map_err(|error| Error::SyncError((error.to_string()).into()))?;
                let key = format!("{}{}", prefix, blake3::Hash::from(digest).to_hex());
                let outcome = if let Some(persistence) = &self.persistence {
                    persistence
                        .load_config_value::<crate::protocol::RefusedSchemaUnitOutcome>(&key)?
                } else {
                    self.in_memory_sync_progress
                        .lock()
                        .refused_schema_units
                        .get(&key)
                        .cloned()
                };
                if let Some(outcome) = outcome {
                    refusals.insert(digest, outcome);
                }
            }
        }
        self.received_policy_plan_with_refusals(
            changes,
            received,
            Some(&receipt.node_id),
            Some((&receipt.tenant_id, receipt.incarnation)),
            &refusals,
            read_memory.clone(),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn received_policy_plan_with_refusals(
        &self,
        changes: &ChangeSet,
        received: Option<&crate::protocol::ReceivedDdlContext>,
        source: Option<&str>,
        identity: Option<(&TenantId, Incarnation)>,
        refusals: &HashMap<[u8; 32], crate::protocol::RefusedSchemaUnitOutcome>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ReceivedPolicyPlan> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.received_policy_plan_with_projection(
            changes,
            received,
            source,
            identity,
            refusals,
            None,
            read_memory.clone(),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn received_policy_plan_with_projection(
        &self,
        changes: &ChangeSet,
        received: Option<&crate::protocol::ReceivedDdlContext>,
        source: Option<&str>,
        identity: Option<(&TenantId, Incarnation)>,
        refusals: &HashMap<[u8; 32], crate::protocol::RefusedSchemaUnitOutcome>,
        projection: Option<&PullPolicyProjection>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ReceivedPolicyPlan> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let identity = received
            .map(|context| (&context.tenant_id, context.source_incarnation))
            .or(identity);
        type UnitKey = (Option<(String, Incarnation)>, Lsn);
        let aligned = received.filter(|received| received.entries.len() == changes.ddl.len());
        let unit_key = |index: usize| -> UnitKey {
            match aligned.map(|received| (received, &received.entries[index])) {
                Some((_, entry)) if entry.author.is_some() => {
                    let author = entry.author.as_ref().expect("checked above");
                    (
                        Some((author.node_id.clone(), author.incarnation)),
                        author.source_ddl_lsn,
                    )
                }
                Some((received, entry)) => (
                    Some((received.source_node_id.clone(), received.source_incarnation)),
                    entry.source_ddl_lsn,
                ),
                None => (None, changes.ddl_lsn.get(index).copied().unwrap_or(Lsn(0))),
            }
        };
        let mut units: Vec<(UnitKey, Vec<usize>)> = Vec::new();
        let mut unit_of = HashMap::<UnitKey, usize>::new();
        for index in 0..changes.ddl.len() {
            let key = unit_key(index);
            let slot = *unit_of.entry(key.clone()).or_insert_with(|| {
                units.push((key, Vec::new()));
                units.len() - 1
            });
            units[slot].1.push(index);
        }

        let (mut holds, _hold_memory) = self.policy_holds_for_changes(changes, source, identity)?;
        if let Some(projection) = projection {
            holds.retain(|hold| !projection.released.contains(&hold.table));
            holds.extend(projection.holds.iter().cloned());
        }
        let edge_tables = self.policy_edge_tables(changes, &holds);
        let data_tables = Self::policy_data_tables(changes, &edge_tables);
        let mut plan = ReceivedPolicyPlan {
            keep_ddl: vec![true; changes.ddl.len()],
            ..ReceivedPolicyPlan::default()
        };
        // Table -> the refusal every later unit on it is held behind, for
        // this batch.
        let mut held = BTreeMap::<String, String>::new();
        let mut kept_creates = projection.map(|p| p.created.clone()).unwrap_or_default();
        for (key, members) in &units {
            let creates = members
                .iter()
                .filter_map(|index| match &changes.ddl[*index] {
                    DdlChange::CreateTable { name, .. } => Some(name.clone()),
                    _ => None,
                })
                .collect::<BTreeSet<_>>();
            let restates = members
                .iter()
                .filter_map(|index| match &changes.ddl[*index] {
                    DdlChange::CreateTable { name, .. } | DdlChange::AlterTable { name, .. } => {
                        Some(name.clone())
                    }
                    _ => None,
                })
                .collect::<BTreeSet<_>>();
            let recorded = if refusals.is_empty() {
                None
            } else {
                let authors = aligned.and_then(|context| {
                    members
                        .iter()
                        .map(|index| context.entries[*index].author.as_ref())
                        .collect::<Option<Vec<_>>>()
                });
                match authors {
                    Some(authors) => refusals.get(
                        &crate::protocol::authored_schema_unit_digest(&authors)
                            .map_err(|error| Error::SyncError((error.to_string()).into()))?,
                    ),
                    None => None,
                }
            };
            let mut refusal: Option<(String, Error)> = recorded.map(|outcome| {
                (
                    outcome.table.clone(),
                    Error::Other((outcome.reason.clone()).into()),
                )
            });
            for index in members {
                if refusal.is_some() {
                    break;
                }
                if let Some(error) =
                    self.arriving_statement_refusal(&changes.ddl[*index], read_memory.clone())?
                {
                    let table = Self::ddl_affected_table(&changes.ddl[*index])
                        .unwrap_or_default()
                        .to_string();
                    refusal = Some((table, error));
                    break;
                }
            }
            if refusal.is_none() {
                for index in members {
                    let Some(table) = Self::ddl_affected_table(&changes.ddl[*index]) else {
                        continue;
                    };
                    let exists = kept_creates.contains(table) || self.table_meta(table).is_some();
                    // A transaction that creates an absent table, or restates
                    // an existing one in a form accepted above, is the author's
                    // correction: it is not held, and installing it releases
                    // the hold.
                    if (exists && restates.contains(table)) || (!exists && creates.contains(table))
                    {
                        continue;
                    }
                    let behind = held.get(table).cloned().or_else(|| {
                        Self::policy_hold_for(&holds, source, identity, table, exists)
                            .map(|hold| hold.refusal.clone())
                    });
                    if let Some(root) = behind {
                        refusal = Some((
                            table.to_string(),
                            Error::TableHeldBehindRefusedUnit {
                                table: table.to_string(),
                                refusal: root,
                            },
                        ));
                        break;
                    }
                }
            }
            let Some((table, error)) = refusal else {
                for table in &restates {
                    held.remove(table);
                    plan.released.insert(table.clone());
                }
                kept_creates.extend(creates);
                continue;
            };
            let text = recorded.map_or_else(|| error.to_string(), |outcome| outcome.reason.clone());
            if !matches!(error, Error::TableHeldBehindRefusedUnit { .. }) {
                plan.refused_units
                    .push((members.clone(), table.clone(), text.clone()));
            }
            let root = match &error {
                Error::TableHeldBehindRefusedUnit { refusal, .. } => refusal.clone(),
                _ => text.clone(),
            };
            let author = key.0.as_ref().map(|(node, _)| node.clone());
            for index in members {
                plan.keep_ddl[*index] = false;
                // Connected data uses the delivery's member LSN, which may
                // differ from the original author's position after forwarding.
                // An edge need not declare a table to belong to this refused unit.
                if let Some(lsn) = changes.ddl_lsn.get(*index)
                    && data_tables.contains_key(lsn)
                {
                    plan.held_data_lsns.insert(*lsn);
                    plan.held_data_tables.entry(*lsn).or_default().extend(
                        members.iter().filter_map(|member| {
                            Self::ddl_affected_table(&changes.ddl[*member]).map(str::to_string)
                        }),
                    );
                }
                let Some(affected) = Self::ddl_affected_table(&changes.ddl[*index]) else {
                    continue;
                };
                held.entry(affected.to_string())
                    .or_insert_with(|| root.clone());
                let exists = kept_creates.contains(affected) || self.table_meta(affected).is_some();
                let table_absent = !exists && creates.contains(affected);
                if !exists && !table_absent {
                    continue;
                }
                // An existing table's hold is about one peer's stream; an
                // unattested delivery names no peer to hold.
                let Some(source) = source.or(table_absent.then_some("")) else {
                    continue;
                };
                if !plan
                    .new_holds
                    .iter()
                    .any(|hold| hold.source_node_id == source && hold.table == affected)
                {
                    plan.new_holds.push(DurablePolicyHold {
                        source_node_id: source.to_string(),
                        tenant_id: received.map(|received| received.tenant_id.clone()),
                        source_incarnation: received.map(|received| received.source_incarnation),
                        author_node_id: author.clone(),
                        source_lsn: key.1,
                        table: affected.to_string(),
                        refusal: root.clone(),
                        table_absent,
                        dag_edge_types: self.policy_table_edge_types(
                            affected,
                            &changes.ddl,
                            &holds,
                        ),
                    });
                }
            }
            plan.conflicts
                .push(Self::table_refusal_conflict(&table, text));
        }

        // Data groups: one source commit's rows, vectors and edges travel and
        // are held together.
        for (lsn, tables) in data_tables {
            let mut behind = None;
            for table in &tables {
                let table = table.as_str();
                if let Some(root) = held.get(table) {
                    behind = Some((table, root.clone()));
                    break;
                }
                if plan.released.contains(table) {
                    continue;
                }
                let exists = kept_creates.contains(table) || self.table_meta(table).is_some();
                if let Some(hold) = Self::policy_hold_for(&holds, source, identity, table, exists) {
                    behind = Some((table, hold.refusal.clone()));
                    break;
                }
            }
            if let Some((table, root)) = behind {
                plan.held_data_lsns.insert(lsn);
                plan.held_data_tables
                    .entry(lsn)
                    .or_default()
                    .extend(tables.iter().cloned());
                plan.conflicts.push(Self::table_refusal_conflict(
                    table,
                    Error::TableHeldBehindRefusedUnit {
                        table: table.to_string(),
                        refusal: root,
                    }
                    .to_string(),
                ));
            }
        }
        Ok(plan)
    }

    /// Edges depend on tables that explicitly declare their DAG type. Keep
    /// refused declarations in the projection: their absence from the installed
    /// catalog is precisely why later deliveries still need a hold.
    pub(super) fn policy_edge_tables(
        &self,
        changes: &ChangeSet,
        holds: &[DurablePolicyHold],
    ) -> BTreeMap<EdgeType, BTreeSet<String>> {
        let mut owners = BTreeMap::<EdgeType, BTreeSet<String>>::new();
        if changes.edges.is_empty() {
            return owners;
        }
        let incoming = changes
            .edges
            .iter()
            .map(|edge| &edge.edge_type)
            .collect::<HashSet<_>>();
        {
            let metadata = self.relational_store.table_meta.read();
            for (table, meta) in metadata.iter() {
                for edge_type in meta
                    .dag_edge_types
                    .iter()
                    .filter(|edge_type| incoming.contains(edge_type))
                {
                    owners
                        .entry(edge_type.clone())
                        .or_default()
                        .insert(table.clone());
                }
            }
        }
        for hold in holds {
            for edge_type in hold
                .dag_edge_types
                .iter()
                .filter(|edge_type| incoming.contains(edge_type))
            {
                owners
                    .entry(edge_type.clone())
                    .or_default()
                    .insert(hold.table.clone());
            }
        }
        for change in &changes.ddl {
            if let DdlChange::CreateTable {
                name, constraints, ..
            }
            | DdlChange::AlterTable {
                name, constraints, ..
            } = change
            {
                for edge_type in ddl_dag_edge_types(constraints)
                    .into_iter()
                    .filter(|edge_type| incoming.contains(edge_type))
                {
                    owners.entry(edge_type).or_default().insert(name.clone());
                }
            }
        }
        owners
    }

    /// Retain only the refused table's declarations for later edge-only pages,
    /// including when the refused schema delivery carries no edges itself.
    pub(super) fn policy_table_edge_types(
        &self,
        table: &str,
        ddl: &[DdlChange],
        holds: &[DurablePolicyHold],
    ) -> Vec<EdgeType> {
        let mut types = BTreeSet::new();
        if let Some(meta) = self.relational_store.table_meta.read().get(table) {
            types.extend(meta.dag_edge_types.iter().cloned());
        }
        for hold in holds.iter().filter(|hold| hold.table == table) {
            types.extend(hold.dag_edge_types.iter().cloned());
        }
        for change in ddl {
            if let DdlChange::CreateTable {
                name, constraints, ..
            }
            | DdlChange::AlterTable {
                name, constraints, ..
            } = change
                && name == table
            {
                types.extend(ddl_dag_edge_types(constraints));
            }
        }
        types.into_iter().collect()
    }

    pub(super) fn policy_data_tables(
        changes: &ChangeSet,
        edge_tables: &BTreeMap<EdgeType, BTreeSet<String>>,
    ) -> BTreeMap<Lsn, BTreeSet<String>> {
        let mut data_tables = BTreeMap::<Lsn, BTreeSet<String>>::new();
        for row in &changes.rows {
            data_tables
                .entry(row.lsn)
                .or_default()
                .insert(row.table.clone());
        }
        for vector in &changes.vectors {
            data_tables
                .entry(vector.lsn)
                .or_default()
                .insert(vector.index.table.clone());
        }
        for edge in &changes.edges {
            let tables = data_tables.entry(edge.lsn).or_default();
            if let Some(owners) = edge_tables.get(&edge.edge_type) {
                tables.extend(owners.iter().cloned());
            }
        }
        data_tables
    }

    /// The durable hold a delivery for `table` from `source` waits behind,
    /// if any: a refused creation while the table is still absent here, or a
    /// refused change of an existing table from that same peer.
    pub(super) fn policy_hold_for<'a>(
        holds: &'a [DurablePolicyHold],
        source: Option<&str>,
        identity: Option<(&TenantId, Incarnation)>,
        table: &str,
        exists: bool,
    ) -> Option<&'a DurablePolicyHold> {
        holds.iter().find(|hold| {
            hold.table == table
                && hold
                    .tenant_id
                    .as_ref()
                    .is_none_or(|tenant| identity.is_some_and(|(id, _)| id == tenant))
                && if exists {
                    !hold.table_absent
                        && Some(hold.source_node_id.as_str()) == source
                        && hold.source_incarnation.is_none_or(|life| {
                            identity.is_some_and(|(_, incarnation)| incarnation == life)
                        })
                } else {
                    hold.table_absent
                }
        })
    }

    /// The peer a delivery arrived from, as the policy holds key it: the
    /// authenticated schema context names it, and a receipted push names its
    /// edge.
    pub(super) fn received_delivery_source<'a>(
        received: Option<&'a crate::protocol::ReceivedDdlContext>,
        receipt: Option<&'a SyncApplyReceipt>,
    ) -> Option<&'a str> {
        received
            .map(|received| received.source_node_id.as_str())
            .or(receipt.map(|receipt| receipt.node_id.as_str()))
    }

    pub(super) fn hold_complete_policy_data(
        plan: &mut ReceivedPolicyPlan,
        changes: &ChangeSet,
        dependency_complete: bool,
    ) {
        if dependency_complete && !plan.held_data_lsns.is_empty() {
            plan.held_data_lsns
                .extend(changes.rows.iter().map(|row| row.lsn));
            plan.held_data_lsns
                .extend(changes.vectors.iter().map(|row| row.lsn));
            plan.held_data_lsns
                .extend(changes.edges.iter().map(|row| row.lsn));
        }
    }

    pub(super) fn policy_plan_covers_all(plan: &ReceivedPolicyPlan, changes: &ChangeSet) -> bool {
        !plan.conflicts.is_empty()
            && !Self::changeset_is_wholly_empty(changes)
            && plan.keep_ddl.iter().all(|keep| !keep)
            && changes
                .rows
                .iter()
                .all(|row| plan.held_data_lsns.contains(&row.lsn))
            && changes
                .vectors
                .iter()
                .all(|vector| plan.held_data_lsns.contains(&vector.lsn))
            && changes
                .edges
                .iter()
                .all(|edge| plan.held_data_lsns.contains(&edge.lsn))
    }

    /// Judge a pull unit against earlier units on the same page before schema
    /// generations or row provenance are projected. Reuse the apply planner,
    /// but retain its holds only in this page's temporary state.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn preflight_pull_unit_policy(
        &self,
        changes: ChangeSet,
        received: Option<&crate::protocol::ReceivedDdlContext>,
        source: &str,
        identity: Option<(&TenantId, Incarnation)>,
        dependency_complete: bool,
        projection: &mut PullPolicyProjection,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(
        ChangeSet,
        Option<crate::protocol::ReceivedDdlContext>,
        PullPolicyPreflight,
    )> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut plan = self.received_policy_plan_with_projection(
            &changes,
            received,
            Some(source),
            identity,
            &HashMap::new(),
            Some(projection),
            read_memory.clone(),
        )?;
        Self::hold_complete_policy_data(&mut plan, &changes, dependency_complete);
        for table in &plan.released {
            projection.holds.retain(|hold| &hold.table != table);
            projection.released.insert(table.clone());
        }
        for hold in plan.new_holds {
            if !plan.released.contains(&hold.table) {
                projection.holds.push(hold);
            }
        }
        for (ddl, keep) in changes.ddl.iter().zip(&plan.keep_ddl) {
            if *keep {
                match ddl {
                    DdlChange::CreateTable { name, .. } => {
                        projection.created.insert(name.clone());
                    }
                    DdlChange::DropTable { name } => {
                        projection.created.remove(name);
                    }
                    _ => {}
                }
            }
        }
        let context = received.map(|context| {
            let mut context = context.clone();
            context.entries = context
                .entries
                .into_iter()
                .zip(&plan.keep_ddl)
                .filter_map(|(entry, keep)| keep.then_some(entry))
                .collect();
            context
        });
        let conflicts = if changes.ddl.is_empty() {
            plan.conflicts
        } else {
            Vec::new()
        };
        let original_ddl = changes.ddl.clone();
        let original_ddl_lsn = changes.ddl_lsn.clone();
        let mut allowed = changes;
        allowed.ddl = allowed
            .ddl
            .into_iter()
            .zip(&plan.keep_ddl)
            .filter_map(|(ddl, keep)| keep.then_some(ddl))
            .collect();
        allowed.ddl_lsn = allowed
            .ddl_lsn
            .into_iter()
            .zip(&plan.keep_ddl)
            .filter_map(|(lsn, keep)| keep.then_some(lsn))
            .collect();
        let mut held_data = ChangeSet::default();
        (allowed.rows, held_data.rows) = allowed
            .rows
            .into_iter()
            .partition(|row| !plan.held_data_lsns.contains(&row.lsn));
        (allowed.vectors, held_data.vectors) = allowed
            .vectors
            .into_iter()
            .partition(|vector| !plan.held_data_lsns.contains(&vector.lsn));
        (allowed.edges, held_data.edges) = allowed
            .edges
            .into_iter()
            .partition(|edge| !plan.held_data_lsns.contains(&edge.lsn));
        Ok((
            allowed,
            context,
            PullPolicyPreflight {
                original_ddl,
                original_ddl_lsn,
                held_data,
                conflicts,
            },
        ))
    }

    /// Take out of a delivery from `source` the data groups held behind a
    /// refused transaction (see `received_policy_plan`), with the typed hold
    /// for each, before the door checks row provenance: a held row names a
    /// table this node either does not have or holds at a schema its author
    /// never had accepted here, so it is neither checked nor installed. A
    /// transaction in the same delivery that creates or restates the table is
    /// left to the apply's plan to judge, with its data.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn split_data_held_behind_refused_units(
        &self,
        mut changes: ChangeSet,
        tenant: &TenantId,
        source: &str,
        incarnation: Option<Incarnation>,
        dependency_complete: bool,
    ) -> Result<(ChangeSet, Vec<Conflict>)> {
        let (holds, _hold_memory) = self.policy_holds_for_changes(
            &changes,
            Some(source),
            incarnation.map(|life| (tenant, life)),
        )?;
        if holds.is_empty() {
            return Ok((changes, Vec::new()));
        }
        let restated = changes
            .ddl
            .iter()
            .filter_map(|change| match change {
                DdlChange::CreateTable { name, .. } | DdlChange::AlterTable { name, .. } => {
                    Some(name.clone())
                }
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        let edge_tables = self.policy_edge_tables(&changes, &holds);
        let data_tables = Self::policy_data_tables(&changes, &edge_tables);
        let mut held_lsns = BTreeSet::new();
        let mut conflicts = Vec::new();
        for (lsn, tables) in data_tables {
            for table in tables {
                let table = table.as_str();
                if restated.contains(table) {
                    continue;
                }
                let exists = self.table_meta(table).is_some();
                if let Some(hold) = Self::policy_hold_for(
                    &holds,
                    Some(source),
                    incarnation.map(|life| (tenant, life)),
                    table,
                    exists,
                ) {
                    held_lsns.insert(lsn);
                    conflicts.push(Self::table_refusal_conflict(
                        table,
                        Error::TableHeldBehindRefusedUnit {
                            table: table.to_string(),
                            refusal: hold.refusal.clone(),
                        }
                        .to_string(),
                    ));
                    break;
                }
            }
        }
        if held_lsns.is_empty() {
            return Ok((changes, conflicts));
        }
        if dependency_complete {
            held_lsns.extend(changes.rows.iter().map(|row| row.lsn));
            held_lsns.extend(changes.vectors.iter().map(|row| row.lsn));
            held_lsns.extend(changes.edges.iter().map(|row| row.lsn));
        }
        changes.rows.retain(|row| !held_lsns.contains(&row.lsn));
        changes
            .vectors
            .retain(|vector| !held_lsns.contains(&vector.lsn));
        changes.edges.retain(|edge| !held_lsns.contains(&edge.lsn));
        Ok((changes, conflicts))
    }

    pub(super) fn table_refusal_conflict(table: &str, reason: String) -> Conflict {
        Conflict {
            natural_key: NaturalKey::single("table".into(), Value::Text(table.to_string())),
            resolution: ConflictPolicy::ServerWins,
            reason: Some(reason),
            table: None,
            mutation_kind: None,
            winning_author_node_id: None,
            hub_acceptance_position: None,
            refusal_cause: None,
        }
    }

    pub(super) const REFUSED_SCHEMA_PREFIX: &'static str = "sync_refused_schema_unit.v1.";
    pub(super) const REFUSED_SCHEMA_LOOKUP_CHECKPOINT_PREFIX: &'static str =
        "sync_refused_schema_lookup_checkpoint.v1.";
    pub(super) const REFUSED_SCHEMA_LOCAL_FACT_PREFIX: &'static str =
        "sync_refused_schema_local_fact.v1.";
    pub(super) const POLICY_HELD_DATA_PREFIX: &'static str = "sync_policy_held_data.v1.";

    pub(super) fn refused_schema_scope(
        tenant: &TenantId,
        node: &str,
        incarnation: Incarnation,
        hub: &str,
        hub_incarnation: Incarnation,
    ) -> String {
        format!(
            "{}{}.{}.{}.{}.{}.",
            Self::REFUSED_SCHEMA_PREFIX,
            Self::hex_component(tenant.as_str().as_bytes()),
            Self::hex_component(node.as_bytes()),
            incarnation.to_hex(),
            Self::hex_component(hub.as_bytes()),
            hub_incarnation.to_hex()
        )
    }

    pub(super) fn refused_schema_lookup_checkpoint_key(
        tenant: &TenantId,
        edge: &str,
        edge_life: Incarnation,
        hub: &str,
    ) -> String {
        format!(
            "{}{}.{}.{}.{}",
            Self::REFUSED_SCHEMA_LOOKUP_CHECKPOINT_PREFIX,
            Self::hex_component(tenant.as_str().as_bytes()),
            Self::hex_component(edge.as_bytes()),
            edge_life.to_hex(),
            Self::hex_component(hub.as_bytes()),
        )
    }

    pub(super) fn refused_schema_local_fact_scope(
        tenant: &TenantId,
        edge: &str,
        edge_life: Incarnation,
        hub: &str,
        hub_life: Incarnation,
    ) -> String {
        format!(
            "{}{}.{}.{}.{}.{}.",
            Self::REFUSED_SCHEMA_LOCAL_FACT_PREFIX,
            Self::hex_component(tenant.as_str().as_bytes()),
            Self::hex_component(edge.as_bytes()),
            edge_life.to_hex(),
            Self::hex_component(hub.as_bytes()),
            hub_life.to_hex(),
        )
    }

    /// Read only one bounded candidate batch. The encoded payload is admitted
    /// before decoding; audit history and other candidate facts are untouched.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn refused_schema_local_facts(
        &self,
        tenant: &TenantId,
        edge: &str,
        edge_life: Incarnation,
        hub: &str,
        hub_life: Incarnation,
        digests: &[[u8; 32]],
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(
        Vec<crate::protocol::RefusedSchemaUnitOutcome>,
        Option<crate::read_image_memory::ImageMemoryHandle>,
    )> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if digests.len() > 128 {
            return Err(Error::SyncError(
                "too many local refusal facts in one lookup".into(),
            ));
        }
        let prefix = Self::refused_schema_local_fact_scope(tenant, edge, edge_life, hub, hub_life);
        let keys = digests
            .iter()
            .map(|digest| format!("{}{}", prefix, Self::hex_component(digest)))
            .collect::<Vec<_>>();
        if let Some(persistence) = &self.persistence {
            let memory = crate::read_image_memory::ImageMemory::charging_reader_for(
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                read_memory.clone(),
                "sync_policy",
                "read_local_refusal_facts",
                "Reduce the candidate batch or raise MEMORY_LIMIT before reading refusal facts.",
            );
            let values = persistence.load_config_values_for_keys_admitted(&keys, &memory)?;
            Ok((values, Some(memory)))
        } else {
            let progress = self.in_memory_sync_progress.lock();
            let memory = crate::read_image_memory::ImageMemory::charging_reader_for(
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                read_memory.clone(),
                "sync_policy",
                "read_local_refusal_facts",
                "Reduce the candidate batch or raise MEMORY_LIMIT before reading refusal facts.",
            );
            let mut values = Vec::new();
            for key in &keys {
                if let Some(outcome) = progress.refused_schema_units.get(key) {
                    memory.reserve(
                        std::mem::size_of::<crate::protocol::RefusedSchemaUnitOutcome>()
                            .saturating_mul(2)
                            .saturating_add(outcome.reason.len())
                            .saturating_add(outcome.table.len())
                            .saturating_add(outcome.author_node_id.len())
                            .saturating_add(outcome.pushing_node_id.len())
                            .saturating_add(outcome.hub_node_id.len())
                            .saturating_add(outcome.tenant_id.as_str().len()),
                    )?;
                    values.push(outcome.clone());
                }
            }
            Ok((values, Some(memory)))
        }
    }

    /// Commit positive facts and the next candidate position together. A
    /// crash may replay one request but cannot skip its unrecorded refusal.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn persist_refused_schema_lookup_batch(
        &self,
        tenant: &TenantId,
        edge: &str,
        edge_life: Incarnation,
        hub: &str,
        checkpoint: &crate::protocol::RefusedSchemaLookupCheckpoint,
        outcomes: &[crate::protocol::RefusedSchemaUnitOutcome],
    ) -> Result<()> {
        let checkpoint_key =
            Self::refused_schema_lookup_checkpoint_key(tenant, edge, edge_life, hub);
        let prefix = Self::refused_schema_local_fact_scope(
            tenant,
            edge,
            edge_life,
            hub,
            checkpoint.hub_incarnation,
        );
        if let Some(persistence) = &self.persistence {
            let mut puts = Vec::with_capacity(outcomes.len() + 1);
            for outcome in outcomes {
                puts.push((
                    format!("{}{}", prefix, Self::hex_component(&outcome.unit_digest)),
                    RedbPersistence::encode_config_value(outcome)?,
                ));
            }
            puts.push((
                checkpoint_key,
                RedbPersistence::encode_config_value(checkpoint)?,
            ));
            self.publish_direct_metadata(|| persistence.update_indexed_config_values(&puts, &[]))
        } else {
            let mut progress = self.in_memory_sync_progress.lock();
            for outcome in outcomes {
                progress.refused_schema_units.insert(
                    format!("{}{}", prefix, Self::hex_component(&outcome.unit_digest)),
                    outcome.clone(),
                );
            }
            progress
                .refused_schema_lookup_checkpoints
                .insert(checkpoint_key, checkpoint.clone());
            Ok(())
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn persist_refused_schema_local_facts(
        &self,
        tenant: &TenantId,
        edge: &str,
        edge_life: Incarnation,
        hub: &str,
        hub_life: Incarnation,
        outcomes: &[crate::protocol::RefusedSchemaUnitOutcome],
    ) -> Result<()> {
        let prefix = Self::refused_schema_local_fact_scope(tenant, edge, edge_life, hub, hub_life);
        if let Some(persistence) = &self.persistence {
            let puts = outcomes
                .iter()
                .map(|outcome| {
                    Ok((
                        format!("{}{}", prefix, Self::hex_component(&outcome.unit_digest)),
                        RedbPersistence::encode_config_value(outcome)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            self.publish_direct_metadata(|| persistence.update_indexed_config_values(&puts, &[]))
        } else {
            let mut progress = self.in_memory_sync_progress.lock();
            for outcome in outcomes {
                progress.refused_schema_units.insert(
                    format!("{}{}", prefix, Self::hex_component(&outcome.unit_digest)),
                    outcome.clone(),
                );
            }
            Ok(())
        }
    }

    /// Resume one edge's candidate pass after a reconstructed SyncClient.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn refused_schema_lookup_checkpoint(
        &self,
        tenant: &TenantId,
        edge: &str,
        edge_life: Incarnation,
        hub: &str,
    ) -> Result<Option<crate::protocol::RefusedSchemaLookupCheckpoint>> {
        let key = Self::refused_schema_lookup_checkpoint_key(tenant, edge, edge_life, hub);
        if let Some(persistence) = &self.persistence {
            persistence.load_config_value(&key)
        } else {
            Ok(self
                .in_memory_sync_progress
                .lock()
                .refused_schema_lookup_checkpoints
                .get(&key)
                .cloned())
        }
    }

    /// Replace or clear one checkpoint as a single durable config value.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn persist_refused_schema_lookup_checkpoint(
        &self,
        tenant: &TenantId,
        edge: &str,
        edge_life: Incarnation,
        hub: &str,
        checkpoint: Option<&crate::protocol::RefusedSchemaLookupCheckpoint>,
    ) -> Result<()> {
        let key = Self::refused_schema_lookup_checkpoint_key(tenant, edge, edge_life, hub);
        if let Some(persistence) = &self.persistence {
            match checkpoint {
                Some(checkpoint) => self
                    .publish_direct_metadata(|| persistence.flush_config_value(&key, checkpoint))?,
                None => self.publish_direct_metadata(|| persistence.remove_config_value(&key))?,
            }
        } else {
            let mut progress = self.in_memory_sync_progress.lock();
            match checkpoint {
                Some(checkpoint) => {
                    progress
                        .refused_schema_lookup_checkpoints
                        .insert(key, checkpoint.clone());
                }
                None => {
                    progress.refused_schema_lookup_checkpoints.remove(&key);
                }
            }
        }
        Ok(())
    }

    /// Fetch only the requested outcomes. Refusal history may be arbitrarily
    /// large, while a status request contains at most one bounded digest batch.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn refused_schema_unit_outcomes_for_digests(
        &self,
        tenant: &TenantId,
        pushing_node: &str,
        pushing_incarnation: Incarnation,
        hub: &str,
        digests: &[[u8; 32]],
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(
        Vec<crate::protocol::RefusedSchemaUnitOutcome>,
        crate::read_image_memory::ImageMemoryHandle,
    )> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if digests.len() > 128 {
            return Err(Error::SyncError(
                "too many refused schema unit digests in one lookup".into(),
            ));
        }
        // Even an empty lookup must mint or load this hub life, which the
        // status response uses to bind subsequent lookup batches.
        let hub_incarnation =
            self.sync_incarnation_with_read_memory(tenant, read_memory.clone())?;
        let prefix = Self::refused_schema_scope(
            tenant,
            pushing_node,
            pushing_incarnation,
            hub,
            hub_incarnation,
        );
        let keys = digests
            .iter()
            .map(|digest| format!("{}{}", prefix, Self::hex_component(digest)))
            .collect::<Vec<_>>();
        let memory = crate::read_image_memory::ImageMemory::charging_reader_for(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            read_memory.clone(),
            "sync_policy",
            "read_refused_outcome_lookup",
            "Reduce the digest batch or raise MEMORY_LIMIT before reading refusal outcomes.",
        );
        if let Some(persistence) = &self.persistence {
            let values = persistence.load_config_values_for_keys_admitted(&keys, &memory)?;
            Ok((values, memory))
        } else {
            let progress = self.in_memory_sync_progress.lock();
            let mut values = Vec::new();
            for key in &keys {
                if let Some(outcome) = progress.refused_schema_units.get(key) {
                    memory.reserve(
                        std::mem::size_of::<crate::protocol::RefusedSchemaUnitOutcome>()
                            .saturating_mul(2)
                            .saturating_add(outcome.reason.len())
                            .saturating_add(outcome.table.len())
                            .saturating_add(outcome.author_node_id.len())
                            .saturating_add(outcome.pushing_node_id.len())
                            .saturating_add(outcome.hub_node_id.len())
                            .saturating_add(outcome.tenant_id.as_str().len()),
                    )?;
                    values.push(outcome.clone());
                }
            }
            Ok((values, memory))
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn record_refused_schema_units(
        &self,
        plan: &ReceivedPolicyPlan,
        received: &crate::protocol::ReceivedDdlContext,
        receipt: &SyncApplyReceipt,
        hub: &str,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if receipt.tenant_id != received.tenant_id
            || receipt.node_id != received.source_node_id
            || receipt.incarnation != received.source_incarnation
        {
            return Err(Error::SyncError(
                "schema refusal identity differs from authenticated push".into(),
            ));
        }
        let hub_incarnation =
            self.sync_incarnation_with_read_memory(&receipt.tenant_id, read_memory.clone())?;
        for (members, table, reason) in &plan.refused_units {
            let authors = members
                .iter()
                .map(|index| {
                    received
                        .entries
                        .get(*index)
                        .and_then(|entry| entry.author.as_ref())
                })
                .collect::<Option<Vec<_>>>();
            let Some(authors) = authors else {
                continue;
            };
            let Some(author) = authors.first() else {
                continue;
            };
            let digest = blake3::Hash::from(
                crate::protocol::authored_schema_unit_digest(&authors)
                    .map_err(|error| Error::SyncError((error.to_string()).into()))?,
            );
            let key = format!(
                "{}{}",
                Self::refused_schema_scope(
                    &receipt.tenant_id,
                    &receipt.node_id,
                    receipt.incarnation,
                    hub,
                    hub_incarnation
                ),
                digest.to_hex()
            );
            let outcome = crate::protocol::RefusedSchemaUnitOutcome {
                tenant_id: receipt.tenant_id.clone(),
                pushing_node_id: receipt.node_id.clone(),
                pushing_incarnation: receipt.incarnation,
                author_node_id: author.node_id.clone(),
                author_incarnation: author.incarnation,
                source_ddl_lsn: author.source_ddl_lsn,
                unit_digest: *digest.as_bytes(),
                hub_node_id: hub.into(),
                hub_incarnation,
                table: table.clone(),
                reason: reason.clone(),
            };
            if let Some(persistence) = &self.persistence {
                if persistence
                    .load_config_value::<crate::protocol::RefusedSchemaUnitOutcome>(&key)?
                    .is_none()
                {
                    self.publish_direct_metadata(|| {
                        persistence.flush_config_value(&key, &outcome)
                    })?;
                }
            } else {
                self.in_memory_sync_progress
                    .lock()
                    .refused_schema_units
                    .entry(key)
                    .or_insert(outcome);
            }
        }
        Ok(())
    }

    /// Capture pending authenticated data BEFORE the transport filters held groups.
    /// No receipt or applied progress is written by staging.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn stage_policy_held_received_data(
        &self,
        changes: &ChangeSet,
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        receipt: &SyncApplyReceipt,
        hub: &str,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        received: Option<&crate::protocol::ReceivedDdlContext>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut plan = self.received_policy_plan_for_push(
            changes,
            received,
            receipt,
            hub,
            read_memory.clone(),
        )?;
        Self::hold_complete_policy_data(&mut plan, changes, receipt.dependency_complete);
        self.record_policy_holds(&plan.new_holds)?;
        let groups = if receipt.dependency_complete {
            if plan.held_data_lsns.is_empty() {
                Vec::new()
            } else {
                vec![plan.held_data_lsns]
            }
        } else {
            plan.held_data_lsns
                .iter()
                .map(|lsn| BTreeSet::from([*lsn]))
                .collect()
        };
        for lsns in groups {
            let pending = ChangeSet {
                rows: changes
                    .rows
                    .iter()
                    .filter(|row| lsns.contains(&row.lsn))
                    .cloned()
                    .collect(),
                vectors: changes
                    .vectors
                    .iter()
                    .filter(|row| lsns.contains(&row.lsn))
                    .cloned()
                    .collect(),
                edges: changes
                    .edges
                    .iter()
                    .filter(|row| lsns.contains(&row.lsn))
                    .cloned()
                    .collect(),
                ..ChangeSet::default()
            };
            let lineage_map = lineages
                .iter()
                .filter(|(_, _, source, _)| lsns.contains(source))
                .map(|(table, key, source, lineage)| {
                    let key = rmp_serde::to_vec(key)
                        .map_err(|error| Error::SyncError((error.to_string()).into()))?;
                    Ok(((table.clone(), key, *source), lineage.clone()))
                })
                .collect::<Result<HashMap<_, _>>>()?;
            let request = crate::protocol::PushRequest {
                changeset: crate::protocol::wire_changeset_with_arrivals_and_lineages(
                    pending.clone(),
                    arrivals,
                    &lineage_map,
                ),
                incarnation: receipt.incarnation,
            };
            let mut outcome_key = Self::sync_push_outcome_key(
                &receipt.tenant_id,
                &receipt.node_id,
                &request,
                receipt.dependency_complete,
            )?;
            let key = format!(
                "{}{}.{}.{}.{}",
                Self::POLICY_HELD_DATA_PREFIX,
                Self::hex_component(receipt.tenant_id.as_str().as_bytes()),
                Self::hex_component(receipt.node_id.as_bytes()),
                receipt.incarnation.to_hex(),
                blake3::Hash::from(outcome_key.request_digest).to_hex()
            );
            // A dedicated immutable outcome slot closes the replay/delete crash window.
            outcome_key.storage_key = format!("{SYNC_PUSH_OUTCOME_PREFIX}policy_replay.{key}");
            let mut held_receipt = receipt.clone();
            held_receipt.source_lsn = *lsns.last().expect("held group has a source position");
            let held = DurablePolicyHeldData {
                changes: pending,
                arrivals: arrivals
                    .iter()
                    .filter(|(source, _)| lsns.contains(*source))
                    .map(|(k, v)| (*k, *v))
                    .collect(),
                receipt: held_receipt,
                hub_node_id: hub.into(),
                lineages: lineages
                    .iter()
                    .filter(|(_, _, source, _)| lsns.contains(source))
                    .cloned()
                    .collect(),
                outcome_key,
                held_tables: lsns
                    .iter()
                    .filter_map(|lsn| plan.held_data_tables.get(lsn))
                    .flatten()
                    .cloned()
                    .collect(),
            };
            if let Some(persistence) = &self.persistence {
                if persistence
                    .load_config_value::<DurablePolicyHeldData>(&key)?
                    .is_none()
                {
                    self.publish_direct_metadata(|| persistence.flush_config_value(&key, &held))?;
                }
            } else {
                self.in_memory_sync_progress
                    .lock()
                    .policy_held_data
                    .entry(key)
                    .or_insert(held);
            }
        }
        Ok(())
    }

    /// Replay released obligations in source order. An apply outcome is committed
    /// atomically with its rows before the pending payload is removed.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn replay_released_policy_data(
        &self,
        tenant: &TenantId,
        node: &str,
        incarnation: Incarnation,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ApplyResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let prefix = format!(
            "{}{}.{}.{}.",
            Self::POLICY_HELD_DATA_PREFIX,
            Self::hex_component(tenant.as_str().as_bytes()),
            Self::hex_component(node.as_bytes()),
            incarnation.to_hex(),
        );
        let mut total = ApplyResult {
            applied_rows: 0,
            skipped_rows: 0,
            conflicts: Vec::new(),
            new_lsn: self.current_lsn(),
        };
        let mut after: Option<(u64, String)> = None;
        loop {
            let next = if let Some(persistence) = &self.persistence {
                persistence
                    .load_next_config_value_by_source::<DurablePolicyHeldData, _>(
                        &prefix,
                        after.as_ref().map(|(lsn, key)| (*lsn, key.as_str())),
                        crate::memory_accounting::AccountantHandle::clone(
                            &self.finalization.accountant,
                        ),
                        |held| {
                            (held.receipt.tenant_id == *tenant
                                && held.receipt.node_id == node
                                && held.receipt.incarnation == incarnation)
                                .then_some(held.receipt.source_lsn.0)
                        },
                    )?
                    .map(|(key, held, memory)| (key, held, Some(memory)))
            } else {
                self.in_memory_sync_progress
                    .lock()
                    .policy_held_data
                    .iter()
                    .filter(|(key, held)| {
                        key.starts_with(&prefix)
                            && held.receipt.tenant_id == *tenant
                            && held.receipt.node_id == node
                            && held.receipt.incarnation == incarnation
                            && after.as_ref().is_none_or(|(lsn, prior_key)| {
                                (held.receipt.source_lsn.0, key.as_str())
                                    > (*lsn, prior_key.as_str())
                            })
                    })
                    .min_by_key(|(key, held)| (held.receipt.source_lsn, *key))
                    .map(|(key, held)| (key.clone(), held.clone(), None))
            };
            let Some((key, held, _memory)) = next else {
                break;
            };
            after = Some((held.receipt.source_lsn.0, key.clone()));
            let selected = self.scoped_policy_hold_entries(
                Some(node),
                Some((tenant, incarnation)),
                &held.held_tables,
                &BTreeSet::new(),
            )?;
            let _hold_memory = selected.memory;
            let holds = selected
                .entries
                .into_iter()
                .map(|(_, hold)| hold)
                .collect::<Vec<_>>();
            if held.held_tables.iter().any(|table| {
                Self::policy_hold_for(
                    &holds,
                    Some(node),
                    Some((tenant, incarnation)),
                    table,
                    self.table_meta(table).is_some(),
                )
                .is_some()
            }) {
                continue;
            }
            if !self
                .received_policy_plan(
                    &held.changes,
                    None,
                    Some(node),
                    Some((tenant, incarnation)),
                    read_memory.clone(),
                )?
                .held_data_lsns
                .is_empty()
            {
                continue;
            }
            let result = self.apply_authenticated_received_changes_with_outcome(
                held.changes,
                &held.arrivals,
                SyncAdoption::Continuing,
                held.receipt.clone(),
                Some(&held.hub_node_id),
                &held.lineages,
                None,
                Some(&held.outcome_key),
                vector_copy_owner,
                read_memory.clone(),
            )?;
            // A concurrent refusal can re-establish a hold before apply takes
            // its schema lease. Such a response has no committed outcome and
            // must leave the exact pending obligation intact.
            if self.replay_sync_push_outcome(&held.outcome_key)?.is_none() {
                total.conflicts.extend(result.conflicts);
                continue;
            }
            if let Some(persistence) = &self.persistence {
                self.publish_direct_metadata(|| persistence.remove_config_value(&key))?;
            } else {
                self.in_memory_sync_progress
                    .lock()
                    .policy_held_data
                    .remove(&key);
            }
            total.applied_rows += result.applied_rows;
            total.skipped_rows += result.skipped_rows;
            total.conflicts.extend(result.conflicts);
            total.new_lsn = total.new_lsn.max(result.new_lsn);
        }
        Ok(total)
    }

    pub(super) const POLICY_HOLD_RELEASE_PREFIX: &'static str = "sync_policy_hold_release.v1.";

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn policy_hold_release_key(hold: &DurablePolicyHold) -> Result<String> {
        // Preserve the original persisted identity when adding dependency metadata.
        // MessagePack encodes this tuple exactly like the original eight-field hold.
        let identity = (
            &hold.source_node_id,
            &hold.tenant_id,
            &hold.source_incarnation,
            &hold.author_node_id,
            hold.source_lsn,
            &hold.table,
            &hold.refusal,
            hold.table_absent,
        );
        let bytes = rmp_serde::to_vec(&identity)
            .map_err(|error| Error::SyncError((error.to_string()).into()))?;
        Ok(format!(
            "{}{}",
            Self::POLICY_HOLD_RELEASE_PREFIX,
            blake3::hash(&bytes).to_hex()
        ))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn policy_hold_releases_for_correction(
        &self,
        changes: &ChangeSet,
        received: &crate::protocol::ReceivedDdlContext,
    ) -> Result<ScopedPolicyHoldReleases> {
        let restated = changes
            .ddl
            .iter()
            .filter_map(|change| match change {
                DdlChange::CreateTable { name, .. } | DdlChange::AlterTable { name, .. } => {
                    Some(name.as_str())
                }
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        let mut releases = Vec::new();
        let (holds, memory) = self.policy_holds_for_changes(
            changes,
            Some(&received.source_node_id),
            Some((&received.tenant_id, received.source_incarnation)),
        )?;
        for hold in holds {
            if !restated.contains(hold.table.as_str())
                || (!hold.table_absent && hold.source_node_id != received.source_node_id)
                || hold
                    .tenant_id
                    .as_ref()
                    .is_some_and(|tenant| tenant != &received.tenant_id)
                || (!hold.table_absent
                    && hold
                        .source_incarnation
                        .is_some_and(|life| life != received.source_incarnation))
            {
                continue;
            }
            // Bind the release to the complete signed unit that restates this table.
            let Some(author) =
                changes
                    .ddl
                    .iter()
                    .enumerate()
                    .find_map(|(index, change)| match change {
                        DdlChange::CreateTable { name, .. }
                        | DdlChange::AlterTable { name, .. }
                            if name == &hold.table =>
                        {
                            received
                                .entries
                                .get(index)
                                .and_then(|entry| entry.author.as_ref())
                        }
                        _ => None,
                    })
            else {
                continue;
            };
            let authors = received
                .entries
                .iter()
                .filter_map(|entry| entry.author.as_ref())
                .filter(|entry| {
                    entry.node_id == author.node_id
                        && entry.incarnation == author.incarnation
                        && entry.source_ddl_lsn == author.source_ddl_lsn
                })
                .collect::<Vec<_>>();
            let correction_digest = crate::protocol::authored_schema_unit_digest(&authors)
                .map_err(|error| Error::SyncError((error.to_string()).into()))?;
            let key = Self::policy_hold_release_key(&hold)?;
            releases.push((
                key,
                DurablePolicyHoldRelease {
                    hold,
                    tenant_id: received.tenant_id.clone(),
                    source_node_id: received.source_node_id.clone(),
                    source_incarnation: received.source_incarnation,
                    correction_digest,
                },
            ));
        }
        Ok(ScopedPolicyHoldReleases {
            entries: releases,
            memory,
        })
    }

    pub(super) const POLICY_HOLD_PREFIX: &'static str = "sync_policy_hold.v1.";

    pub(super) fn policy_hold_key(source_node_id: &str, table: &str) -> String {
        format!("{}{source_node_id}/{table}", Self::POLICY_HOLD_PREFIX)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn policy_hold_storage_key(hold: &DurablePolicyHold) -> Result<String> {
        // A hold without an authenticated scope uses the source/table address
        // for both reads and removal.
        let scope = if hold.table_absent {
            hold.tenant_id
                .as_ref()
                .map(|tenant| (tenant, None, None, &hold.table))
        } else {
            hold.tenant_id.as_ref().and_then(|tenant| {
                hold.source_incarnation
                    .map(|life| (tenant, Some(&hold.source_node_id), Some(life), &hold.table))
            })
        };
        let Some(scope) = scope else {
            return Ok(Self::policy_hold_key(&hold.source_node_id, &hold.table));
        };
        let encoded = rmp_serde::to_vec(&scope)
            .map_err(|error| Error::SyncError((error.to_string()).into()))?;
        Ok(format!(
            "{}scoped.{}",
            Self::POLICY_HOLD_PREFIX,
            blake3::hash(&encoded).to_hex()
        ))
    }

    pub(super) const POLICY_HOLD_INDEX_PREFIX: &'static str = "sync_policy_hold_select.v1.";

    pub(super) fn policy_hold_memory(
        accountant: crate::memory_accounting::AccountantHandle,
    ) -> crate::read_image_memory::ImageMemoryHandle {
        crate::read_image_memory::ImageMemory::charging_for(
            accountant,
            "sync_policy",
            "read_policy_holds",
            "Raise MEMORY_LIMIT before reading this delivery's policy holds.",
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn policy_hold_scope(
        tenant: Option<&TenantId>,
        source: Option<&str>,
        life: Option<Incarnation>,
        absent: bool,
    ) -> Result<String> {
        let bytes = rmp_serde::to_vec(&(tenant, source, life, absent))
            .map_err(|error| Error::SyncError((error.to_string()).into()))?;
        Ok(blake3::hash(&bytes).to_hex().to_string())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn policy_hold_index_keys(
        key: &str,
        hold: &DurablePolicyHold,
    ) -> Result<Vec<String>> {
        let scope = Self::policy_hold_scope(
            hold.tenant_id.as_ref(),
            (!hold.table_absent).then_some(hold.source_node_id.as_str()),
            if hold.table_absent {
                None
            } else {
                hold.source_incarnation
            },
            hold.table_absent,
        )?;
        let suffix = blake3::hash(key.as_bytes()).to_hex();
        let mut indexes = vec![format!(
            "{}{scope}.table.{}.{suffix}",
            Self::POLICY_HOLD_INDEX_PREFIX,
            Self::hex_component(hold.table.as_bytes())
        )];
        indexes.extend(hold.dag_edge_types.iter().map(|edge| {
            format!(
                "{}{scope}.edge.{}.{suffix}",
                Self::POLICY_HOLD_INDEX_PREFIX,
                Self::hex_component(edge.as_bytes())
            )
        }));
        Ok(indexes)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn policy_hold_index_values(
        key: &str,
        hold: &DurablePolicyHold,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        Self::policy_hold_index_keys(key, hold)?
            .iter()
            .map(|index| Ok((index.clone(), RedbPersistence::encode_config_value(&key)?)))
            .collect()
    }

    pub(super) fn policy_hold_matches_scope(
        hold: &DurablePolicyHold,
        source: Option<&str>,
        identity: Option<(&TenantId, Incarnation)>,
    ) -> bool {
        hold.tenant_id
            .as_ref()
            .is_none_or(|tenant| identity.is_some_and(|(id, _)| tenant == id))
            && (hold.table_absent
                || (Some(hold.source_node_id.as_str()) == source
                    && hold
                        .source_incarnation
                        .is_none_or(|life| identity.is_some_and(|(_, id)| life == id))))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn scoped_policy_hold_entries(
        &self,
        source: Option<&str>,
        identity: Option<(&TenantId, Incarnation)>,
        tables: &BTreeSet<String>,
        edges: &BTreeSet<String>,
    ) -> Result<ScopedPolicyHolds> {
        let memory = Self::policy_hold_memory(crate::memory_accounting::AccountantHandle::clone(
            &self.finalization.accountant,
        ));
        let mut entries = if let Some(persistence) = &self.persistence {
            let mut scopes = BTreeSet::new();
            let tenants = [None, identity.map(|(tenant, _)| tenant)];
            for tenant in tenants {
                scopes.insert(Self::policy_hold_scope(tenant, None, None, true)?);
                if let Some(source) = source {
                    for life in [None, identity.map(|(_, life)| life)] {
                        scopes.insert(Self::policy_hold_scope(tenant, Some(source), life, false)?);
                    }
                }
            }
            let mut prefixes = Vec::new();
            for scope in scopes {
                for (kind, names) in [("table", tables), ("edge", edges)] {
                    for name in names {
                        memory.reserve(256usize.saturating_add(name.len().saturating_mul(2)))?;
                        prefixes.push(format!(
                            "{}{scope}.{kind}.{}.",
                            Self::POLICY_HOLD_INDEX_PREFIX,
                            Self::hex_component(name.as_bytes())
                        ));
                    }
                }
            }
            // Current primary addresses can also be selected exactly. This
            // keeps direct historical writes readable without a global decode.
            let mut keys = BTreeSet::new();
            if let Some((tenant, life)) = identity {
                for table in tables {
                    // Probe only the primary matching the current table state.
                    // Selectors also find older states during correction cleanup.
                    let absent = !self.relational_store.table_meta.read().contains_key(table);
                    if absent || source.is_some() {
                        memory.reserve(
                            512usize
                                .saturating_add(table.len())
                                .saturating_add(source.map_or(0, str::len))
                                .saturating_add(tenant.as_str().len())
                                .saturating_mul(2),
                        )?;
                        let hold = DurablePolicyHold {
                            source_node_id: source.unwrap_or_default().into(),
                            tenant_id: Some(tenant.clone()),
                            source_incarnation: Some(life),
                            author_node_id: None,
                            source_lsn: Lsn(0),
                            table: table.clone(),
                            refusal: String::new(),
                            table_absent: absent,
                            dag_edge_types: Vec::new(),
                        };
                        keys.insert(Self::policy_hold_storage_key(&hold)?);
                    }
                }
            }
            persistence.load_indexed_config_values::<DurablePolicyHold>(keys, &prefixes, &memory)?
        } else {
            let state = self.in_memory_sync_progress.lock();
            let mut entries = Vec::new();
            for (key, hold) in &state.policy_holds {
                if !Self::policy_hold_matches_scope(hold, source, identity)
                    || !(tables.contains(&hold.table)
                        || hold.dag_edge_types.iter().any(|edge| edges.contains(edge)))
                {
                    continue;
                }
                let bytes = key
                    .len()
                    .saturating_add(hold.source_node_id.len())
                    .saturating_add(hold.author_node_id.as_ref().map_or(0, String::len))
                    .saturating_add(
                        hold.tenant_id
                            .as_ref()
                            .map_or(0, |tenant| tenant.as_str().len()),
                    )
                    .saturating_add(hold.table.len())
                    .saturating_add(hold.refusal.len())
                    .saturating_add(
                        hold.dag_edge_types
                            .iter()
                            .map(|edge| edge.len().saturating_add(32))
                            .sum::<usize>(),
                    )
                    .saturating_add(std::mem::size_of::<DurablePolicyHold>().saturating_mul(4))
                    .saturating_add(256);
                memory.reserve(bytes)?;
                entries.push((key.clone(), hold.clone()));
            }
            entries
        };
        let mut active = Vec::new();
        for (key, hold) in entries.drain(..) {
            if !Self::policy_hold_matches_scope(&hold, source, identity)
                || !(tables.contains(&hold.table)
                    || hold.dag_edge_types.iter().any(|edge| edges.contains(edge)))
            {
                continue;
            }
            memory.reserve(
                hold.refusal
                    .len()
                    .saturating_add(hold.table.len())
                    .saturating_add(hold.source_node_id.len())
                    .saturating_add(hold.author_node_id.as_ref().map_or(0, String::len))
                    .saturating_add(
                        hold.tenant_id
                            .as_ref()
                            .map_or(0, |tenant| tenant.as_str().len()),
                    )
                    .saturating_mul(2)
                    .saturating_add(512),
            )?;
            let release_key = Self::policy_hold_release_key(&hold)?;
            let released = if let Some(persistence) = &self.persistence {
                persistence.config_value_exists(
                    &RedbPersistence::policy_hold_release_marker(&release_key)
                        .expect("policy release key"),
                )? && persistence
                    .load_config_value_admitted::<DurablePolicyHoldRelease>(&release_key, &memory)?
                    .is_some_and(|release| release.hold == hold)
            } else {
                self.in_memory_sync_progress
                    .lock()
                    .policy_hold_releases
                    .get(&release_key)
                    .is_some_and(|release| release.hold == hold)
            };
            if !released {
                active.push((key, hold));
            }
        }
        Ok(ScopedPolicyHolds {
            entries: active,
            memory,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn policy_holds_for_changes(
        &self,
        changes: &ChangeSet,
        source: Option<&str>,
        identity: Option<(&TenantId, Incarnation)>,
    ) -> Result<(
        Vec<DurablePolicyHold>,
        crate::read_image_memory::ImageMemoryHandle,
    )> {
        let mut tables: BTreeSet<String> = changes
            .rows
            .iter()
            .map(|row| row.table.clone())
            .chain(
                changes
                    .vectors
                    .iter()
                    .map(|vector| vector.index.table.clone()),
            )
            .chain(
                changes
                    .ddl
                    .iter()
                    .filter_map(Self::ddl_affected_table)
                    .map(str::to_owned),
            )
            .collect();
        let edges: BTreeSet<String> = changes
            .edges
            .iter()
            .map(|edge| edge.edge_type.clone())
            .collect();
        if !edges.is_empty() {
            for (table, meta) in self.relational_store.table_meta.read().iter() {
                if meta.dag_edge_types.iter().any(|edge| edges.contains(edge)) {
                    tables.insert(table.clone());
                }
            }
        }
        let selected = self.scoped_policy_hold_entries(source, identity, &tables, &edges)?;
        Ok((
            selected.entries.into_iter().map(|(_, hold)| hold).collect(),
            selected.memory,
        ))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn policy_hold_entries(&self) -> Result<Vec<(String, DurablePolicyHold)>> {
        if let Some(persistence) = &self.persistence {
            let mut active = Vec::new();
            for (key, hold) in persistence
                .load_config_values_with_prefix::<DurablePolicyHold>(Self::POLICY_HOLD_PREFIX)?
            {
                let release = persistence.load_config_value::<DurablePolicyHoldRelease>(
                    &Self::policy_hold_release_key(&hold)?,
                )?;
                if release.is_none_or(|release| release.hold != hold) {
                    active.push((key, hold));
                }
            }
            return Ok(active);
        }
        let state = self.in_memory_sync_progress.lock();
        state
            .policy_holds
            .iter()
            .filter_map(|(key, hold)| {
                let release_key = match Self::policy_hold_release_key(hold) {
                    Ok(key) => key,
                    Err(error) => return Some(Err(error)),
                };
                state
                    .policy_hold_releases
                    .get(&release_key)
                    .is_none_or(|release| release.hold != *hold)
                    .then(|| Ok((key.clone(), hold.clone())))
            })
            .collect()
    }

    /// Every durable hold this node keeps behind a refused authored
    /// transaction.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn policy_holds(&self) -> Result<Vec<DurablePolicyHold>> {
        Ok(self
            .policy_hold_entries()?
            .into_iter()
            .map(|(_, hold)| hold)
            .collect())
    }

    /// Record the holds a refused transaction leaves. The first refusal of a
    /// table from a peer is the one later deliveries wait behind; a later
    /// refusal of the same table does not move it.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn record_policy_holds(&self, holds: &[DurablePolicyHold]) -> Result<()> {
        for hold in holds {
            let key = Self::policy_hold_storage_key(hold)?;
            let selected = self.scoped_policy_hold_entries(
                Some(&hold.source_node_id),
                hold.tenant_id.as_ref().zip(hold.source_incarnation),
                &BTreeSet::from([hold.table.clone()]),
                &BTreeSet::new(),
            )?;
            if selected.entries.iter().any(|(_, prior)| {
                Self::policy_hold_storage_key(prior).is_ok_and(|prior| prior == key)
            }) {
                continue;
            }
            if let Some(persistence) = &self.persistence {
                let removes = persistence
                    .load_config_value_admitted::<DurablePolicyHold>(&key, &selected.memory)?
                    .map(|prior| Self::policy_hold_index_keys(&key, &prior))
                    .transpose()?
                    .unwrap_or_default();
                let mut values = Self::policy_hold_index_values(&key, hold)?;
                values.push((key, RedbPersistence::encode_config_value(hold)?));
                self.publish_direct_metadata(|| {
                    persistence.update_indexed_config_values(&values, &removes)
                })?;
            } else {
                self.in_memory_sync_progress
                    .lock()
                    .policy_holds
                    .insert(key, hold.clone());
            }
        }
        Ok(())
    }

    /// A transaction that creates or acceptably restates a table has
    /// installed: nothing from its peer waits behind an earlier refusal of
    /// that table, and nothing waits behind a refused creation of a table that
    /// now exists.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn release_policy_holds(
        &self,
        source: Option<&str>,
        identity: Option<(&TenantId, Incarnation)>,
        tables: &BTreeSet<String>,
    ) -> Result<()> {
        if tables.is_empty() {
            return Ok(());
        }
        let selected =
            self.scoped_policy_hold_entries(source, identity, tables, &BTreeSet::new())?;
        for (key, hold) in &selected.entries {
            if !tables.contains(&hold.table)
                || hold
                    .tenant_id
                    .as_ref()
                    .is_some_and(|tenant| identity.is_none_or(|(id, _)| id != tenant))
                || !(hold.table_absent
                    || (Some(hold.source_node_id.as_str()) == source
                        && hold.source_incarnation.is_none_or(|life| {
                            identity.is_some_and(|(_, incarnation)| incarnation == life)
                        })))
            {
                continue;
            }
            if let Some(persistence) = &self.persistence {
                let mut removes = Self::policy_hold_index_keys(key, hold)?;
                removes.push(key.clone());
                self.publish_direct_metadata(|| {
                    persistence.update_indexed_config_values(&[], &removes)
                })?;
            } else {
                self.in_memory_sync_progress.lock().policy_holds.remove(key);
            }
        }
        Ok(())
    }

    /// Every table this receiver holds received data back on because an
    /// authored transaction touching it was refused by this node's declared
    /// table policy: who sent it, whose transaction it was and at which source
    /// position, the table, and the refusal the author must act on. Empty is
    /// the ordinary state.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn policy_held_received_tables(&self) -> Result<Vec<PolicyHeldReceivedTable>> {
        let _operation = self.open_operation()?;
        let mut held = self
            .policy_holds()?
            .into_iter()
            .map(|hold| PolicyHeldReceivedTable {
                source_node_id: hold.source_node_id,
                author_node_id: hold.author_node_id,
                source_lsn: hold.source_lsn,
                table: hold.table,
                creation_refused: hold.table_absent,
                refusal: hold.refusal,
            })
            .collect::<Vec<_>>();
        held.sort();
        Ok(held)
    }

    /// Replay an unattested change set's source transactions one at a time, in
    /// source order, after the caller has judged the whole set. Each unit is
    /// committed before the next applies, so a row sees exactly the schema and
    /// triggers its author wrote it under, and every declaration reaches the
    /// log in the author's order. A unit refused here stops the replay; the
    /// units before it stay installed, as they did at the source.
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn apply_unattested_source_units(
        &self,
        units: Vec<ChangeSet>,
        policies: &ConflictPolicies,
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        adoption: SyncAdoption,
        apply_role: SyncApplyRole,
        terminal_refusal_context: Option<&TerminalRefusalPullContext>,
        hub_local_author: Option<&str>,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        stage_verified_lineages: bool,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ApplyResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut total = ApplyResult {
            applied_rows: 0,
            skipped_rows: 0,
            conflicts: Vec::new(),
            new_lsn: self.current_lsn(),
        };
        #[cfg(any(test, feature = "test-seams"))]
        let units = resident_history_test_seams::source_unit_boundaries_for_test(units, self);
        for unit in units {
            // Decoded declaration evidence is consumed by this publication.
            // Release it after all unit locals, including the final unit.
            let _unit_outputs = self.writer_output_scope();
            // This unit completes durability and publication before replay
            // advances. Its materialized copies therefore end here, rather
            // than accumulating credit in the whole delivery's caller token.
            let mut unit_vector_copy_owner = vector_copy_owner.scratch();
            let result = if unit.ddl.is_empty() {
                let unit_reservation = self.preflight_sync_apply_memory(
                    &unit,
                    policies,
                    read_memory.clone(),
                    &mut unit_vector_copy_owner,
                )?;
                let _sync_trigger_gate = self.enter_sync_apply_trigger_gate_bypass();
                self.apply_changes_single_lsn_group(
                    unit,
                    policies,
                    arrivals,
                    adoption,
                    apply_role,
                    None,
                    false,
                    terminal_refusal_context,
                    None,
                    lineages,
                    stage_verified_lineages,
                    None,
                    None,
                    unit_reservation.as_ref(),
                    &mut unit_vector_copy_owner,
                    read_memory.clone(),
                )?
            } else {
                let _sync_trigger_gate = self.enter_sync_apply_trigger_gate_bypass();
                let holder = Self::received_unit_hold_identity(&unit.ddl_lsn, None);
                let applied: Result<ApplyResult> = self
                    .commit_received_schema_stage(
                        &unit,
                        None,
                        lineages,
                        arrivals,
                        adoption,
                        apply_role,
                        None,
                        false,
                        terminal_refusal_context,
                        hub_local_author,
                        None,
                        None,
                        Some(policies),
                        |vector_copy_owner| {
                            self.refuse_received_unit_behind_a_memory_holdback(&unit.ddl, &holder)?;
                            self.preflight_sync_apply_memory(
                                &unit,
                                policies,
                                read_memory.clone(),
                                vector_copy_owner,
                            )
                        },
                        || Ok(()),
                        &mut unit_vector_copy_owner,
                        read_memory.clone(),
                    )
                    .map(Into::into);
                match applied {
                    Ok(result) => {
                        // Best effort, as for a single installed unit.
                        let _ = self.release_memory_schema_holdback_for_applied_unit(
                            &Self::sync_ddl_unit_tables(&unit.ddl),
                            &holder,
                        );
                        result
                    }
                    Err(refusal) => {
                        let _ =
                            self.hold_back_received_unit_for_memory(&unit.ddl, &holder, &refusal);
                        return Err(refusal);
                    }
                }
            };
            total.applied_rows += result.applied_rows;
            total.skipped_rows += result.skipped_rows;
            total.conflicts.extend(result.conflicts);
            total.new_lsn = result.new_lsn;
        }
        Ok(total)
    }

    /// Insert, after each DROP TABLE, a DROP TRIGGER for every trigger then
    /// declared on that table which the same source transaction does not drop
    /// explicitly later.
    pub(super) fn with_implicit_trigger_tombstones(
        &self,
        ddl: &[DdlChange],
        ddl_lsn: &[Lsn],
    ) -> (Vec<DdlChange>, Vec<Lsn>) {
        let mut declared = self
            .trigger
            .declarations
            .lock()
            .iter()
            .map(|(name, declaration)| (name.clone(), declaration.table.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut out_ddl = Vec::with_capacity(ddl.len());
        let mut out_lsn = Vec::with_capacity(ddl_lsn.len());
        for (position, (change, lsn)) in ddl.iter().zip(ddl_lsn).enumerate() {
            out_ddl.push(change.clone());
            out_lsn.push(*lsn);
            match change {
                DdlChange::CreateTrigger { name, table, .. }
                | DdlChange::CreateTriggerIncludingSync { name, table, .. } => {
                    declared.insert(name.clone(), table.clone());
                }
                DdlChange::DropTrigger { name } => {
                    declared.remove(name);
                }
                DdlChange::DropTable { name: dropped } => {
                    let on_table = declared
                        .iter()
                        .filter(|(_, table)| *table == dropped)
                        .map(|(name, _)| name.clone())
                        .collect::<Vec<_>>();
                    for trigger in on_table {
                        declared.remove(&trigger);
                        let dropped_later = ddl[position + 1..]
                            .iter()
                            .zip(&ddl_lsn[position + 1..])
                            .any(|(later, later_lsn)| {
                                later_lsn == lsn
                                    && matches!(later, DdlChange::DropTrigger { name } if *name == trigger)
                            });
                        if !dropped_later {
                            out_ddl.push(DdlChange::DropTrigger { name: trigger });
                            out_lsn.push(*lsn);
                        }
                    }
                }
                _ => {}
            }
        }
        (out_ddl, out_lsn)
    }

    /// Refuse a change set carrying the maximum transaction identity before
    /// the allocator is touched.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn refuse_max_txid_values(changes: &ChangeSet) -> Result<()> {
        for row in &changes.rows {
            for v in row.values.values() {
                if let Value::TxId(incoming) = v
                    && incoming.0 == u64::MAX
                {
                    return Err(Error::TxIdOverflow {
                        table: row.table.clone(),
                        incoming: u64::MAX,
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
    pub(super) fn validate_public_changeset_ddl_lsn(changes: &ChangeSet) -> Result<()> {
        changes
            .validate_ddl_lsn_cardinality()
            .map_err(|error| Error::SyncError(error.into()))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn preflight_sync_apply_trigger_ready(
        &self,
        changes: &ChangeSet,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if self.trigger.ready.load(Ordering::SeqCst) || changes.is_empty() {
            return Ok(());
        }

        if self.sync_changeset_is_trigger_tombstone_only(changes) {
            return Ok(());
        }

        if !changes.ddl.is_empty() {
            let no_triggers = if Self::received_side_metadata_required(changes) {
                let (_charge, projected) = self.preflight_sync_trigger_projection(changes)?;
                projected.is_empty()
            } else {
                self.trigger.declarations.lock().is_empty()
            };
            if no_triggers {
                return Ok(());
            }
        }

        if self.sync_relay_mode_enabled() {
            if changes.ddl.is_empty() {
                let projected_table_meta = self.relational_store.table_meta.read();
                self.preflight_sync_rows_against_projected_schema(
                    &changes.rows,
                    &projected_table_meta,
                    read_memory.clone(),
                )?;
            }
            return Ok(());
        }

        self.ensure_sync_apply_ready()
    }

    /// True when one incoming sync column already holds the same value locally
    /// — the per-column half of the "unchanged" test `upsert_row_for_sync`
    /// answers `UpsertResult::NoOp` on, hoisted ahead of the conflict policies
    /// so re-delivered data is never mistaken for two machines disagreeing.
    ///
    /// A literally equal column costs one comparison; only a column that
    /// differs is run through the column's own coercion, so a cell that differs
    /// solely in representation (an `Int32` arriving for an `INTEGER` column)
    /// still reads as unchanged without allocating on the equal path.
    pub(super) fn sync_incoming_value_is_already_local(
        incoming: &Value,
        local_value: &Value,
        column: &str,
        table: &str,
        meta: Option<&TableMeta>,
    ) -> bool {
        if local_value == incoming {
            return true;
        }
        meta.is_some_and(|meta| {
            crate::executor::coerce_into_column_with_meta(
                table,
                meta,
                column,
                incoming.clone(),
                None,
                None,
            )
            .is_ok_and(|coerced| &coerced == local_value)
        })
    }

    /// Whether an incoming row carries exactly what a committed local version
    /// holds, including the columns a disk-backed writer keeps out of its
    /// resident rows. The resident columns are compared first; only a
    /// version that agrees on all of them has its shed columns read back from
    /// its own durable version, so a large body is never loaded to reject a
    /// row that already differs.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn sync_incoming_row_matches_committed(
        &self,
        values: &HashMap<ColName, Value>,
        local: &VersionedRow,
        table: &str,
        meta: Option<&TableMeta>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut shed_columns = false;
        for (column, incoming) in values {
            match local.values.get(column) {
                Some(local_value) => {
                    if !Self::sync_incoming_value_is_already_local(
                        incoming,
                        local_value,
                        column,
                        table,
                        meta,
                    ) {
                        return Ok(false);
                    }
                }
                None => shed_columns = true,
            }
        }
        if !shed_columns {
            return Ok(true);
        }
        let Some(meta) = meta else {
            return Ok(false);
        };
        if !self
            .relational_store
            .committed_row_may_omit_columns(meta, local)
        {
            return Ok(false);
        }
        let shed = self
            .shed_committed_values_for_change(
                table,
                local,
                &ChangeProjection::unbounded(),
                read_memory.clone(),
            )?
            .into_iter()
            .collect::<HashMap<_, _>>();
        Ok(values
            .iter()
            .filter(|(column, _)| !local.values.contains_key(*column))
            .all(|(column, incoming)| {
                shed.get(column).is_some_and(|local_value| {
                    Self::sync_incoming_value_is_already_local(
                        incoming,
                        local_value,
                        column,
                        table,
                        Some(meta),
                    )
                })
            }))
    }

    /// Put the receiver's retained row back on the normal forward sync log
    /// when a non-overwriting policy refuses an incoming mutation. This is
    /// deliberately part of the *same* sync transaction as the refusal: a
    /// follow-up transaction could snapshot an old row, let a newer hub write
    /// commit, then overwrite that newer value while trying to repair the
    /// loser.
    ///
    /// The conditional guard makes that promise hold even when a local writer
    /// commits between this branch's point lookup and this transaction's final
    /// validation. In that race the staged replacement is discarded and the
    /// newer writer's own ordinary change-log entry is what the edge pulls.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stage_authoritative_row_after_sync_refusal(
        &self,
        tx: TxId,
        table: &str,
        local: &VersionedRow,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let (retained_source_lsn, retained_source_kind) =
            self.relational_sync_source_at_read_root(table, local.row_id, read_memory.clone())?;
        let (retained_acceptance_position, retained_source_kind) =
            match (retained_source_lsn, retained_source_kind) {
                (Some(source_lsn), Some(source_kind)) => (
                    Self::resolve_sync_source_lsn(Some(source_lsn), local.lsn)
                        .expect("a stored sync source LSN always resolves"),
                    source_kind,
                ),
                (None, None) => (
                    local.lsn,
                    contextdb_relational::store::SyncSourceKind::AcceptedLocal,
                ),
                _ => {
                    return Err(Error::SyncError(
                        (format!(
                            "sync provenance sidecar is incomplete for {table} row {}",
                            local.row_id.0
                        ))
                        .into(),
                    ));
                }
            };
        let snapshot = self.snapshot();
        let vector_columns = self
            .table_meta(table)
            .into_iter()
            .flat_map(|meta| meta.columns.into_iter())
            .filter(|column| matches!(column.column_type, ColumnType::Vector(_)))
            .map(|column| column.name)
            .collect::<Vec<_>>();
        let mut vectors = Vec::new();
        for column in vector_columns {
            let index = VectorIndexRef::new(table, column);
            if let Some(entry) = self.vector_store.load_live_entry_for_row(
                &index,
                local.row_id,
                snapshot,
                vector_copy_owner,
            )? {
                vectors.push((index, entry.vector));
            }
        }
        let before = self.write_set_counts(tx)?;

        for (index, _) in &vectors {
            self.delete_vector_with_read_memory(
                tx,
                index.clone(),
                local.row_id,
                vector_copy_owner,
                read_memory.clone(),
            )?;
        }
        self.delete_relational_row_releasing_staged(tx, table, local.row_id, read_memory.clone())?;
        self.insert_synced_row_at(
            tx,
            table,
            local.row_id,
            local.values.clone(),
            snapshot,
            local.created_at,
            read_memory.clone(),
        )?;
        // This repair republishes the same winner in a new change-log entry;
        // it is not a new acceptance.
        self.set_sync_row_source_lsn_kind(
            tx,
            table,
            local.row_id,
            retained_acceptance_position,
            retained_source_kind,
        )?;
        for (index, vector) in vectors {
            self.insert_vector_with_read_memory(
                tx,
                index,
                local.row_id,
                vector,
                vector_copy_owner,
                read_memory.clone(),
            )?;
        }
        let after = self.write_set_counts(tx)?;
        self.record_conditional_update_guard(
            tx,
            table.to_string(),
            local.row_id,
            local
                .values
                .iter()
                .map(|(column, value)| (column.clone(), value.clone()))
                .collect(),
            before,
            after,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn apply_changes_single_lsn_group(
        &self,
        changes: ChangeSet,
        policies: &ConflictPolicies,
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        adoption: SyncAdoption,
        apply_role: SyncApplyRole,
        receipt: Option<SyncApplyReceipt>,
        dependency_complete: bool,
        terminal_refusal_context: Option<&TerminalRefusalPullContext>,
        hub_local_author: Option<&str>,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        stage_verified_lineages: bool,
        delivery_prepare: Option<&mut dyn crate::custody::preparation::CommitPreparation>,
        push_outcome_key: Option<&SyncPushOutcomeContext>,
        unit_reservation: Option<&crate::memory_accounting::SchemaUnitReservation>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ApplyResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.apply_changes_with_private_source_order(
            changes,
            policies,
            arrivals,
            adoption,
            apply_role,
            receipt,
            dependency_complete,
            terminal_refusal_context,
            hub_local_author,
            lineages,
            stage_verified_lineages,
            delivery_prepare,
            push_outcome_key,
            unit_reservation,
            vector_copy_owner,
            read_memory,
            None,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn apply_changes_with_private_source_order(
        &self,
        changes: ChangeSet,
        policies: &ConflictPolicies,
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        adoption: SyncAdoption,
        apply_role: SyncApplyRole,
        receipt: Option<SyncApplyReceipt>,
        dependency_complete: bool,
        terminal_refusal_context: Option<&TerminalRefusalPullContext>,
        hub_local_author: Option<&str>,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        stage_verified_lineages: bool,
        delivery_prepare: Option<&mut dyn crate::custody::preparation::CommitPreparation>,
        push_outcome_key: Option<&SyncPushOutcomeContext>,
        unit_reservation: Option<&crate::memory_accounting::SchemaUnitReservation>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        private_replay: Option<connected_replay::PrivateReplayContext<'_>>,
    ) -> Result<ApplyResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        (|| {
            let published = self.tx_mgr.capture_published_read();
            let _writer_read = self.enter_writer_read(
                writer_read::durable_root(published.handle)?,
                read_memory.clone(),
            );
            let replace_received_vector_image = stage_verified_lineages
                && !changes.vectors.is_empty()
                && self.received_vector_image_pending.load(Ordering::SeqCst);
            let private_preparation_owner = if private_replay.is_some() {
                match crate::persistence::received_working_source::WorkingReadSource::current_for(
                    self.tx_mgr.identity(),
                )? {
                    Some(source) => source.with_original(|original| {
                        crate::memory_accounting::persistence_original_projection(original)?
                            .read_budget()
                    })?,
                    None => read_memory.clone(),
                }
            } else {
                read_memory.clone()
            };
            let mut tx = self.begin()?;
            let private_scope = private_replay
                .map(|context| {
                    connected_replay::PrivateUnitScope::enter(
                        self,
                        tx,
                        context.receiver_tx,
                        context.end,
                        private_preparation_owner.clone(),
                    )
                })
                .transpose()?;
            let source_coordinates = private_replay.and_then(|context| context.coordinates);
            if self.capture_detached_sync_write_set.load(Ordering::SeqCst) {
                // Bind capture to the first outer adjudicator transaction.
                // An independently committed DDL helper must not consume it.
                self.tx_mgr.with_active_write_sets(|sets| {
                    let owned = sets.values().any(|ws| {
                        ws.config_writes
                            .iter()
                            .any(|(key, _)| key == "__contextdb.detached_capture_owner.v1")
                    });
                    if !owned {
                        sets.get_mut(&tx)
                            .expect("new adjudicator transaction")
                            .config_writes
                            .push(("__contextdb.detached_capture_owner.v1".into(), Vec::new()));
                    }
                });
            }

            let _local_schema_bypass = self.enter_sync_apply_local_schema_bypass(tx);
            let atomic_receipt = dependency_complete;
            if let Some(receipt) = receipt.as_ref() {
                self.stage_sync_apply_receipt(tx, receipt, read_memory.clone())?;
            }
            let commit_each_row = false;
            let batch_row_commits = false;
            let mut result = ApplyResult {
                applied_rows: 0,
                skipped_rows: 0,
                conflicts: Vec::new(),
                new_lsn: self.current_lsn(),
            };
            let mut vector_row_map: HashMap<RowId, RowId> = HashMap::new();
            let mut received_image_superseded_vectors = Vec::<VectorVersionIdentity>::new();
            let mut failed_vector_groups: HashSet<(RowId, String, Lsn, bool)> = HashSet::new();
            let mut table_meta_cache: HashMap<String, Option<TableMeta>> = HashMap::new();
            let mut applied_cache_credit = source_coordinates.map(|_| {
                contextdb_core::read_memory::ReadCredit::new(private_preparation_owner.clone())
            });
            let mut applied_rows_cache: HashMap<String, Vec<VersionedRow>> = HashMap::new();
            let mut applied_deleted_committed_row_ids: HashMap<String, HashSet<RowId>> =
                HashMap::new();
            let mut applied_new_row_ids: HashSet<RowId> = HashSet::new();
            let _private_event_declarations = source_coordinates
                .map(|_| {
                    contextdb_core::read_memory::ReadCredit::try_new(
                        private_preparation_owner.clone(),
                        self.received_event_bus_projection_bytes()?
                            .saturating_mul(8)
                            .saturating_add(
                                changes
                                    .ddl
                                    .iter()
                                    .map(DdlChange::estimated_bytes)
                                    .fold(0usize, usize::saturating_add)
                                    .saturating_mul(8),
                            )
                            .saturating_add(4096),
                        "sync",
                        "connected_event_declarations",
                        "Raise the declared memory limit to prepare event declarations.",
                    )
                })
                .transpose()?;
            let mut event_bus_ddl = Vec::new();
            let mut trigger_ddl = Vec::new();
            let mut accepted_authenticated_rows = Vec::new();
            let mut sync_trigger_projection = self.trigger.declarations.lock().clone();
            let dependency_unit_row_count = changes.rows.len();
            let dependency_unit_rows = changes.rows.clone();
            let mut accepted_sync_tombstones = Vec::<AcceptedSyncTombstone>::new();
            let mut terminal_marker_clears = Vec::<(String, NaturalKey)>::new();
            let mut semantic_refused_rows = Vec::<RowChange>::new();
            let phases = connected_replay::PrivateApplyPhases::prepare(
                changes,
                source_coordinates,
                read_memory.clone(),
            )?;
            let mut private_ddl_cursor = 0usize;
            for mut changes in phases {
                let mut vector_row_idx = 0usize;
                let group_has_data = changes.data_entry_count() != 0;
                let dropped_tables = Self::sync_dropped_tables(&changes.ddl);
                let phase_source = changes
                    .ddl_lsn
                    .first()
                    .copied()
                    .or_else(|| changes.rows.first().map(|row| row.lsn))
                    .or_else(|| changes.edges.first().map(|edge| edge.lsn))
                    .or_else(|| changes.vectors.first().map(|vector| vector.lsn))
                    .unwrap_or(Lsn(0));
                if source_coordinates.is_some() {
                    self.set_private_graph_phase(
                        tx,
                        crate::persistence::received_working_graph::WorkingGraphOrigin::Received {
                            source_lsn: phase_source,
                        },
                    )?;
                }
                let _phase_declaration_credit = source_coordinates
                    .map(|_| {
                        let bytes = sync_trigger_projection
                            .values()
                            .fold(0usize, |bytes, declaration| {
                                bytes
                                    .saturating_add(declaration.name.len())
                                    .saturating_add(declaration.table.len())
                                    .saturating_add(declaration.on_events.len().saturating_mul(32))
                                    .saturating_add(512)
                            })
                            .saturating_add(
                                changes
                                    .ddl
                                    .iter()
                                    .map(DdlChange::estimated_bytes)
                                    .fold(0usize, usize::saturating_add)
                                    .saturating_mul(8),
                            );
                        contextdb_core::read_memory::ReadCredit::try_new(
                            private_preparation_owner.clone(),
                            bytes,
                            "sync",
                            "connected_callback_declarations",
                            "Raise the declared memory limit to prepare this unit's callbacks.",
                        )
                    })
                    .transpose()?;
                let phase_trigger_start =
                    source_coordinates.map(|_| sync_trigger_projection.clone());
                // This unit was already admitted as a whole, ahead of every item. While
                // its items install, an allocation the price did not foresee draws on
                // the reservation the preflight is holding for it, and past that on a
                // bounded excess -- see `MemoryAccountant::try_allocate_for`. The scope
                // is on THIS database's accountant, so it can only spend the budget the
                // unit was priced against. Dropped the moment the items are done.
                let admitted_unit = unit_reservation
                    .filter(|_| !changes.ddl.is_empty())
                    .map(crate::memory_accounting::AdmittedSchemaUnit::enter);
                let ddl_result = (|| -> Result<()> {
                    for (phase_ordinal, ddl) in changes.ddl.clone().into_iter().enumerate() {
                        let source_generation =
                            if let (DdlChange::CreateTable { name, .. }, Some(generations)) = (
                                &ddl,
                                private_replay
                                    .and_then(|context| context.authoritative_generations),
                            ) {
                                if self.table_meta(name).is_none() {
                                    let entry = if let Some(coordinates) = source_coordinates {
                                        let local = coordinates.local(phase_source)?;
                                        generations
                                            .iter()
                                            .filter(|entry| entry.local_arrival.local_lsn == local)
                                            .nth(phase_ordinal)
                                    } else {
                                        generations.get(private_ddl_cursor)
                                    };
                                    entry
                                        .filter(|entry| entry.change == ddl)
                                        .and_then(|entry| entry.generation.table_generation)
                                        .map(|generation| (name.clone(), generation))
                                } else {
                                    None
                                }
                            } else {
                                None
                            };
                        private_ddl_cursor += 1;
                        let graph_transition =
                            self.prepare_private_graph_schema_transition(tx, &ddl)?;
                        let pending_ddl = source_coordinates.map(|_| ddl.clone());
                        let pending_vectors = if source_coordinates.is_some() {
                            self.detach_private_pending_vectors_for_alter(
                                tx,
                                &ddl,
                                vector_copy_owner,
                                read_memory.clone(),
                            )?
                        } else {
                            None
                        };
                        if source_coordinates.is_some() {
                            self.invalidate_private_derived_shape(tx, &ddl);
                        }
                        if source_coordinates.is_some()
                            && let DdlChange::DropTable { name } = &ddl
                        {
                            self.drop_private_pending_vectors(tx, name, vector_copy_owner)?;
                            self.drop_private_pending_graph_rows(tx, name)?;
                            self.drop_private_pending_relational_rows(tx, name)?;
                        }
                        match ddl {
                            DdlChange::CreateTable {
                                name,
                                columns,
                                constraints,
                                foreign_keys,
                                composite_foreign_keys,
                                composite_unique,
                            } => {
                                if self.table_meta(&name).is_some() {
                                    // The planner normally removes refused declarations;
                                    // this boundary also refuses an unplanned mismatch.
                                    if let Some(refusal) = self.arriving_create_table_shape_refusal(
                                        &name,
                                        &columns,
                                        &constraints,
                                        &foreign_keys,
                                        &composite_foreign_keys,
                                        &composite_unique,
                                    ) {
                                        return Err(refusal);
                                    }
                                    // Adopt the arriving declaration's policy for a table
                                    // that already exists here undeclared — the same
                                    // adoption the AlterTable path performs, so a hub does
                                    // not silently keep its own default over an edge's
                                    // declared conflict policy, direction, or retention.
                                    //
                                    // For one of the four engine-owned built-in ledger
                                    // tables this is the byte-identical mutation door
                                    // the AlterTable mirror refuses: adopting an
                                    // arriving policy axis WHOLESALE would let a
                                    // hand-crafted or unguarded
                                    // `CreateTable` land the exact change the
                                    // AlterTable path is refused for, just spelled as
                                    // a create. `engine_owned_merged_policy` computes
                                    // the SAME preserve-or-canonical merge the
                                    // AlterTable arm applies (a silent axis PRESERVES
                                    // the current declared value; an axis the arriving
                                    // shape carries is at this point guaranteed to
                                    // already equal canonical — a differing explicit
                                    // value is refused before apply is ever reached,
                                    // at the preflight
                                    // `refuse_engine_owned_policy_sync_ddl`, which
                                    // judges this exact adopt shape too). `None` for a
                                    // plain operator table falls through to the
                                    // existing verbatim-adoption behavior.
                                    let incoming = rough_sync_table_meta(
                                        &columns,
                                        &constraints,
                                        &foreign_keys,
                                        &composite_foreign_keys,
                                        &composite_unique,
                                    );
                                    if let Some(mut meta) = self.table_meta(&name) {
                                        let engine_owned_merged =
                                            crate::executor::engine_owned_merged_policy(
                                                &name, &meta, &incoming,
                                            );
                                        let (
                                            new_conflict,
                                            new_direction,
                                            new_ttl,
                                            new_sync_safe,
                                            new_unit,
                                            new_history,
                                        ) = match &engine_owned_merged {
                                            Some(merged) => (
                                                merged.conflict_policy,
                                                merged.sync_direction,
                                                merged.default_ttl_seconds,
                                                merged.sync_safe,
                                                merged.retain_declared_unit,
                                                merged.history_policy,
                                            ),
                                            None => (
                                                incoming.conflict_policy,
                                                incoming.sync_direction,
                                                incoming.default_ttl_seconds,
                                                incoming.sync_safe,
                                                incoming.retain_declared_unit,
                                                incoming.history_policy,
                                            ),
                                        };
                                        let new_maintained_counts = if engine_owned_merged.is_none()
                                        {
                                            incoming.maintained_counts.clone()
                                        } else {
                                            meta.maintained_counts.clone()
                                        };
                                        if meta.conflict_policy != new_conflict
                                            || meta.sync_direction != new_direction
                                            || meta.default_ttl_seconds != new_ttl
                                            || meta.sync_safe != new_sync_safe
                                            || meta.retain_declared_unit != new_unit
                                            || meta.history_policy != new_history
                                            || meta.maintained_counts != new_maintained_counts
                                        {
                                            let old_meta = meta.clone();
                                            meta.conflict_policy = new_conflict;
                                            meta.sync_direction = new_direction;
                                            meta.default_ttl_seconds = new_ttl;
                                            meta.sync_safe = new_sync_safe;
                                            meta.retain_declared_unit = new_unit;
                                            meta.history_policy = new_history;
                                            if engine_owned_merged.is_none() {
                                                meta.maintained_counts =
                                                    incoming.maintained_counts.clone();
                                            }
                                            // Refused on the projected post-adoption shape
                                            // before any of it is written — the preflight
                                            // (`refuse_reclaimed_history_sync_ddl`) already
                                            // gated this changeset, but this door also
                                            // installs the SAME field through the adopt
                                            // back door, so it asks the identical question
                                            // again rather than trusting the preflight
                                            // alone (defense in depth: the SAME shape, the
                                            // SAME function, matching the parse-time
                                            // refusal rule of asking at every door
                                            // that can produce the combination).
                                            crate::executor::refuse_reclaimed_history_under_keep_first(
                                            &name, &meta,
                                        )?;
                                            // An arriving MAINTAIN COUNT declaration
                                            // is judged by the SAME door the local
                                            // CREATE and ALTER doors use, so a peer
                                            // can never install and render a
                                            // declaration this build refuses by name.
                                            crate::maintained_counts::validate_maintained_counts_for_meta(
                                        &name, &meta,
                                    )?;
                                            let meta = self
                                                .replace_table_meta_and_refresh_auto_indexes(
                                                    &name, &old_meta, meta,
                                                )?;
                                            self.persist_table_meta(&name, &meta)?;
                                            // Relay the adoption onward: allocate an LSN and
                                            // log an AlterTable so a DIFFERENT edge pulling
                                            // later receives the declared policy/direction/
                                            // retention instead of the hub's older undeclared
                                            // CreateTable. Without this the declaration stops
                                            // at the hub. Mirrors the AlterTable door.
                                            self.allocate_ddl_lsn(|lsn| {
                                                self.log_alter_table_ddl(
                                                    &name,
                                                    &meta,
                                                    lsn,
                                                    read_memory.clone(),
                                                )
                                            })?;
                                            self.clear_statement_cache();
                                            self.reconcile_maintenance_thread();
                                            table_meta_cache.remove(&name);
                                            applied_rows_cache.remove(&name);
                                        }
                                    }
                                    continue;
                                }
                                let sql = sync_create_table_sql(
                                    &name,
                                    &columns,
                                    &constraints,
                                    &foreign_keys,
                                    &composite_foreign_keys,
                                    &composite_unique,
                                );
                                self.execute_in_tx_with_read_memory(
                                    tx,
                                    &sql,
                                    &HashMap::new(),
                                    read_memory.clone(),
                                )?;
                                self.clear_statement_cache();
                                table_meta_cache.remove(&name);
                                applied_rows_cache.remove(&name);
                            }
                            DdlChange::DropTable { name } => {
                                let projected_table_triggers = sync_trigger_projection
                                    .values()
                                    .filter(|declaration| declaration.table == name)
                                    .map(|declaration| declaration.name.clone())
                                    .collect::<Vec<_>>();
                                if !projected_table_triggers.is_empty() {
                                    let current_trigger_names = self
                                        .trigger
                                        .declarations
                                        .lock()
                                        .keys()
                                        .cloned()
                                        .collect::<HashSet<_>>();
                                    for trigger_name in &projected_table_triggers {
                                        let already_queued_drop = trigger_ddl.iter().any(|change| {
                                    matches!(change, DdlChange::DropTrigger { name } if name == trigger_name)
                                });
                                        if !current_trigger_names.contains(trigger_name)
                                            && !already_queued_drop
                                        {
                                            trigger_ddl.push(DdlChange::DropTrigger {
                                                name: trigger_name.clone(),
                                            });
                                        }
                                    }
                                    for trigger_name in projected_table_triggers {
                                        sync_trigger_projection.remove(&trigger_name);
                                    }
                                }
                                let table_had_triggers = self
                                    .trigger
                                    .declarations
                                    .lock()
                                    .values()
                                    .any(|declaration| declaration.table == name);
                                if self.table_meta(&name).is_some() {
                                    if let Some(block) =
                                        crate::executor::rank_policy_drop_table_blocker(self, &name)
                                    {
                                        return Err(block);
                                    }
                                    let bytes_to_release =
                                        crate::executor::estimate_drop_table_bytes(
                                            self,
                                            &name,
                                            vector_copy_owner,
                                            read_memory.clone(),
                                        )?;
                                    let prefix_trigger_ddl = std::mem::take(&mut trigger_ddl);
                                    self.drain_vector_table_maintenance_for_ddl(
                                        &name,
                                        vector_copy_owner,
                                    )?;
                                    let _vector_schema = self.vector_schema_write_table(&name)?;
                                    self.allocate_ddl_lsn(|lsn| {
                                        self.log_drop_table_ddl_and_remove_triggers_with_prefix(
                                            &name,
                                            lsn,
                                            &prefix_trigger_ddl,
                                            read_memory.clone(),
                                        )
                                    })?;
                                    self.accountant().release(bytes_to_release);
                                    self.clear_statement_cache();
                                } else if table_had_triggers {
                                    let prefix_trigger_ddl = std::mem::take(&mut trigger_ddl);
                                    self.drain_vector_table_maintenance_for_ddl(
                                        &name,
                                        vector_copy_owner,
                                    )?;
                                    let _vector_schema = self.vector_schema_write_table(&name)?;
                                    self.allocate_ddl_lsn(|lsn| {
                                        self.log_drop_table_ddl_and_remove_triggers_with_prefix(
                                            &name,
                                            lsn,
                                            &prefix_trigger_ddl,
                                            read_memory.clone(),
                                        )
                                    })?;
                                    self.clear_statement_cache();
                                } else if !group_has_data && !trigger_ddl.is_empty() {
                                    self.apply_trigger_ddl_batch(
                                        std::mem::take(&mut trigger_ddl),
                                        read_memory.clone(),
                                    )?;
                                }
                                table_meta_cache.remove(&name);
                                applied_rows_cache.remove(&name);
                            }
                            DdlChange::AlterTable {
                                name,
                                columns,
                                constraints,
                                foreign_keys,
                                composite_foreign_keys,
                                composite_unique,
                            } => {
                                if self.table_meta(&name).is_none() {
                                    continue;
                                }
                                let existing = self.table_meta(&name).unwrap_or_default();
                                let incoming = rough_sync_table_meta(
                                    &columns,
                                    &constraints,
                                    &foreign_keys,
                                    &composite_foreign_keys,
                                    &composite_unique,
                                );
                                let existing_vector_columns = Self::sync_vector_columns(&existing);
                                let incoming_vector_columns = Self::sync_vector_columns(&incoming);
                                let existing_column_names = Self::sync_meta_column_names(&existing);
                                let vector_shape_changed = Self::sync_alter_vector_shape_changed(
                                    &existing,
                                    &incoming,
                                    &constraints,
                                );
                                if vector_shape_changed {
                                    // Only a vector column its author dropped or
                                    // renamed away leaves; an omitted one is kept.
                                    let removed_vector_columns =
                                        Self::sync_marked_removed_vector_columns(
                                            &existing,
                                            &incoming,
                                            &constraints,
                                        );
                                    let renamed_vector_column = Self::sync_marked_vector_rename(
                                        &existing,
                                        &incoming,
                                        &constraints,
                                    );
                                    let changed_existing_columns = existing_vector_columns
                                        .iter()
                                        .filter(|(column, shape)| {
                                            incoming_vector_columns.get(*column).is_some_and(
                                                |incoming_shape| incoming_shape != *shape,
                                            )
                                        })
                                        .map(|(column, _)| column.clone())
                                        .collect::<Vec<_>>();
                                    for column in removed_vector_columns
                                        .iter()
                                        .chain(changed_existing_columns.iter())
                                    {
                                        self.drain_vector_index_maintenance_for_ddl(
                                            &VectorIndexRef::new(&name, column.clone()),
                                            vector_copy_owner,
                                        )?;
                                    }
                                    let vector_schema_refs = existing_vector_columns
                                        .keys()
                                        .chain(incoming_vector_columns.keys())
                                        .map(|column| VectorIndexRef::new(&name, column.clone()));
                                    let _vector_schema =
                                        self.vector_schema_write_many(vector_schema_refs)?;
                                    self.allocate_ddl_lsn(|lsn| {
                                let store = self.relational_store();
                                let meta = Self::merged_sync_full_shape_vector_alter_meta(
                                    &name,
                                    &existing,
                                    incoming.clone(),
                                    &constraints,
                                );
                                if let Some((from, to)) = &renamed_vector_column {
                                    store
                                        .alter_table_rename_column(&name, from, to)
                                        .map_err(|error| Error::Other(error.into()))?;
                                }
                                for column in &removed_vector_columns {
                                    if renamed_vector_column
                                        .as_ref()
                                        .is_some_and(|(from, _)| from == column)
                                    {
                                        continue;
                                    }
                                    store
                                        .alter_table_drop_column(&name, column)
                                        .map_err(|error| Error::Other(error.into()))?;
                                }
                                for column in &changed_existing_columns {
                                    store
                                        .alter_table_drop_column(&name, column)
                                        .map_err(|error| Error::Other(error.into()))?;
                                }

                                let meta = self.replace_table_meta_and_refresh_auto_indexes(
                                    &name, &existing, meta,
                                )?;

                                for column in &removed_vector_columns {
                                    if renamed_vector_column
                                        .as_ref()
                                        .is_some_and(|(from, _)| from == column)
                                    {
                                        continue;
                                    }
                                    self.deregister_vector_index(&name, column);
                                }
                                for column in &changed_existing_columns {
                                    self.deregister_vector_index(&name, column);
                                }
                                for column in &meta.columns {
                                    if matches!(column.column_type, ColumnType::Vector(_)) {
                                        if let Some((from, to)) = &renamed_vector_column
                                            && column.name == *to
                                            && self.vector_store.contains_index(
                                                &VectorIndexRef::new(&name, from),
                                            )
                                        {
                                            // The rename retains the source state. Apply the
                                            // arriving policy there so registration validates
                                            // its actual retained partitions before preparation.
                                            self.register_vector_index_for_column_with_identity(
                                                &name,
                                                from,
                                                column,
                                                vector_copy_owner,
                                                read_memory.clone(),
                                            )?;
                                        } else {
                                            self.register_vector_index_for_column(&name, column, vector_copy_owner, read_memory.clone())?;
                                        }
                                    }
                                }
                                // This node's own logged ALTER carries the
                                // author's markers onward, so a peer pulling
                                // from here drops or renames exactly what the
                                // author did and nothing it merely omitted.
                                let mut markers = renamed_vector_column
                                    .iter()
                                    .map(|(from, to)| sync_vector_rename_constraint(from, to))
                                    .collect::<Vec<_>>();
                                markers.extend(
                                    removed_vector_columns
                                        .iter()
                                        .filter(|column| {
                                            renamed_vector_column
                                                .as_ref()
                                                .is_none_or(|(from, _)| from != *column)
                                        })
                                        .map(|column| sync_column_drop_constraint(column)),
                                );
                                // Prepare after every other vector-store operation. The
                                // token holds the registry gates until durable refusal
                                // drops it or successful publication consumes it.
                                let prepared_vector_rename = if let Some((from, to)) =
                                    &renamed_vector_column
                                    && self.vector_store.contains_index(
                                        &VectorIndexRef::new(&name, from),
                                    )
                                {
                                    let old = VectorIndexRef::new(&name, from);
                                    let new = VectorIndexRef::new(&name, to);
                                    let replaces_arriving_index =
                                        !existing_column_names.contains(to)
                                            && self.vector_store.contains_index(&new);
                                    let owner = crate::vector_read_memory::admission(
                                        read_memory.clone(),
                                    );
                                    Some(if replaces_arriving_index {
                                        // The destination is the arriving layout registered
                                        // for this received unit, not an existing column.
                                        // Preparation preserves it on any refusal.
                                        self.prepare_vector_index_replacement_rename(
                                            &old, &new, &new, owner,
                                        )?
                                    } else {
                                        self.prepare_vector_index_rename(&old, &new, owner)?
                                    })
                                } else {
                                    None
                                };
                                self.persist_table_meta_rows_vectors_and_log_alter_table_ddl_with_markers(
                                    &name, &meta, markers, lsn,
                                read_memory.clone(),)?;
                                if let Some(prepared) = prepared_vector_rename {
                                    let receipt = prepared.publish();
                                    self.finalization.accountant.release(receipt.legacy_live_raw_bytes);
                                }
                                Ok(())
                            })?;
                                } else {
                                    let incoming_column_names =
                                        Self::sync_meta_column_names(&incoming);
                                    // A drop happens only on the author's own
                                    // evidence: the `COLUMN_DROP(col)` marker the
                                    // author's `DROP COLUMN` signs into the ALTER. A
                                    // column the ALTER merely leaves out -- with or
                                    // without the primary key restated -- is kept with
                                    // its data, locally and on every receiver. The
                                    // owner ruled this (2026-09-27): partial
                                    // alterations preserve omitted columns, and a
                                    // receiver never infers a deletion from omission.
                                    let authored_drops =
                                        sync_column_drops_from_constraints(&constraints);
                                    let dropped_columns = existing
                                        .columns
                                        .iter()
                                        .filter(|column| {
                                            !matches!(column.column_type, ColumnType::Vector(_))
                                                && authored_drops.contains(&column.name)
                                                && !incoming_column_names.contains(&column.name)
                                        })
                                        .cloned()
                                        .collect::<Vec<_>>();
                                    let added_columns = incoming
                                        .columns
                                        .iter()
                                        .filter(|column| {
                                            !matches!(column.column_type, ColumnType::Vector(_))
                                                && !existing_column_names.contains(&column.name)
                                        })
                                        .collect::<Vec<_>>();
                                    // A rename happens only on the author's own
                                    // evidence: the `COLUMN_RENAME(from,to)` marker in
                                    // the signed constraints, naming a column this
                                    // receiver holds and the author's shape omits, and
                                    // a new name the author's shape adds.
                                    // Authenticated received DDL replays exactly as
                                    // authored, a receiver "may not add, remove,
                                    // replace, or reorder authenticated DDL", and a
                                    // wiped edge restores "the schema as authored,
                                    // never a receiver-rewritten approximation". Two
                                    // columns that merely share a type are no such
                                    // evidence -- on a divergent peer that guess
                                    // would carry the receiver's own values under a
                                    // name the author never gave them.
                                    let authored_renames =
                                        sync_column_renames_from_constraints(&constraints)
                                            .into_iter()
                                            .filter(|(from, to)| {
                                                existing.columns.iter().any(|column| {
                                                    column.name == *from
                                                        && !matches!(
                                                            column.column_type,
                                                            ColumnType::Vector(_)
                                                        )
                                                }) && !incoming_column_names.contains(from)
                                                    && added_columns
                                                        .iter()
                                                        .any(|column| column.name == *to)
                                            })
                                            .collect::<Vec<_>>();
                                    for (from, to) in &authored_renames {
                                        let sql = format!(
                                            "ALTER TABLE {} RENAME COLUMN {} TO {}",
                                            name, from, to
                                        );
                                        self.execute_in_tx_with_read_memory(
                                            tx,
                                            &sql,
                                            &HashMap::new(),
                                            read_memory.clone(),
                                        )?;
                                    }
                                    for column in &dropped_columns {
                                        if authored_renames
                                            .iter()
                                            .any(|(from, _)| *from == column.name)
                                        {
                                            continue;
                                        }
                                        let sql = format!(
                                            "ALTER TABLE {} DROP COLUMN {}",
                                            name, column.name
                                        );
                                        self.execute_in_tx_with_read_memory(
                                            tx,
                                            &sql,
                                            &HashMap::new(),
                                            read_memory.clone(),
                                        )?;
                                    }
                                    for (col, ty) in &columns {
                                        if existing_column_names.contains(col.as_str()) {
                                            continue;
                                        }
                                        if authored_renames.iter().any(|(_, to)| to == col) {
                                            continue;
                                        }
                                        let sql = format!(
                                            "ALTER TABLE {} ADD COLUMN {} {}",
                                            name,
                                            col,
                                            sync_column_type_with_foreign_key(
                                                col,
                                                ty,
                                                &foreign_keys
                                            )
                                        );
                                        self.execute_in_tx_with_read_memory(
                                            tx,
                                            &sql,
                                            &HashMap::new(),
                                            read_memory.clone(),
                                        )?;
                                    }
                                }
                                // The preceding DDL changed this private working
                                // store. Its captured read descriptor still names
                                // the predecessor and cannot supply the policy merge.
                                let altered_meta = if vector_shape_changed {
                                    None
                                } else if private_replay.is_some() {
                                    self.relational_store.table_meta(&name)
                                } else {
                                    self.table_meta(&name)
                                };
                                if !vector_shape_changed && let Some(mut meta) = altered_meta {
                                    let old_meta = meta.clone();
                                    // Computed BEFORE `incoming`'s column/constraint
                                    // fields are moved out below -- `engine_owned_
                                    // merged_policy` only reads `incoming`'s policy
                                    // fields, but borrowing the whole struct after a
                                    // partial move is illegal, so this is captured
                                    // while `incoming` is still whole. `Some` only
                                    // for one of the four engine-owned built-in
                                    // ledger tables; `None` (a plain operator table)
                                    // falls through to the existing verbatim-adoption
                                    // behavior below.
                                    let engine_owned_merged =
                                        crate::executor::engine_owned_merged_policy(
                                            &name, &old_meta, &incoming,
                                        );
                                    for incoming_column in incoming.columns {
                                        if let Some(existing_column) = meta
                                            .columns
                                            .iter_mut()
                                            .find(|column| column.name == incoming_column.name)
                                        {
                                            merge_sync_alter_existing_column(
                                                existing_column,
                                                incoming_column,
                                            );
                                        }
                                    }
                                    if !incoming.unique_constraints.is_empty() {
                                        meta.unique_constraints = incoming.unique_constraints;
                                    }
                                    if !incoming.composite_foreign_keys.is_empty() {
                                        meta.composite_foreign_keys =
                                            incoming.composite_foreign_keys;
                                    }
                                    // Retention, direction, conflict, and history ride
                                    // the full constraint set every AlterTable emitter
                                    // carries, whether or not that specific axis
                                    // changed -- so for a PLAIN operator table the
                                    // receiver takes the writer's post-alter policy
                                    // verbatim: a changed direction, a newly declared
                                    // window, or a dropped one. For one of the four
                                    // engine-owned built-in ledger tables that verbatim
                                    // adoption is exactly the defect this closes: a
                                    // wire shape SILENT on an axis (a dropped RETAIN,
                                    // or a half-healed peer's own in-progress reconcile
                                    // that has not reached this axis yet) must PRESERVE
                                    // the table's current declared value rather than
                                    // clear it, and an axis it DOES carry is at this
                                    // point guaranteed to already equal the canonical
                                    // value -- a differing explicit value is refused
                                    // before apply is ever reached, at the preflight
                                    // (`refuse_engine_owned_policy_sync_ddl`).
                                    // `engine_owned_merged_policy` is the SAME merge
                                    // the CreateTable adopt branch performs, so both
                                    // doors land on the identical shape.
                                    if let Some(merged) = engine_owned_merged {
                                        meta.default_ttl_seconds = merged.default_ttl_seconds;
                                        meta.sync_safe = merged.sync_safe;
                                        meta.retain_declared_unit = merged.retain_declared_unit;
                                        meta.sync_direction = merged.sync_direction;
                                        meta.conflict_policy = merged.conflict_policy;
                                        meta.history_policy = merged.history_policy;
                                    } else {
                                        meta.default_ttl_seconds = incoming.default_ttl_seconds;
                                        meta.sync_safe = incoming.sync_safe;
                                        meta.retain_declared_unit = incoming.retain_declared_unit;
                                        meta.sync_direction = incoming.sync_direction;
                                        meta.conflict_policy = incoming.conflict_policy;
                                        meta.history_policy = incoming.history_policy;
                                        // The maintained-count declaration rides the
                                        // same complete constraint set, so an ALTER
                                        // that declares, changes or clears it reaches
                                        // the receiver instead of stopping at the
                                        // author.
                                        meta.maintained_counts = incoming.maintained_counts.clone();
                                    }
                                    // The same refusal every local door applies runs
                                    // here too, before any of the projected shape is
                                    // written — defense in depth alongside the
                                    // preflight (`refuse_reclaimed_history_sync_ddl`).
                                    crate::executor::refuse_reclaimed_history_under_keep_first(
                                        &name, &meta,
                                    )?;
                                    // Same door as the CreateTable adopt arm above and
                                    // as every local door.
                                    crate::maintained_counts::validate_maintained_counts_for_meta(
                                        &name, &meta,
                                    )?;
                                    for column in &meta.columns {
                                        if matches!(column.column_type, ColumnType::Vector(_)) {
                                            self.validate_vector_layout_policy_for_ddl(
                                                &VectorIndexRef::new(&name, &column.name),
                                                column,
                                                vector_copy_owner,
                                                read_memory.clone(),
                                            )?;
                                        }
                                    }
                                    let meta = self.replace_table_meta_and_refresh_auto_indexes(
                                        &name, &old_meta, meta,
                                    )?;
                                    for column in &meta.columns {
                                        if matches!(column.column_type, ColumnType::Vector(_)) {
                                            self.register_vector_index_for_column(
                                                &name,
                                                column,
                                                vector_copy_owner,
                                                read_memory.clone(),
                                            )?;
                                        }
                                    }
                                    self.persist_table_meta(&name, &meta)?;
                                    // The shipped defect this closes: an arriving
                                    // AlterTable never called this, so a RETAIN (or
                                    // now HISTORY CURRENT ONLY) declaration arriving
                                    // over sync on an existing table did not arm
                                    // maintenance until the next reopen.
                                    self.reconcile_maintenance_thread();
                                }
                                self.clear_statement_cache();
                                table_meta_cache.remove(&name);
                                applied_rows_cache.remove(&name);
                            }
                            DdlChange::CreateIndex {
                                table,
                                name,
                                columns,
                            } => {
                                // Apply at the receiver: write IndexDecl into
                                // TableMeta.indexes, register storage, rebuild over
                                // locally-resident rows. Emit a matching DDL log entry.
                                // Silently skipping on missing table would hide sync
                                // divergence; surface it as TableNotFound so the caller
                                // can see which index couldn't land.
                                if self.table_meta(&table).is_none() {
                                    return Err(Error::TableNotFound(table.clone()));
                                }
                                let already = self
                                    .table_meta(&table)
                                    .map(|m| m.indexes.iter().any(|i| i.name == name))
                                    .unwrap_or(false);
                                if !already {
                                    // The postings this index is about to build over
                                    // every row already in the table are charged
                                    // before any of them exists, so a limit that
                                    // cannot hold them refuses the arriving DDL
                                    // instead of admitting it and drifting.
                                    let old_meta = self
                                        .table_meta(&table)
                                        .ok_or_else(|| Error::TableNotFound(table.clone()))?;
                                    let mut projected_meta = old_meta.clone();
                                    projected_meta.indexes.push(contextdb_core::IndexDecl {
                                        name: name.clone(),
                                        columns: columns.clone(),
                                        kind: contextdb_core::IndexKind::UserDeclared,
                                    });
                                    let mut index_set_memory = self
                                        .prepare_index_set_change_memory(
                                            &table,
                                            &old_meta,
                                            &projected_meta,
                                        )?;
                                    // Past this point the index set has changed in
                                    // memory, and what building the index turns out to
                                    // need is only learned as it is built -- so this is
                                    // where an arriving index that costs more than it
                                    // was priced at runs out of the room its unit was
                                    // allowed. The refusal returns out of the whole DDL
                                    // closure, which puts every reshaped table's old
                                    // shape back before it answers.
                                    {
                                        let store = self.relational_store();
                                        let mut metas = store.table_meta.write();
                                        if let Some(m) = metas.get_mut(&table) {
                                            m.indexes.push(contextdb_core::IndexDecl {
                                                name: name.clone(),
                                                columns: columns.clone(),
                                                kind: contextdb_core::IndexKind::UserDeclared,
                                            });
                                        }
                                    }
                                    self.relational_store().create_index_storage(
                                        &table,
                                        &name,
                                        columns.clone(),
                                    );
                                    self.relational_store().rebuild_index(&table, &name);
                                    index_set_memory.commit_after_swap();
                                    if let Some(table_meta) = self.table_meta(&table) {
                                        self.persist_table_meta(&table, &table_meta)?;
                                    }
                                    self.allocate_ddl_lsn(|lsn| {
                                        self.log_create_index_ddl(
                                            &table,
                                            &name,
                                            &columns,
                                            lsn,
                                            read_memory.clone(),
                                        )
                                    })?;
                                    self.clear_statement_cache();
                                }
                                table_meta_cache.remove(&table);
                            }
                            DdlChange::DropIndex { table, name } => {
                                if self.table_meta(&table).is_some() {
                                    let exists = self
                                        .table_meta(&table)
                                        .map(|m| m.indexes.iter().any(|i| i.name == name))
                                        .unwrap_or(false);
                                    if exists {
                                        if let Some(prefix) =
                                            crate::executor::reserved_index_prefix(&name)
                                        {
                                            return Err(Error::ReservedIndexName {
                                                table: table.clone(),
                                                name: name.clone(),
                                                prefix: prefix.to_string(),
                                            });
                                        }
                                        if let Some(block) =
                                            crate::executor::rank_policy_drop_index_blocker(
                                                self, &table, &name,
                                            )
                                        {
                                            return Err(block);
                                        }
                                        let old_meta = self
                                            .table_meta(&table)
                                            .ok_or_else(|| Error::TableNotFound(table.clone()))?;
                                        let mut projected_meta = old_meta.clone();
                                        projected_meta.indexes.retain(|i| i.name != name);
                                        // A negative delta: the rows give back the
                                        // posting this index held for each of them.
                                        let mut index_set_memory = self
                                            .prepare_index_set_change_memory(
                                                &table,
                                                &old_meta,
                                                &projected_meta,
                                            )?;
                                        {
                                            let store = self.relational_store();
                                            let mut metas = store.table_meta.write();
                                            if let Some(m) = metas.get_mut(&table) {
                                                m.indexes.retain(|i| i.name != name);
                                            }
                                        }
                                        self.relational_store().drop_index_storage(&table, &name);
                                        index_set_memory.commit_after_swap();
                                        if let Some(table_meta) = self.table_meta(&table) {
                                            self.persist_table_meta(&table, &table_meta)?;
                                        }
                                        self.allocate_ddl_lsn(|lsn| {
                                            self.log_drop_index_ddl(
                                                &table,
                                                &name,
                                                lsn,
                                                read_memory.clone(),
                                            )
                                        })?;
                                        self.clear_statement_cache();
                                    }
                                }
                                table_meta_cache.remove(&table);
                            }
                            trigger_change @ (DdlChange::CreateTrigger { .. }
                            | DdlChange::CreateTriggerIncludingSync { .. }
                            | DdlChange::DropTrigger { .. }) => {
                                let trigger_table = match &trigger_change {
                                    DdlChange::CreateTrigger { table, .. }
                                    | DdlChange::CreateTriggerIncludingSync { table, .. } => {
                                        Some(table.clone())
                                    }
                                    DdlChange::DropTrigger { .. } => None,
                                    _ => None,
                                };
                                self.require_admin_trigger_ddl(match &trigger_change {
                                    DdlChange::CreateTrigger { .. }
                                    | DdlChange::CreateTriggerIncludingSync { .. } => {
                                        "apply_changes CREATE TRIGGER"
                                    }
                                    DdlChange::DropTrigger { .. } => "apply_changes DROP TRIGGER",
                                    _ => "apply_changes trigger DDL",
                                })?;
                                let projected_table_meta = self.relational_store.table_meta.read();
                                self.apply_sync_trigger_ddl_to_projection(
                                    &mut sync_trigger_projection,
                                    &trigger_change,
                                    &projected_table_meta,
                                )?;
                                trigger_ddl.push(trigger_change);
                                if let Some(table) = trigger_table {
                                    table_meta_cache.remove(&table);
                                    applied_rows_cache.remove(&table);
                                }
                            }
                            event_ddl @ (DdlChange::CreateEventType { .. }
                            | DdlChange::CreateSink { .. }
                            | DdlChange::CreateRoute { .. }
                            | DdlChange::DropRoute { .. }) => {
                                event_bus_ddl.push(event_ddl);
                            }
                        }
                        if let Some((table, generation)) = source_generation {
                            let mut generations = self.in_memory_table_generations.lock();
                            let current = generations.get_mut(&table).ok_or_else(|| {
                                Error::Other(
                                    "private CREATE did not install its table generation".into(),
                                )
                            })?;
                            *current = generation;
                        }
                        if let Some(ddl) = pending_ddl {
                            self.alter_private_pending_relational_rows(
                                tx,
                                &ddl,
                                pending_vectors.as_ref().map(|plan| &plan.previous),
                                read_memory.clone(),
                            )?;
                            if let Some(plan) = pending_vectors {
                                self.restore_private_pending_vectors_after_alter(tx, plan)?;
                            }
                            self.finish_private_graph_schema_transition(tx, graph_transition)?;
                            let mut definitions = self.received_event_bus_projection();
                            self.apply_received_event_bus_ddl_to_projection(
                                &mut definitions,
                                &ddl,
                                &self.relational_store.table_meta.read(),
                            )?;
                            self.seed_received_event_bus_projection(definitions);
                        }
                    }
                    event_bus_ddl.retain(|ddl| {
                        !Self::skip_event_bus_ddl_for_dropped_table(ddl, &dropped_tables, |table| {
                            self.table_meta(table).is_some()
                        })
                    });
                    Ok(())
                })();
                drop(admitted_unit);
                if let Err(err) = ddl_result {
                    let _ = self.rollback(tx);
                    return Err(err);
                }
                if let Some(credit) = applied_cache_credit.as_mut() {
                    applied_rows_cache.clear();
                    self.tx_mgr.with_ordinary_read(tx, |ws| -> Result<()> {
                    let bytes = ws.relational_inserts.iter().try_fold(0usize, |bytes, (table, row)| {
                        Ok::<_, Error>(bytes.saturating_add(crate::memory_accounting::preparation_row_copy_bytes(row)?)
                            .saturating_add(table.len()).saturating_add(
                                std::mem::size_of::<VersionedRow>().saturating_mul(4).saturating_add(512)))
                    })?;
                    if bytes > credit.bytes() {
                        credit.try_grow_for(bytes - credit.bytes(), "sync", "connected_pending_row_cache",
                            "Raise the declared memory limit to prepare this unit's pending rows.")?;
                    }
                    for (table, row) in &ws.relational_inserts {
                        let copied = crate::memory_accounting::preparation_copy_row(row)?;
                        if let Some(rows) = applied_rows_cache.get_mut(table) { rows.push(copied); }
                        else { applied_rows_cache.insert(table.clone(), vec![copied]); }
                    }
                    Ok(())
                })??;
                }
                let phase_scope = source_coordinates.map(|_| {
                    connected_replay::PrivatePhaseScope::enter(
                        self,
                        tx,
                        private_preparation_owner.clone(),
                    )
                });
                let fresh_creator_deletes = match self.authenticated_fresh_creator_deletes(
                    &changes.rows,
                    arrivals,
                    receipt.as_ref(),
                    lineages,
                    read_memory.clone(),
                ) {
                    Ok(permitted) => permitted,
                    Err(err) => {
                        let _ = self.rollback(tx);
                        return Err(err);
                    }
                };
                let projected_sync_apply = match self
                    .projected_sync_incoming_values_for_applied_rows(
                        &changes.rows,
                        policies,
                        arrivals,
                        adoption,
                        &fresh_creator_deletes,
                        read_memory.clone(),
                    ) {
                    Ok(projection) => projection,
                    Err(err) => {
                        let _ = self.rollback(tx);
                        return Err(err);
                    }
                };
                let vector_tables = changes
                    .vectors
                    .iter()
                    .map(|vector| vector.index.table.clone())
                    .collect::<HashSet<_>>();
                let mut rows = changes.rows;
                rows.sort_by(|left, right| {
                    left.lsn.cmp(&right.lsn).then_with(|| {
                        if vector_tables.contains(&left.table)
                            || vector_tables.contains(&right.table)
                        {
                            std::cmp::Ordering::Equal
                        } else {
                            right.deleted.cmp(&left.deleted)
                        }
                    })
                });
                // A vector that arrived from a peer names its owner by the SENDER's
                // row number, which addresses that node's storage and not this one's;
                // the owner is identified here by pairing the vector's group with the
                // row it was served beside. Such a page can carry groups whose row is
                // absent from this apply — a re-read of a hub's history serves each
                // row once at its current version while the vectors it wrote along the
                // way all arrive. Those groups have no owner this node can name, so
                // they are set aside and reported as skipped rather than guessed at.
                //
                // A locally-applied changeset is the opposite case: its row numbers
                // are this node's own, so they are used as given.
                let vectors_carry_sender_row_numbering = stage_verified_lineages;
                if vectors_carry_sender_row_numbering {
                    let (owned_vectors, unowned_vector_groups) =
                        partition_owned_vector_groups(changes.vectors, &rows);
                    changes.vectors = owned_vectors;
                    result.skipped_rows += unowned_vector_groups;
                }
                // Only these rows receive terminal diagnostics on an ordinary
                // authenticated push. No-op siblings and successfully applied siblings
                // remain receipt-confirmed without being mislabeled as losers.
                for row in rows {
                    // Discarded source lookups and permission probes end with the entry.
                    let _row_outputs = self.writer_output_scope();
                    if row.values.is_empty() {
                        if receipt.is_some() {
                            let _ = self.rollback(tx);
                            return Err(Error::SyncError(
                                ("authenticated sync row omitted its values".to_string()).into(),
                            ));
                        }
                        result.skipped_rows += 1;
                        if commit_each_row {
                            self.commit_with_source(
                                tx,
                                CommitSource::SyncPull,
                                vector_copy_owner,
                                read_memory.clone(),
                            )?;
                            tx = self.begin()?;
                        }
                        continue;
                    }

                    // Resolve by the table's DECLARED policy (carried on its meta),
                    // honoring what the application wrote instead of only the uniform
                    // default the hub was handed — the same resolution the projection
                    // paths use.
                    let row_meta = cached_table_meta(self, &mut table_meta_cache, &row.table);
                    let mut policy = Self::sync_conflict_policy_for_table(
                        policies,
                        row_meta.as_ref(),
                        &row.table,
                    );
                    // A missing receipt is NOT proof of a pull: the hub's keyless
                    // push also applies receipt-less, and letting it adopt would hand
                    // an unidentified pusher the power to overwrite the hub's
                    // first-accepted value. The explicit apply role is the
                    // discriminator; declared-direction gating below decides adoption.
                    let pull_only_inbound = receipt.is_none()
                        && apply_role == SyncApplyRole::PullLeg
                        && row_meta.as_ref().is_some_and(|meta| {
                            matches!(
                                crate::executor::effective_sync_direction(meta),
                                SyncDirection::Pull
                            )
                        });
                    if pull_only_inbound {
                        // A pull-only declaration gives the hub the sole writable
                        // copy. Its inbound row or tombstone therefore replaces local
                        // state regardless of the table's ordinary two-way policy.
                        policy = ConflictPolicy::EdgeWins;
                    }
                    // KEEP FIRST names the value the fleet accepted first. Arriving
                    // over a declared pull leg, that is the hub's, so the edge adopts
                    // it and reports the value it gave up. A declaration with NO pull
                    // leg — push-only, or sync off — never adopts: there the local
                    // value is terminal and an inbound row is refused as before.
                    let inbound_over_declared_pull_leg = receipt.is_none()
                        && apply_role == SyncApplyRole::PullLeg
                        && row_meta.as_ref().is_some_and(|meta| {
                            matches!(
                                crate::executor::effective_sync_direction(meta),
                                SyncDirection::Pull | SyncDirection::Both
                            )
                        });

                    let row_has_vector = row_meta.as_ref().is_some_and(|meta| {
                        meta.columns
                            .iter()
                            .any(|col| matches!(col.column_type, ColumnType::Vector(_)))
                    });
                    if !row.deleted
                        && let Some(meta) =
                            cached_table_meta(self, &mut table_meta_cache, &row.table)
                        && !self.sync_incoming_values_allowed_for_access(
                            &row.table,
                            &meta,
                            &row.values,
                            row.lsn,
                            read_memory.clone(),
                        )?
                    {
                        if receipt.is_some() {
                            let _ = self.rollback(tx);
                            return Err(Error::SyncError(
                                ("sync access denial cannot confirm a terminal refusal"
                                    .to_string())
                                .into(),
                            ));
                        }
                        result.skipped_rows += 1;
                        let row_has_vector = meta
                            .columns
                            .iter()
                            .any(|col| matches!(col.column_type, ColumnType::Vector(_)));
                        if row_has_vector
                            && next_vector_row_group_matches(&changes.vectors, vector_row_idx, &row)
                        {
                            consume_failed_vector_row_group(
                                &changes.vectors,
                                &mut vector_row_idx,
                                &mut failed_vector_groups,
                            );
                        }
                        // A receiver that cannot see a row cannot name a winner without
                        // leaking that row. Pulls retain the private skipped result;
                        // authenticated pushes above remain pending rather than emit an
                        // incomplete terminal diagnostic.
                        if commit_each_row {
                            self.commit_with_source(
                                tx,
                                CommitSource::SyncPull,
                                vector_copy_owner,
                                read_memory.clone(),
                            )?;
                            tx = self.begin()?;
                        }
                        continue;
                    }

                    let skip_deleted = applied_deleted_committed_row_ids
                        .get(&row.table)
                        .cloned()
                        .unwrap_or_default();
                    let existing = match sync_visible_point_lookup(
                        self,
                        &applied_rows_cache,
                        &row.table,
                        &row.natural_key,
                        self.snapshot(),
                        &skip_deleted,
                        read_memory.clone(),
                    ) {
                        Ok(existing) => existing,
                        Err(err) => {
                            let _ = self.rollback(tx);
                            return Err(err);
                        }
                    };
                    let is_delete = row.deleted;
                    let terminal_refusal_override = match terminal_refusal_context {
                        Some(context) => self.terminal_refusal_marker_matches(
                            context,
                            &row,
                            existing.as_ref(),
                            read_memory.clone(),
                        )?,
                        None => false,
                    };
                    if terminal_refusal_override {
                        terminal_marker_clears.push((row.table.clone(), row.natural_key.clone()));
                        policy = ConflictPolicy::EdgeWins;
                    }

                    if !pull_only_inbound
                        && adoption == SyncAdoption::Continuing
                        && existing
                            .as_ref()
                            .map(|local| {
                                self.relational_sync_source_at_read_root(
                                    &row.table,
                                    local.row_id,
                                    read_memory.clone(),
                                )
                            })
                            .transpose()?
                            .is_some_and(|(_, kind)| {
                                matches!(
                            kind,
                            Some(contextdb_relational::store::SyncSourceKind::AcceptedLocalPending)
                        )
                            })
                    {
                        // This source was restored before it ordered the local row.
                        // Until resend receives a fresh hub acknowledgement, none of
                        // its pulls may overwrite (or delete) the pending local work.
                        continue;
                    }

                    if !pull_only_inbound
                        && !is_delete
                        && existing.is_none()
                        && self.incoming_row_is_stale_against_sync_tombstone(
                            &row, arrivals, adoption,
                        )?
                    {
                        // An old hub copy can re-offer a row from before this edge's
                        // acknowledged local delete. The delete remains local work
                        // (AcceptedLocal is never outbound-suppressed), so a later
                        // push repairs that hub instead of reviving its stale row.
                        continue;
                    }

                    if is_delete {
                        if let Some(local) = existing {
                            let incoming_arrival = Self::resolve_incoming_arrival(&row, arrivals);
                            let fresh_creator_permission =
                                Self::authenticated_fresh_creator_delete_permission(
                                    &row,
                                    &local,
                                    policy,
                                    &fresh_creator_deletes,
                                );
                            let applies = terminal_refusal_override
                                || fresh_creator_permission.is_some()
                                || self.sync_row_applies_over_committed(
                                    &row,
                                    &local,
                                    policy,
                                    incoming_arrival,
                                    adoption,
                                    read_memory.clone(),
                                )?;
                            if !applies {
                                if matches!(
                                    policy,
                                    ConflictPolicy::InsertIfNotExists | ConflictPolicy::ServerWins
                                ) && let Err(err) = self
                                    .stage_authoritative_row_after_sync_refusal(
                                        tx,
                                        &row.table,
                                        &local,
                                        vector_copy_owner,
                                        read_memory.clone(),
                                    )
                                {
                                    let _ = self.rollback(tx);
                                    return Err(err);
                                }
                                // A rejected delete must leave both the relational row
                                // and any vector it owns intact. The vector change is
                                // tied to this remote owner, so mark that owner failed
                                // rather than mapping it to the local row for deletion.
                                if row_has_vector
                                    && let Some(remote_row_id) = changes
                                        .vectors
                                        .get(vector_row_idx)
                                        .map(|vector| vector.row_id)
                                    && changes.vectors.iter().any(|vector| {
                                        vector.row_id == remote_row_id
                                            && vector.vector.is_empty()
                                            && vector.index.table == row.table
                                            && vector.lsn == row.lsn
                                    })
                                {
                                    consume_failed_vector_row_group(
                                        &changes.vectors,
                                        &mut vector_row_idx,
                                        &mut failed_vector_groups,
                                    );
                                }
                                match policy {
                                    ConflictPolicy::LatestWins => {}
                                    ConflictPolicy::InsertIfNotExists => {
                                        result.skipped_rows += 1;
                                        semantic_refused_rows.push(row.clone());
                                    }
                                    ConflictPolicy::ServerWins => {
                                        result.skipped_rows += 1;
                                        semantic_refused_rows.push(row.clone());
                                        result.conflicts.push(Conflict {
                                            natural_key: row.natural_key.clone(),
                                            resolution: policy,
                                            reason: Some("server_wins".to_string()),
                                            table: None,
                                            mutation_kind: None,
                                            winning_author_node_id: None,
                                            hub_acceptance_position: None,
                                            refusal_cause: None,
                                        });
                                    }
                                    ConflictPolicy::EdgeWins => {
                                        unreachable!("edge wins always applies")
                                    }
                                }
                                if commit_each_row {
                                    self.commit_with_source(
                                        tx,
                                        CommitSource::SyncPull,
                                        vector_copy_owner,
                                        read_memory.clone(),
                                    )?;
                                    tx = self.begin()?;
                                }
                                continue;
                            }
                            if let Some(permission) = fresh_creator_permission
                                && let Err(err) = self.record_sync_lineage_guard(tx, permission)
                            {
                                let _ = self.rollback(tx);
                                return Err(err);
                            }
                            if row_has_vector {
                                // The cursor identifies one remote owner group. Pair
                                // and consume only that group; a same-LSN transaction
                                // can delete several vector owners, and scanning every
                                // empty vector at that LSN would steal the next
                                // owner's mapping.
                                let paired_deletes = changes
                                    .vectors
                                    .get(vector_row_idx)
                                    .map(|vector| vector.row_id)
                                    .map(|remote_row_id| {
                                        changes
                                            .vectors
                                            .iter()
                                            .filter(|vector| {
                                                vector.row_id == remote_row_id
                                                    && vector.vector.is_empty()
                                                    && vector.index.table == row.table
                                                    && vector.lsn == row.lsn
                                            })
                                            .map(|vector| vector.index.clone())
                                            .collect::<Vec<_>>()
                                    })
                                    .unwrap_or_default();
                                for index in &paired_deletes {
                                    // A retained replay may carry the earlier live
                                    // relational row and the later row/vector
                                    // tombstones while omitting the superseded vector
                                    // insert. The relational owner exists in this
                                    // transaction, but there is no ANN entry to
                                    // delete; persistent vector storage treats an
                                    // absent owner as NotFound, so do not stage a
                                    // fictitious delete.
                                    if !self.vector_store_has_live_entry_for_row(
                                        index,
                                        local.row_id,
                                        self.snapshot(),
                                        vector_copy_owner,
                                    )? {
                                        continue;
                                    }
                                    if let Err(err) = self.delete_vector_with_read_memory(
                                        tx,
                                        index.clone(),
                                        local.row_id,
                                        vector_copy_owner,
                                        read_memory.clone(),
                                    ) {
                                        let _ = self.rollback(tx);
                                        return Err(err);
                                    }
                                }
                                if !paired_deletes.is_empty() {
                                    consume_failed_vector_row_group(
                                        &changes.vectors,
                                        &mut vector_row_idx,
                                        &mut failed_vector_groups,
                                    );
                                }
                            }
                            if let Err(err) = self.delete_row_keeping_vectors(
                                tx,
                                &row.table,
                                local.row_id,
                                vector_copy_owner,
                                read_memory.clone(),
                            ) {
                                if receipt.is_some() {
                                    let _ = self.rollback(tx);
                                    return Err(Error::SyncError(
                                        (format!("authenticated sync delete failed: {err}")).into(),
                                    ));
                                }
                                result.conflicts.push(Conflict {
                                    natural_key: row.natural_key.clone(),
                                    resolution: policy,
                                    reason: Some(format!("delete failed: {err}")),
                                    table: None,
                                    mutation_kind: None,
                                    winning_author_node_id: None,
                                    hub_acceptance_position: None,
                                    refusal_cause: None,
                                });
                                result.skipped_rows += 1;
                            } else {
                                remove_cached_row(
                                    &mut applied_rows_cache,
                                    &row.table,
                                    local.row_id,
                                );
                                if !applied_new_row_ids.remove(&local.row_id) {
                                    applied_deleted_committed_row_ids
                                        .entry(row.table.clone())
                                        .or_default()
                                        .insert(local.row_id);
                                }
                                self.stage_detached_accepted_row(
                                    tx,
                                    &row,
                                    Some(local.row_id),
                                    dependency_complete,
                                    read_memory.clone(),
                                )?;
                                accepted_authenticated_rows.push(row.clone());
                                result.applied_rows += 1;
                                accepted_sync_tombstones.push((
                                    row.table.clone(),
                                    row.natural_key.clone(),
                                    incoming_arrival,
                                    contextdb_relational::store::SyncSourceKind::Pulled,
                                    local.row_id,
                                ));
                            }
                        } else {
                            // A deleting edge can pull its own accepted tombstone
                            // after the keyed row is already absent locally. Consume
                            // only the vector group paired with THIS remote owner;
                            // leaving it for the generic vector loop would try to
                            // delete the hub's RowId on this edge, and consuming every
                            // same-LSN tombstone would steal a later owner's group.
                            if row_has_vector
                                && let Some(remote_row_id) = changes
                                    .vectors
                                    .get(vector_row_idx)
                                    .map(|vector| vector.row_id)
                                && changes.vectors.iter().any(|vector| {
                                    vector.row_id == remote_row_id
                                        && vector.vector.is_empty()
                                        && vector.index.table == row.table
                                        && vector.lsn == row.lsn
                                })
                            {
                                consume_failed_vector_row_group(
                                    &changes.vectors,
                                    &mut vector_row_idx,
                                    &mut failed_vector_groups,
                                );
                            }
                            // Absence is still accepted ancestry: this edge learned an
                            // authoritative tombstone even though it had no live row
                            // left to mutate. Persist the same received lineage record
                            // as an applied delete so restart and a later explicit
                            // destination move cannot forget it.
                            self.stage_detached_accepted_row(
                                tx,
                                &row,
                                None,
                                dependency_complete,
                                read_memory.clone(),
                            )?;
                            accepted_authenticated_rows.push(row.clone());
                            result.skipped_rows += 1;
                        }
                        if commit_each_row {
                            self.commit_with_source(
                                tx,
                                CommitSource::SyncPull,
                                vector_copy_owner,
                                read_memory.clone(),
                            )?;
                            tx = self.begin()?;
                        }
                        continue;
                    }

                    let mut values = row.values.clone();
                    values.remove("__deleted");

                    // A row that arrives carrying exactly what this node already holds
                    // is a re-delivery, not a divergence — the everyday case being an
                    // edge pulling back the rows it pushed a moment ago. Under EVERY
                    // conflict policy that is a PURE no-op, counted in none of the three
                    // buckets: nothing was written, so it is not applied work; nothing
                    // disagreed, so it is not a conflict; and nothing was refused, so it
                    // is not skipped either — `skipped_rows` reports rows a policy or the
                    // context scope turned away, and an echo is a convergence
                    // confirmation rather than turned-away work. The decision is made per
                    // row against that row's own content, so a batch mixing an echo with
                    // a genuine divergence still reports the divergence in full; only
                    // rows whose content differs reach the policy arms below.
                    if let Some(local) = existing.as_ref()
                        && self.sync_incoming_row_matches_committed(
                            &values,
                            local,
                            &row.table,
                            row_meta.as_ref(),
                            read_memory.clone(),
                        )?
                    {
                        let confirmed_pending_echo = adoption
                        == SyncAdoption::ConfirmedPendingReconciliation
                        && match self
                            .relational_sync_source_at_read_root(
                                &row.table,
                                local.row_id,
                                read_memory.clone(),
                            )?
                            .1
                        {
                            Some(
                                contextdb_relational::store::SyncSourceKind::AcceptedLocalPending,
                            ) => true,
                            // Ordinary reply recovery has already persisted the
                            // exact accepted order before pulling. Its identical
                            // echo must preserve local authorship for later restore.
                            Some(contextdb_relational::store::SyncSourceKind::AcceptedLocal) => {
                                Self::resolve_incoming_arrival(&row, arrivals)
                                    == self
                                        .relational_sync_source_at_read_root(
                                            &row.table,
                                            local.row_id,
                                            read_memory.clone(),
                                        )?
                                        .0
                            }
                            _ => false,
                        };
                        // The content is unchanged, but a row arriving WITH an
                        // established arrival is now KNOWN to be at that fleet
                        // position — even on its first appearance in this node's own
                        // provenance sidecar (e.g. this node authored the row
                        // directly, and only now receives the identical value back
                        // through sync). Recording it here is what lets a LATER,
                        // genuinely newer arrival be judged against the fleet's own
                        // ordering instead of this node's unrelated local clock.
                        //
                        // The same record settles the other half: the peer that served
                        // this row demonstrably holds it, so this node no longer owes
                        // it a push. Without the sidecar the row is indistinguishable
                        // from unsent local work and stays outbound forever — the case
                        // a wiped edge hits, where every restored row is local work
                        // this node never pushed. The record rides this apply's own
                        // transaction, so it survives reopen exactly like an
                        // ordinarily applied pulled row's.
                        //
                        // The row itself is left untouched: it already holds the right
                        // values, and the tables this matters most for are immutable,
                        // which forbids rewriting them at all.
                        if !confirmed_pending_echo
                            && let Err(err) = self.mark_sync_row_provenance(
                                tx,
                                &row.table,
                                local.row_id,
                                Self::resolve_incoming_arrival(&row, arrivals)
                                    .unwrap_or(SYNC_SOURCE_LSN_OWN_COMMIT),
                                contextdb_relational::store::SyncSourceKind::Pulled,
                            )
                        {
                            let _ = self.rollback(tx);
                            return Err(err);
                        }
                        if row_has_vector
                            && next_vector_row_group_matches(&changes.vectors, vector_row_idx, &row)
                        {
                            consume_vector_row_group(
                                &changes.vectors,
                                &mut vector_row_idx,
                                local.row_id,
                                &mut vector_row_map,
                            );
                        }
                        if commit_each_row {
                            self.commit_with_source(
                                tx,
                                CommitSource::SyncPull,
                                vector_copy_owner,
                                read_memory.clone(),
                            )?;
                            tx = self.begin()?;
                        }
                        continue;
                    }

                    match (existing, policy) {
                        (None, _) => {
                            if let Some(meta) =
                                cached_table_meta(self, &mut table_meta_cache, &row.table)
                            {
                                let mut constraint_error: Option<String> = None;

                                for col_def in &meta.columns {
                                    if !col_def.nullable && col_def.default.is_none() {
                                        match values.get(&col_def.name) {
                                            None | Some(Value::Null) => {
                                                constraint_error = Some(format!(
                                                    "NOT NULL constraint violated: {}.{}",
                                                    row.table, col_def.name
                                                ));
                                                break;
                                            }
                                            _ => {}
                                        }
                                    }
                                }

                                if constraint_error.is_none()
                                    && let Some(err) = self
                                        .sync_insert_constraint_error_for_values(
                                            &row.table,
                                            &values,
                                            &applied_rows_cache,
                                            &skip_deleted,
                                            read_memory.clone(),
                                        )?
                                {
                                    constraint_error = Some(err);
                                }

                                if constraint_error.is_some()
                                    && matches!(policy, ConflictPolicy::EdgeWins)
                                    && !row_has_vector
                                    && let Some(conflict) = self
                                        .sync_committed_unique_conflict_for_values(
                                            &row.table,
                                            &values,
                                            &skip_deleted,
                                            read_memory.clone(),
                                        )?
                                {
                                    let mut skip_with_conflict = skip_deleted.clone();
                                    skip_with_conflict.insert(conflict.row_id);
                                    if self
                                        .sync_insert_constraint_error_for_values(
                                            &row.table,
                                            &values,
                                            &applied_rows_cache,
                                            &skip_with_conflict,
                                            read_memory.clone(),
                                        )?
                                        .is_none()
                                    {
                                        if let Err(err) = self.delete_row_keeping_vectors(
                                            tx,
                                            &row.table,
                                            conflict.row_id,
                                            vector_copy_owner,
                                            read_memory.clone(),
                                        ) {
                                            constraint_error = Some(format!(
                                                "edge_wins unique replacement delete failed: {err}"
                                            ));
                                        } else {
                                            remove_cached_row(
                                                &mut applied_rows_cache,
                                                &row.table,
                                                conflict.row_id,
                                            );
                                            if !applied_new_row_ids.remove(&conflict.row_id) {
                                                applied_deleted_committed_row_ids
                                                    .entry(row.table.clone())
                                                    .or_default()
                                                    .insert(conflict.row_id);
                                            }
                                            constraint_error = None;
                                        }
                                    }
                                }

                                if let Some(err_msg) = constraint_error {
                                    if receipt.is_some() {
                                        let _ = self.rollback(tx);
                                        return Err(Error::SyncError((format!(
                                        "authenticated sync constraint apply failed: {err_msg}"
                                    )).into()));
                                    }
                                    result.skipped_rows += 1;
                                    if row_has_vector
                                        && next_vector_row_group_matches(
                                            &changes.vectors,
                                            vector_row_idx,
                                            &row,
                                        )
                                    {
                                        consume_failed_vector_row_group(
                                            &changes.vectors,
                                            &mut vector_row_idx,
                                            &mut failed_vector_groups,
                                        );
                                    }
                                    result.conflicts.push(Conflict {
                                        natural_key: row.natural_key.clone(),
                                        resolution: policy,
                                        reason: Some(err_msg),
                                        table: None,
                                        mutation_kind: None,
                                        winning_author_node_id: None,
                                        hub_acceptance_position: None,
                                        refusal_cause: None,
                                    });
                                    if commit_each_row {
                                        self.commit_with_source(
                                            tx,
                                            CommitSource::SyncPull,
                                            vector_copy_owner,
                                            read_memory.clone(),
                                        )?;
                                        tx = self.begin()?;
                                    }
                                    continue;
                                }

                                if let Some(err) = if source_coordinates.is_some() {
                                    None
                                } else {
                                    self.sync_composite_fk_violation_for_values(
                                        tx,
                                        &row.table,
                                        &meta,
                                        &values,
                                        &projected_sync_apply.incoming_values,
                                        &projected_sync_apply.deleted_committed_row_ids,
                                        read_memory.clone(),
                                    )?
                                } {
                                    if receipt.is_some() {
                                        let _ = self.rollback(tx);
                                        return Err(Error::SyncError((format!(
                                        "authenticated sync composite foreign-key apply failed: {err}"
                                    )).into()));
                                    }
                                    result.skipped_rows += 1;
                                    if row_has_vector
                                        && next_vector_row_group_matches(
                                            &changes.vectors,
                                            vector_row_idx,
                                            &row,
                                        )
                                    {
                                        consume_failed_vector_row_group(
                                            &changes.vectors,
                                            &mut vector_row_idx,
                                            &mut failed_vector_groups,
                                        );
                                    }
                                    result.conflicts.push(Conflict {
                                        natural_key: row.natural_key.clone(),
                                        resolution: policy,
                                        reason: Some(format!("{err}")),
                                        table: None,
                                        mutation_kind: None,
                                        winning_author_node_id: None,
                                        hub_acceptance_position: None,
                                        refusal_cause: None,
                                    });
                                    if commit_each_row {
                                        self.commit_with_source(
                                            tx,
                                            CommitSource::SyncPull,
                                            vector_copy_owner,
                                            read_memory.clone(),
                                        )?;
                                        tx = self.begin()?;
                                    }
                                    continue;
                                }
                            }

                            // Sync-apply overflow guard + allocator/watermark advance for Value::TxId cells.
                            let mut overflow: Option<Error> = None;
                            for v in values.values() {
                                if let Value::TxId(incoming) = v
                                    && let Err(err) =
                                        self.tx_mgr.advance_for_sync(tx, &row.table, *incoming)
                                {
                                    overflow = Some(err);
                                    break;
                                }
                            }
                            if let Some(err) = overflow {
                                let _ = self.rollback(tx);
                                return Err(err);
                            }

                            match self.insert_row_for_sync(
                                tx,
                                &row.table,
                                values.clone(),
                                row.created_at,
                                vector_copy_owner,
                                read_memory.clone(),
                            ) {
                                Ok(new_row_id) => {
                                    // The row's provenance sidecar takes the incoming
                                    // row's arrival when it carries one (a value
                                    // already established somewhere in the fleet);
                                    // otherwise this node is the one ordering it, so
                                    // the sidecar takes THIS node's own accepting
                                    // position — never the sender's unrelated local
                                    // clock (`row.lsn`), which is exactly the
                                    // cross-sender defect this stamp must not
                                    // reintroduce. A stampless row gets the SENTINEL,
                                    // never a `current_lsn()` sample: this insert's
                                    // transaction's commit has not happened yet, and a
                                    // concurrent apply's commit landing first would
                                    // make a sampled value stale before this one even
                                    // commits (see `SYNC_SOURCE_LSN_OWN_COMMIT`).
                                    let source_lsn = Self::resolve_incoming_arrival(&row, arrivals)
                                        .unwrap_or(SYNC_SOURCE_LSN_OWN_COMMIT);
                                    if let Err(err) = self.set_sync_row_source_lsn(
                                        tx, &row.table, new_row_id, source_lsn,
                                    ) {
                                        let _ = self.rollback(tx);
                                        return Err(err);
                                    }
                                    applied_new_row_ids.insert(new_row_id);
                                    record_cached_insert(
                                        &mut applied_rows_cache,
                                        &row.table,
                                        VersionedRow {
                                            row_id: new_row_id,
                                            values: values.clone(),
                                            created_tx: tx,
                                            deleted_tx: None,
                                            lsn: row.lsn,
                                            created_at: None,
                                        },
                                    );
                                    self.stage_detached_accepted_row(
                                        tx,
                                        &row,
                                        Some(new_row_id),
                                        dependency_complete,
                                        read_memory.clone(),
                                    )?;
                                    accepted_authenticated_rows.push(row.clone());
                                    result.applied_rows += 1;
                                    if row_has_vector
                                        && next_vector_row_group_matches(
                                            &changes.vectors,
                                            vector_row_idx,
                                            &row,
                                        )
                                    {
                                        consume_vector_row_group(
                                            &changes.vectors,
                                            &mut vector_row_idx,
                                            new_row_id,
                                            &mut vector_row_map,
                                        );
                                    }
                                }
                                Err(err) => {
                                    if is_fatal_sync_apply_error(&err) {
                                        let _ = self.rollback(tx);
                                        return Err(err);
                                    }
                                    if receipt.is_some() {
                                        let _ = self.rollback(tx);
                                        return Err(Error::SyncError(
                                            (format!("authenticated sync insert failed: {err}"))
                                                .into(),
                                        ));
                                    }
                                    result.skipped_rows += 1;
                                    if row_has_vector
                                        && next_vector_row_group_matches(
                                            &changes.vectors,
                                            vector_row_idx,
                                            &row,
                                        )
                                    {
                                        consume_failed_vector_row_group(
                                            &changes.vectors,
                                            &mut vector_row_idx,
                                            &mut failed_vector_groups,
                                        );
                                    }
                                    result.conflicts.push(Conflict {
                                        natural_key: row.natural_key.clone(),
                                        resolution: policy,
                                        reason: Some(format!("{err}")),
                                        table: None,
                                        mutation_kind: None,
                                        winning_author_node_id: None,
                                        hub_acceptance_position: None,
                                        refusal_cause: None,
                                    });
                                }
                            }
                        }
                        (Some(local), ConflictPolicy::InsertIfNotExists) => {
                            // The pull leg adopts the hub-accepted value; the local
                            // value it displaces is reported, never dropped silently.
                            // This is not special to a connected unit — the same
                            // resolution applies to every pulled row on a table that
                            // declares a pull leg. Refusing instead would leave a unit
                            // member disagreeing with the siblings committing beside
                            // it, and would strand an edge holding any divergent
                            // structural row: it could never restore.
                            let mut adoption_refused_by_row = false;
                            if inbound_over_declared_pull_leg {
                                for value in values.values() {
                                    if let Value::TxId(incoming) = value
                                        && let Err(err) =
                                            self.tx_mgr.advance_for_sync(tx, &row.table, *incoming)
                                    {
                                        let _ = self.rollback(tx);
                                        return Err(err);
                                    }
                                }
                                match self.upsert_row_for_sync(
                                    tx,
                                    &row.table,
                                    &row.natural_key,
                                    values.clone(),
                                    row.created_at,
                                    vector_copy_owner,
                                    read_memory.clone(),
                                ) {
                                    Ok(upsert_result) => {
                                        if !matches!(upsert_result, UpsertResult::NoOp) {
                                            if let Err(err) = self.set_sync_row_source_lsn(
                                                tx,
                                                &row.table,
                                                local.row_id,
                                                Self::resolve_incoming_arrival(&row, arrivals)
                                                    .unwrap_or(SYNC_SOURCE_LSN_OWN_COMMIT),
                                            ) {
                                                let _ = self.rollback(tx);
                                                return Err(err);
                                            }
                                            applied_deleted_committed_row_ids
                                                .entry(row.table.clone())
                                                .or_default()
                                                .insert(local.row_id);
                                            upsert_cached_projection(
                                                &mut applied_rows_cache,
                                                &row.table,
                                                local.row_id,
                                                values.clone(),
                                                row.lsn,
                                            );
                                            self.stage_detached_accepted_row(
                                                tx,
                                                &row,
                                                Some(local.row_id),
                                                dependency_complete,
                                                read_memory.clone(),
                                            )?;
                                            accepted_authenticated_rows.push(row.clone());
                                            result.applied_rows += 1;
                                            // The adopted value replaced a local one, so
                                            // the resolution is reported rather than
                                            // applied silently. The winner is the
                                            // authenticated author of the row just
                                            // adopted, and its hub position is that
                                            // row's established arrival.
                                            result.conflicts.push(Conflict {
                                                natural_key: row.natural_key.clone(),
                                                resolution: ConflictPolicy::InsertIfNotExists,
                                                reason: Some("keep_first_hub_adopted".to_string()),
                                                table: Some(row.table.clone()),
                                                mutation_kind: Some("edit".to_string()),
                                                winning_author_node_id: lineages
                                                    .iter()
                                                    .find(|(table, key, lsn, _)| {
                                                        table == &row.table
                                                            && key == &row.natural_key
                                                            && *lsn == row.lsn
                                                    })
                                                    .map(|(_, _, _, lineage)| {
                                                        lineage.author_node_id.clone()
                                                    }),
                                                hub_acceptance_position:
                                                    Self::resolve_incoming_arrival(&row, arrivals),
                                                refusal_cause: None,
                                            });
                                        }
                                        if row_has_vector
                                            && next_vector_row_group_matches(
                                                &changes.vectors,
                                                vector_row_idx,
                                                &row,
                                            )
                                        {
                                            consume_vector_row_group(
                                                &changes.vectors,
                                                &mut vector_row_idx,
                                                local.row_id,
                                                &mut vector_row_map,
                                            );
                                        }
                                    }
                                    Err(err) if is_fatal_sync_apply_error(&err) => {
                                        // A genuine apply failure, unlike a per-row
                                        // resolution, still fails the whole unit.
                                        let _ = self.rollback(tx);
                                        return Err(err);
                                    }
                                    Err(_) => {
                                        // The row itself refuses the adopted value —
                                        // an immutable column the incoming value would
                                        // rewrite is the standing case. That is a
                                        // refusal of THIS row, not of the apply that
                                        // carried it, so it falls through to the
                                        // ordinary refusal handling below: the local
                                        // value stands, counted and reported.
                                        adoption_refused_by_row = true;
                                    }
                                }
                                if !adoption_refused_by_row {
                                    if commit_each_row {
                                        self.commit_with_source(
                                            tx,
                                            CommitSource::SyncPull,
                                            vector_copy_owner,
                                            read_memory.clone(),
                                        )?;
                                        tx = self.begin()?;
                                    }
                                    continue;
                                }
                            }
                            if row_has_vector
                                && next_vector_row_group_matches(
                                    &changes.vectors,
                                    vector_row_idx,
                                    &row,
                                )
                            {
                                consume_failed_vector_row_group(
                                    &changes.vectors,
                                    &mut vector_row_idx,
                                    &mut failed_vector_groups,
                                );
                            }
                            if let Err(err) = self.stage_authoritative_row_after_sync_refusal(
                                tx,
                                &row.table,
                                &local,
                                vector_copy_owner,
                                read_memory.clone(),
                            ) {
                                let _ = self.rollback(tx);
                                return Err(err);
                            }
                            result.skipped_rows += 1;
                            semantic_refused_rows.push(row.clone());
                            if receipt.is_none() {
                                // An ordinary pull has no authenticated-push receipt
                                // from which to synthesize the terminal winner
                                // diagnostic below. Report the visible disagreement
                                // here, but keep it accounting-only so the client
                                // cannot mistake it for a terminal push refusal.
                                result.conflicts.push(Conflict {
                                    natural_key: row.natural_key.clone(),
                                    resolution: ConflictPolicy::InsertIfNotExists,
                                    reason: Some("keep_first".to_string()),
                                    table: None,
                                    mutation_kind: None,
                                    winning_author_node_id: None,
                                    hub_acceptance_position: None,
                                    refusal_cause: None,
                                });
                            }
                        }
                        (Some(local), ConflictPolicy::ServerWins) => {
                            result.skipped_rows += 1;
                            semantic_refused_rows.push(row.clone());
                            if row_has_vector
                                && next_vector_row_group_matches(
                                    &changes.vectors,
                                    vector_row_idx,
                                    &row,
                                )
                            {
                                consume_failed_vector_row_group(
                                    &changes.vectors,
                                    &mut vector_row_idx,
                                    &mut failed_vector_groups,
                                );
                            }
                            if let Err(err) = self.stage_authoritative_row_after_sync_refusal(
                                tx,
                                &row.table,
                                &local,
                                vector_copy_owner,
                                read_memory.clone(),
                            ) {
                                let _ = self.rollback(tx);
                                return Err(err);
                            }
                            result.conflicts.push(Conflict {
                                natural_key: row.natural_key.clone(),
                                resolution: ConflictPolicy::ServerWins,
                                reason: Some("server_wins".to_string()),
                                table: None,
                                mutation_kind: None,
                                winning_author_node_id: None,
                                hub_acceptance_position: None,
                                refusal_cause: None,
                            });
                        }
                        (Some(local), ConflictPolicy::LatestWins) => {
                            // Resolve the row's stored provenance once: it feeds the
                            // win/lose comparison and, on apply, seeds the fallback
                            // used when the incoming row itself carries no arrival.
                            // A single sidecar probe per row keeps the hot apply path
                            // allocation- and double-probe-free.
                            let committed_source = self
                                .relational_sync_source_at_read_root(
                                    &row.table,
                                    local.row_id,
                                    read_memory.clone(),
                                )?
                                .0;
                            let incoming_arrival = Self::resolve_incoming_arrival(&row, arrivals);
                            let incoming_wins = if adoption
                            == SyncAdoption::ConfirmedPendingReconciliation
                            && matches!(
                            self.relational_sync_source_at_read_root(&row.table, local.row_id, read_memory.clone())?
                                .1,
                            Some(contextdb_relational::store::SyncSourceKind::AcceptedLocalPending)
                        ) {
                            true
                        } else {
                            self.sync_latest_wins_incoming(
                                &row,
                                &local,
                                committed_source,
                                incoming_arrival,
                                adoption,
                            )
                        };

                            if !incoming_wins {
                                // A stale echo: an ordering position at or below the
                                // one already stored. Not a conflict — nothing was
                                // refused, there is simply nothing new — and not
                                // skipped either, so this is a complete no-op on the
                                // receipt. The vector bookkeeping still needs to
                                // consume this row's slot to keep the vector-row index
                                // aligned with the rows that carry one.
                                if row_has_vector
                                    && next_vector_row_group_matches(
                                        &changes.vectors,
                                        vector_row_idx,
                                        &row,
                                    )
                                {
                                    consume_failed_vector_row_group(
                                        &changes.vectors,
                                        &mut vector_row_idx,
                                        &mut failed_vector_groups,
                                    );
                                }
                                if commit_each_row {
                                    self.commit_with_source(
                                        tx,
                                        CommitSource::SyncPull,
                                        vector_copy_owner,
                                        read_memory.clone(),
                                    )?;
                                    tx = self.begin()?;
                                }
                                continue;
                            } else {
                                // State machine conflict detection
                                if let Some(meta) = self.table_meta(&row.table)
                                    && let Some(sm) = &meta.state_machine
                                {
                                    let sm_col = sm.column.clone();
                                    let transitions = sm.transitions.clone();
                                    let incoming_state =
                                        values.get(&sm_col).and_then(|v| match v {
                                            Value::Text(s) => Some(s.clone()),
                                            _ => None,
                                        });
                                    let local_state =
                                        local.values.get(&sm_col).and_then(|v| match v {
                                            Value::Text(s) => Some(s.clone()),
                                            _ => None,
                                        });

                                    if let (Some(incoming), Some(current)) =
                                        (incoming_state, local_state)
                                    {
                                        // Check if the transition from current to incoming is valid
                                        let valid = transitions
                                            .get(&current)
                                            .is_some_and(|targets| targets.contains(&incoming));
                                        if !valid && incoming != current {
                                            if receipt.is_some() {
                                                let _ = self.rollback(tx);
                                                return Err(Error::SyncError((format!(
                                                "authenticated sync state transition refused: {current} -> {incoming}"
                                            )).into()));
                                            }
                                            result.skipped_rows += 1;
                                            if row_has_vector
                                                && next_vector_row_group_matches(
                                                    &changes.vectors,
                                                    vector_row_idx,
                                                    &row,
                                                )
                                            {
                                                consume_failed_vector_row_group(
                                                    &changes.vectors,
                                                    &mut vector_row_idx,
                                                    &mut failed_vector_groups,
                                                );
                                            }
                                            result.conflicts.push(Conflict {
                                        natural_key: row.natural_key.clone(),
                                        resolution: ConflictPolicy::LatestWins,
                                        reason: Some(format!(
                                            "state_machine: invalid transition {} -> {} (current: {})",
                                            current, incoming, current
                                        )),
                                        table: None,
                                        mutation_kind: None,
                                        winning_author_node_id: None,
                                        hub_acceptance_position: None,
                                        refusal_cause: None,
                                    });
                                            if commit_each_row {
                                                self.commit_with_source(
                                                    tx,
                                                    CommitSource::SyncPull,
                                                    vector_copy_owner,
                                                    read_memory.clone(),
                                                )?;
                                                tx = self.begin()?;
                                            }
                                            continue;
                                        }
                                    }
                                }

                                // Sync-apply overflow guard + allocator/watermark advance.
                                let mut overflow: Option<Error> = None;
                                for v in values.values() {
                                    if let Value::TxId(incoming) = v
                                        && let Err(err) =
                                            self.tx_mgr.advance_for_sync(tx, &row.table, *incoming)
                                    {
                                        overflow = Some(err);
                                        break;
                                    }
                                }
                                if let Some(err) = overflow {
                                    let _ = self.rollback(tx);
                                    return Err(err);
                                }

                                match self.upsert_row_for_sync(
                                    tx,
                                    &row.table,
                                    &row.natural_key,
                                    values.clone(),
                                    row.created_at,
                                    vector_copy_owner,
                                    read_memory.clone(),
                                ) {
                                    Ok(upsert_result) => {
                                        // A byte-identical re-delivery upserts to NoOp:
                                        // storage is untouched, provenance stays put, and
                                        // it must not count as applied work.
                                        if matches!(upsert_result, UpsertResult::NoOp) {
                                            if row_has_vector
                                                && next_vector_row_group_matches(
                                                    &changes.vectors,
                                                    vector_row_idx,
                                                    &row,
                                                )
                                            {
                                                consume_vector_row_group(
                                                    &changes.vectors,
                                                    &mut vector_row_idx,
                                                    local.row_id,
                                                    &mut vector_row_map,
                                                );
                                            }
                                            if commit_each_row {
                                                self.commit_with_source(
                                                    tx,
                                                    CommitSource::SyncPull,
                                                    vector_copy_owner,
                                                    read_memory.clone(),
                                                )?;
                                                tx = self.begin()?;
                                            }
                                            continue;
                                        }
                                        // Re-stamp the sidecar with the WINNING
                                        // arrival: the incoming row's own established
                                        // position when it carries one, or this node's
                                        // own accepting position when it does not (the
                                        // role-free stamping rule — see
                                        // `resolve_incoming_arrival`). The upsert
                                        // internally deletes+reinserts the row, which
                                        // clears the sidecar, so it must be written
                                        // back here regardless. Freezing this at the
                                        // OLD stored value (the prior behavior) is
                                        // exactly the bug: it left a later, genuinely
                                        // newer arrival with nothing correct to compare
                                        // against once this update landed. A stampless
                                        // row gets the SENTINEL, never a `current_lsn()`
                                        // sample -- see `SYNC_SOURCE_LSN_OWN_COMMIT`.
                                        let source_lsn =
                                            incoming_arrival.unwrap_or(SYNC_SOURCE_LSN_OWN_COMMIT);
                                        if let Err(err) = self.set_sync_row_source_lsn(
                                            tx,
                                            &row.table,
                                            local.row_id,
                                            source_lsn,
                                        ) {
                                            let _ = self.rollback(tx);
                                            return Err(err);
                                        }
                                        applied_deleted_committed_row_ids
                                            .entry(row.table.clone())
                                            .or_default()
                                            .insert(local.row_id);
                                        upsert_cached_projection(
                                            &mut applied_rows_cache,
                                            &row.table,
                                            local.row_id,
                                            values.clone(),
                                            row.lsn,
                                        );
                                        self.stage_detached_accepted_row(
                                            tx,
                                            &row,
                                            Some(local.row_id),
                                            dependency_complete,
                                            read_memory.clone(),
                                        )?;
                                        accepted_authenticated_rows.push(row.clone());
                                        result.applied_rows += 1;
                                        if row_has_vector
                                            && next_vector_row_group_matches(
                                                &changes.vectors,
                                                vector_row_idx,
                                                &row,
                                            )
                                        {
                                            // The upsert preserves `local.row_id` (the
                                            // WHOLE-key-resolved target) across its
                                            // delete+reinsert, so attach the vector there
                                            // directly. A leading-column re-lookup would
                                            // pick the wrong sibling when two rows share
                                            // that column.
                                            consume_vector_row_group(
                                                &changes.vectors,
                                                &mut vector_row_idx,
                                                local.row_id,
                                                &mut vector_row_map,
                                            );
                                        }
                                    }
                                    Err(err) => {
                                        if is_fatal_sync_apply_error(&err) {
                                            let _ = self.rollback(tx);
                                            return Err(err);
                                        }
                                        if receipt.is_some() {
                                            let _ = self.rollback(tx);
                                            return Err(Error::SyncError(
                                                (format!(
                                                    "authenticated sync upsert failed: {err}"
                                                ))
                                                .into(),
                                            ));
                                        }
                                        result.skipped_rows += 1;
                                        if row_has_vector
                                            && next_vector_row_group_matches(
                                                &changes.vectors,
                                                vector_row_idx,
                                                &row,
                                            )
                                        {
                                            consume_failed_vector_row_group(
                                                &changes.vectors,
                                                &mut vector_row_idx,
                                                &mut failed_vector_groups,
                                            );
                                        }
                                        result.conflicts.push(Conflict {
                                            natural_key: row.natural_key.clone(),
                                            resolution: ConflictPolicy::LatestWins,
                                            reason: Some(format!(
                                                "state_machine_or_constraint: {err}"
                                            )),
                                            table: None,
                                            mutation_kind: None,
                                            winning_author_node_id: None,
                                            hub_acceptance_position: None,
                                            refusal_cause: None,
                                        });
                                    }
                                }
                            }
                        }
                        (Some(local), ConflictPolicy::EdgeWins) => {
                            result.conflicts.push(Conflict {
                                natural_key: row.natural_key.clone(),
                                resolution: ConflictPolicy::EdgeWins,
                                reason: Some("edge_wins".to_string()),
                                table: None,
                                mutation_kind: None,
                                winning_author_node_id: None,
                                hub_acceptance_position: None,
                                refusal_cause: None,
                            });
                            let committed_source = self
                                .relational_sync_source_at_read_root(
                                    &row.table,
                                    local.row_id,
                                    read_memory.clone(),
                                )?
                                .0;
                            let mut overflow: Option<Error> = None;
                            for v in values.values() {
                                if let Value::TxId(incoming) = v
                                    && let Err(err) =
                                        self.tx_mgr.advance_for_sync(tx, &row.table, *incoming)
                                {
                                    overflow = Some(err);
                                    break;
                                }
                            }
                            if let Some(err) = overflow {
                                let _ = self.rollback(tx);
                                return Err(err);
                            }

                            match self.upsert_row_for_sync(
                                tx,
                                &row.table,
                                &row.natural_key,
                                values.clone(),
                                row.created_at,
                                vector_copy_owner,
                                read_memory.clone(),
                            ) {
                                Ok(_upsert_result) => {
                                    // Re-stamp the frozen provenance the upsert just
                                    // cleared; an EdgeWins update does not move the anchor
                                    // (see the LatestWins arm). Resolved against `local.lsn`
                                    // (the row's OWN committed position BEFORE this upsert)
                                    // rather than re-persisted raw: if `committed_source` held
                                    // the sentinel, blindly re-writing it would let the anchor
                                    // silently drift forward to this upsert's NEW commit LSN
                                    // instead of staying pinned at the position it actually
                                    // arrived at -- exactly the "does not move the anchor"
                                    // contract this comment already promises.
                                    let provenance_result = if terminal_refusal_override {
                                        self.set_sync_row_source_lsn_kind(
                                            tx,
                                            &row.table,
                                            local.row_id,
                                            Self::resolve_incoming_arrival(&row, arrivals)
                                                .unwrap_or(SYNC_SOURCE_LSN_OWN_COMMIT),
                                            contextdb_relational::store::SyncSourceKind::Pulled,
                                        )
                                    } else if let Some(source_lsn) =
                                        Self::resolve_sync_source_lsn(committed_source, local.lsn)
                                    {
                                        self.set_sync_row_source_lsn(
                                            tx,
                                            &row.table,
                                            local.row_id,
                                            source_lsn,
                                        )
                                    } else {
                                        Ok(())
                                    };
                                    if let Err(err) = provenance_result {
                                        let _ = self.rollback(tx);
                                        return Err(err);
                                    }
                                    applied_deleted_committed_row_ids
                                        .entry(row.table.clone())
                                        .or_default()
                                        .insert(local.row_id);
                                    upsert_cached_projection(
                                        &mut applied_rows_cache,
                                        &row.table,
                                        local.row_id,
                                        values.clone(),
                                        row.lsn,
                                    );
                                    self.stage_detached_accepted_row(
                                        tx,
                                        &row,
                                        Some(local.row_id),
                                        dependency_complete,
                                        read_memory.clone(),
                                    )?;
                                    accepted_authenticated_rows.push(row.clone());
                                    result.applied_rows += 1;
                                    if row_has_vector
                                        && next_vector_row_group_matches(
                                            &changes.vectors,
                                            vector_row_idx,
                                            &row,
                                        )
                                    {
                                        // Attach to the WHOLE-key-resolved row the upsert
                                        // preserved, not a leading-column re-lookup that
                                        // would pick the wrong sibling (see the LatestWins
                                        // arm).
                                        consume_vector_row_group(
                                            &changes.vectors,
                                            &mut vector_row_idx,
                                            local.row_id,
                                            &mut vector_row_map,
                                        );
                                    }
                                }
                                Err(err) => {
                                    if is_fatal_sync_apply_error(&err) {
                                        let _ = self.rollback(tx);
                                        return Err(err);
                                    }
                                    if receipt.is_some() {
                                        let _ = self.rollback(tx);
                                        return Err(Error::SyncError(
                                            (format!(
                                                "authenticated sync edge-wins apply failed: {err}"
                                            ))
                                            .into(),
                                        ));
                                    }
                                    result.skipped_rows += 1;
                                    if row_has_vector
                                        && next_vector_row_group_matches(
                                            &changes.vectors,
                                            vector_row_idx,
                                            &row,
                                        )
                                    {
                                        consume_failed_vector_row_group(
                                            &changes.vectors,
                                            &mut vector_row_idx,
                                            &mut failed_vector_groups,
                                        );
                                    }
                                    if let Some(last) = result.conflicts.last_mut() {
                                        last.reason =
                                            Some(format!("state_machine_or_constraint: {err}"));
                                    }
                                }
                            }
                        }
                    }

                    if commit_each_row {
                        self.commit_with_source(
                            tx,
                            CommitSource::SyncPull,
                            vector_copy_owner,
                            read_memory.clone(),
                        )?;
                        tx = self.begin()?;
                    }
                }

                if batch_row_commits {
                    self.commit_with_source(
                        tx,
                        CommitSource::SyncPull,
                        vector_copy_owner,
                        read_memory.clone(),
                    )?;
                    tx = self.begin()?;
                }

                for edge in changes.edges {
                    let is_delete =
                        matches!(edge.properties.get("__deleted"), Some(Value::Bool(true)));
                    if is_delete {
                        if let Err(err) = self.delete_edge_with_read_memory(
                            tx,
                            edge.source,
                            edge.target,
                            &edge.edge_type,
                            read_memory.clone(),
                        ) {
                            if is_sync_access_scope_error(&err) {
                                if receipt.is_some() {
                                    let _ = self.rollback(tx);
                                    return Err(Error::SyncError(
                                        ("authenticated sync edge access was denied".to_string())
                                            .into(),
                                    ));
                                }
                                result.skipped_rows += 1;
                                continue;
                            }
                            let _ = self.rollback(tx);
                            return Err(err);
                        }
                    } else {
                        if let Err(err) = self.insert_edge_with_read_memory(
                            tx,
                            edge.source,
                            edge.target,
                            edge.edge_type,
                            edge.properties,
                            read_memory.clone(),
                        ) {
                            if is_sync_access_scope_error(&err) {
                                if receipt.is_some() {
                                    let _ = self.rollback(tx);
                                    return Err(Error::SyncError(
                                        ("authenticated sync edge access was denied".to_string())
                                            .into(),
                                    ));
                                }
                                result.skipped_rows += 1;
                                continue;
                            }
                            let _ = self.rollback(tx);
                            return Err(err);
                        }
                    }
                }

                for vector in changes.vectors {
                    if failed_vector_groups.contains(&(
                        vector.row_id,
                        vector.index.table.clone(),
                        vector.lsn,
                        vector.vector.is_empty(),
                    )) {
                        continue; // skip vectors for rows that failed to insert
                    }
                    // Only a row applied (or already held) in this group can name a
                    // received vector's owner. Without that, the sender's row number
                    // is the only thing left, and it addresses that node's storage:
                    // applying it writes an unrelated local row, or aborts the whole
                    // pull on a number this node never issued.
                    let local_row_id = match vector_row_map.get(&vector.row_id).copied() {
                        Some(local_row_id) => local_row_id,
                        None if !vectors_carry_sender_row_numbering => vector.row_id,
                        None => {
                            result.skipped_rows += 1;
                            continue;
                        }
                    };
                    // An unchanged received vector is the same no-op as its owner
                    // row's echo. Re-inserting it would create a new local change-log
                    // position and make already confirmed local work outbound again.
                    if !vector.vector.is_empty()
                        && self
                            .find_vector_by_index_and_row(
                                &vector.index,
                                local_row_id,
                                vector_copy_owner,
                                read_memory.clone(),
                            )?
                            .is_some_and(|previous| previous.vector == vector.vector)
                    {
                        continue;
                    }
                    if replace_received_vector_image && !vector.vector.is_empty() {
                        received_image_superseded_vectors.extend(
                            self.vector_store.directory_versions_for_rows(
                                &vector.index,
                                &HashSet::from([local_row_id]),
                                vector_copy_owner,
                            )?,
                        );
                    }
                    if vector.vector.is_empty() {
                        if let Err(err) = self.delete_vector_with_read_memory(
                            tx,
                            vector.index.clone(),
                            local_row_id,
                            vector_copy_owner,
                            read_memory.clone(),
                        ) {
                            let _ = self.rollback(tx);
                            return Err(err);
                        }
                    } else {
                        if self.has_live_vector_with_read_memory(
                            local_row_id,
                            self.snapshot(),
                            read_memory.clone(),
                        )? {
                            let _ = self.delete_vector_with_read_memory(
                                tx,
                                vector.index.clone(),
                                local_row_id,
                                vector_copy_owner,
                                read_memory.clone(),
                            );
                        }
                        if let Err(err) = self.insert_vector_with_read_memory(
                            tx,
                            vector.index.clone(),
                            local_row_id,
                            vector.vector,
                            vector_copy_owner,
                            read_memory.clone(),
                        ) {
                            let _ = self.rollback(tx);
                            return Err(err);
                        }
                    }
                }

                if let (Some(scope), Some(coordinates), Some(mut declarations)) =
                    (phase_scope, source_coordinates, phase_trigger_start)
                {
                    let targets = scope.finish()?;
                    // Preserve the existing same-source transaction semantics:
                    // initial declarations and the last CREATE of each name can
                    // observe this group even when that group also drops them.
                    for ddl in &changes.ddl {
                        if let DdlChange::CreateTrigger { name, .. }
                        | DdlChange::CreateTriggerIncludingSync { name, .. } = ddl
                        {
                            declarations.remove(name);
                            self.apply_sync_trigger_ddl_to_projection(
                                &mut declarations,
                                ddl,
                                &self.relational_store.table_meta.read(),
                            )?;
                        }
                    }
                    self.prepare_active_sync_trigger_write_set_for_dispatch(tx)?;
                    let phase_lsn = coordinates.local(phase_source)?;
                    self.set_private_graph_phase(
                        tx,
                        crate::persistence::received_working_graph::WorkingGraphOrigin::Callback {
                            phase_position: phase_lsn,
                            authored: hub_local_author.is_some(),
                        },
                    )?;
                    self.private_callback_start(tx)?;
                    let (outcome, targets, initial_len) = match self
                        .dispatch_private_sync_trigger_rows_for_tx(
                            tx,
                            targets,
                            &declarations,
                            vector_copy_owner,
                            read_memory.clone(),
                        ) {
                        Ok(outcome) => outcome,
                        Err(failure) => {
                            let _ = self.rollback(tx);
                            return Err(failure.error);
                        }
                    };
                    self.private_callback_end(
                        tx,
                        initial_len,
                        hub_local_author.is_none(),
                        phase_lsn,
                    )?;
                    self.retain_private_plain_trigger_firings(
                        tx,
                        &targets,
                        initial_len,
                        phase_lsn,
                        &declarations,
                        read_memory.clone(),
                    )?;
                    self.prepare_private_prefix_sink_events(
                        tx,
                        phase_lsn,
                        &targets,
                        initial_len,
                        read_memory.clone(),
                    )?;
                    self.retain_private_prefix_audits(tx, phase_lsn, outcome.pending)?;
                    // Each callback has returned; later source DDL must not be
                    // mistaken for reentrant DDL inside a still-running callback.
                    drop(outcome.active_guards);
                }
            }
            if source_coordinates.is_some() {
                self.prepare_private_callbacks_for_commit(tx)?;
            }

            if !terminal_marker_clears.is_empty() {
                let context = terminal_refusal_context.expect(
                    "terminal refusal markers can only clear during private reconciliation",
                );
                let clears = terminal_marker_clears
                    .iter()
                    .map(|(table, natural_key)| {
                        Ok((
                            Self::terminal_refusal_marker_config_key(context, table, natural_key),
                            RedbPersistence::encode_config_value(&TerminalRefusalMarkerRecord {
                                table: table.clone(),
                                natural_key: natural_key.clone(),
                                lsn: 0,
                                row_id: 0,
                                active: false,
                                generation: context.generation,
                            })?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                self.tx_mgr.append_config_writes(tx, clears)?;
            }

            // An authenticated connected unit is one acceptance decision. The
            // ordinary row-at-a-time conflict accounting remains available to
            // unrelated batches, but one policy-refused member rolls this
            // transaction back before the hub can expose any connected parent or
            // sibling. Apply, constraint, and storage failures returned above stay
            // transient instead of being recast as arbitration.
            // Only the hub decides a connected unit as one acceptance. On the pull
            // side there is no acceptance to withhold — the unit already IS the
            // hub's authoritative content — so a member the local policy resolved
            // is reported through its conflict diagnostic and the unit commits.
            // Failing here instead left an edge unable to restore at all whenever
            // it held one divergent row. Genuine apply, constraint, and storage
            // failures still return above, before this point.
            if atomic_receipt
                && let Some(receipt) = receipt.as_ref()
                && !semantic_refused_rows.is_empty()
            {
                let _ = self.rollback(tx);
                // A semantic refusal is definitive, unlike a transport failure:
                // none of the member mutations survive, but the authenticated
                // edge receipt must survive so this source unit is never resent.
                result.conflicts = self.dependency_unit_refusal_conflicts(
                    &dependency_unit_rows,
                    policies,
                    hub_local_author,
                    read_memory.clone(),
                )?;
                let diagnostics_cover_every_member = result.conflicts.len()
                    == dependency_unit_rows.len()
                    && dependency_unit_rows.iter().all(|row| {
                        result.conflicts.iter().any(|conflict| {
                            conflict.table.as_deref() == Some(row.table.as_str())
                                && conflict.natural_key == row.natural_key
                                && Self::refusal_diagnostic_is_complete(conflict)
                        })
                    });
                if !diagnostics_cover_every_member {
                    return Err(Error::SyncError(
                        ("dependency-unit refusal lacked complete winner diagnostics".to_string())
                            .into(),
                    ));
                }
                result.applied_rows = 0;
                result.skipped_rows = dependency_unit_row_count;
                self.commit_sync_apply_outcome_only(
                    receipt,
                    push_outcome_key,
                    &result,
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
                result.new_lsn = self.current_lsn();
                return Ok(result);
            }

            // An ordinary source-LSN group can have both accepted and refused
            // members. Replace only the rejected members' local policy-shaped
            // conflicts with hub-winner diagnostics; accepted and no-op siblings
            // remain ordinary receipt-confirmed work. If any rejected member lacks
            // a complete current-winner proof, this remains an error so the edge
            // retains the whole group as transient pending work.
            if !atomic_receipt && receipt.is_some() && !semantic_refused_rows.is_empty() {
                result.conflicts = self.dependency_unit_refusal_conflicts(
                    &semantic_refused_rows,
                    policies,
                    hub_local_author,
                    read_memory.clone(),
                )?;
                let diagnostics_cover_every_member = result.conflicts.len()
                    == semantic_refused_rows.len()
                    && semantic_refused_rows.iter().all(|row| {
                        result.conflicts.iter().any(|conflict| {
                            conflict.table.as_deref() == Some(row.table.as_str())
                                && conflict.natural_key == row.natural_key
                                && Self::refusal_diagnostic_is_complete(conflict)
                        })
                    });
                if !diagnostics_cover_every_member {
                    let _ = self.rollback(tx);
                    return Err(Error::SyncError(
                        ("ordinary refusal lacked complete winner diagnostics".to_string()).into(),
                    ));
                }
            }

            // The immutable creator tuple/root must commit with the row it
            // describes.  Staging only the rows that actually won avoids both a
            // crash window after relational apply and a refused remote loser
            // overwriting the local winner's relay provenance.
            if stage_verified_lineages
                && let Err(err) = self.stage_received_row_lineages(
                    tx,
                    &accepted_authenticated_rows,
                    lineages,
                    &applied_rows_cache,
                    &accepted_sync_tombstones,
                )
            {
                let _ = self.rollback(tx);
                return Err(err);
            }

            let accepted_author_entries = match receipt.as_ref() {
                Some(receipt) => self.stage_accepted_sync_row_authors(
                    tx,
                    &receipt.node_id,
                    &accepted_authenticated_rows,
                )?,
                None => Vec::new(),
            };

            let sync_pull_trigger_audit_projection = if trigger_ddl.is_empty() {
                None
            } else {
                Some(self.sync_pull_trigger_audit_projection(&trigger_ddl)?)
            };

            // Test-only pause: every row in this changeset is already staged
            // (any stampless row's sidecar carries the sentinel, not a sampled
            // `current_lsn()`), but this changeset has not yet entered the
            // commit-lock-protected commit below. Thread-local-gated so a test
            // can pause exactly ONE concurrent apply here while a second,
            // unmarked apply on another thread passes straight through and
            // commits normally -- see `sync_apply_pre_commit_pause`'s doc comment.
            if take_sync_apply_pre_commit_pause_armed_here_for_test() {
                self.sync_apply_pre_commit_pause.maybe_pause();
            }

            if let Some(key) = push_outcome_key {
                self.stage_sync_push_outcome(tx, key, &result)?;
            }
            self.commit_adjudicated_sync_apply(
                tx,
                if source_coordinates.is_some() {
                    &[]
                } else {
                    &event_bus_ddl
                },
                &trigger_ddl,
                sync_pull_trigger_audit_projection.as_ref(),
                &accepted_sync_tombstones,
                accepted_author_entries,
                terminal_refusal_context,
                terminal_marker_clears,
                hub_local_author.is_some(),
                delivery_prepare,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            if replace_received_vector_image {
                received_image_superseded_vectors.sort_by(|left, right| {
                    left.index
                        .table
                        .cmp(&right.index.table)
                        .then(left.index.column.cmp(&right.index.column))
                        .then(left.row_id.cmp(&right.row_id))
                        .then(left.created_tx.cmp(&right.created_tx))
                        .then(left.lsn.cmp(&right.lsn))
                });
                received_image_superseded_vectors.dedup();
                if let Some(persistence) = &self.persistence
                    && !received_image_superseded_vectors.is_empty()
                {
                    persistence.prune_vector_versions_scoped(&received_image_superseded_vectors)?;
                }
                // A state carrying the accountant returns each pruned body from
                // its own record inside the prune; only bytes still charged to a
                // per-insert admission come back for this release.
                let accounted_bytes = self.vector_store.prune_superseded_versions(
                    &received_image_superseded_vectors,
                    vector_copy_owner,
                )?;
                self.finalization.accountant.release(accounted_bytes);
                let replaced_indexes = received_image_superseded_vectors
                    .iter()
                    .map(|version| version.index.clone())
                    .collect::<HashSet<_>>();
                for index in replaced_indexes {
                    self.vector_store
                        .clear_hnsw_for(&index, self.finalization.accountant.as_ref());
                }
                self.received_vector_image_pending
                    .store(false, Ordering::SeqCst);
            }
            let committed_lsn = self.current_lsn();
            result.new_lsn = committed_lsn;
            if let Some(scope) = private_scope {
                let mut effects = scope.finish()?;
                if let Some(captured) = self.detached_sync_write_set.lock().as_mut() {
                    effects.take_captured_rows_invoice(captured.rows_invoice.take());
                    captured.prefix = Some(effects);
                } else if !effects.audits.is_empty() {
                    return Err(Error::Other(
                        "private source audit capture is missing".into(),
                    ));
                }
            }
            Ok(result)
        })()
    }

    /// Commit/publish the already adjudicated sync transaction.  Keeping this
    /// boundary separate from the conflict/adoption loop is what lets the
    /// received-schema path run that loop against a detached projected image
    /// and later commit its finalized local WriteSet atomically with schema.
    /// The ordinary no-schema path still calls it immediately, unchanged.
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn commit_adjudicated_sync_apply(
        &self,
        tx: TxId,
        event_bus_ddl: &[DdlChange],
        trigger_ddl: &[DdlChange],
        sync_pull_trigger_audit_projection: Option<&BTreeMap<String, TriggerDeclaration>>,
        accepted_sync_tombstones: &[AcceptedSyncTombstone],
        accepted_author_entries: Vec<(String, Vec<u8>, String)>,
        terminal_refusal_context: Option<&TerminalRefusalPullContext>,
        terminal_marker_clears: Vec<(String, NaturalKey)>,
        sync_trigger_derived_rows_are_local: bool,
        delivery_prepare: Option<&mut dyn crate::custody::preparation::CommitPreparation>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // The transaction-manager callback publishes accepted delete
        // provenance while it still holds the commit lock, after relational
        // apply made the tombstone visible and before another commit can run.
        self.commit_with_source_and_sync_ddl_and_trigger_audit_projection(
            tx,
            CommitSource::SyncPull,
            event_bus_ddl,
            trigger_ddl,
            sync_pull_trigger_audit_projection,
            Some(accepted_sync_tombstones),
            sync_trigger_derived_rows_are_local,
            delivery_prepare,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        if !accepted_author_entries.is_empty() {
            self.accepted_sync_row_authors.write().extend(
                accepted_author_entries
                    .into_iter()
                    .map(|(table, identity, author)| ((table, identity), author)),
            );
        }
        if !terminal_marker_clears.is_empty() {
            let context = terminal_refusal_context
                .expect("terminal refusal markers can only clear during private reconciliation");
            let mut markers = self.terminal_refusal_markers.write();
            for (table, natural_key) in terminal_marker_clears {
                markers.remove(&Self::terminal_refusal_marker_key(
                    context,
                    &table,
                    &natural_key,
                ));
            }
        }
        Ok(())
    }

    /// Record the adjudicator's exact accepted occurrence, before a later
    /// member can replace its same-key cached row. This marker lives only in
    /// the private working write set and is removed before durable staging.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stage_detached_accepted_row(
        &self,
        tx: TxId,
        row: &RowChange,
        local_row_id: Option<RowId>,
        connected: bool,
        owner: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let owner: contextdb_core::read_memory::ReadMemoryOwner = owner.into();

        if !connected || !self.capture_detached_sync_write_set.load(Ordering::SeqCst) {
            return Ok(());
        }
        let key = Self::detached_accepted_row_key(row, owner)?;
        let value = RedbPersistence::encode_config_value(&local_row_id)?;
        self.tx_mgr.append_config_writes(tx, [(key, value)])?;
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn detached_accepted_row_key(
        row: &RowChange,
        owner: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<String> {
        let owner: contextdb_core::read_memory::ReadMemoryOwner = owner.into();

        // Same-key members at one source position can carry different bodies.
        // Hash the complete received image in deterministic column order;
        // natural key plus LSN alone is not an occurrence identity. This key
        // exists only in the unpublished capture and is stripped before END.
        let _credit = contextdb_core::read_memory::ReadCredit::try_new(
            owner,
            row.held_bytes()
                .saturating_mul(8)
                .saturating_add(row.values.len().saturating_mul(512))
                .saturating_add(4096),
            "sync",
            "connected_exact_member_key",
            "Raise the declared memory limit to identify a received member.",
        )?;
        let values = row
            .values
            .iter()
            .map(|(name, value)| (name.as_str(), value))
            .collect::<BTreeMap<_, _>>();
        let encoded = RedbPersistence::encode_config_value(&(
            row.table.as_str(),
            &row.natural_key,
            values,
            row.deleted,
            row.lsn,
            row.created_at,
        ))?;
        Ok(format!(
            "__contextdb.detached.accepted_row.v1.{:020}.{}",
            row.lsn.0,
            blake3::hash(&encoded).to_hex()
        ))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stage_received_row_lineages(
        &self,
        tx: TxId,
        accepted_rows: &[RowChange],
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        applied_rows_cache: &HashMap<String, Vec<VersionedRow>>,
        accepted_sync_tombstones: &[AcceptedSyncTombstone],
    ) -> Result<()> {
        if accepted_rows.is_empty() {
            return Ok(());
        }
        let mut writes = Vec::new();
        for row in accepted_rows {
            let matching = lineages
                .iter()
                .filter(|(table, natural_key, lsn, _)| {
                    table == &row.table && natural_key == &row.natural_key && *lsn == row.lsn
                })
                .collect::<Vec<_>>();
            if matching.len() != 1 {
                return Err(Error::SyncError((format!(
                    "accepted sync row {} {:?} at source LSN {} lacks exactly one verified lineage",
                    row.table, row.natural_key, row.lsn.0
                )).into()));
            }
            let (_, _, _, lineage) = matching[0];
            {
                let accepted_row_id = if let Some(owner) = self.private_replay_owner(tx) {
                    let key = Self::detached_accepted_row_key(row, owner)?;
                    self.tx_mgr
                        .with_read(tx, |ws| {
                            ws.config_writes
                                .iter()
                                .rev()
                                .find(|(candidate, _)| candidate == &key)
                                .map(|(_, value)| {
                                    RedbPersistence::decode_config_value::<Option<RowId>>(value)
                                })
                                .transpose()
                        })??
                        .flatten()
                } else {
                    None
                };
                let cached_row_id = accepted_row_id.or_else(|| {
                    applied_rows_cache
                        .get(&row.table)
                        .and_then(|rows| {
                            rows.iter()
                                .rev()
                                .find(|live| row.natural_key.matches_values(&live.values))
                        })
                        .map(|live| live.row_id)
                });
                // Generation was validated against the complete wire
                // DDL projection before row mutation. At this point a
                // same-transaction DROP/CREATE may not yet be visible
                // in the committed DDL log, so retain the proven wire
                // generation rather than rechecking stale local state.
                let generation = lineage.table_generation;
                let local_row_id = match cached_row_id {
                    Some(row_id) => Some(row_id),
                    None if row.deleted => accepted_sync_tombstones
                        .iter()
                        .find(|(table, natural_key, _, _, _)| {
                            table == &row.table && natural_key == &row.natural_key
                        })
                        .map(|(_, _, _, _, row_id)| *row_id),
                    None => {
                        let lineage_state = self.lineage_state_lock.lock();
                        self.load_row_lineage_sidecar(
                            &lineage_state,
                            &Self::durable_row_lineage_config_key(
                                &row.table,
                                &row.natural_key,
                                generation,
                            ),
                        )?
                        .and_then(|sidecar| sidecar.local_row_id)
                    }
                };
                let expected_root = format!(
                    "author:{}:{}:{}",
                    lineage.author_node_id,
                    lineage.author_database_incarnation.to_hex(),
                    lineage.author_local_mutation_position.0,
                );
                if lineage.lineage_root != expected_root {
                    return Err(Error::SyncError(
                        ("wire row lineage root does not match its immutable creator tuple"
                            .to_string())
                        .into(),
                    ));
                }
                writes.push((
                    Self::durable_row_lineage_config_key(&row.table, &row.natural_key, generation),
                    RedbPersistence::encode_config_value(&DurableRowLineageSidecar {
                        author_node_id: lineage.author_node_id.clone(),
                        author_database_incarnation: lineage.author_database_incarnation,
                        author_local_mutation_position: lineage.author_local_mutation_position,
                        table_generation: lineage.table_generation,
                        lineage_root: lineage.lineage_root.clone(),
                        lineage_attestation: lineage.attestation.clone(),
                        local_row_id,
                        locally_created: false,
                    })?,
                ));
                // The detached capture has no persistent mirror to update,
                // but its prepared stage must carry the same original author
                // as the accepted lineage sidecar.  This runs only for rows
                // the adjudicator already accepted, never by iterating the
                // wire set independently.
                if self.capture_detached_sync_write_set.load(Ordering::SeqCst) && !row.deleted {
                    let identity = sync_identity_key(&row.table, &row.natural_key);
                    self.accepted_sync_row_authors.write().insert(
                        (row.table.clone(), identity.clone()),
                        lineage.author_node_id.clone(),
                    );
                    writes.push((
                        format!("sync_row_author.{}", Self::hex_component(&identity)),
                        RedbPersistence::encode_config_value(&lineage.author_node_id)?,
                    ));
                }
                if row.deleted {
                    // A delete learned from an authoritative hub is accepted
                    // ancestry this edge now holds. Keep it in the same
                    // config-keyed lineage record used by locally accepted
                    // deletes. Redb retains the absence across restart;
                    // memory retains it for this database life before an
                    // explicit destination move re-uploads it.
                    writes.push((
                        Self::durable_lineage_config_key(&row.table, &row.natural_key, generation),
                        RedbPersistence::encode_config_value(&DurableLineageRecord {
                            table: row.table.clone(),
                            natural_key: row.natural_key.clone(),
                            table_generation: generation,
                            local_row_id,
                            locally_created: false,
                            lineage_root: lineage.lineage_root.clone(),
                            lineage_attestation: lineage.attestation.clone(),
                            author_node_id: Some(lineage.author_node_id.clone()),
                            author_database_incarnation: Some(
                                lineage.author_database_incarnation.to_hex(),
                            ),
                            author_local_mutation_position: lineage
                                .author_local_mutation_position
                                .0,
                            // The commit preparer replaces this sentinel with
                            // the exact local commit position atomically.
                            delete_lsn: 0,
                            delete_obligation: DurableDeleteObligation::Accepted,
                            accepted_hub_lsn: None,
                            bound_hub_node_id: None,
                            purge_frontier: None,
                        })?,
                    ));
                }
            }
        }
        if !writes.is_empty() {
            self.tx_mgr.append_config_writes(tx, writes)?;
        }
        Ok(())
    }

    pub(super) fn sync_push_apply_lock_for_key(&self, key: &SyncPushOutcomeKey) -> Arc<Mutex<()>> {
        // The durable retention slot is deliberately shared by every request
        // from one edge, but the in-flight lock is not. Only byte-identical
        // retries join an apply; a different request from the same edge must
        // remain free to commit while that apply is parked.
        let apply_key = format!(
            "{}.{}",
            key.storage_key,
            Self::hex_component(&key.request_digest),
        );
        let mut locks = self.sync_push_apply_locks.lock();
        locks.retain(|_, lock| lock.strong_count() != 0);
        if let Some(lock) = locks.get(&apply_key).and_then(std::sync::Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(apply_key, Arc::downgrade(&lock));
        lock
    }

    /// All ordinary clients sharing this source database and authoritative
    /// hub advance one push stream. Holding this lock through reply handling
    /// makes the next different request proof that the preceding request can
    /// no longer be retried, including when callers constructed more than one
    /// `SyncClient` over the same database handle.
    #[cfg(feature = "sync-orchestration")]
    pub(crate) fn sync_client_push_lock(
        &self,
        tenant_id: &TenantId,
        hub_node_id: &str,
    ) -> Arc<tokio::sync::Mutex<()>> {
        let key = format!(
            "{}.{}",
            Self::hex_component(tenant_id.as_str().as_bytes()),
            Self::hex_component(hub_node_id.as_bytes()),
        );
        let mut locks = self.sync_client_push_locks.lock();
        locks.retain(|_, lock| lock.strong_count() != 0);
        if let Some(lock) = locks.get(&key).and_then(std::sync::Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        locks.insert(key, Arc::downgrade(&lock));
        lock
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn sync_push_outcome_key(
        tenant_id: &TenantId,
        node_id: &str,
        request: &crate::protocol::PushRequest,
        dependency_complete: bool,
    ) -> Result<SyncPushOutcomeKey> {
        // Sort only maps. Preserve floating-point bits and every ordered
        // source occurrence; JSON encoding would collapse non-finite values.
        let changes = &request.changeset;
        let rows = changes
            .rows
            .iter()
            .map(|row| {
                (
                    &row.table,
                    &row.natural_key,
                    row.values.iter().collect::<BTreeMap<_, _>>(),
                    row.deleted,
                    row.lsn,
                    row.created_at,
                    row.arrival,
                    &row.lineage,
                )
            })
            .collect::<Vec<_>>();
        let edges = changes
            .edges
            .iter()
            .map(|edge| {
                (
                    edge.source,
                    edge.target,
                    &edge.edge_type,
                    edge.properties.iter().collect::<BTreeMap<_, _>>(),
                    edge.lsn,
                )
            })
            .collect::<Vec<_>>();
        let encoded = rmp_serde::to_vec(&(
            dependency_complete,
            request.incarnation,
            &changes.ddl,
            &changes.ddl_lsn,
            rows,
            edges,
            &changes.vectors,
            &changes.ddl_provenance,
            &changes.purges,
        ))
        .map_err(|err| Error::SyncError((err.to_string()).into()))?;
        let mut diagnostic_identities = changes
            .rows
            .iter()
            .map(|row| SyncPushDiagnosticIdentity {
                table: Some(row.table.clone()),
                natural_key: row.natural_key.clone().into(),
            })
            .collect::<Vec<_>>();
        diagnostic_identities.extend(changes.ddl.iter().filter_map(|ddl| {
            let ddl = DdlChange::from(ddl.clone());
            let table = Self::ddl_affected_table(&ddl)?.to_string();
            Some(SyncPushDiagnosticIdentity {
                table: Some(table.clone()),
                natural_key: NaturalKey::single("table".to_string(), Value::Text(table)),
            })
        }));
        Ok(SyncPushOutcomeKey {
            // The slot deliberately omits the incarnation and request hash.
            // A new incarnation supersedes the old edge life, and a new
            // request on one life follows receipt of the preceding response.
            storage_key: format!(
                "{}{}.{}",
                SYNC_PUSH_OUTCOME_PREFIX,
                Self::hex_component(tenant_id.as_str().as_bytes()),
                Self::hex_component(node_id.as_bytes()),
            ),
            incarnation: request.incarnation,
            request_digest: *blake3::hash(&encoded).as_bytes(),
            diagnostic_identities,
        })
    }

    pub(crate) fn accepted_delete_replay_conflicts(
        &self,
        changes: &ChangeSet,
        table: &str,
        key: &[(String, Value)],
    ) -> Vec<Conflict> {
        let cause = changes
            .rows
            .iter()
            .find(|row| row.table == table && row.natural_key.pairs() == key)
            .expect("accepted-delete replay names a transmitted member");
        let cause = RefusalCause {
            table: cause.table.clone(),
            natural_key: cause.natural_key.clone(),
        };
        let policies = Self::declared_sync_policies(true);
        let mut remaining = changes.rows.iter().collect::<Vec<_>>();
        let mut conflicts = Vec::new();
        while !remaining.is_empty() {
            let index = remaining
                .iter()
                .enumerate()
                .filter(|(_, row)| {
                    !remaining
                        .iter()
                        .any(|parent| dependency_row_references_parent(self, row, parent))
                })
                .min_by_key(|(_, row)| sync_row_identity(row))
                .map(|(index, _)| index)
                .unwrap_or_else(|| {
                    (0..remaining.len())
                        .min_by_key(|index| sync_row_identity(remaining[*index]))
                        .unwrap()
                });
            let row = remaining.remove(index);
            conflicts.push(Conflict {
                natural_key: row.natural_key.clone(),
                resolution: Self::sync_conflict_policy_for_table(
                    &policies,
                    self.table_meta(&row.table).as_ref(),
                    &row.table,
                ),
                reason: Some("replays_accepted_delete".to_string()),
                table: Some(row.table.clone()),
                mutation_kind: Some(if row.deleted { "delete" } else { "edit" }.to_string()),
                winning_author_node_id: None,
                hub_acceptance_position: None,
                refusal_cause: Some(cause.clone()),
            });
        }
        conflicts
    }

    /// Exact authenticated request lookup in the existing durable apply path.
    /// A retry returns the committed result; it never arbitrates old work again.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn replay_sync_push_outcome(
        &self,
        key: &SyncPushOutcomeKey,
    ) -> Result<Option<ApplyResult>> {
        let result = if let Some(persistence) = &self.persistence {
            persistence.load_config_value::<DurableSyncPushOutcome>(&key.storage_key)?
        } else {
            self.lineage_state_lock
                .lock()
                .push_outcomes
                .get(&key.storage_key)
                .cloned()
        };
        let Some(result) = result else {
            return Ok(None);
        };
        if result.incarnation != key.incarnation || result.request_digest != key.request_digest {
            return Ok(None);
        }
        let conflicts = result
            .result
            .conflicts
            .into_iter()
            .map(|conflict| {
                let identity = key
                    .diagnostic_identities
                    .get(conflict.request_identity_ordinal)
                    .ok_or_else(|| {
                        Error::SyncError(
                            ("durable push outcome names a missing request diagnostic".to_string())
                                .into(),
                        )
                    })?;
                let refusal_cause = conflict
                    .refusal_cause_identity_ordinal
                    .map(|ordinal| {
                        let cause = key.diagnostic_identities.get(ordinal).ok_or_else(|| {
                            Error::SyncError(
                                ("durable push outcome names a missing refusal cause".to_string())
                                    .into(),
                            )
                        })?;
                        Ok::<RefusalCause, Error>(RefusalCause {
                            table: cause.table.clone().ok_or_else(|| {
                                Error::SyncError(
                                    ("durable push refusal cause has no request table".to_string())
                                        .into(),
                                )
                            })?,
                            natural_key: cause.natural_key.clone(),
                        })
                    })
                    .transpose()?;
                Ok(Conflict {
                    natural_key: identity.natural_key.clone(),
                    resolution: conflict.resolution,
                    reason: conflict.reason,
                    table: conflict.table,
                    mutation_kind: conflict.mutation_kind,
                    winning_author_node_id: conflict.winning_author_node_id,
                    hub_acceptance_position: conflict.hub_acceptance_position,
                    refusal_cause,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(ApplyResult {
            applied_rows: result.result.applied_rows,
            skipped_rows: result.result.skipped_rows,
            conflicts,
            new_lsn: result.result.new_lsn,
        }))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn durable_sync_push_outcome(
        context: &SyncPushOutcomeContext,
        result: ApplyResult,
    ) -> Result<DurableSyncPushOutcome> {
        let mut claimed_identities = HashSet::new();
        let conflicts =
            result
                .conflicts
                .into_iter()
                .map(|conflict| {
                    let matches = |identity: &SyncPushDiagnosticIdentity| {
                        identity.natural_key == conflict.natural_key
                            && conflict
                                .table
                                .as_ref()
                                .is_none_or(|table| identity.table.as_ref() == Some(table))
                    };
                    let request_identity_ordinal = context
                        .key
                        .diagnostic_identities
                        .iter()
                        .enumerate()
                        .find_map(|(ordinal, identity)| {
                            (matches(identity) && !claimed_identities.contains(&ordinal))
                                .then_some(ordinal)
                        })
                        .or_else(|| context.key.diagnostic_identities.iter().position(matches))
                        .ok_or_else(|| {
                            Error::SyncError((
                            "push outcome diagnostic does not identify a transmitted row or DDL"
                                .to_string()).into())
                        })?;
                    claimed_identities.insert(request_identity_ordinal);
                    let refusal_cause_identity_ordinal =
                        conflict
                            .refusal_cause
                            .as_ref()
                            .map(|cause| {
                                context
                                    .key
                                    .diagnostic_identities
                                    .iter()
                                    .position(|identity| {
                                        identity.table.as_deref() == Some(cause.table.as_str())
                                            && identity.natural_key == cause.natural_key
                                    })
                                    .ok_or_else(|| {
                                        Error::SyncError((
                                    "push refusal cause does not identify a transmitted row"
                                        .to_string()).into())
                                    })
                            })
                            .transpose()?;
                    Ok(DurableSyncPushConflict {
                        request_identity_ordinal,
                        resolution: conflict.resolution,
                        reason: conflict.reason,
                        table: conflict.table,
                        mutation_kind: conflict.mutation_kind,
                        winning_author_node_id: conflict.winning_author_node_id,
                        hub_acceptance_position: conflict.hub_acceptance_position,
                        refusal_cause_identity_ordinal,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
        Ok(DurableSyncPushOutcome {
            incarnation: context.key.incarnation,
            request_digest: context.key.request_digest,
            result: DurableSyncPushApplyResult {
                applied_rows: result.applied_rows,
                skipped_rows: result.skipped_rows,
                conflicts,
                new_lsn: result.new_lsn,
            },
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stage_sync_push_outcome(
        &self,
        tx: TxId,
        context: &SyncPushOutcomeContext,
        result: &ApplyResult,
    ) -> Result<()> {
        let mut result = result.clone();
        result.new_lsn = context.committed_lsn.unwrap_or(Lsn(0));
        result.skipped_rows += context.refused_rows;
        result.conflicts.extend(context.conflicts.clone());
        let bytes = RedbPersistence::encode_config_value(&Self::durable_sync_push_outcome(
            context, result,
        )?)?;
        self.tx_mgr
            .append_config_writes(tx, [(context.key.storage_key.clone(), bytes)])
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stage_sync_apply_receipt(
        &self,
        tx: TxId,
        receipt: &SyncApplyReceipt,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(contact_ms) = receipt.hub_contact_ms {
            self.stage_hub_node_contact(tx, &receipt.node_id, contact_ms, read_memory.clone())?;
        }
        if let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr {
            return handle.with_host(|host| {
                host.events.with_owned_persistence_original(|original| {
                    let frame = crate::persistence::OriginalPersistenceFrame::new(original)?;
                    // Existing nested key formatting overlaps its node prefix,
                    // incarnation suffix and final tenant key while assembling.
                    let key_work = receipt
                        .node_id
                        .len()
                        .checked_mul(8)
                        .and_then(|n| {
                            n.checked_add(receipt.tenant_id.as_str().len().checked_mul(4)?)
                        })
                        .and_then(|n| n.checked_add(1024))
                        .ok_or_else(EngineMetadataJournal::identity_error)?;
                    frame.reserve(key_work)?;
                    let keys = [
                        Self::applied_push_watermark_node_incarnation_key(
                            &receipt.tenant_id,
                            &receipt.node_id,
                            receipt.incarnation,
                        ),
                        receipt.tenant_id.config_key("sync_applied_push_watermark"),
                    ];
                    let encoded = frame.encode(&receipt.source_lsn.0)?;
                    let writes = [
                        (frame.copy_text(&keys[0])?, frame.copy_bytes(&encoded)?),
                        (frame.copy_text(&keys[1])?, frame.copy_bytes(&encoded)?),
                    ];
                    let checkpoint = self.write_set_checkpoint_owned(host, tx)?;
                    let result = host.transactions.with_transactions(|transactions| {
                        transactions.with_engine(tx, |held| {
                            let maxima = held.copy_aux(
                                contextdb_tx::staged_source::AuxBorrow::ConfigMaxU64Keys(&keys),
                            )?;
                            let maxima = held.prepare_aux_splice(
                                held.read().config_max_u64_keys.len(),
                                &[],
                                maxima,
                            )?;
                            held.publish_tx(maxima);
                            let writes = held.copy_aux(
                                contextdb_tx::staged_source::AuxBorrow::ConfigWrites(&writes),
                            )?;
                            let writes = held.prepare_aux_splice(
                                held.read().config_writes.len(),
                                &[],
                                writes,
                            )?;
                            held.publish_tx(writes);
                            Ok(())
                        })
                    });
                    if result.is_err() {
                        self.restore_write_set_checkpoint(tx, checkpoint)?;
                    } else {
                        checkpoint.settle();
                    }
                    result
                })
            });
        }
        let edge_key = Self::applied_push_watermark_node_incarnation_key(
            &receipt.tenant_id,
            &receipt.node_id,
            receipt.incarnation,
        );
        let tenant_key = receipt.tenant_id.config_key("sync_applied_push_watermark");
        let encoded = RedbPersistence::encode_config_value(&receipt.source_lsn.0)?;
        self.tx_mgr.with_write_set(tx, |ws| {
            ws.config_max_u64_keys
                .extend([edge_key.clone(), tenant_key.clone()]);
            ws.config_writes
                .extend([(edge_key, encoded.clone()), (tenant_key, encoded)]);
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stage_hub_node_contact(
        &self,
        tx: TxId,
        node_id: &str,
        now_ms: i64,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.execute_in_tx_with_read_memory(
            tx,
            "INSERT INTO work_node_contacts (node_id, last_contact_ms) VALUES ($node_id, $last_contact_ms) \
             ON CONFLICT (node_id) DO UPDATE SET last_contact_ms = $last_contact_ms",
            &HashMap::from([
                ("node_id".to_string(), Value::Text(node_id.to_string())),
                ("last_contact_ms".to_string(), Value::Timestamp(now_ms)),
            ]),
         read_memory.clone())?;
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn commit_sync_apply_outcome_only(
        &self,
        receipt: &SyncApplyReceipt,
        key: Option<&SyncPushOutcomeContext>,
        result: &ApplyResult,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let tx = self.begin()?;
        if let Err(err) = self.stage_sync_apply_receipt(tx, receipt, read_memory.clone()) {
            let _ = self.rollback(tx);
            return Err(err);
        }
        if let Some(key) = key
            && let Err(err) = self.stage_sync_push_outcome(tx, key, result)
        {
            let _ = self.rollback(tx);
            return Err(err);
        }
        if self.capture_detached_sync_write_set.load(Ordering::SeqCst) {
            if matches!(&self.tx_mgr, DatabaseTransactionHost::Owned(_)) {
                self.with_preparation_original(|original| {
                    let frame = crate::persistence::OriginalPersistenceFrame::new(original)?;
                    self.tx_mgr.append_config_writes(
                        tx,
                        [
                            (
                                frame.copy_text("__contextdb.detached_capture_owner.v1")?,
                                Vec::new(),
                            ),
                            (
                                frame.copy_text("__contextdb.detached_refusal_capture.v1")?,
                                Vec::new(),
                            ),
                        ],
                    )
                })?;
            } else {
                self.tx_mgr.with_write_set(tx, |ws| {
                    ws.config_writes.extend([
                        ("__contextdb.detached_capture_owner.v1".into(), Vec::new()),
                        ("__contextdb.detached_refusal_capture.v1".into(), Vec::new()),
                    ])
                })?;
            }
        }
        self.commit_with_source(
            tx,
            CommitSource::SyncPull,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn commit_sync_apply_receipt_only(
        &self,
        receipt: &SyncApplyReceipt,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let tx = self.begin()?;
        if let Err(err) = self.stage_sync_apply_receipt(tx, receipt, read_memory.clone()) {
            let _ = self.rollback(tx);
            return Err(err);
        }
        self.commit_with_source(
            tx,
            CommitSource::SyncPull,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    /// A refused member is fully accounted for one of exactly two ways: it
    /// reports the accepted value that beat it — who wrote it and where it
    /// was accepted — or, having no accepted value at its own key, it names
    /// the row that caused its unit to be refused. Anything less leaves the
    /// sender unable to explain the outcome, so the push stays resumable
    /// instead of being retired on a half-told story.
    pub(super) fn refusal_diagnostic_is_complete(conflict: &Conflict) -> bool {
        match conflict.refusal_cause {
            Some(_) => {
                conflict.winning_author_node_id.is_none()
                    && conflict.hub_acceptance_position.is_none()
            }
            None => {
                conflict.winning_author_node_id.is_some()
                    && conflict.hub_acceptance_position.is_some()
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn dependency_unit_refusal_conflicts(
        &self,
        rows: &[RowChange],
        policies: &ConflictPolicies,
        hub_local_author: Option<&str>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<Conflict>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let snapshot = self.snapshot();
        let no_deleted = HashSet::new();
        let mut remaining = rows.iter().collect::<Vec<_>>();
        let mut ordered = Vec::with_capacity(remaining.len());

        while !remaining.is_empty() {
            let mut eligible = remaining
                .iter()
                .enumerate()
                .filter(|(_, row)| {
                    !remaining
                        .iter()
                        .any(|parent| dependency_row_references_parent(self, row, parent))
                })
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            if eligible.is_empty() {
                eligible.extend(0..remaining.len());
            }
            let selected = eligible
                .into_iter()
                .min_by_key(|index| sync_row_identity(remaining[*index]))
                .expect("non-empty dependency refusal set");
            ordered.push(remaining.remove(selected));
        }

        // A connected unit can mix a member whose key already holds an
        // accepted value with a member nobody but this sender ever wrote.
        // Only the first kind has a winner to report. The second kind is
        // still refused — the unit is decided whole — so it reports the
        // member that actually caused the refusal instead of inventing an
        // author and a position it has no basis for. Reading the cause in
        // the same deterministic parent-first order keeps it stable across
        // repeats of the same push.
        let cause = ordered
            .iter()
            .find_map(|row| {
                match self.visible_row_by_natural_key(
                    &row.table,
                    &row.natural_key,
                    snapshot,
                    &no_deleted,
                    read_memory.clone(),
                ) {
                    Ok(Some(_)) => Some(Ok(RefusalCause {
                        table: row.table.clone(),
                        natural_key: row.natural_key.clone(),
                    })),
                    Ok(None) => None,
                    Err(err) => Some(Err(err)),
                }
            })
            .transpose()?;

        ordered
            .into_iter()
            .map(|row| {
                let policy = Self::sync_conflict_policy_for_table(
                    policies,
                    self.table_meta(&row.table).as_ref(),
                    &row.table,
                );
                let winner = self.visible_row_by_natural_key(
                    &row.table,
                    &row.natural_key,
                    snapshot,
                    &no_deleted,
                read_memory.clone(),)?;
                let Some(winner) = winner else {
                    // No accepted value at this key, so nothing of this
                    // member's own to report. Without a cause to name there
                    // is no honest diagnostic at all, and the caller's
                    // completeness check keeps the push resumable.
                    return Ok(cause.clone().map(|cause| Conflict {
                        hub_acceptance_position: None,
                        natural_key: row.natural_key.clone(),
                        resolution: policy,
                        reason: Some("keep_first_refused".to_string()),
                        table: Some(row.table.clone()),
                        mutation_kind: Some(
                            if row.deleted { "delete" } else { "edit" }.to_string(),
                        ),
                        winning_author_node_id: None,
                        refusal_cause: Some(cause),
                    }));
                };
                let winner_author = {
                    let accepted_author =
                        self.accepted_sync_row_author(&row.table, &row.natural_key)?;
                    match self.relational_sync_source_at_read_root(&row.table, winner.row_id, read_memory.clone())?.1
                    {
                        Some(contextdb_relational::store::SyncSourceKind::Pulled)
                        | Some(contextdb_relational::store::SyncSourceKind::AcceptedLocalPending) => {
                            accepted_author
                        }
                        Some(contextdb_relational::store::SyncSourceKind::AcceptedLocal) | None => {
                            accepted_author.or_else(|| hub_local_author.map(str::to_string))
                        }
                    }
                };
                Ok(Some(Conflict {
                    // A synced winner carries the accepting hub's sidecar;
                    // a hub-local winner's row LSN is already its acceptance
                    // position. Never report the rejected edge's LSN here.
                    hub_acceptance_position: self.relational_sync_source_at_read_root(&row.table, winner.row_id, read_memory.clone())?.0
                        .and_then(|lsn| Self::resolve_sync_source_lsn(Some(lsn), winner.lsn))
                        .or(Some(winner.lsn)),
                    natural_key: row.natural_key.clone(),
                    resolution: policy,
                    reason: Some("keep_first_refused".to_string()),
                    table: Some(row.table.clone()),
                    mutation_kind: Some(if row.deleted { "delete" } else { "edit" }.to_string()),
                    winning_author_node_id: winner_author,
                    refusal_cause: None,
                }))
            })
            .collect::<Result<Vec<_>>>()
            .map(|conflicts| conflicts.into_iter().flatten().collect())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn restore_vector_owner_rows(
        &self,
        rows: &mut Vec<RowChange>,
        vectors: &[VectorChange],
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.restore_vector_owner_rows_projected(
            rows,
            vectors,
            &ChangeProjection::unbounded(),
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn restore_vector_owner_rows_projected(
        &self,
        rows: &mut Vec<RowChange>,
        vectors: &[VectorChange],
        projection: &ChangeProjection<'_>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if vectors.is_empty() {
            return Ok(());
        }
        if projection.bounded {
            // The set of represented rows below keys every row once more.
            (projection.admit)(rows.iter().map(row_pairing_entry_bytes).sum::<usize>())?;
        }
        // Key on the WHOLE natural key, not the leading column alone: two
        // composite-PK rows that share only their leading key are DIFFERENT rows,
        // and folding them together drops the second row's synthesized owner
        // change — leaving its vector with no remote-to-local mapping on the
        // receiver. The same whole-key basis the row-upsert dedup uses.
        let mut represented = rows
            .iter()
            .filter(|row| !row.deleted)
            .map(|row| {
                (
                    row.table.clone(),
                    format!("{:?}", row.natural_key.pairs()),
                    row.lsn,
                )
            })
            .collect::<HashSet<_>>();
        // Outbound extraction holds its captured writer root across this repair.
        // A concurrent commit must not move the logical read past that root.
        let snapshot = self.snapshot_for_read();
        for vector in vectors {
            (projection.checkpoint)()?;
            let Some(row) = self.committed_row_at(
                &vector.index.table,
                vector.row_id,
                snapshot,
                read_memory.clone(),
            )?
            else {
                continue;
            };
            if !self.row_read_allowed_for_change(
                &vector.index.table,
                &row,
                snapshot,
                read_memory.clone(),
            )? {
                continue;
            }
            let Some((natural_key, values)) = self.row_change_values_projected(
                &vector.index.table,
                &row,
                projection,
                vector_copy_owner,
                read_memory.clone(),
            )?
            else {
                continue;
            };
            let key = (
                vector.index.table.clone(),
                format!("{:?}", natural_key.pairs()),
                vector.lsn,
            );
            if !represented.insert(key) {
                continue;
            }
            rows.push(RowChange {
                table: vector.index.table.clone(),
                natural_key,
                values,
                deleted: false,
                lsn: vector.lsn,
                created_at: None,
            });
        }
        Ok(())
    }

    /// The committed columns a disk-backed writer shed from its resident
    /// directory row, read back from that row's exact durable version.
    ///
    /// A file-backed store keeps only keys, constraint columns and small
    /// values resident; large text/JSON bodies and vectors live in Redb
    /// alone. A change handed to a peer (a pushed or pulled changeset, a
    /// custody digest) must carry every committed column, so the shed ones
    /// are read here, one row at a time, when the change is built -- never by
    /// re-materialising the table. Under a bounded projection the stored size
    /// of the version is admitted before its body is read.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn shed_committed_values_for_change(
        &self,
        table: &str,
        row: &VersionedRow,
        projection: &ChangeProjection<'_>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<(String, Value)>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let Some(meta) = self.table_meta(table) else {
            return Ok(Vec::new());
        };
        if !self
            .relational_store
            .committed_row_may_omit_columns(&meta, row)
        {
            return Ok(Vec::new());
        }
        let durable = if let Some(source) = &self.retained_relational_source {
            source.relational_row_by_id_lsn_with_checkpoint(
                table,
                &meta,
                row.row_id,
                row.lsn,
                read_memory.clone(),
                &mut || (projection.checkpoint)(),
            )?
        } else if let Some(root) = self.writer_read_root()? {
            let source = self.statement_relational_source(root, read_memory.clone())?;
            let mut key = [0u8; 24];
            key[..8].copy_from_slice(&row.row_id.0.to_be_bytes());
            key[8..16].copy_from_slice(&row.created_tx.0.to_be_bytes());
            key[16..].copy_from_slice(&row.lsn.0.to_be_bytes());
            if let Some(window) =
                source.exact_historical_row_with_checkpoint(table, &key, &mut || {
                    (projection.checkpoint)()
                })?
            {
                if projection.bounded {
                    (projection.admit)(window.value().estimated_bytes())?;
                }
                Some(self.clone_writer_row(window.value(), read_memory.clone())?)
            } else {
                None
            }
        } else {
            if self.persistence.is_none() {
                return Ok(Vec::new());
            }
            return Err(Error::Other(
                "disk-backed row extraction requires its captured read root".into(),
            ));
        };
        let Some(durable) = durable else {
            return Err(Error::Other(
                (format!(
                    "committed row {} of table {table} at lsn {} has no durable body to read \
                 its paged columns from; refusing to hand on the row without them",
                    row.row_id.0, row.lsn.0
                ))
                .into(),
            ));
        };
        Ok(durable
            .values
            .into_iter()
            .filter(|(column, _)| !row.values.contains_key(column))
            .collect())
    }

    /// The row version a change names. On the writer route `admit_copy`
    /// admits the version's copy before it is made.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn row_for_change(
        &self,
        table: &str,
        row_id: RowId,
        lsn: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        checkpoint: &dyn Fn() -> Result<()>,
        admit_copy: Option<&dyn Fn(usize) -> Result<()>>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(source) = &self.retained_relational_source {
            // A change to a table this image no longer declares has no row to
            // answer, exactly as in a live store: the table was dropped after
            // the change was committed.
            let Some(meta) = self.table_meta(table) else {
                return Ok(None);
            };
            return source.relational_row_by_id_lsn_with_checkpoint(
                table,
                &meta,
                row_id,
                lsn,
                read_memory.clone(),
                &mut || checkpoint(),
            );
        }
        if let Some(root) = self.writer_read_root()? {
            // The version a change names is read by one exact seek at the
            // snapshot of its own commit, after authenticating the row table's
            // history root: never a walk of the row's other versions.
            checkpoint()?;
            let Some(tx) = root.transaction_at_lsn(
                crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                lsn,
            )?
            else {
                return Ok(None);
            };
            let mut exact = [0u8; 24];
            exact[..8].copy_from_slice(&row_id.0.to_be_bytes());
            exact[8..16].copy_from_slice(&tx.0.to_be_bytes());
            exact[16..].copy_from_slice(&lsn.0.to_be_bytes());
            if let Some(window) = root.authored_row_at(
                crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                table,
                &exact,
            )? {
                if window.value.lsn != lsn {
                    return Err(crate::funded_error::corruption_text(
                        Ok(read_memory.clone()),
                        table,
                        "retained body does not match the selected change LSN".into(),
                    ));
                }
                if let Some(admit) = admit_copy {
                    admit(
                        std::mem::size_of::<VersionedRow>()
                            .saturating_add(held::values(&window.value.values)),
                    )?;
                }
                return self
                    .clone_writer_row(&window.value, read_memory.clone())
                    .map(Some);
            }
            let Some(meta) = root.borrow_captured_table_meta(table) else {
                return Ok(None);
            };
            let snapshot = SnapshotId(tx.0);
            let source = self.statement_relational_source(root.clone(), read_memory.clone())?;
            if meta.indexes.is_empty() {
                source.row_point_preflight(table, snapshot, &mut || checkpoint())?;
            } else {
                for index in &meta.indexes {
                    source.indexed_point_preflight(table, &index.name, snapshot, &mut || {
                        checkpoint()
                    })?;
                }
            }
            if let Some(window) = source.indexed_point_row(table, row_id, snapshot)? {
                if window.value().lsn != lsn {
                    return Err(crate::funded_error::corruption_display(
                        Ok(read_memory.clone()),
                        table,
                        format_args!(
                            "selected change at LSN {} resolves to row LSN {}",
                            lsn.0,
                            window.value().lsn.0
                        ),
                    ));
                }
                if let Some(admit) = admit_copy {
                    admit(
                        std::mem::size_of::<VersionedRow>()
                            .saturating_add(held::values(&window.value().values)),
                    )?;
                }
                return self
                    .clone_writer_row(window.value(), read_memory.clone())
                    .map(Some);
            }
            // Compacted or pruned with its change-log entry: no row to answer.
            return Ok(None);
        }
        self.resident_row_for_change(table, row_id, lsn, read_memory, checkpoint, admit_copy)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn resident_row_for_change(
        &self,
        table: &str,
        row_id: RowId,
        lsn: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        checkpoint: &dyn Fn() -> Result<()>,
        admit_copy: Option<&dyn Fn(usize) -> Result<()>>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let copy = |row: &VersionedRow| {
            checkpoint()?;
            if let Some(admit) = admit_copy {
                admit(
                    std::mem::size_of::<VersionedRow>().saturating_add(held::values(&row.values)),
                )?;
                Ok(row.clone())
            } else {
                self.clone_writer_row(row, read_memory.clone())
            }
        };
        if let Some(row) = self
            .relational_store
            .with_authored_row(table, row_id, lsn, |found| {
                found.map(|(_, row)| copy(row)).transpose()
            })?
        {
            return Ok(Some(row));
        }
        let snapshot = self.history_change_snapshot_at(lsn, read_memory.clone(), checkpoint)?;
        let _probe_key = contextdb_core::read_memory::ReadCredit::try_new(
            read_memory.clone(),
            held::text(table),
            "reader",
            "pull_source_window",
            "Raise the declared memory limit to admit this history probe.",
        )?;
        let tables = self.relational_store.tables.read();
        let row = tables.get(table).and_then(|rows| {
            self.relational_store
                .visible_version_position(table, rows, row_id, snapshot)
                .and_then(|position| rows.get(position))
        });
        let Some(row) = row else {
            return Ok(None);
        };
        if row.lsn != lsn {
            return Err(crate::funded_error::corruption_display(
                Ok(read_memory.clone()),
                table,
                format_args!(
                    "selected change at LSN {} resolves to row LSN {}",
                    lsn.0, row.lsn.0
                ),
            ));
        }
        copy(row).map(Some)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn history_change_snapshot_at(
        &self,
        lsn: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        checkpoint: &dyn Fn() -> Result<()>,
    ) -> Result<SnapshotId> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        checkpoint()?;
        let lsn = self.connected_history_end(lsn, read_memory.clone())?;
        let snapshot = match self.image_history.as_ref() {
            Some(_) => self.tx_mgr.try_snapshot_at_lsn_with_context(
                lsn,
                &contextdb_tx::ReadCommitContext::new(read_memory, checkpoint),
            )?,
            None => self.tx_mgr.snapshot_at_lsn(lsn),
        };
        checkpoint()?;
        Ok(snapshot)
    }

    pub(super) fn snapshot_before_lsn(&self, lsn: Lsn) -> SnapshotId {
        self.snapshot_at(Lsn(lsn.0.saturating_sub(1)))
    }

    /// A row version for a change, its copy admitted first under a bounded
    /// projection.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn row_for_change_projected(
        &self,
        table: &str,
        row_id: RowId,
        lsn: Lsn,
        projection: &ChangeProjection<'_>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if projection.bounded
            && self.retained_relational_source.is_none()
            && self.persistence.is_none()
        {
            return self.resident_row_for_change(
                table,
                row_id,
                lsn,
                read_memory,
                projection.checkpoint,
                Some(projection.admit),
            );
        }
        // On the writer route the version's copy is admitted before it is
        // made; the other routes already hold the version they answer.
        let writer_route = self.retained_relational_source.is_none() && self.persistence.is_some();
        let row = self.row_for_change(
            table,
            row_id,
            lsn,
            read_memory.clone(),
            projection.checkpoint,
            (projection.bounded && writer_route).then_some(projection.admit),
        )?;
        if projection.bounded
            && !writer_route
            && let Some(row) = row.as_ref()
        {
            // The version read for this change lives until the change is
            // built; one row's worth is the most that is ever read ahead of
            // its admission.
            (projection.admit)(
                std::mem::size_of::<VersionedRow>().saturating_add(held::values(&row.values)),
            )?;
        }
        Ok(row)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_change_table_meta<T>(
        &self,
        table: &str,
        row: &VersionedRow,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        consume: impl FnOnce(Option<&TableMeta>) -> Result<T>,
    ) -> Result<T> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(root) = self.writer_read_root()? {
            let declaration = RedbPersistence::declared_meta_in_read(
                &root,
                crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                table,
                row.lsn,
            )?;
            return consume(declaration.as_ref().map(|window| &window.value.0));
        }
        // Retirement keeps the complete declaration beside its original body.
        // The closure borrows both only while the revocable store root is locked.
        let metadata = self.relational_store.table_meta.read();
        self.relational_store
            .with_authored_row(table, row.row_id, row.lsn, |found| {
                if let Some((meta, _)) = found {
                    return consume(Some(meta));
                }
                consume(metadata.get(table))
            })
    }

    /// [`Self::row_change_values_from_row`], with the copied values admitted
    /// before they are copied under a bounded projection.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn row_change_values_projected(
        &self,
        table: &str,
        row: &VersionedRow,
        projection: &ChangeProjection<'_>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<(NaturalKey, HashMap<String, Value>)>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.row_change_values_projected_at(
            table,
            row,
            projection,
            SnapshotId(row.created_tx.0),
            vector_copy_owner,
            read_memory,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn row_change_values_projected_at(
        &self,
        table: &str,
        row: &VersionedRow,
        projection: &ChangeProjection<'_>,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<(NaturalKey, HashMap<String, Value>)>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if !projection.bounded {
            return self.row_change_values_from_row_at(
                table,
                row,
                snapshot,
                vector_copy_owner,
                read_memory.clone(),
            );
        }
        let natural_key = self.with_change_table_meta(table, row, read_memory.clone(), |meta| {
            let Some(meta) = meta else {
                return Ok(None);
            };
            crate::sync_types::natural_key_from_row_values_admitted(
                meta,
                &row.values,
                projection.admit,
            )
        })?;
        let Some(natural_key) = natural_key else {
            return Ok(None);
        };
        let copied = held::values(&row.values);
        (projection.admit)(answer_list_entry_bytes::<RowChange>(
            std::mem::size_of::<RowChange>()
                .saturating_add(held::text(table))
                .saturating_add(copied),
        ))?;
        let mut values = row
            .values
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<HashMap<_, _>>();
        // The shed columns' stored size was admitted before they were read.
        values.extend(self.shed_committed_values_for_change(
            table,
            row,
            projection,
            read_memory.clone(),
        )?);
        self.fill_quantized_columns(
            table,
            row,
            snapshot,
            &mut values,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        Ok(Some((natural_key, values)))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn row_change_values_from_row(
        &self,
        table: &str,
        row: &VersionedRow,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<(NaturalKey, HashMap<String, Value>)>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.row_change_values_from_row_at(
            table,
            row,
            SnapshotId(row.created_tx.0),
            vector_copy_owner,
            read_memory,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn row_change_values_from_row_at(
        &self,
        table: &str,
        row: &VersionedRow,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<(NaturalKey, HashMap<String, Value>)>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let natural_key = self.with_change_table_meta(table, row, read_memory.clone(), |meta| {
            Ok(meta.and_then(|meta| natural_key_from_row_values(meta, &row.values)))
        })?;
        let Some(natural_key) = natural_key else {
            return Ok(None);
        };

        let mut values = row
            .values
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<HashMap<_, _>>();
        values.extend(self.shed_committed_values_for_change(
            table,
            row,
            &ChangeProjection::unbounded(),
            read_memory.clone(),
        )?);
        self.fill_quantized_columns(
            table,
            row,
            snapshot,
            &mut values,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        Ok(Some((natural_key, values)))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn row_id_for_natural_key(
        &self,
        table: &str,
        key_col: &str,
        key_value: &Value,
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<RowId>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        Ok(self
            .required_indexed_visible_row_by_column(
                table,
                key_col,
                key_value,
                snapshot,
                &HashSet::new(),
                read_memory.clone(),
            )?
            .map(|row| row.row_id))
    }

    /// The visible row carrying a whole sync identity, matched on every key
    /// column — the composite-aware sibling of [`Self::row_id_for_natural_key`].
    // Like existing row scopes, custody metadata follows the root's visibility.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn delivery_root_visible(
        &self,
        table: &str,
        key: &NaturalKey,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if self.access_is_admin() {
            return Ok(true);
        }
        let snapshot = self.snapshot_for_read();
        if self.table_meta(table).is_none() {
            return Ok(false);
        }
        // Inspect the named root through its covering key
        // index; scoped metadata must not scan application rows per receipt.
        let Some(row) = self.visible_row_by_natural_key(
            table,
            key,
            snapshot,
            &HashSet::new(),
            read_memory.clone(),
        )?
        else {
            return Ok(false);
        };
        Ok(!self
            .filter_rows_for_read(table, vec![row], snapshot, read_memory.clone())?
            .is_empty())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn row_id_for_natural_key_full(
        &self,
        table: &str,
        natural_key: &NaturalKey,
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<RowId>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        Ok(self
            .visible_row_by_natural_key(
                table,
                natural_key,
                snapshot,
                &HashSet::new(),
                read_memory.clone(),
            )?
            .map(|row| row.row_id))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn row_read_allowed_for_change(
        &self,
        table: &str,
        row: &VersionedRow,
        snapshot: SnapshotId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.with_change_table_meta(table, row, read_memory.clone(), |meta| {
            let Some(meta) = meta else {
                return Ok(false);
            };
            let mut credit = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
            self.bounded_read_allowed_for_row(
                None,
                table,
                meta,
                row,
                snapshot,
                |bytes| {
                    credit.try_grow_for(
                        bytes,
                        "reader",
                        "change_acl",
                        "Reduce the selected rows or raise the declared read memory limit.",
                    )
                },
                read_memory.clone(),
            )
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn row_id_read_allowed_for_change(
        &self,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut credit = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        let Some(row) = self.committed_row_at_admitted(
            table,
            row_id,
            snapshot,
            &mut credit,
            read_memory.clone(),
        )?
        else {
            return Ok(false);
        };
        self.row_read_allowed_for_change(table, &row, snapshot, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn graph_edge_read_allowed_for_change(
        &self,
        source: NodeId,
        target: NodeId,
        edge_type: &str,
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        Ok(
            self.node_read_allowed(source, snapshot, read_memory.clone())?
                && self.node_read_allowed(target, snapshot, read_memory.clone())?
                && self.edge_read_allowed(
                    source,
                    target,
                    edge_type,
                    snapshot,
                    read_memory.clone(),
                )?,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn edge_properties(
        &self,
        source: NodeId,
        target: NodeId,
        edge_type: &str,
        lsn: Lsn,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<HashMap<String, Value>>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let root = self
            .graph_read_root()?
            .map(|root| root.snapshot(root.frontier(), read_memory.clone()))
            .transpose()?;
        let mut properties = None;
        let mut memory = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        self.graph_store.visit_edges(
            root.as_ref(),
            contextdb_graph::source::GraphQuery {
                node: source,
                direction: Direction::Outgoing,
                identity: Some((target, edge_type)),
            },
            |edge| {
                if edge.lsn == lsn {
                    memory.try_grow_for(
                        edge.estimated_bytes(),
                        "writer_source",
                        "edge_history_output",
                        "Raise MEMORY_LIMIT to return edge history.",
                    )?;
                    properties = Some(edge.properties.clone());
                    return Ok(false);
                }
                Ok(true)
            },
        )?;
        self.retain_writer_output(memory);
        Ok(properties)
    }
}
