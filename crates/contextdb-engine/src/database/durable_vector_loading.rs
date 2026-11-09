use super::*;

impl CursorIdleWindow {
    pub(crate) fn new(clock: Arc<dyn DeadlineClock>, idle_ms: u64, opened_ms: u64) -> Self {
        Self {
            clock,
            idle_ms,
            last_used_ms: AtomicU64::new(opened_ms),
        }
    }

    /// Record that the holder just used the cursor.
    pub(crate) fn touch(&self, now_ms: u64) {
        self.last_used_ms.store(now_ms, Ordering::SeqCst);
    }

    /// `true` while the holder is still inside its idle window. A clock that
    /// reads BACKWARDS yields a zero-length idle here rather than an early
    /// expiry: removal stays on the safe side and keeps deferring, and the
    /// cursor's own next-page check is the one that reports the disordered
    /// clock.
    pub(super) fn holder_still_entitled(&self) -> bool {
        let idle = self
            .clock
            .now_ms()
            .saturating_sub(self.last_used_ms.load(Ordering::SeqCst));
        idle <= self.idle_ms
    }
}

impl SnapshotFloorRegistry {
    /// Whether an in-flight removal pass must hold a registration for
    /// `snapshot` until the pass finishes.
    ///
    /// Blocking rule: a pass can only ever prune a version superseded AT OR
    /// BEFORE its own sampled `watermark` -- every row it can see was
    /// committed under the SAME commit-lock hold that produced `watermark`
    /// (`compact_currency_versions_inner` runs entirely inside
    /// `with_commit_lock`, so nothing commits in between). A registration
    /// for a snapshot STRICTLY AFTER `watermark` can therefore never
    /// resolve a version this pass might remove (its `deleted_tx` is
    /// `<= watermark < snapshot`, so `VersionedRow::visible_at` is false
    /// regardless), and may register immediately. A snapshot AT OR BEFORE
    /// `watermark` might be exactly what this pass is deciding about, and
    /// this pass's own deferral only consulted the registered set at ITS
    /// start -- which cannot already include a registration racing in now
    /// -- so it must wait for the pass to finish rather than register onto
    /// a sample that will never account for it.
    pub(super) fn pass_holds(state: &SnapshotFloorState, snapshot: SnapshotId) -> bool {
        state
            .active_pass
            .is_some_and(|pass| snapshot.0 <= pass.watermark.0)
    }

    /// Racy peek like the one above, ignoring construction floors (see
    /// `begin_vector_removal_pass`).
    pub(super) fn no_snapshots_besides_construction_floors(&self) -> bool {
        let floors = self.construction_floors.lock();
        self.state
            .lock()
            .active
            .keys()
            .all(|token| floors.values().any(|floor| floor == token))
    }

    /// Hold `snapshot` for `partition`'s unpublished paged build. Called at
    /// open before cleanup can run and at build start under the commit gate,
    /// where no removal pass can be sampling concurrently.
    pub(super) fn hold_construction_floor(
        &self,
        partition: &VectorPartitionRef,
        snapshot: SnapshotId,
    ) {
        let mut floors = self.construction_floors.lock();
        let mut state = self.state.lock();
        if let Some(token) = floors.get(partition) {
            if state
                .active
                .get(token)
                .is_some_and(|registered| registered.snapshot == snapshot)
            {
                return;
            }
            state.active.remove(token);
        }
        let token = self.next_token.fetch_add(1, Ordering::SeqCst);
        state.active.insert(
            token,
            RegisteredSnapshot {
                snapshot,
                native_root: None,
                class: SnapshotRegistrationClass::ReaderHeld,
                idle_window: None,
            },
        );
        floors.insert(partition.clone(), token);
    }

    pub(super) fn release_construction_floor(&self, partition: &VectorPartitionRef) {
        let Some(token) = self.construction_floors.lock().remove(partition) else {
            return;
        };
        self.state.lock().active.remove(&token);
    }

    pub(super) fn register(self: &Arc<Self>, snapshot: SnapshotId) -> SnapshotRegistration {
        self.register_as(snapshot, SnapshotRegistrationClass::ReaderHeld)
    }

    pub(super) fn register_as(
        self: &Arc<Self>,
        snapshot: SnapshotId,
        class: SnapshotRegistrationClass,
    ) -> SnapshotRegistration {
        let mut state = self.state.lock();
        while Self::pass_holds(&state, snapshot) {
            self.pass_finished.wait(&mut state);
        }
        self.admit(state, snapshot, class, None)
    }

    /// Register one statement and remember its exact token on this thread.
    /// The separate wrapper is deliberately not `Send`: its thread-local
    /// nesting marker must be removed on the same thread that installed it.
    pub(super) fn register_statement(
        self: &Arc<Self>,
        snapshot: SnapshotId,
    ) -> StatementSnapshotRegistration {
        let registration = self.register_as(snapshot, SnapshotRegistrationClass::Statement);
        STATEMENT_SNAPSHOT_REGISTRATION_STACK
            .with(|stack| stack.borrow_mut().push(registration.token));
        StatementSnapshotRegistration {
            registration,
            _same_thread: std::marker::PhantomData,
        }
    }

    pub(super) fn register_statement_root(
        self: &Arc<Self>,
        snapshot: SnapshotId,
        has_root: bool,
    ) -> StatementSnapshotRegistration {
        if !has_root {
            return self.register_statement(snapshot);
        }
        let registration = self.admit(
            self.state.lock(),
            snapshot,
            SnapshotRegistrationClass::Statement,
            None,
        );
        STATEMENT_SNAPSHOT_REGISTRATION_STACK
            .with(|stack| stack.borrow_mut().push(registration.token));
        StatementSnapshotRegistration {
            registration,
            _same_thread: std::marker::PhantomData,
        }
    }

    /// Capture a memory publication and register its floor under the same
    /// removal-pass exclusion. A later publication cannot invalidate this
    /// registered generation; no writer serialization lock is acquired.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn register_published_with_budget(
        self: &Arc<Self>,
        capture: impl Fn() -> Result<(SnapshotId, Lsn)>,
        idle_window: Option<Arc<CursorIdleWindow>>,
        budget: &ReadRegistrationBudget,
        withdrawn: &OwnerReadCancellation,
    ) -> Result<Option<(SnapshotId, Lsn, SnapshotRegistration)>> {
        let wake_on_cancel = Arc::clone(&self.pass_finished);
        let state_on_cancel = Arc::clone(&self.state);
        let _cancel_listener = withdrawn.try_tell_on_cancel(move || {
            let _state = state_on_cancel.lock();
            wake_on_cancel.notify_all();
        })?;
        let mut state = self.state.lock();
        loop {
            if withdrawn.is_cancelled() {
                return Ok(None);
            }
            let remaining = budget.remaining()?;
            let (snapshot, lsn) = capture()?;
            if !Self::pass_holds(&state, snapshot) {
                let registration = self.admit(
                    state,
                    snapshot,
                    SnapshotRegistrationClass::ReaderHeld,
                    idle_window,
                );
                return Ok(Some((snapshot, lsn, registration)));
            }
            self.pass_finished
                .wait_for(&mut state, remaining.min(Duration::from_millis(5)));
        }
    }

    /// A durable root already protects the physical versions selected by this
    /// read. Register its logical floor without waiting for a sampled removal
    /// pass; its source handle remains owned by the reader through completion.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn register_pinned_root_with_budget(
        self: &Arc<Self>,
        snapshot: SnapshotId,
        idle_window: Option<Arc<CursorIdleWindow>>,
        budget: &ReadRegistrationBudget,
        withdrawn: &OwnerReadCancellation,
    ) -> Result<Option<SnapshotRegistration>> {
        if withdrawn.is_cancelled() {
            return Ok(None);
        }
        budget.remaining()?;
        let state = self.state.lock();
        if withdrawn.is_cancelled() {
            return Ok(None);
        }
        Ok(Some(self.admit(
            state,
            snapshot,
            SnapshotRegistrationClass::ReaderHeld,
            idle_window,
        )))
    }

    /// Take the registration slot under the lock the caller already holds,
    /// so nothing can begin a pass between the admission decision above and
    /// the registered set growing here.
    pub(super) fn admit(
        self: &Arc<Self>,
        mut state: MutexGuard<'_, SnapshotFloorState>,
        snapshot: SnapshotId,
        class: SnapshotRegistrationClass,
        idle_window: Option<Arc<CursorIdleWindow>>,
    ) -> SnapshotRegistration {
        let token = self.next_token.fetch_add(1, Ordering::SeqCst);
        state.active.insert(
            token,
            RegisteredSnapshot {
                snapshot,
                native_root: None,
                class,
                idle_window,
            },
        );
        drop(state);
        SnapshotRegistration {
            registry: self.clone(),
            token,
        }
    }

    /// Begin a removal pass: clone the currently-registered snapshot set
    /// (sorted) and record `watermark` (the committed watermark the caller
    /// already sampled via `Database::committed_watermark`, under the same
    /// commit-lock hold the whole pass runs inside) as this pass's own,
    /// atomically with each other under `state`'s single lock -- a
    /// registration can never land between these two reads un-observed.
    /// Returns a guard that clears the marker and wakes every blocked
    /// `register` on drop -- hold it for the pass's FULL duration,
    /// including its persisted removal, so a late registration can never
    /// land on a sample the pass no longer accounts for. See
    /// `SnapshotFloorState::active_pass`'s doc comment.
    pub(super) fn begin_removal_pass(self: &Arc<Self>, watermark: TxId) -> ActiveRemovalPassGuard {
        self.begin_removal_pass_inner(watermark, None, true)
    }

    /// Retention reclaims only versions no registered statement, explicit
    /// snapshot pin, or suspended bounded cursor can still observe.
    pub(super) fn begin_retention_removal_pass(
        self: &Arc<Self>,
        watermark: TxId,
    ) -> ActiveRemovalPassGuard {
        self.begin_removal_pass_inner(watermark, None, true)
    }

    /// Begin the commit-time pass while omitting at most the exact statement
    /// registration that is publishing this commit. An explicit pin at the
    /// same snapshot has a different token and remains in the sample, as do
    /// every concurrent statement, suspended cursor, and outer callback.
    pub(super) fn begin_removal_pass_excluding(
        self: &Arc<Self>,
        watermark: TxId,
        excluded_registration: Option<u64>,
    ) -> ActiveRemovalPassGuard {
        self.begin_removal_pass_inner(watermark, excluded_registration, true)
    }

    /// Removal pass for vector partition retirement and retired graph
    /// reclamation. A construction floor protects only its own partition's
    /// source rows, never a partition state or a retired graph generation, so
    /// those registrations are left out of this sample; relational version
    /// cleanup keeps honoring them through the ordinary passes.
    pub(super) fn begin_vector_removal_pass(
        self: &Arc<Self>,
        watermark: TxId,
    ) -> ActiveRemovalPassGuard {
        let floors = self.construction_floors.lock();
        let mut state = self.state.lock();
        let mut registered_snapshots: Vec<SnapshotId> = state
            .active
            .iter()
            .filter(|(token, registered)| {
                !floors.values().any(|floor| floor == *token) && registered.holder_still_entitled()
            })
            .map(|(_, registered)| registered.snapshot)
            .collect();
        drop(floors);
        registered_snapshots.sort_unstable();
        let history_through = state
            .active
            .values()
            .filter_map(|registered| match registered.class {
                SnapshotRegistrationClass::PullHistory { through } => Some(through),
                _ => None,
            })
            .max();
        state.active_pass = Some(ActiveRemovalPass { watermark });
        drop(state);
        ActiveRemovalPassGuard {
            registry: self.clone(),
            registered_snapshots: registered_snapshots.into(),
            history_through,
        }
    }

    pub(super) fn begin_removal_pass_inner(
        self: &Arc<Self>,
        watermark: TxId,
        excluded_registration: Option<u64>,
        include_statement_registrations: bool,
    ) -> ActiveRemovalPassGuard {
        let mut state = self.state.lock();
        // A registration whose holder is past its own idle deadline is left
        // out: the engine already refuses that holder's next page, so it is
        // no longer entitled to hold anything back. See
        // `RegisteredSnapshot::holder_still_entitled`.
        let mut registered_snapshots: Vec<SnapshotId> = state
            .active
            .iter()
            .filter(|(token, registered)| {
                Some(**token) != excluded_registration
                    && (include_statement_registrations
                        || registered.class != SnapshotRegistrationClass::Statement)
                    && registered.holder_still_entitled()
            })
            .map(|(_, registered)| registered.snapshot)
            .collect();
        registered_snapshots.sort_unstable();
        let history_through = state
            .active
            .values()
            .filter_map(|registered| match registered.class {
                SnapshotRegistrationClass::PullHistory { through } => Some(through),
                _ => None,
            })
            .max();
        state.active_pass = Some(ActiveRemovalPass { watermark });
        drop(state);
        ActiveRemovalPassGuard {
            registry: self.clone(),
            registered_snapshots: registered_snapshots.into(),
            history_through,
        }
    }
}

impl ActiveRemovalPassGuard {
    /// Inspect one real reader root at a time, outside the registry lock.
    /// The token horizon bounds the pass even under continuous reader arrivals.
    /// Newer selected generations do not need obsolete current-tree entries;
    /// their actual read transactions still hold the ordinary physical floor.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn native_readers_allow_cleanup(
        &self,
        generation: u64,
        basis: SnapshotId,
        mut selected: impl FnMut(crate::persistence::WriterSnapshotHandle) -> Result<Option<u64>>,
    ) -> Result<bool> {
        let through = self.registry.next_token.load(Ordering::SeqCst);
        let mut after = None;
        loop {
            let next = {
                let floors = self.registry.construction_floors.lock();
                let state = self.registry.state.lock();
                state
                    .active
                    .iter()
                    .filter(|(token, registered)| {
                        **token < through
                            && after.is_none_or(|after| **token > after)
                            && !floors.values().any(|floor| floor == *token)
                            && registered.holder_still_entitled()
                    })
                    .min_by_key(|(token, _)| **token)
                    .map(|(token, registered)| {
                        (*token, registered.snapshot, registered.native_root.clone())
                    })
            };
            let Some((token, snapshot, root)) = next else {
                return Ok(true);
            };
            after = Some(token);
            if let Some(root) = root.and_then(|root| root.upgrade()) {
                if selected(root)?.is_some_and(|selected| selected < generation) {
                    return Ok(false);
                }
            } else if snapshot < basis {
                // No retained native root proves this older reader independent
                // of current-tree history. Preserve that history conservatively.
                return Ok(false);
            }
        }
    }

    pub(super) fn protects_history(&self, lsn: Lsn) -> bool {
        self.history_through.is_some_and(|through| lsn <= through)
    }

    /// Every snapshot registered when this pass began, sorted ascending.
    pub(super) fn registered_snapshots(&self) -> &[SnapshotId] {
        &self.registered_snapshots
    }
}

impl Drop for ActiveRemovalPassGuard {
    fn drop(&mut self) {
        self.registry.state.lock().active_pass = None;
        self.registry.pass_finished.notify_all();
    }
}

impl Drop for SnapshotRegistration {
    fn drop(&mut self) {
        self.registry.state.lock().active.remove(&self.token);
    }
}

impl Drop for StatementSnapshotRegistration {
    fn drop(&mut self) {
        STATEMENT_SNAPSHOT_REGISTRATION_STACK.with(|stack| {
            let mut stack = stack.borrow_mut();
            let position = stack
                .iter()
                .rposition(|token| *token == self.registration.token);
            debug_assert!(position.is_some(), "statement snapshot token is registered");
            if let Some(position) = position {
                stack.remove(position);
            }
        });
    }
}

/// Whether ANY snapshot in `registered_snapshots` (sorted ascending) is
/// still entitled to see `row` -- the generalization of
/// [`VersionedRow::visible_at`] to a full registered set rather than one
/// floor value. The smallest registered snapshot at or after
/// `row.created_tx` is the only candidate worth checking: a LARGER
/// registered snapshot can only ever have an EQUAL or LATER
/// `row.deleted_tx` comparison point, never an earlier one, so if the
/// smallest such candidate cannot see the row, no larger one can either --
/// this makes the check `O(log n)` per version (a binary search) rather
/// than `O(n)`.
pub(super) fn any_registered_snapshot_sees(
    registered_snapshots: &[SnapshotId],
    row: &VersionedRow,
) -> bool {
    any_registered_snapshot_sees_version(registered_snapshots, row.created_tx, row.deleted_tx)
}

/// The same question for an ADJACENCY entry. An edge carries its own
/// creation and deletion stamps, and a schema-free store lets it point at an
/// id that carries no row at all, so whether a reader can still see the edge
/// is never answerable from the node rows around it -- see
/// [`any_registered_snapshot_sees_version`].
pub(super) fn any_registered_snapshot_sees_edge(
    registered_snapshots: &[SnapshotId],
    entry: &AdjEntry,
) -> bool {
    any_registered_snapshot_sees_version(registered_snapshots, entry.created_tx, entry.deleted_tx)
}

/// Whether any registered snapshot still resolves a version created at
/// `created_tx` and deleted at `deleted_tx` -- the shared rule behind both
/// helpers above, with the same binary search over the sorted registered
/// set.
pub(super) fn any_registered_snapshot_sees_version(
    registered_snapshots: &[SnapshotId],
    created_tx: TxId,
    deleted_tx: Option<TxId>,
) -> bool {
    let idx = registered_snapshots.partition_point(|s| s.0 < created_tx.0);
    match registered_snapshots.get(idx) {
        Some(candidate) => deleted_tx.is_none_or(|deleted| candidate.0 < deleted.0),
        None => false,
    }
}

impl Drop for DurableRawVectorPartitionLoader {
    fn drop(&mut self) {
        // Finish route and metadata-credit destruction while the source's
        // immutable slot still owns the original writer epoch, if any.
        self.partition.index = VectorIndexRef::default();
        self.partition.partition_key = VectorPartitionKey::unpartitioned();
        drop(self.metadata_copy_owner.take());
    }
}

impl DurableRawVectorSource {
    // derive from both the original source witness and supplied payer.
    // Production writer constructors attach this exact live pair first. The
    // absent/mismatched/committed-image constructors are reader-only and are
    // never repurposed by a later first matching writer attachment.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn vector_budget(
        &self,
        accountant: &crate::memory_accounting::AccountantHandle,
    ) -> Result<contextdb_vector::MemoryBudgetOwner> {
        if let Self::Writable(persistence) = self
            && let Some(original) = persistence.proof_accountant_handle_for(accountant)
        {
            return original.vector_budget();
        }
        accountant.vector_budget_for_original_reader()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stream_memberships<T>(
        &self,
        partition: &VectorPartitionRef,
        dimension: usize,
        quantization: VectorQuantization,
        prepare: impl FnOnce(usize) -> Result<T>,
        visit: impl FnMut(
            &mut T,
            VectorPartitionMembershipRecord,
            &mut dyn FnMut(&VectorPartitionMembershipRecord) -> Result<(VectorEntry, Option<TxId>)>,
        ) -> Result<()>,
    ) -> Result<T> {
        match self {
            Self::Writable(persistence) => persistence.stream_vector_partition_memberships(
                partition,
                dimension,
                quantization,
                prepare,
                visit,
            ),
            Self::CommittedImage(source) => source.stream_raw_vector_memberships(
                partition,
                dimension,
                quantization,
                prepare,
                visit,
            ),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn load_candidate(
        &self,
        partition: &VectorPartitionRef,
        identity: &RawVectorDirectoryEntry,
        dimension: usize,
        quantization: VectorQuantization,
    ) -> Result<VectorEntry> {
        match self {
            Self::Writable(persistence) => persistence.load_vector_entry_for_partition_identity(
                &partition.index,
                &partition.partition_key,
                identity,
                dimension,
                quantization,
            ),
            Self::CommittedImage(source) => source.load_raw_vector_entry_for_partition_identity(
                &partition.index,
                &partition.partition_key,
                identity,
                dimension,
                quantization,
            ),
        }
    }
    pub(super) fn membership_owner(
        &self,
    ) -> crate::persistence::vector_membership_source::MembershipOwner {
        use crate::persistence::vector_membership_source::MembershipOwner;
        match self {
            Self::Writable(source) => MembershipOwner::Writable(source.clone()),
            Self::CommittedImage(source) => MembershipOwner::Retained(source.clone()),
        }
    }
    pub(super) fn historical_operation(&self) -> bool {
        self.membership_owner().historical_operation()
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn load_candidate_before(
        &self,
        partition: &VectorPartitionRef,
        identity: &RawVectorDirectoryEntry,
        dimension: usize,
        quantization: VectorQuantization,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<VectorEntry> {
        let controls = std::mem::size_of::<crate::persistence::WritableRawVectorBodyScan>()
            .max(std::mem::size_of::<
                crate::persistence::RetainedRawVectorBodyScan,
            >())
            .checked_add(std::mem::size_of::<redb::ReadTransaction>())
            .ok_or_else(|| Error::Other("vector body scan control capacity overflow".into()))?;
        let memory = crate::persistence::vector_membership_source::read_memory_for_operation()
            .ok_or_else(|| {
                Error::Other("historical vector body scan has no operation reader".into())
            })?;
        let _controls = contextdb_core::read_memory::ReadCredit::try_new(
            memory,
            controls,
            "reader",
            "historical_vector_candidate_scan",
            "Raise the declared reader memory limit.",
        )?;
        let mut before = || {
            if request(DormantVectorLoadRequest::Checkpoint) {
                Ok(())
            } else {
                Err(Error::ReadCancelled)
            }
        };
        before()?;
        match self {
            Self::Writable(source) => source
                .open_raw_vector_body_scan(&partition.index)?
                .read_partition_body_before(
                    &partition.index,
                    &partition.partition_key,
                    identity,
                    dimension,
                    quantization,
                    false,
                    &mut before,
                ),
            Self::CommittedImage(source) => source
                .open_raw_vector_body_scan()?
                .read_partition_body_before(
                    &partition.index,
                    &partition.partition_key,
                    identity,
                    dimension,
                    quantization,
                    false,
                    &mut before,
                ),
        }
    }
}

impl DurableRawVectorPartitionLoader {
    pub(super) const RAW_LOAD_PAGE_BOUND: (usize, usize) = (64, 64 * 1024);

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn membership_lifetime_slots(count: usize) -> Result<usize> {
        let slot_bytes =
            std::mem::size_of::<(RowId, TxId, Lsn, TxId)>() + std::mem::size_of::<Option<TxId>>();
        // Four slots per record cover non-root node occupancy and splitting.
        // Even one record allocates a whole leaf, so charge one full node's
        // keys, values, edge slots and header independently of record count.
        let node_slack = if count == 0 {
            0
        } else {
            Self::checked_add(
                Self::checked_mul(
                    12,
                    slot_bytes + std::mem::size_of::<usize>(),
                    "durable lifetime node slack",
                )?,
                32,
                "durable lifetime node header",
            )?
        };
        Self::checked_add(
            Self::checked_mul(count, 4 * (slot_bytes + 1), "durable lifetime slots")?,
            node_slack,
            "durable lifetime table",
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_membership_lifetime(
        seen: &mut BTreeMap<(RowId, TxId, Lsn, TxId), Option<TxId>>,
        membership: &VectorPartitionMembershipRecord,
    ) -> Result<()> {
        let body = (
            membership.row_id,
            membership.vector_created_tx,
            membership.vector_lsn,
        );
        let key = (body.0, body.1, body.2, membership.visible_from);
        let lower = (body.0, body.1, body.2, TxId(0));
        let upper = (body.0, body.1, body.2, TxId(u64::MAX));
        // The set spans every streamed page. Its interval ordering is separate
        // from the authenticated source ordering, which orders owning-row stamps.
        let overlaps_previous = seen
            .range(lower..=key)
            .next_back()
            .is_some_and(|(_, end)| end.is_none_or(|end| membership.visible_from < end));
        let overlaps_next = seen
            .range(key..=upper)
            .next()
            .is_some_and(|(next, _)| membership.deleted_tx.is_none_or(|end| next.3 < end));
        if overlaps_previous || overlaps_next {
            return Err(Error::Other(
                ("raw vector partition contains duplicate directory identities".to_string()).into(),
            ));
        }
        seen.insert(key, membership.deleted_tx);
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn checked_mul(left: usize, right: usize, name: &str) -> Result<usize> {
        left.checked_mul(right).ok_or_else(|| {
            Error::Other(
                (format!("raw vector {name} exceeds this process's addressable range")).into(),
            )
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn checked_add(left: usize, right: usize, name: &str) -> Result<usize> {
        left.checked_add(right).ok_or_else(|| {
            Error::Other(
                (format!("raw vector {name} exceeds this process's addressable range")).into(),
            )
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stored_bytes_per_entry(&self) -> Result<usize> {
        match self.quantization {
            VectorQuantization::F32 => Self::checked_mul(
                self.dimension,
                std::mem::size_of::<f32>(),
                "stored F32 body",
            ),
            VectorQuantization::SQ8 => Self::checked_add(self.dimension, 8, "stored SQ8 body"),
            VectorQuantization::SQ4 => {
                Self::checked_add(self.dimension.div_ceil(2), 8, "stored SQ4 body")
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn load_checkpoint(
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<()> {
        if request(DormantVectorLoadRequest::Checkpoint) {
            Ok(())
        } else {
            Err(Error::ReadCancelled)
        }
    }
}

impl DormantRawVectorLoader for DurableRawVectorPartitionLoader {
    fn visit_retained_copy_credits(
        &self,
        caller: &dyn contextdb_vector::MemoryBudget,
        visit: &mut dyn FnMut(usize, usize, bool),
    ) {
        if let Some(credit) = &self.metadata_copy_owner {
            contextdb_vector::VectorCopyAdmission::visit_retained_copy_credit(
                credit, caller, visit,
            );
        }
    }

    fn for_renamed_index(
        &self,
        index: &VectorIndexRef,
        owner: &mut contextdb_vector::VectorCopyAdmission,
        mut retained: contextdb_vector::VectorCopyAdmission,
    ) -> Result<Arc<dyn DormantRawVectorLoader>> {
        if !matches!(self.source, DurableRawVectorSource::Writable(_)) {
            return Err(Error::Other(
                "a retained raw vector loader cannot change its captured route".into(),
            ));
        }
        let mut charge = owner.scratch();
        charge.reserve(
            std::mem::size_of::<Self>()
                .saturating_add(std::mem::size_of::<contextdb_vector::VectorCopyAdmission>())
                .saturating_add(4 * std::mem::size_of::<usize>())
                .saturating_add(index.table.len())
                .saturating_add(index.column.len())
                .saturating_add(self.partition.partition_key.estimated_bytes()),
        )?;
        retained.reserve(
            std::mem::size_of::<Self>()
                .saturating_add(std::mem::size_of::<contextdb_vector::VectorCopyAdmission>())
                .saturating_add(4 * std::mem::size_of::<usize>())
                .saturating_add(index.table.len())
                .saturating_add(index.column.len())
                .saturating_add(self.partition.partition_key.estimated_bytes()),
        )?;
        Ok(Arc::new(Self {
            source: self.source.clone(),
            partition: VectorPartitionRef::new(index.clone(), self.partition.partition_key.clone()),
            dimension: self.dimension,
            quantization: self.quantization,
            metadata_copy_owner: Some(Arc::new(retained)),
            accountant: self.accountant.clone(),
        }))
    }

    fn supports_bounded_candidate_load(&self) -> bool {
        true
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn load_all(
        &self,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<LoadedRawVectorPartition> {
        let decoded_bytes_per_entry = Self::checked_mul(
            self.dimension,
            std::mem::size_of::<f32>(),
            "decoded F32 body",
        )?;
        let stored_bytes_per_entry = self.stored_bytes_per_entry()?;
        let index_bytes = Self::checked_add(
            self.partition.index.table.len(),
            self.partition.index.column.len(),
            "index metadata",
        )?;
        // One decoded route and one body decode coexist with the entries.
        let decode_scratch = Self::checked_add(
            Self::checked_add(
                decoded_bytes_per_entry,
                stored_bytes_per_entry,
                "decode scratch",
            )?,
            Self::checked_add(
                Self::checked_mul(
                    Self::checked_add(
                        index_bytes.saturating_add(self.partition.partition_key.estimated_bytes()),
                        128,
                        "route framing",
                    )?,
                    8,
                    "streamed route scratch",
                )?,
                1024 * 1024,
                "serde initial sequence capacity",
            )?,
            "body and metadata scratch",
        )?;
        // Permanent bodies belong to the writer; decode/output to the read.
        let accountant = if self.source.historical_operation() {
            owner.funder()
        } else {
            self.source.vector_budget(&self.accountant)?
        };
        let mut resident_owner = if self.source.historical_operation() {
            owner.scratch()
        } else {
            contextdb_vector::VectorCopyAdmission::new(accountant.clone())
        };
        let mut workspace_owner = owner.scratch();
        let mut transient_owner = owner.scratch();
        let mut resident_bytes = 0usize;
        let loaded = self.source.stream_memberships(
            &self.partition,
            self.dimension,
            self.quantization,
            |count| {
                transient_owner.reserve(decode_scratch)?;
                transient_owner.reserve(Self::membership_lifetime_slots(count)?)?;
                Ok((count, Vec::<contextdb_vector::store::LoadedRawVectorEntry>::new(), BTreeMap::new()))
            },
            |(count, entries, seen), membership, load_body| {
                if membership.visible_from
                    < TxId(membership.row_created_tx.0.max(membership.vector_created_tx.0))
                    || membership
                        .deleted_tx
                        .is_some_and(|deleted| deleted.0 <= membership.visible_from.0)
                {
                    return Err(Error::Other((
                        "raw vector partition membership has an invalid lifecycle boundary"
                            .to_string()).into()));
                }
                Self::validate_membership_lifetime(seen, &membership)?;
                if entries.len() == *count {
                    return Err(Error::Other((
                        "raw vector partition grew outside its counted load".to_string()).into()));
                }
                if entries.len() == entries.capacity() {
                    let page_entries = (*count - entries.len()).min(Self::RAW_LOAD_PAGE_BOUND.0);
                    workspace_owner.reserve(Self::checked_mul(page_entries,
                        std::mem::size_of::<contextdb_vector::store::LoadedRawVectorEntry>(), "carrier page")?)?;
                    entries.reserve_exact(page_entries);
                }
                workspace_owner.reserve(Self::checked_add(decoded_bytes_per_entry, index_bytes, "decoded entry")?)?;
                resident_owner.reserve_for(stored_bytes_per_entry, "vector_store", "load_raw_vector_partition",
                    "Evict another raw vector partition, narrow the selected scope, or raise MEMORY_LIMIT before reading these vector bodies.")?;
                resident_bytes = Self::checked_add(resident_bytes, stored_bytes_per_entry, "partition residency")?;
                let (entry, canonical_deleted_tx) = load_body(&membership)?;
                let provenance = contextdb_vector::store::VectorMembershipProvenance::new(
                    membership.row_created_tx, membership.row_lsn,
                    membership.visible_from, membership.deleted_tx,
                );
                entries.push(contextdb_vector::store::LoadedRawVectorEntry::new(
                    entry, provenance, canonical_deleted_tx,
                ));
                Ok(())
            },
        );
        let (_, entries, _) = loaded?;
        // The source/decoder temporaries are gone before this release.
        drop(transient_owner);
        resident_owner.transfer();
        Ok(LoadedRawVectorPartition {
            entries,
            resident_bytes,
            workspace_owner,
            accountant,
        })
    }

    fn exact_comparison_streams_when_partition_does_not_fit(&self) -> bool {
        true
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn load(
        &self,
        directory: &[RawVectorDirectoryEntry],
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<LoadedRawVectorPartition> {
        let expected_entry_count = directory.len();
        let decoded_bytes_per_entry = Self::checked_mul(
            self.dimension,
            std::mem::size_of::<f32>(),
            "decoded F32 body",
        )?;
        let stored_bytes_per_entry = self.stored_bytes_per_entry()?;
        let mut resident_bytes = 0usize;
        // The returned entries also own their container and index strings.
        let index_bytes = Self::checked_add(
            self.partition.index.table.len(),
            self.partition.index.column.len(),
            "index metadata",
        )?;
        // Prefix construction, one decoded route, key verification, and the
        // one-body decoder coexist. Route copies stay constant in row count.
        let route_bytes = Self::checked_add(
            index_bytes,
            self.partition.partition_key.estimated_bytes(),
            "route metadata",
        )?;
        let metadata_scratch = Self::checked_mul(
            Self::checked_add(route_bytes, 128, "route framing")?,
            8,
            "streamed route scratch",
        )?;
        let decode_scratch = Self::checked_add(
            Self::checked_add(
                decoded_bytes_per_entry,
                self.stored_bytes_per_entry()?,
                "decode scratch",
            )?,
            Self::checked_add(
                metadata_scratch,
                1024 * 1024,
                "serde initial sequence capacity",
            )?,
            "body and metadata scratch",
        )?;
        type Identity = (
            RowId,
            TxId,
            Option<TxId>,
            Lsn,
            TxId,
            Lsn,
            TxId,
            Option<TxId>,
        );
        let directory_slots = Self::checked_mul(
            expected_entry_count,
            Self::checked_add(
                4 * (std::mem::size_of::<(Identity, usize)>() + 1),
                std::mem::size_of::<(usize, contextdb_vector::store::LoadedRawVectorEntry)>(),
                "directory and ordering slots",
            )?,
            "directory scratch",
        )?;
        // Four buckets per item cover power-of-two rounding at 7/8 load.
        // Include the terminal control group and alignment for small tables.
        let directory_scratch = Self::checked_add(
            directory_slots,
            usize::from(expected_entry_count != 0) * 32,
            "directory table controls",
        )?;
        let transient = Self::checked_add(
            decode_scratch,
            directory_scratch,
            "partition transient reservation",
        )?;
        let accountant = if self.source.historical_operation() {
            owner.funder()
        } else {
            self.source.vector_budget(&self.accountant)?
        };
        let mut resident_owner = if self.source.historical_operation() {
            owner.scratch()
        } else {
            contextdb_vector::VectorCopyAdmission::new(accountant.clone())
        };
        let mut workspace_owner = owner.scratch();
        let mut transient_owner = owner.scratch();
        // The point-loader contract requires exact returned workspace before
        // decoder preflight. Only one bounded page is prefunded; larger copies
        // are admitted as validated entries stream.
        let initial_workspace = if expected_entry_count <= Self::RAW_LOAD_PAGE_BOUND.0 {
            Self::checked_mul(
                expected_entry_count,
                Self::checked_add(
                    std::mem::size_of::<contextdb_vector::store::LoadedRawVectorEntry>(),
                    Self::checked_add(decoded_bytes_per_entry, index_bytes, "decoded entry")?,
                    "returned entry",
                )?,
                "initial output page",
            )?
        } else {
            0
        };
        let workspace_pre_admitted = expected_entry_count <= Self::RAW_LOAD_PAGE_BOUND.0
            && initial_workspace <= Self::RAW_LOAD_PAGE_BOUND.1;
        if workspace_pre_admitted {
            workspace_owner.reserve(initial_workspace)?;
        }
        transient_owner.reserve(transient)?;
        let loaded = (|| -> Result<Vec<contextdb_vector::store::LoadedRawVectorEntry>> {
            let (_, positions, mut selected) = self.source.stream_memberships(&self.partition, self.dimension, self.quantization,
                |durable_count| {
                    // Include unexpected durable records; directory length is
                    // not an upper bound for the validation identity set.
                    let membership_scratch = Self::membership_lifetime_slots(durable_count)?;
                    transient_owner.reserve(membership_scratch)?;
                    let positions = directory.iter().enumerate().map(|(position, entry)|
                        ((entry.row_id, entry.created_tx, entry.deleted_tx, entry.lsn,
                            entry.membership.owning_row_tx, entry.membership.row_lsn,
                            entry.membership.visible_from, entry.membership.deleted_tx), position)).collect::<HashMap<_, _>>();
                    Ok((BTreeMap::new(), positions, Vec::<(usize, contextdb_vector::store::LoadedRawVectorEntry)>::new()))
                },
                |(seen, positions, selected), membership, load_body| {
                    if membership.visible_from < TxId(membership.row_created_tx.0.max(membership.vector_created_tx.0))
                        || membership.deleted_tx.is_some_and(|deleted| deleted.0 <= membership.visible_from.0) {
                        return Err(Error::Other(("raw vector partition membership has an invalid lifecycle boundary".to_string()).into()));
                    }
                    let identity = (membership.row_id, membership.vector_created_tx, membership.deleted_tx, membership.vector_lsn,
                        membership.row_created_tx, membership.row_lsn, membership.visible_from, membership.deleted_tx);
                    Self::validate_membership_lifetime(seen, &membership)?;
                    if let Some(position) = positions.remove(&identity) {
                        if selected.len() == selected.capacity() {
                            let page_entries = (expected_entry_count - selected.len()).min(Self::RAW_LOAD_PAGE_BOUND.0);
                            if !workspace_pre_admitted {
                                workspace_owner.reserve(Self::checked_mul(page_entries,
                                    std::mem::size_of::<contextdb_vector::store::LoadedRawVectorEntry>(), "carrier page")?)?;
                            }
                            selected.reserve_exact(page_entries);
                        }
                        if !workspace_pre_admitted {
                            workspace_owner.reserve(Self::checked_add(decoded_bytes_per_entry, index_bytes, "decoded entry")?)?;
                        }
                        resident_owner.reserve_for(stored_bytes_per_entry, "vector_store", "load_raw_vector_partition",
                            "Evict another raw vector partition, narrow the selected scope, or raise MEMORY_LIMIT before reading these vector bodies.")?;
                        resident_bytes = Self::checked_add(resident_bytes, stored_bytes_per_entry, "partition residency")?;
                        let (entry, canonical_deleted_tx) = load_body(&membership)?;
                        let provenance = contextdb_vector::store::VectorMembershipProvenance::new(
                            membership.row_created_tx, membership.row_lsn,
                            membership.visible_from, membership.deleted_tx,
                        );
                        selected.push((position, contextdb_vector::store::LoadedRawVectorEntry::new(
                            entry, provenance, canonical_deleted_tx,
                        )));
                    }
                    Ok(())
                })?;
            if !positions.is_empty() || selected.len() != expected_entry_count {
                return Err(Error::Other(
                    ("raw vector partition identity changed outside its atomic publication"
                        .to_string())
                    .into(),
                ));
            }
            selected.sort_unstable_by_key(|(position, _)| *position);
            // Allocate the exact returned entry layout: in-place collect may
            // retain the wider (position, entry) allocation after its transient
            // charge is released.
            let mut entries = Vec::with_capacity(expected_entry_count);
            for (_, entry) in selected {
                entries.push(entry);
            }
            Ok(entries)
        })();

        let entries = loaded?;
        // Directory/identity/ordering buffers were destroyed in the closure.
        drop(transient_owner);
        resident_owner.transfer();
        Ok(LoadedRawVectorPartition {
            entries,
            resident_bytes,
            workspace_owner,
            accountant,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn load_candidate(
        &self,
        identity: &RawVectorDirectoryEntry,
        caller_charged: bool,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<LoadedRawVectorCandidate> {
        self.load_candidate_via(identity, caller_charged, request, &mut |request| {
            if self.source.historical_operation() {
                self.source.load_candidate_before(
                    &self.partition,
                    identity,
                    self.dimension,
                    self.quantization,
                    request,
                )
            } else {
                self.source.load_candidate(
                    &self.partition,
                    identity,
                    self.dimension,
                    self.quantization,
                )
            }
        })
    }

    fn open_body_scan_for_membership(
        &self,
        receipt: &dyn std::any::Any,
    ) -> Result<Option<Box<dyn RawVectorBodyScan>>> {
        let cursor = receipt
            .downcast_ref::<crate::persistence::vector_membership_source::MembershipCursor>()
            .ok_or_else(|| Error::StoreCorrupted {
                path: "vector_partition_memberships".into(),
                reason: "raw body stream received another root receipt type".into(),
            })?;
        let owner = match &self.source {
            DurableRawVectorSource::Writable(source) => {
                crate::persistence::vector_membership_source::MembershipOwner::Writable(
                    source.clone(),
                )
            }
            DurableRawVectorSource::CommittedImage(source) => {
                crate::persistence::vector_membership_source::MembershipOwner::Retained(
                    source.clone(),
                )
            }
        };
        cursor.open_body_scan(&owner, &self.partition).map(Some)
    }

    fn body_scan_control_bytes(&self) -> usize {
        use crate::persistence::vector_membership_source::MembershipOwner;
        let (scan, captured) = match &self.source {
            DurableRawVectorSource::Writable(source) => (
                std::mem::size_of::<crate::persistence::WritableRawVectorBodyScan>(),
                MembershipOwner::Writable(source.clone()).has_captured_read(),
            ),
            DurableRawVectorSource::CommittedImage(source) => (
                std::mem::size_of::<crate::persistence::RetainedRawVectorBodyScan>(),
                MembershipOwner::Retained(source.clone()).has_captured_read(),
            ),
        };
        // A captured operation lends its existing paid Arc root. Only the
        // uncaptured path constructs a new transaction and Arc allocation.
        scan.saturating_add(if captured {
            0
        } else {
            std::mem::size_of::<redb::ReadTransaction>() + 4 * std::mem::size_of::<usize>()
        })
    }

    fn body_scan_source(&self) -> usize {
        match &self.source {
            DurableRawVectorSource::CommittedImage(source) => source.identity(),
            DurableRawVectorSource::Writable(persistence) => Arc::as_ptr(persistence) as usize,
        }
    }

    fn open_body_scan(&self) -> Result<Option<Box<dyn RawVectorBodyScan>>> {
        match &self.source {
            DurableRawVectorSource::CommittedImage(source) => {
                Ok(Some(Box::new(source.open_raw_vector_body_scan()?)))
            }
            DurableRawVectorSource::Writable(persistence) => Ok(Some(Box::new(
                persistence.open_raw_vector_body_scan(&self.partition.index)?,
            ))),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn load_candidate_with_scan(
        &self,
        scan: &mut dyn RawVectorBodyScan,
        identity: &RawVectorDirectoryEntry,
        caller_charged: bool,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<LoadedRawVectorCandidate> {
        self.load_candidate_via(identity, caller_charged, request, &mut |request| {
            if self.source.historical_operation() {
                self.source.load_candidate_before(
                    &self.partition,
                    identity,
                    self.dimension,
                    self.quantization,
                    request,
                )
            } else {
                scan.read_body(&self.partition, identity, self.dimension, self.quantization)
            }
        })
    }

    fn bind_streamed_membership(
        &self,
        scan: &mut dyn RawVectorBodyScan,
        receipt: &dyn std::any::Any,
    ) -> Result<()> {
        scan.bind_streamed_membership(&self.partition, receipt)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn load_selected_candidate(
        &self,
        selected: &dyn contextdb_vector::store::RawVectorMembershipSelection,
        caller_charged: bool,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<LoadedRawVectorCandidate> {
        let receipt = selected
            .as_any()
            .downcast_ref::<crate::persistence::vector_membership_source::SelectedVectorMembership>(
            )
            .ok_or_else(|| Error::StoreCorrupted {
                path: "vector_partition_memberships".into(),
                reason: "raw body projection received another membership receipt type".into(),
            })?;
        let owner = match &self.source {
            DurableRawVectorSource::Writable(source) => {
                crate::persistence::vector_membership_source::MembershipOwner::Writable(
                    source.clone(),
                )
            }
            DurableRawVectorSource::CommittedImage(source) => {
                crate::persistence::vector_membership_source::MembershipOwner::Retained(
                    source.clone(),
                )
            }
        };
        let identity = selected.identity();
        self.load_candidate_via(&identity, caller_charged, request, &mut |request| {
            receipt.read_body_before(
                &owner,
                &self.partition,
                self.dimension,
                self.quantization,
                &mut || {
                    if request(DormantVectorLoadRequest::Checkpoint) {
                        Ok(())
                    } else {
                        Err(Error::ReadCancelled)
                    }
                },
            )
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn load_streamed_candidate_with_scan(
        &self,
        scan: &mut dyn RawVectorBodyScan,
        identity: &RawVectorDirectoryEntry,
        caller_charged: bool,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<LoadedRawVectorCandidate> {
        self.load_candidate_via(identity, caller_charged, request, &mut |request| {
            if self.source.historical_operation() {
                self.source.load_candidate_before(
                    &self.partition,
                    identity,
                    self.dimension,
                    self.quantization,
                    request,
                )
            } else {
                scan.read_streamed_body(
                    &self.partition,
                    identity,
                    self.dimension,
                    self.quantization,
                )
            }
        })
    }

    fn one_pass_reads(
        &self,
        session: &mut contextdb_vector::store::OnePassSession,
    ) -> Option<Box<dyn std::any::Any>> {
        use crate::persistence::vector_membership_source::{self, MembershipOwner};
        let owner = match &self.source {
            DurableRawVectorSource::Writable(source) => MembershipOwner::Writable(source.clone()),
            DurableRawVectorSource::CommittedImage(source) => {
                MembershipOwner::Retained(source.clone())
            }
        };
        if vector_membership_source::has_scan_scope_for_operation(&owner) {
            // Preflight entered the actual operation's allowance before root
            // capture. Its scope spans this call, including every partition.
            return None;
        }
        // Legacy uncaptured callers retain their existing session semantics.
        // This fallback does not certify their redb control backing as funded.
        let allowance = session
            .0
            .get_or_insert_with(|| Box::new(redb::ScanAllowance::new()))
            .downcast_ref::<redb::ScanAllowance>()?
            .clone();
        Some(Box::new(allowance.enter()))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn load_candidate_with_scan_once(
        &self,
        scan: &mut dyn RawVectorBodyScan,
        identity: &RawVectorDirectoryEntry,
        caller_charged: bool,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<LoadedRawVectorCandidate> {
        // A one-pass exhaustive scan's body pages must not fill the shared
        // read cache and displace its warm working set.
        self.load_candidate_via(identity, caller_charged, request, &mut |request| {
            redb::with_scan_reads(|| {
                if self.source.historical_operation() {
                    self.source.load_candidate_before(
                        &self.partition,
                        identity,
                        self.dimension,
                        self.quantization,
                        request,
                    )
                } else {
                    scan.read_body(&self.partition, identity, self.dimension, self.quantization)
                }
            })
        })
    }
}

impl contextdb_vector::store::RawVectorBodyScan for crate::persistence::RetainedRawVectorBodyScan {
    fn bind_streamed_membership(
        &mut self,
        partition: &VectorPartitionRef,
        receipt: &dyn std::any::Any,
    ) -> Result<()> {
        self.bind_stream_selection(partition, receipt)
    }

    fn read_streamed_body(
        &mut self,
        partition: &VectorPartitionRef,
        identity: &RawVectorDirectoryEntry,
        dimension: usize,
        quantization: VectorQuantization,
    ) -> Result<VectorEntry> {
        let authenticated = self.consume_stream_selection(identity);
        self.read_partition_body(
            &partition.index,
            &partition.partition_key,
            identity,
            dimension,
            quantization,
            authenticated,
        )
    }

    fn read_body(
        &mut self,
        partition: &VectorPartitionRef,
        identity: &RawVectorDirectoryEntry,
        dimension: usize,
        quantization: VectorQuantization,
    ) -> Result<VectorEntry> {
        self.read_partition_body(
            &partition.index,
            &partition.partition_key,
            identity,
            dimension,
            quantization,
            false,
        )
    }
}

impl contextdb_vector::store::RawVectorBodyScan for crate::persistence::WritableRawVectorBodyScan {
    fn bind_streamed_membership(
        &mut self,
        partition: &VectorPartitionRef,
        receipt: &dyn std::any::Any,
    ) -> Result<()> {
        self.bind_stream_selection(partition, receipt)
    }

    fn read_streamed_body(
        &mut self,
        partition: &VectorPartitionRef,
        identity: &RawVectorDirectoryEntry,
        dimension: usize,
        quantization: VectorQuantization,
    ) -> Result<VectorEntry> {
        let authenticated = self.consume_stream_selection(identity);
        self.read_partition_body(
            &partition.index,
            &partition.partition_key,
            identity,
            dimension,
            quantization,
            authenticated,
        )
    }

    fn read_body(
        &mut self,
        partition: &VectorPartitionRef,
        identity: &RawVectorDirectoryEntry,
        dimension: usize,
        quantization: VectorQuantization,
    ) -> Result<VectorEntry> {
        self.read_partition_body(
            &partition.index,
            &partition.partition_key,
            identity,
            dimension,
            quantization,
            false,
        )
    }
}

impl DurableRawVectorPartitionLoader {
    /// Admit one candidate body's peak before it is read, read it with
    /// `fetch`, and check it is exactly the directory identity asked for.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn load_candidate_via(
        &self,
        identity: &RawVectorDirectoryEntry,
        caller_charged: bool,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
        fetch: &mut dyn FnMut(
            &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
        ) -> Result<VectorEntry>,
    ) -> Result<LoadedRawVectorCandidate> {
        // Admit the adapter before payload admission. Refusal cannot strand a
        // payload reservation while trying to construct its escaping owner.
        let retained_accountant = if caller_charged {
            None
        } else {
            Some(self.source.vector_budget(&self.accountant)?)
        };
        let decoded_bytes = Self::checked_mul(
            self.dimension,
            std::mem::size_of::<f32>(),
            "candidate decoded F32 body",
        )?;
        // Quantizer min/max/length are inline scalars, not backing buffers.
        let stored_bytes = match self.quantization {
            VectorQuantization::F32 => decoded_bytes,
            VectorQuantization::SQ8 => self.dimension,
            VectorQuantization::SQ4 => self.dimension.div_ceil(2),
        };
        let retained = Self::checked_add(
            decoded_bytes,
            stored_bytes,
            "candidate decoded and stored bodies",
        )?;
        // Redb lends the encoded bytes under its separately funded page guard.
        // Decode scratch and the scoring representation are successive phases:
        // SQ8/SQ4 payload scratch dies before scoring re-encodes the F32 body;
        // F32 decoding moves its Vec into the entry. At most one stored-sized
        // allocation coexists with the decoded body, not two body pairs.
        // SQ4 scoring first forms one byte per component, then packs nibbles.
        // That temporary coexists with the final packed representation.
        let body_peak = Self::checked_add(
            retained,
            if self.quantization == VectorQuantization::SQ4 {
                self.dimension
            } else {
                0
            },
            "candidate score encoding scratch",
        )?;
        // The decoded route strings coexist with the physical identity key
        // and the logical proof key. The latter grows through Vec extension,
        // so its old and new buffers need up to three complete key lengths.
        // Physical framing is 34 bytes; logical key framing is 32 bytes;
        // the proof value's dimension header is eight bytes.
        let route_bytes = Self::checked_add(
            self.partition.index.table.len(),
            self.partition.index.column.len(),
            "candidate route strings",
        )?;
        let metadata_peak = Self::checked_add(
            Self::checked_mul(route_bytes, 5, "candidate route metadata copies")?,
            34 + 3 * 32 + 8,
            "candidate route metadata framing",
        )?;
        let transient_bytes = Self::checked_add(
            body_peak,
            metadata_peak,
            "candidate body and identity reservation",
        )?;

        Self::load_checkpoint(request)?;
        if caller_charged {
            if !request(DormantVectorLoadRequest::Reserve(transient_bytes)) {
                return Err(Error::ReadCancelled);
            }
            #[cfg(any(test, feature = "test-seams"))]
            crate::vector_read_memory::note_reader_body_admission_for_test(
                transient_bytes,
                identity.row_id,
                identity.created_tx,
                identity.lsn,
            );
        } else {
            self.accountant.try_allocate_for(
                transient_bytes,
                "vector_store",
                "load_raw_vector_candidate",
                "Reduce the selected candidate scope or raise MEMORY_LIMIT before reading this vector body.",
            )?;
        }

        let loaded = (|| -> Result<VectorEntry> {
            Self::load_checkpoint(request)?;
            let entry = if caller_charged {
                crate::read_image_memory::with_candidate_decode_credit(
                    Self::checked_add(body_peak, route_bytes, "candidate codec window")?,
                    || fetch(request),
                )?
            } else {
                fetch(request)?
            };
            Self::load_checkpoint(request)?;
            if entry.index != self.partition.index
                || entry.row_id != identity.row_id
                || entry.created_tx != identity.created_tx
                || entry.deleted_tx != identity.deleted_tx
                || entry.lsn != identity.lsn
                || entry.vector.len() != self.dimension
            {
                return Err(Error::Other(
                    ("raw vector candidate disagrees with its exact durable identity".to_string())
                        .into(),
                ));
            }
            Ok(entry)
        })();

        match loaded {
            Err(error) => {
                if !caller_charged {
                    self.accountant.release(transient_bytes);
                }
                Err(error)
            }
            Ok(entry) if caller_charged => Ok(LoadedRawVectorCandidate::caller_charged(
                entry,
                transient_bytes,
            )),
            Ok(entry) => Ok(LoadedRawVectorCandidate::loader_charged(
                entry,
                transient_bytes,
                retained_accountant.expect("loader-funded candidate has its admitted owner"),
            )),
        }
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn durable_raw_vector_partitions(
    vector: &VectorStore,
    source: DurableRawVectorSource,
    memberships: Vec<VectorPartitionMembershipRecord>,
    accountant: &crate::memory_accounting::AccountantHandle,
    copy_owner: &mut contextdb_vector::VectorCopyAdmission,
) -> Result<Vec<DormantRawVectorPartition>> {
    // Historical EXACT reads need the same immutable vector identities after
    // reopen. Membership retirement affects maintained graphs, not retained
    // raw bodies: keep every durable visibility interval in the directory.
    let workspace_bytes = memberships.iter().fold(
        memberships.len().saturating_mul(
            std::mem::size_of::<VectorPartitionMembershipRecord>()
                .saturating_add(std::mem::size_of::<VectorPartitionRef>())
                .saturating_add(std::mem::size_of::<DormantRawVectorPartition>())
                .saturating_add(std::mem::size_of::<RawVectorDirectoryEntry>())
                .saturating_add(4 * std::mem::size_of::<usize>())
                .saturating_mul(8),
        ),
        |bytes, membership| {
            bytes.saturating_add(
                membership
                    .index
                    .table
                    .len()
                    .saturating_add(membership.index.column.len())
                    .saturating_add(membership.partition_key.estimated_bytes())
                    .saturating_mul(4),
            )
        },
    );
    copy_owner.reserve(workspace_bytes)?;
    let mut grouped = HashMap::<VectorPartitionRef, Vec<VectorPartitionMembershipRecord>>::new();
    for membership in memberships {
        grouped
            .entry(VectorPartitionRef::new(
                membership.index.clone(),
                membership.partition_key.clone(),
            ))
            .or_default()
            .push(membership);
    }

    let mut grouped = grouped.into_iter().collect::<Vec<_>>();
    grouped.sort_by(|(left, _), (right, _)| {
        left.index
            .table
            .cmp(&right.index.table)
            .then(left.index.column.cmp(&right.index.column))
            .then(left.partition_key.cmp(&right.partition_key))
    });
    let mut dormant = Vec::with_capacity(grouped.len());
    for (partition, mut memberships) in grouped {
        memberships.sort_by_key(|membership| {
            (
                membership.row_id,
                membership.row_created_tx,
                membership.row_lsn,
                membership.vector_created_tx,
                membership.vector_lsn,
            )
        });
        let layout = contextdb_vector::scoped_index_layout(vector, &partition.index, copy_owner)?;
        let directory = memberships
            .iter()
            .map(|membership| RawVectorDirectoryEntry {
                membership: contextdb_vector::store::VectorMembershipProvenance::new(
                    membership.row_created_tx,
                    membership.row_lsn,
                    membership.visible_from,
                    membership.deleted_tx,
                ),
                row_id: membership.row_id,
                created_tx: membership.vector_created_tx,
                deleted_tx: membership.deleted_tx,
                lsn: membership.vector_lsn,
            })
            .collect::<Vec<_>>();
        let mut loader_workspace = copy_owner.scratch();
        let mut loader_owner = vector.retained_column_copy_owner(&partition.index, copy_owner)?;
        loader_owner.reserve(
            std::mem::size_of::<DurableRawVectorPartitionLoader>()
                .saturating_add(std::mem::size_of::<contextdb_vector::VectorCopyAdmission>())
                .saturating_add(4 * std::mem::size_of::<usize>())
                .saturating_add(partition.index.table.len())
                .saturating_add(partition.index.column.len())
                .saturating_add(partition.partition_key.estimated_bytes()),
        )?;
        loader_workspace.reserve(
            std::mem::size_of::<DurableRawVectorPartitionLoader>()
                .saturating_add(std::mem::size_of::<contextdb_vector::VectorCopyAdmission>())
                .saturating_add(4 * std::mem::size_of::<usize>())
                .saturating_add(partition.index.table.len())
                .saturating_add(partition.index.column.len())
                .saturating_add(partition.partition_key.estimated_bytes()),
        )?;
        let loader: Arc<dyn DormantRawVectorLoader> = Arc::new(DurableRawVectorPartitionLoader {
            source: source.clone(),
            partition: partition.clone(),
            dimension: layout.dimension,
            quantization: layout.quantization,
            metadata_copy_owner: Some(Arc::new(loader_owner)),
            accountant: accountant.clone(),
        });
        dormant.push(DormantRawVectorPartition {
            partition,
            directory,
            loader,
        });
    }
    Ok(dormant)
}

/// Read-only partitions retain a registered source and summary geometry;
/// placement intervals and reverse identities remain in that pinned store.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn register_retained_raw_vector_partitions(
    vector: &VectorStore,
    source: &crate::persistence::RetainedReadSource,
    accountant: &crate::memory_accounting::AccountantHandle,
    copy_owner: &mut contextdb_vector::VectorCopyAdmission,
) -> Result<()> {
    use crate::persistence::vector_membership_source::{
        DurableMembershipSource, DurableReverseMembershipSource, MembershipOwner,
    };
    let owner = MembershipOwner::Retained(source.clone());
    let layouts = vector.index_layout_infos(copy_owner)?;
    for info in &layouts {
        let retained = vector.retained_column_copy_owner(&info.index, copy_owner)?;
        vector.register_durable_raw_reverse_source(
            &info.index,
            DurableReverseMembershipSource::from_owner_admitted(
                owner.clone(),
                &info.index,
                accountant.clone(),
                copy_owner,
                retained,
            )?,
        )?;
    }
    let mut partitions = source.durable_membership_partitions(copy_owner)?;
    for info in &layouts {
        if !info.layout.is_partitioned()
            && !partitions
                .iter()
                .any(|(partition, _)| partition.index == info.index)
        {
            copy_owner.reserve(
                std::mem::size_of::<(
                    VectorPartitionRef,
                    contextdb_vector::store::RawVectorMembershipSummary,
                )>()
                .saturating_add(info.index.table.len())
                .saturating_add(info.index.column.len())
                .saturating_mul(2),
            )?;
            partitions.reserve_exact(1);
            partitions.push((
                VectorPartitionRef::new(info.index.clone(), VectorPartitionKey::unpartitioned()),
                source.durable_membership_summary(
                    &VectorPartitionRef::new(
                        info.index.clone(),
                        VectorPartitionKey::unpartitioned(),
                    ),
                    copy_owner,
                )?,
            ));
        }
    }
    for (partition, summary) in partitions {
        let info = layouts
            .iter()
            .find(|info| info.index == partition.index)
            .ok_or_else(|| Error::StoreCorrupted {
                path: "vector_membership_summary_pages".into(),
                reason: "membership summary names an undeclared vector column".into(),
            })?;
        let mut loader_workspace = copy_owner.scratch();
        loader_workspace.reserve(
            partition
                .index
                .table
                .len()
                .saturating_add(partition.index.column.len())
                .saturating_add(partition.partition_key.estimated_bytes())
                .saturating_add(std::mem::size_of::<DurableRawVectorPartitionLoader>())
                .saturating_add(std::mem::size_of::<contextdb_vector::VectorCopyAdmission>())
                .saturating_add(4 * std::mem::size_of::<usize>()),
        )?;
        let mut loader_owner = vector.retained_column_copy_owner(&partition.index, copy_owner)?;
        loader_owner.reserve(
            std::mem::size_of::<DurableRawVectorPartitionLoader>()
                .saturating_add(std::mem::size_of::<contextdb_vector::VectorCopyAdmission>())
                .saturating_add(4 * std::mem::size_of::<usize>())
                .saturating_add(partition.index.table.len())
                .saturating_add(partition.index.column.len())
                .saturating_add(partition.partition_key.estimated_bytes()),
        )?;
        let loader = Arc::new(DurableRawVectorPartitionLoader {
            source: DurableRawVectorSource::CommittedImage(source.clone()),
            partition: partition.clone(),
            dimension: info.layout.dimension,
            quantization: info.layout.quantization,
            metadata_copy_owner: Some(Arc::new(loader_owner)),
            accountant: accountant.clone(),
        });
        let retained = vector.retained_column_copy_owner(&partition.index, copy_owner)?;
        vector.register_durable_raw_partition(
            &partition,
            DurableMembershipSource::from_owner_admitted(
                owner.clone(),
                None,
                &partition,
                summary,
                accountant.clone(),
                copy_owner,
                retained,
            )?,
            loader,
            copy_owner,
        )?;
    }
    Ok(())
}
impl LoaderCallbackCaptures {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn load(
        &self,
        partition: &VectorPartitionRef,
        layout: &contextdb_vector::VectorIndexLayout,
        mut workspace: contextdb_vector::VectorCopyAdmission,
        mut owner: contextdb_vector::VectorCopyAdmission,
    ) -> Result<Arc<dyn DormantRawVectorLoader>> {
        let bytes = std::mem::size_of::<DurableRawVectorPartitionLoader>()
            .saturating_add(std::mem::size_of::<contextdb_vector::VectorCopyAdmission>())
            .saturating_add(4 * std::mem::size_of::<usize>())
            .saturating_add(partition.index.table.len())
            .saturating_add(partition.index.column.len())
            .saturating_add(partition.partition_key.estimated_bytes());
        workspace.reserve(bytes)?;
        owner.reserve(bytes)?;
        Ok(Arc::new(DurableRawVectorPartitionLoader {
            source: DurableRawVectorSource::Writable(Arc::clone(&self.persistence)),
            partition: partition.clone(),
            dimension: layout.dimension,
            quantization: layout.quantization,
            metadata_copy_owner: Some(Arc::new(owner)),
            accountant: self.accountant.as_handle(),
        }) as Arc<dyn DormantRawVectorLoader>)
    }
}
impl LoaderCallbackCleanup {
    pub(super) fn finish(&mut self) {
        while self.next < 3 {
            let step = self.next;
            self.next += 1;
            // SAFETY: only the capture's destructor constructs this guard.
            // Advancing first ensures single-unwind cleanup never drops a
            // field twice; the final handle follows both active children.
            unsafe {
                match step {
                    0 => std::mem::ManuallyDrop::drop(&mut (*self.source).persistence),
                    1 => std::mem::ManuallyDrop::drop(&mut (*self.source).captured_owner),
                    2 => std::mem::ManuallyDrop::drop(&mut (*self.source).accountant),
                    _ => unreachable!(),
                }
            }
        }
    }
}
impl Drop for LoaderCallbackCleanup {
    fn drop(&mut self) {
        self.finish();
    }
}
impl Drop for LoaderCallbackCaptures {
    fn drop(&mut self) {
        let mut cleanup = LoaderCallbackCleanup {
            source: self,
            next: 0,
        };
        cleanup.finish();
    }
}

/// A writable open registers durable membership sources: one reverse source
/// and one partition factory per vector column, and one forward source per
/// partition with a maintained summary. Registration reads only those
/// summary records; no membership record is enumerated and no directory is
/// retained. Every later operation reads its captured writer root.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn register_dormant_raw_vector_partitions(
    vector: &VectorStore,
    manager: &DatabaseTransactionHost,
    persistence: &Arc<RedbPersistence>,
    accountant: &crate::memory_accounting::AccountantHandle,
    copy_owner: &mut contextdb_vector::VectorCopyAdmission,
) -> Result<()> {
    use crate::persistence::vector_membership_source::{
        DurableColumnSourceFactory, DurableMembershipSource,
    };
    let mut factory_owner = copy_owner.scratch();
    factory_owner.reserve(
        std::mem::size_of::<DurableColumnSourceFactory>()
            .saturating_add(std::mem::size_of::<contextdb_vector::VectorCopyAdmission>())
            .saturating_add(16 * std::mem::size_of::<usize>()),
    )?;
    let factory_owner = Arc::new(factory_owner);
    let loader_for: crate::persistence::vector_membership_source::DurableLoaderBuilder = {
        let captures = LoaderCallbackCaptures {
            persistence: std::mem::ManuallyDrop::new(persistence.clone()),
            captured_owner: std::mem::ManuallyDrop::new(factory_owner.clone()),
            accountant: std::mem::ManuallyDrop::new(
                crate::memory_accounting::StoreAccountantHandle::new(accountant.clone()),
            ),
        };
        Arc::new(
            move |partition: &VectorPartitionRef,
                  layout: &contextdb_vector::VectorIndexLayout,
                  workspace: contextdb_vector::VectorCopyAdmission,
                  owner: contextdb_vector::VectorCopyAdmission| {
                // The method call deliberately captures the complete value.
                captures.load(partition, layout, workspace, owner)
            },
        )
    };
    let published_root: crate::persistence::vector_membership_source::PublishedMembershipRoot = {
        let manager = manager.downgrade();
        Arc::new(move || {
            let manager = manager
                .upgrade()
                .ok_or_else(|| Error::Other("writer publication is unavailable".into()))?;
            // This reference decides whether an operation captured an older
            // publication. Returning its scoped root would compare it to itself
            // and let the current registry replace its retained catalog.
            writer_read::durable_root(manager.capture_published_read().handle)
        })
    };
    let columns = Arc::new(DurableColumnSourceFactory {
        published_root: published_root.clone(),
        persistence: persistence.clone(),
        loader: loader_for.clone(),
        _copy_owner: factory_owner,
        accountant: crate::memory_accounting::StoreAccountantHandle::new(accountant.clone()),
    });
    let layouts = vector.index_layout_infos(copy_owner)?;
    for info in &layouts {
        let (reverse, partitions) = columns.column_sources(
            &info.index,
            &info.layout,
            copy_owner.scratch(),
            vector.retained_column_copy_owner(&info.index, copy_owner)?,
        )?;
        vector.register_durable_raw_reverse_source(&info.index, reverse)?;
        vector.register_durable_raw_partition_factory(&info.index, partitions)?;
    }
    let mut partitions = persistence.durable_membership_partitions(copy_owner)?;
    // An unpartitioned column's single partition exists from declaration;
    // it is durable even before its first membership is published.
    for info in &layouts {
        if !info.layout.is_partitioned()
            && !partitions
                .iter()
                .any(|(partition, _)| partition.index == info.index)
        {
            copy_owner.reserve(
                std::mem::size_of::<(
                    VectorPartitionRef,
                    contextdb_vector::store::RawVectorMembershipSummary,
                )>()
                .saturating_mul(4)
                .saturating_add(info.index.table.len())
                .saturating_add(info.index.column.len()),
            )?;
            partitions.push((
                VectorPartitionRef::new(info.index.clone(), VectorPartitionKey::unpartitioned()),
                contextdb_vector::store::RawVectorMembershipSummary::default(),
            ));
        }
    }
    for (partition, summary) in partitions {
        let Some(info) = layouts.iter().find(|info| info.index == partition.index) else {
            return Err(crate::funded_error::corruption_display(
                accountant.read_budget(),
                "vector_membership_summary_pages",
                format_args!(
                    "membership summary names undeclared vector column {}.{}",
                    partition.index.table, partition.index.column
                ),
            ));
        };
        let retained = vector.retained_column_copy_owner(&partition.index, copy_owner)?;
        vector.register_durable_raw_partition(
            &partition,
            DurableMembershipSource::from_owner_admitted(
                crate::persistence::vector_membership_source::MembershipOwner::Writable(
                    persistence.clone(),
                ),
                Some(published_root.clone()),
                &partition,
                summary,
                accountant.clone(),
                copy_owner,
                retained,
            )?,
            loader_for(
                &partition,
                &info.layout,
                copy_owner.scratch(),
                vector.retained_column_copy_owner(&partition.index, copy_owner)?,
            )?,
            copy_owner,
        )?;
    }
    vector.register_durable_raw_column_factory(columns);
    Ok(())
}
