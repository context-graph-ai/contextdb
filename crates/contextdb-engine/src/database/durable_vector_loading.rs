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

    /// Begin a repair removal pass after admitting both allocations that
    /// coexist while its registered-snapshot sample moves from a collection
    /// buffer into the guard's retained slice. The registry lock stays held
    /// from the authoritative count through admission and sampling, and the
    /// pass marker is installed before that lock is released.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn begin_removal_pass_with_sample_admission(
        self: &Arc<Self>,
        watermark: TxId,
        admit_transition: impl FnOnce(usize) -> Result<()>,
    ) -> Result<(ActiveRemovalPassGuard, usize)> {
        let mut state = self.state.lock();
        // Capacity is bounded from the complete registered set before the
        // eligibility filter. This remains safe if an idle cursor expires
        // while the lock is held, and no registration can race the count.
        let sample_capacity = state.active.len();
        let vector_capacity_bound = if sample_capacity == 0 {
            0
        } else {
            sample_capacity.saturating_mul(2).max(4)
        };
        let arc_metadata_bound = std::mem::size_of::<usize>().saturating_mul(4);
        let transition_bound = vector_capacity_bound
            .saturating_mul(std::mem::size_of::<SnapshotId>())
            .saturating_add(sample_capacity.saturating_mul(std::mem::size_of::<SnapshotId>()))
            .saturating_add(arc_metadata_bound);
        admit_transition(transition_bound)?;

        let mut sampled_snapshots = Vec::with_capacity(sample_capacity);
        sampled_snapshots.extend(
            state
                .active
                .values()
                .filter(|registered| registered.holder_still_entitled())
                .map(|registered| registered.snapshot),
        );
        sampled_snapshots.sort_unstable();
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

        let retained_bytes = sampled_snapshots
            .len()
            .saturating_mul(std::mem::size_of::<SnapshotId>())
            .saturating_add(arc_metadata_bound);
        let registered_snapshots = Arc::<[SnapshotId]>::from(sampled_snapshots.as_slice());
        let guard = ActiveRemovalPassGuard {
            registry: self.clone(),
            registered_snapshots,
            history_through,
        };
        #[cfg(any(test, feature = "test-seams"))]
        maybe_pause_vector_repair_snapshot_sample_transition_for_test();
        drop(sampled_snapshots);
        Ok((guard, retained_bytes))
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
    // CDB127: derive from both the original source witness and supplied payer.
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
    #[cfg(test)]
    d1_initialized_control_proofs::observe_factory_credit_for_test(&factory_owner, accountant);
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
    #[cfg(test)]
    d1_initialized_control_proofs::observe_callback_for_test(&loader_for, accountant);
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
    #[cfg(test)]
    d1_initialized_control_proofs::observe_columns_for_test(&columns);
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

impl Drop for DurableVectorGenerationLoader {
    fn drop(&mut self) {
        // The existing source carries the writable persistence/captured-root
        // epoch. Its automatic first-field drop follows this payload cleanup.
        self.partition.index = VectorIndexRef::default();
        self.partition.partition_key = VectorPartitionKey::unpartitioned();
        self.layout.partition_key_columns = Vec::new();
        self.declared_serving_cache = DeclaredServingCache::AsOfOpen(None);
    }
}

impl DeclaredServingCache {
    pub(super) fn get(&self) -> Option<usize> {
        match self {
            Self::Live(cell) => declared_serving_cache_bytes(cell.load(Ordering::SeqCst)),
            Self::AsOfOpen(bytes) => *bytes,
        }
    }
}

impl DurableVectorGenerationSource {
    pub(super) fn is_committed_image(&self) -> bool {
        matches!(self, Self::CommittedImage(_))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_paged_graph<T>(
        &self,
        partition: &VectorPartitionRef,
        base_generation_id: u64,
        generation_id: u64,
        base: Option<&crate::persistence::ChangeGenerationBase>,
        consume: impl FnOnce(crate::persistence::ReadOnlyVectorGenerationGraph<'_>) -> Result<T>,
    ) -> Result<T> {
        match self {
            Self::CommittedImage(source) => match base {
                None => source.with_base_graph(
                    &partition.index,
                    &partition.partition_key,
                    generation_id,
                    consume,
                ),
                Some(base) => source.with_change_graph(
                    &partition.index,
                    &partition.partition_key,
                    base_generation_id,
                    generation_id,
                    base,
                    consume,
                ),
            },
            Self::Writable(_) | Self::CapturedRoot(_) => Err(Error::Other(
                ("paged graph loading requires a retained read image".to_string()).into(),
            )),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn tail_entry_count(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        covered_lsn: Lsn,
    ) -> Result<usize> {
        #[cfg(any(test, feature = "test-seams"))]
        let _reads = crate::vector_observations::observe_reads(index, partition_key, covered_lsn);
        match self {
            Self::Writable(persistence) => {
                persistence.vector_partition_tail_entry_count(index, partition_key, covered_lsn)
            }
            Self::CommittedImage(source) => {
                source.tail_entry_count(index, partition_key, covered_lsn)
            }
            // A captured root admits its suffix from saved metadata before
            // pricing; a whole-journal count walk is never its admission.
            Self::CapturedRoot(_) => Err(Error::Other(
                ("a captured serving root admits its suffix from saved metadata".to_string())
                    .into(),
            )),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn record_bytes(
        &self,
        partition: &VectorPartitionRef,
        base: u64,
        change: Option<u64>,
    ) -> Result<usize> {
        match self {
            Self::Writable(persistence) => persistence.vector_generation_record_bytes(
                &partition.index,
                &partition.partition_key,
                base,
                change,
            ),
            Self::CommittedImage(source) => source.generation_record_bytes(
                &partition.index,
                &partition.partition_key,
                base,
                change,
            ),
            Self::CapturedRoot(root) => root.with_read(&partition.index, |read| {
                RedbPersistence::generation_record_bytes_in_read(
                    read,
                    &partition.index,
                    &partition.partition_key,
                    base,
                    change,
                )
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn load_base(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        generation_id: u64,
    ) -> Result<VectorPartitionBaseGenerationRecord> {
        match self {
            Self::Writable(persistence) => persistence.load_vector_partition_base_generation(
                index,
                partition_key,
                generation_id,
            ),
            Self::CommittedImage(source) => source.load_base(index, partition_key, generation_id),
            Self::CapturedRoot(root) => root.with_read(index, |read| {
                RedbPersistence::load_vector_partition_base_generation_in_read(
                    read,
                    index,
                    partition_key,
                    generation_id,
                )
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn load_change(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        base_generation_id: u64,
        change_generation_id: u64,
    ) -> Result<VectorPartitionChangeGenerationRecord> {
        match self {
            Self::Writable(persistence) => persistence.load_vector_partition_change_generation(
                index,
                partition_key,
                base_generation_id,
                change_generation_id,
            ),
            Self::CommittedImage(source) => source.load_change(
                index,
                partition_key,
                base_generation_id,
                change_generation_id,
            ),
            Self::CapturedRoot(root) => root.with_read(index, |read| {
                RedbPersistence::load_vector_partition_change_generation_in_read(
                    read,
                    index,
                    partition_key,
                    base_generation_id,
                    change_generation_id,
                )
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn load_tail(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        covered_lsn: Lsn,
    ) -> Result<(Vec<VectorEntry>, Lsn)> {
        #[cfg(any(test, feature = "test-seams"))]
        let _reads = crate::vector_observations::observe_reads(index, partition_key, covered_lsn);
        match self {
            Self::Writable(persistence) => {
                persistence.load_vector_partition_tail_entries(index, partition_key, covered_lsn)
            }
            Self::CommittedImage(source) => source.load_tail(index, partition_key, covered_lsn),
            Self::CapturedRoot(root) => root.with_read(index, |read| {
                RedbPersistence::load_vector_partition_tail_entries_in_read(
                    read,
                    index,
                    partition_key,
                    covered_lsn,
                    root.through_lsn(),
                    || Ok(()),
                    |_| Ok(()),
                )
            }),
        }
    }
}

impl DurableVectorGenerationLoader {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn descriptor_usize(value: u64, name: &str) -> Result<usize> {
        usize::try_from(value).map_err(|_| {
            Error::Other(
                (format!("durable vector {name} exceeds this process's addressable range")).into(),
            )
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn checked_mul(left: usize, right: usize, name: &str) -> Result<usize> {
        left.checked_mul(right).ok_or_else(|| {
            Error::Other(
                (format!("durable vector {name} exceeds this process's addressable range")).into(),
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
                (format!("durable vector {name} exceeds this process's addressable range")).into(),
            )
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn tail_reservation(&self, count: usize) -> Result<(usize, usize)> {
        vector_tail_reservation(
            count,
            self.dimension,
            self.quantization,
            self.layout.resolve_policy(self.live_rows, 1),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn route_metadata_workspace(&self) -> Result<usize> {
        vector_route_metadata_workspace(&self.partition)
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

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn load_retained(
        &self,
        generation: DormantVectorGraphGeneration,
        caller_reserved_load_bytes: usize,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<LoadedVectorGraphGeneration> {
        if generation.identity.generation_id != self.descriptor.generation_id
            || generation.identity.covered_tx != self.descriptor.covered_tx
            || generation.identity.covered_lsn != self.descriptor.covered_lsn
            || generation.resident_bytes != 1
            || generation.load_bytes != 1
            || generation.reclaim_bytes
                != committed_layer_least_load(
                    self.live_rows,
                    self.descriptor.durable_bytes,
                    self.dimension,
                    self.quantization,
                    generation.identity.hnsw_m as usize,
                )
            || (caller_reserved_load_bytes != 0 && caller_reserved_load_bytes != 1)
        {
            return Err(Error::Other(
                ("retained vector loader received a descriptor other than the one it owns"
                    .to_string())
                .into(),
            ));
        }
        Self::load_checkpoint(request)?;
        // Admit the escaping budget control before any retained graph charge.
        // The durable source supplies the captured writer role, never a later
        // association rediscovered from an originally untagged accountant.
        let vector_accountant = match &self.source {
            DurableVectorGenerationSource::Writable(persistence) => {
                match persistence.proof_accountant_handle_for(&self.accountant) {
                    Some(original) => original.vector_budget()?,
                    None => self.accountant.vector_budget_for_original_reader()?,
                }
            }
            DurableVectorGenerationSource::CommittedImage(_)
            | DurableVectorGenerationSource::CapturedRoot(_) => {
                self.accountant.vector_budget_for_original_reader()?
            }
        };
        let metadata_workspace = self.route_metadata_workspace()?.max(64 * 1024);
        let _metadata = crate::memory_accounting::ScopedMemoryReservation::try_new_for(
            &self.accountant,
            metadata_workspace,
            "vector_index",
            "load_retained_vector_generation",
            "Evict another vector partition or raise MEMORY_LIMIT before loading this maintained route.",
        )?;

        let tail_count = match self.tail_covered_lsn {
            Some(covered_lsn) => self.source.tail_entry_count(
                &self.partition.index,
                &self.partition.partition_key,
                covered_lsn,
            )?,
            None => 0,
        };
        let (tail_resident, tail_workspace) = if self.tail_covered_lsn.is_some() {
            self.tail_reservation(tail_count)?
        } else {
            (0, 0)
        };
        let mut owned = 0usize;
        // What this load costs, for the attribution sink only: when it
        // started, and the hash meters before it, so its entry can say how
        // many full passes it made over the saved bytes.
        let load_started = contextdb_core::find_attribution::enabled().then(Instant::now);
        let hashes_before = contextdb_core::find_attribution::load_meters();
        // A change layer is checked against its base as the catalog names it.
        let base = (!self.is_base).then_some(crate::persistence::ChangeGenerationBase {
            covered_tx: self.base_covered.0,
            covered_lsn: self.base_covered.1,
            dimension: self.dimension as u32,
            quantization: self.quantization,
            algorithm_version: VECTOR_GRAPH_ALGORITHM_VERSION,
            graph_format_version: VECTOR_GRAPH_FORMAT_VERSION,
        });
        let loaded = self.source.with_paged_graph(
            &self.partition,
            self.base_generation_id,
            self.descriptor.generation_id,
            base.as_ref(),
            |saved| {
                let paged = saved.bytes.starts_with(&crate::persistence::vector_paged::META_MAGIC) || saved.graph_format_version
                        == crate::persistence::vector_paged::VECTOR_GRAPH_PAGED_FORMAT_VERSION;
                if saved.covered_tx != self.descriptor.covered_tx
                    || saved.covered_lsn != self.descriptor.covered_lsn
                    || saved.bytes.len() as u64 != self.descriptor.durable_bytes
                    || saved.dimension as usize != self.dimension
                    || saved.quantization != self.quantization
                    || saved.algorithm_version != VECTOR_GRAPH_ALGORITHM_VERSION
                    || (saved.graph_format_version != VECTOR_GRAPH_FORMAT_VERSION && !paged)
                {
                    return Err(Error::Other((
                        "saved vector graph disagrees with its catalog or column declaration"
                            .to_string()).into()));
                }
                if !paged {
                    return Err(crate::persistence::vector_paged::corruption("saved generation has no published topology pages"));
                }
                if paged {
                    // A paged generation is served from the image's pinned
                    // pages; only its small control handle stays resident.
                    let resident = vector_paged_build::PAGED_GENERATION_RESIDENT_BYTES as usize;
                    #[cfg(any(test, feature = "test-seams"))]
                    contextdb_vector::hnsw::observe_paged_resident_admission(resident, metadata_workspace);
                    // The fresh tail built after this graph is retained under
                    // the same owner: admit its resident bound and workspace
                    // now, exactly as the unpaged path does, so the tail bytes
                    // the generation later returns were charged here.
                    let admitted = resident
                        .checked_add(tail_resident)
                        .and_then(|value| value.checked_add(tail_workspace))
                        .ok_or_else(|| Error::Other(("retained vector load overflow".to_string()).into()))?;
                    self.accountant.try_allocate_for(
                        admitted,
                        "vector_index",
                        "load_retained_vector_generation",
                        "Evict another vector partition or raise MEMORY_LIMIT before loading this maintained route.",
                    )?;
                    owned = admitted;
                    Self::load_checkpoint(request)?;
                    let graph = self.paged_generation_graph(saved.bytes)?;
                    contextdb_core::find_attribution::note_saved_graph_load(
                        contextdb_core::find_attribution::LoadCharge {
                            saved_bytes: saved.bytes.len(),
                            priced_bytes: generation.reclaim_bytes,
                            minimum_bytes: resident,
                            admitted_bytes: resident,
                            held_bytes: resident,
                            cache_bytes: 0,
                            minimum_cache_bytes: 0,
                            maximum_cache_bytes: 0,
                        },
                    );
                    return Ok((graph, resident));
                }
                // The serving cache's ceiling follows the declaration and the
                // data, not a constant: a size this reader was given, else a
                // share of the memory the session declared, else what this
                // partition's own pages occupy. A ceiling below the partition
                // makes a walk re-read pages it has already authenticated,
                // which is what a first find over a large partition was paying.
                let ceiling = contextdb_vector::hnsw::durable_reader_cache_ceiling(
                    // What the consumer declared, where it declared one. It
                    // beats the share below, and the partition still caps both.
                    self.declared_serving_cache.get(),
                    self.accountant.usage().limit,
                    saved
                        .source
                        .total_page_bytes()
                        .unwrap_or(saved.bytes.len()),
                );
                let (minimum_cache, maximum_cache) =
                    HnswIndex::durable_reader_cache_bounds(ceiling);
                let source_resident = saved.source.retained_bytes();
                let minimum_resident = HnswIndex::durable_reader_resident_bound_for_cache(
                    saved.bytes,
                    self.dimension,
                    self.quantization,
                    generation.identity.hnsw_m as usize,
                    minimum_cache,
                )?
                .checked_add(source_resident)
                .ok_or_else(|| {
                    Error::Other(("retained vector page metadata overflow".to_string()).into())
                })?;
                let minimum_total = minimum_resident
                    .checked_add(tail_resident)
                    .and_then(|value| value.checked_add(tail_workspace))
                    .ok_or_else(|| Error::Other(("retained vector load overflow".to_string()).into()))?;
                // Route metadata is already held by its scoped reservation,
                // so admission below asks only for the remaining bytes. Load
                // attribution still reports the complete coexisting charge.
                let complete_minimum_total = minimum_total
                    .checked_add(metadata_workspace)
                    .ok_or_else(|| Error::Other(("retained vector load overflow".to_string()).into()))?;
                // Leave room for later layers' least loads. When the declared
                // spare memory also covers every later layer's page bodies,
                // keep this layer's complete cache: dividing it equally would
                // leave a large partition short even though all pages fit.
                // Otherwise retain the old equal share, so a tight reader
                // still admits every later topology and least serving slot.
                let share = contextdb_core::serving_cache_share::claim_share(
                    generation.reclaim_bytes,
                    usize::try_from(generation.identity.durable_bytes).unwrap_or(usize::MAX),
                );
                let wanted = minimum_total.saturating_add(share.reserved_for_later);
                // Make room for exactly this load's least footprint, read from
                // its saved topology, and the room reserved for the rest, by
                // evicting idle graphs this statement is not searching.
                if let Some(available) = self.accountant.usage().available
                    && available < wanted
                {
                    self.accountant.make_room_for_graph_load(wanted - available);
                }
                let future_page_headroom = share.full_page_bytes_for_later.saturating_sub(
                    share
                        .divisor
                        .saturating_sub(1)
                        .saturating_mul(minimum_cache),
                );
                // A filled slot has a lookup entry and a recency stamp in
                // addition to its page body. Reserve the maximum directory
                // growth before rounding spare bytes to whole pages; using
                // page bodies alone can make a fitting smaller cache refuse.
                let directory_growth = HnswIndex::durable_reader_cache_directory_growth(
                    saved.bytes.len(),
                    maximum_cache,
                );
                let cache_headroom = self
                    .accountant
                    .usage()
                    .available
                    .map_or(maximum_cache - minimum_cache, |available| {
                        let spare = available
                            .saturating_sub(wanted)
                            .saturating_sub(directory_growth);
                        let full_extra = maximum_cache - minimum_cache;
                        if spare >= full_extra.saturating_add(future_page_headroom) {
                            full_extra
                        } else {
                            spare.saturating_div(share.divisor).min(full_extra)
                        }
                    });
                let cache_bytes = minimum_cache
                    + cache_headroom / minimum_cache * minimum_cache;
                contextdb_core::find_attribution::count("paged_graph_loads", 1);
                contextdb_core::find_attribution::count(
                    "paged_load_available_bytes",
                    self.accountant.usage().available.unwrap_or(0) as u64,
                );
                contextdb_core::find_attribution::count(
                    "paged_load_used_bytes",
                    self.accountant.usage().used as u64,
                );
                contextdb_core::find_attribution::count(
                    "paged_load_minimum_total_bytes",
                    complete_minimum_total as u64,
                );
                contextdb_core::find_attribution::count(
                    "paged_load_maximum_cache_bytes",
                    maximum_cache as u64,
                );
                contextdb_core::find_attribution::count(
                    "paged_load_cache_bytes",
                    cache_bytes as u64,
                );
                let resident = HnswIndex::durable_reader_resident_bound_for_cache(
                    saved.bytes,
                    self.dimension,
                    self.quantization,
                    generation.identity.hnsw_m as usize,
                    cache_bytes,
                )?
                .checked_add(source_resident)
                .ok_or_else(|| {
                    Error::Other(("retained vector page metadata overflow".to_string()).into())
                })?;
                let total = resident
                    .checked_add(tail_resident)
                    .and_then(|value| value.checked_add(tail_workspace))
                    .ok_or_else(|| Error::Other(("retained vector load overflow".to_string()).into()))?;
                let complete_total = total
                    .checked_add(metadata_workspace)
                    .ok_or_else(|| Error::Other(("retained vector load overflow".to_string()).into()))?;
                #[cfg(any(test, feature = "test-seams"))]
                contextdb_vector::hnsw::observe_paged_resident_admission(total, metadata_workspace);
                self.accountant
                    .try_allocate_for(
                        total,
                        "vector_index",
                        "load_retained_vector_generation",
                        "Evict another vector partition or raise MEMORY_LIMIT before loading this maintained route.",
                    )
                    .inspect_err(|_| {
                        contextdb_core::find_attribution::note_refused_saved_graph_load();
                        contextdb_core::find_attribution::count("paged_graph_loads_refused", 1);
                    })?;
                owned = total;
                Self::load_checkpoint(request)?;
                let _decoding = contextdb_core::find_attribution::stage("graph_load_decode");
                // The record these bytes came from was read under its own
                // digest, which covers every one of them; the decode does not
                // hash them a second time.
                let graph = HnswIndex::decode_paged_authenticated_generation(
                    saved.bytes,
                    saved.source,
                    self.dimension,
                    self.quantization,
                    resident,
                    cache_bytes,
                )?;
                let actual_resident = graph.estimated_resident_bytes();
                contextdb_core::find_attribution::note_saved_graph_load(
                    contextdb_core::find_attribution::LoadCharge {
                        saved_bytes: saved.bytes.len(),
                        priced_bytes: generation.reclaim_bytes,
                        minimum_bytes: complete_minimum_total,
                        admitted_bytes: complete_total,
                        held_bytes: actual_resident,
                        cache_bytes,
                        minimum_cache_bytes: minimum_cache,
                        maximum_cache_bytes: maximum_cache,
                    },
                );
                if let Some(started) = load_started {
                    use contextdb_core::find_attribution::Field;
                    let now = contextdb_core::find_attribution::load_meters();
                    contextdb_core::find_attribution::note_load(vec![
                        (
                            "partition".to_string(),
                            Field::Text(format!("{:?}", self.partition.partition_key)),
                        ),
                        (
                            "layer".to_string(),
                            Field::Text(if self.is_base { "base" } else { "change" }.to_string()),
                        ),
                        (
                            "saved_bytes".to_string(),
                            Field::Count(saved.bytes.len() as u64),
                        ),
                        (
                            "priced_bytes".to_string(),
                            Field::Count(generation.reclaim_bytes as u64),
                        ),
                        (
                            "minimum_total_bytes".to_string(),
                            Field::Count(complete_minimum_total as u64),
                        ),
                        ("admitted_bytes".to_string(), Field::Count(complete_total as u64)),
                        ("held_bytes".to_string(), Field::Count(actual_resident as u64)),
                        ("cache_bytes".to_string(), Field::Count(cache_bytes as u64)),
                        (
                            "hash_passes".to_string(),
                            Field::Count(now.hash_passes.saturating_sub(hashes_before.hash_passes)),
                        ),
                        (
                            "hashed_bytes".to_string(),
                            Field::Count(now.hashed_bytes.saturating_sub(hashes_before.hashed_bytes)),
                        ),
                        (
                            "ms".to_string(),
                            Field::Millis(started.elapsed().as_secs_f64() * 1e3),
                        ),
                    ]);
                }
                Self::load_checkpoint(request)?;
                Ok((graph, actual_resident))
            },
        );
        let (graph, resident_bytes) = match loaded {
            Ok(loaded) => loaded,
            Err(error) => {
                self.accountant.release(owned);
                return Err(error);
            }
        };
        let fresh_tail = (|| -> Result<_> {
            Ok(match self.tail_covered_lsn {
                None => None,
                Some(covered_lsn) => {
                    let (entries, replayed_through_lsn) = self.source.load_tail(
                        &self.partition.index,
                        &self.partition.partition_key,
                        covered_lsn,
                    )?;
                    if entries.len() > tail_count {
                        return Err(Error::Other(
                            ("vector journal grew beyond its admitted replay bound".to_string())
                                .into(),
                        ));
                    }
                    let graph = HnswIndex::from_vector_entries_with_progress(
                        &entries,
                        self.dimension,
                        self.quantization,
                        self.layout.resolve_policy(self.live_rows, 1),
                        |done| {
                            if request(DormantVectorLoadRequest::Progress {
                                done,
                                total: entries.len(),
                            }) {
                                Ok(())
                            } else {
                                Err(Error::ReadCancelled)
                            }
                        },
                    )?;
                    let actual = graph.estimated_resident_bytes();
                    if actual > tail_resident {
                        return Err(Error::Other(
                            ("vector tail graph exceeded its admitted resident bound".to_string())
                                .into(),
                        ));
                    }
                    Some((graph, actual, replayed_through_lsn))
                }
            })
        })();
        let fresh_tail = match fresh_tail {
            Ok(fresh_tail) => fresh_tail,
            Err(error) => {
                self.accountant.release(owned);
                return Err(error);
            }
        };
        let actual_tail = fresh_tail.as_ref().map(|(_, bytes, _)| *bytes).unwrap_or(0);
        let retained = resident_bytes.saturating_add(actual_tail);
        if let Err(error) = Self::load_checkpoint(request) {
            self.accountant.release(owned);
            return Err(error);
        }
        self.accountant.release(owned.saturating_sub(retained));
        Ok(LoadedVectorGraphGeneration {
            graph,
            resident_bytes,
            accountant: (vector_accountant.clone()),
            caller_adopted_bytes: 0,
            fresh_tail: fresh_tail.map(|(graph, resident_bytes, replayed_through_lsn)| {
                LoadedVectorFreshTail {
                    graph,
                    resident_bytes,
                    accountant: (vector_accountant.clone()),
                    replayed_through_lsn,
                }
            }),
        })
    }
}

impl DurableVectorGenerationLoader {
    /// Serve a published paged base from its native pages through readers of
    /// the writer's published root. Nothing population-sized is decoded.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn paged_generation_graph(&self, meta_bytes: &[u8]) -> Result<HnswIndex> {
        let capture = match &self.source {
            DurableVectorGenerationSource::Writable(persistence) => {
                persistence.paged_root_capture()?
            }
            DurableVectorGenerationSource::CommittedImage(source) => source.paged_root_capture(),
            DurableVectorGenerationSource::CapturedRoot(root) => root.paged_root_capture(),
        };
        paged_generation_graph_from(
            capture,
            &self.partition,
            &self.descriptor,
            self.dimension,
            self.quantization,
            &self.layout,
            self.accountant.vector_budget()?,
            meta_bytes,
            self.is_base,
            self.base_generation_id,
            self.base_covered,
        )
    }
}

/// Resident and construction workspace of a journal-suffix graph of `count`
/// points, shared by every loader that rebuilds a suffix.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn vector_tail_reservation(
    count: usize,
    dimension: usize,
    quantization: VectorQuantization,
    policy: contextdb_vector::store::ResolvedVectorPolicy,
) -> Result<(usize, usize)> {
    let resident =
        HnswIndex::estimated_resident_bytes_with_m(count, dimension, quantization, policy.hnsw_m);
    let vector_values = count
        .checked_mul(dimension)
        .and_then(|values| values.checked_mul(std::mem::size_of::<f32>()))
        .ok_or_else(|| {
            Error::Other(
                ("durable vector tail values exceed this process's addressable range".to_string())
                    .into(),
            )
        })?;
    let entry_overhead = count
        .checked_mul(std::mem::size_of::<VectorEntry>().saturating_add(256))
        .ok_or_else(|| {
            Error::Other(
                ("durable vector tail entries exceed this process's addressable range".to_string())
                    .into(),
            )
        })?;
    // During construction the immutable journal values, the converted
    // stored values, and the new graph coexist. This is an admission upper
    // bound; the unused part is returned immediately after construction.
    let workspace = DurableVectorGenerationLoader::checked_add(
        vector_values,
        entry_overhead,
        "tail workspace",
    )?;
    let workspace = DurableVectorGenerationLoader::checked_add(
        workspace,
        resident.checked_mul(2).ok_or_else(|| {
            Error::Other(
                ("durable vector tail build exceeds this process's addressable range".to_string())
                    .into(),
            )
        })?,
        "tail workspace",
    )?;
    let build_reservation =
        HnswIndex::estimated_build_reservation(count, dimension, quantization, policy);
    let workspace = workspace.max(build_reservation.saturating_sub(resident));
    Ok((resident, workspace))
}

/// Route metadata that coexists with one generation or suffix load,
/// independent of graph size.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn vector_route_metadata_workspace(partition: &VectorPartitionRef) -> Result<usize> {
    // Expected-prefix prevalidation caps decoded route lengths before serde.
    // Two base/change routes, geometric component capacity, physical keys,
    // page-manifest route and framing coexist independently of graph size.
    let route = DurableVectorGenerationLoader::checked_add(
        DurableVectorGenerationLoader::checked_add(
            partition.index.table.len(),
            partition.index.column.len(),
            "generation index metadata",
        )?,
        partition.partition_key.estimated_bytes(),
        "generation route metadata",
    )?;
    DurableVectorGenerationLoader::checked_add(
        DurableVectorGenerationLoader::checked_mul(
            DurableVectorGenerationLoader::checked_add(route, 128, "generation route framing")?,
            8,
            "generation route owners",
        )?,
        1024,
        "generation metadata scratch",
    )
}

/// Serve a published paged base from its native pages through readers the
/// supplied capture opens. Its page cache is charged to `accountant`.
#[allow(clippy::too_many_arguments)]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn paged_generation_graph_from(
    capture: crate::persistence::vector_paged::PagedRootCapture,
    partition: &VectorPartitionRef,
    descriptor: &VectorPartitionGenerationDescriptor,
    dimension: usize,
    quantization: VectorQuantization,
    layout: &VectorIndexLayout,
    accountant: impl Into<contextdb_vector::MemoryBudgetOwner>,
    meta_bytes: &[u8],
    is_base: bool,
    base_generation_id: u64,
    base_covered: (TxId, Lsn),
) -> Result<HnswIndex> {
    let accountant: contextdb_vector::MemoryBudgetOwner = accountant.into();

    use crate::persistence::vector_paged::{NativePagedGraphSource, PagedGenerationMeta};
    let mut backing_owner = contextdb_vector::VectorCopyAdmission::new(accountant.clone());
    backing_owner.reserve_for(
        meta_bytes
            .len()
            .checked_mul(12)
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(|| {
                crate::persistence::vector_paged::corruption("saved backing workspace overflow")
            })?,
        "vector",
        "resolve_native_change_backing",
        "Release idle vector state before resolving a saved backing.",
    )?;
    let original = PagedGenerationMeta::decode(meta_bytes)?;
    let captured = capture()?;
    captured.authenticate()?;
    let read = &captured.read;
    let meta = crate::persistence::vector_paged::import_tail::resolve_native_change_backing(
        read, &original,
    )?;
    let progress = meta.progress()?;
    if progress.generation_id != descriptor.generation_id
        || progress.frontier_tx != descriptor.covered_tx
        || progress.frontier_lsn != descriptor.covered_lsn
        || progress.phase != contextdb_vector::hnsw::paged_builder::PagedVectorPhase::Complete
        || meta.declaration.dimension as usize != dimension
        || meta.declaration.quantization != quantization
    {
        return Err(Error::StoreCorrupted {
            path: "vector_paged_generation_pages".into(),
            reason: "paged generation metadata disagrees with its catalog".into(),
        });
    }
    let mut policy = layout.resolve_policy(progress.graph.points, 1);
    policy.hnsw_m = meta.declaration.hnsw_m as usize;
    policy.hnsw_ef_construction = meta.declaration.hnsw_ef_construction as usize;
    policy.hnsw_ef_search = meta.declaration.hnsw_ef_search as usize;
    let plan = vector_paged_build::paged_batch_plan(&meta.declaration)?;
    let node_limit = match &meta.saved {
        Some(binding) => {
            let route = RedbPersistence::vector_partition_prefix(
                &partition.index,
                &partition.partition_key,
            );
            if binding.route != *blake3::hash(&route).as_bytes()
                || binding.is_base != is_base
                || binding.base_generation_id != base_generation_id
                || (!is_base && (binding.base_tx, binding.base_lsn) != base_covered)
                || progress.declaration_digest != meta.declaration.digest()?
                || meta.declaration.policy_revision != descriptor.policy_revision
                || meta.declaration.hnsw_m != descriptor.hnsw_m
                || meta.declaration.hnsw_ef_construction != descriptor.hnsw_ef_construction
            {
                return Err(crate::persistence::vector_paged::corruption(
                    "saved manifest binding disagrees with its selected root catalog",
                ));
            }
            usize::try_from(binding.node_limit).map_err(|_| {
                crate::persistence::vector_paged::corruption("saved point window exceeds usize")
            })?
        }
        None => plan.max_node_bytes,
    };
    let source = Arc::new(NativePagedGraphSource {
        capture,
        index: partition.index.clone(),
        schema_binding: meta.schema_binding,
        storage_id: meta.storage_id,
        declaration: meta.declaration.clone(),
        frontier: progress.frontier_tx,
        // Identities: graph points and graph-identity members together.
        points: progress.graph.points + progress.members,
        rows: meta.saved.as_ref().map_or(
            Ok(progress.graph.points + progress.members),
            |saved| {
                usize::try_from(saved.distinct_rows).map_err(|_| {
                    crate::persistence::vector_paged::corruption("saved row count exceeds usize")
                })
            },
        )?,
        node_limit,
        checkpoint: Arc::new(|| Ok(())),
        page_set: meta.page_set,
        verified: Arc::new(parking_lot::Mutex::new(false)),
    });
    #[cfg(any(test, feature = "test-seams"))]
    contextdb_vector::hnsw::observe_paged_native_control_allocation();
    let tail = contextdb_vector::hnsw::paged_builder::PagedVectorTail::new(
        partition.clone(),
        progress,
        policy,
        dimension,
        quantization,
        Arc::new(vector_paged_build::SealedPagedGeneration),
        source,
        accountant,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    )?;
    Ok(HnswIndex::from_paged_tail(tail))
}

impl DormantVectorGraphLoader for DurableVectorGenerationLoader {
    fn load(
        &self,
        generation: DormantVectorGraphGeneration,
        caller_reserved_load_bytes: usize,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<LoadedVectorGraphGeneration> {
        self.load_selected_tail(
            generation,
            caller_reserved_load_bytes,
            request,
            self.tail_covered_lsn,
        )
    }

    fn load_reusing_fresh_tail(
        &self,
        generation: DormantVectorGraphGeneration,
        caller_reserved_load_bytes: usize,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
        fresh_tail_present: bool,
    ) -> Result<LoadedVectorGraphGeneration> {
        let tail_covered_lsn = if fresh_tail_present
            && matches!(self.source, DurableVectorGenerationSource::Writable(_))
        {
            None
        } else {
            self.tail_covered_lsn
        };
        self.load_selected_tail(
            generation,
            caller_reserved_load_bytes,
            request,
            tail_covered_lsn,
        )
    }
}

impl DurableVectorGenerationLoader {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn load_selected_tail(
        &self,
        generation: DormantVectorGraphGeneration,
        caller_reserved_load_bytes: usize,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
        tail_covered_lsn: Option<Lsn>,
    ) -> Result<LoadedVectorGraphGeneration> {
        if self.source.is_committed_image() {
            return self.load_retained(generation, caller_reserved_load_bytes, request);
        }
        let mut resident_bytes =
            Self::descriptor_usize(self.descriptor.resident_bytes, "resident byte declaration")?;
        let graph_workspace =
            Self::descriptor_usize(self.descriptor.durable_bytes, "graph decode workspace")?;
        let known_load_reservation =
            Self::checked_add(resident_bytes, graph_workspace, "known generation load")?;
        if generation.identity.generation_id != self.descriptor.generation_id
            || generation.identity.covered_tx != self.descriptor.covered_tx
            || generation.identity.covered_lsn != self.descriptor.covered_lsn
            || generation.resident_bytes != resident_bytes
            || generation.load_bytes != known_load_reservation
        {
            return Err(Error::Other(
                ("durable vector loader received a descriptor other than the one it owns"
                    .to_string())
                .into(),
            ));
        }
        if caller_reserved_load_bytes != 0 && caller_reserved_load_bytes != known_load_reservation {
            return Err(Error::Other(
                ("durable vector caller reservation disagrees with graph load admission"
                    .to_string())
                .into(),
            ));
        }

        Self::load_checkpoint(request)?;
        let _metadata = crate::memory_accounting::ScopedMemoryReservation::try_new_for(
            &self.accountant,
            self.route_metadata_workspace()?,
            "vector_index",
            "load_durable_vector_generation",
            "Evict another vector partition or raise MEMORY_LIMIT before loading this maintained route.",
        )?;
        let paged_resident_floor =
            (crate::database::vector_paged_build::PAGED_GENERATION_RESIDENT_BYTES as usize)
                .max(resident_bytes);
        let caller_charged = caller_reserved_load_bytes != 0;
        let mut loader_reservation = if caller_charged {
            0
        } else {
            paged_resident_floor
        };
        if !caller_charged {
            self.accountant.try_allocate_for(
                loader_reservation,
                "vector_index",
                "load_durable_vector_generation",
                "Evict another vector partition, reduce the selected scope, or raise MEMORY_LIMIT before loading this maintained route.",
            )?;
        }
        let record_workspace = match self.source.record_bytes(
            &self.partition,
            self.base_generation_id,
            (!self.is_base).then_some(self.descriptor.generation_id),
        ) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.accountant.release(loader_reservation);
                return Err(error);
            }
        };
        let tail_count = match tail_covered_lsn {
            Some(covered_lsn) => match self.source.tail_entry_count(
                &self.partition.index,
                &self.partition.partition_key,
                covered_lsn,
            ) {
                Ok(count) => count,
                Err(error) => {
                    self.accountant.release(loader_reservation);
                    return Err(error);
                }
            },
            None => 0,
        };
        if let Err(error) = Self::load_checkpoint(request) {
            self.accountant.release(loader_reservation);
            return Err(error);
        }
        let (tail_resident_reservation, tail_workspace) = if tail_covered_lsn.is_some() {
            match self.tail_reservation(tail_count) {
                Ok(res) => res,
                Err(error) => {
                    self.accountant.release(loader_reservation);
                    return Err(error);
                }
            }
        } else {
            (0, 0)
        };
        let retained_reservation = match Self::checked_add(
            paged_resident_floor,
            tail_resident_reservation,
            "resident generation reservation",
        ) {
            Ok(res) => res,
            Err(error) => {
                self.accountant.release(loader_reservation);
                return Err(error);
            }
        };
        let workspace_reservation = match Self::checked_add(
            graph_workspace.max(record_workspace.saturating_mul(2)),
            tail_workspace,
            "generation workspace reservation",
        ) {
            Ok(res) => res,
            Err(error) => {
                self.accountant.release(loader_reservation);
                return Err(error);
            }
        };
        let mut total_reservation = match Self::checked_add(
            retained_reservation,
            workspace_reservation,
            "generation load reservation",
        ) {
            Ok(res) => res,
            Err(error) => {
                self.accountant.release(loader_reservation);
                return Err(error);
            }
        };
        if caller_charged {
            let additional = total_reservation
                .checked_sub(caller_reserved_load_bytes)
                .ok_or_else(|| {
                    Error::Other(
                        ("durable vector caller reservation exceeds load admission".to_string())
                            .into(),
                    )
                })
                .inspect_err(|_| {
                    self.accountant.release(loader_reservation);
                })?;
            if additional != 0 && !request(DormantVectorLoadRequest::Reserve(additional)) {
                self.accountant.release(loader_reservation);
                return Err(Error::ReadCancelled);
            }
        } else {
            let remaining = total_reservation.saturating_sub(loader_reservation);
            if remaining != 0 {
                if let Err(error) = self.accountant.try_allocate_for(
                    remaining,
                    "vector_index",
                    "load_durable_vector_generation",
                    "Evict another vector partition, reduce the selected scope, or raise MEMORY_LIMIT before loading this maintained route.",
                ) {
                    self.accountant.release(loader_reservation);
                    return Err(error);
                }
                loader_reservation = loader_reservation.saturating_add(remaining);
            }
        }

        let loaded = (|| -> Result<_> {
            Self::load_checkpoint(request)?;
            let (graph_bytes, paged) = if self.is_base {
                let record = self.source.load_base(
                    &self.partition.index,
                    &self.partition.partition_key,
                    self.descriptor.generation_id,
                )?;
                let paged = record
                    .graph
                    .bytes
                    .starts_with(&crate::persistence::vector_paged::META_MAGIC)
                    || record.graph_format_version
                        == crate::persistence::vector_paged::VECTOR_GRAPH_PAGED_FORMAT_VERSION;
                if record.highest_included_tx != self.descriptor.covered_tx
                    || record.highest_included_lsn != self.descriptor.covered_lsn
                    || record.dimension as usize != self.dimension
                    || record.quantization != self.quantization
                    || record.algorithm_version != VECTOR_GRAPH_ALGORITHM_VERSION
                    || (record.graph_format_version != VECTOR_GRAPH_FORMAT_VERSION && !paged)
                {
                    return Err(Error::Other(
                        ("saved vector base disagrees with its catalog or column declaration"
                            .to_string())
                        .into(),
                    ));
                }
                (record.graph.bytes, paged)
            } else {
                let record = self.source.load_change(
                    &self.partition.index,
                    &self.partition.partition_key,
                    self.base_generation_id,
                    self.descriptor.generation_id,
                )?;
                if record.covered_through_tx != self.descriptor.covered_tx
                    || record.covered_through_lsn != self.descriptor.covered_lsn
                    || record.dimension as usize != self.dimension
                    || record.quantization != self.quantization
                    || record.algorithm_version != VECTOR_GRAPH_ALGORITHM_VERSION
                    || record.graph_format_version != VECTOR_GRAPH_FORMAT_VERSION
                {
                    return Err(Error::Other((
                        "saved vector change graph disagrees with its catalog or column declaration"
                            .to_string()).into()));
                }
                let paged = record
                    .graph
                    .bytes
                    .starts_with(&crate::persistence::vector_paged::META_MAGIC);
                (record.graph.bytes, paged)
            };
            Self::load_checkpoint(request)?;
            if graph_bytes.len() != graph_workspace {
                return Err(Error::Other(
                    ("saved vector graph length disagrees with its catalog".to_string()).into(),
                ));
            }
            #[cfg(feature = "in-process-test-seams")]
            crate::writer_query_test_seams::note_saved_graph_load(graph_bytes.len());
            #[cfg(any(test, feature = "test-seams"))]
            if contextdb_vector::test_seam::take_dormant_vector_decode_failure_for_test() {
                return Err(Error::Other(
                    ("injected dormant vector decode failure".to_string()).into(),
                ));
            }
            Self::load_checkpoint(request)?;
            if !paged {
                return Err(crate::persistence::vector_paged::corruption(
                    "saved generation has no published topology pages",
                ));
            }
            let current_resident = if paged {
                paged_resident_floor
            } else {
                HnswIndex::durable_resident_bound(
                    &graph_bytes,
                    self.dimension,
                    self.quantization,
                    generation.identity.hnsw_m as usize,
                )?
            };
            // The codec moves point values into the serving graph and consumes
            // adjacency as it installs edges. Its encoded payload coexists with
            // one graph, not a second graph with full neighbour capacity.
            let decode_total = current_resident
                .saturating_add(record_workspace.saturating_mul(2))
                .saturating_add(tail_resident_reservation)
                .saturating_add(tail_workspace);
            let additional = decode_total.saturating_sub(total_reservation);
            if additional != 0 {
                if caller_charged {
                    if !request(DormantVectorLoadRequest::Reserve(additional)) {
                        return Err(Error::ReadCancelled);
                    }
                } else {
                    self.accountant.try_allocate_for(additional, "vector_index", "load_durable_vector_generation", "Evict another vector partition or raise MEMORY_LIMIT before loading this maintained route.")?;
                    loader_reservation = loader_reservation.saturating_add(additional);
                }
                total_reservation = total_reservation.saturating_add(additional);
            }
            resident_bytes = current_resident;
            let graph = if paged {
                self.paged_generation_graph(&graph_bytes)?
            } else {
                HnswIndex::decode_durable_generation(
                    &graph_bytes,
                    self.dimension,
                    self.quantization,
                )?
            };
            Self::load_checkpoint(request)?;
            if !paged && graph.estimated_resident_bytes() != resident_bytes {
                return Err(Error::Other(
                    ("saved vector graph residency disagrees with its catalog".to_string()).into(),
                ));
            }

            let fresh_tail = match tail_covered_lsn {
                None => None,
                Some(covered_lsn) => {
                    Self::load_checkpoint(request)?;
                    let (entries, replayed_through_lsn) = self.source.load_tail(
                        &self.partition.index,
                        &self.partition.partition_key,
                        covered_lsn,
                    )?;
                    Self::load_checkpoint(request)?;
                    if entries.len() > tail_count {
                        return Err(Error::Other(
                            ("vector journal grew beyond its admitted replay bound".to_string())
                                .into(),
                        ));
                    }
                    let graph = HnswIndex::from_vector_entries_with_progress(
                        &entries,
                        self.dimension,
                        self.quantization,
                        self.layout.resolve_policy(self.live_rows, 1),
                        |done| {
                            if request(DormantVectorLoadRequest::Progress {
                                done,
                                total: entries.len(),
                            }) {
                                Ok(())
                            } else {
                                Err(Error::ReadCancelled)
                            }
                        },
                    )?;
                    Self::load_checkpoint(request)?;
                    let actual_resident = graph.estimated_resident_bytes();
                    if actual_resident > tail_resident_reservation {
                        return Err(Error::Other(
                            ("vector tail graph exceeded its admitted resident bound".to_string())
                                .into(),
                        ));
                    }
                    Some((graph, actual_resident, replayed_through_lsn))
                }
            };
            Ok((graph, fresh_tail))
        })();

        match loaded {
            Err(error) => {
                self.accountant.release(loader_reservation);
                Err(error)
            }
            Ok((graph, fresh_tail)) => {
                // One admitted adapter follows every independently surviving graph
                // from this invocation, including the fresh tail and a zero-byte owner.
                let retained_accountant = match self.accountant.vector_budget() {
                    Ok(accountant) => accountant,
                    Err(error) => {
                        drop(fresh_tail);
                        drop(graph);
                        self.accountant.release(loader_reservation);
                        return Err(error);
                    }
                };
                let actual_tail_resident =
                    fresh_tail.as_ref().map(|(_, bytes, _)| *bytes).unwrap_or(0);
                let retained = match Self::checked_add(
                    resident_bytes,
                    actual_tail_resident,
                    "loaded generation residency",
                ) {
                    Ok(retained) => retained,
                    Err(error) => {
                        drop(fresh_tail);
                        drop(graph);
                        self.accountant.release(loader_reservation);
                        return Err(error);
                    }
                };
                if let Err(error) = Self::load_checkpoint(request) {
                    drop(fresh_tail);
                    drop(graph);
                    self.accountant.release(loader_reservation);
                    return Err(error);
                }
                if caller_charged {
                    match self.accountant.adopt_caller_reservation(retained) {
                        Ok(true) => {}
                        Ok(false) => {
                            drop(fresh_tail);
                            drop(graph);
                            self.accountant.release(loader_reservation);
                            return Err(Error::Other(
                                ("bounded vector load lost its caller reservation transfer"
                                    .to_string())
                                .into(),
                            ));
                        }
                        Err(error) => {
                            drop(fresh_tail);
                            drop(graph);
                            self.accountant.release(loader_reservation);
                            return Err(error);
                        }
                    }
                }
                if !caller_charged {
                    self.accountant
                        .release(total_reservation.saturating_sub(retained));
                }
                let fresh_tail = fresh_tail.map(|(graph, resident_bytes, replayed_through_lsn)| {
                    LoadedVectorFreshTail {
                        graph,
                        resident_bytes,
                        accountant: (retained_accountant.clone()),
                        replayed_through_lsn,
                    }
                });
                Ok(LoadedVectorGraphGeneration {
                    graph,
                    resident_bytes,
                    accountant: (retained_accountant.clone()),
                    caller_adopted_bytes: if caller_charged { retained } else { 0 },
                    fresh_tail,
                })
            }
        }
    }
}

/// What a statement keeps for one committed-image layer it has still to
/// load, from the catalog alone: a paged load of the points that layer can
/// hold, never more than its partition's rows. A statement keeps room for
/// every layer it has still to load at that layer's own estimate, so pricing
/// a small change layer at its whole partition would starve the loads before
/// it of serving cache the reservation never needed.
pub(super) fn committed_layer_least_load(
    live_rows: usize,
    durable_bytes: u64,
    dimension: usize,
    quantization: VectorQuantization,
    m: usize,
) -> usize {
    let points = if quantization == VectorQuantization::F32 {
        live_rows.min(
            contextdb_vector::hnsw::HnswIndex::paged_reader_points_bound(durable_bytes, dimension),
        )
    } else {
        live_rows
    };
    contextdb_vector::hnsw::HnswIndex::paged_reader_resident_bound(points, m)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn durable_vector_generation_identity(
    descriptor: VectorPartitionGenerationDescriptor,
    layout: &VectorIndexLayout,
    live_rows: usize,
) -> Result<VectorGraphGeneration> {
    let fallback = layout.resolve_policy(live_rows, 1);
    let fallback_m = u32::try_from(fallback.hnsw_m).map_err(|_| {
        Error::Other(("resolved HNSW M exceeds the durable range".to_string()).into())
    })?;
    let fallback_ef_construction = u32::try_from(fallback.hnsw_ef_construction).map_err(|_| {
        Error::Other(("resolved HNSW EF_CONSTRUCTION exceeds the durable range".to_string()).into())
    })?;
    Ok(VectorGraphGeneration {
        generation_id: descriptor.generation_id,
        covered_tx: descriptor.covered_tx,
        covered_lsn: descriptor.covered_lsn,
        durable_bytes: descriptor.durable_bytes,
        policy_revision: if descriptor.policy_revision == 0 {
            fallback.policy_revision
        } else {
            descriptor.policy_revision
        },
        hnsw_m: if descriptor.hnsw_m == 0 {
            fallback_m
        } else {
            descriptor.hnsw_m
        },
        hnsw_ef_construction: if descriptor.hnsw_ef_construction == 0 {
            fallback_ef_construction
        } else {
            descriptor.hnsw_ef_construction
        },
    })
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn register_dormant_vector_generations(
    vector: &VectorStore,
    persistence: &Arc<RedbPersistence>,
    accountant: &crate::memory_accounting::AccountantHandle,
    declared_serving_cache: DeclaredServingCache,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
) -> Result<()> {
    let known_routes = vector_partition_routes(vector, vector_copy_owner)?;
    let loaded = persistence.inspect_vector_partition_generation_catalog(&known_routes)?;
    register_loaded_dormant_vector_generations(
        vector,
        loaded,
        DurableVectorGenerationSource::Writable(persistence.clone()),
        accountant,
        Some(persistence.as_ref()),
        declared_serving_cache,
    )
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn register_loaded_dormant_vector_generations(
    vector: &VectorStore,
    loaded: LoadedVectorPartitionGenerationCatalog,
    source: DurableVectorGenerationSource,
    accountant: &crate::memory_accounting::AccountantHandle,
    journal_truncation_owner: Option<&RedbPersistence>,
    declared_serving_cache: DeclaredServingCache,
) -> Result<()> {
    for (partition, reason) in loaded.quarantines {
        vector.quarantine_partition_route(&partition, reason);
    }
    let catalogs = loaded.catalogs;
    for catalog in catalogs {
        let mut maintenance_vector_copy_owner =
            contextdb_vector::VectorCopyAdmission::new(accountant.vector_budget()?);
        let partition =
            VectorPartitionRef::new(catalog.index.clone(), catalog.partition_key.clone());
        let Ok(layout) = contextdb_vector::scoped_index_layout(
            vector,
            &catalog.index,
            &mut maintenance_vector_copy_owner,
        ) else {
            continue;
        };
        if vector.partition_is_historical_only(&catalog.index, &catalog.partition_key) {
            // Historical raw values remain exact-readable, but a retired
            // partition no longer owns an indexed route.
            continue;
        }
        let Some(info) = vector.try_partition_info(
            &catalog.index,
            &catalog.partition_key,
            &mut maintenance_vector_copy_owner,
        )?
        else {
            continue;
        };
        let live_rows = info.live_rows;
        if catalog.base.generation_id == 0
            || catalog.base.resident_bytes == 0
            || catalog.base.durable_bytes == 0
            || catalog.base.covered_tx.0 > catalog.published_tx.0
            || catalog.base.covered_lsn.0 > catalog.published_lsn.0
        {
            vector.quarantine_partition_route(&partition, VectorRouteQuarantineReason::CorruptBase);
            continue;
        }
        if catalog.change.is_some_and(|change| {
            change.generation_id == 0
                || change.resident_bytes == 0
                || change.durable_bytes == 0
                || change.covered_tx.0 < catalog.base.covered_tx.0
                || change.covered_lsn.0 < catalog.base.covered_lsn.0
                || change.covered_tx.0 > catalog.published_tx.0
                || change.covered_lsn.0 > catalog.published_lsn.0
        }) {
            vector.quarantine_partition_route(
                &partition,
                VectorRouteQuarantineReason::CorruptChanges,
            );
            continue;
        }

        // Reopen is also a retry owner for T2. Only a validated catalog may
        // witness covered deletion; quarantined bytes never authorize it.
        // Opening takes one bounded step of it; maintenance finishes the rest.
        // A cleanup that cannot fit its memory is left to maintenance rather
        // than failing the open: leftover covered records sit below the
        // boundary every loader replays from.
        if let Some(persistence) = journal_truncation_owner {
            let step = accountant
                .vector_budget()
                .and_then(|budget| {
                    vector.reserve_workspace_static(
                        budget,
                        0,
                        "truncate_vector_partition_journal",
                        "Raise MEMORY_LIMIT with SET MEMORY_LIMIT <SIZE> (or reopen with --memory-limit <SIZE>) before cleaning this maintained route.",
                    )
                })
                .and_then(|mut cleanup_workspace| {
                    persistence
                        .truncate_vector_partition_journal_open_step(&catalog, &mut cleanup_workspace)
                });
            match step {
                Ok(_) | Err(Error::MemoryBudgetExceeded { .. }) => {}
                Err(Error::StoreCorrupted { .. }) => {
                    vector.quarantine_partition_route(
                        &partition,
                        VectorRouteQuarantineReason::CorruptChanges,
                    );
                    continue;
                }
                Err(error) => return Err(error),
            }
        }

        let base_resident_bytes = DurableVectorGenerationLoader::descriptor_usize(
            catalog.base.resident_bytes,
            "base resident byte declaration",
        )?;
        let base_durable_bytes = DurableVectorGenerationLoader::descriptor_usize(
            catalog.base.durable_bytes,
            "base durable byte declaration",
        )?;
        let base_load_bytes = if source.is_committed_image() {
            1
        } else {
            DurableVectorGenerationLoader::checked_add(
                base_resident_bytes,
                base_durable_bytes,
                "base graph load admission",
            )?
        };
        let base_identity = durable_vector_generation_identity(catalog.base, &layout, live_rows)?;
        let base_descriptor = DormantVectorGraphGeneration {
            identity: base_identity,
            resident_bytes: if source.is_committed_image() {
                1
            } else {
                base_resident_bytes
            },
            load_bytes: base_load_bytes,
            // A committed-image generation is served PAGED: it keeps topology
            // and a serving cache and never holds its vectors. Its load is
            // admitted at what it reads from the saved topology; this
            // catalog-only estimate of that lets a statement keep room for
            // the loads it has not reached yet.
            reclaim_bytes: if source.is_committed_image() {
                committed_layer_least_load(
                    live_rows,
                    catalog.base.durable_bytes,
                    layout.dimension,
                    layout.quantization,
                    base_identity.hnsw_m as usize,
                )
            } else {
                base_load_bytes
            },
        };
        let base_loader: Arc<dyn DormantVectorGraphLoader> =
            Arc::new(DurableVectorGenerationLoader {
                source: source.clone(),
                accountant: accountant.clone(),
                partition: partition.clone(),
                descriptor: catalog.base,
                base_generation_id: catalog.base.generation_id,
                is_base: true,
                base_covered: (catalog.base.covered_tx, catalog.base.covered_lsn),
                dimension: layout.dimension,
                quantization: layout.quantization,
                layout: layout.clone(),
                live_rows,
                declared_serving_cache: declared_serving_cache.clone(),
                // Captured serving owns journal replay at its selected root. The
                // shared retained loader installs only this sealed generation.
                tail_covered_lsn: None,
            });
        vector.register_dormant_partition_generation(
            &partition,
            true,
            base_descriptor,
            base_loader,
        )?;

        if let Some(change) = catalog.change {
            let change_resident_bytes = DurableVectorGenerationLoader::descriptor_usize(
                change.resident_bytes,
                "change resident byte declaration",
            )?;
            let change_durable_bytes = DurableVectorGenerationLoader::descriptor_usize(
                change.durable_bytes,
                "change durable byte declaration",
            )?;
            let change_load_bytes = if source.is_committed_image() {
                1
            } else {
                DurableVectorGenerationLoader::checked_add(
                    change_resident_bytes,
                    change_durable_bytes,
                    "change graph load admission",
                )?
            };
            let change_identity = durable_vector_generation_identity(change, &layout, live_rows)?;
            let change_descriptor = DormantVectorGraphGeneration {
                identity: change_identity,
                resident_bytes: if source.is_committed_image() {
                    1
                } else {
                    change_resident_bytes
                },
                load_bytes: change_load_bytes,
                reclaim_bytes: if source.is_committed_image() {
                    committed_layer_least_load(
                        live_rows,
                        change.durable_bytes,
                        layout.dimension,
                        layout.quantization,
                        change_identity.hnsw_m as usize,
                    )
                } else {
                    change_load_bytes
                },
            };
            let change_loader: Arc<dyn DormantVectorGraphLoader> =
                Arc::new(DurableVectorGenerationLoader {
                    source: source.clone(),
                    accountant: accountant.clone(),
                    partition: partition.clone(),
                    descriptor: change,
                    base_generation_id: catalog.base.generation_id,
                    is_base: false,
                    base_covered: (catalog.base.covered_tx, catalog.base.covered_lsn),
                    dimension: layout.dimension,
                    quantization: layout.quantization,
                    layout: layout.clone(),
                    live_rows,
                    declared_serving_cache: declared_serving_cache.clone(),
                    tail_covered_lsn: None,
                });
            vector.register_dormant_partition_generation(
                &partition,
                false,
                change_descriptor,
                change_loader,
            )?;
        }
    }
    Ok(())
}
