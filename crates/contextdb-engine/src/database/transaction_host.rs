use super::*;

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn crash_at_purge_publication_boundary_for_test(boundary: &str) {
    if std::env::var("CONTEXTDB_PURGE_PUBLICATION_CRASH_AT").as_deref() == Ok(boundary) {
        // This process must disappear without destructors: dropping the
        // prepared guard would deliberately unlock but leave pending state,
        // while ordinary Rust unwinding could run unrelated cleanup that a
        // real process loss does not.
        unsafe { libc::_exit(86) }
    }
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn arm_vector_memory_fault(armed: &AtomicBool, consumed: &AtomicBool) {
    consumed.store(false, Ordering::SeqCst);
    armed.store(true, Ordering::SeqCst);
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn take_vector_memory_fault(armed: &AtomicBool, consumed: &AtomicBool) -> bool {
    if armed.swap(false, Ordering::SeqCst) {
        consumed.store(true, Ordering::SeqCst);
        true
    } else {
        false
    }
}

impl ReceivedDdlArrivalValidation {
    pub(super) fn is_received(&self, key: &(Lsn, u32)) -> bool {
        match self {
            Self::Marked(marked) => marked.contains(key),
        }
    }
}

impl DdlProvenanceSource {
    pub(super) fn capture(kind: DdlProvenanceKind, changes: &ChangeSet) -> Self {
        let mut next_ordinal = HashMap::<Lsn, u32>::new();
        let entries = changes
            .ddl
            .iter()
            .zip(changes.ddl_lsn.iter())
            .map(|(ddl, source_lsn)| {
                let ordinal = next_ordinal.entry(*source_lsn).or_default();
                let entry = SourceDdlIdentity {
                    ddl: ddl.clone(),
                    source_lsn: *source_lsn,
                    ordinal: *ordinal,
                };
                *ordinal = ordinal.saturating_add(1);
                entry
            })
            .collect();
        Self {
            kind,
            entries: Arc::new(entries),
        }
    }

    pub(crate) fn historical(changes: &ChangeSet) -> Self {
        Self::capture(DdlProvenanceKind::Historical, changes)
    }

    pub(super) fn synthetic_snapshot(changes: &ChangeSet) -> Self {
        Self::capture(DdlProvenanceKind::SyntheticSnapshot, changes)
    }

    pub(crate) fn is_synthetic_snapshot(&self) -> bool {
        self.kind == DdlProvenanceKind::SyntheticSnapshot
    }

    /// Match each surviving DDL occurrence back to the immutable occurrence
    /// captured before direction, destination, dependency, or size filtering.
    /// Equality alone is insufficient for duplicate DDL at one local LSN: the
    /// first unused occurrence owns its ordinal.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn surviving_entries<'a>(
        &'a self,
        changes: &ChangeSet,
    ) -> Result<Vec<&'a SourceDdlIdentity>> {
        if changes.ddl.len() != changes.ddl_lsn.len() {
            return Err(Error::SyncError(
                ("outbound DDL has mismatched ddl_lsn cardinality".to_string()).into(),
            ));
        }
        let mut used = HashSet::new();
        changes
            .ddl
            .iter()
            .zip(changes.ddl_lsn.iter())
            .map(|(ddl, lsn)| {
                self.entries
                    .iter()
                    .enumerate()
                    .find_map(|(index, entry)| {
                        (!used.contains(&index) && entry.source_lsn == *lsn && entry.ddl == *ddl)
                            .then_some(index)
                    })
                    .map(|index| {
                        used.insert(index);
                        &self.entries[index]
                    })
                    .ok_or_else(|| {
                        Error::SyncError((format!(
                            "outbound DDL at source LSN {} is not part of its captured extraction",
                            lsn.0
                        )).into())
                    })
            })
            .collect()
    }
}
impl AuthoredRowShape {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare(
        meta: &TableMeta,
        owner: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Arc<Self>> {
        let owner: contextdb_core::read_memory::ReadMemoryOwner = owner.into();

        let credit = contextdb_core::read_memory::ReadCredit::try_new(
            owner,
            meta.estimated_bytes()
                .saturating_mul(2)
                .saturating_add(std::mem::size_of::<Self>())
                .saturating_add(2 * std::mem::size_of::<usize>()),
            "sync",
            "connected_authored_shape",
            "Raise the declared reader memory limit to retain authored history.",
        )?;
        Ok(Arc::new(Self {
            meta: meta.clone(),
            _credit: credit,
        }))
    }
}
impl std::ops::Deref for AuthoredRowShape {
    type Target = TableMeta;
    fn deref(&self) -> &Self::Target {
        &self.meta
    }
}

impl ReceivedSchemaStageRegistration {
    pub(super) fn cleanup_arriving(&mut self) {
        if self.arriving_metadata_installed && !self.arriving_metadata_cleaned {
            self.arriving_metadata_cleaned = true;
            self.arriving_table_meta.lock().remove(&self.lsn);
        }
        while self.next_arriving_index_cleanup < self.arriving_vector_indexes.len() {
            let position = self.next_arriving_index_cleanup;
            self.next_arriving_index_cleanup += 1;
            let receipt = self.vector_store.teardown_index_without_copy(
                &self.arriving_vector_indexes[position],
                self.accountant.as_ref(),
            );
            self.accountant.release(receipt.legacy_live_raw_bytes);
        }
    }

    /// Ownership passes to the caller after all temporary preparation effects
    /// have been removed. An unwind keeps this registration armed.
    pub(super) fn take(&mut self) -> Option<ReceivedSchemaStage> {
        self.cleanup_arriving();
        let stage = self.registry.lock().remove(&self.lsn);
        drop(self.occupancy.take());
        self.armed = false;
        stage
    }

    pub(super) fn leave_registered(mut self) {
        self.armed = false;
    }
}
impl ReceivedRegistrationCleanup<'_> {
    pub(super) fn finish(&mut self) {
        self.0.cleanup_arriving();
        // Occupancy cancellation and visible-stage removal precede the
        // payload destructor. Clear armed first so no removal is repeated.
        let stage = if self.0.published {
            self.0.registry.lock().remove(&self.0.lsn)
        } else {
            None
        };
        drop(self.0.occupancy.take());
        self.0.armed = false;
        drop(stage);
    }
}
impl Drop for ReceivedRegistrationCleanup<'_> {
    fn drop(&mut self) {
        if self.0.armed {
            self.finish();
        }
    }
}
impl Drop for ReceivedSchemaStageRegistration {
    fn drop(&mut self) {
        if self.armed {
            let mut cleanup = ReceivedRegistrationCleanup(self);
            cleanup.finish();
        }
    }
}

#[cfg(any(test, feature = "test-seams"))]
impl ReceivedSchemaApplyObserverForTest {
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot actual storage observation even when a receipted retry skips schema apply.
    #[cfg(any(test, feature = "test-seams"))]
    pub fn storage_observation(&self) -> ReceivedSchemaApplyObservationForTest {
        self.inner.lock().observation.clone()
    }

    pub fn observation(&self) -> Option<ReceivedSchemaApplyObservationForTest> {
        let state = self.inner.lock();
        (state.traces_started != 0).then(|| state.observation.clone())
    }
}

#[cfg(any(test, feature = "test-seams"))]
impl Drop for ReceivedSchemaApplyObservationScopeForTest {
    fn drop(&mut self) {
        let mut slot = self.slot.lock();
        if slot
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &self.installed))
        {
            *slot = self.previous.take();
        }
    }
}

#[cfg(any(test, feature = "test-seams"))]
impl Drop for ReceivedSchemaApplyThreadObservationScopeForTest {
    fn drop(&mut self) {
        RECEIVED_SCHEMA_APPLY_OBSERVER.with(|slot| {
            *slot.borrow_mut() = self.previous.take();
        });
    }
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn begin_received_schema_apply_observation_for_test(
    observer: Option<SharedReceivedSchemaApplyObservationForTest>,
) -> Option<ReceivedSchemaApplyThreadObservationScopeForTest> {
    let observer = observer?;
    observer.lock().traces_started += 1;
    let previous = RECEIVED_SCHEMA_APPLY_OBSERVER.with(|slot| slot.borrow_mut().replace(observer));
    Some(ReceivedSchemaApplyThreadObservationScopeForTest { previous })
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn received_schema_apply_observation_is_active_for_test() -> bool {
    RECEIVED_SCHEMA_APPLY_OBSERVER.with(|slot| slot.borrow().is_some())
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn with_received_schema_apply_observation_for_test(
    update: impl FnOnce(&mut ReceivedSchemaApplyObservationForTest),
) {
    RECEIVED_SCHEMA_APPLY_OBSERVER.with(|slot| {
        let observer = slot.borrow().clone();
        if let Some(observer) = observer {
            update(&mut observer.lock().observation);
        }
    });
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn record_received_schema_profile_nanos_for_test(name: &str, nanos: u128) {
    with_received_schema_apply_observation_for_test(|observation| {
        *observation
            .profile_nanos
            .entry(name.to_string())
            .or_default() += nanos;
    });
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn record_received_schema_profile_work_for_test(name: &str, count: u64) {
    with_received_schema_apply_observation_for_test(|observation| {
        *observation
            .profile_work
            .entry(name.to_string())
            .or_default() += count;
    });
}

#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn record_received_schema_persistence_profile_for_test(
    name: &str,
    started: std::time::Instant,
) {
    record_received_schema_profile_nanos_for_test(name, started.elapsed().as_nanos());
}

#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn record_received_schema_persistence_work_for_test(name: &str, count: u64) {
    record_received_schema_profile_work_for_test(name, count);
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn record_received_schema_staged_table_image_for_test(table: &str) {
    with_received_schema_apply_observation_for_test(|observation| {
        observation.staged_table_images_copied += 1;
        observation.staged_table_names.insert(table.to_string());
    });
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn record_received_schema_staged_row_for_test() {
    with_received_schema_apply_observation_for_test(|observation| {
        observation.staged_rows_copied += 1;
    });
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn record_received_schema_staged_graph_edge_for_test() {
    with_received_schema_apply_observation_for_test(|observation| {
        observation.staged_graph_edges_copied += 1;
    });
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn record_received_schema_staged_vector_for_test() {
    with_received_schema_apply_observation_for_test(|observation| {
        observation.staged_vectors_copied += 1;
    });
}

#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn record_received_schema_durable_table_rewrite_for_test(table: &str) {
    with_received_schema_apply_observation_for_test(|observation| {
        observation.durable_table_rewrites += 1;
        observation.durable_table_names.insert(table.to_string());
    });
}

#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn record_received_schema_durable_row_rewrite_for_test() {
    with_received_schema_apply_observation_for_test(|observation| {
        observation.durable_rows_rewritten += 1;
    });
}

impl<V> CommitStageRegistration<V> {
    pub(crate) fn new(registry: &Arc<Mutex<HashMap<Lsn, V>>>, lsn: Lsn) -> Self {
        Self {
            registry: registry.clone(),
            lsn,
            armed: true,
        }
    }

    /// Ownership has passed to the commit: the entry was (or is about to be)
    /// consumed by the durable apply or by publication, which now answers for
    /// what it holds.
    pub(crate) fn ownership_transferred(&mut self) {
        self.armed = false;
    }
}

impl<V> Drop for CommitStageRegistration<V> {
    fn drop(&mut self) {
        if self.armed {
            let removed = self.registry.lock().remove(&self.lsn);
            drop(removed);
        }
    }
}

impl PublicationInterruptionWatch {
    pub(super) fn arm(interrupted: &Arc<AtomicBool>) -> Self {
        Self {
            interrupted: interrupted.clone(),
            armed: true,
        }
    }

    /// Publication returned, or the commit failed with an ordinary `Err`
    /// before anything was published: the handle keeps serving.
    pub(super) fn completed(mut self) {
        self.armed = false;
    }
}

/// Physical relocation changes the durable root while preserving its logical
/// pair. The batch holds only its own bounded storage write transaction, so a
/// foreground commit prepares under the commit boundary and queues at the
/// storage writer rather than behind the whole batch. The relocated root is
/// then published under the commit boundary, retiring the writer's own
/// predecessor before another batch begins. A commit that lands between the
/// batch and that publication already published a root that contains it.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn publish_storage_compaction_batch(
    manager: &DatabaseTransactionHost,
    persistence: &Arc<RedbPersistence>,
    accountant: &crate::memory_accounting::AccountantHandle,
    interrupted: &Arc<AtomicBool>,
    fragmentation: Option<f64>,
) -> Result<(redb::CompactionProgress, Option<u64>)> {
    publish_storage_compaction_batch_with_deferral(
        manager,
        persistence,
        accountant,
        interrupted,
        fragmentation,
        false,
    )?
    .ok_or_else(|| Error::ReaderCoordinationUnavailable {
        operation: "storage reclamation".to_owned(),
        path: persistence.path().display().to_string(),
        reason: "reader publication is contended".to_owned(),
    })
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn publish_storage_compaction_batch_with_deferral(
    manager: &DatabaseTransactionHost,
    persistence: &Arc<RedbPersistence>,
    accountant: &crate::memory_accounting::AccountantHandle,
    interrupted: &Arc<AtomicBool>,
    fragmentation: Option<f64>,
    defer_busy: bool,
) -> Result<Option<(redb::CompactionProgress, Option<u64>)>> {
    #[cfg(any(test, feature = "test-seams"))]
    if let Some(remaining) = STORAGE_COMPACTION_BATCHES_BEFORE_REFUSAL.with(std::cell::Cell::get) {
        if remaining == 0 {
            STORAGE_COMPACTION_BATCHES_BEFORE_REFUSAL.with(|slot| slot.set(None));
            return Err(Error::Other(
                ("storage compaction batch refused by the test seam".to_string()).into(),
            ));
        }
        STORAGE_COMPACTION_BATCHES_BEFORE_REFUSAL.with(|slot| slot.set(Some(remaining - 1)));
    }
    let watch = PublicationInterruptionWatch::arm(interrupted);
    let progress = match persistence.compact_incremental_with_deferral(
        accountant.clone(),
        fragmentation,
        defer_busy,
    ) {
        Ok(Some(progress)) => progress,
        Ok(None) => {
            watch.completed();
            return Ok(None);
        }
        Err(error) => {
            // Page admission and reader-presence refusal happen before
            // the relocation transaction commits.
            // Other storage errors may have crossed its durable boundary.
            if matches!(
                error,
                Error::MemoryBudgetExceeded { .. } | Error::ReaderCoordinationUnavailable { .. }
            ) {
                watch.completed();
            }
            return Err(error);
        }
    };
    // Relocation preserves every logical row, so the previously published
    // root stays a correct view of the same pair; a refused successor
    // admission leaves it serving and is reported, never an interruption.
    watch.completed();
    manager.with_maintenance_publication(|publication| {
        publication.reserve()?;
        publication.complete();
        Ok(())
    })?;
    Ok(Some(progress))
}

impl Drop for PublicationInterruptionWatch {
    fn drop(&mut self) {
        if self.armed {
            self.interrupted.store(true, Ordering::SeqCst);
        }
    }
}

/// Panic here if a test armed this point, and disarm it. One arming fires
/// once.
#[cfg(test)]
pub(super) fn take_local_commit_stage_fault_for_test(point: LocalCommitStageFaultPoint) {
    let armed = LOCAL_COMMIT_STAGE_FAULT.with(|fault| {
        if fault.get() == Some(point) {
            fault.set(None);
            true
        } else {
            false
        }
    });
    if armed {
        panic!("injected unwind after a local commit stage registration: {point:?}");
    }
}

impl PullPolicyPreflight {
    pub(crate) fn restore_for_apply(self, mut changes: ChangeSet) -> ChangeSet {
        if !self.original_ddl.is_empty() {
            changes.ddl = self.original_ddl;
            changes.ddl_lsn = self.original_ddl_lsn;
            changes.rows.extend(self.held_data.rows);
            changes.vectors.extend(self.held_data.vectors);
            changes.edges.extend(self.held_data.edges);
        }
        changes
    }
}

/// Fire the armed fault if this is the point it was armed at, and disarm it.
/// One arming fires once, so a test that arms it cannot leak the fault into
/// the next apply.
#[cfg(any(test, feature = "test-seams"))]
pub(super) fn take_received_unit_fault_for_test(point: ReceivedUnitFaultPoint) -> Result<()> {
    let armed = RECEIVED_UNIT_FAULT.with(|fault| match fault.get() {
        Some((armed_point, kind)) if armed_point == point => {
            fault.set(None);
            Some(kind)
        }
        _ => None,
    });
    match armed {
        None => Ok(()),
        Some(ReceivedUnitCopyFault::Error) => Err(Error::SyncError(
            ("injected fault while taking delivery of a received unit".to_string()).into(),
        )),
        Some(ReceivedUnitCopyFault::Unwind) => {
            panic!("injected unwind while taking delivery of a received unit")
        }
        Some(ReceivedUnitCopyFault::Abort) => std::process::abort(),
    }
}

/// Fire the armed fault at this point if it is a caught unwind, and disarm
/// it. Used at post-durable-commit boundaries, which do no fallible work and
/// so have no `Result` to carry an injected `Error` case through -- past the
/// durable commit an `Err` here would not be a real outcome this engine ever
/// produces, only a caught panic is.
#[cfg(any(test, feature = "test-seams"))]
pub(super) fn take_received_unit_unwind_fault_for_test(point: ReceivedUnitFaultPoint) {
    let armed = RECEIVED_UNIT_FAULT.with(|fault| match fault.get() {
        Some((armed_point, kind)) if armed_point == point => {
            fault.set(None);
            Some(kind)
        }
        _ => None,
    });
    match armed {
        Some(ReceivedUnitCopyFault::Unwind) => {
            panic!("injected unwind while taking delivery of a received unit");
        }
        Some(ReceivedUnitCopyFault::Abort) => std::process::abort(),
        Some(ReceivedUnitCopyFault::Error) | None => {}
    }
}

impl TemporaryMemoryReservation {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reserve(
        accountant: crate::memory_accounting::AccountantHandle,
        bytes: usize,
        operation: &'static str,
    ) -> Result<Self> {
        accountant.try_allocate_for(
            bytes,
            "ddl",
            operation,
            "Reduce the schema transaction size or raise MEMORY_LIMIT before retrying.",
        )?;
        Ok(Self {
            original: None,
            _writer_use: crate::memory_accounting::WriterUse::retain_if_writer(&(accountant)),
            accountant,
            bytes,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reserve_for_database(
        database: &Database,
        bytes: usize,
        operation: &'static str,
    ) -> Result<Self> {
        if matches!(database.tx_mgr, DatabaseTransactionHost::Owned(_)) {
            let original = database.reserve_original_preparation_for(
                bytes,
                "ddl",
                operation,
                "Reduce the schema transaction size or raise MEMORY_LIMIT before retrying.",
            )?;
            Ok(Self {
                original: Some(original),
                accountant: crate::memory_accounting::AccountantHandle::clone(
                    &database.finalization.accountant,
                ),
                bytes: 0,
                _writer_use: None,
            })
        } else {
            Self::reserve(
                crate::memory_accounting::AccountantHandle::clone(
                    &database.finalization.accountant,
                ),
                bytes,
                operation,
            )
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn absorb(&mut self, mut other: Self) -> Result<()> {
        match (&mut self.original, &mut other.original) {
            (Some(left), Some(right)) => {
                return crate::memory_accounting::merge_preparation_original(left, right);
            }
            (None, None) => (),
            _ => return Err(EngineMetadataJournal::identity_error()),
        }
        debug_assert!(std::ptr::eq(
            self.accountant.as_ptr(),
            other.accountant.as_ptr()
        ));
        self.bytes = self.bytes.saturating_add(other.bytes);
        other.bytes = 0;
        Ok(())
    }
}

impl Drop for TemporaryMemoryReservation {
    fn drop(&mut self) {
        self.accountant.release(self.bytes);
    }
}

impl StagedCopyCharge {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn admit(
        accountant: &crate::memory_accounting::AccountantHandle,
        vector_accountant: contextdb_vector::MemoryBudgetOwner,
        bytes: usize,
        operation: &'static str,
    ) -> Result<Self> {
        use std::mem::ManuallyDrop;
        let writer_use = crate::memory_accounting::WriterUse::retain_if_writer(accountant);
        accountant.try_allocate_for(bytes, "sync", operation, STAGE_RECEIVED_UNIT_COPY_HINT)?;
        Ok(Self {
            vector_accountant: ManuallyDrop::new(vector_accountant),
            accountant: ManuallyDrop::new(accountant.clone()),
            bytes,
            _writer_use: ManuallyDrop::new(writer_use),
            own_control: None,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn admit_for_host(
        host: &DatabaseTransactionHost,
        accountant: &crate::memory_accounting::AccountantHandle,
        vector_accountant: contextdb_vector::MemoryBudgetOwner,
        bytes: usize,
        operation: &'static str,
    ) -> Result<Self> {
        match host {
            DatabaseTransactionHost::Legacy(_) => {
                Self::admit(accountant, vector_accountant, bytes, operation)
            }
            DatabaseTransactionHost::Owned(handle) => handle.with_host(|host| {
                host.events.with_owned_persistence_original(|original| {
                    crate::memory_accounting::validate_owned_original(
                        original,
                        accountant,
                        &host.funder,
                    )?;
                    let admission = crate::memory_accounting::reserve_preparation_from_original(
                        original,
                        bytes,
                        "sync",
                        operation,
                        STAGE_RECEIVED_UNIT_COPY_HINT,
                    )?;
                    let (accountant, writer, bytes) =
                        crate::memory_accounting::transfer_preparation_to_retained(admission)?;
                    Ok(Self {
                        vector_accountant: std::mem::ManuallyDrop::new(vector_accountant),
                        accountant: std::mem::ManuallyDrop::new(accountant),
                        bytes,
                        _writer_use: std::mem::ManuallyDrop::new(Some(writer)),
                        own_control: None,
                    })
                })
            }),
        }
    }

    /// Move an already admitted private pending body out of its write set.
    /// The caller removes that exact body before constructing this invoice;
    /// its replacement cannot release/re-admit these still-live bytes.
    pub(super) fn take_admitted(
        accountant: &crate::memory_accounting::AccountantHandle,
        vector_accountant: contextdb_vector::MemoryBudgetOwner,
        bytes: usize,
    ) -> Self {
        use std::mem::ManuallyDrop;
        Self {
            vector_accountant: ManuallyDrop::new(vector_accountant),
            accountant: ManuallyDrop::new(accountant.clone()),
            bytes,
            _writer_use: ManuallyDrop::new(crate::memory_accounting::WriterUse::retain_if_writer(
                accountant,
            )),
            own_control: None,
        }
    }

    /// Admit the sole outer Box at the existing fallible prepaid handoff.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_prepaid_box(mut self) -> Result<Self> {
        assert!(
            self.own_control.is_none(),
            "one staged copy owns one prepaid Box control"
        );
        let layout = std::alloc::Layout::for_value(&self);
        self.own_control = Some(crate::memory_accounting::ControlCredit::admit(
            &self.accountant,
            layout.size(),
            "staged_copy_prepaid_box_control",
        )?);
        Ok(self)
    }

    /// The owner that releases these bytes now holds them.
    pub(super) fn transfer(mut self) {
        self.bytes = 0;
    }
}

impl contextdb_vector::PrepaidVectorCopyCharge for StagedCopyCharge {
    fn accountant(&self) -> contextdb_vector::MemoryBudgetOwner {
        contextdb_vector::MemoryBudgetOwner::clone(&self.vector_accountant)
    }

    fn retained_bytes(&self) -> usize {
        self.bytes
    }

    fn transfer(mut self: Box<Self>) {
        // The destination owns the payload charge. Keep the original Box's
        // control refund outside its payload destructor until Box deallocation
        // completes, including the Box drop glue's single-unwind cleanup.
        self.bytes = 0;
        let refund = self
            .own_control
            .take()
            .map(crate::memory_accounting::ControlCredit::prepare);
        drop(self);
        drop(refund);
    }
}

impl StagedCopyChargeCleanup {
    pub(super) fn finish(&mut self) {
        use std::mem::ManuallyDrop;
        while self.next < 5 {
            let step = self.next;
            self.next += 1;
            // All fields remain initialized until their one advancing step.
            // The stack guard resumes cleanup if one child destructor unwinds.
            unsafe {
                match step {
                    0 => (*self.source)
                        .accountant
                        .release(std::mem::take(&mut (*self.source).bytes)),
                    1 => ManuallyDrop::drop(&mut (*self.source).vector_accountant),
                    2 => ManuallyDrop::drop(&mut (*self.source).accountant),
                    3 => ManuallyDrop::drop(&mut (*self.source)._writer_use),
                    4 => drop(self.refund.take()),
                    _ => unreachable!(),
                }
            }
        }
    }
}

impl Drop for StagedCopyChargeCleanup {
    fn drop(&mut self) {
        self.finish();
    }
}

impl Drop for StagedCopyCharge {
    fn drop(&mut self) {
        let refund = self
            .own_control
            .take()
            .map(crate::memory_accounting::ControlCredit::prepare);
        let mut cleanup = StagedCopyChargeCleanup {
            source: self,
            next: 0,
            refund,
        };
        cleanup.finish();
    }
}

impl ReceivedStagingAccess {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare(source: &Database, staged_tables: &HashSet<String>) -> Result<Arc<Self>> {
        use std::mem::ManuallyDrop;
        // Preserve the historical bookkeeping allowance; it establishes no
        // particular Arc allocator layout and is separate from own control.
        let bytes = staged_tables
            .iter()
            .map(|table| table.len() + 128)
            .sum::<usize>()
            + 256;
        let charge = StagedCopyCharge::admit(
            &source.accountant,
            contextdb_vector::MemoryBudgetOwner::clone(&source.finalization.vector_accountant),
            bytes,
            "stage_received_table_accesses",
        )?;
        // Keep ordinary locals until all fallible/panicking preparation is
        // complete, so partial construction never hides an active child.
        let relational = source.relational_store.clone();
        let staged_tables = staged_tables.clone();
        let writer_use = crate::memory_accounting::WriterUse::retain_if_writer(&source.accountant);
        let mut value = Self {
            source: ManuallyDrop::new(relational),
            staged_tables: ManuallyDrop::new(staged_tables),
            accountant: ManuallyDrop::new(source.accountant.clone()),
            miss: ManuallyDrop::new(Mutex::new(None)),
            _charge: ManuallyDrop::new(charge),
            _writer_use: ManuallyDrop::new(writer_use),
            vector_accountant: ManuallyDrop::new(contextdb_vector::MemoryBudgetOwner::clone(
                &source.finalization.vector_accountant,
            )),
            own_control: None,
        };
        let layout = crate::memory_accounting::writer_adapter_layout(&value);
        value.own_control = Some(crate::memory_accounting::ControlCredit::admit(
            &source.accountant,
            layout.size(),
            "received_staging_access_control",
        )?);
        Ok(Arc::new(value))
    }

    pub(super) fn record(&self, table: &str) {
        if self.staged_tables.contains(table) || !self.source.table_meta.read().contains_key(table)
        {
            return;
        }
        let mut miss = self.miss.lock();
        if miss.is_some() {
            return;
        }
        *miss = Some(
            match StagedCopyCharge::admit(
                &self.accountant,
                contextdb_vector::MemoryBudgetOwner::clone(&self.vector_accountant),
                table.len() + 128,
                "stage_received_table_miss",
            ) {
                Ok(charge) => ReceivedStagingMiss::Table {
                    name: table.to_string(),
                    _charge: charge,
                },
                Err(error) => ReceivedStagingMiss::Refused(error),
            },
        );
    }
}

impl ReceivedStagingAccessCleanup {
    pub(super) fn finish(&mut self) {
        use std::mem::ManuallyDrop;
        while self.next < 8 {
            let step = self.next;
            self.next += 1;
            // Miss strings precede their inline charge in the enum's drop;
            // all table/source payloads end before the access payload refund.
            // The independent source use spans every nested child teardown.
            unsafe {
                match step {
                    0 => ManuallyDrop::drop(&mut (*self.source).miss),
                    1 => ManuallyDrop::drop(&mut (*self.source).staged_tables),
                    2 => ManuallyDrop::drop(&mut (*self.source).source),
                    3 => ManuallyDrop::drop(&mut (*self.source)._charge),
                    4 => ManuallyDrop::drop(&mut (*self.source).vector_accountant),
                    5 => ManuallyDrop::drop(&mut (*self.source).accountant),
                    6 => ManuallyDrop::drop(&mut (*self.source)._writer_use),
                    7 => drop(self.refund.take()),
                    _ => unreachable!(),
                }
            }
        }
    }
}

impl Drop for ReceivedStagingAccessCleanup {
    fn drop(&mut self) {
        self.finish();
    }
}

impl Drop for ReceivedStagingAccess {
    fn drop(&mut self) {
        let refund = self
            .own_control
            .take()
            .map(crate::memory_accounting::ControlCredit::prepare);
        let mut cleanup = ReceivedStagingAccessCleanup {
            source: self,
            next: 0,
            refund,
        };
        cleanup.finish();
    }
}

impl PreparedMemorySwap {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn from_original_preparation(
        original: &mut OwnedMemoryReservation,
        old_bytes: usize,
        new_bytes: usize,
        retired_bytes_released_by_store: usize,
    ) -> Result<Self> {
        if retired_bytes_released_by_store > old_bytes {
            return Err(EngineMetadataJournal::identity_error());
        }
        let grant = crate::memory_accounting::detach_preparation_original(original, new_bytes)?;
        let (accountant, writer, bytes) =
            crate::memory_accounting::transfer_preparation_to_retained(grant)?;
        Ok(Self {
            history: None,
            accountant,
            reserved_new_image: bytes,
            release_after_swap: old_bytes - retired_bytes_released_by_store,
            staged_admissions: None,
            _writer_use: Some(writer),
        })
    }

    /// Pure accounting for a replacement that is not yet authorized to
    /// reserve allocator capacity.  Authoritative-purge preparation carries
    /// this alongside opaque store publications; the later commit kernel is
    /// the only layer allowed to turn it into a live reservation.
    pub(super) fn accounting(
        old_bytes: usize,
        new_bytes: usize,
        retired_bytes_released_by_store: usize,
    ) -> PreparedMemorySwapAccounting {
        PreparedMemorySwapAccounting {
            old_bytes,
            new_bytes,
            retired_bytes_released_by_store,
        }
    }

    /// `retired_bytes_released_by_store` is the HNSW allocation owned by the
    /// outgoing vector registry.  The registry tears it down after the swap,
    /// so the reservation covers the part of the old image that remains
    /// charged until this guard settles the relational/graph/vector payload
    /// delta.  Keeping the full old total at this boundary makes that special
    /// release explicit rather than silently under-counting index memory.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare(
        accountant: crate::memory_accounting::AccountantHandle,
        old_bytes: usize,
        new_bytes: usize,
        retired_bytes_released_by_store: usize,
    ) -> Result<Self> {
        Self::prepare_for(
            accountant,
            old_bytes,
            new_bytes,
            retired_bytes_released_by_store,
            "sync",
            "prepare_received_schema_replacement",
            "Reduce the replicated schema/data working set or raise MEMORY_LIMIT before receiving schema.",
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_for(
        accountant: crate::memory_accounting::AccountantHandle,
        old_bytes: usize,
        new_bytes: usize,
        retired_bytes_released_by_store: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> Result<Self> {
        debug_assert!(retired_bytes_released_by_store <= old_bytes);
        let old_continuing_bytes = old_bytes.saturating_sub(retired_bytes_released_by_store);
        // Both physical images coexist until publication. The outgoing image
        // is not headroom for allocating its replacement, even when the final
        // replacement is smaller. Capture the payer before admitting bytes.
        let writer_use = crate::memory_accounting::WriterUse::retain_if_writer(&accountant);
        let projection = match writer_use.as_ref() {
            Some(original) => {
                crate::memory_accounting::StoreAccountantHandle::from_writer_use(original)
            }
            None => crate::memory_accounting::StoreAccountantHandle::from_original_nonwriter(
                &accountant,
            ),
        };
        projection.try_allocate_for_original(new_bytes, subsystem, operation, hint)?;
        Ok(Self {
            _writer_use: writer_use,
            accountant,
            history: None,
            reserved_new_image: new_bytes,
            release_after_swap: old_continuing_bytes,
            staged_admissions: None,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_schema_generation(
        mut self,
        db: &Database,
        names: impl Iterator<Item = impl AsRef<str>>,
    ) -> Result<Self> {
        if !db.relational_store.uses_disk_backed_committed_bodies() {
            self.history = Some(
                db.relational_store
                    .prepare_schema_generation(names, db.engine_sync_read_memory()?)?,
            );
        }
        Ok(self)
    }

    /// Carry the staged rows this swap re-prices, to be recorded when the
    /// shape they are priced against actually becomes the live one.
    pub(super) fn carrying_staged_admissions(
        mut self,
        repricing: StagedAdmissionRepricing,
    ) -> Self {
        if !repricing.per_tx.is_empty() {
            self.staged_admissions = Some(repricing);
        }
        self
    }

    pub(crate) fn commit_after_swap(&mut self) {
        // Publication has installed the non-vector image whose final owner
        // recomputes and releases this retained delta. Staged row deltas are
        // handed to their write-set admissions below. No self-funded vector
        // directory, payload or graph is included in this reservation.
        self.reserved_new_image = 0;
        self.accountant.release(self.release_after_swap);
        self.release_after_swap = 0;
        if let Some(repricing) = self.staged_admissions.take() {
            repricing.tx_mgr.with_active_write_sets(|active| {
                for (tx, rows) in &repricing.per_tx {
                    let Some(ws) = active.get_mut(tx) else {
                        continue;
                    };
                    for (table, row_id, bytes) in rows {
                        ws.set_relational_insert_admitted_bytes(table.clone(), *row_id, *bytes);
                    }
                }
            });
        }
    }
}

impl Drop for PreparedMemorySwap {
    fn drop(&mut self) {
        self.accountant.release(self.reserved_new_image);
    }
}

/// Build the exact source-order portion of an authenticated received schema
/// stage.  This is deliberately pure: no store lock, plugin, parser, or
/// persistence access is allowed while establishing the durable identity and
/// ordered bytes of the received vector.
#[allow(dead_code)]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn plan_received_schema_source_order(
    local_lsn: Lsn,
    ddl: &[DdlChange],
    ddl_lsn: &[Lsn],
    received: Option<&crate::protocol::ReceivedDdlContext>,
) -> Result<(
    Vec<ReceivedSchemaOrderEntry>,
    ReceivedSchemaPersistencePayload,
)> {
    plan_received_schema_source_order_with_coordinates(local_lsn, ddl, ddl_lsn, received, None)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn plan_received_schema_source_order_with_coordinates(
    local_lsn: Lsn,
    ddl: &[DdlChange],
    ddl_lsn: &[Lsn],
    received: Option<&crate::protocol::ReceivedDdlContext>,
    coordinates: Option<&ConnectedCoordinates>,
) -> Result<(
    Vec<ReceivedSchemaOrderEntry>,
    ReceivedSchemaPersistencePayload,
)> {
    if ddl.len() != ddl_lsn.len()
        || received.is_some_and(|received| ddl.len() != received.entries.len())
    {
        return Err(Error::SyncError(
            ("received schema source-order plan does not cover the exact authenticated DDL vector"
                .to_string())
            .into(),
        ));
    }

    // Whatever door built the context, the author's signed account governs
    // what is staged: each statement installs against the generation its
    // author signed, in the author's order, and every authored transaction
    // is staged whole. This node's declared table policy refuses a whole
    // transaction or none of it, so a proper subset reaching the stage is a
    // defect, never a receiver decision.
    if let Some(received) = received {
        if received.entries.iter().any(|entry| {
            entry
                .author
                .as_ref()
                .is_some_and(|author| author.table_generation != entry.table_generation)
        }) {
            return Err(Error::SyncError(
                ("received schema delivers a table generation its author did not sign".to_string())
                    .into(),
            ));
        }
        crate::protocol::check_authored_units_whole_and_in_order(
            received
                .entries
                .iter()
                .filter_map(|entry| entry.author.as_ref()),
        )
        .map_err(|error| Error::SyncError((error.to_string()).into()))?;
    }
    let mut source_order = Vec::with_capacity(ddl.len());
    let mut generation_values = Vec::with_capacity(ddl.len());
    let mut marker_values = Vec::with_capacity(ddl.len());
    let mut received_arrival_values = Vec::with_capacity(ddl.len());
    let mut ordered_ddl = Vec::with_capacity(ddl.len());
    let mut previous_local = None;
    let mut next_ordinal = 0u32;
    for (position, (change, source_lsn)) in ddl.iter().zip(ddl_lsn.iter()).enumerate() {
        // An unattested unit carries no signed provenance, so there is nothing
        // to check its order against and nothing to be idempotent on: its
        // position in the unit IS its ordinal, it asserts no table generation,
        // and it writes no marker. Author order still holds
        // -- the order is the order the unit arrived in, unchanged.
        let entry = received.map(|received| &received.entries[position]);
        if let Some(entry) = entry
            && *source_lsn != entry.source_ddl_lsn
        {
            return Err(Error::SyncError(
                ("received schema source LSN differs from authenticated DDL provenance"
                    .to_string())
                .into(),
            ));
        }
        let table = Database::ddl_affected_table(change).map(str::to_string);
        if let Some(entry) = entry
            && table != entry.table
        {
            return Err(Error::SyncError(
                ("received schema table differs from authenticated DDL provenance".to_string())
                    .into(),
            ));
        }
        let local_lsn = coordinates
            .map(|map| map.local(*source_lsn))
            .transpose()?
            .unwrap_or(local_lsn);
        if previous_local.is_some_and(|previous| previous > local_lsn) {
            return Err(Error::SyncError(
                "received schema source groups are out of order".into(),
            ));
        }
        if previous_local != Some(local_lsn) {
            next_ordinal = 0;
        }
        let persisted_ordinal = next_ordinal;
        next_ordinal = next_ordinal
            .checked_add(1)
            .ok_or_else(|| Error::SyncError("received schema DDL ordinal overflow".into()))?;
        previous_local = Some(local_lsn);
        let generation = DurableDdlGenerationSidecar {
            table,
            // No author asserted one, and the item-by-item door this replaces
            // never minted one either -- only a locally authored CREATE does
            // (`executor.rs:3617`). Asserting a generation here would invent an
            // identity nothing attests to.
            table_generation: entry.and_then(|entry| entry.table_generation),
        };
        let generation_key =
            Database::durable_ddl_generation_sidecar_key(local_lsn, persisted_ordinal);
        generation_values.push((
            generation_key,
            RedbPersistence::encode_config_value(&generation)?,
        ));
        if let (Some(received), Some(entry)) = (received, entry) {
            let marker_key = Database::durable_received_ddl_marker_key(
                &received.tenant_id,
                &received.source_node_id,
                received.source_incarnation,
                entry.source_ddl_lsn,
                entry.ordinal,
            );
            marker_values.push((
                marker_key,
                RedbPersistence::encode_config_value(&DurableReceivedDdlMarker {
                    digest: entry.digest.clone(),
                })?,
            ));
        }
        let local_arrival = DurableReceivedDdlArrival {
            local_lsn,
            persisted_ordinal,
            digest: Database::canonical_local_ddl_arrival_digest(
                change,
                local_lsn,
                persisted_ordinal,
                &generation,
            )?,
            author: received
                .zip(entry)
                .map(|(received, entry)| received.author_of(entry)),
        };
        received_arrival_values.push((
            Database::durable_received_ddl_arrival_key(local_lsn, persisted_ordinal),
            RedbPersistence::encode_config_value(&local_arrival)?,
        ));
        source_order.push(ReceivedSchemaOrderEntry {
            change: change.clone(),
            source_ddl_lsn: entry.map_or(*source_lsn, |entry| entry.source_ddl_lsn),
            source_ordinal: entry.map_or(persisted_ordinal, |entry| entry.ordinal),
            persisted_ordinal,
            // The AUTHOR's signed digest. An unattested unit has none, and an
            // empty one is the honest answer -- the receiver's own arrival
            // digest above is a different fact and must not stand in for it.
            digest: entry.map(|entry| entry.digest.clone()).unwrap_or_default(),
            generation,
            local_arrival,
        });
        ordered_ddl.push((local_lsn, change.clone()));
    }
    Ok((
        source_order,
        ReceivedSchemaPersistencePayload {
            ddl_log_replacement: ordered_ddl,
            generation_values,
            marker_values,
            received_arrival_values,
            superseded_occurrence_values: Vec::new(),
            lineage_values: Vec::new(),
            accepted_author_values: Vec::new(),
            source_state_values: Vec::new(),
            receipt_values: Vec::new(),
            applied_push_watermark_values: Vec::new(),
            terminal_values: Vec::new(),
            config_values: Vec::new(),
            config_max_u64_keys: Vec::new(),
        },
    ))
}

/// The ordered pending source stores a reference to the canonical lifecycle
/// record. Decode under the caller's admitted scope, without retaining a copy.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn pending_delete_index_position(key: &str, bytes: &[u8]) -> Result<Option<Lsn>> {
    if !key.starts_with(Database::DURABLE_LINEAGE_CONFIG_PREFIX) {
        return Ok(None);
    }
    let record = RedbPersistence::decode_config_value::<DurableLineageRecord>(bytes)?;
    Ok(
        (record.delete_obligation == DurableDeleteObligation::Pending)
            .then_some(Lsn(record.delete_lsn)),
    )
}

impl Database {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn lineage_recovery_floors_for_write(
        key: &str,
        bytes: &[u8],
    ) -> Result<(RowId, Lsn)> {
        if key.starts_with("local_read_erasure_frontier.v1.") {
            return Ok((
                RowId(0),
                RedbPersistence::decode_erasure_frontier_control(bytes)?,
            ));
        }
        if !key.starts_with(Self::DURABLE_LINEAGE_CONFIG_PREFIX)
            && !key.starts_with(Self::DURABLE_PURGED_LINEAGE_HISTORY_PREFIX)
        {
            return Ok((RowId(0), Lsn(0)));
        }
        let mut bounds = DurableLineageRecoveryBounds::default();
        bounds.admit(key, bytes)?;
        Ok((bounds.max_row_id, bounds.purge_frontier_max_lsn))
    }
}

impl Database {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn erasure_frontier_for_write(
        key: &str,
        bytes: &[u8],
    ) -> Result<Option<(String, Lsn)>> {
        // Only lineage records can publish erasure. Other configuration
        // values (generation counters, pending-delete selectors, receipts)
        // have their own codecs and must never be decoded as lineage.
        if !key.starts_with(Self::DURABLE_LINEAGE_CONFIG_PREFIX)
            && !key.starts_with(Self::DURABLE_PURGED_LINEAGE_HISTORY_PREFIX)
        {
            return Ok(None);
        }
        let record = RedbPersistence::decode_config_value::<DurableLineageRecord>(bytes)?;
        let mut bounds = DurableLineageRecoveryBounds::default();
        bounds.admit_record(key, &record)?;
        Ok(
            (record.delete_obligation == DurableDeleteObligation::Purged)
                .then_some((record.table, bounds.purge_frontier_max_lsn)),
        )
    }
}

impl DurableLineageRecoveryBounds {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn admit(&mut self, key: &str, bytes: &[u8]) -> Result<()> {
        let record = RedbPersistence::decode_config_value::<DurableLineageRecord>(bytes)?;
        self.admit_record(key, &record)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn admit_record(&mut self, key: &str, record: &DurableLineageRecord) -> Result<()> {
        let is_history = key.starts_with(Database::DURABLE_PURGED_LINEAGE_HISTORY_PREFIX);
        let expected_key = if is_history {
            Database::durable_purged_lineage_history_key(record)
        } else {
            Database::durable_lineage_config_key(
                &record.table,
                &record.natural_key,
                record.table_generation,
            )
        };
        if key != expected_key {
            return Err(Error::SyncError(
                ("durable lineage lifecycle key disagrees with its canonical record".to_string())
                    .into(),
            ));
        }
        match (record.delete_obligation, record.purge_frontier.as_deref()) {
            (DurableDeleteObligation::Purged, Some(frontier)) => {
                let frontier = frontier.parse::<u64>().map_err(|_| {
                    Error::SyncError(
                        ("durable lineage purge frontier is not a valid LSN".to_string()).into(),
                    )
                })?;
                self.purge_frontier_max_lsn = self.purge_frontier_max_lsn.max(Lsn(frontier));
            }
            (DurableDeleteObligation::Purged, None) => {
                return Err(Error::SyncError(
                    ("durable purged lineage is missing its permanent frontier".to_string()).into(),
                ));
            }
            (_, Some(_)) => {
                return Err(Error::SyncError(
                    ("non-purged durable lineage carries a purge frontier".to_string()).into(),
                ));
            }
            (_, None) if is_history => {
                return Err(Error::SyncError(
                    ("durable purged-lineage history holds a non-purged record".to_string()).into(),
                ));
            }
            (_, None) => {}
        }
        if let Some(row_id) = record.local_row_id {
            self.max_row_id = self.max_row_id.max(row_id);
        }
        Ok(())
    }
}

impl std::ops::Deref for FundedGraphArrival {
    type Target = DurableGraphArrival;
    fn deref(&self) -> &Self::Target {
        &self.0.record
    }
}

pub(super) fn sync_row_identity(row: &RowChange) -> Vec<u8> {
    sync_identity_key(&row.table, &row.natural_key)
}

pub(super) fn sync_identity_key(table: &str, natural_key: &NaturalKey) -> Vec<u8> {
    let mut identity = (table.len() as u64).to_be_bytes().to_vec();
    identity.extend_from_slice(table.as_bytes());
    // MessagePack encodes this fixed NaturalKey shape without display-format
    // ambiguities, so distinct key values cannot collapse into one component.
    identity.extend(rmp_serde::to_vec(natural_key).expect("sync natural key serializes"));
    identity
}

pub(super) fn dependency_row_references_parent(
    db: &Database,
    child: &RowChange,
    parent: &RowChange,
) -> bool {
    let Some(meta) = db.table_meta(&child.table) else {
        return false;
    };
    meta.columns.iter().any(|column| {
        column.references.as_ref().is_some_and(|reference| {
            parent.table == reference.table
                && child.values.get(&column.name).is_some_and(|value| {
                    !matches!(value, Value::Null)
                        && parent.values.get(&reference.column) == Some(value)
                })
        })
    }) || meta.composite_foreign_keys.iter().any(|reference| {
        parent.table == reference.parent_table
            && reference
                .child_columns
                .iter()
                .zip(reference.parent_columns.iter())
                .all(|(child_column, parent_column)| {
                    child.values.get(child_column).is_some_and(|value| {
                        !matches!(value, Value::Null)
                            && parent.values.get(parent_column) == Some(value)
                    })
                })
    })
}

#[cfg(any(test, feature = "test-seams"))]
impl ExecutionProbe for ReadExecutionKernelProbe {
    fn before_work(&self, _source: TestWorkSource, _completed_work: u64) {}

    fn before_source_touch(&self, source: TestSourceTouch, completed_items: u64) {
        self.observer
            .observe(ReadExecutionConvergenceEvent::PullKernelSourceTouch {
                source: source.into(),
                completed_items,
            });
    }

    fn cancellation_observed(&self, _completed_work: u64) {}
}

#[cfg(any(test, feature = "test-seams"))]
impl Drop for CommonOptionsOpenInterceptorGuard {
    fn drop(&mut self) {
        let previous = self.previous.take();
        COMMON_OPTIONS_OPEN_INTERCEPTOR.with(|slot| {
            let _ = slot.replace(previous);
        });
    }
}

#[cfg(any(test, feature = "test-seams"))]
impl Drop for ReadExecutionConvergenceObserverGuard {
    fn drop(&mut self) {
        let previous = self.previous.take();
        READ_EXECUTION_CONVERGENCE_OBSERVER.with(|slot| {
            let _ = slot.replace(previous);
        });
    }
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn observe_read_execution_convergence(event: ReadExecutionConvergenceEvent) {
    let observer = READ_EXECUTION_CONVERGENCE_OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(observer) = observer {
        observer.observe(event);
    }
}

impl VectorSearchScopeShape {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::One => "one",
            Self::Few => "several",
            Self::All => "all",
        }
    }
}

impl VectorSearchRoute {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Indexed => "indexed",
            Self::FilteredIndexed => "filtered-indexed",
        }
    }
}

impl VectorSearchRouteEvidence {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PassivePrediction => "passive-prediction",
            Self::Executed => "executed",
        }
    }
}

impl VectorSearchResidual {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Bounded => "bounded",
            Self::Unsupported => "unsupported",
        }
    }
}

impl VectorSearchLayerPresence {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Present => "present",
            Self::Absent => "absent",
        }
    }
}
#[cfg(any(test, feature = "test-seams"))]
impl WriterReceiptSampleHeld {
    pub(super) fn enter(identity: usize) -> Self {
        WRITER_RECEIPT_CONTENTION_OBSERVERS
            .lock()
            .samples
            .push(identity);
        Self(identity)
    }
}
#[cfg(any(test, feature = "test-seams"))]
impl Drop for WriterReceiptSampleHeld {
    fn drop(&mut self) {
        let mut state = WRITER_RECEIPT_CONTENTION_OBSERVERS.lock();
        let at = state
            .samples
            .iter()
            .rposition(|identity| *identity == self.0)
            .expect("a memory sample retains its actual exclusion");
        state.samples.remove(at);
    }
}
#[cfg(any(test, feature = "test-seams"))]
pub(super) fn note_writer_receipt_contention(identity: usize) {
    let callbacks = {
        let mut state = WRITER_RECEIPT_CONTENTION_OBSERVERS.lock();
        if !state.samples.contains(&identity) {
            return;
        }
        state
            .observers
            .iter_mut()
            .filter_map(|entry| {
                if entry.identity == identity && !entry.fired {
                    entry.fired = true;
                    Some(entry.observer.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    };
    for callback in callbacks {
        callback();
    }
}
impl<'a> MaintenanceReceiptGuard<'a> {
    pub(super) fn lock(mutex: &'a Arc<parking_lot::Mutex<()>>) -> Self {
        #[cfg(any(test, feature = "test-seams"))]
        let identity = Arc::as_ptr(mutex) as usize;
        #[cfg(any(test, feature = "test-seams"))]
        let guard = mutex.try_lock().unwrap_or_else(|| {
            note_writer_receipt_contention(identity);
            mutex.lock()
        });
        #[cfg(not(any(test, feature = "test-seams")))]
        let guard = mutex.lock();
        #[cfg(any(test, feature = "test-seams"))]
        HELD_MAINTENANCE_RECEIPT_GUARDS.with(|held| held.borrow_mut().push(identity));
        Self {
            _guard: guard,
            #[cfg(any(test, feature = "test-seams"))]
            identity,
        }
    }
    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn held_for(mutex: &Arc<parking_lot::Mutex<()>>) -> bool {
        HELD_MAINTENANCE_RECEIPT_GUARDS
            .with(|held| held.borrow().contains(&(Arc::as_ptr(mutex) as usize)))
    }
}
#[cfg(any(test, feature = "test-seams"))]
impl Drop for MaintenanceReceiptGuard<'_> {
    fn drop(&mut self) {
        HELD_MAINTENANCE_RECEIPT_GUARDS
            .with(|held| assert_eq!(held.borrow_mut().pop(), Some(self.identity)));
    }
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn serialize_relational_inventory_for_test<S: serde::Serializer>(
    inventory: &contextdb_relational::store::RetainedStructureBytes,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    use serde::ser::SerializeStruct;
    let mut fields = serializer.serialize_struct("RetainedStructureBytes", 9)?;
    fields.serialize_field("row_versions", &inventory.row_versions)?;
    fields.serialize_field("row_positions", &inventory.row_positions)?;
    fields.serialize_field("row_version_positions", &inventory.row_version_positions)?;
    fields.serialize_field("sync_provenance", &inventory.sync_provenance)?;
    fields.serialize_field("index_trees", &inventory.index_trees)?;
    fields.serialize_field("index_exact", &inventory.index_exact)?;
    fields.serialize_field("index_exact_filters", &inventory.index_exact_filters)?;
    fields.serialize_field("index_membership", &inventory.index_membership)?;
    fields.serialize_field("index_descriptors", &inventory.index_descriptors)?;
    fields.end()
}

impl VectorSearchTailState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Present => "present",
            Self::Empty => "empty",
        }
    }
}

impl VectorQuerySourceDisclosure {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Vector => "<vector>",
            Self::RedactedRowKey => "<redacted>",
            Self::Unknown => "unknown",
        }
    }
}

impl std::fmt::Display for VectorSearchDisclosure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let partition_key = if self.partition_key_columns.is_empty() {
            "none".to_owned()
        } else {
            self.partition_key_columns.join(",")
        };
        let partition_hnsw = self
            .partition_hnsw
            .iter()
            .map(|partition| {
                format!(
                    "partition={} route={} reason={} hnsw_m={} hnsw_ef_construction={} hnsw_ef_search={} \
                     ef_search_source={} policy_revision={}",
                    partition.partition,
                    partition.route.map_or("null", VectorSearchRoute::as_str),
                    partition.reason.as_deref().unwrap_or("none"),
                    partition.hnsw_m,
                    partition.hnsw_ef_construction,
                    partition.hnsw_ef_search,
                    partition.ef_search_source,
                    partition.policy_revision,
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        write!(
            formatter,
            "vector_search route_evidence={} requested_mode={} resolved_mode={} aggregate_allowed_vectors={} \
             effective_auto_index_at={} auto_index_at_source={} effective_flat_below={} \
             flat_below_source={} effective_search_radius={} search_radius_source={} \
             effective_auto_expansion_ceiling={} auto_expansion_ceiling_source={} \
             partition_key={} scope={} route={} \
             base={} change={} tail={} merge=global residual={} fallback={} refusal={} recovery={} \
             query_source={} partition_hnsw=[{}]",
            self.route_evidence.as_str(),
            self.requested_mode,
            self.resolved_mode,
            render_optional_count(self.aggregate_allowed_vectors),
            self.effective_auto_index_at,
            self.auto_index_at_source,
            self.effective_flat_below,
            self.flat_below_source,
            contextdb_core::render_search_radius_millionths(
                self.effective_search_radius_millionths as u32
            ),
            self.search_radius_source,
            self.effective_auto_expansion_ceiling,
            self.auto_expansion_ceiling_source,
            partition_key,
            self.scope.as_str(),
            self.route.map_or("null", VectorSearchRoute::as_str),
            self.base.as_str(),
            self.change.as_str(),
            self.tail.as_str(),
            self.residual.as_str(),
            self.fallback.as_deref().unwrap_or("none"),
            self.refusal.as_deref().unwrap_or("none"),
            self.recovery.as_deref().unwrap_or("none"),
            self.query_source.as_str(),
            partition_hnsw,
        )
    }
}

/// A count that has no value yet renders the way every other unresolved
/// inspection fact does: as `null`, never as a number.
pub(super) fn render_optional_count(count: Option<usize>) -> String {
    count.map_or_else(|| "null".to_owned(), |count| count.to_string())
}

pub(super) fn executed_auto_fallback(
    partition_routes: &[VectorPartitionSearchTrace],
    fallback_reason: Option<&'static str>,
    filtered_exact: bool,
) -> Option<&'static str> {
    if filtered_exact {
        return Some("filtered_route_exact");
    }
    let mut first = None;
    let mut differs = false;
    for partition in partition_routes
        .iter()
        .filter(|partition| partition.route == VectorPartitionSearchRoute::Exact)
    {
        if let Some(first) = first {
            differs |= first != partition.reason;
        } else {
            first = Some(partition.reason);
        }
    }
    match (first, differs) {
        (Some(_), true) => Some("mixed_partition_routes"),
        (Some(reason), false) => Some(reason),
        (None, _) => fallback_reason,
    }
}
impl std::ops::Deref for RetainedVectorSearchTrace {
    type Target = VectorSearchDebugTrace;
    fn deref(&self) -> &Self::Target {
        &self.trace
    }
}
impl RetainedVectorSearchTrace {
    /// Hand the trace and the admission holding it to the trace slot.
    pub(super) fn into_published(self) -> contextdb_vector::mem::OwnedVectorSearchTrace {
        contextdb_vector::mem::OwnedVectorSearchTrace::from_admitted(
            self.trace,
            self.executing_owner,
            self.owner,
        )
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn from_owned(
        trace: contextdb_vector::mem::OwnedVectorSearchTrace,
        executing_owner: impl Into<contextdb_vector::MemoryBudgetOwner>,
    ) -> Result<Self> {
        let executing_owner: contextdb_vector::MemoryBudgetOwner = executing_owner.into();

        let (trace, credit) = trace.into_prepaid(&executing_owner)?;
        let owner =
            contextdb_vector::VectorCopyAdmission::from_prepaid(executing_owner.clone(), credit)?;
        let mut retained = Self {
            trace,
            owner,
            executing_owner,
        };
        retained.owner.reserve(Self::bytes(&retained.trace))?;
        Ok(retained)
    }
    pub(super) fn bytes(trace: &VectorSearchDebugTrace) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(trace.index.table.capacity())
            .saturating_add(trace.index.column.capacity())
            .saturating_add(
                trace
                    .hnsw_candidate_row_ids
                    .capacity()
                    .saturating_mul(std::mem::size_of::<RowId>()),
            )
            .saturating_add(
                trace
                    .final_row_ids
                    .capacity()
                    .saturating_mul(std::mem::size_of::<RowId>()),
            )
            .saturating_add(
                trace
                    .partition_routes
                    .capacity()
                    .saturating_mul(std::mem::size_of::<
                        contextdb_vector::mem::VectorPartitionSearchTrace,
                    >()),
            )
            .saturating_add(trace.selected_generations.capacity().saturating_mul(
                std::mem::size_of::<contextdb_vector::store::VectorGraphGeneration>(),
            ))
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn new(
        trace: VectorSearchDebugTrace,
        executing_owner: impl Into<contextdb_vector::MemoryBudgetOwner>,
    ) -> Result<Self> {
        let executing_owner: contextdb_vector::MemoryBudgetOwner = executing_owner.into();

        let owner = contextdb_vector::VectorCopyAdmission::new(executing_owner.clone());
        let mut retained = Self {
            trace,
            owner,
            executing_owner,
        };
        retained.owner.reserve(Self::bytes(&retained.trace))?;
        Ok(retained)
    }
    pub(crate) fn mark_filtered_exact_route(&mut self) {
        self.trace.fallback_reason = Some("filtered_route_exact");
        for partition in &mut self.trace.partition_routes {
            if partition.route == contextdb_vector::VectorPartitionSearchRoute::Exact {
                partition.reason = "filtered_route_exact";
            }
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn set_final_row_ids(&mut self, rows: &[(RowId, f32)]) -> Result<()> {
        self.owner
            .reserve(rows.len().saturating_mul(std::mem::size_of::<RowId>()))?;
        let mut ids = Vec::with_capacity(rows.len());
        ids.extend(rows.iter().map(|(row_id, _)| *row_id));
        self.trace.final_row_ids = ids;
        Ok(())
    }
}
impl VectorSearchTraceObservation {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from native_buffer_transfer_tests.rs"
        )
    )]
    pub(super) fn copy_from(
        trace: &contextdb_vector::mem::OwnedVectorSearchTrace,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Self> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let bytes =
            std::mem::size_of::<Self>()
                .saturating_add(trace.index().table.len())
                .saturating_add(trace.index().column.len())
                .saturating_add(
                    trace
                        .hnsw_candidate_row_ids()
                        .len()
                        .saturating_mul(std::mem::size_of::<RowId>()),
                )
                .saturating_add(trace.selected_generations().len().saturating_mul(
                    std::mem::size_of::<contextdb_vector::store::VectorGraphGeneration>(),
                ))
                .saturating_add(
                    trace
                        .final_row_ids()
                        .len()
                        .saturating_mul(std::mem::size_of::<RowId>()),
                )
                .saturating_add(trace.partition_routes().len().saturating_mul(
                    std::mem::size_of::<contextdb_vector::mem::VectorPartitionSearchTrace>(),
                ));
        let memory = contextdb_core::read_memory::ReadCredit::try_new(
            read_memory,
            bytes,
            "vector_search",
            "trace_observation",
            "Raise the executing reader's memory limit to copy this trace.",
        )?;
        Ok(Self {
            index: trace.index().clone(),
            used_hnsw: trace.used_hnsw(),
            hnsw_len: trace.hnsw_len(),
            hnsw_candidate_count: trace.hnsw_candidate_count(),
            hnsw_candidate_row_ids: trace.hnsw_candidate_row_ids().to_vec(),
            supplemented_row_count: trace.supplemented_row_count(),
            hnsw_ef_search: trace.hnsw_ef_search(),
            selected_generations: trace.selected_generations().to_vec(),
            final_row_ids: trace.final_row_ids().to_vec(),
            partition_routes: trace.partition_routes().to_vec(),
            fallback_reason: trace.fallback_reason(),
            budget_receipt: trace.budget_receipt(),
            _memory: memory,
        })
    }
}

impl QueryTrace {
    /// Stub default: scan-labeled trace with no plan data. The no-op writes
    /// this everywhere. Impl must replace construction sites with real plan
    /// inspection.
    pub fn scan() -> Self {
        Self {
            physical_plan: "Scan",
            ..Default::default()
        }
    }
}

impl SnapshotInspector {
    pub(super) const MAX_RETAINED_VERSIONS: usize = 128;
    pub(super) const MAX_INSPECTED_COLUMNS: usize = 16;
    pub(super) const MAX_COLUMN_NAME_BYTES: usize = 128;
    pub(super) const MAX_TEXT_VALUE_BYTES: usize = 32 * 1024;
    pub(super) const MAX_EXACT_VALUE_OUTPUT_BYTES: usize = 1024 * 1024;

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn open(
        artifact: impl AsRef<Path>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        declared_read_memory: crate::vector_read_memory::DeclaredReadRoot,
    ) -> Result<Self> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let artifact = artifact.as_ref();
        if !artifact.is_file() {
            return Err(Error::Other(
                (format!(
                    "inspection requires a snapshot artifact or database file path: '{}'",
                    artifact.display()
                ))
                .into(),
            ));
        }
        let copy = tempfile::Builder::new()
            .prefix("contextdb-inspection-")
            .tempdir()
            .map_err(|err| {
                Error::Other((format!("failed to create private inspection copy: {err}")).into())
            })?;
        let copy_path = copy.path().join("snapshot.redb");
        std::fs::copy(artifact, &copy_path).map_err(|err| {
            Error::Other(
                (format!(
                    "failed to copy '{}' for inspection: {err}",
                    artifact.display()
                ))
                .into(),
            )
        })?;
        let database = Database::open_loaded(
            &copy_path,
            Arc::new(CorePlugin),
            MemoryAccountant::writer_no_limit()?,
            None,
            OpenDisposition::CreateIfMissing,
            None,
            declared_read_memory,
            read_memory,
            DatabaseHostConstruction::Legacy,
        )?;
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_probe::note_plugin_start();
        database.plugin.on_open()?;
        Ok(Self {
            database,
            _copy: copy,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn inspect_key(
        &self,
        table: &str,
        natural_key: &NaturalKey,
        columns: &[String],
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<KeyInspection> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if columns.len() > Self::MAX_INSPECTED_COLUMNS
            || columns
                .iter()
                .any(|column| column.len() > Self::MAX_COLUMN_NAME_BYTES)
            || columns.iter().collect::<BTreeSet<_>>().len() != columns.len()
        {
            return Err(Error::Other((format!(
                "snapshot key inspection accepts at most {} distinct column names of at most {} bytes each",
                Self::MAX_INSPECTED_COLUMNS,
                Self::MAX_COLUMN_NAME_BYTES
            )).into()));
        }
        let meta = self
            .database
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
        let declared_key = natural_key_columns_for_meta(&meta).ok_or_else(|| {
            Error::SyncError(
                (format!("table '{table}' has no declared natural key to inspect")).into(),
            )
        })?;
        if natural_key.key_columns() != declared_key {
            return Err(Error::SyncError(
                (format!(
                    "snapshot key inspection requires the declared natural key ({})",
                    declared_key.join(", ")
                ))
                .into(),
            ));
        }
        for column in columns {
            if !meta.columns.iter().any(|declared| declared.name == *column) {
                return Err(Error::ColumnNotFound {
                    table: table.to_string(),
                    column: column.clone(),
                });
            }
        }
        let root = self.database.writer_read_root()?.ok_or_else(|| {
            Error::Other("snapshot inspection copy has no captured durable root".into())
        })?;
        let _directory_memory = contextdb_core::read_memory::ReadCredit::try_new(
            read_memory.clone(),
            Self::MAX_RETAINED_VERSIONS.saturating_mul(128),
            "snapshot_inspection",
            "selected_versions",
            "Raise the reader memory limit to inspect retained versions.",
        )?;
        // Retain only the newest output coordinates, never a directory of
        // every matching body. Each physical body belongs to its read window.
        let mut selected = BTreeMap::new();
        let mut total_retained_versions = 0_u64;
        let mut cursor = root.cursor(
            crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
            &RedbPersistence::rel_table_name(table),
            crate::persistence::writer_source::WriterCursorFamily::Bytes,
            std::ops::Bound::Unbounded,
            std::ops::Bound::Unbounded,
            false,
        )?;
        while let Some(window) = cursor.next_row()? {
            let row = &window.value;
            if natural_key.matches_values(&row.values) {
                total_retained_versions = total_retained_versions.saturating_add(1);
                let key: [u8; 24] = window.position().try_into().map_err(|_| {
                    crate::funded_error::corruption_text(
                        Ok(read_memory.clone()),
                        table,
                        "snapshot row has an invalid physical version key".into(),
                    )
                })?;
                selected.insert((row.lsn, row.created_tx, row.row_id), key);
                if selected.len() > Self::MAX_RETAINED_VERSIONS {
                    selected.pop_first();
                }
            }
        }
        drop(cursor);
        let versions_truncated = total_retained_versions > Self::MAX_RETAINED_VERSIONS as u64;
        let mut remaining_exact_bytes = Self::MAX_EXACT_VALUE_OUTPUT_BYTES;
        let mut retained_versions = Vec::with_capacity(selected.len());
        for key in selected.into_values() {
            let mut cursor = root.exact_row(
                crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                table,
                &key,
            )?;
            let window = cursor.next_row()?.ok_or_else(|| {
                crate::funded_error::corruption_text(
                    Ok(read_memory.clone()),
                    table,
                    "snapshot key inspection is missing a selected saved version".into(),
                )
            })?;
            let row = &window.value;
            let values = columns
                .iter()
                .filter_map(|column| {
                    row.values.get(column).map(|value| {
                        (
                            column.clone(),
                            Self::inspect_retained_value(value, &mut remaining_exact_bytes),
                        )
                    })
                })
                .collect();
            retained_versions.push(RetainedVersionInspection {
                row_id: row.row_id.0,
                created_tx: row.created_tx.0,
                deleted_tx: row.deleted_tx.map(|tx| tx.0),
                lsn: row.lsn.0,
                values,
            });
        }
        let lineage = self
            .database
            .durable_lineage_record(table, natural_key, read_memory.clone())?
            .map(|record| -> Result<LineageInspection> {
                let purge_frontier_lsn = record
                    .purge_frontier
                    .as_deref()
                    .map(str::parse::<u64>)
                    .transpose()
                    .map_err(|_| {
                        Error::SyncError(
                            ("durable lineage purge frontier is not a valid LSN".to_string())
                                .into(),
                        )
                    })?;
                let delete_obligation = match record.delete_obligation {
                    DurableDeleteObligation::Pending => DeleteObligationInspection::Pending,
                    DurableDeleteObligation::Accepted => DeleteObligationInspection::Accepted,
                    DurableDeleteObligation::Purged => DeleteObligationInspection::Purged,
                };
                Ok(LineageInspection {
                    table_generation: record.table_generation,
                    lineage_fingerprint: blake3::hash(record.lineage_root.as_bytes())
                        .to_hex()
                        .to_string(),
                    delete_obligation,
                    accepted_hub_lsn: record.accepted_hub_lsn,
                    purge_frontier_lsn,
                })
            })
            .transpose()?;
        Ok(KeyInspection {
            total_retained_versions,
            versions_truncated,
            retained_versions,
            lineage,
        })
    }

    pub(super) fn inspect_retained_value(
        value: &Value,
        remaining_exact_bytes: &mut usize,
    ) -> RetainedValueInspection {
        let (kind, exact_bytes, source_units, always_omit) = match value {
            Value::Null => ("null", 8, None, false),
            Value::Bool(_) => ("bool", 8, None, false),
            Value::Int64(_) => ("int64", 24, None, false),
            Value::Float64(_) => ("float64", 32, None, false),
            Value::Text(text) => (
                "text",
                text.len().saturating_mul(6).saturating_add(32),
                Some(text.len() as u64),
                text.len() > Self::MAX_TEXT_VALUE_BYTES,
            ),
            Value::Uuid(_) => ("uuid", 64, None, false),
            Value::Timestamp(_) => ("timestamp", 32, None, false),
            Value::Json(_) => ("json", 0, None, true),
            Value::Vector(values) => ("vector", 0, Some(values.len() as u64), true),
            Value::TxId(_) => ("txid", 24, None, false),
        };
        let omission_reason = if always_omit {
            Some(match value {
                Value::Text(_) => "value_exceeds_per_value_bound",
                Value::Json(_) => "json_value_omitted",
                Value::Vector(_) => "vector_value_omitted",
                _ => unreachable!("only explicitly bounded value kinds are always omitted"),
            })
        } else if exact_bytes > *remaining_exact_bytes {
            Some("report_value_budget_exhausted")
        } else {
            None
        };
        let exact = omission_reason.is_none();
        if exact {
            *remaining_exact_bytes = remaining_exact_bytes.saturating_sub(exact_bytes);
        }
        RetainedValueInspection {
            kind: kind.to_string(),
            value: exact.then(|| value.clone()),
            omitted: !exact,
            omission_reason: omission_reason.map(str::to_string),
            source_units,
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn inspect_blob(&self, hash: [u8; 32]) -> Result<BlobInspection> {
        use crate::blob_repository::{BlobManifestState, BlobTagRole};

        let repository = self.database.blob_repository();
        let generation = repository.generation_state(&hash)?;
        let read = repository.open_read(hash)?;
        let (manifest, partial) = match read {
            Some(read) => {
                if generation.active.map(|active| active.generation) != Some(read.generation()) {
                    return Err(Error::Other(
                        ("snapshot blob generation changed during inspection".to_string()).into(),
                    ));
                }
                let source = read.manifest();
                let state = match source.state {
                    BlobManifestState::Partial => BlobManifestStateInspection::Partial,
                    BlobManifestState::Complete => BlobManifestStateInspection::Complete,
                };
                let partial = read
                    .partial()
                    .map(|_| read.contiguous_payload_prefix())
                    .transpose()?
                    .map(|contiguous_payload_prefix| BlobPartialInspection {
                        contiguous_payload_prefix,
                    });
                (
                    Some(BlobManifestInspection {
                        total_size: source.total_size,
                        outboard_size: source.outboard_size,
                        validated_size: source.validated_size,
                        payload_chunk_count: source.payload_chunk_count,
                        outboard_chunk_count: source.outboard_chunk_count,
                        state,
                    }),
                    partial,
                )
            }
            None => (None, None),
        };
        Ok(BlobInspection {
            hash: crate::work_ledger::BlobHash::from_bytes(hash).to_hex(),
            active_generation: generation.active.map(|active| active.generation),
            last_retired_generation: generation.last_retired_generation,
            last_purge_lsn: generation.last_purge_lsn,
            manifest,
            partial,
            tag_roles: BlobTagRolesInspection {
                servable: repository.has_tag_role(&hash, BlobTagRole::Servable)?,
                fetch_protection: repository.has_tag_role(&hash, BlobTagRole::FetchProtection)?,
            },
        })
    }

    /// Fingerprint every durable category that authenticated schema/data
    /// apply can mutate. Relational rows use canonical decoded content so
    /// snapshot re-encoding cannot change the result; vectors and private
    /// sidecars retain exact persisted key/value-byte coverage. Large
    /// row/vector values never enter the report.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn inspect_sync_apply_state(&self) -> Result<SyncApplyStateInspection> {
        let persistence = self.database.persistence.as_ref().ok_or_else(|| {
            Error::Other(("snapshot inspection copy is not file-backed".to_string()).into())
        })?;
        // The digest and census share one physical transaction. Lazy process
        // caches do not describe the retained contents of the copied artifact.
        // An inspection is a read: the declared reader owns its decodes.
        let decode = crate::read_image_memory::ImageMemory::operation_reader(
            crate::memory_accounting::AccountantHandle::clone(
                &self.database.finalization.accountant,
            ),
            self.database.declared_reader_memory()?,
        )?;
        let _decode = decode.enter();
        let raw = persistence.raw_sync_apply_state_digest()?;
        Ok(SyncApplyStateInspection {
            digest: raw.digest,
            current_lsn: self.database.current_lsn().0,
            tables: raw.tables,
            retained_row_versions: raw.retained_row_versions,
            index_postings: raw.index_postings,
            edges: raw.edges,
            vectors: raw.vectors,
            ddl_log_entries: raw.ddl_log_entries,
            change_log_entries: raw.change_log_entries,
            commit_index_entries: raw.commit_index_entries,
            persisted_config_entries: raw.persisted_config_entries,
            trigger_audit_entries: raw.trigger_audit_entries,
            sink_audit_entries: raw.sink_audit_entries,
            sink_queue_entries: raw.sink_queue_entries,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn close(self) -> Result<()> {
        self.database.close()
    }
}

impl IncomingPurgeFrontierAdoption {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn at_frontier(&self, frontier: Lsn) -> Result<DurableLineageRecord> {
        let mut record = if let Some(existing) = &self.existing {
            if existing.table != self.table
                || existing.natural_key != self.natural_key
                || existing.table_generation != self.table_generation
                || existing.lineage_root != self.lineage_root
            {
                return Err(Error::SyncError(
                    ("authoritative purge frontier conflicts with existing lineage evidence"
                        .to_string())
                    .into(),
                ));
            }
            existing.clone()
        } else {
            let mut author_node_id = None;
            let mut author_database_incarnation = None;
            let mut author_local_mutation_position = 0;
            let components = self.lineage_root.split(':').collect::<Vec<_>>();
            if components.first() == Some(&"author") {
                if components.len() != 4
                    || components[1].len() != 64
                    || !components[1]
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                {
                    return Err(Error::SyncError(
                        ("authoritative purge frontier has an invalid creator root".to_string())
                            .into(),
                    ));
                }
                let incarnation = Incarnation::from_hex(components[2]).ok_or_else(|| {
                    Error::SyncError(
                        ("authoritative purge frontier has an invalid creator root".to_string())
                            .into(),
                    )
                })?;
                author_local_mutation_position = components[3].parse::<u64>().map_err(|_| {
                    Error::SyncError(
                        ("authoritative purge frontier has an invalid creator position"
                            .to_string())
                        .into(),
                    )
                })?;
                let canonical_root = format!(
                    "author:{}:{}:{}",
                    components[1],
                    incarnation.to_hex(),
                    author_local_mutation_position
                );
                if self.lineage_root != canonical_root {
                    return Err(Error::SyncError(
                        ("authoritative purge frontier creator root is not canonical".to_string())
                            .into(),
                    ));
                }
                author_node_id = Some(components[1].to_string());
                author_database_incarnation = Some(incarnation.to_hex());
            }
            DurableLineageRecord {
                table: self.table.clone(),
                natural_key: self.natural_key.clone(),
                table_generation: self.table_generation,
                local_row_id: None,
                locally_created: false,
                lineage_root: self.lineage_root.clone(),
                lineage_attestation: Vec::new(),
                author_node_id,
                author_database_incarnation,
                author_local_mutation_position,
                delete_lsn: frontier.0,
                delete_obligation: DurableDeleteObligation::Purged,
                accepted_hub_lsn: None,
                bound_hub_node_id: None,
                purge_frontier: Some(frontier.0.to_string()),
            }
        };
        record.delete_lsn = frontier.0;
        record.delete_obligation = DurableDeleteObligation::Purged;
        record.accepted_hub_lsn = None;
        record.bound_hub_node_id = None;
        record.purge_frontier = Some(frontier.0.to_string());
        Ok(record)
    }
}

impl AuthoritativePurgeLifecycleTemplate {
    pub(super) fn at_frontier(&self, frontier: Lsn) -> DurableLineageRecord {
        DurableLineageRecord {
            table: self.table.clone(),
            natural_key: self.natural_key.clone(),
            table_generation: self.table_generation,
            local_row_id: Some(self.local_row_id),
            locally_created: self.locally_created,
            lineage_root: self.lineage_root.clone(),
            lineage_attestation: self.lineage_attestation.clone(),
            author_node_id: self.author_node_id.clone(),
            author_database_incarnation: self.author_database_incarnation.clone(),
            author_local_mutation_position: self.author_local_mutation_position,
            delete_lsn: frontier.0,
            delete_obligation: DurableDeleteObligation::Purged,
            accepted_hub_lsn: None,
            bound_hub_node_id: None,
            purge_frontier: Some(frontier.0.to_string()),
        }
    }
}

impl SemanticQuery {
    pub fn new(
        table: impl Into<String>,
        vector_column: impl Into<String>,
        query: Vec<f32>,
        limit: usize,
    ) -> Self {
        Self {
            table: table.into(),
            vector_column: vector_column.into(),
            query,
            limit,
            search_mode: None,
            sort_key: None,
            min_similarity: None,
            where_clause: None,
        }
    }
}

impl std::fmt::Debug for CronPauseGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CronPauseGuard").finish_non_exhaustive()
    }
}

impl Drop for CronPauseGuard {
    fn drop(&mut self) {
        self.cron.resume_tickler();
    }
}

impl ApplyPhasePauseGuard {
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn arm_for_test(inner: Arc<ApplyPhasePause>) -> Self {
        let generation = inner.arm();
        Self { inner, generation }
    }

    pub fn wait_until_reached(&self, timeout: Duration) -> bool {
        self.inner.wait_until_reached(self.generation, timeout)
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub fn wait_until_reached_blocking(&self) -> bool {
        self.inner.wait_until_reached_blocking(self.generation)
    }

    pub fn release(&self) {
        self.inner.release(self.generation);
    }
}

impl Drop for ApplyPhasePauseGuard {
    fn drop(&mut self) {
        self.release();
    }
}

// Public serialization carries the identity in both human-readable and binary
// formats. The durable queue has its own positional codec below.
impl Serialize for SinkEvent {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            use serde::ser::SerializeStruct;
            let mut state = serializer.serialize_struct("SinkEvent", 6)?;
            state.serialize_field("action_id", &self.action_id)?;
            state.serialize_field("event_type", &self.event_type)?;
            state.serialize_field("table", &self.table)?;
            state.serialize_field("row_values", &self.row_values)?;
            state.serialize_field("severity", &self.severity)?;
            state.serialize_field("at_lsn", &self.at_lsn)?;
            state.end()
        } else {
            (
                &self.action_id,
                &self.event_type,
                &self.table,
                &self.row_values,
                &self.severity,
                &self.at_lsn,
            )
                .serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for SinkEvent {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            #[derive(Deserialize)]
            struct HumanSinkEvent {
                #[serde(default)]
                action_id: u64,
                event_type: String,
                table: String,
                row_values: HashMap<String, Value>,
                severity: String,
                at_lsn: Lsn,
            }
            let event = HumanSinkEvent::deserialize(deserializer)?;
            Ok(Self {
                action_id: event.action_id,
                event_type: event.event_type,
                table: event.table,
                row_values: event.row_values,
                severity: event.severity,
                at_lsn: event.at_lsn,
            })
        } else {
            let (action_id, event_type, table, row_values, severity, at_lsn) =
                <(u64, String, String, HashMap<String, Value>, String, Lsn)>::deserialize(
                    deserializer,
                )?;
            Ok(Self {
                action_id,
                event_type,
                table,
                row_values,
                severity,
                at_lsn,
            })
        }
    }
}

impl DatabaseId {
    pub(super) fn next() -> Self {
        Self(NEXT_DATABASE_ID.fetch_add(1, Ordering::Relaxed))
    }
}

#[cfg(any(test, feature = "test-seams"))]
impl MaintenancePollIntervalUpdateProbe {
    pub(super) fn note_entered(&self) {
        let mut entries = self.entries.lock();
        *entries = entries.saturating_add(1);
        self.changed.notify_all();
    }

    pub(super) fn entries(&self) -> u64 {
        *self.entries.lock()
    }

    pub(super) fn wait_for(&self, target: u64) {
        let mut entries = self.entries.lock();
        while *entries < target {
            self.changed.wait(&mut entries);
        }
    }
}

impl<'a> QueryRowsExaminedScope<'a> {
    pub(super) fn enter(database: &'a Database) -> Self {
        let database_key = database.id;
        let id = QUERY_ROWS_EXAMINED_SCOPES.with(|scopes| {
            let mut scopes = scopes.borrow_mut();
            let id = scopes.next_id;
            scopes.next_id = scopes.next_id.wrapping_add(1);
            scopes.active.push(ActiveQueryRowsExaminedScope {
                id,
                database_key,
                rows_examined: 0,
            });
            id
        });
        database.rows_examined.store(0, Ordering::SeqCst);
        Self {
            database,
            database_key,
            id,
            finished: false,
        }
    }

    pub(super) fn finish(mut self, result: &mut Result<OwnedQueryResult>) {
        let rows_examined = self.remove_active_scope();
        self.database
            .rows_examined
            .store(rows_examined, Ordering::SeqCst);
        if let Ok(result) = result {
            result.trace.rows_examined = rows_examined;
        }
        self.finished = true;
    }

    pub(super) fn remove_active_scope(&self) -> u64 {
        QUERY_ROWS_EXAMINED_SCOPES.with(|scopes| {
            let mut scopes = scopes.borrow_mut();
            let position = scopes
                .active
                .iter()
                .rposition(|scope| scope.id == self.id && scope.database_key == self.database_key)
                .expect("active query rows-examined scope");
            scopes.active.remove(position).rows_examined
        })
    }
}

impl Drop for QueryRowsExaminedScope<'_> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.remove_active_scope();
        }
    }
}

impl QueryResultData {
    pub(crate) fn empty() -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            trace: QueryTrace::scan(),
            cascade: None,
        }
    }

    pub(crate) fn empty_with_affected(rows_affected: u64) -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            rows_affected,
            trace: QueryTrace::scan(),
            cascade: None,
        }
    }
}

impl VectorSchemaGates {
    pub(super) fn sorted_refs(
        refs: impl IntoIterator<Item = VectorIndexRef>,
    ) -> Vec<VectorIndexRef> {
        let mut refs = refs.into_iter().collect::<Vec<_>>();
        refs.sort_by(|a, b| a.table.cmp(&b.table).then(a.column.cmp(&b.column)));
        refs.dedup();
        refs
    }

    pub(super) fn gate_for(&self, index: &VectorIndexRef) -> Arc<RwLock<()>> {
        self.gates
            .lock()
            .entry(index.clone())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    pub(super) fn epoch_for(&self, index: &VectorIndexRef) -> u64 {
        self.epochs.lock().get(index).copied().unwrap_or(0)
    }

    pub(super) fn bump_epochs(&self, refs: &[VectorIndexRef]) {
        let mut epochs = self.epochs.lock();
        for index in refs {
            let epoch = epochs.entry(index.clone()).or_insert(0);
            *epoch = epoch.saturating_add(1);
        }
    }
}

impl VectorSchemaReadGuard {
    pub(super) fn new(
        db_id: usize,
        refs: Vec<VectorIndexRef>,
        guards: Vec<ArcRwLockReadGuard<parking_lot::RawRwLock, ()>>,
    ) -> Self {
        VECTOR_SCHEMA_READ_STACK.with(|stack| {
            stack
                .borrow_mut()
                .extend(refs.iter().cloned().map(|index| (db_id, index)));
        });
        Self {
            db_id,
            refs,
            detached: false,
            detached_holdings: None,
            _guards: guards,
        }
    }

    /// Build a guard that carries its own gate handles and touches no
    /// thread-local bookkeeping, so moving it between threads and releasing it
    /// on a thread that did not open it leaves both threads' records intact.
    pub(super) fn new_detached(
        db_id: usize,
        refs: Vec<VectorIndexRef>,
        guards: Vec<ArcRwLockReadGuard<parking_lot::RawRwLock, ()>>,
    ) -> Self {
        let holdings = VECTOR_SCHEMA_DETACHED_HOLDINGS.with(Arc::clone);
        holdings
            .lock()
            .extend(refs.iter().cloned().map(|index| (db_id, index)));
        Self {
            db_id,
            refs,
            detached: true,
            detached_holdings: Some(holdings),
            _guards: guards,
        }
    }
}

impl Drop for VectorSchemaReadGuard {
    fn drop(&mut self) {
        if self.detached {
            // The claim comes off the list of the thread that opened it, which
            // this guard carries, so releasing on a foreign thread still
            // clears the record the opening thread is answered against.
            if let Some(holdings) = self.detached_holdings.take() {
                let mut holdings = holdings.lock();
                for expected in self.refs.iter().rev() {
                    if let Some(position) = holdings
                        .iter()
                        .rposition(|held| held == &(self.db_id, expected.clone()))
                    {
                        holdings.remove(position);
                    }
                }
            }
            return;
        }
        VECTOR_SCHEMA_READ_STACK.with(|stack| {
            let mut stack = stack.borrow_mut();
            for expected in self.refs.iter().rev() {
                let popped = stack.pop();
                debug_assert_eq!(popped, Some((self.db_id, expected.clone())));
            }
        });
    }
}

/// Whether this thread holds a retained claim on `index` through a bounded
/// cursor it opened and has not closed.
pub(super) fn detached_vector_schema_claim_held(db_id: usize, index: &VectorIndexRef) -> bool {
    let key = (db_id, index.clone());
    VECTOR_SCHEMA_DETACHED_HOLDINGS.with(|holdings| holdings.lock().contains(&key))
}

impl Drop for SyncApplyTriggerGateGuard {
    fn drop(&mut self) {
        SYNC_APPLY_TRIGGER_GATE_BYPASS_DEPTH.with(|depth| {
            depth.set(depth.get().saturating_sub(1));
        });
    }
}

impl Drop for SyncApplyLocalSchemaBypassGuard {
    fn drop(&mut self) {
        SYNC_APPLY_LOCAL_SCHEMA_BYPASS_STACK.with(|stack| {
            let popped = stack.borrow_mut().pop();
            debug_assert_eq!(
                popped,
                Some((self.db_id, self.tx)),
                "sync-apply local-schema bypass stack mismatch"
            );
        });
    }
}

impl Drop for SqlWriteControlBypassGuard {
    fn drop(&mut self) {
        SQL_WRITE_CONTROL_BYPASS_STACK.with(|stack| {
            let popped = stack.borrow_mut().pop();
            debug_assert_eq!(
                popped,
                Some((self.db_id, self.tx)),
                "SQL write-control bypass stack mismatch"
            );
        });
    }
}

impl Drop for UserCommitCallbackGuard {
    fn drop(&mut self) {
        USER_COMMIT_TRIGGER_REENTRY.with(|slot| slot.set(self.prior_reentry));
        USER_COMMIT_ACTIVE.with(|slot| slot.set(self.prior_active));
    }
}

pub(super) fn global_callback_active_count() -> &'static AtomicUsize {
    static ACTIVE: AtomicUsize = AtomicUsize::new(0);
    &ACTIVE
}

impl CloseOwnerGuard<'_> {
    pub(super) fn complete(&mut self) {
        self.complete = true;
    }
}

impl Drop for CloseOwnerGuard<'_> {
    fn drop(&mut self) {
        if self.deferred {
            return;
        }
        *self.progress.lock() = if self.complete {
            CloseProgress::Complete
        } else {
            CloseProgress::Idle
        };
        self.changed.notify_all();
    }
}

impl PluginBinding {
    pub(super) fn builtin() -> Self {
        Self::Builtin(Arc::new(CorePlugin))
    }

    pub(super) fn supplied(plugin: Arc<dyn DatabasePlugin>) -> Self {
        Self::Supplied(plugin)
    }

    pub(super) fn erased(&self) -> Arc<dyn DatabasePlugin> {
        match self {
            Self::Builtin(plugin) => plugin.clone(),
            Self::Supplied(plugin) => plugin.clone(),
        }
    }
}

impl OpenConfiguration {
    pub(super) fn take_options(options: DatabaseOpenOptions) -> (Self, PluginBinding) {
        let DatabaseOpenOptions {
            owner_reads,
            open_disposition,
            plugin,
            memory_limit,
            disk_limit,
            maintenance_policy,
            maintenance_poll_interval,
            contexts,
            scope_labels,
            principal,
            reader_wait_stop,
            reader_wait_deadline,
            #[cfg(any(test, feature = "test-seams"))]
            test_observer,
            #[cfg(any(test, feature = "test-seams"))]
            test_kernel_observer,
        } = options;
        (
            Self {
                owner_reads,
                open_disposition,
                memory_limit,
                disk_limit,
                maintenance_policy,
                maintenance_poll_interval,
                contexts,
                scope_labels,
                principal,
                reader_wait_stop,
                reader_wait_deadline,
                #[cfg(any(test, feature = "test-seams"))]
                test_observer,
                #[cfg(any(test, feature = "test-seams"))]
                test_kernel_observer,
            },
            PluginBinding::supplied(plugin),
        )
    }
}
impl ReceivedCommitTerminal {
    pub(super) fn with_write_set<R>(&self, f: impl FnOnce(&WriteSet) -> R) -> R {
        match self {
            Self::Legacy(ws) => f(ws),
            Self::Owned(pair) => pair.with_legacy(f),
        }
    }
}
impl ReceivedCommitFailure {
    pub(super) fn error(&self) -> &Error {
        match self {
            Self::Legacy(failure) => &failure.error,
            Self::Owned(failure) => failure.error(),
        }
    }
    pub(super) fn with_write_set<R>(&self, f: impl FnOnce(Option<&WriteSet>) -> R) -> R {
        match self {
            Self::Legacy(failure) => f(failure.write_set.as_deref()),
            Self::Owned(failure) => failure.with_write_set(|pair| match pair {
                Some(pair) => pair.with_legacy(|ws| f(Some(ws))),
                None => f(None),
            }),
        }
    }
    pub(super) fn into_error(self) -> Error {
        match self {
            Self::Legacy(failure) => failure.error,
            Self::Owned(failure) => {
                let (error, pair) = failure.into_parts();
                drop(pair);
                error
            }
        }
    }
}

impl DatabaseHostConstruction {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare(
        self,
        common: Arc<EventBusState>,
        writer: &WriterBudget,
    ) -> Result<Option<OwnedHostAssembly>> {
        match self {
            Self::Legacy => {
                let _ = (common, writer);
                Ok(None)
            }
            #[cfg(test)]
            Self::Owned => OwnedHostAssembly::prepare(common, writer).map(Some),
        }
    }
}
impl OwnedHostAssembly {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare(common: Arc<EventBusState>, writer: &WriterBudget) -> Result<Self> {
        let funder = writer.prepare_source_funder()?;
        let events = event_bus::OwnedEventBusHandle::try_new_for_writer(common, writer, &funder)?;
        let accountant = writer.accountant();
        Ok(Self {
            events,
            funder,
            accountant,
        })
    }
}

impl DatabaseTransactionHost {
    /// Assemble one restricted host around the actual persistent applicator.
    /// Selection waits for the remaining original producers to migrate; this
    /// constructor performs no fallback or second-manager construction.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_owned_persistent(
        persistent: PersistentCompositeStore,
        assembly: OwnedHostAssembly,
        persistence: Arc<RedbPersistence>,
        root: crate::persistence::WriterSnapshotHandle,
        next_tx: TxId,
        next_lsn: Lsn,
        committed_watermark: TxId,
    ) -> Result<Self> {
        let OwnedHostAssembly {
            events,
            funder,
            accountant,
        } = assembly;
        let persistent = crate::persistent_store::OwnedPersistentCompositeStore::try_new(
            persistent,
            events.clone(),
        )?;
        let prepared_store = OwnedPersistentDynStore::prepare(persistent, &funder)?;
        // These moves cannot fail. Declare the debit before its payload so
        // reverse local cleanup also destroys an unconsumed Box first.
        let store_control = prepared_store.control;
        let store = prepared_store.store;
        let store_address = prepared_store.address;
        let lower = contextdb_tx::manager::EngineTransactions::try_new_eager_with_native_directory(
            store,
            next_tx,
            next_lsn,
            committed_watermark,
            persistence,
            root.into_native(),
            funder.clone(),
        )?
        .with_store_control(store_control)?;
        let transactions = EngineTransactionHandle::try_new(lower, &funder)?;
        let host = EngineDatabaseTransactions::try_new(
            transactions,
            &accountant,
            events,
            Some(store_address),
            funder.clone(),
        )?;
        Ok(Self::Owned(EngineDatabaseHandle::try_new(host, &funder)?))
    }

    /// Assemble the memory host around its actual original-funded applicator.
    /// Private received adjudicators keep their separate borrowed Legacy host.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_owned_memory(
        composite: CompositeStore,
        assembly: OwnedHostAssembly,
    ) -> Result<Self> {
        let OwnedHostAssembly {
            events,
            funder,
            accountant,
        } = assembly;
        let prepared = OwnedMemoryDynStore::prepare(composite, events.clone(), &funder)?;
        let store_control = prepared.control;
        let store = prepared.store;
        let lower =
            contextdb_tx::manager::EngineTransactions::try_new_eager_funded(store, funder.clone())?
                .with_store_control(store_control)?;
        let transactions = EngineTransactionHandle::try_new(lower, &funder)?;
        let host = EngineDatabaseTransactions::try_new(
            transactions,
            &accountant,
            events,
            None,
            funder.clone(),
        )?;
        Ok(Self::Owned(EngineDatabaseHandle::try_new(host, &funder)?))
    }

    pub(super) fn legacy(transactions: Arc<TransactionManager<DynStore>>) -> Self {
        Self::Legacy(transactions)
    }

    pub(super) fn capture_published_read(
        &self,
    ) -> contextdb_tx::native_capture::EnginePublishedRead {
        match self {
            Self::Legacy(transactions) => transactions.capture_published_read().into(),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions
                    .with_transactions(|transactions| transactions.capture_published_read())
            }),
        }
    }

    pub(super) fn published_read_snapshot(&self) -> (SnapshotId, Lsn) {
        match self {
            Self::Legacy(transactions) => transactions.published_read_snapshot(),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions
                    .with_transactions(|transactions| transactions.published_read_snapshot())
            }),
        }
    }

    pub(super) fn snapshot(&self) -> SnapshotId {
        match self {
            Self::Legacy(transactions) => transactions.snapshot(),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions
                    .with_transactions(|transactions| transactions.snapshot())
            }),
        }
    }

    pub(super) fn current_lsn(&self) -> Lsn {
        match self {
            Self::Legacy(transactions) => transactions.current_lsn(),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions
                    .with_transactions(|transactions| transactions.current_lsn())
            }),
        }
    }

    pub(super) fn current_tx_max(&self) -> TxId {
        match self {
            Self::Legacy(transactions) => transactions.current_tx_max(),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions
                    .with_transactions(|transactions| transactions.current_tx_max())
            }),
        }
    }
    pub(super) fn peek_next_tx(&self) -> TxId {
        match self {
            Self::Legacy(manager) => manager.peek_next_tx(),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions
                    .with_transactions(|transactions| transactions.peek_next_tx())
            }),
        }
    }

    pub(super) fn store(&self) -> &DynStore {
        match self {
            Self::Legacy(manager) => manager.store(),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions
                    .with_transactions(|transactions| transactions.store())
            }),
        }
    }

    pub(super) fn release_publication_handle(&self) {
        match self {
            Self::Legacy(manager) => manager.release_publication_handle(),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions
                    .with_transactions(|transactions| transactions.release_publication_handle())
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn read_commit_directory_cursor(
        &self,
        lower: std::ops::Bound<Lsn>,
        upper: std::ops::Bound<Lsn>,
        context: &contextdb_tx::ReadCommitContext<'_>,
    ) -> Result<Box<dyn contextdb_tx::ReadCommitCursor>> {
        match self {
            Self::Legacy(manager) => manager.read_commit_directory_cursor(lower, upper, context),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.read_commit_directory_cursor(lower, upper, context)
                })
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn try_commit_index_prefix(&self, up_to: Lsn) -> Result<BTreeMap<Lsn, TxId>> {
        match self {
            Self::Legacy(manager) => manager.try_commit_index_prefix(up_to),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions
                    .with_transactions(|transactions| transactions.try_commit_index_prefix(up_to))
            }),
        }
    }

    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from received_schema_atomic_retry_tests.rs"
        )
    )]
    pub(super) fn commit_index_snapshot(&self) -> BTreeMap<Lsn, TxId> {
        match self {
            Self::Legacy(manager) => manager.commit_index_snapshot(),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions
                    .with_transactions(|transactions| transactions.commit_index_snapshot())
            }),
        }
    }

    pub(super) fn commit_index_len(&self) -> usize {
        match self {
            Self::Legacy(manager) => manager.commit_index_len(),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions
                    .with_transactions(|transactions| transactions.commit_index_len())
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from manager.rs"
        )
    )]
    pub(super) fn try_commit_index_census(&self) -> Result<(usize, Lsn)> {
        match self {
            Self::Legacy(manager) => manager.try_commit_index_census(),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions
                    .with_transactions(|transactions| transactions.try_commit_index_census())
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from manager.rs"
        )
    )]
    pub(super) fn try_commit_index_census_with_context(
        &self,
        context: &contextdb_tx::ReadCommitContext<'_>,
    ) -> Result<(usize, Lsn)> {
        match self {
            Self::Legacy(manager) => manager.try_commit_index_census_with_context(context),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.try_commit_index_census_with_context(context)
                })
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn try_commit_index_contains(&self, lsn: Lsn) -> Result<bool> {
        match self {
            Self::Legacy(manager) => manager.try_commit_index_contains(lsn),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions
                    .with_transactions(|transactions| transactions.try_commit_index_contains(lsn))
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from manager.rs"
        )
    )]
    pub(super) fn try_commit_index_contains_with_context(
        &self,
        lsn: Lsn,
        context: &contextdb_tx::ReadCommitContext<'_>,
    ) -> Result<bool> {
        match self {
            Self::Legacy(manager) => manager.try_commit_index_contains_with_context(lsn, context),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.try_commit_index_contains_with_context(lsn, context)
                })
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn try_commit_index_floor(
        &self,
        upper: std::ops::Bound<Lsn>,
    ) -> Result<Option<(Lsn, TxId)>> {
        match self {
            Self::Legacy(manager) => manager.try_commit_index_floor(upper),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions
                    .with_transactions(|transactions| transactions.try_commit_index_floor(upper))
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from manager.rs"
        )
    )]
    pub(super) fn try_commit_index_floor_with_context(
        &self,
        upper: std::ops::Bound<Lsn>,
        context: &contextdb_tx::ReadCommitContext<'_>,
    ) -> Result<Option<(Lsn, TxId)>> {
        match self {
            Self::Legacy(manager) => manager.try_commit_index_floor_with_context(upper, context),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.try_commit_index_floor_with_context(upper, context)
                })
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from manager.rs"
        )
    )]
    pub(super) fn try_commit_index_get_with_context(
        &self,
        lsn: Lsn,
        context: &contextdb_tx::ReadCommitContext<'_>,
    ) -> Result<Option<TxId>> {
        match self {
            Self::Legacy(manager) => manager.try_commit_index_get_with_context(lsn, context),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.try_commit_index_get_with_context(lsn, context)
                })
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn try_snapshot_at_lsn_with_context(
        &self,
        lsn: Lsn,
        context: &contextdb_tx::ReadCommitContext<'_>,
    ) -> Result<SnapshotId> {
        match self {
            Self::Legacy(manager) => manager.try_snapshot_at_lsn_with_context(lsn, context),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.try_snapshot_at_lsn_with_context(lsn, context)
                })
            }),
        }
    }

    pub(super) fn snapshot_at_lsn(&self, lsn: Lsn) -> SnapshotId {
        match self {
            Self::Legacy(manager) => manager.snapshot_at_lsn(lsn),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions
                    .with_transactions(|transactions| transactions.snapshot_at_lsn(lsn))
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn try_remove_commit_index_entries(&self, lsns: &[Lsn]) -> Result<u64> {
        match self {
            Self::Legacy(manager) => manager.try_remove_commit_index_entries(lsns),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.try_remove_commit_index_entries(lsns)
                })
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn allocate_ddl_lsn<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(Lsn) -> Result<R>,
    {
        match self {
            Self::Legacy(manager) => manager.allocate_ddl_lsn(f),
            Self::Owned(handle) => handle.with_host(|host| {
                host.with_metadata_publication_scope(|| {
                    host.transactions
                        .with_transactions(|transactions| transactions.allocate_ddl_lsn(f))
                })
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn allocate_ddl_lsn_maybe<F, R>(&self, f: F) -> Result<Option<R>>
    where
        F: FnOnce(Lsn) -> Result<Option<R>>,
    {
        match self {
            Self::Legacy(manager) => manager.allocate_ddl_lsn_maybe(f),
            Self::Owned(handle) => handle.with_host(|host| {
                host.with_metadata_publication_scope(|| {
                    host.transactions
                        .with_transactions(|transactions| transactions.allocate_ddl_lsn_maybe(f))
                })
            }),
        }
    }

    pub(super) fn with_commit_lock<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        match self {
            Self::Legacy(manager) => manager.with_commit_lock(f),
            Self::Owned(handle) => handle.with_host(|host| {
                host.with_metadata_publication_scope(|| {
                    host.transactions
                        .with_transactions(|transactions| transactions.with_commit_lock(f))
                })
            }),
        }
    }

    pub(super) fn try_with_commit_lock_for<F, R>(
        &self,
        duration: std::time::Duration,
        f: F,
    ) -> Option<R>
    where
        F: FnOnce() -> R,
    {
        match self {
            Self::Legacy(manager) => manager.try_with_commit_lock_for(duration, f),
            Self::Owned(handle) => handle.with_host(|host| {
                host.with_metadata_publication_scope(|| {
                    host.transactions.with_transactions(|transactions| {
                        transactions.try_with_commit_lock_for(duration, f)
                    })
                })
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_maintenance_publication<F, R>(&self, f: F) -> Result<R>
    where
        F: for<'guard> FnOnce(
            &mut contextdb_tx::manager::MaintenancePublication<'guard, DynStore>,
        ) -> Result<R>,
    {
        match self {
            Self::Legacy(manager) => manager.with_maintenance_publication(f),
            Self::Owned(handle) => handle.with_host(|host| {
                host.with_metadata_publication_scope(|| {
                    host.transactions.with_transactions(|transactions| {
                        transactions.with_maintenance_publication(f)
                    })
                })
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_original_maintenance_publication<R>(
        &self,
        original: &OwnedMemoryReservation,
        f: impl for<'guard> FnOnce(
            &mut contextdb_tx::manager::MaintenancePublication<'guard, DynStore>,
        ) -> crate::funded_error::OriginalResult<R>,
    ) -> crate::funded_error::OriginalResult<R> {
        let diagnostic = crate::funded_error::OriginalError::prepare_route(original)?;
        self.with_maintenance_publication(|publication| {
            let result = f(publication);
            // The lower Error-only adapter checks success before it destroys
            // the guard. A private refusal must abandon the same unused
            // reservation BEFORE returning it as an opaque success payload.
            // No Error/diagnosis is extracted or replaced with a marker.
            if result.is_err() {
                publication.abandon();
            }
            Ok(result)
        })
        .map_err(|error| diagnostic.error(error))?
    }

    pub(super) fn commit_boundary_held_by_current_thread(&self) -> bool {
        match self {
            Self::Legacy(manager) => manager.commit_boundary_held_by_current_thread(),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.commit_boundary_held_by_current_thread()
                })
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_deferred_metadata_write<R>(
        &self,
        write: impl FnOnce() -> Result<R>,
    ) -> Result<R> {
        let publish = || {
            let reservation = self.reserve_deferred_metadata_publication()?;
            let result = write()?;
            self.owe_metadata_publication(reservation);
            Ok(result)
        };
        match self {
            Self::Legacy(_) => publish(),
            Self::Owned(handle) => handle.with_host(|host| {
                host.with_metadata_publication_scope(|| {
                    if self.commit_boundary_held_by_current_thread() {
                        publish()
                    } else {
                        // A native deferred Box must be completed by this
                        // operation's commit boundary before its stack owner
                        // ends. Inherit an existing guard; otherwise take the
                        // same guard used by every other metadata publication.
                        host.transactions.with_transactions(|transactions| {
                            transactions.with_commit_lock(publish)
                        })
                    }
                })
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Preserve the existing public diagnostic through postcommit publication."
    )]
    pub(super) fn with_deferred_metadata_write_after_commit<R>(
        &self,
        committed: &std::cell::Cell<bool>,
        write: impl FnOnce() -> Result<R>,
    ) -> Result<R> {
        let publish = || {
            let reservation = self.reserve_deferred_metadata_publication()?;
            let result = write();
            // A postcommit error cannot withdraw the already durable schema.
            // Complete the existing prepaid publication on either outcome.
            if committed.get() {
                self.owe_metadata_publication(reservation);
            }
            result
        };
        match self {
            Self::Legacy(_) => publish(),
            Self::Owned(handle) => handle.with_host(|host| {
                host.with_metadata_publication_scope(|| {
                    if self.commit_boundary_held_by_current_thread() {
                        publish()
                    } else {
                        // A native deferred Box must be completed by this
                        // operation's commit boundary before its stack owner
                        // ends. Inherit an existing guard; otherwise take the
                        // same guard used by every other metadata publication.
                        host.transactions.with_transactions(|transactions| {
                            transactions.with_commit_lock(publish)
                        })
                    }
                })
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reserve_deferred_metadata_publication(
        &self,
    ) -> Result<Option<Box<dyn std::any::Any + Send>>> {
        match self {
            Self::Legacy(manager) => manager.reserve_deferred_metadata_publication(),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.reserve_deferred_metadata_publication()
                })
            }),
        }
    }

    pub(super) fn owe_metadata_publication(
        &self,
        reservation: Option<Box<dyn std::any::Any + Send>>,
    ) {
        match self {
            Self::Legacy(manager) => manager.owe_metadata_publication(reservation),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.owe_metadata_publication(reservation)
                })
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn transaction_read(
        &self,
        tx: TxId,
    ) -> Result<contextdb_tx::native_capture::EnginePublishedRead> {
        match self {
            Self::Legacy(manager) => manager.transaction_read(tx).map(Into::into),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions
                    .with_transactions(|transactions| transactions.transaction_read(tx))
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn row_shadows_committed(&self, tx: TxId, table: &str, row: RowId) -> Result<bool> {
        match self {
            Self::Legacy(manager) => manager.row_shadows_committed(tx, table, row),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.row_shadows_committed(tx, table, row)
                })
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn advance_for_sync(&self, tx: TxId, table: &str, incoming: TxId) -> Result<()> {
        match self {
            Self::Legacy(manager) => manager.advance_for_sync(tx, table, incoming),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.advance_for_sync(tx, table, incoming)
                })
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn rollback_empty_without_commit_lock(&self, tx: TxId) -> Result<()> {
        match self {
            Self::Legacy(manager) => manager.rollback_empty_without_commit_lock(tx),
            // An empty source may still own metadata. The existing paired
            // empty terminal validates and unlinks both under the active-map
            // guard without reacquiring a commit lock held by this caller.
            Self::Owned(handle) => {
                handle.with_host(|host| host.rollback_empty_terminal(tx).map(drop))
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn try_begin(&self) -> Result<TxId> {
        // A transaction begin is a boundary: its admissions check the process
        // ceiling as it stands now.
        crate::memory_accounting::observe_inherited_process_ceiling();
        match self {
            Self::Legacy(manager) => manager.try_begin(),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions
                    .with_transactions(|transactions| transactions.try_begin())
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_write_set<F, R>(&self, tx: TxId, f: F) -> Result<R>
    where
        F: FnOnce(&mut WriteSet) -> R,
    {
        match self {
            Self::Legacy(manager) => manager.with_write_set(tx, f),
            Self::Owned(_) => Err(Self::producer_gap("with_write_set")),
        }
    }

    /// Move ordinary config bodies on the legacy host; the owning host copies
    /// the borrowed input through its genuine source funder before publication.
    /// The input remains live until the prepared target has its own custody.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn append_config_writes<I>(&self, tx: TxId, writes: I) -> Result<()>
    where
        I: AsRef<[(String, Vec<u8>)]> + IntoIterator<Item = (String, Vec<u8>)>,
    {
        match self {
            Self::Legacy(manager) => manager.with_write_set(tx, |ws| {
                ws.config_writes.extend(writes);
            }),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.with_engine(tx, |guard| {
                        let batch = guard.copy_aux(
                            contextdb_tx::staged_source::AuxBorrow::ConfigWrites(writes.as_ref()),
                        )?;
                        let prepared = guard.prepare_aux_splice(
                            guard.read().config_writes.len(),
                            &[],
                            batch,
                        )?;
                        guard.publish_tx(prepared);
                        Ok(())
                    })
                })
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_ordinary_read<R>(
        &self,
        tx: TxId,
        f: impl for<'w> FnOnce(&'w WriteSet) -> R,
    ) -> Result<R> {
        match self {
            Self::Legacy(manager) => manager.with_write_set(tx, |ws| f(ws)),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.with_engine(tx, |guard| Ok(guard.with_legacy(f)))
                })
            }),
        }
    }

    /// Scalar changes use the lower host's prepared mutation so its original
    /// checkpoint journal observes the same update as the ordinary image.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn update_flags(
        &self,
        tx: TxId,
        update: impl FnOnce(&mut contextdb_tx::manager::EngineFlags),
    ) -> Result<()> {
        match self {
            Self::Legacy(manager) => manager.with_write_set(tx, |ws| {
                let mut flags = contextdb_tx::manager::EngineFlags {
                    commit_lsn: ws.commit_lsn,
                    finalized_tx: ws.finalized_tx,
                    requires_commit_lsn: ws.requires_commit_lsn,
                    visibility_floor: ws.visibility_floor,
                    propagation_in_progress: ws.propagation_in_progress,
                };
                update(&mut flags);
                ws.commit_lsn = flags.commit_lsn;
                ws.finalized_tx = flags.finalized_tx;
                ws.requires_commit_lsn = flags.requires_commit_lsn;
                ws.visibility_floor = flags.visibility_floor;
                ws.propagation_in_progress = flags.propagation_in_progress;
            }),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.with_engine(tx, |guard| {
                        let mut flags = {
                            let read = guard.read();
                            contextdb_tx::manager::EngineFlags {
                                commit_lsn: *read.commit_lsn,
                                finalized_tx: *read.finalized_tx,
                                requires_commit_lsn: *read.requires_commit_lsn,
                                visibility_floor: *read.visibility_floor,
                                propagation_in_progress: *read.propagation_in_progress,
                            }
                        };
                        update(&mut flags);
                        let prepared = guard.prepare_flags(flags)?;
                        guard.publish_tx(prepared);
                        Ok(())
                    })
                })
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn cloned_write_set(&self, tx: TxId) -> Result<WriteSet> {
        match self {
            Self::Legacy(manager) => manager.cloned_write_set(tx),
            Self::Owned(_) => Err(Self::producer_gap("cloned_write_set")),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn rollback_write_set(&self, tx: TxId) -> Result<WriteSet> {
        match self {
            Self::Legacy(manager) => manager.rollback_write_set(tx),
            Self::Owned(_) => Err(Self::producer_gap("rollback_write_set")),
        }
    }

    pub(super) fn drain_active_write_sets(&self) -> Vec<WriteSet> {
        match self {
            Self::Legacy(manager) => manager.drain_active_write_sets(),
            Self::Owned(_) => {
                panic!("owned transaction host requires migrated drain_active_write_sets caller")
            }
        }
    }

    pub(super) fn with_active_write_sets<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut HashMap<TxId, WriteSet>) -> R,
    {
        match self {
            Self::Legacy(manager) => manager.with_active_write_sets(f),
            Self::Owned(_) => {
                panic!("owned transaction host requires migrated with_active_write_sets caller")
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn commit_with_reserved_lsn_callback_mut<F, G>(
        &self,
        tx: TxId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        before_commit: F,
        prepare: G,
    ) -> std::result::Result<(Lsn, WriteSet), contextdb_tx::manager::CommitFailure>
    where
        F: FnOnce(Lsn) -> Result<()>,
        G: FnOnce(&mut WriteSet) -> Result<()>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        match self {
            Self::Legacy(manager) => manager.commit_with_reserved_lsn_callback_mut(
                tx,
                read_memory,
                before_commit,
                prepare,
            ),
            Self::Owned(_) => Err(contextdb_tx::manager::CommitFailure {
                error: Self::producer_gap("commit_with_reserved_lsn_callback_mut"),
                write_set: None,
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn commit_with_lsn_precanonicalized_active_prepare_and_applied_mut<F, G>(
        &self,
        tx: TxId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        requires_commit: impl FnOnce() -> Result<bool>,
        active_prepare: F,
        prepare: G,
        after_apply: impl FnOnce(Lsn, &WriteSet),
    ) -> std::result::Result<(Lsn, WriteSet), contextdb_tx::manager::CommitFailure>
    where
        F: FnOnce(Lsn, TxId) -> Result<()>,
        G: FnOnce(&mut WriteSet) -> Result<()>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        match self {
            Self::Legacy(manager) => manager
                .commit_with_lsn_precanonicalized_active_prepare_and_applied_mut(
                    tx,
                    read_memory,
                    requires_commit,
                    active_prepare,
                    prepare,
                    after_apply,
                ),
            Self::Owned(_) => Err(contextdb_tx::manager::CommitFailure {
                error: Self::producer_gap(
                    "commit_with_lsn_precanonicalized_active_prepare_and_applied_mut",
                ),
                write_set: None,
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn commit_received_interval<F, G, H>(
        &self,
        tx: TxId,
        positions: u64,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        active_prepare: F,
        prepare_legacy: G,
        prepare_owned: H,
        after_apply: impl FnOnce(Lsn, &WriteSet),
    ) -> std::result::Result<(Lsn, ReceivedCommitTerminal), ReceivedCommitFailure>
    where
        F: FnOnce(Option<&DatabaseCommitScope<'_, '_, '_>>, Lsn, Lsn, TxId) -> Result<()>,
        G: FnOnce(&mut WriteSet) -> Result<()>,
        H: FnOnce(
            &EngineDatabaseTransactions,
            &mut contextdb_tx::manager::EngineGuard<'_>,
        ) -> Result<()>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        match self {
            Self::Legacy(manager) => manager
                .commit_with_private_lsn_interval_active_prepare_and_applied_mut(
                    tx,
                    positions,
                    read_memory,
                    || Ok(false),
                    |start, end, receiver| active_prepare(None, start, end, receiver),
                    prepare_legacy,
                    after_apply,
                )
                .map(|(lsn, ws)| (lsn, ReceivedCommitTerminal::Legacy(ws)))
                .map_err(ReceivedCommitFailure::Legacy),
            Self::Owned(handle) => handle.with_host(|host| {
                host.commit_interval_terminal_scoped(
                    tx,
                    positions,
                    read_memory,
                    || Ok(false),
                    |scope, start, end, receiver| {
                        active_prepare(Some(scope), start, end, receiver)
                            .map_err(TemporaryFailure::from_error)
                    },
                    |guard, _metadata, _identity| prepare_owned(host, guard),
                    after_apply,
                )
                .map(|(lsn, pair)| (lsn, ReceivedCommitTerminal::Owned(pair)))
                .map_err(ReceivedCommitFailure::Owned)
            }),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_ordered_staged_cursor<F, R>(
        &self,
        tx: TxId,
        table: &str,
        index: &str,
        bounds: contextdb_tx::ordered_staged::OrderedStagedBounds<'_>,
        reverse: bool,
        f: F,
    ) -> Result<R>
    where
        F: FnOnce(Option<contextdb_tx::ordered_staged::OrderedStagedCursor<'_>>, &WriteSet) -> R,
    {
        match self {
            Self::Legacy(manager) => {
                manager.with_ordered_staged_cursor(tx, table, index, bounds, reverse, f)
            }
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.with_engine(tx, |guard| {
                        Ok(guard.with_legacy(|ws| {
                            f(
                                ws.ordered_staged_rows
                                    .cursor(table, index, bounds.0, bounds.1, reverse),
                                ws,
                            )
                        }))
                    })
                })
            }),
        }
    }

    pub(super) fn producer_gap(operation: &str) -> Error {
        Error::InvalidStateTransition(format!(
            "owned transaction host requires a migrated {operation} producer"
        ))
    }

    pub(super) fn begin(&self) -> TxId {
        match self {
            Self::Legacy(manager) => {
                crate::memory_accounting::observe_inherited_process_ceiling();
                manager.begin()
            }
            Self::Owned(_) => self
                .try_begin()
                .expect("transaction begin must be admitted"),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn rollback(&self, tx: TxId) -> Result<()> {
        match self {
            Self::Legacy(manager) => manager.rollback(tx),
            Self::Owned(handle) => handle.with_host(|host| host.rollback_terminal(tx).map(drop)),
        }
    }

    pub(super) fn identity(&self) -> usize {
        match self {
            Self::Legacy(manager) => Arc::as_ptr(manager) as usize,
            Self::Owned(handle) => handle.identity(),
        }
    }

    pub(super) fn as_ref(&self) -> &Self {
        self
    }

    pub(super) fn downgrade(&self) -> DatabaseTransactionWeak {
        match self {
            Self::Legacy(manager) => DatabaseTransactionWeak::Legacy(Arc::downgrade(manager)),
            Self::Owned(handle) => DatabaseTransactionWeak::Owned(handle.downgrade()),
        }
    }

    pub(super) fn relational_executor(
        &self,
        store: Arc<RelationalStore>,
    ) -> MemRelationalExecutor<DynStore> {
        match self {
            Self::Legacy(manager) => MemRelationalExecutor::new(store, manager.clone()),
            Self::Owned(handle) => handle.with_host(|host| {
                MemRelationalExecutor::new_engine(store, host.transactions.clone())
            }),
        }
    }

    pub(super) fn graph_executor(&self, store: Arc<GraphStore>) -> MemGraphExecutor<DynStore> {
        match self {
            Self::Legacy(manager) => MemGraphExecutor::new(store, manager.clone()),
            Self::Owned(handle) => handle
                .with_host(|host| MemGraphExecutor::new_engine(store, host.transactions.clone())),
        }
    }

    pub(super) fn vector_executor(
        &self,
        store: Arc<VectorStore>,
        hnsw: Arc<OnceLock<RwLock<Option<HnswIndex>>>>,
        accountant: impl Into<contextdb_vector::MemoryBudgetOwner>,
    ) -> MemVectorExecutor<DynStore> {
        let accountant: contextdb_vector::MemoryBudgetOwner = accountant.into();

        match self {
            Self::Legacy(manager) => {
                MemVectorExecutor::new_with_accountant(store, manager.clone(), hnsw, accountant)
            }
            Self::Owned(handle) => handle.with_host(|host| {
                MemVectorExecutor::new_engine(store, host.transactions.clone(), hnsw, accountant)
            }),
        }
    }

    /// Borrow only immutable ordinary fields while holding this exact manager's
    /// active-map lock. Owned callers cannot obtain a bare WriteSet.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_read<R>(
        &self,
        tx: TxId,
        f: impl FnOnce(contextdb_tx::manager::EngineRead<'_>) -> R,
    ) -> Result<R> {
        match self {
            Self::Legacy(manager) => manager.with_write_set(tx, |ws| f(Self::legacy_read(ws))),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions
                    .with_transactions(|transactions| transactions.with_read(tx, f))
            }),
        }
    }

    pub(super) fn legacy_read(ws: &WriteSet) -> contextdb_tx::manager::EngineRead<'_> {
        contextdb_tx::manager::EngineRead {
            base_read: &ws.base_read,
            engine_base_read: None,
            relational_inserts: &ws.relational_inserts,
            relational_deletes: &ws.relational_deletes,
            relational_delete_predicates: &ws.relational_delete_predicates,
            adj_inserts: &ws.adj_inserts,
            adj_deletes: &ws.adj_deletes,
            row_adj_inserts: &ws.row_adj_inserts,
            row_adj_deletes: &ws.row_adj_deletes,
            vector_inserts: &ws.vector_inserts,
            vector_deletes: &ws.vector_deletes,
            vector_moves: &ws.vector_moves,
            commit_lsn: &ws.commit_lsn,
            finalized_tx: &ws.finalized_tx,
            relational_insert_source_lsns: &ws.relational_insert_source_lsns,
            relational_insert_source_kinds: &ws.relational_insert_source_kinds,
            relational_insert_admitted_bytes: &ws.relational_insert_admitted_bytes,
            sync_source_provenance_marks: &ws.sync_source_provenance_marks,
            config_writes: &ws.config_writes,
            config_deletes: &ws.config_deletes,
            config_max_u64_keys: &ws.config_max_u64_keys,
            requires_commit_lsn: &ws.requires_commit_lsn,
            visibility_floor: &ws.visibility_floor,
            propagation_in_progress: &ws.propagation_in_progress,
        }
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from commit_and_row_writes.rs"
        )
    )]
    pub(super) fn write_set_lock_is_held_for_test(&self) -> bool {
        match self {
            Self::Legacy(manager) => manager.write_set_lock_is_held_for_test(),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.write_set_lock_is_held_for_test()
                })
            }),
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_write_set_detached<F, R>(&self, tx: TxId, f: F) -> Result<R>
    where
        F: FnOnce(&mut WriteSet) -> R,
    {
        match self {
            Self::Legacy(manager) => manager.with_write_set_detached(tx, f),
            Self::Owned(_) => Err(Self::producer_gap("with_write_set_detached")),
        }
    }
    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn has_captured_commit_directory_for_test(&self) -> bool {
        match self {
            Self::Legacy(manager) => manager.has_captured_commit_directory_for_test(),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.has_captured_commit_directory_for_test()
                })
            }),
        }
    }
    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn visit_captured_read_memory_for_test(
        &self,
        visit: impl FnOnce(&contextdb_core::read_memory::ReadMemoryOwner),
    ) {
        match self {
            Self::Legacy(manager) => manager.visit_captured_read_memory_for_test(visit),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.visit_captured_read_memory_for_test(visit)
                })
            }),
        }
    }
    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn retained_commit_directory_bytes_for_test(&self) -> usize {
        match self {
            Self::Legacy(manager) => manager.retained_commit_directory_bytes_for_test(),
            Self::Owned(handle) => handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.retained_commit_directory_bytes_for_test()
                })
            }),
        }
    }
}
impl DatabaseTransactionWeak {
    pub(super) fn upgrade(&self) -> Option<DatabaseTransactionHost> {
        match self {
            Self::Legacy(manager) => manager.upgrade().map(DatabaseTransactionHost::Legacy),
            Self::Owned(handle) => handle.upgrade().map(DatabaseTransactionHost::Owned),
        }
    }
}
