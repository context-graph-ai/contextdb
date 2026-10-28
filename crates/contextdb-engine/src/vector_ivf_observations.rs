//! Borrowed observations of authenticated clustered storage operations.
pub(crate) mod disk;
pub use crate::persistence::vector_ivf::NativeIvfPhysicalDiskOwnerForTest;
use contextdb_core::{Error, Lsn, Result, VectorIndexRef, VectorPartitionKey};
use contextdb_vector::ivf::{
    RowIdentity,
    storage::{BuildState, Manifest, PageKey, PageKind},
};
pub use disk::{
    NativeIvfBuildWriteClassForTest, NativeIvfCeilingAtAdmissionForTest,
    NativeIvfDiskBoundaryForTest, NativeIvfDiskEventForTest, NativeIvfDiskObserverForTest,
    NativeIvfReusableSpaceWithheldForTest, choose_native_ivf_ceiling_for_test,
    with_native_ivf_disk_observer_for_test, withhold_native_ivf_reusable_space_for_test,
};
use std::{
    alloc::Layout,
    cell::{Cell, RefCell},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

pub use crate::persistence::vector_ivf::{NativeIvfReadForTest, NativeIvfWriteForTest};
pub struct NativeIvfServingDescriptorForTest {
    pub bytes: [u8; 200],
    pub manifest: Manifest,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeIvfObjectInstance(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeIvfObjectKind {
    Scope,
    SealWorkspace,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeIvfOperationPrefixStage {
    Begin,
    AfterDrain,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeIvfGlobalPendingStage {
    SessionBegin,
    BeforeFinalBinding,
}
#[derive(Clone, Copy, Debug)]
pub enum NativeIvfSourceInstance {
    Read {
        admitted_object: u64,
        snapshot: redb::StorageSnapshotIdentity,
    },
    Write {
        admitted_session: u64,
        outer_write_object: usize,
    },
}
#[derive(Clone, Copy, Debug)]
pub enum NativeIvfDriveKind {
    RowApply,
    Maintenance,
    AbortingStorage,
}
#[derive(Clone, Copy, Debug)]
pub struct NativeIvfScopeRef<'a> {
    pub index: &'a VectorIndexRef,
    pub tuple: &'a VectorPartitionKey,
    pub table_generation: u64,
    pub column_incarnation: u64,
    pub snapshot_lsn: Lsn,
    pub row_commit_lsn: Option<Lsn>,
}
#[derive(Clone, Copy, Debug)]
pub struct NativeIvfOperationRef<'a> {
    pub scope: NativeIvfScopeRef<'a>,
    pub operation: Option<u64>,
    pub source_instance: NativeIvfSourceInstance,
    pub storage: u64,
    pub generation: u64,
}
#[derive(Clone, Copy, Debug)]
pub enum NativeIvfWindowRole {
    NativeValue,
    PreviousPayload,
    RelatedMetadata,
}
#[derive(Clone, Copy, Debug)]
pub struct NativeIvfWindowRange {
    pub role: NativeIvfWindowRole,
    pub offset: usize,
    pub layout: Layout,
}
#[derive(Clone, Copy, Debug)]
pub struct NativeIvfCapturedRootRef {
    pub snapshot: redb::StorageSnapshotIdentity,
    pub scopes_fingerprint: Option<[u8; 32]>,
    pub configuration_vector_binding: [u8; 32],
}
#[derive(Clone, Copy, Debug)]
pub struct NativeIvfCoverageRef<'a> {
    pub digest: &'a [u8; 32],
    pub entries: u64,
    pub live_entries: u64,
    pub frontier: Lsn,
}
#[derive(Clone, Copy, Debug)]
pub struct NativeIvfFamilyRef<'a> {
    pub storage: u64,
    pub generation: u64,
    pub present: u16,
    pub roots: &'a [[u8; 32]; 16],
    pub frozen_policy_revision: u64,
    pub frozen_frontier: Lsn,
    pub frozen_source_identity: &'a [u8; 32],
    pub frozen_entries: u64,
    pub frozen_live_entries: u64,
    pub current_commit_frontier: Lsn,
    pub manifest: Option<Manifest>,
    pub reservoir_samples: Option<u64>,
}
#[derive(Clone, Copy, Debug)]
pub struct NativeIvfSourceCursorRef<'a> {
    pub canonical: &'a [u8; 32],
    pub membership: &'a [u8; 24],
}
#[derive(Clone, Copy, Debug)]
pub struct NativeIvfStoredScopeRef<'a> {
    pub scope_key: &'a [u8; 64],
    pub index_digest: &'a [u8; 32],
    pub tuple_digest: &'a [u8; 32],
    pub table_generation: u64,
    pub column_incarnation: u64,
    pub families: [Option<NativeIvfFamilyRef<'a>>; 3],
    pub serving: Option<Manifest>,
    pub build: Option<BuildState>,
    pub source_cursor: Option<NativeIvfSourceCursorRef<'a>>,
    pub current: NativeIvfCoverageRef<'a>,
    pub canonical_complete: bool,
}
#[derive(Clone, Copy)]
pub enum NativeIvfPageReadOutcome<'a> {
    Absent,
    Authenticated,
    Failed(&'a Error),
}
#[derive(Clone, Copy)]
pub enum NativeIvfSealEvent<'a> {
    ReservationDebited {
        invoice: u64,
        original_owner: usize,
        bytes: usize,
        operation: &'static str,
    },
    ReservationReleased {
        invoice: u64,
        original_owner: usize,
        bytes: usize,
        operation: &'static str,
    },

    QueryRejected {
        index: &'a VectorIndexRef,
        tuple: &'a VectorPartitionKey,
        error: &'a Error,
    },
    LegacyEntriesPrepared {
        scope: NativeIvfScopeRef<'a>,
        entries: u64,
        total: u64,
    },
    BuildStarted {
        scope: NativeIvfScopeRef<'a>,
    },
    QueryExecuted {
        index: &'a VectorIndexRef,
        tuple: &'a VectorPartitionKey,
        generation: u64,
        indexed: bool,
        served_depth: Option<u32>,
        objective_met: bool,
        work: &'a contextdb_vector::ivf::Work,
    },
    CaptureBegan {
        capture: u64,
        attempted: u64,
        delivered: u64,
        producers_open: u64,
        overflow: bool,
        reentry: bool,
    },
    CaptureEnded {
        capture: u64,
        attempted: u64,
        delivered: u64,
        producers_open: u64,
        overflow: bool,
        reentry: bool,
    },
    ObjectAdmitted {
        instance: NativeIvfObjectInstance,
        kind: NativeIvfObjectKind,
        layout: Layout,
        original_owner: usize,
        adapter_owner: usize,
        scope_key: Option<[u8; 64]>,
        windows: Option<&'a [NativeIvfWindowRange; 3]>,
    },
    ObjectReleased {
        instance: NativeIvfObjectInstance,
        kind: NativeIvfObjectKind,
        layout: Layout,
        original_owner: usize,
        adapter_owner: usize,
        scope_key: Option<[u8; 64]>,
    },
    OperationPrefixChecked {
        source_instance: NativeIvfSourceInstance,
        scope_key: &'a [u8; 64],
        operation: u64,
        prefix: &'a [u8; 72],
        empty: bool,
        stage: NativeIvfOperationPrefixStage,
    },
    GlobalPendingChecked {
        source_instance: NativeIvfSourceInstance,
        empty: bool,
        stage: NativeIvfGlobalPendingStage,
    },
    ScopeOpened {
        scope: NativeIvfScopeRef<'a>,
        source_instance: NativeIvfSourceInstance,
        captured_root: NativeIvfCapturedRootRef,
        stored: NativeIvfStoredScopeRef<'a>,
    },
    MarkerInserted {
        operation: NativeIvfOperationRef<'a>,
        key: PageKey,
        newly_inserted: bool,
    },
    MarkerRemoved {
        operation: NativeIvfOperationRef<'a>,
        key: PageKey,
    },
    PayloadWritten {
        operation: NativeIvfOperationRef<'a>,
        key: PageKey,
    },
    HeaderSealed {
        operation: NativeIvfOperationRef<'a>,
        key: PageKey,
        header: &'a [u8; 192],
        payload: &'a [u8; 3904],
        first_slot: u64,
        record_count: u32,
    },
    PageOpened {
        operation: NativeIvfOperationRef<'a>,
        key: PageKey,
        header: &'a [u8; 192],
        payload: &'a [u8; 3904],
    },
    PageReadAttempt {
        operation: NativeIvfOperationRef<'a>,
        source_instance: NativeIvfSourceInstance,
        attempt: u64,
        key: PageKey,
    },
    PageReadOutcome {
        operation: NativeIvfOperationRef<'a>,
        source_instance: NativeIvfSourceInstance,
        attempt: u64,
        key: PageKey,
        outcome: NativeIvfPageReadOutcome<'a>,
    },
    KindHandlesClosed {
        operation: NativeIvfOperationRef<'a>,
        kind: PageKind,
    },
    KindFingerprintCompared {
        operation: NativeIvfOperationRef<'a>,
        kind: PageKind,
        expected: Option<[u8; 32]>,
        actual: Option<[u8; 32]>,
    },
    VerificationMaskReset {
        operation: NativeIvfOperationRef<'a>,
        previous: u16,
        current: u16,
    },
    VerificationMaskUsed {
        operation: NativeIvfOperationRef<'a>,
        kind: PageKind,
        mask: u16,
    },
    FamilyFingerprintCaptured {
        operation: NativeIvfOperationRef<'a>,
        kind: PageKind,
        root: [u8; 32],
    },
    ScopeWritten {
        scope: NativeIvfScopeRef<'a>,
        operation: Option<u64>,
        stored: NativeIvfStoredScopeRef<'a>,
    },
    ConfigurationBound {
        root: [u8; 32],
    },
    OuterCommitted {
        actual_commit_lsn: Lsn,
    },
    OuterAborted,
    CoverageApplied {
        operation: NativeIvfOperationRef<'a>,
        authoritative: NativeIvfCoverageRef<'a>,
        applied: NativeIvfCoverageRef<'a>,
    },
    ServingDescriptorLoaded {
        scope: NativeIvfScopeRef<'a>,
        stored: &'a [u8; 200],
        manifest: Manifest,
    },
    QueryManifestSelected {
        scope: NativeIvfScopeRef<'a>,
        descriptor: &'a [u8; 200],
        manifest: Manifest,
    },
    RootControlLoaded {
        scope: NativeIvfScopeRef<'a>,
        stored_bytes: usize,
    },
    SourceRowIngested {
        scope: NativeIvfScopeRef<'a>,
        identity: RowIdentity,
    },
    JournalInitialized {
        scope: NativeIvfScopeRef<'a>,
        identity: RowIdentity,
        deleted: bool,
    },
}
#[derive(Clone, Copy)]
pub enum NativeIvfFaultBoundary<'a> {
    BeforeMarkerWrite {
        operation: NativeIvfOperationRef<'a>,
        key: PageKey,
    },
    BeforePayloadWrite {
        operation: NativeIvfOperationRef<'a>,
        key: PageKey,
    },
    BeforeHeaderSeal {
        operation: NativeIvfOperationRef<'a>,
        key: PageKey,
    },
    BeforeKindFingerprint {
        operation: NativeIvfOperationRef<'a>,
        kind: PageKind,
    },
    BeforeScopeWrite {
        scope: NativeIvfScopeRef<'a>,
        operation: Option<u64>,
    },
    BeforeConfigurationBind,
    BeforeOuterCommit,
    AfterAuthoritativeAndJournalStaged {
        scope: NativeIvfScopeRef<'a>,
        identity: RowIdentity,
        deleted: bool,
    },
}
pub trait NativeIvfSealObserver: Send + Sync {
    fn observe(&self, event: NativeIvfSealEvent<'_>);
    fn before_boundary(&self, boundary: NativeIvfFaultBoundary<'_>) -> Result<()>;
}
struct Capture {
    parent: Option<Arc<Capture>>,
    id: u64,
    observer: Arc<dyn NativeIvfSealObserver>,
    attempted: AtomicU64,
    delivered: AtomicU64,
    producers: AtomicU64,
    overflow: AtomicBool,
    reentry: AtomicBool,
}
thread_local! {
    static CURRENT:RefCell<Option<Arc<Capture>>> = const { RefCell::new(None) };
    static DELIVERING:Cell<bool> = const { Cell::new(false) };
}
static NEXT_ID: AtomicU64 = AtomicU64::new(1);
pub(crate) fn instance() -> Result<u64> {
    NEXT_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
        .map_err(|_| Error::Other("native vector observation identity exhausted".into()))
}
impl Capture {
    fn increment(&self, counter: &AtomicU64) -> bool {
        if counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .is_err()
        {
            self.overflow.store(true, Ordering::Relaxed);
            false
        } else {
            true
        }
    }
    fn entered(&self) -> Option<Delivery> {
        if DELIVERING.with(|active| active.replace(true)) {
            self.reentry.store(true, Ordering::Relaxed);
            None
        } else {
            Some(Delivery)
        }
    }
    fn emit(&self, event: NativeIvfSealEvent<'_>) {
        if std::thread::panicking() {
            self.overflow.store(true, Ordering::Relaxed);
            return;
        }
        if let Some(parent) = &self.parent {
            parent.emit(event);
        }

        if !self.increment(&self.attempted) {
            return;
        }
        let Some(_delivery) = self.entered() else {
            return;
        };
        self.observer.observe(event);
        self.increment(&self.delivered);
    }
    fn boundary(&self, boundary: NativeIvfFaultBoundary<'_>) -> Result<()> {
        if let Some(parent) = &self.parent {
            parent.boundary(boundary)?;
        }
        if self.overflow.load(Ordering::Relaxed) || self.reentry.load(Ordering::Relaxed) {
            return Err(Error::Other(
                "native vector observation capture is incomplete".into(),
            ));
        }
        let _delivery = self
            .entered()
            .ok_or_else(|| Error::Other("native vector observer reentry".into()))?;
        self.observer.before_boundary(boundary)
    }
    fn frame(&self, begin: bool) {
        if std::thread::panicking() {
            self.overflow.store(true, Ordering::Relaxed);
            return;
        }

        let (capture, attempted, delivered, producers_open, overflow, reentry) = (
            self.id,
            self.attempted.load(Ordering::Relaxed),
            self.delivered.load(Ordering::Relaxed),
            self.producers.load(Ordering::Relaxed),
            self.overflow.load(Ordering::Relaxed),
            self.reentry.load(Ordering::Relaxed),
        );
        if let Some(_delivery) = self.entered() {
            self.observer.observe(if begin {
                NativeIvfSealEvent::CaptureBegan {
                    capture,
                    attempted,
                    delivered,
                    producers_open,
                    overflow,
                    reentry,
                }
            } else {
                NativeIvfSealEvent::CaptureEnded {
                    capture,
                    attempted,
                    delivered,
                    producers_open,
                    overflow,
                    reentry,
                }
            });
        }
    }
}
struct Delivery;
impl Drop for Delivery {
    fn drop(&mut self) {
        DELIVERING.with(|active| active.set(false));
    }
}
/// Captured explicitly when the executing storage producer is constructed.
pub(crate) struct Producer(Option<Arc<Capture>>);
impl Producer {
    pub(crate) fn current() -> Self {
        let capture = CURRENT.with(|c| c.borrow().clone());
        let mut current = capture.as_ref();
        while let Some(c) = current {
            c.increment(&c.producers);
            current = c.parent.as_ref();
        }
        Self(capture)
    }
    pub(crate) fn emit(&self, event: NativeIvfSealEvent<'_>) {
        if let Some(c) = &self.0 {
            c.emit(event);
        }
    }
    pub(crate) fn boundary(&self, boundary: NativeIvfFaultBoundary<'_>) -> Result<()> {
        self.0.as_ref().map_or(Ok(()), |c| c.boundary(boundary))
    }
}
impl Drop for Producer {
    fn drop(&mut self) {
        let mut current = self.0.as_ref();
        while let Some(c) = current {
            c.producers.fetch_sub(1, Ordering::Relaxed);
            current = c.parent.as_ref();
        }
    }
}
pub fn with_native_ivf_seal_observer_for_test<R>(
    observer: Arc<dyn NativeIvfSealObserver>,
    run: impl FnOnce() -> R,
) -> R {
    let c = Arc::new(Capture {
        parent: CURRENT.with(|slot| slot.borrow().clone()),
        id: instance().expect("capture identity"),
        observer,
        attempted: AtomicU64::new(0),
        delivered: AtomicU64::new(0),
        producers: AtomicU64::new(0),
        overflow: AtomicBool::new(false),
        reentry: AtomicBool::new(false),
    });
    struct Installed(Option<Arc<Capture>>, Arc<Capture>);
    impl Drop for Installed {
        fn drop(&mut self) {
            CURRENT.with(|p| *p.borrow_mut() = self.0.take());
            self.1.frame(false);
        }
    }
    let _installed = Installed(CURRENT.with(|p| p.replace(Some(c.clone()))), c.clone());
    c.frame(true);
    run()
}
pub struct NativeIvfDriveCallbacks<'a> {
    pub checkpoint: &'a dyn Fn() -> Result<()>,
    pub before_distance: &'a dyn Fn(contextdb_vector::ivf::storage::DistanceKind) -> Result<()>,
}

#[derive(Clone, Copy, Debug)]
pub struct NativeIvfResolvedPolicyForTest {
    pub policy: contextdb_vector::ivf::Policy,
    pub training: contextdb_vector::ivf::TrainingPolicy,
    pub active_lists: u32,
    pub population: u64,
    pub expected_vectors: u64,
    pub build_budget_bytes: u64,
    pub codebook_budget_bytes: u64,
    pub workspace_budget_bytes: usize,
    pub codebook: contextdb_vector::ivf::CodebookAllocationLayouts,
    pub workspace: contextdb_vector::ivf::WriterWorkspaceLayout,
}

pub struct NativeIvfBuildProgressForTest {
    pub replacement_trigger: Option<&'static str>,
    pub build: Option<BuildState>,
    pub source_cursor: Option<NativeIvfSourceCursorForTest>,
    pub frozen_live_entries: u64,
    pub ingested_vectors: u64,
    pub canonical_complete: bool,
    pub aborted_builds: u64,
}
pub struct NativeIvfSourceCursorForTest {
    pub canonical: [u8; 32],
    pub membership: [u8; 24],
}
pub struct NativeIvfJournalIdentityForTest {
    pub initialized: bool,
    pub identity: Option<RowIdentity>,
    pub deleted: bool,
    pub copies: u64,
}

pub(crate) fn observe_query(
    index: &VectorIndexRef,
    tuple: &VectorPartitionKey,
    generation: u64,
    indexed: bool,
    served_depth: Option<u32>,
    objective_met: bool,
    work: &contextdb_vector::ivf::Work,
) {
    Producer::current().emit(NativeIvfSealEvent::QueryExecuted {
        index,
        tuple,
        generation,
        indexed,
        served_depth,
        objective_met,
        work,
    });
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct NativeIvfIndexTestOptions {
    pub write_buffer_cap_bytes: Option<u64>,
    pub build_batch_rows: Option<u64>,
}

pub(crate) struct NativeIvfOuterWriteObservation {
    producer: Producer,
    committed: bool,
}
impl NativeIvfOuterWriteObservation {
    pub(crate) fn begin() -> Self {
        Self {
            producer: Producer::current(),
            committed: false,
        }
    }
    pub(crate) fn before_commit(&self) -> Result<()> {
        self.producer
            .boundary(NativeIvfFaultBoundary::BeforeOuterCommit)
    }
    pub(crate) fn committed(&mut self, lsn: Lsn) {
        self.committed = true;
        self.producer.emit(NativeIvfSealEvent::OuterCommitted {
            actual_commit_lsn: lsn,
        });
    }
}
impl Drop for NativeIvfOuterWriteObservation {
    fn drop(&mut self) {
        if !self.committed {
            self.producer.emit(NativeIvfSealEvent::OuterAborted);
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeIvfScopeRootsForTest {
    pub index_digest: [u8; 32],
    pub tuple_digest: [u8; 32],
    pub snapshot_lsn: u64,
    pub table_generation: u64,
    pub column_incarnation: u64,
    pub frozen_frontier: u64,
    pub frozen_source_identity: [u8; 32],
    pub current_frontier: u64,
    pub current_digest: [u8; 32],
    pub current_entries: u64,
    pub current_live_entries: u64,
    pub expected_roots: [[u8; 32]; 16],
    pub actual_roots: [[u8; 32]; 16],
    pub present: u16,
}
pub struct NativeIvfSourceRecordForTest<'a> {
    pub key: &'a [u8],
    pub encoded: &'a [u8],
    pub live: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct NativeIvfRetainedCodebookForTest {
    pub manifest: Option<Manifest>,
    pub charged_bytes: usize,
    pub original_owner: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct NativeIvfMaintenanceStateForTest {
    pub manifest: Option<Manifest>,
    pub build: Option<BuildState>,
    pub serving_seed: Option<u64>,
    pub policy_revision: u64,
    pub training_evaluations: u64,
    pub assignment_evaluations: u64,
    pub source_live_entries: u64,
    pub legacy_entries_prepared: u64,
}
#[derive(Clone, Copy, Debug)]
pub struct NativeIvfPhysicalPageForTest<'a> {
    pub storage: u64,
    pub key: PageKey,
    pub building: bool,
    pub payload: &'a [u8; contextdb_vector::ivf::storage::PAGE_BYTES],
    pub bytes: &'a [u8],
}

/// Passive host-owned capture of the real kernel buffer at the end of each
/// native writer loan. This observer never supplies storage, policy or funding.
pub struct NativeIvfBufferObservation<'a> {
    pub original_owner: usize,
    pub index: &'a VectorIndexRef,
    pub state: contextdb_vector::ivf::BufferState,
}
type BufferObserver = Arc<dyn for<'a> Fn(NativeIvfBufferObservation<'a>) + Send + Sync>;
thread_local! {
    static BUFFER_OBSERVER: RefCell<Option<BufferObserver>> = const { RefCell::new(None) };
}
pub fn set_native_ivf_buffer_observer_for_test(observer: BufferObserver) {
    BUFFER_OBSERVER.with(|slot| slot.replace(Some(observer)));
}
pub(crate) fn observe_buffer(
    owner: usize,
    index: &VectorIndexRef,
    state: contextdb_vector::ivf::BufferState,
) {
    let observer = BUFFER_OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(observer) = observer {
        observer(NativeIvfBufferObservation {
            original_owner: owner,
            index,
            state,
        });
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeIvfWorkPoint {
    ListPageRead,
    CandidateScoring,
    BuildBatch,
    ReclaimBatch,
}
type WorkObserver = Arc<dyn Fn(NativeIvfWorkPoint) + Send + Sync>;
thread_local! {
    static WORK_OBSERVER: RefCell<Option<WorkObserver>>=const {RefCell::new(None)};
    static MAINTENANCE_CANCELLATION: RefCell<Option<contextdb_core::read_contract::OwnerReadCancellation>>=const {RefCell::new(None)};
}
pub fn with_native_ivf_work_observer_for_test<R>(
    observer: WorkObserver,
    run: impl FnOnce() -> R,
) -> R {
    struct Restore(Option<WorkObserver>);
    impl Drop for Restore {
        fn drop(&mut self) {
            WORK_OBSERVER.with(|slot| slot.replace(self.0.take()));
        }
    }
    let _restore = Restore(WORK_OBSERVER.with(|slot| slot.replace(Some(observer))));
    run()
}
pub fn with_native_ivf_maintenance_cancellation_for_test<R>(
    cancellation: &contextdb_core::read_contract::OwnerReadCancellation,
    run: impl FnOnce() -> R,
) -> R {
    struct Restore(Option<contextdb_core::read_contract::OwnerReadCancellation>);
    impl Drop for Restore {
        fn drop(&mut self) {
            MAINTENANCE_CANCELLATION.with(|slot| slot.replace(self.0.take()));
        }
    }
    let _restore =
        Restore(MAINTENANCE_CANCELLATION.with(|slot| slot.replace(Some(cancellation.clone()))));
    run()
}
pub(crate) fn work_point(point: NativeIvfWorkPoint) {
    let observer = WORK_OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(observer) = observer {
        observer(point);
    }
}
pub(crate) fn maintenance_cancel_checkpoint() -> Result<()> {
    if MAINTENANCE_CANCELLATION
        .with(|slot| slot.borrow().as_ref().is_some_and(|c| c.is_cancelled()))
    {
        return Err(Error::ReadCancelled);
    }
    Ok(())
}

/// Observes an existing admitted caller credit; it creates no second invoice.
pub(crate) struct ObservedReadCredit {
    credit: Option<contextdb_core::read_memory::ReadCredit>,
    observer: Producer,
    invoice: u64,
    owner: usize,
    bytes: usize,
    operation: &'static str,
}
pub(crate) fn observe_existing_read_credit(
    credit: contextdb_core::read_memory::ReadCredit,
    operation: &'static str,
) -> Result<ObservedReadCredit> {
    let invoice = instance()?;
    let owner = credit.original_payer_identity_for_test();
    let bytes = credit.bytes_for_test();
    let observer = Producer::current();
    observer.emit(NativeIvfSealEvent::ReservationDebited {
        invoice,
        original_owner: owner,
        bytes,
        operation,
    });
    Ok(ObservedReadCredit {
        credit: Some(credit),
        observer,
        invoice,
        owner,
        bytes,
        operation,
    })
}
impl Drop for ObservedReadCredit {
    fn drop(&mut self) {
        drop(self.credit.take());
        self.observer.emit(NativeIvfSealEvent::ReservationReleased {
            invoice: self.invoice,
            original_owner: self.owner,
            bytes: self.bytes,
            operation: self.operation,
        });
    }
}

#[derive(Clone, Copy, Debug)]
pub enum NativeIvfConstructorTargetForTest {
    Codebook { training_counts: bool },
    WriterWorkspace,
}

#[derive(Clone, Copy, Debug)]
pub struct NativeIvfVerificationForTest<'a> {
    pub window: u64,
    pub index: &'a VectorIndexRef,
    pub generation: u64,
    pub verdict: contextdb_vector::ivf::storage::Verdict,
}
type VerificationObserver = Arc<dyn for<'a> Fn(NativeIvfVerificationForTest<'a>) + Send + Sync>;
thread_local! {static VERIFICATION_OBSERVER:RefCell<Option<VerificationObserver>>=const{RefCell::new(None)};}
pub fn set_native_ivf_verification_observer_for_test(observer: VerificationObserver) {
    VERIFICATION_OBSERVER.with(|slot| slot.replace(Some(observer)));
}
pub(crate) fn observe_verification(event: NativeIvfVerificationForTest<'_>) {
    VERIFICATION_OBSERVER.with(|slot| {
        if let Some(observer) = slot.borrow().as_ref() {
            observer(event);
        }
    });
}

pub use crate::persistence::vector_ivf::qualification::{
    Depth as NativeIvfDepthQualificationForTest, Header as NativeIvfQualificationHeaderForTest,
    Prefix as NativeIvfPrefixQualificationForTest, Record as NativeIvfQualificationRecordForTest,
};
