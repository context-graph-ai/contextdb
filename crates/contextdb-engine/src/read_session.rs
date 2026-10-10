//! Public bounded-read declarations owned by database route assembly.
//!
//! The direct-file reader, local owner service, and bounded execution kernel
//! remain separate modules. This module deliberately defines only their shared
//! public doors and the narrow test-time clock injection needed to prove owner
//! shutdown without wall-clock waits.

#![forbid(unsafe_code)]

use crate::database::OwnedQueryResult;
use crate::direct_file_reader::{
    DirectMetadataBody as MetadataBody, DirectMetadataRequest as MetadataRequest,
};
#[cfg(feature = "test-seams")]
use crate::executor::bounded_read_test_support::{ExecutionProbe, TestSourceTouch, TestWorkSource};
use crate::owned_cursor_page::OwnedCursorPage;
use crate::plugin::{CorePlugin, DatabasePlugin};
use crate::read_progress::{ReadProgressObserver, with_progress_observer};
use crate::{Database, QueryResult};
use contextdb_core::read_contract::ChannelAddress;
#[cfg(feature = "test-seams")]
use contextdb_core::read_contract::DeadlineClock;
use contextdb_core::read_contract::{
    CursorPage, OwnerReadCancellation, OwnerReadLimits, OwnerReadStatus, OwnerRequestHandler,
    OwnerServiceTimeouts, ReadClientTimeouts, ReadLimits, ReadRoute,
};
use contextdb_core::{ContextId, Error, Principal, Result, ScopeLabel, Value};
use std::collections::{BTreeSet, HashMap};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Per-session bounded read policy, owner-client deadlines, and the
/// visibility this session reads under.
///
/// The three declarations mirror [`DatabaseOpenOptions`] exactly, because they
/// mean exactly the same thing: who this reader is and what part of the store
/// it is looking at. They reach the same row gate a writable handle's do, so
/// an out-of-scope, out-of-context, or ungranted row is INVISIBLE whatever
/// shape of statement asks for it -- `OR 1 = 1`, `NOT (...)`, an IN-list, a
/// LIKE, a subquery -- on either read route and across cursor pages. No
/// statement is inspected anywhere; the engine decides per row.
///
/// A consumer that filters SQL instead cannot make this promise: every one of
/// those shapes re-widens a statement an analyzer believed it had narrowed.
///
/// All three default to `None`, which is a session that declares nothing and
/// reads exactly what it reads today.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReadSessionOptions {
    /// Non-raisable session memory ceiling. Bounds the file reader's loaded
    /// state and combines with `limits.memory` for each read on either route.
    /// It never changes the store's durable MEMORY_LIMIT declaration.
    pub memory_limit: Option<usize>,
    pub limits: ReadLimits,
    pub timeouts: ReadClientTimeouts,
    pub contexts: Option<BTreeSet<ContextId>>,
    pub scope_labels: Option<BTreeSet<ScopeLabel>>,
    pub principal: Option<Principal>,
}

/// Owner inspection policy attached to a writable database open.
#[derive(Clone)]
pub struct OwnerReadConfig {
    pub enabled: bool,
    pub limits: OwnerReadLimits,
    pub timeouts: OwnerServiceTimeouts,
    pub runtime_dir: Option<PathBuf>,
    pub handler: Option<Arc<dyn OwnerRequestHandler>>,
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub test_hooks: Option<OwnerReadTestHooks>,
}

impl Default for OwnerReadConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            limits: OwnerReadLimits::default(),
            timeouts: OwnerServiceTimeouts::default(),
            runtime_dir: None,
            handler: None,
            #[cfg(feature = "test-seams")]
            test_hooks: None,
        }
    }
}

/// Whether a writable open may bring a store into existence.
///
/// A consumer that means to CHANGE a store it already has must be able to say
/// so in the open itself. Without that, its only options are to look the path
/// up first -- and race whoever moves it between the look and the open -- or to
/// accept that a typo, an unmounted volume, or a link whose target is gone
/// quietly materializes a brand-new empty deployment and reports success.
///
/// [`OpenDisposition::ExistingOnly`] removes the choice: the open is attempted,
/// nothing is created, and a store that is not there comes back as
/// [`contextdb_core::Error::StoreMissing`] naming the path -- distinct from a
/// store that IS there and cannot be read, which keeps its own refusal, because
/// "it is not there" would send an operator to restore a backup when the real
/// fix is a permission or a mount.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OpenDisposition {
    /// Open the store at the path, creating it if nothing is there. This is
    /// what every opener that says nothing about a disposition has always
    /// done, and it stays the default.
    #[default]
    CreateIfMissing,
    /// Open a store that already exists, or refuse. Nothing is created, and a
    /// refusal leaves the store, its companion, and the directory holding them
    /// byte-for-byte as they were found.
    ExistingOnly,
}

/// Complete writable-open configuration. Existing convenience openers retain
/// their signatures and eventually route through this value.
///
/// Internal format and destructive-reset authority cannot be supplied by a
/// normal caller:
///
/// ```compile_fail
/// use contextdb_engine::DatabaseOpenOptions;
/// let _ = DatabaseOpenOptions {
///     allow_legacy: true,
///     ..DatabaseOpenOptions::default()
/// };
/// ```
///
/// ```compile_fail
/// use contextdb_engine::DatabaseOpenOptions;
/// let _ = DatabaseOpenOptions {
///     force_reset: true,
///     ..DatabaseOpenOptions::default()
/// };
/// ```
#[derive(Clone)]
pub struct DatabaseOpenOptions {
    pub owner_reads: OwnerReadConfig,
    /// Whether this open may create the store it names. Defaults to
    /// [`OpenDisposition::CreateIfMissing`], so an open that never mentions it
    /// behaves exactly as it always has.
    pub open_disposition: OpenDisposition,
    pub plugin: Arc<dyn DatabasePlugin>,
    pub memory_limit: Option<usize>,
    pub disk_limit: Option<u64>,
    /// Who drives maintenance for this writable open.
    pub maintenance_policy: crate::database::MaintenancePolicy,
    /// Optional database poll interval override. A file-backed override is
    /// persisted as the database setting used by later opens.
    pub maintenance_poll_interval: Option<Duration>,
    pub contexts: Option<BTreeSet<ContextId>>,
    pub scope_labels: Option<BTreeSet<ScopeLabel>>,
    pub principal: Option<Principal>,
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub test_observer: Option<Arc<dyn ReadSessionTestObserver>>,
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub test_kernel_observer: Option<Arc<dyn ReadKernelTestObserver>>,
}

impl Default for DatabaseOpenOptions {
    fn default() -> Self {
        Self {
            owner_reads: OwnerReadConfig::default(),
            open_disposition: OpenDisposition::default(),
            plugin: Arc::new(CorePlugin),
            memory_limit: None,
            disk_limit: None,
            maintenance_policy: crate::database::MaintenancePolicy::default(),
            maintenance_poll_interval: None,
            contexts: None,
            scope_labels: None,
            principal: None,
            #[cfg(feature = "test-seams")]
            test_observer: None,
            #[cfg(feature = "test-seams")]
            test_kernel_observer: None,
        }
    }
}

/// The vocabulary route selection speaks about its own assembly.
///
/// Production route code classifies open failures and announces the
/// boundaries it reaches, so these types exist in every build. What the
/// `test-seams` feature adds is not the vocabulary but the DOORS -- the
/// options field and the `*_for_test` constructors that let a proof install
/// an observer or inject a handshake mismatch. Compiling the vocabulary only
/// with the feature left the shipped configuration unbuildable.
mod route_observation {
    use super::*;

    // This vocabulary is exposed only to `test-seams` integration proofs.
    /// The bounded operations whose cancellation identity is observable.
    #[cfg_attr(not(feature = "test-seams"), allow(dead_code))]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ReadSessionOperation {
        Execute,
        Metadata,
        CursorOpen,
        CursorFetch,
    }

    /// Failure classes observed at the route selector's actual open boundaries.
    /// Only `WriterBecameOwner` permits one ownership-race retry.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ReadRouteOpenFailure {
        WriterBecameOwner,
        DirectReadRequiresWriter,
        DirectOpen,
        OwnerAuthentication,
        /// A writer holds this store and its declared budget ran out before
        /// that writer published what it decided. Nothing was authenticated
        /// and nothing refused this caller: the owner simply had not answered
        /// yet. Terminal for this selection like every class but
        /// `WriterBecameOwner` -- asking again is the caller's decision, on a
        /// budget it declares, not something route selection does for it.
        OwnerHeldWithoutDecision,
        StoreDamage,
        /// The caller asked to reach the owner and nobody owns this store, so
        /// selection stopped before the committed file was consulted at all.
        OwnerRequired,
    }

    /// Deliberate owner-handshake mismatch used to prove that authentication
    /// failures remain terminal to one route-selection invocation.
    #[cfg_attr(not(feature = "test-seams"), allow(dead_code))]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum OwnerHandshakeMismatchForTest {
        DatabaseIdentity,
    }

    /// Route-assembly events needed by cross-process and no-fallback proofs.
    #[cfg_attr(not(feature = "test-seams"), allow(dead_code))]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ReadSessionEvent {
        OwnerResolution {
            attempt: u64,
            owner_available: bool,
        },
        /// Emitted immediately before calling the real direct-file opener. A test
        /// observer may block here so another process can win writer ownership.
        BeforeDirectBackendOpen {
            attempt: u64,
        },
        /// Emitted by an owner-only selection that found a live claim on
        /// this store whose holder has not published a serving decision
        /// yet, immediately before the caller waits for that decision
        /// inside its own declared `routing_retry_ms`. `attempt` is the
        /// selection attempt that observed the claim. A test observer may
        /// block here, which is what lets a proof drive the writer's
        /// publication against a caller that is provably already waiting.
        ClaimWindowWait {
            attempt: u64,
        },
        RouteOpenFailed {
            attempt: u64,
            failure: ReadRouteOpenFailure,
        },
        OwnershipRaceRetry {
            failed_attempt: u64,
            retry_attempt: u64,
        },
        RouteSelected(ReadRoute),
        OpenRegistryAcquired,
        /// Emitted inside a writable open at the moment this writer takes
        /// the companion's FIRST exclusive claim on the store, before it
        /// has published anything a reader can dial and before any serving
        /// decision exists. A test observer may block here, which is what
        /// lets a proof hold a store claimed-but-unannounced against a
        /// caller in another process -- the window in which an owner-only
        /// ask must not be told the store is free.
        CompanionClaimTaken,
        PersistenceOpened,
        BlobRepositoryOpened,
        PluginOpened,
        BackgroundWorkerStarted,
        LocalChannelOperation,
        DirectBackendOpen,
        OwnerServiceStarted,
        ResponseFrameReceived {
            terminal: bool,
        },
        /// The cursor has registered its provisional hold before asking the
        /// owner for its first page.
        OwnerCursorHandoffStarted,
        /// The cursor-open response is complete, but the client has not yet
        /// exchanged its provisional page hold for the exact saved root.
        BeforeOwnerCursorHandoffRegistration,
        /// The deadline watcher released a retained owner cursor while its
        /// caller still held the cursor object.
        OwnerCursorExpiryReleased,
    }

    /// Concrete production source reached by a bounded read. These values are
    /// emitted only by the adapter implementing the bounded kernel's execution
    /// probe; route selection and backend setup have no callback door for them.
    #[cfg_attr(not(feature = "test-seams"), allow(dead_code))]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ReadKernelSource {
        TableRow,
        IndexEntry,
        SortCandidate,
        GraphEdge,
        BruteForceVectorCandidate,
        HnswCandidate,
        RankCandidate,
        AccessRow,
    }

    /// Evidence emitted from the bounded source loop, after route and backend
    /// selection and immediately before one real source item is inspected.
    #[cfg_attr(not(feature = "test-seams"), allow(dead_code))]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ReadKernelSourceEvent {
        pub funding_owner: Option<usize>,
        pub control_reservation_bytes: Option<usize>,
        pub operation: ReadSessionOperation,
        pub route: ReadRoute,
        pub source: ReadKernelSource,
        pub completed_items: u64,
    }

    /// Evidence emitted by the bounded kernel immediately before it returns its
    /// typed cancellation result.
    #[cfg_attr(not(feature = "test-seams"), allow(dead_code))]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ReadKernelCancellationEvent {
        pub operation: ReadSessionOperation,
        pub route: ReadRoute,
        pub completed_work: u64,
    }

    /// Proof-only handoff from the indexed source after a real residual
    /// rejection and before the walk considers a replacement.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ReadKernelResidualRejectionEvent {
        pub operation: ReadSessionOperation,
        pub route: ReadRoute,
        pub row_id: contextdb_core::types::RowId,
    }

    /// A narrow observer for route assembly. Implementations may block at an
    /// event, allowing a test to kill an owner between response frames without a
    /// wall-clock race.
    #[cfg_attr(not(feature = "test-seams"), allow(dead_code))]
    pub trait ReadSessionTestObserver: Send + Sync {
        fn observe_event(&self, _event: ReadSessionEvent) {}

        /// Replace one owner's cursor-fetch page for a proof of client-side
        /// decoding and terminal resource release.
        #[cfg(feature = "test-seams")]
        fn cursor_fetch_payload_for_test(&self, _payload: &[u8]) -> Option<Vec<u8>> {
            None
        }

        /// Replace one owner's cursor-open page before the client decodes it.
        #[cfg(feature = "test-seams")]
        fn cursor_open_payload_for_test(&self, _payload: &[u8]) -> Option<Vec<u8>> {
            None
        }
    }

    /// Test-only observation at the bounded-kernel boundary. The route layer
    /// supplies this observer to the selected backend; it must never invoke these
    /// methods itself. The cancellation value is the value consumed by that
    /// kernel invocation, not a route-level preflight copy.
    #[cfg_attr(not(feature = "test-seams"), allow(dead_code))]
    pub trait ReadKernelTestObserver: Send + Sync {
        #[cfg(feature = "test-seams")]
        fn registered_snapshot_for_test(
            &self,
            _route: ReadRoute,
            _operation: ReadSessionOperation,
            _snapshot: contextdb_core::SnapshotId,
        ) {
        }

        fn before_source_touch(
            &self,
            event: ReadKernelSourceEvent,
            cancellation: OwnerReadCancellation,
        );

        #[cfg(feature = "test-seams")]
        fn after_vector_residual_rejection(
            &self,
            _event: ReadKernelResidualRejectionEvent,
            _cancellation: OwnerReadCancellation,
        ) {
        }

        fn cancellation_observed(
            &self,
            event: ReadKernelCancellationEvent,
            cancellation: OwnerReadCancellation,
        );
    }
}

// Without the doors these names are the engine's own; with them they are the
// surface a proof observes through.
// The production-private façade does not consume every proof export itself.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub use route_observation::{
    OwnerHandshakeMismatchForTest, ReadKernelCancellationEvent, ReadKernelResidualRejectionEvent,
    ReadKernelSource, ReadKernelSourceEvent, ReadKernelTestObserver, ReadRouteOpenFailure,
    ReadSessionEvent, ReadSessionOperation, ReadSessionTestObserver,
};
#[cfg_attr(not(feature = "test-seams"), allow(unused_imports))]
#[cfg(not(feature = "test-seams"))]
pub(crate) use route_observation::{
    OwnerHandshakeMismatchForTest, ReadKernelCancellationEvent, ReadKernelSource,
    ReadKernelSourceEvent, ReadKernelTestObserver, ReadRouteOpenFailure, ReadSessionEvent,
    ReadSessionOperation, ReadSessionTestObserver,
};

#[cfg(feature = "test-seams")]
impl From<TestSourceTouch> for ReadKernelSource {
    fn from(value: TestSourceTouch) -> Self {
        match value {
            TestSourceTouch::TableRow => Self::TableRow,
            TestSourceTouch::IndexEntry => Self::IndexEntry,
            TestSourceTouch::SortCandidate => Self::SortCandidate,
            // An adjacency entry IS a graph edge to anyone watching the
            // kernel; the two are counted apart only so that what the trace
            // publishes as examined counts entries rather than the steps a
            // walk takes over them. No new source is published for it.
            TestSourceTouch::GraphEdge | TestSourceTouch::AdjacencyEntry => Self::GraphEdge,
            TestSourceTouch::BruteForceVectorCandidate => Self::BruteForceVectorCandidate,
            TestSourceTouch::HnswCandidate => Self::HnswCandidate,
            TestSourceTouch::RankCandidate => Self::RankCandidate,
            TestSourceTouch::AccessRow => Self::AccessRow,
        }
    }
}

#[cfg(feature = "test-seams")]
thread_local! {
    static READ_RUNTIME_DIRECTORY_OVERRIDE: std::cell::RefCell<Option<PathBuf>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(feature = "test-seams")]
struct ReadRuntimeDirectoryOverrideGuard {
    previous: Option<PathBuf>,
}

#[cfg(feature = "test-seams")]
impl Drop for ReadRuntimeDirectoryOverrideGuard {
    fn drop(&mut self) {
        let previous = self.previous.take();
        READ_RUNTIME_DIRECTORY_OVERRIDE.with(|slot| {
            let _ = slot.replace(previous);
        });
    }
}

/// Return the task-scoped runtime root selected by the surrounding test. The
/// production route resolver consumes this in place of process environment;
/// no alternate route selector or transport is exposed.
#[cfg(feature = "test-seams")]
pub(crate) fn runtime_directory_override_for_test() -> Option<PathBuf> {
    READ_RUNTIME_DIRECTORY_OVERRIDE.with(|slot| slot.borrow().clone())
}

// The route observer a writable open on THIS thread reports its own
// milestones to.
//
// A writer's first exclusive claim happens deep inside persistence, several
// layers below the options the caller passed, and none of those layers carry
// an observer. Rather than thread one through every writable-open signature
// for a test-only door, the observer the open was given travels on the thread
// that is doing the opening. Shipped builds compile this away entirely.
#[cfg(feature = "test-seams")]
thread_local! {
    static WRITER_OPEN_OBSERVER: std::cell::RefCell<Option<Arc<dyn ReadSessionTestObserver>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(feature = "test-seams")]
struct WriterOpenObserverGuard {
    previous: Option<Arc<dyn ReadSessionTestObserver>>,
}

#[cfg(feature = "test-seams")]
impl Drop for WriterOpenObserverGuard {
    fn drop(&mut self) {
        let previous = self.previous.take();
        WRITER_OPEN_OBSERVER.with(|slot| {
            let _ = slot.replace(previous);
        });
    }
}

/// Run a writable open with the observer it was given in force for the whole
/// of it, so milestones reached below the options layer still reach it.
#[cfg(feature = "test-seams")]
pub(crate) fn with_writer_open_observer<T>(
    observer: Option<&Arc<dyn ReadSessionTestObserver>>,
    run: impl FnOnce() -> T,
) -> T {
    let previous = WRITER_OPEN_OBSERVER.with(|slot| slot.replace(observer.cloned()));
    let _guard = WriterOpenObserverGuard { previous };
    run()
}

/// Tell the writable open on this thread that it reached one of its own
/// milestones. Nothing outside a proof ever listens.
pub(crate) fn note_writer_open_event(event: ReadSessionEvent) {
    #[cfg(feature = "test-seams")]
    {
        let observer = WRITER_OPEN_OBSERVER.with(|slot| slot.borrow().clone());
        if let Some(observer) = observer {
            observer.observe_event(event);
        }
    }
    #[cfg(not(feature = "test-seams"))]
    let _ = event;
}

// The clock route selection on THIS thread deadlines against.
//
// Channel operations and a claim window both expire on caller-declared
// budgets. A proof drives either one by moving this manual clock rather than
// sleeping. Route selection reaches those waits through several layers that
// carry no clock, so the supplied clock travels on the opening thread.
#[cfg(feature = "test-seams")]
thread_local! {
    static ROUTE_SELECTION_CLOCK: std::cell::RefCell<
        Option<Arc<dyn contextdb_core::read_contract::DeadlineClock>>,
    > = const { std::cell::RefCell::new(None) };
}

#[cfg(feature = "test-seams")]
struct RouteSelectionClockGuard {
    previous: Option<Arc<dyn contextdb_core::read_contract::DeadlineClock>>,
}

#[cfg(feature = "test-seams")]
impl Drop for RouteSelectionClockGuard {
    fn drop(&mut self) {
        let previous = self.previous.take();
        ROUTE_SELECTION_CLOCK.with(|slot| {
            let _ = slot.replace(previous);
        });
    }
}

#[cfg(feature = "test-seams")]
fn with_route_selection_clock_for_test<T>(
    clock: Arc<dyn contextdb_core::read_contract::DeadlineClock>,
    run: impl FnOnce() -> T,
) -> T {
    let previous = ROUTE_SELECTION_CLOCK.with(|slot| slot.replace(Some(clock)));
    let _guard = RouteSelectionClockGuard { previous };
    run()
}

/// The clock this thread's claim-window wait deadlines against: the one a
/// proof supplied, or the ordinary monotonic clock every other deadline in a
/// read session already uses.
fn claim_window_clock() -> Arc<dyn contextdb_core::read_contract::DeadlineClock> {
    #[cfg(feature = "test-seams")]
    if let Some(clock) = ROUTE_SELECTION_CLOCK.with(|slot| slot.borrow().clone()) {
        return clock;
    }
    read_clock()
}

#[cfg(feature = "test-seams")]
struct SessionKernelProbe {
    observer: Arc<dyn ReadKernelTestObserver>,
    operation: ReadSessionOperation,
    route: ReadRoute,
    cancellation: OwnerReadCancellation,
}

#[cfg(feature = "test-seams")]
impl ExecutionProbe for SessionKernelProbe {
    fn after_hnsw_residual_rejection(
        &self,
        event: contextdb_vector::hnsw::HnswResidualRejectionEvent,
    ) {
        self.observer.after_vector_residual_rejection(
            ReadKernelResidualRejectionEvent {
                operation: self.operation,
                route: self.route,
                row_id: event.row_id(),
            },
            self.cancellation.clone(),
        );
    }
    fn registered_snapshot_for_test(&self, snapshot: contextdb_core::SnapshotId) {
        self.observer
            .registered_snapshot_for_test(self.route, self.operation, snapshot);
    }

    fn before_work(&self, _source: TestWorkSource, _completed_work: u64) {}

    fn before_source_touch(&self, source: TestSourceTouch, completed_items: u64) {
        self.observer.before_source_touch(
            ReadKernelSourceEvent {
                funding_owner: crate::read_work_probe::observed_funding_identity(),
                control_reservation_bytes: crate::read_work_probe::observed_funding().map(|v| v.1),
                operation: self.operation,
                route: self.route,
                source: source.into(),
                completed_items,
            },
            self.cancellation.clone(),
        );
    }

    fn cancellation_observed(&self, completed_work: u64) {
        self.observer.cancellation_observed(
            ReadKernelCancellationEvent {
                operation: self.operation,
                route: self.route,
                completed_work,
            },
            self.cancellation.clone(),
        );
    }
}

/// The kernel probe an owner hands its own bounded run when the STORE carries
/// an observer.
///
/// Work asked for over the channel runs on the owner's thread, so an observer
/// attached to the store is the only one that can watch it -- a reader's own
/// thread-local never touches the kernel the owner runs.
#[cfg(feature = "test-seams")]
pub(crate) fn owner_route_kernel_probe(
    observer: &Arc<dyn ReadKernelTestObserver>,
    operation: ReadSessionOperation,
    cancellation: &OwnerReadCancellation,
) -> Arc<dyn crate::executor::BoundedExecutionProbe> {
    crate::executor::bounded_read_test_support::kernel_probe(kernel_probe_for_test(
        Arc::clone(observer),
        operation,
        ReadRoute::Owner,
        cancellation,
    ))
}

/// Build the sole observer adapter accepted by bounded production execution.
/// Backend adapters pass this value through; route code cannot manufacture a
/// source-touch callback without entering the bounded executor probe.
#[cfg(feature = "test-seams")]
pub(crate) fn kernel_probe_for_test(
    observer: Arc<dyn ReadKernelTestObserver>,
    operation: ReadSessionOperation,
    route: ReadRoute,
    cancellation: &OwnerReadCancellation,
) -> Arc<dyn ExecutionProbe> {
    Arc::new(SessionKernelProbe {
        observer,
        operation,
        route,
        cancellation: cancellation.clone(),
    })
}

/// Resource identity and side-effect counts retained by one selected route.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReadRouteResourceSnapshot {
    pub channel_identity: Option<ChannelAddress>,
    pub owner_services_started: u64,
    pub local_channel_operations: u64,
    pub direct_backend_opens: u64,
    /// Cumulative items completed by the selected backend's real bounded
    /// sources. This is kernel progress, never a route callback count or
    /// requested-item count.
    pub bounded_source_items_completed: u64,
    pub active_owner_slots: u64,
    pub active_cursors: u64,
    pub open_registry_owned: bool,
    pub persistence_owned: bool,
    pub blob_repository_owned: bool,
    pub plugin_open: bool,
    pub snapshot_registry_owned: bool,
    pub memory_accountant_owned: bool,
}

/// What one selected route is holding, counted as it is actually used.
#[derive(Default)]
struct RouteResources {
    channel_identity: Mutex<Option<ChannelAddress>>,
    #[cfg_attr(not(feature = "test-seams"), allow(dead_code))]
    owner_services_started: std::sync::atomic::AtomicU64,
    local_channel_operations: std::sync::atomic::AtomicU64,
    direct_backend_opens: std::sync::atomic::AtomicU64,
    #[cfg_attr(not(feature = "test-seams"), allow(dead_code))]
    active_owner_slots: std::sync::atomic::AtomicU64,
    active_cursors: std::sync::atomic::AtomicU64,
    /// Real source items THIS route's reads have finished inspecting. Kept per
    /// route rather than read from the process-wide probe, so a caller asking
    /// what its own read has done is not told what its neighbours did.
    #[cfg(feature = "test-seams")]
    bounded_source_items: Arc<std::sync::atomic::AtomicU64>,
    /// The owner's own counter for the channel this route selected, when that
    /// owner runs in this process. Work asked for over the channel is done on
    /// the owner's thread, where this reader's counter above is not in force,
    /// so the owner's number is the only true one for this route.
    #[cfg(feature = "test-seams")]
    channel_source_items: Mutex<Option<Arc<std::sync::atomic::AtomicU64>>>,
}

impl RouteResources {
    fn note_channel_operation(&self) {
        self.local_channel_operations
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn note_direct_backend_open(&self) {
        self.direct_backend_opens
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(feature = "test-seams")]
    fn snapshot(&self) -> ReadRouteResourceSnapshot {
        use std::sync::atomic::Ordering;
        ReadRouteResourceSnapshot {
            channel_identity: *self
                .channel_identity
                .lock()
                .expect("route channel identity"),
            owner_services_started: self.owner_services_started.load(Ordering::SeqCst),
            local_channel_operations: self.local_channel_operations.load(Ordering::SeqCst),
            direct_backend_opens: self.direct_backend_opens.load(Ordering::SeqCst),
            // The kernel's own count of items THIS route finished inspecting
            // -- an item it was stopped on before inspecting is not among
            // them, and neither is anything another read in this process did.
            bounded_source_items_completed: match self
                .channel_source_items
                .lock()
                .expect("route channel source counter")
                .as_ref()
            {
                Some(owner) => owner.load(Ordering::SeqCst),
                None => self.bounded_source_items.load(Ordering::SeqCst),
            },
            active_owner_slots: self.active_owner_slots.load(Ordering::SeqCst),
            active_cursors: self.active_cursors.load(Ordering::SeqCst),
            open_registry_owned: false,
            persistence_owned: false,
            blob_repository_owned: false,
            plugin_open: false,
            snapshot_registry_owned: false,
            memory_accountant_owned: false,
        }
    }
}

/// Poll one local-channel answer to completion on the calling thread. A read
/// session owns no runtime and starts no thread of its own to wait.
fn wait_for_local_answer<F: std::future::Future>(future: F) -> F::Output {
    struct ParkedThread(std::thread::Thread);

    impl std::task::Wake for ParkedThread {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = std::task::Waker::from(Arc::new(ParkedThread(std::thread::current())));
    let mut context = std::task::Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match std::pin::Pin::as_mut(&mut future).poll(&mut context) {
            std::task::Poll::Ready(output) => return output,
            std::task::Poll::Pending => std::thread::park(),
        }
    }
}

/// Say what an answer that came back over the owner's channel means to the
/// caller who asked for it.
fn owner_error(error: crate::owner_read::OwnerReadScaffoldError) -> Error {
    use crate::owner_read::OwnerReadScaffoldError as Scaffold;
    match error {
        Scaffold::Refused(failure) => Error::ReadFailure(failure),
        Scaffold::Database(error) => error,
        Scaffold::ReadContract(violation) => Error::Other(violation.to_string()),
        Scaffold::LocalTransport(_) => Error::ReadFailure(owner_disconnected()),
        Scaffold::Unimplemented { seam } => Error::Other(seam.to_owned()),
    }
}

/// The answer a reader gets when the owner of this store is winding down and
/// will not take new work.
fn owner_not_serving() -> contextdb_core::read_contract::ReadFailure {
    contextdb_core::read_contract::ReadFailure::new(
        contextdb_core::read_contract::ReadFailureKind::OwnerNotServing,
        contextdb_core::read_contract::ReadFailureDetail::None,
    )
    .expect("an owner-not-serving refusal carries no further detail")
}

/// The same refusal, carrying the reason the writer recorded beside the store
/// so the caller learns WHY inspection is unavailable, not merely that it is.
fn owner_not_serving_because(
    status: &contextdb_core::read_contract::OwnerReadStatus,
) -> contextdb_core::read_contract::ReadFailure {
    contextdb_core::read_contract::ReadFailure::new(
        contextdb_core::read_contract::ReadFailureKind::OwnerNotServing,
        contextdb_core::read_contract::ReadFailureDetail::Reason {
            reason: status.refusal_reason(),
        },
    )
    .expect("an owner-not-serving refusal may carry the recorded reason")
}

/// The answer a reader gets when the owner it selected stopped answering.
fn owner_disconnected() -> contextdb_core::read_contract::ReadFailure {
    contextdb_core::read_contract::ReadFailure::new(
        contextdb_core::read_contract::ReadFailureKind::OwnerDisconnected,
        contextdb_core::read_contract::ReadFailureDetail::None,
    )
    .expect("an owner-disconnect refusal carries no further detail")
}

/// A cursor cannot be opened while this handle has a transaction open.
fn cursor_transaction_active() -> contextdb_core::read_contract::ReadFailure {
    contextdb_core::read_contract::ReadFailure::new(
        contextdb_core::read_contract::ReadFailureKind::CursorTransactionActive,
        contextdb_core::read_contract::ReadFailureDetail::None,
    )
    .expect("a cursor-transaction-active refusal carries no further detail")
}

/// The answer a reader gets when it asks a file-route session for something
/// only an owner can do. There is no owner to ask, and this session will not
/// go looking for one.
fn owner_not_running() -> contextdb_core::read_contract::ReadFailure {
    contextdb_core::read_contract::ReadFailure::new(
        contextdb_core::read_contract::ReadFailureKind::OwnerNotRunning,
        contextdb_core::read_contract::ReadFailureDetail::None,
    )
    .expect("an owner-not-running refusal carries no further detail")
}

/// The cursor this request names is not one this route is holding any more.
fn cursor_not_found() -> contextdb_core::read_contract::ReadFailure {
    contextdb_core::read_contract::ReadFailure::new(
        contextdb_core::read_contract::ReadFailureKind::CursorNotFound,
        contextdb_core::read_contract::ReadFailureDetail::None,
    )
    .expect("a cursor-not-found refusal carries no further detail")
}

/// Say what a direct read of the committed file means to its caller, keeping
/// the engine's own typed answer so both routes say the same thing about the
/// same fault.
fn direct_error(path: &Path, error: crate::direct_file_reader::DirectFileReaderError) -> Error {
    use crate::direct_file_reader::DirectFileReaderError as Direct;
    let reason = error.to_string();
    match error {
        Direct::ReadFailure(failure)
        | Direct::StoreNotFound { failure }
        | Direct::DirectReadRequiresWriter { failure, .. } => Error::ReadFailure(failure),

        Direct::Cancelled => Error::ReadCancelled,
        Direct::Engine(prose) => Error::Other(prose),
        Direct::MemoryBudget {
            subsystem,
            operation,
            requested_bytes,
            available_bytes,
            budget_limit_bytes,
            hint,
        } => Error::MemoryBudgetExceeded {
            subsystem,
            operation,
            requested_bytes,
            available_bytes,
            budget_limit_bytes,
            hint,
        },
        // A store nobody can read is the one answer an operator has to act on,
        // so a reader must not hand them less than a writer does. The direct
        // reader names what it found but has no vocabulary for what to do
        // next, so the same next step the writable open publishes is attached
        // here -- one corrupt file, one thing to go and do, whoever opened it.
        Direct::CorruptStore(..) | Direct::LegacyLayout(..) => Error::StoreCorrupted {
            path: path.display().to_string(),
            reason: format!(
                "{reason} — {}",
                crate::persistence::RedbPersistence::CORRUPT_STORE_NEXT_STEP
            ),
        },
        // A cursor this route no longer holds is a CURSOR condition, and it is
        // said in the cursor vocabulary. `owner_not_running` means Rust
        // `request_owner` was called on a direct-file route -- an internal
        // misuse -- so handing it to a caller who asked a direct-file cursor
        // for a page tells them about the plumbing instead of about their
        // cursor, and names no step they can take.
        Direct::CursorUnavailable(_) => Error::ReadFailure(cursor_not_found()),
        Direct::Contended { .. } => Error::Other(reason),
        // "Not implemented" is one answer and it means one thing: there is no
        // production path for this route. Every other prerequisite the direct
        // reader can find missing is an internal invariant that failed on a
        // route that IS implemented, and saying it is unimplemented tells the
        // caller to stop asking instead of what went wrong.
        Direct::MissingPrerequisite(
            crate::direct_file_reader::DirectReaderPrerequisite::PersistenceLoadReadImage,
        ) => Error::ReadSessionNotImplemented,
        Direct::MissingPrerequisite(_) => Error::Other(reason),
    }
}

/// Which routes an open is willing to take.
///
/// The ordinary answer is both: ask the owner, and read the committed file
/// when there is no owner to ask. A caller whose question only an owner can
/// answer asks for the owner alone, and a store nobody owns is then an
/// owner-absent answer rather than a file to open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RouteRequirement {
    OwnerOrFile,
    OwnerOnly,
}

/// What one route-selection attempt found at the path.
enum SelectedRoute {
    Owner(OwnerRoute),
    File(Box<crate::direct_file_reader::DirectFileReader>),
}

/// A route-open failure that this selection may answer by asking again.
struct RouteOpenRefusal {
    failure: ReadRouteOpenFailure,
    error: Error,
}

/// Where this process looks for local channels, and the runtime root that
/// directory sits in. A test scopes both to its own runtime root through the
/// production resolver; there is no second selector and no alternate
/// transport. Both parts travel together so no caller has to work one out
/// from the other's pathname.
fn read_runtime_directory(
    owner_user: contextdb_core::read_contract::LocalUserIdentity,
    supplied: Option<&Path>,
) -> std::result::Result<crate::local_transport::RuntimeDirectory, Error> {
    #[cfg(feature = "test-seams")]
    let supplied = supplied
        .map(Path::to_path_buf)
        .or_else(runtime_directory_override_for_test)
        .map(std::borrow::Cow::<Path>::Owned);
    #[cfg(not(feature = "test-seams"))]
    let supplied = supplied.map(std::borrow::Cow::Borrowed);
    crate::local_transport::runtime_directory_for_store(supplied.as_deref(), owner_user).map_err(
        |error| {
            Error::ReadFailure(
                contextdb_core::read_contract::ReadFailure::new(
                    contextdb_core::read_contract::ReadFailureKind::OwnerNotServing,
                    contextdb_core::read_contract::ReadFailureDetail::Reason {
                        reason: match supplied.as_deref() {
                            Some(root) => format!(
                                "owner channel runtime directory {} is unusable: {error}",
                                root.display()
                            ),
                            None => {
                                format!("owner channel runtime directory is unavailable: {error}")
                            }
                        },
                    },
                )
                .expect("owner-not-serving accepts a reason"),
            )
        },
    )
}

/// What the canonical store's companion record says about who owns it, if
/// anyone has ever published that. A store nobody has owned has no channel to
/// try. The caller resolves the canonical path once for every route fact.
fn published_owner_channel(
    canonical_path: &Path,
) -> Option<(
    contextdb_core::read_contract::DatabaseIdentity,
    contextdb_core::read_contract::WriterRunNumber,
    contextdb_core::read_contract::LocalUserIdentity,
    ChannelAddress,
)> {
    // A durable companion can outlive both its writer and the store pathname.
    // Once the resolved pathname is positively absent, that old record and
    // channel describe an unlinked inode, not the store the caller named.
    // An indeterminate existence check is not treated as absence; the normal
    // authenticated path remains responsible for refusing it safely.
    if matches!(canonical_path.try_exists(), Ok(false)) {
        return None;
    }
    crate::persistence::published_writer_identity(canonical_path)
}

/// Resolve the one pathname every route fact uses when a store can exist.
///
/// An existing store, or a missing file whose parent exists, has a canonical
/// identity. A path beneath an absent parent cannot have a companion, channel,
/// claim, or committed file at all; keeping that confirmed-missing spelling
/// lets the direct reader return the stable typed `StoreNotFound` answer. Any
/// other canonicalization failure stays terminal rather than being mistaken
/// for proof that the store is absent.
fn resolved_route_path(path: &Path) -> Result<PathBuf> {
    resolved_route_path_inner(path, 0)
}

fn resolved_route_path_inner(path: &Path, symlink_depth: usize) -> Result<PathBuf> {
    if symlink_depth > 32 {
        return Err(Error::Other(format!(
            "too many symlink levels while resolving the read route for {}",
            path.display()
        )));
    }
    // Preserve the target spelling even when the target's parent does not
    // exist. Falling back to the alias spelling would let a stale companion
    // beside that alias describe a different, formerly reachable store.
    if let Ok(metadata) = std::fs::symlink_metadata(path)
        && metadata.file_type().is_symlink()
    {
        let target = std::fs::read_link(path)
            .map_err(|error| Error::Other(format!("read_link {}: {error}", path.display())))?;
        let resolved = if target.is_absolute() {
            target
        } else {
            path.parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."))
                .join(target)
        };
        return resolved_route_path_inner(&resolved, symlink_depth.saturating_add(1));
    }
    match crate::database::canonical_database_path(path) {
        Ok(canonical) => Ok(canonical),
        Err(_error) if matches!(path.try_exists(), Ok(false)) => Ok(path.to_path_buf()),
        Err(error) => Err(error),
    }
}

/// Try one route at this path, in the one order a reader may use: ask a
/// published owner candidate to authenticate, otherwise read the file. Once
/// either route is selected there is no fallback in either direction.
/// The answer a caller is owed when a writer is holding this store and has
/// not published what it decided about serving inside the deadlines that
/// caller declared.
///
/// It is the existing not-serving vocabulary, and that is the point: a writer
/// IS holding this store, so the one answer that must never be given is the
/// one a consumer reads as permission to treat the store as absent and take it
/// over. This stays distinguishable from the timeout a caller gets dialling a
/// store no writer has claimed, so the difference a consumer acts on survives.
fn owner_holding_without_a_decision() -> contextdb_core::read_contract::ReadFailure {
    contextdb_core::read_contract::ReadFailure::new(
        contextdb_core::read_contract::ReadFailureKind::OwnerNotServing,
        contextdb_core::read_contract::ReadFailureDetail::Reason {
            reason: "a writer holds this store and has not published a serving decision within \
                     the deadlines this caller declared"
                .to_owned(),
        },
    )
    .expect("an owner-not-serving refusal may carry the reason it was given")
}

/// How long a caller is willing to keep trying to reach a route, taken from
/// what it declared rather than from anything this code chose. `connect_ms`
/// pays for reaching a channel once it exists; `routing_retry_ms` is the
/// caller's own statement of how long it will keep looking for one, and a
/// writer's claim window is exactly that kind of looking.
fn claim_window_budget(timeouts: ReadClientTimeouts) -> u64 {
    timeouts.routing_retry_ms
}

/// Try one route, and — for a caller that asked for the owner alone — wait out
/// a writer that is holding the store with its decision unpublished.
///
/// A store nobody owns still answers owner-absent here, immediately. What
/// changes is a store somebody DOES own: a claim that has said nothing yet is
/// re-observed once, through the caller's own declared budget, and the answer
/// is the writer's own — or, if the budget runs out with the claim still held,
/// the not-serving answer a live holder deserves. Never owner-absent, because
/// that answer is a consumer's licence to take a store this one is holding.
#[allow(clippy::too_many_arguments)]
fn attempt_route(
    path: &Path,
    options: &ReadSessionOptions,
    runtime_dir: Option<&Path>,
    requirement: RouteRequirement,
    attempt: u64,
    resources: &Arc<RouteResources>,
    mismatch: Option<OwnerHandshakeMismatchForTest>,
    observe: &dyn Fn(ReadSessionEvent),
) -> std::result::Result<SelectedRoute, RouteOpenRefusal> {
    // The holder has finished all writes and reclamation before publishing
    // this state. Its trusted companion record and live lock jointly attest
    // that a direct snapshot is safe even while accepted channel reads drain.
    if requirement == RouteRequirement::OwnerOrFile && owner_is_quiescent(path) {
        return open_direct_route(path, options, runtime_dir, resources, attempt, observe);
    }
    let mut waited = false;
    loop {
        let refusal = match attempt_route_once(
            path,
            options,
            runtime_dir,
            requirement,
            attempt,
            resources,
            mismatch,
            observe,
        ) {
            Ok(selected) => return Ok(selected),
            Err(refusal) => refusal,
        };
        if refusal.failure != ReadRouteOpenFailure::OwnerRequired {
            return Err(refusal);
        }
        // Nothing in the record named an owner that could be asked. Whether
        // this store is genuinely unowned is a different question, and the
        // companion beside it — never the store file — answers it.
        let Some(claim) = crate::persistence::observe_unsettled_claim(path) else {
            return Err(refusal);
        };
        if waited {
            return Err(RouteOpenRefusal {
                failure: ReadRouteOpenFailure::OwnerHeldWithoutDecision,
                error: Error::ReadFailure(owner_holding_without_a_decision()),
            });
        }
        waited = true;
        observe(ReadSessionEvent::ClaimWindowWait { attempt });
        match crate::persistence::wait_for_claim_settlement(
            claim,
            claim_window_budget(options.timeouts),
            claim_window_clock().as_ref(),
        ) {
            crate::persistence::ClaimSettlement::Settled => continue,
            crate::persistence::ClaimSettlement::StillHeld => {
                return Err(RouteOpenRefusal {
                    failure: ReadRouteOpenFailure::OwnerHeldWithoutDecision,
                    error: Error::ReadFailure(owner_holding_without_a_decision()),
                });
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn attempt_route_once(
    path: &Path,
    options: &ReadSessionOptions,
    runtime_dir: Option<&Path>,
    requirement: RouteRequirement,
    attempt: u64,
    resources: &Arc<RouteResources>,
    mismatch: Option<OwnerHandshakeMismatchForTest>,
    observe: &dyn Fn(ReadSessionEvent),
) -> std::result::Result<SelectedRoute, RouteOpenRefusal> {
    let published = published_owner_channel(path);
    if requirement == RouteRequirement::OwnerOrFile && owner_is_quiescent(path) {
        return open_direct_route(path, options, runtime_dir, resources, attempt, observe);
    }
    let owner_channel = match published {
        Some((identity, writer_run, owner_user, address)) => {
            let runtime = read_runtime_directory(owner_user, runtime_dir).map_err(|error| {
                RouteOpenRefusal {
                    failure: ReadRouteOpenFailure::OwnerAuthentication,
                    error,
                }
            })?;
            let channel_path = crate::local_transport::channel_socket_path(runtime.path(), address)
                .map_err(|error| RouteOpenRefusal {
                    failure: ReadRouteOpenFailure::OwnerAuthentication,
                    error: Error::Other(error.to_string()),
                })?;
            // A published channel that is absent or inaccessible is still a
            // candidate whose live companion state must be classified. If it
            // is filtered out here, a live holder can be reported absent or
            // read around merely because its socket pathname disappeared.
            Some((
                identity,
                writer_run,
                owner_user,
                address,
                channel_path,
                runtime.path().to_path_buf(),
            ))
        }
        None => None,
    };
    // The test-seam field retains its established meaning: whether a channel
    // pathname is presently available. A durable published candidate whose
    // pathname is absent is still classified below; it is no longer dropped
    // from the production decision merely to keep this observation `false`.
    let owner_available = owner_channel
        .as_ref()
        .is_some_and(|(_, _, _, _, channel_path, _)| channel_path.exists());
    observe(ReadSessionEvent::OwnerResolution {
        attempt,
        owner_available,
    });

    if let Some((identity, writer_run, owner_user, address, channel_path, runtime_path)) =
        owner_channel
    {
        // Cleanup after a failed dial may remove only the exact channel entry
        // this attempt observed before asking it. Capturing after the dial
        // would let a replacement that arrived in between be mistaken for
        // the abandoned entry.
        #[cfg(unix)]
        let channel_identity =
            crate::local_transport::channel_filesystem_identity(&channel_path).ok();
        // What this session declared travels with the handshake, once, so the
        // owner narrows the handle it will serve this whole connection from
        // rather than being asked again per statement.
        let declared = declared_visibility(options);
        let presented = match mismatch {
            Some(OwnerHandshakeMismatchForTest::DatabaseIdentity) => {
                crate::local_transport::LocalHandshake::current(
                    mismatched_database_identity(identity),
                    writer_run,
                    owner_user,
                )
                .declaring(declared)
            }
            None => {
                crate::local_transport::LocalHandshake::current(identity, writer_run, owner_user)
                    .declaring(declared)
            }
        };
        match wait_for_local_answer(crate::owner_read::OwnerClient::connect(
            &channel_path,
            presented,
            options.timeouts,
            read_clock(),
        )) {
            Ok(client) => {
                // An owner that is winding down accepts the connection and
                // then refuses every statement. Selecting it would hand the
                // caller a session that cannot read a single row and would
                // hide the reason behind the first failed read, so its own
                // word about itself is the answer to this open.
                if let Some(status) = client.accepted_status()
                    && status.state != contextdb_core::read_contract::OwnerServingState::Serving
                {
                    if requirement == RouteRequirement::OwnerOrFile && owner_is_quiescent(path) {
                        return open_direct_route(
                            path,
                            options,
                            runtime_dir,
                            resources,
                            attempt,
                            observe,
                        );
                    }
                    return Err(RouteOpenRefusal {
                        failure: ReadRouteOpenFailure::OwnerAuthentication,
                        error: Error::ReadFailure(owner_not_serving()),
                    });
                }
                *resources
                    .channel_identity
                    .lock()
                    .expect("route channel identity") = Some(address);
                #[cfg(feature = "test-seams")]
                {
                    *resources
                        .channel_source_items
                        .lock()
                        .expect("route channel source counter") =
                        crate::read_probe::owner_route_source_counter(address);
                }
                observe(ReadSessionEvent::LocalChannelOperation);
                return Ok(SelectedRoute::Owner(OwnerRoute {
                    path: path.to_path_buf(),
                    runtime_dir: runtime_path,
                    identity,
                    writer_run,
                    client: Mutex::new(Box::new(client)),
                    resources: Arc::clone(resources),
                }));
            }
            // Nothing answered at the pathname, so there is no owner here --
            // a writer that went away without taking its channel down leaves
            // exactly this behind, and the store beside it is a perfectly
            // readable idle file. The dial says so in its own words, which is
            // what keeps this apart from an owner that answered and refused
            // while it still owns the store.
            Err(error) if nobody_answered_the_channel(&error) => {
                #[cfg(unix)]
                reclaim_abandoned_channel(&channel_path, channel_identity);
                #[cfg(not(unix))]
                reclaim_abandoned_channel(&channel_path);

                // The dial found no listener; the companion decides whether
                // that means an idle file or a live holder whose channel is
                // unavailable. Its record and hold are observed together, so
                // a departed writer's durable state can never speak for an
                // idle store.
                if let Some(status) = crate::persistence::recorded_held_owner_status(path) {
                    use contextdb_core::read_contract::OwnerServingState;
                    match status.state {
                        OwnerServingState::Serving => {
                            return Err(RouteOpenRefusal {
                                failure: ReadRouteOpenFailure::OwnerAuthentication,
                                error: Error::ReadFailure(owner_not_serving()),
                            });
                        }
                        OwnerServingState::ServingDisabled | OwnerServingState::NotServing => {
                            if requirement == RouteRequirement::OwnerOrFile
                                && owner_is_quiescent(path)
                            {
                                return open_direct_route(
                                    path,
                                    options,
                                    runtime_dir,
                                    resources,
                                    attempt,
                                    observe,
                                );
                            }
                            return Err(RouteOpenRefusal {
                                failure: ReadRouteOpenFailure::OwnerAuthentication,
                                error: Error::ReadFailure(owner_not_serving_because(&status)),
                            });
                        }
                        // This is the placeholder a writer publishes while
                        // its claim window is still open. The owner-only door
                        // below re-observes that window through the caller's
                        // declared budget; an ordinary read retains its
                        // existing direct-lock race path.
                        OwnerServingState::NotApplicable => {}
                    }
                }
                match crate::persistence::observed_writer_hold(path) {
                    Some(true) => {
                        // If this is the undecided claim window, let the
                        // owner-only path below wait for the writer's actual
                        // decision. Otherwise the settled fact is simply that
                        // a live holder has no usable channel.
                        if requirement != RouteRequirement::OwnerOnly
                            || crate::persistence::observe_unsettled_claim(path).is_none()
                        {
                            return Err(RouteOpenRefusal {
                                failure: ReadRouteOpenFailure::OwnerAuthentication,
                                error: Error::ReadFailure(owner_not_serving()),
                            });
                        }
                    }
                    Some(false) => {}
                    None => {
                        // This attempt began from a trusted published owner
                        // candidate. If its companion can no longer prove
                        // either a live holder or an idle store, that change
                        // is indeterminate and cannot license opening the
                        // file. Companionless and untrusted idle stores still
                        // take the direct route when no candidate was
                        // discoverable in the first place.
                        return Err(RouteOpenRefusal {
                            failure: ReadRouteOpenFailure::OwnerAuthentication,
                            error: Error::ReadFailure(owner_disconnected()),
                        });
                    }
                }
            }
            // `OwnerClient::connect` returns a client only after the peer has
            // authenticated and answered OwnerStatus. Every error in this
            // arm is therefore a pathname that did not establish an owner.
            // A durable published identity cannot make that pathname live:
            // when nobody holds the companion lock, the existing idle-file
            // route is the answer. If ownership races in after this probe,
            // the direct reader's lock refuses it and the one sanctioned
            // ownership-race retry asks again.
            Err(_error)
                if requirement == RouteRequirement::OwnerOrFile
                    && crate::persistence::observed_writer_hold(path) == Some(false) => {}
            Err(error) => {
                // A writer whose own channel startup failed can share the
                // pathname with the live or malformed responder that caused
                // that failure. The authenticated companion hold and the
                // writer's durable decision are the authoritative answer in
                // that case; the unrelated responder must not hide it behind
                // a handshake error.
                if let Some(status) = crate::persistence::recorded_unserved_owner(path) {
                    if requirement == RouteRequirement::OwnerOrFile && owner_is_quiescent(path) {
                        return open_direct_route(
                            path,
                            options,
                            runtime_dir,
                            resources,
                            attempt,
                            observe,
                        );
                    }
                    return Err(RouteOpenRefusal {
                        failure: ReadRouteOpenFailure::OwnerAuthentication,
                        error: Error::ReadFailure(owner_not_serving_because(&status)),
                    });
                }
                return Err(RouteOpenRefusal {
                    failure: ReadRouteOpenFailure::OwnerAuthentication,
                    error: owner_authentication_error(owner_error(error)),
                });
            }
        }
    }

    // Nobody answered on a channel, but the store's companion may still name a
    // writer that IS holding it and has recorded why it cannot be asked. That
    // recorded word is the answer this reader is owed, and it is owed to EVERY
    // caller: an owner-only caller told "not running" about a live writer that
    // simply will not serve has been told the store is idle when a process is
    // holding it. Nothing here touches the store file -- the companion beside
    // it carries both the record and the lock that proves the writer is still
    // there -- so the owner-only promise to leave the file alone stands.
    // A store nobody owns has no such writer; the record alone never refuses
    // anyone.
    if let Some(status) = crate::persistence::recorded_unserved_owner(path) {
        if requirement == RouteRequirement::OwnerOrFile && owner_is_quiescent(path) {
            return open_direct_route(path, options, runtime_dir, resources, attempt, observe);
        }
        return Err(RouteOpenRefusal {
            failure: ReadRouteOpenFailure::OwnerAuthentication,
            error: Error::ReadFailure(owner_not_serving_because(&status)),
        });
    }

    // Nobody owns this store. A caller that asked for the owner alone is
    // answered here, before the committed file is looked at in any way: no
    // hydration, no reader breadcrumb, no lock, nothing on disk touched. The
    // file's own condition is deliberately not consulted, so a store that
    // would need a writable repair says the same plain thing as an idle one --
    // there is no owner to ask.
    if requirement == RouteRequirement::OwnerOnly {
        return Err(RouteOpenRefusal {
            failure: ReadRouteOpenFailure::OwnerRequired,
            error: Error::ReadFailure(owner_not_running()),
        });
    }

    open_direct_route(path, options, runtime_dir, resources, attempt, observe)
}

fn owner_is_quiescent(path: &Path) -> bool {
    crate::persistence::recorded_held_owner_status(path).is_some_and(|status| {
        status.state == contextdb_core::read_contract::OwnerServingState::NotServing
            && status.reason
                == Some(contextdb_core::read_contract::OwnerServingReason::ShutdownDraining)
    })
}

fn open_direct_route(
    path: &Path,
    options: &ReadSessionOptions,
    runtime_dir: Option<&Path>,
    resources: &Arc<RouteResources>,
    attempt: u64,
    observe: &dyn Fn(ReadSessionEvent),
) -> std::result::Result<SelectedRoute, RouteOpenRefusal> {
    observe(ReadSessionEvent::BeforeDirectBackendOpen { attempt });
    resources.note_direct_backend_open();
    #[cfg(feature = "test-seams")]
    let coordination_runtime = runtime_dir
        .map(Path::to_path_buf)
        .or_else(runtime_directory_override_for_test);
    #[cfg(not(feature = "test-seams"))]
    let coordination_runtime = runtime_dir.map(Path::to_path_buf);
    // The declaration this session opened with travels with the reader, so
    // the committed image it hydrates is narrowed before a single row is
    // answered from it.
    let mut config =
        crate::direct_file_reader::DirectReaderConfig::new(options.limits, read_clock()).declaring(
            options.contexts.clone(),
            options.scope_labels.clone(),
            options.principal.clone(),
        );
    config.memory_limit = options.memory_limit;
    config.runtime_dir = coordination_runtime;
    match crate::direct_file_reader::DirectFileReader::open(path, config) {
        Ok(reader) => {
            observe(ReadSessionEvent::DirectBackendOpen);
            Ok(SelectedRoute::File(Box::new(reader)))
        }
        Err(error) => Err(classify_direct_open_failure(path, error)),
    }
}

/// What this session declared about the visibility it reads under, in the
/// shape the local channel carries.
///
/// A session that declared nothing produces nothing: the handshake stays the
/// undeclared one, and the owner serves it exactly as it always has.
fn declared_visibility(
    options: &ReadSessionOptions,
) -> Option<crate::local_transport::LocalReadDeclaration> {
    if options.contexts.is_none() && options.scope_labels.is_none() && options.principal.is_none() {
        return None;
    }
    Some(crate::local_transport::LocalReadDeclaration {
        contexts: options.contexts.clone(),
        scope_labels: options.scope_labels.clone(),
        principal: options.principal.clone(),
    })
}

/// Whether the dial found nobody at all at the pathname.
///
/// This is the connect step's own answer, not an interpretation of a later
/// failure: a channel that answered and then stopped is a disconnection from a
/// real owner, and an owner that answered and refused has given its answer.
/// Only "nothing was there" sends a reader to the file.
fn nobody_answered_the_channel(error: &crate::owner_read::OwnerReadScaffoldError) -> bool {
    matches!(
        error,
        crate::owner_read::OwnerReadScaffoldError::Refused(failure)
            if failure.kind() == contextdb_core::read_contract::ReadFailureKind::OwnerNotRunning
    )
}

/// Take down a channel entry nobody is listening on.
///
/// Removal is bound to the exact object just probed, so a channel somebody
/// else has since bound at the same pathname is preserved. Failing to reclaim
/// costs the next reader one refused dial, not an answer, so this is best
/// effort by design.
#[cfg(unix)]
fn reclaim_abandoned_channel(
    channel_path: &Path,
    observed_identity: Option<crate::local_transport::ChannelFilesystemIdentity>,
) {
    if let Some(identity) = observed_identity {
        let _ = crate::local_transport::remove_own_bound_channel(channel_path, identity);
    }
}

#[cfg(not(unix))]
fn reclaim_abandoned_channel(_channel_path: &Path) {}

#[cfg(all(test, unix))]
mod abandoned_channel_cleanup_tests {
    use super::*;

    #[test]
    fn a_replacement_arriving_after_the_dial_observation_is_never_reclaimed_as_the_old_channel() {
        let root = tempfile::tempdir().expect("task-scoped channel-cleanup root");
        let channel = root.path().join("published.sock");
        let staged = root.path().join("replacement.sock");

        let abandoned = std::os::unix::net::UnixListener::bind(&channel)
            .expect("bind the channel this route attempt observed");
        let observed = crate::local_transport::channel_filesystem_identity(&channel)
            .expect("capture the observed channel identity before the dial");
        drop(abandoned);

        let replacement =
            std::os::unix::net::UnixListener::bind(&staged).expect("bind a replacement channel");
        let replacement_identity = crate::local_transport::channel_filesystem_identity(&staged)
            .expect("inspect the replacement identity");
        std::fs::rename(&staged, &channel)
            .expect("replace the abandoned pathname after the dial observation");

        reclaim_abandoned_channel(&channel, Some(observed));

        assert_eq!(
            crate::local_transport::channel_filesystem_identity(&channel)
                .expect("the replacement remains at the published pathname"),
            replacement_identity,
            "cleanup may remove only the socket observed before the failed dial"
        );
        let connected = std::os::unix::net::UnixStream::connect(&channel)
            .expect("the preserved replacement remains reachable");
        drop(connected);
        drop(replacement);
    }
}

/// Why a direct open stopped, and whether asking again could answer it.
fn classify_direct_open_failure(
    path: &Path,
    error: crate::direct_file_reader::DirectFileReaderError,
) -> RouteOpenRefusal {
    use crate::direct_file_reader::DirectFileReaderError as Direct;
    let failure = match &error {
        // Somebody took the store while this reader was on its way to it.
        // That somebody is the owner now, and the owner is who to ask.
        Direct::Contended { .. } => ReadRouteOpenFailure::WriterBecameOwner,
        Direct::DirectReadRequiresWriter { .. } => ReadRouteOpenFailure::DirectReadRequiresWriter,
        Direct::CorruptStore(..) | Direct::LegacyLayout(..) => ReadRouteOpenFailure::StoreDamage,
        _ => ReadRouteOpenFailure::DirectOpen,
    };
    RouteOpenRefusal {
        failure,
        error: direct_error(path, error),
    }
}

/// An identity that is deliberately not this store's, used only to put a real
/// handshake refusal in front of a real owner. Only a proof can ask for one --
/// the door that supplies the mismatch is behind the test-seams feature -- but
/// the route code that answers it is the same in every build.
fn mismatched_database_identity(
    identity: contextdb_core::read_contract::DatabaseIdentity,
) -> contextdb_core::read_contract::DatabaseIdentity {
    let mut bytes = identity.0;
    bytes[0] = bytes[0].wrapping_add(1);
    contextdb_core::read_contract::DatabaseIdentity(bytes)
}

/// A refused handshake is the owner saying "you are not asking me about the
/// store I hold." It is terminal for this selection: the reader does not go
/// around the owner to the file.
fn owner_authentication_error(error: Error) -> Error {
    match error {
        Error::ReadFailure(failure) => Error::ReadFailure(failure),
        _ => Error::ReadFailure(
            contextdb_core::read_contract::ReadFailure::new(
                contextdb_core::read_contract::ReadFailureKind::OwnerMismatch,
                contextdb_core::read_contract::ReadFailureDetail::None,
            )
            .expect("an owner-mismatch refusal carries no further detail"),
        ),
    }
}

fn current_read_user()
-> std::result::Result<contextdb_core::read_contract::LocalUserIdentity, Error> {
    use crate::local_transport::RuntimeDirectoryEnvironment;
    crate::local_transport::ProcessRuntimeDirectoryEnvironment
        .effective_user_identity()
        .map_err(|error| Error::Other(error.to_string()))
}

#[cfg(feature = "test-seams")]
thread_local! {
    static SESSION_KERNEL_PROBE: std::cell::RefCell<
        Option<Arc<dyn crate::executor::BoundedExecutionProbe>>,
    > = const { std::cell::RefCell::new(None) };
}

#[cfg(feature = "test-seams")]
struct SessionKernelProbeGuard {
    previous: Option<Arc<dyn crate::executor::BoundedExecutionProbe>>,
}

#[cfg(feature = "test-seams")]
impl Drop for SessionKernelProbeGuard {
    fn drop(&mut self) {
        let previous = self.previous.take();
        SESSION_KERNEL_PROBE.with(|slot| {
            let _ = slot.replace(previous);
        });
    }
}

/// Run one committed-file operation with this session's kernel observer in
/// force.
///
/// The direct backend reaches the bounded kernel through several of its own
/// doors, and a route may not reach past them; so the observer is left where
/// the kernel itself picks it up, for exactly the span of one operation, and
/// is restored even if that operation panics.
#[cfg(feature = "test-seams")]
pub(crate) fn with_session_kernel_probe<T>(
    probe: Option<Arc<dyn crate::executor::BoundedExecutionProbe>>,
    operation: impl FnOnce() -> T,
) -> T {
    let previous = SESSION_KERNEL_PROBE.with(|slot| slot.replace(probe));
    let _guard = SessionKernelProbeGuard { previous };
    operation()
}

/// Run one session operation with THIS route's source-item counter in force.
///
/// The kernel counts the items it finishes inspecting against whichever
/// counter the thread it runs on is carrying, so a route that wants to know
/// what its own reads did puts its counter there for exactly the span of one
/// operation, and takes it back afterwards even if the operation panics.
#[cfg(feature = "test-seams")]
fn with_route_source_counter<T>(
    resources: &Arc<RouteResources>,
    operation: impl FnOnce() -> T,
) -> T {
    struct Restore {
        previous: Option<Arc<std::sync::atomic::AtomicU64>>,
    }
    impl Drop for Restore {
        fn drop(&mut self) {
            crate::read_probe::install_session_source_counter(self.previous.take());
        }
    }
    let previous = crate::read_probe::install_session_source_counter(Some(Arc::clone(
        &resources.bounded_source_items,
    )));
    let _restore = Restore { previous };
    operation()
}

#[cfg(not(feature = "test-seams"))]
fn with_route_source_counter<T>(
    _resources: &Arc<RouteResources>,
    operation: impl FnOnce() -> T,
) -> T {
    operation()
}

/// The kernel observer in force for the operation this thread is running.
/// Only the bounded kernel's own callers read this.
#[cfg(feature = "test-seams")]
pub(crate) fn session_kernel_probe_for_test()
-> Option<Arc<dyn crate::executor::BoundedExecutionProbe>> {
    SESSION_KERNEL_PROBE.with(|slot| slot.borrow().clone())
}

/// A route-stable bounded reading handle.
pub struct ReadSession {
    route: ReadRoute,
    options: ReadSessionOptions,
    /// The runtime directory this session was opened against, when an operator
    /// supplied one. Kept because the session goes on asking the same
    /// deployment questions -- `.owner status` reaches the same owner the
    /// reads do -- and both must look in the one place the operator named.
    runtime_dir: Option<PathBuf>,
    state: Arc<ReadSessionState>,
    resources: Arc<RouteResources>,
    /// Whoever asked to be told what this session's reads are doing while they
    /// run. Held by the session rather than by its options because an
    /// observer is a live caller, not a setting.
    progress: Option<Arc<dyn ReadProgressObserver>>,
    /// When this session took its view of the store, for a route that has one.
    ///
    /// Stamped here rather than inside the direct reader because that module
    /// is deliberately closed over a small set of dependencies and a clock is
    /// not one of them -- and because the question is about this session's
    /// view, which is exactly what route selection has just finished
    /// producing.
    snapshot_at: Option<contextdb_core::Wallclock>,
    #[cfg(feature = "test-seams")]
    observer: Option<Arc<dyn ReadSessionTestObserver>>,
    #[cfg(feature = "test-seams")]
    kernel_observer: Option<Arc<dyn ReadKernelTestObserver>>,
    /// A clock this session's own reads deadline against, so a test can move
    /// time deliberately instead of waiting for it. The owner route already
    /// takes a clock; a live-database session hardcoded the monotonic one,
    /// which left its deadline behaviour unreachable from a test.
    #[cfg(feature = "test-seams")]
    clock: Option<Arc<dyn contextdb_core::read_contract::DeadlineClock>>,
}

enum ReadSessionState {
    /// The reader is inside the writer's own process, so the route is the
    /// owner's live state reached directly rather than over a channel.
    LiveDatabase(Arc<Database>),
    /// Nobody owns the store, so the reader reads the committed file itself.
    DirectFile(Box<crate::direct_file_reader::DirectFileReader>),
    /// Statements use this owner channel. A cursor which already published
    /// its own snapshot registration can continue that root after owner loss.
    OwnerChannel(OwnerRoute),
}

/// The reader's end of one selected owner channel.
struct OwnerRoute {
    path: PathBuf,
    runtime_dir: PathBuf,
    identity: contextdb_core::read_contract::DatabaseIdentity,
    writer_run: contextdb_core::read_contract::WriterRunNumber,
    /// Boxed so the selected-route vocabulary stays cheap to move: one owner
    /// connection carries its whole protocol state, and every route value --
    /// including the file one -- would otherwise be sized by it.
    client: Mutex<Box<crate::owner_read::OwnerClient>>,
    resources: Arc<RouteResources>,
}

/// Turn what the bounded kernel refused into what a caller of this module
/// sees. A refusal keeps the ceiling that stopped it, a cancellation stays a
/// cancellation rather than becoming a fault, and an engine answer arrives in
/// the engine's own words.
fn read_error(error: crate::executor::BoundedExecutionError) -> Error {
    match error {
        crate::executor::BoundedExecutionError::Refused(failure) => Error::ReadFailure(failure),
        crate::executor::BoundedExecutionError::Cancelled => Error::ReadCancelled,
        crate::executor::BoundedExecutionError::Engine(error) => error,
        crate::executor::BoundedExecutionError::Unimplemented => Error::ReadSessionNotImplemented,
    }
}

/// Wrap one session observer as the bounded kernel's own probe for a single
/// operation, so what the observer sees is what the kernel actually did.
#[cfg(feature = "test-seams")]
fn observed_kernel_probe(
    observer: &Arc<dyn ReadKernelTestObserver>,
    operation: ReadSessionOperation,
    route: ReadRoute,
    cancellation: &OwnerReadCancellation,
) -> Arc<dyn crate::executor::BoundedExecutionProbe> {
    crate::executor::bounded_read_test_support::kernel_probe(kernel_probe_for_test(
        Arc::clone(observer),
        operation,
        route,
        cancellation,
    ))
}

impl ReadSession {
    /// The clock this session deadlines against: whatever a test installed,
    /// otherwise the monotonic one every live read has always used.
    fn session_clock(&self) -> Arc<dyn contextdb_core::read_contract::DeadlineClock> {
        #[cfg(feature = "test-seams")]
        if let Some(clock) = self.clock.as_ref() {
            return Arc::clone(clock);
        }
        read_clock()
    }
}

pub(crate) fn read_clock() -> Arc<dyn contextdb_core::read_contract::DeadlineClock> {
    #[cfg(feature = "test-seams")]
    if let Some(clock) = ROUTE_SELECTION_CLOCK.with(|slot| slot.borrow().clone()) {
        return clock;
    }
    Arc::new(crate::local_transport::MonotonicDeadlineClock::new())
}

/// One live-state cursor, holding the kernel continuation that produced its
/// first page. It owns that continuation outright, so the handle that opened
/// it may go away without ending the cursor.
struct LiveCursorExecution {
    database: Arc<Database>,
    handle: crate::executor::BoundedCursorHandle,
    #[cfg(feature = "test-seams")]
    kernel_observer: Option<Arc<dyn ReadKernelTestObserver>>,
}

impl OwnedCursorExecution for LiveCursorExecution {
    fn work_observation(&mut self) -> Result<Option<crate::executor::CursorObservation>> {
        self.handle.work_observation().map_err(read_error)
    }

    fn fetch(
        &mut self,
        rows: Option<NonZeroUsize>,
        cancellation: &OwnerReadCancellation,
    ) -> Result<ObservedCursorPage> {
        #[cfg(feature = "test-seams")]
        let probe = self.kernel_observer.as_ref().map(|observer| {
            observed_kernel_probe(
                observer,
                ReadSessionOperation::CursorFetch,
                ReadRoute::Owner,
                cancellation,
            )
        });
        let fetched = crate::executor::fetch_bounded_cursor(
            &mut self.handle,
            rows,
            cancellation.clone(),
            #[cfg(feature = "test-seams")]
            probe,
        )
        .map_err(read_error)?;
        self.database.publish_authoritative_read(
            &fetched.publication,
            |error| error,
            || {
                Ok(ObservedCursorPage {
                    cancelled: false,
                    page: fetched.page,
                    observation: fetched.observation,
                    owner_trace_charge: None,
                })
            },
        )
    }

    fn close(&mut self) -> Result<()> {
        crate::executor::close_bounded_cursor(&mut self.handle).map_err(read_error)
    }

    fn is_live(&self) -> bool {
        crate::executor::bounded_cursor_is_live(&self.handle)
    }
}

impl ReadSession {
    /// Run an operation through the normal route doors while resolving local
    /// channels below an explicit secure test runtime root. The scoped value
    /// is restored even when the operation panics.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn with_runtime_directory_for_test<T>(
        runtime_dir: impl AsRef<Path>,
        operation: impl FnOnce() -> T,
    ) -> T {
        let previous = READ_RUNTIME_DIRECTORY_OVERRIDE
            .with(|slot| slot.replace(Some(runtime_dir.as_ref().to_path_buf())));
        let _guard = ReadRuntimeDirectoryOverrideGuard { previous };
        operation()
    }

    /// Open a read-only session by selecting the owner or file route once.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, ReadSessionOptions::default())
    }

    /// Open a read-only session with caller-requested bounds and deadlines.
    pub fn open_with_options(path: impl AsRef<Path>, options: ReadSessionOptions) -> Result<Self> {
        Self::select(
            path.as_ref(),
            options,
            None,
            RouteRequirement::OwnerOrFile,
            None,
            None,
            None,
            None,
        )
    }

    /// Open a read-only session that will reach this store's live owner or
    /// nothing at all.
    ///
    /// Some questions only an owner can answer -- what the running process is
    /// doing, whether it is serving yet, anything asked of it directly -- and
    /// the committed file has no answer to give. This door is for the caller
    /// asking those: it takes the owner route when a process owns the store,
    /// and when none does it says plainly that the owner is not running.
    ///
    /// It never opens the store's file. That is the point of it rather than a
    /// detail of it. Reading the committed image publishes a reader that a
    /// writer starting beside it must wait for, so a readiness probe that
    /// fell through to the file would stand in the way of the very process it
    /// is waiting for; and a file that cannot be read directly would answer
    /// with the file's condition instead of the plain fact that nobody owns
    /// the store. Both are avoided by not looking at the file: a store nobody
    /// owns is owner-absent here, whatever shape its file is in.
    ///
    /// A caller that wants the store's committed rows when there is no owner
    /// opens an ordinary session instead, which reads the file exactly as it
    /// always has.
    pub fn open_owner_only(path: impl AsRef<Path>, options: ReadSessionOptions) -> Result<Self> {
        Self::open_owner_only_in_runtime_dir(path, options, None)
    }

    /// The owner-only open, looking for the owner's channel in a runtime
    /// directory the operator supplied -- see [`Self::open_owner_only`] and
    /// [`Self::open_in_runtime_dir`].
    ///
    /// The same one directory has to serve both sides here too: a caller that
    /// asks only the owner, in a container or packaged service that names its
    /// runtime directory, would otherwise be told the owner is not running
    /// about the very owner it was pointed at.
    ///
    /// `None` is the ordinary open: the platform's own runtime location.
    pub fn open_owner_only_in_runtime_dir(
        path: impl AsRef<Path>,
        options: ReadSessionOptions,
        runtime_dir: Option<PathBuf>,
    ) -> Result<Self> {
        Self::select(
            path.as_ref(),
            options,
            runtime_dir,
            RouteRequirement::OwnerOnly,
            None,
            None,
            None,
            None,
        )
    }

    /// Open a read-only session that reports what its reads are doing while
    /// they run.
    ///
    /// Opening a store is itself one of the slow things a caller waits on --
    /// the committed image is loaded here -- so the observer is in force for
    /// this call as well as for every statement the session goes on to run.
    pub fn open_with_progress(
        path: impl AsRef<Path>,
        options: ReadSessionOptions,
        progress: Arc<dyn ReadProgressObserver>,
    ) -> Result<Self> {
        Self::select(
            path.as_ref(),
            options,
            None,
            RouteRequirement::OwnerOrFile,
            None,
            None,
            None,
            Some(progress),
        )
    }

    /// Open a read-only session that looks for the owner's channel in a
    /// runtime directory the operator supplied.
    ///
    /// A container, a packaged service, or a Home Assistant add-on has no
    /// platform runtime location to fall back on, so it names one — and that
    /// one directory has to serve BOTH sides. A writer opened with the same
    /// directory puts its channel there; a reader given it looks there. A root
    /// only the writer honored would mean a packaged deployment could start a
    /// writer nobody is ever able to inspect.
    ///
    /// `None` is the ordinary open: the platform's own runtime location.
    pub fn open_in_runtime_dir(
        path: impl AsRef<Path>,
        options: ReadSessionOptions,
        runtime_dir: Option<PathBuf>,
    ) -> Result<Self> {
        Self::select(
            path.as_ref(),
            options,
            runtime_dir,
            RouteRequirement::OwnerOrFile,
            None,
            None,
            None,
            None,
        )
    }

    /// The supplied-runtime-directory open, reporting what it is doing while
    /// it loads — see [`Self::open_in_runtime_dir`] and
    /// [`Self::open_with_progress`].
    pub fn open_with_progress_in_runtime_dir(
        path: impl AsRef<Path>,
        options: ReadSessionOptions,
        runtime_dir: Option<PathBuf>,
        progress: Arc<dyn ReadProgressObserver>,
    ) -> Result<Self> {
        Self::select(
            path.as_ref(),
            options,
            runtime_dir,
            RouteRequirement::OwnerOrFile,
            None,
            None,
            None,
            Some(progress),
        )
    }

    /// Open through the production route selector with deterministic route
    /// observation enabled.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn open_with_observer_for_test(
        path: impl AsRef<Path>,
        options: ReadSessionOptions,
        observer: Arc<dyn ReadSessionTestObserver>,
    ) -> Result<Self> {
        Self::select(
            path.as_ref(),
            options,
            None,
            RouteRequirement::OwnerOrFile,
            Some(observer),
            None,
            None,
            None,
        )
    }

    /// Open through the production route selector while a proof drives every
    /// local-channel deadline with its manual clock.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn open_with_clock_for_test(
        path: impl AsRef<Path>,
        options: ReadSessionOptions,
        clock: Arc<dyn contextdb_core::read_contract::DeadlineClock>,
    ) -> Result<Self> {
        let session_clock = Arc::clone(&clock);
        let mut session = with_route_selection_clock_for_test(clock, || {
            Self::select(
                path.as_ref(),
                options,
                None,
                RouteRequirement::OwnerOrFile,
                None,
                None,
                None,
                None,
            )
        })?;
        session.clock = Some(session_clock);
        Ok(session)
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn open_with_clock_and_observer_for_test(
        path: impl AsRef<Path>,
        options: ReadSessionOptions,
        clock: Arc<dyn contextdb_core::read_contract::DeadlineClock>,
        observer: Arc<dyn ReadSessionTestObserver>,
    ) -> Result<Self> {
        let session_clock = Arc::clone(&clock);
        let mut session = with_route_selection_clock_for_test(clock, || {
            Self::select(
                path.as_ref(),
                options,
                None,
                RouteRequirement::OwnerOrFile,
                Some(observer),
                None,
                None,
                None,
            )
        })?;
        session.clock = Some(session_clock);
        Ok(session)
    }

    /// The owner-only door, opened through the production route selector
    /// with deterministic route observation enabled.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn open_owner_only_with_observer_for_test(
        path: impl AsRef<Path>,
        options: ReadSessionOptions,
        observer: Arc<dyn ReadSessionTestObserver>,
    ) -> Result<Self> {
        Self::select(
            path.as_ref(),
            options,
            None,
            RouteRequirement::OwnerOnly,
            Some(observer),
            None,
            None,
            None,
        )
    }

    /// The owner-only door, deadlining its claim-window wait against a
    /// clock the proof supplies rather than against real time.
    ///
    /// A claim window expires on the caller's declared `routing_retry_ms`,
    /// and that is a declared time value like every other in this work: it
    /// is proven by advancing a manual clock, never by sleeping. Route
    /// selection therefore has to carry this clock into the claim wait, so
    /// the wait deadlines against it instead of against `Instant::now()`.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn open_owner_only_with_clock_for_test(
        path: impl AsRef<Path>,
        options: ReadSessionOptions,
        clock: Arc<dyn contextdb_core::read_contract::DeadlineClock>,
    ) -> Result<Self> {
        with_route_selection_clock_for_test(clock, || {
            Self::select(
                path.as_ref(),
                options,
                None,
                RouteRequirement::OwnerOnly,
                None,
                None,
                None,
                None,
            )
        })
    }

    /// Traverse the normal owner route while presenting one deliberately
    /// mismatched identity field to its real authentication boundary.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn open_with_owner_handshake_mismatch_for_test(
        path: impl AsRef<Path>,
        options: ReadSessionOptions,
        mismatch: OwnerHandshakeMismatchForTest,
        observer: Arc<dyn ReadSessionTestObserver>,
    ) -> Result<Self> {
        Self::select(
            path.as_ref(),
            options,
            None,
            RouteRequirement::OwnerOrFile,
            Some(observer),
            None,
            Some(mismatch),
            None,
        )
    }

    /// Open through the production route selector while attaching an observer
    /// that can be called only from the selected backend's bounded kernel.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn open_with_kernel_observer_for_test(
        path: impl AsRef<Path>,
        options: ReadSessionOptions,
        observer: Arc<dyn ReadKernelTestObserver>,
    ) -> Result<Self> {
        Self::select(
            path.as_ref(),
            options,
            None,
            RouteRequirement::OwnerOrFile,
            None,
            Some(observer),
            None,
            None,
        )
    }

    /// Compose the existing observers and manual clock without changing route
    /// selection, request semantics or the runtime protocol.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn open_with_observations_for_test(
        path: impl AsRef<Path>,
        options: ReadSessionOptions,
        clock: Arc<dyn DeadlineClock>,
        observer: Option<Arc<dyn ReadSessionTestObserver>>,
        kernel: Option<Arc<dyn ReadKernelTestObserver>>,
    ) -> Result<Self> {
        let session_clock = Arc::clone(&clock);
        let mut session = with_route_selection_clock_for_test(clock, || {
            Self::select(
                path.as_ref(),
                options,
                None,
                RouteRequirement::OwnerOrFile,
                observer,
                kernel,
                None,
                None,
            )
        })?;
        session.clock = Some(session_clock);
        Ok(session)
    }

    /// Only the file backend owns a session-wide committed image today.
    /// Absence on a live-owner route is an observation, never a fabricated root.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn file_snapshot_for_test(&self) -> Option<crate::direct_file_reader::DirectSnapshot> {
        match self.state.as_ref() {
            ReadSessionState::DirectFile(reader) => Some(
                crate::direct_file_reader::test_seams::snapshot_for_test(reader),
            ),
            _ => None,
        }
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn file_storage_registration_for_test(
        &self,
    ) -> Option<crate::direct_file_reader::DirectStorageRegistrationObservation> {
        match self.state.as_ref() {
            ReadSessionState::DirectFile(reader) => {
                Some(crate::direct_file_reader::test_seams::storage_registration_for_test(reader))
            }
            _ => None,
        }
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn file_memory_for_test(
        &self,
    ) -> Option<(
        crate::memory_accounting::MemoryUsage,
        usize,
        usize,
        std::sync::Weak<dyn Send + Sync>,
    )> {
        match self.state.as_ref() {
            ReadSessionState::DirectFile(reader) => {
                let (usage, held, witness) =
                    crate::direct_file_reader::test_seams::image_memory_for_test(reader);
                Some((
                    usage,
                    held,
                    crate::direct_file_reader::test_seams::charged_high_water_for_test(reader),
                    witness,
                ))
            }
            _ => None,
        }
    }

    /// Bytes this session's accountant took through `charge_retained` -- the
    /// writer's unconditional retention door. A read session must report zero:
    /// see `a_read_only_session_pays_nothing_at_the_writers_retention_door`.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn file_cache_index_charge_for_test(&self) -> Option<(usize, usize)> {
        match self.state.as_ref() {
            ReadSessionState::DirectFile(reader) => {
                Some(crate::direct_file_reader::test_seams::cache_index_charge_for_test(reader))
            }
            _ => None,
        }
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn file_retained_charge_bytes_for_test(&self) -> Option<usize> {
        match self.state.as_ref() {
            ReadSessionState::DirectFile(reader) => {
                Some(crate::direct_file_reader::test_seams::retained_charge_bytes_for_test(reader))
            }
            _ => None,
        }
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn file_admitted_bytes_for_test(&self) -> Option<usize> {
        match self.state.as_ref() {
            ReadSessionState::DirectFile(reader) => {
                Some(crate::direct_file_reader::test_seams::charged_admissions_for_test(reader))
            }
            _ => None,
        }
    }

    /// Snapshot real vector generation load/eviction receipts without changing
    /// the file reader's cache state.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn file_vector_passive_activity_for_test(
        &self,
    ) -> Option<contextdb_vector::VectorPassiveActivityCounters> {
        match self.state.as_ref() {
            ReadSessionState::DirectFile(reader) => Some(
                crate::direct_file_reader::test_seams::vector_passive_activity_for_test(reader),
            ),
            _ => None,
        }
    }

    /// The vector store a file-route session searches, so a proof can arm a
    /// store seam on the very reader it measures.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn file_vector_store_for_test(&self) -> Option<Arc<contextdb_vector::VectorStore>> {
        match self.state.as_ref() {
            ReadSessionState::DirectFile(reader) => {
                Some(crate::direct_file_reader::test_seams::vector_store_for_test(reader))
            }
            _ => None,
        }
    }

    /// The bytes this session's current statement is holding in its shared
    /// authenticated proof-node/leaf set (first), and how many of those
    /// bytes are actually accounted for by a session charge or the reader's
    /// free floor (second) -- independent of whatever `charged_bytes`
    /// reports as charged to the declared session.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn file_retained_content_proof_bytes_for_test(&self) -> Option<(usize, usize)> {
        match self.state.as_ref() {
            ReadSessionState::DirectFile(reader) => Some(
                crate::direct_file_reader::test_seams::retained_content_proof_bytes_for_test(
                    reader,
                ),
            ),
            _ => None,
        }
    }

    /// Drop every resident proof chunk WITHOUT resetting this session's own
    /// open statement's retained proof-page set, so a page the statement
    /// copied out of residency stays reachable at exactly its pre-eviction
    /// bytes while the resident set's charge for it disappears -- the
    /// ownership gap a statement-preserving forced eviction needs to observe.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn file_evict_resident_proof_chunks_only_for_test(&self) -> Option<usize> {
        match self.state.as_ref() {
            ReadSessionState::DirectFile(reader) => Some(
                crate::direct_file_reader::test_seams::evict_resident_proof_chunks_only_for_test(
                    reader,
                ),
            ),
            _ => None,
        }
    }

    /// Choose this session's one route.
    ///
    /// The order is fixed: ask the owner if the store says it has one, and
    /// read the file only when it does not -- and only when the caller is
    /// willing to read it, since an owner-only open stops with an owner-absent
    /// answer instead. Exactly one failure may be asked
    /// again -- a writer taking ownership between the moment this reader saw
    /// no owner and the moment it reached the file, because the answer to
    /// that is simply "then ask the owner." Every other failure is the
    /// answer, and the route chosen here is the route for this session's
    /// whole life.
    #[allow(clippy::too_many_arguments)]
    fn select(
        path: &Path,
        options: ReadSessionOptions,
        runtime_dir: Option<PathBuf>,
        requirement: RouteRequirement,
        #[cfg_attr(not(feature = "test-seams"), allow(unused_variables))] session_observer: Option<
            Arc<dyn ReadSessionTestObserver>,
        >,
        #[cfg_attr(not(feature = "test-seams"), allow(unused_variables))] kernel_observer: Option<
            Arc<dyn ReadKernelTestObserver>,
        >,
        mismatch: Option<OwnerHandshakeMismatchForTest>,
        progress: Option<Arc<dyn ReadProgressObserver>>,
    ) -> Result<Self> {
        #[cfg(not(feature = "test-seams"))]
        let _ = (&session_observer, &kernel_observer);
        let mut options = options;
        if let Some(limit) = options.memory_limit {
            options.limits.memory = options.limits.memory.min(limit as u64);
        }
        options
            .limits
            .validate()
            .map_err(|violation| Error::Other(violation.to_string()))?;
        options
            .timeouts
            .validate()
            .map_err(|violation| Error::Other(violation.to_string()))?;
        // One real store has one companion, channel address and direct-file
        // door regardless of whether its caller used an absolute path, a
        // relative path or a symlink. Resolve that identity once before any
        // route fact is consulted. A path proven absent beneath a missing
        // parent keeps its spelling only so the direct reader can return the
        // established typed missing-store answer.
        let canonical_path = resolved_route_path(path)?;
        let resources = Arc::new(RouteResources::default());
        #[cfg(feature = "test-seams")]
        let observed = session_observer.clone();
        let observe = move |event: ReadSessionEvent| {
            #[cfg(feature = "test-seams")]
            if let Some(observer) = observed.as_ref() {
                observer.observe_event(event);
            }
            #[cfg(not(feature = "test-seams"))]
            let _ = event;
        };
        // Loading the committed image happens inside route selection, so the
        // observer is in force for the whole of it: a caller opening a large
        // store hears about the load rather than waiting in silence.
        let selected = with_progress_observer(progress.as_ref(), || {
            let mut attempt = 1u64;
            loop {
                match attempt_route(
                    &canonical_path,
                    &options,
                    runtime_dir.as_deref(),
                    requirement,
                    attempt,
                    &resources,
                    mismatch,
                    &observe,
                ) {
                    Ok(selected) => break Ok(selected),
                    Err(refusal) => {
                        observe(ReadSessionEvent::RouteOpenFailed {
                            attempt,
                            failure: refusal.failure,
                        });
                        if refusal.failure != ReadRouteOpenFailure::WriterBecameOwner || attempt > 1
                        {
                            return Err(refusal.error);
                        }
                        let retry_attempt = attempt.saturating_add(1);
                        observe(ReadSessionEvent::OwnershipRaceRetry {
                            failed_attempt: attempt,
                            retry_attempt,
                        });
                        attempt = retry_attempt;
                    }
                }
            }
        })?;
        let (route, state, snapshot_at) = match selected {
            SelectedRoute::Owner(owner) => (
                ReadRoute::Owner,
                ReadSessionState::OwnerChannel(owner),
                None,
            ),
            SelectedRoute::File(reader) => (
                ReadRoute::File,
                ReadSessionState::DirectFile(reader),
                // The committed image has just been read; this is when.
                Some(contextdb_core::Wallclock::now()),
            ),
        };
        observe(ReadSessionEvent::RouteSelected(route));
        Ok(Self {
            route,
            options,
            runtime_dir,
            state: Arc::new(state),
            resources,
            progress,
            snapshot_at,
            #[cfg(feature = "test-seams")]
            observer: session_observer,
            #[cfg(feature = "test-seams")]
            kernel_observer,
            #[cfg(feature = "test-seams")]
            clock: None,
        })
    }

    pub(crate) fn from_live_database(database: Database, limits: ReadLimits) -> Self {
        Self {
            route: ReadRoute::Owner,
            options: ReadSessionOptions {
                limits,
                timeouts: ReadClientTimeouts::default(),
                ..ReadSessionOptions::default()
            },
            runtime_dir: None,
            state: Arc::new(ReadSessionState::LiveDatabase(Arc::new(database))),
            resources: Arc::new(RouteResources::default()),
            progress: None,
            snapshot_at: None,
            #[cfg(feature = "test-seams")]
            observer: None,
            #[cfg(feature = "test-seams")]
            kernel_observer: None,
            #[cfg(feature = "test-seams")]
            clock: None,
        }
    }

    /// A live-database session that reports what its reads are doing while
    /// they run. The store is already open here, so there is no hydration to
    /// report; the observer hears the executing phase of every statement,
    /// page, and cursor fetch this session goes on to run.
    pub(crate) fn from_live_database_with_progress(
        database: Database,
        limits: ReadLimits,
        progress: Arc<dyn ReadProgressObserver>,
    ) -> Self {
        let mut session = Self::from_live_database(database, limits);
        session.progress = Some(progress);
        session
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn from_live_database_with_observer(
        database: Database,
        limits: ReadLimits,
        observer: Arc<dyn ReadSessionTestObserver>,
    ) -> Self {
        observer.observe_event(ReadSessionEvent::RouteSelected(ReadRoute::Owner));
        Self {
            route: ReadRoute::Owner,
            options: ReadSessionOptions {
                limits,
                timeouts: ReadClientTimeouts::default(),
                ..ReadSessionOptions::default()
            },
            runtime_dir: None,
            state: Arc::new(ReadSessionState::LiveDatabase(Arc::new(database))),
            resources: Arc::new(RouteResources::default()),
            progress: None,
            snapshot_at: None,
            observer: Some(observer),
            kernel_observer: None,
            #[cfg(feature = "test-seams")]
            clock: None,
        }
    }

    /// A live-database session whose reads deadline against the clock the
    /// caller supplies, so a deadline can be reached deliberately instead of
    /// waited for. The owner route has always taken a clock; this is the same
    /// door for the in-process route.
    #[cfg(feature = "test-seams")]
    pub(crate) fn from_live_database_with_clock(
        database: Database,
        limits: ReadLimits,
        clock: Arc<dyn contextdb_core::read_contract::DeadlineClock>,
    ) -> Self {
        let mut session = Self::from_live_database(database, limits);
        session.clock = Some(clock);
        session
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn from_live_database_with_kernel_observer(
        database: Database,
        limits: ReadLimits,
        observer: Arc<dyn ReadKernelTestObserver>,
    ) -> Self {
        Self {
            route: ReadRoute::Owner,
            options: ReadSessionOptions {
                limits,
                timeouts: ReadClientTimeouts::default(),
                ..ReadSessionOptions::default()
            },
            runtime_dir: None,
            state: Arc::new(ReadSessionState::LiveDatabase(Arc::new(database))),
            resources: Arc::new(RouteResources::default()),
            progress: None,
            snapshot_at: None,
            observer: None,
            kernel_observer: Some(observer),
            #[cfg(feature = "test-seams")]
            clock: None,
        }
    }

    pub const fn route(&self) -> ReadRoute {
        self.route
    }

    /// Current bytes charged to the reads this route serves.
    ///
    /// A live database reports what its declared reader memory holds for
    /// reads -- never the database's shared total, which also carries writer
    /// state and background-maintenance reservations whose size depends on
    /// timing, so identical reads would report different charges.
    /// A file reader has its own accountant. An owner-channel reader observes
    /// the owner's shared accountant through its already-selected channel, so
    /// that figure can also include other work the owner is serving. Charges
    /// held only during a statement return when that work releases them.
    pub fn charged_bytes(&self) -> Result<u64> {
        match self.state.as_ref() {
            ReadSessionState::LiveDatabase(database) => Ok(database.declared_read_charged_bytes()),
            ReadSessionState::DirectFile(reader) => Ok(reader.charged_bytes()),
            ReadSessionState::OwnerChannel(owner) => {
                let responses = owner.ask(
                    self.options.limits,
                    crate::local_transport::LocalRequest::OwnerStatus,
                    None,
                    #[cfg(feature = "test-seams")]
                    self.observer.as_ref(),
                )?;
                for response in responses {
                    match response {
                        crate::local_transport::LocalResponse::OwnerStatus { status } => {
                            return Ok(status.memory.used_bytes);
                        }
                        crate::local_transport::LocalResponse::Failure { failure } => {
                            return Err(Error::ReadFailure(failure));
                        }
                        _ => {}
                    }
                }
                Err(Error::ReadFailure(owner_disconnected()))
            }
        }
    }

    /// The runtime directory this session was opened against, when an operator
    /// supplied one.
    pub fn runtime_dir(&self) -> Option<&Path> {
        self.runtime_dir.as_deref()
    }

    pub fn options(&self) -> ReadSessionOptions {
        self.options.clone()
    }

    /// Run a bounded query with a fresh cancellation token.
    pub fn execute(&self, sql: &str, params: &HashMap<String, Value>) -> Result<QueryResult> {
        self.execute_with_cancellation(sql, params, &OwnerReadCancellation::new())
    }

    /// Run a bounded query using the caller's cancellation token.
    pub fn execute_with_cancellation(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        cancellation: &OwnerReadCancellation,
    ) -> Result<QueryResult> {
        contextdb_core::find_attribution::begin(sql);
        let executed = with_progress_observer(self.progress.as_ref(), || {
            with_route_source_counter(&self.resources, || {
                self.execute_within_route(sql, params, cancellation)
            })
        });
        contextdb_core::find_attribution::finish();
        executed.map(OwnedQueryResult::into_public)
    }

    fn execute_within_route(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        cancellation: &OwnerReadCancellation,
    ) -> Result<OwnedQueryResult> {
        match self.state.as_ref() {
            ReadSessionState::LiveDatabase(database) => {
                let executed = crate::executor::execute_bounded(
                    database,
                    sql,
                    params,
                    database.declared_read_request_provider().request_budget()?,
                    self.options.limits,
                    self.session_clock(),
                    cancellation.clone(),
                    #[cfg(feature = "test-seams")]
                    self.kernel_probe(ReadSessionOperation::Execute, cancellation),
                )
                .map_err(read_error)?;
                database.publish_authoritative_read(
                    &executed.publication,
                    |error| error,
                    || Ok(executed.result),
                )
            }
            // The committed file is asked through the target the reader
            // already holds, so the engine answers this route in the same
            // words it answers the owner's -- the direct reader's own doors
            // can only relay a fault as prose.
            ReadSessionState::DirectFile(reader) => {
                let executed = crate::executor::execute_bounded_on_target(
                    reader.execution_target().as_ref(),
                    sql,
                    params,
                    reader
                        .request_read_memory()
                        .map_err(|error| direct_error(Path::new(""), error))?,
                    self.options.limits,
                    self.session_clock(),
                    cancellation.clone(),
                    #[cfg(feature = "test-seams")]
                    self.kernel_probe(ReadSessionOperation::Execute, cancellation),
                )
                .map_err(read_error)?;
                reader.publish_bounded_result(&executed.publication, executed.result)
            }
            ReadSessionState::OwnerChannel(owner) => {
                let receiving_memory = crate::read_contract::decoder_memory(
                    self.options.limits.memory.min(
                        self.options
                            .memory_limit
                            .map_or(u64::MAX, |bytes| bytes as u64),
                    ),
                )?;
                let responses = owner.ask(
                    self.options.limits,
                    crate::local_transport::LocalRequest::Query {
                        statement: sql.to_owned(),
                        params: params.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                    },
                    Some(cancellation),
                    #[cfg(feature = "test-seams")]
                    self.observer.as_ref(),
                )?;
                published_query_result(responses, self.options.limits, receiving_memory)
            }
        }
    }

    /// When the view this session reads was taken.
    ///
    /// A session that reads a committed file reads ONE image, taken at one
    /// moment, and every answer it gives describes the store as it was then --
    /// so a caller can say how old the answer is. A session talking to a live
    /// owner has no such moment: the owner's state is still moving, and every
    /// answer is as current as the instant it was asked, which is why there is
    /// no instant to report rather than a made-up one.
    pub const fn snapshot_at(&self) -> Option<contextdb_core::Wallclock> {
        self.snapshot_at
    }

    /// Ask this store a question about itself.
    ///
    /// One door, whichever route the session chose: the same question gets the
    /// same answer whether it is projected from a committed file, from the
    /// writer's own live state in this process, or from the owner over a
    /// channel. The answer is the body itself, and the continuation -- which
    /// only the paged kinds have -- rides beside it rather than inside it, so
    /// the body a caller compares is the body that was published.
    ///
    /// A continuation belongs to the read that issued it. Offering one to a
    /// kind that never issues a continuation is refused rather than quietly
    /// ignored, because a caller that thinks it is resuming and is in fact
    /// starting over would silently read the inventory twice.
    pub fn metadata(
        &self,
        request: MetadataRequest,
        continuation: Option<&str>,
    ) -> Result<MetadataAnswer> {
        if continuation.is_some() && !metadata_kind_pages(&request) {
            return Err(Error::ReadFailure(
                contextdb_core::read_contract::ReadFailure::invalid_continuation(format!(
                    "{} answers in one piece and issues no continuation",
                    metadata_kind_name(&request)
                )),
            ));
        }
        with_progress_observer(self.progress.as_ref(), || {
            with_route_source_counter(&self.resources, || {
                let cancellation = OwnerReadCancellation::new();
                #[cfg(feature = "test-seams")]
                return with_session_kernel_probe(
                    self.kernel_probe(ReadSessionOperation::Metadata, &cancellation),
                    || self.metadata_within_route(request, continuation, &cancellation),
                );
                #[cfg(not(feature = "test-seams"))]
                self.metadata_within_route(request, continuation, &cancellation)
            })
        })
    }

    fn metadata_within_route(
        &self,
        request: MetadataRequest,
        continuation: Option<&str>,
        cancellation: &OwnerReadCancellation,
    ) -> Result<MetadataAnswer> {
        match self.state.as_ref() {
            ReadSessionState::LiveDatabase(database) => {
                if matches!(request, MetadataRequest::ImageState { .. }) {
                    return Err(crate::read_image::image_state_is_a_file_question());
                }
                let clock = self.session_clock();
                let started_ms = clock.now_ms();
                let publication = database.capture_authoritative_read_publication(
                    crate::direct_file_reader::metadata_publication_tables(&request),
                    self.options.limits,
                    Arc::clone(&clock),
                    cancellation.clone(),
                    started_ms,
                );
                let answer = crate::read_image::project_metadata_from_database(
                    database,
                    database.declared_read_request_provider().request_budget()?,
                    request,
                    self.options.limits,
                    clock,
                    continuation,
                    cancellation,
                )
                .map(|(body, continuation)| MetadataAnswer {
                    body,
                    continuation,
                    charge: Default::default(),
                })
                .map_err(|error| direct_error(Path::new(""), error))?;
                database.publish_authoritative_read(&publication, |error| error, || Ok(answer))
            }
            // The committed file answers from the image the reader already
            // holds, through the direct reader's own door, so this route says
            // exactly what a direct reader says.
            ReadSessionState::DirectFile(reader) => reader
                .metadata_from_with_cancellation(request, continuation, cancellation)
                .map(|answered| MetadataAnswer {
                    body: answered.body,
                    continuation: answered.continuation,
                    charge: answered.charge,
                })
                .map_err(|error| direct_error(Path::new(""), error)),
            ReadSessionState::OwnerChannel(owner) => {
                self.owner_metadata(owner, request, continuation, cancellation)
            }
        }
    }

    /// Ask the owner, and turn its answer back into the same body the other
    /// routes produce.
    ///
    /// The owner speaks two shapes: the kinds that fit in one piece arrive as
    /// the canonical body itself, and the inventories arrive as a page of
    /// items with the continuation that resumes them. Both become one body
    /// here, so a caller never has to know which shape travelled.
    fn owner_metadata(
        &self,
        owner: &OwnerRoute,
        request: MetadataRequest,
        continuation: Option<&str>,
        cancellation: &OwnerReadCancellation,
    ) -> Result<MetadataAnswer> {
        let limits = self.options.limits;
        let local = match &request {
            MetadataRequest::DeliveryStatus { root_table } => {
                crate::local_transport::LocalMetadataRequest::DeliveryStatus {
                    root_table: root_table.clone(),
                    continuation: continuation.map(str::to_owned),
                }
            }
            MetadataRequest::DeliveryOutcome {
                root_table,
                root_key,
            } => crate::local_transport::LocalMetadataRequest::DeliveryOutcome {
                root_table: root_table.clone(),
                root_key: root_key.clone(),
                continuation: continuation.map(str::to_owned),
            },
            MetadataRequest::Tables => crate::local_transport::LocalMetadataRequest::Tables {
                continuation: continuation.map(str::to_owned),
            },
            MetadataRequest::Schema { table } => {
                crate::local_transport::LocalMetadataRequest::Schema {
                    table: table.clone(),
                }
            }
            MetadataRequest::EventsStatus => {
                crate::local_transport::LocalMetadataRequest::EventsStatus {
                    continuation: continuation.map(str::to_owned),
                }
            }
            MetadataRequest::MaintenanceStatus => {
                crate::local_transport::LocalMetadataRequest::MaintenanceStatus
            }
            // Explaining a statement is the owner planning it, not an
            // inventory it keeps, so it travels as its own request, and the
            // caller's binding travels with it: the owner plans the statement
            // exactly as the file and live routes plan the same request.
            MetadataRequest::Explain { sql, params } => {
                return self.owner_explain(owner, sql.clone(), params, cancellation);
            }
            // The local protocol carries no request for the state of a
            // committed image, because an owner is not one.
            MetadataRequest::ImageState { .. } => {
                return Err(crate::read_image::image_state_is_a_file_question());
            }
        };
        let receiving_limit = limits.memory.min(
            self.options
                .memory_limit
                .map_or(u64::MAX, |bytes| bytes as u64),
        );
        let receiving_memory = crate::read_contract::decoder_memory(receiving_limit)?;
        let responses = owner.ask(
            limits,
            crate::local_transport::LocalRequest::Metadata { request: local },
            Some(cancellation),
            #[cfg(feature = "test-seams")]
            self.observer.as_ref(),
        )?;
        let payload = published_metadata_payload(responses, limits.memory)?;
        let mut receiving_limits = limits;
        receiving_limits.memory = receiving_limit;
        promoted_metadata_answer(&request, &payload, receiving_limits, receiving_memory)
    }

    /// Ask the owner to plan a statement against the caller's own binding
    /// without running it. The binding travels with the statement: a
    /// partition scope resolves only after its key binds, so an explain that
    /// left the binding behind would describe a scope the caller never named.
    fn owner_explain(
        &self,
        owner: &OwnerRoute,
        sql: String,
        params: &HashMap<String, Value>,
        cancellation: &OwnerReadCancellation,
    ) -> Result<MetadataAnswer> {
        let answered = owner.ask(
            self.options.limits,
            crate::local_transport::LocalRequest::Explain {
                statement: sql,
                params: params
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect(),
            },
            Some(cancellation),
            #[cfg(feature = "test-seams")]
            self.observer.as_ref(),
        )?;
        // The owner writes an explained plan through the same canonical
        // writer the file route uses, so it is read back with that writer's
        // own inverse rather than re-derived from a query result.
        let body =
            crate::read_contract::decode_metadata_body(&published_explain_payload(answered)?)
                .map_err(|error| Error::Other(error.to_string()))?;
        Ok(MetadataAnswer {
            body,
            continuation: None,
            charge: Default::default(),
        })
    }

    /// Present this session's kernel observer, if a test attached one, in the
    /// only shape the bounded kernel accepts.
    #[cfg(feature = "test-seams")]
    fn kernel_probe(
        &self,
        operation: ReadSessionOperation,
        cancellation: &OwnerReadCancellation,
    ) -> Option<Arc<dyn crate::executor::BoundedExecutionProbe>> {
        self.kernel_observer
            .as_ref()
            .map(|observer| observed_kernel_probe(observer, operation, self.route, cancellation))
    }

    /// Open a bounded cursor with a fresh cancellation token.
    pub fn open_cursor(&self, sql: &str, params: &HashMap<String, Value>) -> Result<ReadCursor> {
        self.open_cursor_with_cancellation(sql, params, &OwnerReadCancellation::new())
    }

    /// Open a bounded cursor using the caller's cancellation token.
    pub fn open_cursor_with_cancellation(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        cancellation: &OwnerReadCancellation,
    ) -> Result<ReadCursor> {
        contextdb_core::find_attribution::begin(sql);
        let opened = with_progress_observer(self.progress.as_ref(), || {
            with_route_source_counter(&self.resources, || {
                self.open_cursor_within_route(sql, params, cancellation)
            })
        });
        contextdb_core::find_attribution::finish();
        opened
    }

    fn open_cursor_within_route(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        cancellation: &OwnerReadCancellation,
    ) -> Result<ReadCursor> {
        match self.state.as_ref() {
            ReadSessionState::OwnerChannel(owner) => {
                let clock = self.session_clock();
                let now = clock.now_ms();
                let handoff =
                    crate::persistence::ReadSnapshotRegistrationGuard::owner_handoff_start(
                        &owner.path,
                        owner.identity,
                        owner.writer_run,
                        Some(&owner.runtime_dir),
                    )
                    .map_err(|error| direct_error(&owner.path, error.into_direct_reader_error()))?;
                #[cfg(feature = "test-seams")]
                if let Some(observer) = self.observer.as_ref() {
                    observer.observe_event(ReadSessionEvent::OwnerCursorHandoffStarted);
                }
                let retained_memory = crate::read_contract::decoder_memory(
                    self.options.limits.memory.min(
                        self.options
                            .memory_limit
                            .map_or(u64::MAX, |bytes| bytes as u64),
                    ),
                )?;
                // The request owns a statement clone throughout the reply.
                // Admit it into this same owner before making that clone.
                let request_bytes = crate::executor::bounded::cursor_statement_bytes(sql, params)
                    .map_err(read_error)?;
                retained_memory.reserve(request_bytes)?;
                let request_charge = OwnerTraceCharge {
                    memory: Arc::clone(&retained_memory),
                    bytes: request_bytes,
                };
                let responses = owner.ask_with_cursor_memory(
                    self.options.limits,
                    crate::local_transport::LocalRequest::CursorOpen {
                        statement: sql.to_owned(),
                        params: params.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                    },
                    Some(cancellation),
                    Some(cursor_receive_allowance(
                        self.options.limits.memory,
                        &retained_memory,
                    )),
                    #[cfg(feature = "test-seams")]
                    self.observer.as_ref(),
                )?;
                drop(request_charge);
                let opened_cursor_id = responses.iter().find_map(|response| match response {
                    crate::local_transport::LocalResponse::CursorOpened { opened } => {
                        Some(opened.cursor_id)
                    }
                    _ => None,
                });
                let published = match published_cursor_open(
                    responses,
                    self.options.limits,
                    &retained_memory,
                    #[cfg(feature = "test-seams")]
                    self.observer.as_ref(),
                ) {
                    Ok(published) => published,
                    Err(error) => {
                        drop(handoff);
                        if let Some(cursor_id) = opened_cursor_id {
                            let _ = owner.ask(
                                self.options.limits,
                                crate::local_transport::LocalRequest::CursorClose { cursor_id },
                                None,
                                #[cfg(feature = "test-seams")]
                                self.observer.as_ref(),
                            );
                        }
                        return Err(error);
                    }
                };
                let PublishedOwnerCursor {
                    cursor_id,
                    first_page,
                    observation,
                    owner_trace_charge,
                    snapshot,
                    physical_identity,
                    limits,
                } = published;
                #[cfg(feature = "test-seams")]
                if let Some(observer) = self.observer.as_ref() {
                    observer.observe_event(ReadSessionEvent::BeforeOwnerCursorHandoffRegistration);
                }
                let continuation = (|| -> Result<Option<OwnerCursorContinuation>> {
                    Ok(match (snapshot, physical_identity) {
                        (Some(snapshot), Some(physical_identity)) => {
                            let registration =
                                crate::persistence::ReadSnapshotRegistrationGuard::owner_handoff_finish(
                                    &handoff,
                                    &owner.path,
                                    owner.identity,
                                    physical_identity,
                                    snapshot,
                                    Some(&owner.runtime_dir),
                                )
                                .map_err(|error| {
                                    direct_error(&owner.path, error.into_direct_reader_error())
                                })?;
                            let mut config = crate::direct_file_reader::DirectReaderConfig::new(
                                limits,
                                Arc::clone(&clock),
                            )
                            .declaring(
                                self.options.contexts.clone(),
                                self.options.scope_labels.clone(),
                                self.options.principal.clone(),
                            );
                            config.memory_limit = self.options.memory_limit;
                            config.runtime_dir = Some(owner.runtime_dir.clone());
                            let retained_bytes =
                                crate::executor::bounded::cursor_statement_bytes(sql, params)
                                    .map_err(read_error)?;
                            if retained_bytes
                                .checked_add(retained_memory.held())
                                .is_none_or(|bytes| bytes as u64 > limits.memory)
                            {
                                return Err(read_encoding_error(
                                    crate::read_contract::ReadEncodingError::MemoryCeilingExceeded {
                                        ceiling: limits.memory,
                                    }));
                            }
                            retained_memory.reserve(retained_bytes)?;
                            let mut statement_charge = OwnerTraceCharge {
                                memory: Arc::clone(&retained_memory),
                                bytes: retained_bytes,
                            };
                            let retained_sql = sql.to_owned();
                            let retained_params = params.clone();
                            let actual_statement_bytes =
                                crate::executor::bounded::cursor_statement_bytes(
                                    &retained_sql,
                                    &retained_params,
                                )
                                .map_err(read_error)?;
                            if actual_statement_bytes > retained_bytes {
                                return Err(Error::Other(
                                    "cursor statement exceeded its admitted clone allowance"
                                        .to_string(),
                                ));
                            }
                            retained_memory.release(retained_bytes - actual_statement_bytes);
                            statement_charge.bytes = 0;
                            Some(OwnerCursorContinuation {
                                retained_memory: Arc::clone(&retained_memory),
                                retained_statement_bytes: actual_statement_bytes,
                                discarded_rows_examined: 0,
                                failed_open_observation: None,
                                path: owner.path.clone(),
                                registration: Some(registration),
                                config,
                                sql: retained_sql,
                                params: retained_params,
                                delivered: first_page.rows.len(),
                                cursor: None,
                                skipped: 0,
                            })
                        }
                        _ => None,
                    })
                })();
                let continuation = match continuation {
                    Ok(continuation) => continuation,
                    Err(error) => {
                        let _ = owner.ask(
                            limits,
                            crate::local_transport::LocalRequest::CursorClose { cursor_id },
                            None,
                            #[cfg(feature = "test-seams")]
                            self.observer.as_ref(),
                        );
                        return Err(error);
                    }
                };
                let owner_groups =
                    match OwnerGroupBaseline::capture(&observation.trace, &retained_memory) {
                        Ok(groups) => groups,
                        Err(error) => {
                            let _ = owner.ask(
                                limits,
                                crate::local_transport::LocalRequest::CursorClose { cursor_id },
                                None,
                                #[cfg(feature = "test-seams")]
                                self.observer.as_ref(),
                            );
                            return Err(error);
                        }
                    };
                let complete = !first_page.has_more;
                if !complete {
                    self.resources
                        .active_cursors
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                let owner_rows_examined = observation.rows_examined;
                let (trace, observation) = take_cursor_observation(observation);
                let cursor = ReadCursor {
                    trace,
                    observation,
                    owner_trace_charge: Some(owner_trace_charge),
                    state: Arc::new(SuspendedReadCursorState {
                        route: self.route,
                        session: Arc::clone(&self.state),
                        resources: Arc::clone(&self.resources),
                        progress: self.progress.as_ref().map(Arc::clone),
                        first_page: first_page.into_public(),
                        execution: Mutex::new(Box::new(OwnerCursorExecution {
                            session: Arc::clone(&self.state),
                            resources: Arc::clone(&self.resources),
                            limits,
                            cursor_id,
                            owner_rows_examined,
                            owner_groups,
                            cancelled_observation: None,
                            cancelled_trace_charge: None,
                            retained_memory: Arc::clone(&retained_memory),
                            continuation: if complete { None } else { continuation },
                            last_used_ms: clock.now_ms(),
                            clock,
                            created_ms: now,
                            closed: complete,
                            expired: None,
                            expiry_waker: Arc::new(Mutex::new(None)),
                            #[cfg(feature = "test-seams")]
                            observer: self.observer.as_ref().map(Arc::clone),
                        })),
                        #[cfg(feature = "test-seams")]
                        observer: self.observer.as_ref().map(Arc::clone),
                        #[cfg(feature = "test-seams")]
                        kernel_observer: self.kernel_observer.as_ref().map(Arc::clone),
                    }),
                };
                if !complete {
                    start_owner_cursor_expiry_watch(&cursor.state);
                }
                Ok(cursor)
            }
            ReadSessionState::DirectFile(reader) => {
                let rows = NonZeroUsize::new(
                    usize::try_from(self.options.limits.cursor_page_rows).unwrap_or(usize::MAX),
                )
                .unwrap_or(NonZeroUsize::MIN);
                #[cfg(feature = "test-seams")]
                let opened = with_session_kernel_probe(
                    self.kernel_probe(ReadSessionOperation::CursorOpen, cancellation),
                    || reader.open_cursor_with_cancellation(sql, params, rows, cancellation),
                );
                #[cfg(not(feature = "test-seams"))]
                let opened = reader.open_cursor_with_cancellation(sql, params, rows, cancellation);
                let opened = opened.map_err(|error| direct_error(Path::new(""), error))?;
                self.resources
                    .active_cursors
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let (trace, observation) = take_cursor_observation(opened.first_page.observation);
                Ok(ReadCursor {
                    trace,
                    observation,
                    owner_trace_charge: None,
                    state: Arc::new(SuspendedReadCursorState {
                        route: self.route,
                        session: Arc::clone(&self.state),
                        resources: Arc::clone(&self.resources),
                        progress: self.progress.as_ref().map(Arc::clone),
                        first_page: opened.first_page.page.into_public(),
                        execution: Mutex::new(Box::new(DirectCursorExecution {
                            cursor: opened.cursor,
                            resources: Arc::clone(&self.resources),
                            #[cfg(feature = "test-seams")]
                            kernel_observer: self.kernel_observer.as_ref().map(Arc::clone),
                        })),
                        #[cfg(feature = "test-seams")]
                        observer: self.observer.as_ref().map(Arc::clone),
                        #[cfg(feature = "test-seams")]
                        kernel_observer: self.kernel_observer.as_ref().map(Arc::clone),
                    }),
                })
            }
            ReadSessionState::LiveDatabase(database) => {
                // A cursor outlives the call that opened it, and the
                // transaction this handle has open does not have to: it can
                // commit or roll back between two fetches, and whatever this
                // cursor had already shown from it would then be a view of
                // something that either changed or never happened. So a
                // cursor is not opened inside one -- the same refusal, in the
                // same words, that the reading session already gives.
                if database.active_read_transaction().is_some() {
                    return Err(Error::ReadFailure(cursor_transaction_active()));
                }
                let opened = crate::executor::open_bounded_cursor(
                    Arc::clone(database),
                    sql,
                    params,
                    database.declared_read_request_provider().request_budget()?,
                    self.options.limits,
                    self.session_clock(),
                    cancellation.clone(),
                    #[cfg(feature = "test-seams")]
                    self.kernel_probe(ReadSessionOperation::CursorOpen, cancellation),
                )
                .map_err(read_error)?;
                let publication = opened.publication.clone();
                database.publish_authoritative_read(
                    &publication,
                    |error| error,
                    || {
                        let (trace, observation) = take_cursor_observation(opened.observation);
                        Ok(ReadCursor {
                            trace,
                            observation,
                            owner_trace_charge: None,
                            state: Arc::new(SuspendedReadCursorState {
                                route: self.route,
                                session: Arc::clone(&self.state),
                                resources: Arc::clone(&self.resources),
                                progress: self.progress.as_ref().map(Arc::clone),
                                first_page: opened.first_page.into_public(),
                                execution: Mutex::new(Box::new(LiveCursorExecution {
                                    database: Arc::clone(database),
                                    handle: opened.cursor,
                                    #[cfg(feature = "test-seams")]
                                    kernel_observer: self.kernel_observer.as_ref().map(Arc::clone),
                                })),
                                #[cfg(feature = "test-seams")]
                                observer: self.observer.as_ref().map(Arc::clone),
                                #[cfg(feature = "test-seams")]
                                kernel_observer: self.kernel_observer.as_ref().map(Arc::clone),
                            }),
                        })
                    },
                )
            }
        }
    }

    /// Send an administrative request to the owner selected for this session.
    pub fn request_owner(&self, namespace: &str, request: &[u8]) -> Result<Vec<u8>> {
        match self.state.as_ref() {
            // The embedded caller already holds the owner, so it asks the
            // handler directly rather than dialling its own channel.
            ReadSessionState::LiveDatabase(database) => {
                database.request_owner_in_process(namespace, request)
            }
            // There is no owner on this route and this session will not go
            // looking for one; the caller is told so plainly.
            ReadSessionState::DirectFile(_) => Err(Error::ReadFailure(owner_not_running())),
            ReadSessionState::OwnerChannel(owner) => {
                let responses = owner.ask(
                    self.options.limits,
                    crate::local_transport::LocalRequest::Custom {
                        namespace: namespace.to_owned(),
                        payload: request.to_vec(),
                    },
                    None,
                    #[cfg(feature = "test-seams")]
                    self.observer.as_ref(),
                )?;
                match responses.into_iter().next() {
                    Some(crate::local_transport::LocalResponse::Custom { payload }) => Ok(payload),
                    _ => Err(Error::ReadFailure(owner_disconnected())),
                }
            }
        }
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn route_resources_for_test(&self) -> Result<ReadRouteResourceSnapshot> {
        Ok(self.resources.snapshot())
    }

    /// What a store is still holding at a pathname, without opening it or
    /// selecting a route.
    ///
    /// A proof uses this after the last handle is dropped, to see what the
    /// finalizer is still holding on the way out: the channel identity is
    /// derived from the pathname, so it matches what the live owner reported,
    /// and each resource reads as held for as long as this process still owns
    /// the store's registry entry. Asking is free of side effects — no route
    /// is selected, no channel is dialled, no backend is opened.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn route_resources_for_path_for_test(
        path: impl AsRef<Path>,
    ) -> Result<ReadRouteResourceSnapshot> {
        let path = path.as_ref();
        let observed = crate::read_probe::observed();
        let retained = Database::store_is_retained_by_this_process_for_test(path);
        Ok(ReadRouteResourceSnapshot {
            channel_identity: crate::local_transport::derive_channel_address(path).ok(),
            owner_services_started: u64::from(retained),
            local_channel_operations: 0,
            direct_backend_opens: 0,
            bounded_source_items_completed: observed.bounded_source_touches,
            active_owner_slots: 0,
            active_cursors: 0,
            open_registry_owned: retained,
            persistence_owned: retained,
            blob_repository_owned: retained,
            plugin_open: retained,
            snapshot_registry_owned: retained,
            memory_accountant_owned: retained,
        })
    }

    /// What the owner of the store at this path says about itself.
    ///
    /// Status resolves no route: it asks the listener the store's companion
    /// record names and returns what that listener answers, so an owner that
    /// has stopped admitting work and is draining is distinguishable from one
    /// that is serving, and both from a path with no listener at all. Asking
    /// bypasses admission — a saturated owner still answers what it is — but
    /// it still authenticates, so a stale or foreign listener is refused
    /// rather than believed.
    ///
    /// A store nobody owns is `OwnerNotRunning`. An in-memory store has no
    /// path to ask about and is `NotApplicable`.
    pub fn owner_status(
        path: impl AsRef<Path>,
        options: ReadSessionOptions,
    ) -> Result<OwnerReadStatus> {
        Self::owner_report(path, options).map(|report| report.status)
    }

    /// Everything the owner of this store can say about how it is serving.
    ///
    /// An operator asking "what is this owner doing" wants more than a word:
    /// which limits are actually in force and whether each is a default or
    /// something set here, how long it will wait, how many readers it can take
    /// and how many it has, and what memory it is holding. The owner already
    /// computes all of that to answer a status request; this hands it back
    /// whole instead of throwing it away and reporting only the word.
    ///
    /// Where there is no owner there is nothing to report BUT the word, so
    /// those answers carry the state alone: a store nobody owns has no limits
    /// in force, and an in-memory store has no owner to have any.
    pub fn owner_report(
        path: impl AsRef<Path>,
        options: ReadSessionOptions,
    ) -> Result<OwnerReport> {
        Self::owner_report_in_runtime_dir(path, options, None)
    }

    /// The same report, asked of an owner whose channel lives in a runtime
    /// directory the operator supplied — see [`Self::open_in_runtime_dir`]. A
    /// session that reads through a supplied root asks about its owner through
    /// the same root, or it would report "not running" about the very owner it
    /// is reading from.
    pub fn owner_report_in_runtime_dir(
        path: impl AsRef<Path>,
        options: ReadSessionOptions,
        runtime_dir: Option<&Path>,
    ) -> Result<OwnerReport> {
        if runtime_dir.is_some() {
            read_runtime_directory(current_read_user()?, runtime_dir)?;
        }
        // Status resolves no ROW route, but it must resolve the same store
        // identity as one. Otherwise a symlink alias asks beside itself and
        // misses the live owner, while a repointed or unlinked name can ask an
        // old owner that no longer owns what the pathname names.
        let resolved_path = resolved_route_path(path.as_ref())?;
        let path = resolved_path.as_path();
        let mut waited = false;
        loop {
            // `Some` means this pass began from a trusted published owner but
            // nobody answered its channel. The inner value records whether
            // the same trusted companion then proved a live holder, an idle
            // store, or no stable answer. It prevents a failed published
            // candidate from collapsing into the answer "nobody owns this"
            // merely because its claim settled or its companion changed
            // while status was being resolved.
            let mut unanswered_published_hold: Option<Option<bool>> = None;
            let published = published_owner_channel(path);
            if let Some((identity, writer_run, owner_user, address)) = published {
                let runtime = read_runtime_directory(owner_user, runtime_dir)?;
                let channel_path =
                    crate::local_transport::channel_socket_path(runtime.path(), address)
                        .map_err(|error| Error::Other(error.to_string()))?;
                #[cfg(unix)]
                let channel_identity =
                    crate::local_transport::channel_filesystem_identity(&channel_path).ok();
                let presented = crate::local_transport::LocalHandshake::current(
                    identity, writer_run, owner_user,
                );
                match wait_for_local_answer(crate::owner_read::OwnerClient::connect(
                    &channel_path,
                    presented,
                    options.timeouts,
                    read_clock(),
                )) {
                    Ok(mut client) => {
                        let answered = wait_for_local_answer(client.request(
                            crate::local_transport::LocalRequestEnvelope {
                                limits: options.limits,
                                request: crate::local_transport::LocalRequest::OwnerStatus,
                            },
                        ))
                        .map_err(owner_error)?;
                        for response in answered {
                            match response {
                                crate::local_transport::LocalResponse::OwnerStatus { status } => {
                                    return Ok(OwnerReport::from_channel(status));
                                }
                                crate::local_transport::LocalResponse::Failure { failure } => {
                                    return Err(Error::ReadFailure(failure));
                                }
                                _ => {}
                            }
                        }
                        return Err(Error::ReadFailure(owner_disconnected()));
                    }
                    Err(error) if nobody_answered_the_channel(&error) => {
                        #[cfg(unix)]
                        reclaim_abandoned_channel(&channel_path, channel_identity);
                        #[cfg(not(unix))]
                        reclaim_abandoned_channel(&channel_path);
                        // No listener answered, but a trusted record held by a
                        // live writer is still that writer's own status. A
                        // disabled or failed owner can therefore answer status
                        // without a channel; a writer recorded as serving but
                        // no longer reachable is unavailable, never absent.
                        if let Some(status) = crate::persistence::recorded_held_owner_status(path) {
                            use contextdb_core::read_contract::OwnerServingState;
                            match status.state {
                                OwnerServingState::Serving => {
                                    return Err(Error::ReadFailure(owner_not_serving()));
                                }
                                OwnerServingState::ServingDisabled
                                | OwnerServingState::NotServing => {
                                    return Ok(OwnerReport {
                                        status,
                                        serving: None,
                                    });
                                }
                                // The writer has not published its decision
                                // yet. The claim below is the authority for
                                // waiting; this placeholder is not a status.
                                OwnerServingState::NotApplicable => {}
                            }
                        }
                        unanswered_published_hold =
                            Some(crate::persistence::observed_writer_hold(path));
                    }
                    Err(error) => {
                        if let Some(status) = crate::persistence::recorded_unserved_owner(path) {
                            return Ok(OwnerReport {
                                status,
                                serving: None,
                            });
                        }
                        return Err(owner_authentication_error(owner_error(error)));
                    }
                }
            }

            // No channel answered, which is not the same as no owner. A
            // writer that has claimed this store and not yet published what
            // it decided is holding it, and telling this caller the store is
            // unowned is the one answer that licenses taking it over. The
            // claim may exist even before a new store has published any
            // identity, so it is observed whether or not the branch above had
            // a durable candidate.
            let Some(claim) = crate::persistence::observe_unsettled_claim(path) else {
                return match unanswered_published_hold {
                    Some(Some(true)) => Err(Error::ReadFailure(owner_not_serving())),
                    Some(None) => Err(Error::ReadFailure(owner_disconnected())),
                    // A positively absent pathname may still have an old
                    // companion beside it; `published_owner_channel`
                    // deliberately excluded that old identity, and status
                    // must do the same. A trusted published identity whose
                    // holder is proven gone is equally an absent owner.
                    Some(Some(false)) | None => Err(Error::ReadFailure(owner_not_running())),
                };
            };
            if waited {
                return Err(Error::ReadFailure(owner_holding_without_a_decision()));
            }
            waited = true;
            match crate::persistence::wait_for_claim_settlement(
                claim,
                claim_window_budget(options.timeouts),
                claim_window_clock().as_ref(),
            ) {
                crate::persistence::ClaimSettlement::Settled => continue,
                crate::persistence::ClaimSettlement::StillHeld => {
                    return Err(Error::ReadFailure(owner_holding_without_a_decision()));
                }
            }
        }
    }

    /// Send an owner-status request over the real local channel. Status
    /// bypasses admission but still authenticates the selected listener, so a
    /// draining listener is distinguishable from an unbound or stale path.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn probe_owner_status_for_test(
        path: impl AsRef<Path>,
        options: ReadSessionOptions,
    ) -> Result<OwnerReadStatus> {
        Self::owner_status(path, options)
    }
}

impl OwnerRoute {
    /// Put one question to the selected owner and wait for its complete
    /// answer. Nothing about the route changes here: the same connection is
    /// used, and a channel that stops answering ends the request rather than
    /// starting a second route.
    /// The read runs in the owner, so what it is doing arrives as frames and
    /// what stops it leaves as one. The caller's interest in both is already
    /// in force on this thread: the observer it opened the session with is
    /// installed for the span of this operation, and the token it may cancel
    /// is the one it handed to this call.
    fn ask(
        &self,
        limits: ReadLimits,
        request: crate::local_transport::LocalRequest,
        cancellation: Option<&OwnerReadCancellation>,
        #[cfg(feature = "test-seams")] observer: Option<&Arc<dyn ReadSessionTestObserver>>,
    ) -> Result<Vec<crate::local_transport::LocalResponse>> {
        self.ask_with_cursor_memory(
            limits,
            request,
            cancellation,
            None,
            #[cfg(feature = "test-seams")]
            observer,
        )
    }

    fn ask_with_cursor_memory(
        &self,
        limits: ReadLimits,
        request: crate::local_transport::LocalRequest,
        cancellation: Option<&OwnerReadCancellation>,
        cursor_memory: Option<u64>,
        #[cfg(feature = "test-seams")] observer: Option<&Arc<dyn ReadSessionTestObserver>>,
    ) -> Result<Vec<crate::local_transport::LocalResponse>> {
        self.resources.note_channel_operation();
        let progress = crate::read_progress::observer_for_this_read();
        let mut client = self.client.lock().expect("selected owner channel");
        #[cfg(feature = "test-seams")]
        if let Some(observer) = observer {
            let observer = Arc::clone(observer);
            client.observe_response_frames_for_test(Some(Arc::new(move |terminal| {
                observer.observe_event(ReadSessionEvent::ResponseFrameReceived { terminal });
            })));
        }
        let answered = wait_for_local_answer(client.request_watching_under_cursor_memory(
            crate::local_transport::LocalRequestEnvelope { limits, request },
            progress.as_ref(),
            cancellation,
            cursor_memory,
        ));
        #[cfg(feature = "test-seams")]
        client.observe_response_frames_for_test(None);
        answered.map_err(owner_error)
    }
}

/// Reassemble one ordinary answer that arrived over the channel. A partial
/// answer is never published: the chunks stay inside the shared terminal
/// receiver until the owner says the answer is complete.
/// Everything an owner says about how it is serving this store.
///
/// The state alone answers "is anyone there"; the rest answers "and what will
/// it do for me" -- which ceilings are in force and whether each is a default
/// or something this deployment set, how long it waits, how many readers it
/// can take against how many it has, and what memory it holds. All of it is
/// what the owner itself computed, not this reader's guess.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerReport {
    pub status: OwnerReadStatus,
    /// Absent when there is no owner to have limits: the fields below describe
    /// a live owner, and inventing zeroes for an absent one would read as an
    /// owner that allows nothing.
    pub serving: Option<OwnerServingReport>,
}

/// The part of an owner's report that only exists while it is serving.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerServingReport {
    pub effective_limits: OwnerEffectiveLimits,
    pub timeouts: OwnerTimeoutReport,
    pub admission: OwnerAdmissionReport,
    pub memory: OwnerMemoryReport,
}

/// One ceiling in force, and whether it is the shipped default or something
/// this deployment chose. An operator reading a surprising limit needs to know
/// which, because only one of them is theirs to change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnerConfiguredValue {
    pub value: u64,
    pub source: OwnerConfigurationSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerConfigurationSource {
    Default,
    Override,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnerEffectiveLimits {
    pub result_rows: OwnerConfiguredValue,
    pub result_bytes: OwnerConfiguredValue,
    pub work: OwnerConfiguredValue,
    pub active_ms: OwnerConfiguredValue,
    pub memory: OwnerConfiguredValue,
    pub cursor_page_rows: OwnerConfiguredValue,
    pub cursor_page_bytes: OwnerConfiguredValue,
    pub cursor_idle_ms: OwnerConfiguredValue,
    pub cursor_lifetime_ms: OwnerConfiguredValue,
    pub concurrency: OwnerConfiguredValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnerTimeoutReport {
    pub request_ms: OwnerConfiguredValue,
    pub shutdown_drain_ms: OwnerConfiguredValue,
}

/// How many readers this owner can serve at once, against how many it is
/// serving now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnerAdmissionReport {
    pub capacity: u64,
    pub active_readers: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnerMemoryReport {
    pub used_bytes: u64,
    /// Absent when this owner declares no memory ceiling, which is not the
    /// same as having none left.
    pub available_bytes: Option<u64>,
}

impl OwnerReport {
    /// The whole answer the channel already produced, in the reading
    /// vocabulary rather than the wire's.
    fn from_channel(answered: crate::local_transport::LocalOwnerStatusResponse) -> Self {
        let value =
            |configured: crate::local_transport::LocalConfiguredValue| OwnerConfiguredValue {
                value: configured.value,
                source: match configured.source {
                    crate::local_transport::LocalConfigurationSource::Default => {
                        OwnerConfigurationSource::Default
                    }
                    crate::local_transport::LocalConfigurationSource::Override => {
                        OwnerConfigurationSource::Override
                    }
                },
            };
        let limits = answered.effective_limits;
        // Only an owner that is serving has anything below the word to
        // report: a drained or disabled one will take no reader, and saying
        // what it "would" allow describes a service nobody can have.
        if !matches!(
            answered.status.state,
            contextdb_core::read_contract::OwnerServingState::Serving
        ) {
            return Self {
                status: answered.status,
                serving: None,
            };
        }
        Self {
            status: answered.status,
            serving: Some(OwnerServingReport {
                effective_limits: OwnerEffectiveLimits {
                    result_rows: value(limits.result_rows),
                    result_bytes: value(limits.result_bytes),
                    work: value(limits.work),
                    active_ms: value(limits.active_ms),
                    memory: value(limits.memory),
                    cursor_page_rows: value(limits.cursor_page_rows),
                    cursor_page_bytes: value(limits.cursor_page_bytes),
                    cursor_idle_ms: value(limits.cursor_idle_ms),
                    cursor_lifetime_ms: value(limits.cursor_lifetime_ms),
                    concurrency: value(limits.concurrency),
                },
                timeouts: OwnerTimeoutReport {
                    request_ms: value(answered.timeouts.request_ms),
                    shutdown_drain_ms: value(answered.timeouts.shutdown_drain_ms),
                },
                admission: OwnerAdmissionReport {
                    capacity: answered.admission.capacity,
                    active_readers: answered.admission.active_readers,
                },
                memory: OwnerMemoryReport {
                    used_bytes: answered.memory.used_bytes,
                    available_bytes: answered.memory.available_bytes,
                },
            }),
        }
    }
}

/// The one answer a metadata question has, and where to resume if the answer
/// was only part of an inventory.
///
/// The continuation sits beside the body rather than inside it: the body is
/// the published document, byte-for-byte the same on every route, and where a
/// paged read left off is this exchange's business, not the document's.
#[derive(Debug, Clone, PartialEq)]
pub struct MetadataAnswer {
    pub body: MetadataBody,
    /// Where to resume, when this answer was one page of an inventory and more
    /// of it remains. Absent means the answer is complete.
    pub continuation: Option<String>,
    /// What the answer holds against the session's memory while it is held.
    pub(crate) charge: crate::read_image_memory::AnswerCharge,
}

/// Whether this kind of question can be answered a page at a time.
///
/// Only the inventories can: a schema, an explain, a maintenance status and an
/// image state are each one document that arrives whole.
fn metadata_kind_pages(request: &MetadataRequest) -> bool {
    matches!(
        request,
        MetadataRequest::Tables | MetadataRequest::EventsStatus
    )
}

/// The question's name, for a refusal a caller can act on.
fn metadata_kind_name(request: &MetadataRequest) -> &'static str {
    match request {
        MetadataRequest::Tables => "the table inventory",
        MetadataRequest::Schema { .. } => "a table schema",
        MetadataRequest::Explain { .. } => "an explained statement",
        MetadataRequest::EventsStatus => "the event inventory",
        MetadataRequest::MaintenanceStatus => "the maintenance status",
        MetadataRequest::ImageState { .. } => "the committed image state",
        MetadataRequest::DeliveryStatus { .. } => "delivery status",
        MetadataRequest::DeliveryOutcome { .. } => "a delivery outcome",
    }
}

/// The owner's explained plan, or the reason there is not one.
fn published_explain_payload(
    responses: Vec<crate::local_transport::LocalResponse>,
) -> Result<Vec<u8>> {
    for response in responses {
        match response {
            crate::local_transport::LocalResponse::Explain { payload } => return Ok(payload),
            crate::local_transport::LocalResponse::Failure { failure } => {
                return Err(Error::ReadFailure(failure));
            }
            _ => {}
        }
    }
    Err(Error::ReadFailure(owner_disconnected()))
}

/// The owner's metadata reply, or the reason there is not one.
fn published_metadata_payload(
    responses: Vec<crate::local_transport::LocalResponse>,
    memory_ceiling: u64,
) -> Result<Vec<u8>> {
    for response in responses {
        match response {
            crate::local_transport::LocalResponse::Metadata { metadata } => {
                return Ok(metadata.payload);
            }
            crate::local_transport::LocalResponse::Failure { failure } => {
                return Err(Error::ReadFailure(failure));
            }
            crate::local_transport::LocalResponse::EngineFailure { failure } => {
                return Err(failure.into_error(memory_ceiling));
            }
            _ => {}
        }
    }
    Err(Error::ReadFailure(owner_disconnected()))
}

/// Turn the owner's canonical answer into the body every route publishes.
///
/// The kinds that travel whole are the canonical body already, and are read
/// back with the exact inverse of the writer that produced them. The
/// inventories travel as a page, so the page's items are promoted back into
/// the body they were flattened from -- and the page's continuation comes out
/// beside it.
fn promoted_metadata_answer(
    request: &MetadataRequest,
    payload: &[u8],
    limits: ReadLimits,
    receiving_memory: Arc<crate::read_image_memory::ImageMemory>,
) -> Result<MetadataAnswer> {
    match request {
        MetadataRequest::Schema { .. }
        | MetadataRequest::MaintenanceStatus
        | MetadataRequest::DeliveryStatus { .. }
        | MetadataRequest::DeliveryOutcome { .. } => {
            let body = crate::read_contract::decode_metadata_body_with_memory(
                payload,
                &receiving_memory,
                limits.memory,
            )
            .map_err(read_encoding_error)?;
            Ok(MetadataAnswer {
                body,
                continuation: None,
                charge: crate::read_image_memory::AnswerCharge::holding(receiving_memory.clone()),
            })
        }
        MetadataRequest::Tables => {
            let page = decoded_metadata_page(payload, limits)?;
            Ok(MetadataAnswer {
                body: MetadataBody::Tables {
                    items: crate::metadata_page::table_names_of(&page),
                    has_more: page.has_more,
                },
                continuation: page.continuation,
                charge: Default::default(),
            })
        }
        MetadataRequest::EventsStatus => {
            let page = decoded_metadata_page(payload, limits)?;
            Ok(MetadataAnswer {
                body: MetadataBody::EventsStatus {
                    status: crate::metadata_page::events_of(&page),
                    has_more: page.has_more,
                    continuation: page.continuation.clone(),
                },
                continuation: page.continuation,
                charge: Default::default(),
            })
        }
        MetadataRequest::Explain { .. } | MetadataRequest::ImageState { .. } => {
            Err(Error::ReadFailure(owner_disconnected()))
        }
    }
}

fn decoded_metadata_page(
    payload: &[u8],
    limits: ReadLimits,
) -> Result<contextdb_core::read_contract::MetadataPage> {
    crate::read_contract::decode_metadata_page_under_memory_ceiling(payload, limits.memory)
        .map_err(read_encoding_error)
}

/// Preserve a decoder's budget refusal across the owner channel and classify
/// unreadable canonical bytes as a channel data failure.
fn read_encoding_error(error: crate::read_contract::ReadEncodingError) -> Error {
    use contextdb_core::read_contract::{
        OwnerLimitExceededDetail, ReadFailure, ReadFailureDetail, ReadFailureKind, ReadFailureLimit,
    };

    match error {
        crate::read_contract::ReadEncodingError::MemoryCeilingExceeded { ceiling } => {
            Error::ReadFailure(ReadFailure::owner_limit_exceeded(
                OwnerLimitExceededDetail {
                    limit: ReadFailureLimit::Memory,
                    value: ceiling,
                    required: None,
                    statement: None,
                },
            ))
        }
        crate::read_contract::ReadEncodingError::InvalidPayload => Error::ReadFailure(
            ReadFailure::new(ReadFailureKind::InvalidChannelData, ReadFailureDetail::None)
                .expect("invalid channel data accepts canonical empty detail"),
        ),

        crate::read_contract::ReadEncodingError::Unimplemented => Error::ReadSessionNotImplemented,
    }
}

fn published_query_result(
    responses: Vec<crate::local_transport::LocalResponse>,
    limits: ReadLimits,
    receiving_memory: Arc<crate::read_image_memory::ImageMemory>,
) -> Result<OwnedQueryResult> {
    use crate::local_transport::{OrdinaryResultReceiver, ResultReceiveOutcome};
    let mut receiver = OrdinaryResultReceiver::with_receiving_memory(
        limits.result_bytes,
        limits.memory,
        receiving_memory.clone(),
    )
    .map_err(|error| owner_error(crate::owner_read::OwnerReadScaffoldError::from_local(error)))?;
    let mut published = None;
    for response in responses {
        match crate::owner_read::OwnerClient::receive_ordinary(&mut receiver, response)
            .map_err(owner_error)?
        {
            ResultReceiveOutcome::Pending => {}
            ResultReceiveOutcome::Published(result) => published = Some(result.bytes),
            ResultReceiveOutcome::Failed(failure) => return Err(Error::ReadFailure(failure)),
            ResultReceiveOutcome::EngineFailed(failure) => {
                return Err(failure.into_error(limits.memory));
            }
            ResultReceiveOutcome::Disconnected => {
                return Err(Error::ReadFailure(owner_disconnected()));
            }
        }
    }
    let bytes = published.ok_or_else(|| Error::ReadFailure(owner_disconnected()))?;
    let baseline = receiving_memory.held();
    let canonical = crate::read_contract::decode_query_result_with_memory(
        &bytes,
        &receiving_memory,
        limits.memory,
    )
    .map_err(read_encoding_error)?;
    restored_query_result(canonical, receiving_memory, baseline)
}

/// Turn the owner's canonical answer back into the same value an in-process
/// call would have produced.
///
/// The plan label is the one field that is a fixed engine word rather than
/// free text, so a label this reader does not recognise means the owner is
/// speaking a vocabulary this build does not have -- which is an answer about
/// the channel, not a result to hand back.
fn restored_query_result(
    canonical: crate::read_contract::CanonicalQueryResult,
    receiving_memory: Arc<crate::read_image_memory::ImageMemory>,
    baseline: usize,
) -> Result<OwnedQueryResult> {
    let mut canonical = canonical;
    let trace = std::mem::replace(
        &mut canonical.trace,
        crate::read_contract::CanonicalQueryTrace::empty(),
    );
    drop(canonical.cascade.take());
    let row_bytes = crate::executor::bounded::canonical_query_capacity_bytes(&canonical)
        .map_err(read_error)?
        .checked_sub(std::mem::size_of::<
            crate::read_contract::CanonicalQueryResult,
        >())
        .ok_or_else(|| Error::Other("canonical answer size underflow".into()))?;
    let copies = trace_restoration_bytes(&trace)?;
    receiving_memory.reserve(copies)?;
    let trace = restored_query_trace(trace)?;
    drop(canonical.cascade.take());
    let trace_bytes =
        crate::executor::bounded::query_trace_capacity_bytes(&trace).map_err(read_error)?;
    let retained = row_bytes
        .checked_add(trace_bytes)
        .ok_or_else(|| Error::Other("query answer size overflow".into()))?;
    // The decoding owner also contains fixed native controls. All payload
    // credit stays on this owner; excess container/temporary charges return
    // only after restoration destroys their source buffers.
    let payload_admitted = receiving_memory
        .held()
        .checked_sub(baseline)
        .ok_or_else(|| Error::Other("query answer admission underflow".into()))?;
    if retained > payload_admitted {
        return Err(Error::Other(
            "query answer exceeded prepaid native credit".into(),
        ));
    }
    receiving_memory.release(payload_admitted - retained);
    Ok(OwnedQueryResult {
        result: QueryResult {
            columns: canonical.columns,
            rows: canonical.rows,
            rows_affected: canonical.rows_affected,
            trace,
            cascade: None,
        },
        answer_charge: crate::read_image_memory::AnswerCharge::holding(receiving_memory.clone()),
        vector_copy_charge: crate::read_image_memory::AnswerCharge::holding(
            receiving_memory.clone(),
        ),
        retained_read_memory: Some(receiving_memory),
    })
}

/// Only native containers newly allocated during the canonical-to-engine
/// move. Existing strings, vectors and JSON payloads keep their prepaid credit.
fn trace_restoration_bytes(trace: &crate::read_contract::CanonicalQueryTrace) -> Result<usize> {
    let add = |bytes: usize, count: usize, width: usize| {
        count
            .checked_mul(width)
            .and_then(|n| bytes.checked_add(n))
            .ok_or_else(|| Error::Other("trace restoration size overflow".into()))
    };
    let mut bytes = 0;
    if trace.predicates_pushed.len() > 4 {
        bytes = add(
            bytes,
            trace.predicates_pushed.len(),
            std::mem::size_of::<std::borrow::Cow<'static, str>>(),
        )?;
    }
    if trace.indexes_considered.len() > 4 {
        bytes = add(
            bytes,
            trace.indexes_considered.len(),
            std::mem::size_of::<crate::database::IndexCandidate>(),
        )?;
    }
    if trace.groups.capacity() != trace.groups.len() {
        bytes = add(
            bytes,
            trace.groups.len(),
            std::mem::size_of::<crate::database::GroupDisclosure>(),
        )?;
    }
    if let Some(vector) = &trace.vector_search {
        bytes = add(
            bytes,
            vector.partition_hnsw.len(),
            std::mem::size_of::<crate::VectorPartitionHnswDisclosure>(),
        )?;
    }
    if let Some(count) = &trace.maintained_count {
        bytes = add(
            bytes,
            1,
            std::mem::size_of::<crate::maintained_counts::MaintainedCountDisclosure>(),
        )?;
        bytes = add(
            bytes,
            count.columns.len(),
            std::mem::size_of::<(String, contextdb_core::SortDirection)>(),
        )?;
        if !count.used && !count.declared {
            bytes = add(
                bytes,
                count.already_declared.len(),
                std::mem::size_of::<Vec<(String, contextdb_core::SortDirection)>>(),
            )?;
            for columns in &count.already_declared {
                bytes = add(
                    bytes,
                    columns.len(),
                    std::mem::size_of::<(String, contextdb_core::SortDirection)>(),
                )?;
            }
        }
    }
    Ok(bytes)
}

fn restored_query_trace(
    canonical: crate::read_contract::CanonicalQueryTrace,
) -> Result<crate::QueryTrace> {
    let physical_plan = known_plan_label(&canonical.physical_plan).ok_or_else(|| {
        Error::ReadFailure(
            contextdb_core::read_contract::ReadFailure::new(
                contextdb_core::read_contract::ReadFailureKind::InvalidChannelData,
                contextdb_core::read_contract::ReadFailureDetail::None,
            )
            .expect("invalid channel data accepts canonical empty detail"),
        )
    })?;
    let vector_search = canonical
        .vector_search
        .map(crate::database::VectorSearchDisclosure::try_from)
        .transpose()
        .map_err(|_| {
            Error::ReadFailure(
                contextdb_core::read_contract::ReadFailure::new(
                    contextdb_core::read_contract::ReadFailureKind::InvalidChannelData,
                    contextdb_core::read_contract::ReadFailureDetail::None,
                )
                .expect("invalid channel data accepts canonical empty detail"),
            )
        })?;
    Ok(crate::database::QueryTrace {
        physical_plan,
        index_used: canonical.index_used,
        predicates_pushed: {
            let mut predicates =
                smallvec::SmallVec::with_capacity(canonical.predicates_pushed.len());
            for predicate in canonical.predicates_pushed {
                predicates.push(std::borrow::Cow::Owned(predicate));
            }
            predicates
        },
        indexes_considered: {
            let mut candidates =
                smallvec::SmallVec::with_capacity(canonical.indexes_considered.len());
            for candidate in canonical.indexes_considered {
                candidates.push(crate::database::IndexCandidate {
                    name: candidate.name,
                    rejected_reason: std::borrow::Cow::Owned(candidate.rejected_reason),
                });
            }
            candidates
        },
        sort_elided: canonical.sort_elided,
        query_vector_source: canonical.query_vector_source,
        vector_search,
        rows_examined: canonical.rows_examined,
        groups: canonical.groups.into_boxed_slice(),
        maintained_count: canonical.maintained_count.map(|disclosure| {
            Box::new(crate::read_contract::CanonicalMaintainedCountDisclosure::restored(disclosure))
        }),
    })
}

/// Every plan label the engine writes. A reader recognises the word or says
/// it cannot.
fn known_plan_label(label: &str) -> Option<&'static str> {
    const LABELS: &[&str] = &[
        "Scan",
        "IndexScan",
        "Sort",
        "AdjacencyProbe",
        "EdgesScan",
        "GraphBfs",
        "HNSWSearch",
        "VectorSearch",
        "IndexScan -> HNSWSearch",
        "IndexScan -> VectorSearch",
        "Scan -> HNSWSearch",
        "Scan -> VectorSearch",
    ];
    LABELS.iter().copied().find(|known| *known == label)
}

/// One committed-file cursor, holding the suspended read that produced its
/// first page.
struct DirectCursorExecution {
    cursor: crate::direct_file_reader::DirectCursor,
    resources: Arc<RouteResources>,
    #[cfg(feature = "test-seams")]
    kernel_observer: Option<Arc<dyn ReadKernelTestObserver>>,
}

impl OwnedCursorExecution for DirectCursorExecution {
    fn work_observation(&mut self) -> Result<Option<crate::executor::CursorObservation>> {
        self.cursor
            .work_observation()
            .map_err(|error| direct_error(Path::new(""), error))
    }

    #[cfg(feature = "test-seams")]
    fn file_resource_witness_for_test(
        &self,
    ) -> Result<Option<crate::direct_file_reader::DirectCursorResourceWitness>> {
        crate::direct_file_reader::test_seams::cursor_resource_witness_for_test(&self.cursor)
            .map(Some)
            .map_err(|error| direct_error(Path::new(""), error))
    }

    fn fetch(
        &mut self,
        rows: Option<NonZeroUsize>,
        cancellation: &OwnerReadCancellation,
    ) -> Result<ObservedCursorPage> {
        #[cfg(feature = "test-seams")]
        let probe = self.kernel_observer.as_ref().map(|observer| {
            observed_kernel_probe(
                observer,
                ReadSessionOperation::CursorFetch,
                ReadRoute::File,
                cancellation,
            )
        });
        #[cfg(feature = "test-seams")]
        let fetched = with_session_kernel_probe(probe, || {
            self.cursor.fetch_with_cancellation(rows, cancellation)
        });
        #[cfg(not(feature = "test-seams"))]
        let fetched = self.cursor.fetch_with_cancellation(rows, cancellation);
        fetched
            .map(ObservedCursorPage::from)
            .map_err(|error| direct_error(Path::new(""), error))
    }

    fn close(&mut self) -> Result<()> {
        let closed = self
            .cursor
            .close()
            .map_err(|error| direct_error(Path::new(""), error));
        self.resources
            .active_cursors
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |count| Some(count.saturating_sub(1)),
            )
            .ok();
        closed
    }

    fn is_live(&self) -> bool {
        self.cursor.is_live()
    }
}

struct ObservedCursorPage {
    cancelled: bool,
    page: OwnedCursorPage,
    observation: crate::executor::CursorObservation,
    owner_trace_charge: Option<Arc<OwnerTraceCharge>>,
}
impl std::ops::Deref for ObservedCursorPage {
    type Target = OwnedCursorPage;
    fn deref(&self) -> &OwnedCursorPage {
        &self.page
    }
}
impl std::ops::DerefMut for ObservedCursorPage {
    fn deref_mut(&mut self) -> &mut OwnedCursorPage {
        &mut self.page
    }
}
impl From<crate::direct_file_reader::DirectCursorPage> for ObservedCursorPage {
    fn from(page: crate::direct_file_reader::DirectCursorPage) -> Self {
        Self {
            cancelled: false,
            page: page.page,
            observation: page.observation,
            owner_trace_charge: None,
        }
    }
}
fn take_cursor_observation(
    observation: crate::executor::CursorObservation,
) -> (
    crate::QueryTrace,
    Option<crate::executor::CursorObservation>,
) {
    let rows_examined = observation.rows_examined;
    if observation.charge.is_none() && observation.group_page_returns.is_none() {
        // Channel restoration already owns its admitted trace; move it into
        // the handle instead of allocating another copy.
        let mut trace =
            Arc::try_unwrap(observation.trace).unwrap_or_else(|trace| trace.as_ref().clone());
        trace.rows_examined = rows_examined;
        (trace, None)
    } else {
        // This single owned copy is covered by the shared kernel snapshot's
        // charge and reused until actual plan metadata changes.
        let mut trace = observation.trace.as_ref().clone();
        trace.rows_examined = rows_examined;
        (trace, Some(observation))
    }
}
/// Restored metadata remains charged to the continuation's existing owner
/// until the handle replaces it or drops, including after execution ends.
struct OwnerTraceCharge {
    memory: Arc<crate::read_image_memory::ImageMemory>,
    bytes: usize,
}
impl Drop for OwnerTraceCharge {
    fn drop(&mut self) {
        self.memory.release(self.bytes);
    }
}
fn cursor_receive_allowance(
    memory: u64,
    retained_memory: &crate::read_image_memory::ImageMemory,
) -> u64 {
    let configured = retained_memory
        .accountant
        .usage()
        .limit
        .map_or(u64::MAX, |bytes| u64::try_from(bytes).unwrap_or(u64::MAX));
    memory
        .min(configured)
        .saturating_sub(retained_memory.held() as u64)
}
fn decode_observed_page(
    payload: &[u8],
    memory: u64,
    retained_memory: &Arc<crate::read_image_memory::ImageMemory>,
) -> Result<ObservedCursorPage> {
    // The wire payload is still live while canonical containers and the
    // restored trace are built; it cannot spend the same allowance twice.
    let available =
        cursor_receive_allowance(memory, retained_memory).saturating_sub(payload.len() as u64);
    let (decoded_bytes, trace_bytes) =
        crate::local_transport::observed_cursor_page_allocation_bytes(payload, available)
            .map_err(read_encoding_error)?;
    // Canonical containers coexist with the restored trace. Two trace
    // footprints cover restored container growth; fixed trace and Arc owners
    // are added separately. Admission precedes decoding and restoration.
    let admitted = trace_bytes
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(decoded_bytes))
        .and_then(|bytes| {
            bytes.checked_add(
                2 * std::mem::size_of::<crate::QueryTrace>()
                    + std::mem::size_of::<OwnerTraceCharge>()
                    + 4 * std::mem::size_of::<u64>(),
            )
        })
        .ok_or_else(|| Error::Other("cursor trace memory overflow".to_string()))?;
    if admitted as u64 > available {
        return Err(read_encoding_error(
            crate::read_contract::ReadEncodingError::MemoryCeilingExceeded { ceiling: memory },
        ));
    }
    let mut credit = contextdb_core::read_memory::ReadCredit::try_new(
        retained_memory.clone(),
        admitted,
        "read_channel",
        "decode_cursor_page",
        "Reduce the page or raise the reader memory limit.",
    )?;
    let decoded = crate::read_contract::decode_observed_cursor_page(payload, available)
        .map_err(read_encoding_error)?;
    let rows_examined = decoded.trace.rows_examined;
    let trace = restored_query_trace(decoded.trace)?;
    let actual = crate::executor::bounded::query_trace_capacity_bytes(&trace)
        .map_err(read_error)?
        .checked_add(
            std::mem::size_of::<crate::QueryTrace>()
                + std::mem::size_of::<OwnerTraceCharge>()
                + 4 * std::mem::size_of::<usize>(),
        )
        .ok_or_else(|| Error::Other("cursor trace memory overflow".to_string()))?;
    let page_bytes = decoded_bytes
        .checked_sub(trace_bytes)
        .ok_or_else(|| Error::Other("cursor page admission underflow".into()))?;
    let retained = actual
        .checked_add(page_bytes)
        .ok_or_else(|| Error::Other("cursor page retention overflow".into()))?;
    if retained > admitted {
        return Err(Error::Other(
            "cursor trace exceeded its admitted restoration allowance".into(),
        ));
    }
    let page_credit = credit.try_split_off(page_bytes)?;
    credit.try_shrink(admitted - retained)?;
    let charge = OwnerTraceCharge {
        memory: retained_memory.clone(),
        bytes: actual,
    };
    credit.transfer();
    Ok(ObservedCursorPage {
        cancelled: false,
        page: OwnedCursorPage::from_parts(
            decoded.page.columns,
            decoded.page.rows,
            decoded.page.has_more,
        )
        .with_credit(page_credit),
        observation: crate::executor::CursorObservation {
            trace: Arc::new(trace),
            rows_examined,
            charge: None,
            group_page_returns: None,
        },
        owner_trace_charge: Some(Arc::new(charge)),
    })
}

/// A route-bound bounded cursor. Its lifetime is independent from the handle
/// that opened it: the cursor owns an `Arc` containing the route, session
/// snapshot owner, and suspended execution state.
pub struct ReadCursor {
    trace: crate::QueryTrace,
    observation: Option<crate::executor::CursorObservation>,
    owner_trace_charge: Option<Arc<OwnerTraceCharge>>,
    state: Arc<SuspendedReadCursorState>,
}

struct SuspendedReadCursorState {
    route: ReadRoute,
    session: Arc<ReadSessionState>,
    /// The route this cursor belongs to, so a fetch counts its source items
    /// against the same read the session does.
    resources: Arc<RouteResources>,
    /// The observer the session was opened with, so a page fetched later is
    /// reported to the same caller that asked about the read.
    progress: Option<Arc<dyn ReadProgressObserver>>,
    first_page: CursorPage,
    execution: Mutex<Box<dyn OwnedCursorExecution>>,
    #[cfg(feature = "test-seams")]
    observer: Option<Arc<dyn ReadSessionTestObserver>>,
    #[cfg(feature = "test-seams")]
    kernel_observer: Option<Arc<dyn ReadKernelTestObserver>>,
}

/// Object-safe ownership boundary for a backend's suspended snapshot and
/// execution state. Implementations must own their state; borrowed database
/// lifetimes cannot cross this boundary.
/// A cursor the owner is holding for this session.
///
/// The owner serves pages while available. This side retains the exact
/// registered storage root, statement and delivered-row position so an owner
/// withdrawal can resume through the existing file execution kernel.
struct OwnerGroupBaseline {
    groups: Box<[crate::database::GroupDisclosure]>,
    ordinals: Box<[u64]>,
    _charge: OwnerTraceCharge,
}
impl OwnerGroupBaseline {
    fn capture(
        trace: &crate::QueryTrace,
        memory: &Arc<crate::read_image_memory::ImageMemory>,
    ) -> Result<Self> {
        let bytes = trace
            .groups
            .len()
            .checked_mul(
                std::mem::size_of::<crate::database::GroupDisclosure>()
                    + std::mem::size_of::<u64>(),
            )
            .and_then(|bytes| {
                trace.groups.iter().try_fold(bytes, |bytes, group| {
                    bytes.checked_add(group.partition_key.len())
                })
            })
            .ok_or_else(|| Error::Other("group baseline memory overflow".into()))?;
        memory.reserve(bytes)?;
        let charge = OwnerTraceCharge {
            memory: memory.clone(),
            bytes,
        };
        let groups = trace
            .groups
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let ordinals = trace
            .groups
            .iter()
            .map(|group| group.ordinal)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(Self {
            groups,
            ordinals,
            _charge: charge,
        })
    }
}

struct OwnerCursorExecution {
    session: Arc<ReadSessionState>,
    resources: Arc<RouteResources>,
    limits: ReadLimits,
    cursor_id: [u8; 16],
    continuation: Option<OwnerCursorContinuation>,
    owner_rows_examined: u64,
    owner_groups: OwnerGroupBaseline,
    cancelled_observation: Option<crate::executor::CursorObservation>,
    cancelled_trace_charge: Option<Arc<OwnerTraceCharge>>,
    retained_memory: Arc<crate::read_image_memory::ImageMemory>,
    clock: Arc<dyn contextdb_core::read_contract::DeadlineClock>,
    created_ms: u64,
    last_used_ms: u64,
    closed: bool,
    expired: Option<contextdb_core::read_contract::CursorExpiryKind>,
    expiry_waker: Arc<Mutex<Option<std::thread::Thread>>>,
    #[cfg(feature = "test-seams")]
    observer: Option<Arc<dyn ReadSessionTestObserver>>,
}

struct OwnerCursorContinuation {
    retained_memory: Arc<crate::read_image_memory::ImageMemory>,
    retained_statement_bytes: usize,
    discarded_rows_examined: u64,
    failed_open_observation: Option<crate::executor::CursorObservation>,
    path: PathBuf,
    registration: Option<crate::persistence::ReadSnapshotRegistrationGuard>,
    config: crate::direct_file_reader::DirectReaderConfig,
    sql: String,
    params: HashMap<String, Value>,
    delivered: usize,
    cursor: Option<crate::direct_file_reader::DirectCursor>,
    skipped: usize,
}

impl Drop for OwnerCursorContinuation {
    fn drop(&mut self) {
        self.retained_memory.release(self.retained_statement_bytes);
    }
}

impl OwnerCursorContinuation {
    fn fetch(
        &mut self,
        rows: Option<NonZeroUsize>,
        cancellation: &OwnerReadCancellation,
    ) -> Result<ObservedCursorPage> {
        crate::executor::bounded::cursor_replay_checkpoint().map_err(read_error)?;
        if rows.is_some_and(|rows| rows.get() as u64 > self.config.limits.result_rows) {
            return Err(Error::ReadFailure(
                contextdb_core::read_contract::ReadFailure::owner_limit_exceeded(
                    contextdb_core::read_contract::OwnerLimitExceededDetail {
                        limit: contextdb_core::read_contract::ReadFailureLimit::ResultRows,
                        value: self.config.limits.result_rows,
                        required: None,
                        statement: None,
                    },
                ),
            ));
        }
        if self.cursor.is_none() {
            let registration = self
                .registration
                .as_ref()
                .ok_or_else(|| Error::ReadFailure(owner_disconnected()))?
                .retain_owner_snapshot();
            let mut config = self.config.clone();
            let available = usize::try_from(config.limits.memory)
                .unwrap_or(usize::MAX)
                .min(config.memory_limit.unwrap_or(usize::MAX))
                .saturating_sub(self.retained_memory.held());
            config.memory_limit = Some(config.memory_limit.unwrap_or(available).min(available));
            config.limits.memory = config.limits.memory.min(available as u64);
            config.limits.cursor_page_rows = config
                .limits
                .cursor_page_rows
                .min(self.delivered.max(1) as u64);
            let reader = crate::direct_file_reader::DirectFileReader::from_owner_snapshot_with_cancellation(
                &self.path,
                config,
                registration,
                cancellation,
            )
            .map_err(|error| direct_error(&self.path, error))?;
            let (opened, examined, open_observation) =
                crate::executor::bounded::with_cursor_open_work(|| {
                    reader.open_cursor_with_cancellation(
                        &self.sql,
                        &self.params,
                        NonZeroUsize::new(
                            self.delivered
                                .max(1)
                                .min(self.config.limits.cursor_page_rows as usize),
                        )
                        .unwrap(),
                        cancellation,
                    )
                });
            let opened = match opened {
                Ok(opened) => opened,
                Err(error) => {
                    self.failed_open_observation = open_observation
                        .map(|receipt| receipt.into_observation().map_err(read_error))
                        .transpose()?
                        .flatten();
                    self.discarded_rows_examined = self
                        .discarded_rows_examined
                        .checked_add(examined.map_err(read_error)?)
                        .ok_or_else(|| {
                            Error::Other("cursor rows-examined counter overflow".to_string())
                        })?;
                    return Err(direct_error(&self.path, error));
                }
            };
            let mut first_page = ObservedCursorPage::from(opened.first_page);
            self.cursor = Some(opened.cursor);
            if self.delivered == 0 {
                return Ok(first_page);
            }
            self.skipped = first_page.rows.len();
            if !crate::executor::bounded::cursor_replay_work_available() {
                first_page.observation = first_page
                    .observation
                    .unpublished_groups_withdrawn()
                    .map_err(read_error)?;
                first_page.rows.clear();
                first_page.has_more = true;
                return Ok(first_page);
            }
        }
        while self.skipped < self.delivered {
            crate::executor::bounded::cursor_replay_checkpoint().map_err(read_error)?;
            let mut page = self
                .cursor
                .as_mut()
                .unwrap()
                .fetch_with_cancellation(
                    NonZeroUsize::new(
                        (self.delivered - self.skipped)
                            .min(self.config.limits.cursor_page_rows as usize),
                    ),
                    cancellation,
                )
                .map(ObservedCursorPage::from)
                .map_err(|error| direct_error(&self.path, error))?;
            self.skipped += page.rows.len();
            if !page.has_more && self.skipped < self.delivered {
                return Err(Error::ReadFailure(owner_disconnected()));
            }
            if !crate::executor::bounded::cursor_replay_work_available() {
                page.observation = page
                    .observation
                    .unpublished_groups_withdrawn()
                    .map_err(read_error)?;
                page.rows.clear();
                page.has_more = true;
                return Ok(page);
            }
        }
        self.cursor
            .as_mut()
            .unwrap()
            .fetch_with_cancellation(
                rows.or_else(|| NonZeroUsize::new(self.config.limits.cursor_page_rows as usize)),
                cancellation,
            )
            .map(ObservedCursorPage::from)
            .map_err(|error| direct_error(&self.path, error))
    }
}

impl OwnerCursorExecution {
    fn merge_replay_groups(
        &self,
        mut observation: crate::executor::CursorObservation,
    ) -> Result<crate::executor::CursorObservation> {
        let groups = &observation.trace.groups;
        if groups.len() != self.owner_groups.groups.len()
            || groups
                .iter()
                .zip(self.owner_groups.ordinals.iter())
                .any(|(group, prior)| group.ordinal != *prior)
        {
            return Err(Error::ReadFailure(
                contextdb_core::read_contract::ReadFailure::new(
                    contextdb_core::read_contract::ReadFailureKind::InvalidChannelData,
                    contextdb_core::read_contract::ReadFailureDetail::None,
                )
                .expect("invalid replay group sequence accepts empty detail"),
            ));
        }
        for (replayed, owner) in groups.iter().zip(self.owner_groups.groups.iter()) {
            replayed
                .examined_entries
                .checked_add(owner.examined_entries)
                .ok_or_else(|| Error::Other("group examined counter overflow".into()))?;
        }
        // The existing kernel observation credit owns the outward trace copy.
        for (replayed, owner) in Arc::make_mut(&mut observation.trace)
            .groups
            .iter_mut()
            .zip(self.owner_groups.groups.iter())
        {
            replayed.examined_entries += owner.examined_entries;
        }
        Ok(observation)
    }

    fn fetch_continuation(
        &mut self,
        rows: Option<NonZeroUsize>,
        cancellation: &OwnerReadCancellation,
        started_ms: u64,
    ) -> Result<ObservedCursorPage> {
        let fetched = crate::executor::bounded::with_cursor_replay_budget(
            self.limits,
            Arc::clone(&self.clock),
            started_ms,
            self.created_ms,
            self.last_used_ms,
            || {
                self.continuation
                    .as_mut()
                    .unwrap()
                    .fetch(rows, cancellation)
            },
        );
        if fetched.is_err() {
            let failed = self
                .continuation
                .as_mut()
                .unwrap()
                .failed_open_observation
                .take();
            if let Some(observation) = failed {
                // Alignment is checked before merging or updating the durable
                // replay baseline. The failed open published no rows.
                let mut observation = self.merge_replay_groups(observation)?;
                for (baseline, merged) in self
                    .owner_groups
                    .groups
                    .iter_mut()
                    .zip(observation.trace.groups.iter())
                {
                    baseline.examined_entries = merged.examined_entries;
                }
                observation.rows_examined = self
                    .owner_rows_examined
                    .checked_add(self.continuation.as_ref().unwrap().discarded_rows_examined)
                    .ok_or_else(|| Error::Other("cursor rows-examined counter overflow".into()))?;
                // Move the admitted carrier to the outer observation path;
                // its charge survives terminal resource release.
                self.cancelled_observation = Some(observation);
            }
        }
        fetched.and_then(|mut page| {
            page.observation = self.merge_replay_groups(page.observation)?;
            page.observation.rows_examined = self
                .owner_rows_examined
                .checked_add(self.continuation.as_ref().unwrap().discarded_rows_examined)
                .and_then(|total| total.checked_add(page.observation.rows_examined))
                .ok_or_else(|| Error::Other("cursor rows-examined counter overflow".to_string()))?;
            Ok(page)
        })
    }

    fn release_local_cursor_slot(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.continuation = None;
        self.resources
            .active_cursors
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        if let Some(watcher) = self
            .expiry_waker
            .lock()
            .expect("cursor expiry watcher")
            .as_ref()
        {
            watcher.unpark();
        }
    }

    fn expiry_kind(&self, now: u64) -> Option<contextdb_core::read_contract::CursorExpiryKind> {
        if now.saturating_sub(self.created_ms) > self.limits.cursor_lifetime_ms {
            Some(contextdb_core::read_contract::CursorExpiryKind::Lifetime)
        } else if now.saturating_sub(self.last_used_ms) > self.limits.cursor_idle_ms {
            Some(contextdb_core::read_contract::CursorExpiryKind::Idle)
        } else {
            None
        }
    }

    fn expire_if_due(&mut self, now: u64) {
        if self.closed {
            return;
        }
        if let Some(kind) = self.expiry_kind(now) {
            self.expired = Some(kind);
            self.release_local_cursor_slot();
        }
    }

    fn expiry_deadline_ms(&self) -> Option<u64> {
        (!self.closed).then(|| {
            self.created_ms
                .saturating_add(self.limits.cursor_lifetime_ms)
                .min(self.last_used_ms.saturating_add(self.limits.cursor_idle_ms))
                .saturating_add(1)
        })
    }

    fn failure_ends_cursor(error: &Error) -> bool {
        use contextdb_core::read_contract::{ReadFailureDetail, ReadFailureKind, ReadFailureLimit};

        match error {
            Error::MemoryBudgetExceeded { .. } => true,
            Error::ReadSessionNotImplemented => true,
            Error::ReadFailure(failure) => {
                matches!(
                    failure.kind(),
                    ReadFailureKind::CursorExpired
                        | ReadFailureKind::AuthoritativeErasure
                        | ReadFailureKind::InvalidChannelData
                ) || matches!(
                    failure.detail(),
                    ReadFailureDetail::OwnerLimitExceeded(detail)
                        if detail.limit != ReadFailureLimit::ResultRows
                )
            }
            _ => false,
        }
    }

    fn ask(
        &self,
        request: crate::local_transport::LocalRequest,
        cancellation: Option<&OwnerReadCancellation>,
    ) -> Result<Vec<crate::local_transport::LocalResponse>> {
        let ReadSessionState::OwnerChannel(owner) = self.session.as_ref() else {
            return Err(Error::ReadFailure(owner_not_running()));
        };
        let cursor_memory = matches!(
            request,
            crate::local_transport::LocalRequest::CursorFetch { .. }
        )
        .then(|| cursor_receive_allowance(self.limits.memory, &self.retained_memory));
        owner.ask_with_cursor_memory(
            self.limits,
            request,
            cancellation,
            cursor_memory,
            #[cfg(feature = "test-seams")]
            self.observer.as_ref(),
        )
    }
}

impl OwnedCursorExecution for OwnerCursorExecution {
    fn work_observation(&mut self) -> Result<Option<crate::executor::CursorObservation>> {
        if let Some(observation) = self.cancelled_observation.take() {
            return Ok(Some(observation));
        }
        if let Some(continuation) = self.continuation.as_ref() {
            if let Some(cursor) = &continuation.cursor {
                let observation = cursor
                    .work_observation()
                    .map_err(|error| direct_error(&continuation.path, error))?;
                return observation
                    .map(|observation| {
                        let mut observation = self.merge_replay_groups(observation)?;
                        observation.rows_examined = self
                            .owner_rows_examined
                            .checked_add(continuation.discarded_rows_examined)
                            .and_then(|total| total.checked_add(observation.rows_examined))
                            .ok_or_else(|| {
                                Error::Other("cursor rows-examined counter overflow".into())
                            })?;
                        Ok(observation)
                    })
                    .transpose();
            }
        }
        Ok(None)
    }
    fn take_work_trace_charge(&mut self) -> Option<Arc<OwnerTraceCharge>> {
        self.cancelled_trace_charge.take()
    }

    fn fetch(
        &mut self,
        rows: Option<NonZeroUsize>,
        cancellation: &OwnerReadCancellation,
    ) -> Result<ObservedCursorPage> {
        let now = self.clock.now_ms();
        self.expire_if_due(now);
        if let Some(expiry) = self.expired {
            return Err(Error::ReadFailure(
                contextdb_core::read_contract::ReadFailure::cursor_expired(expiry),
            ));
        }
        if self
            .continuation
            .as_ref()
            .is_some_and(|continuation| continuation.cursor.is_some())
        {
            let page = self.fetch_continuation(rows, cancellation, now);
            if let Ok(page) = &page {
                self.last_used_ms = self.clock.now_ms();
                if !page.has_more {
                    self.release_local_cursor_slot();
                }
            } else if page.as_ref().is_err_and(Self::failure_ends_cursor) {
                self.release_local_cursor_slot();
            }
            return page;
        }
        let wire_rows = rows.map(|rows| {
            std::num::NonZeroU64::new(u64::try_from(rows.get()).unwrap_or(u64::MAX))
                .unwrap_or(std::num::NonZeroU64::MIN)
        });
        let page = self
            .ask(
                crate::local_transport::LocalRequest::CursorFetch {
                    cursor_id: self.cursor_id,
                    rows: wire_rows,
                },
                Some(cancellation),
            )
            .and_then(|responses| {
                published_cursor_page(
                    responses,
                    self.limits,
                    &self.retained_memory,
                    #[cfg(feature = "test-seams")]
                    self.observer.as_ref(),
                )
            });
        let (page, owner_cursor_may_be_live) = match page {
            Err(Error::ReadFailure(ref failure))
                if matches!(
                    failure.kind(),
                    contextdb_core::read_contract::ReadFailureKind::OwnerDisconnected
                        | contextdb_core::read_contract::ReadFailureKind::OwnerNotRunning
                        | contextdb_core::read_contract::ReadFailureKind::OwnerNotServing
                ) && self.continuation.is_some() =>
            {
                (self.fetch_continuation(rows, cancellation, now), false)
            }
            page => (page, true),
        };
        if let Ok(page) = &page {
            if self
                .continuation
                .as_ref()
                .is_none_or(|continuation| continuation.cursor.is_none())
            {
                self.owner_groups = match OwnerGroupBaseline::capture(
                    &page.observation.trace,
                    &self.retained_memory,
                ) {
                    Ok(groups) => groups,
                    Err(error) => {
                        self.release_local_cursor_slot();
                        if owner_cursor_may_be_live {
                            let _ = self.ask(
                                crate::local_transport::LocalRequest::CursorClose {
                                    cursor_id: self.cursor_id,
                                },
                                None,
                            );
                        }
                        return Err(error);
                    }
                };
            }
            if page.cancelled {
                self.owner_rows_examined = page.observation.rows_examined;
                self.cancelled_observation = Some(page.observation.clone());
                self.cancelled_trace_charge = page.owner_trace_charge.clone();
                return Err(Error::ReadCancelled);
            }
            self.cancelled_observation = None;
            self.cancelled_trace_charge = None;
            if self
                .continuation
                .as_ref()
                .is_none_or(|continuation| continuation.cursor.is_none())
            {
                self.owner_rows_examined = page.observation.rows_examined;
            }
            self.last_used_ms = self.clock.now_ms();
            if let Some(continuation) = &mut self.continuation
                && continuation.cursor.is_none()
            {
                continuation.delivered += page.rows.len();
            }
            if !page.has_more {
                self.release_local_cursor_slot();
            }
        } else if page.as_ref().is_err_and(Self::failure_ends_cursor) {
            self.release_local_cursor_slot();
            // An owner response can fail on this client's decode ceiling or
            // contain unreadable bytes. The owner may still hold its cursor.
            // Local storage release must happen before a best-effort round trip.
            if owner_cursor_may_be_live {
                let _ = self.ask(
                    crate::local_transport::LocalRequest::CursorClose {
                        cursor_id: self.cursor_id,
                    },
                    None,
                );
            }
        }
        page
    }

    fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        if let Some(continuation) = &mut self.continuation
            && let Some(cursor) = &mut continuation.cursor
        {
            let closed = cursor
                .close()
                .map_err(|error| direct_error(&continuation.path, error));
            self.release_local_cursor_slot();
            return closed;
        }
        self.release_local_cursor_slot();
        let responses = self.ask(
            crate::local_transport::LocalRequest::CursorClose {
                cursor_id: self.cursor_id,
            },
            None,
        )?;
        for response in responses {
            match response {
                crate::local_transport::LocalResponse::CursorClosed { .. } => return Ok(()),
                crate::local_transport::LocalResponse::Failure { failure } => {
                    return Err(Error::ReadFailure(failure));
                }
                _ => {}
            }
        }
        Err(Error::ReadFailure(owner_disconnected()))
    }

    fn is_live(&self) -> bool {
        !self.closed
            && self
                .continuation
                .as_ref()
                .and_then(|continuation| continuation.cursor.as_ref())
                .is_none_or(|cursor| cursor.is_live())
    }

    fn expiry_watch(
        &self,
    ) -> Option<(
        Arc<dyn contextdb_core::read_contract::DeadlineClock>,
        u64,
        Arc<Mutex<Option<std::thread::Thread>>>,
    )> {
        self.expiry_deadline_ms().map(|deadline| {
            (
                Arc::clone(&self.clock),
                deadline,
                Arc::clone(&self.expiry_waker),
            )
        })
    }

    fn expire_from_watch(&mut self, now: u64) {
        self.expire_if_due(now);
    }
}

struct PublishedOwnerCursor {
    cursor_id: [u8; 16],
    first_page: OwnedCursorPage,
    observation: crate::executor::CursorObservation,
    owner_trace_charge: Arc<OwnerTraceCharge>,
    snapshot: Option<redb::StorageSnapshotIdentity>,
    physical_identity: Option<(u64, u64)>,
    limits: ReadLimits,
}

/// The identity and first page the owner published when it opened a cursor.
fn published_cursor_open(
    responses: Vec<crate::local_transport::LocalResponse>,
    limits: ReadLimits,
    retained_memory: &Arc<crate::read_image_memory::ImageMemory>,
    #[cfg(feature = "test-seams")] observer: Option<&Arc<dyn ReadSessionTestObserver>>,
) -> Result<PublishedOwnerCursor> {
    for response in responses {
        match response {
            crate::local_transport::LocalResponse::CursorOpened { opened } => {
                #[cfg(feature = "test-seams")]
                let replacement = observer
                    .and_then(|observer| observer.cursor_open_payload_for_test(&opened.payload));
                #[cfg(feature = "test-seams")]
                let payload = replacement.as_deref().unwrap_or(&opened.payload);
                #[cfg(not(feature = "test-seams"))]
                let payload = &opened.payload;
                let limits =
                    ReadLimits::stricter_of(limits, opened.effective_limits.unwrap_or(limits))
                        .map_err(|error| Error::Other(error.to_string()))?;
                let page = decode_observed_page(payload, limits.memory, retained_memory)?;
                let snapshot = opened.storage_snapshot.map(|(transaction_id, root)| {
                    redb::StorageSnapshotIdentity {
                        transaction_id,
                        user_root: root.map(|(page, checksum, length)| {
                            redb::StorageUserRootIdentity {
                                page,
                                checksum: u128::from_le_bytes(checksum),
                                length,
                            }
                        }),
                    }
                });
                return Ok(PublishedOwnerCursor {
                    cursor_id: opened.cursor_id,
                    first_page: page.page,
                    observation: page.observation,
                    owner_trace_charge: page
                        .owner_trace_charge
                        .expect("owner page retains its trace charge"),
                    snapshot,
                    physical_identity: opened.physical_identity,
                    limits,
                });
            }
            crate::local_transport::LocalResponse::Failure { failure } => {
                return Err(Error::ReadFailure(failure));
            }
            _ => {}
        }
    }
    Err(Error::ReadFailure(owner_disconnected()))
}

/// One page the owner published for a cursor it is holding.
fn published_cursor_page(
    responses: Vec<crate::local_transport::LocalResponse>,
    limits: ReadLimits,
    retained_memory: &Arc<crate::read_image_memory::ImageMemory>,
    #[cfg(feature = "test-seams")] observer: Option<&Arc<dyn ReadSessionTestObserver>>,
) -> Result<ObservedCursorPage> {
    for response in responses {
        match response {
            crate::local_transport::LocalResponse::CursorCancelled { page } => {
                let mut observed =
                    decode_observed_page(&page.payload, limits.memory, retained_memory)?;
                if !observed.rows.is_empty() {
                    return Err(Error::Other(
                        "cancelled cursor observation contains rows".into(),
                    ));
                }
                observed.cancelled = true;
                return Ok(observed);
            }
            crate::local_transport::LocalResponse::CursorPage { page } => {
                #[cfg(feature = "test-seams")]
                let replacement = observer
                    .and_then(|observer| observer.cursor_fetch_payload_for_test(&page.payload));
                #[cfg(feature = "test-seams")]
                let payload = replacement.as_deref().unwrap_or(&page.payload);
                #[cfg(not(feature = "test-seams"))]
                let payload = &page.payload;
                return decode_observed_page(payload, limits.memory, retained_memory);
            }
            crate::local_transport::LocalResponse::Failure { failure } => {
                return Err(Error::ReadFailure(failure));
            }
            _ => {}
        }
    }
    Err(Error::ReadFailure(owner_disconnected()))
}

type CursorExpiryWatch = (
    Arc<dyn contextdb_core::read_contract::DeadlineClock>,
    u64,
    Arc<Mutex<Option<std::thread::Thread>>>,
);

trait OwnedCursorExecution: Send {
    fn take_work_trace_charge(&mut self) -> Option<Arc<OwnerTraceCharge>> {
        None
    }
    fn work_observation(&mut self) -> Result<Option<crate::executor::CursorObservation>> {
        Ok(None)
    }

    #[cfg(feature = "test-seams")]
    fn file_resource_witness_for_test(
        &self,
    ) -> Result<Option<crate::direct_file_reader::DirectCursorResourceWitness>> {
        Ok(None)
    }

    fn fetch(
        &mut self,
        rows: Option<NonZeroUsize>,
        cancellation: &OwnerReadCancellation,
    ) -> Result<ObservedCursorPage>;

    fn close(&mut self) -> Result<()>;

    /// Whether the suspended read behind this cursor is still there.
    fn is_live(&self) -> bool;

    fn expiry_watch(&self) -> Option<CursorExpiryWatch> {
        None
    }

    fn expire_from_watch(&mut self, _now: u64) {}
}

/// A cursor's deadline remains active even when its caller keeps the object
/// but never fetches again. The watcher owns no cursor or page hold; it wakes
/// on the declared clock, then closes the retained root under the same cursor
/// mutex as a foreground fetch. Closing a cursor unparks it immediately.
fn start_owner_cursor_expiry_watch(state: &Arc<SuspendedReadCursorState>) {
    struct WakeThread(std::thread::Thread);
    impl std::task::Wake for WakeThread {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let Some((clock, mut deadline, waker_slot)) = state
        .execution
        .lock()
        .expect("read cursor state")
        .expiry_watch()
    else {
        return;
    };
    let weak = Arc::downgrade(state);
    std::thread::Builder::new()
        .name("contextdb-cursor-expiry".to_owned())
        .spawn(move || {
            *waker_slot.lock().expect("cursor expiry watcher") = Some(std::thread::current());
            let waker = std::task::Waker::from(Arc::new(WakeThread(std::thread::current())));
            let mut context = std::task::Context::from_waker(&waker);
            loop {
                let mut wait = clock.wait_until(deadline);
                loop {
                    if wait.as_mut().poll(&mut context).is_ready() {
                        break;
                    }
                    std::thread::park();
                    let Some(state) = weak.upgrade() else {
                        return;
                    };
                    if !state.execution.lock().expect("read cursor state").is_live() {
                        return;
                    }
                }
                let Some(state) = weak.upgrade() else {
                    return;
                };
                let mut execution = state.execution.lock().expect("read cursor state");
                let was_live = execution.is_live();
                execution.expire_from_watch(clock.now_ms());
                #[cfg(feature = "test-seams")]
                if was_live
                    && !execution.is_live()
                    && let Some(observer) = state.observer.as_ref()
                {
                    observer.observe_event(ReadSessionEvent::OwnerCursorExpiryReleased);
                }
                #[cfg(not(feature = "test-seams"))]
                let _ = was_live;
                let Some((_, next_deadline, _)) = execution.expiry_watch() else {
                    return;
                };
                deadline = next_deadline;
            }
        })
        .expect("start cursor expiry watcher");
}

impl Drop for OwnerCursorExecution {
    fn drop(&mut self) {
        self.release_local_cursor_slot();
    }
}

impl ReadCursor {
    /// Observe the existing file cursor's privately owned retention resources.
    /// A remote owner cursor has no locally inspectable file-resource witness.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn file_resource_witness_for_test(
        &self,
    ) -> Result<Option<crate::direct_file_reader::DirectCursorResourceWitness>> {
        self.state
            .execution
            .lock()
            .expect("read cursor state")
            .file_resource_witness_for_test()
    }

    pub fn route(&self) -> ReadRoute {
        self.state.route
    }

    /// The page produced atomically with cursor creation. A live cursor owns
    /// this page together with its suspended execution state.
    pub fn first_page(&self) -> &CursorPage {
        &self.state.first_page
    }

    /// The actual plan and cumulative source work at the latest successfully
    /// published page. A failed fetch retains the preceding observation;
    /// work from a cancelled kernel attempt is included by its next successful
    /// page. Owner withdrawal continues counting actual file replay work too.
    ///
    /// ```no_run
    /// # fn example() -> contextdb_core::Result<()> {
    /// use contextdb_engine::ReadSession;
    /// use std::collections::HashMap;
    /// let session = ReadSession::open("store.db")?;
    /// let mut cursor = session.open_cursor("SELECT * FROM items", &HashMap::new())?;
    /// println!("{}: {}", cursor.trace().physical_plan, cursor.rows_examined());
    /// while cursor.first_page().has_more && cursor.is_live() {
    ///     let page = cursor.fetch(None)?;
    ///     println!("examined {}", cursor.trace().rows_examined);
    ///     if !page.has_more { break; }
    /// }
    /// # Ok(()) }
    /// ```
    pub fn trace(&self) -> &crate::QueryTrace {
        &self.trace
    }

    /// Cumulative rows, index entries and adjacency entries actually examined
    /// through the latest successful page; this is not the number of emitted rows.
    pub fn rows_examined(&self) -> u64 {
        self.trace.rows_examined
    }

    /// Fetch a cursor page with a fresh cancellation token.
    pub fn fetch(&mut self, rows: Option<NonZeroUsize>) -> Result<CursorPage> {
        self.fetch_with_cancellation(rows, &OwnerReadCancellation::new())
    }

    /// Fetch a cursor page using the caller's cancellation token.
    pub fn fetch_with_cancellation(
        &mut self,
        rows: Option<NonZeroUsize>,
        cancellation: &OwnerReadCancellation,
    ) -> Result<CursorPage> {
        let resources = Arc::clone(&self.state.resources);
        let progress = self.state.progress.as_ref().map(Arc::clone);
        contextdb_core::find_attribution::begin("cursor fetch");
        let fetched = with_progress_observer(progress.as_ref(), || {
            with_route_source_counter(&resources, || self.fetch_within_route(rows, cancellation))
        });
        contextdb_core::find_attribution::finish();
        fetched
    }

    fn fetch_within_route(
        &mut self,
        rows: Option<NonZeroUsize>,
        cancellation: &OwnerReadCancellation,
    ) -> Result<CursorPage> {
        let mut execution = self.state.execution.lock().expect("read cursor state");
        let _ = &self.state.session;
        #[cfg(feature = "test-seams")]
        let _ = (&self.state.observer, &self.state.kernel_observer);
        let fetched = execution.fetch(rows, cancellation);
        if fetched.is_err() {
            if let Ok(Some(observation)) = execution.work_observation() {
                let observation = observation
                    .unpublished_groups_withdrawn()
                    .map_err(read_error)?;
                (self.trace, self.observation) = take_cursor_observation(observation);
                if let Some(charge) = execution.take_work_trace_charge() {
                    self.owner_trace_charge = Some(charge);
                }
            }
        }
        if fetched.is_err() && !execution.is_live() {
            let _ = execution.close();
        }
        fetched.map(|page| {
            // The owner answers a later exhausted fetch with its empty sentinel.
            // It contains no new execution and cannot replace the last trace.
            if !(page.columns.is_empty() && page.rows.is_empty() && !page.has_more) {
                let rows_examined = page.observation.rows_examined;
                if self
                    .observation
                    .as_ref()
                    .is_none_or(|prior| !Arc::ptr_eq(&prior.trace, &page.observation.trace))
                {
                    (self.trace, self.observation) = take_cursor_observation(page.observation);
                    self.owner_trace_charge = page.owner_trace_charge;
                } else {
                    self.observation = Some(page.observation);
                }
                self.trace.rows_examined = rows_examined;
            }
            page.page.into_public()
        })
    }

    /// Whether this cursor still holds the read it was opened with.
    ///
    /// A refusal that ended the read gave back its retained bytes and unpinned
    /// its snapshot rather than waiting for a caller to remember to close it,
    /// so the handle survives its own cursor. A session that keeps one cursor
    /// slot asks here before putting the handle back: a slot that goes on
    /// claiming a released cursor answers `cursor_already_open` to the next
    /// `.cursor open` while every fetch and close says the cursor is gone,
    /// which leaves the session with no cursor and no way to get one.
    pub fn is_live(&self) -> bool {
        self.state
            .execution
            .lock()
            .expect("read cursor state")
            .is_live()
    }

    /// Release the cursor's snapshot and route resources.
    pub fn close(self) -> Result<()> {
        self.state
            .execution
            .lock()
            .expect("read cursor state")
            .close()
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn retained_state_owners_for_test(&self) -> usize {
        Arc::strong_count(&self.state)
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn retained_session_owners_for_test(&self) -> usize {
        Arc::strong_count(&self.state.session)
    }
}

/// Test-only controls consumed by the owner service once it is assembled.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Clone)]
pub struct OwnerReadTestHooks {
    pub clock: Arc<dyn DeadlineClock>,
    pub drain_started: std::sync::mpsc::Sender<()>,
    /// Fires after a request is registered in the owner cancellation registry.
    /// Observation only: the hook never holds a service lock or a worker.
    pub active_request_registered: Option<std::sync::mpsc::Sender<()>>,
    /// Fires after the accept worker has completed its listener loop.
    pub accept_thread_exited: Option<std::sync::mpsc::Sender<()>>,
    /// Fires exactly once after the service itself has joined its accept
    /// worker during an explicit close. Ordinary drop transfers that join to
    /// a nonwaiting reaper and deliberately leaves this hook unsent.
    pub accept_thread_joined: Option<std::sync::mpsc::Sender<()>>,
    /// Fires from the service's final drop after every `Arc` holder released
    /// it. Kept per configured owner so tests never infer lifecycle from a
    /// process-wide counter shared with another fixture.
    pub service_dropped: Option<std::sync::mpsc::Sender<()>>,
    /// Parks an owner close at each externally observable handoff boundary.
    pub quiescent_stop: Option<Arc<dyn Fn(QuiescentStopPhase) + Send + Sync>>,
}

#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuiescentStopPhase {
    WritesStopped,
    Published,
    ChannelWithdrawn,
    AcceptedDrained,
}

/// Internal status placeholder retained so the public `Database` door has a
/// stable type before owner lifecycle assembly exists.
pub(crate) fn owner_read_not_implemented_status() -> OwnerReadStatus {
    OwnerReadStatus {
        state: contextdb_core::read_contract::OwnerServingState::NotServing,
        reason: Some(
            contextdb_core::read_contract::OwnerServingReason::StartupFailure(
                "owner reads are not implemented".to_owned(),
            ),
        ),
    }
}

#[cfg(feature = "test-seams")]
impl ReadSession {
    /// Use the Database already installed behind this session's execution
    /// target, preserving its captured root and fallible history capability.
    #[doc(hidden)]
    pub fn __with_resident_history_for_test<T>(
        &self,
        operation: impl FnOnce(&Database) -> Result<T>,
    ) -> Result<T> {
        match self.state.as_ref() {
            ReadSessionState::LiveDatabase(database) => operation(database.as_ref()),
            ReadSessionState::DirectFile(reader) => {
                let target = reader.execution_target();
                operation(target.resident_history_database_for_test()?)
            }
            ReadSessionState::OwnerChannel(_) => Err(Error::PlanError(
                "owner-channel session has no local installed history database".into(),
            )),
        }
    }
}
