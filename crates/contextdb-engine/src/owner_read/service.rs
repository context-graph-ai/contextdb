//! Service-side RED assembly for authenticated owner reads.

use super::admission::{OwnerAdmission, RequestLease};
use super::{OwnerReadScaffoldError, OwnerReadScaffoldResult, response_expectation};
use crate::executor::{
    BoundedCursorHandle, BoundedCursorOpen, BoundedExecutionError, close_bounded_cursor,
    execute_bounded, fetch_bounded_cursor, open_bounded_cursor,
};
#[cfg(feature = "test-seams")]
use crate::executor::{BoundedExecutionProbe, BoundedSourceTouch, BoundedWorkSource};
use crate::local_transport::response_is_encodable;
use crate::local_transport::{
    ChannelPathFacts, CursorCloseAcknowledgement, CursorOpenedResponse, CursorPageResponse,
    LocalConfigurationSource, LocalDeadlineOperation, LocalEffectiveLimits, LocalEngineFailure,
    LocalHandshake, LocalInboundKind, LocalInboundMessage, LocalMetadataRequest,
    LocalOutboundMessage, LocalOwnerStatusResponse, LocalOwnerTimeouts, LocalProtocolBoundary,
    LocalRequest, LocalRequestEnvelope, LocalResponse, LocalResponseExpectation, MetadataResponse,
    OwnerMemoryCounters, ReadPrincipal, drain_shutdown_with_deadline, split_canonical_result,
    split_payload_answer, validate_channel_path, validate_handshake,
};
#[cfg(unix)]
use crate::local_transport::{
    UnixLocalCarrier, authenticate_framed_stream_handshake, serve_request_with_deadline,
};
use crate::read_contract::{encode_cursor_page, encode_metadata_page, encode_query_result};
use crate::{Database, OwnerReadConfig};
use contextdb_core::read_contract::{
    CursorExpiryKind, CursorPage, DeadlineClock, LocalUserIdentity, MetadataPage,
    MetadataPageVocabulary, OwnerLimitExceededDetail, OwnerReadCancellation, OwnerReadLimits,
    OwnerReadStatus, OwnerServingReason, OwnerServingState, ReadFailure, ReadFailureDetail,
    ReadFailureKind, ReadFailureLimit, ReadLimits,
};
use contextdb_parser::{StatementEffect, parse, statement_effect};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Poll, Waker};

#[cfg(feature = "test-seams")]
use crate::executor::bounded_read_test_support::{TestSourceTouch, TestWorkSource};

/// Private cursor identifiers are allocated behind one deterministic seam so
/// lifecycle tests can force actual reuse without manufacturing cursor state.
pub trait CursorIdentifierAllocator: Send + Sync {
    fn allocate(&self, writer_run: [u8; 16]) -> OwnerReadScaffoldResult<[u8; 16]>;
}

#[derive(Debug, Default)]
struct SequenceCursorIdentifierAllocator {
    sequence: AtomicU64,
}

impl CursorIdentifierAllocator for SequenceCursorIdentifierAllocator {
    fn allocate(&self, mut writer_run: [u8; 16]) -> OwnerReadScaffoldResult<[u8; 16]> {
        let prior = self
            .sequence
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_add(1)
            })
            .map_err(|_| {
                OwnerReadScaffoldError::unimplemented("cursor identifier sequence exhaustion")
            })?;
        for (target, source) in writer_run[8..].iter_mut().zip((prior + 1).to_be_bytes()) {
            *target ^= source;
        }
        Ok(writer_run)
    }
}

#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerBoundedOperation {
    Execute,
    CursorOpen,
    CursorFetch,
}

/// Test-only observation is attached to the production bounded-loop probe;
/// it does not execute, classify, size, or account for any work itself.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub trait OwnerBoundedExecutionObserver: Send + Sync {
    fn before_operation(
        &self,
        operation: OwnerBoundedOperation,
        effective_limits: ReadLimits,
        cancellation: OwnerReadCancellation,
    );

    fn before_work(&self, source: TestWorkSource, completed_work: u64);

    fn before_source_touch(&self, _touch: TestSourceTouch, _completed_items: u64) {}

    fn before_temporary_reservation(
        &self,
        _source: TestWorkSource,
        _requested_bytes: u64,
        _held_temporary_bytes: u64,
    ) {
    }

    fn after_temporary_reservation(
        &self,
        _source: TestWorkSource,
        _reserved_bytes: u64,
        _held_temporary_bytes: u64,
    ) {
    }

    fn cancellation_observed(&self, completed_work: u64);

    /// Notification only; the carrier path still owns detecting EOF/HUP and
    /// signalling the production token before this callback runs.
    fn disconnect_cancellation_signalled(&self) {}

    fn request_deadline_cancellation_signalled(&self) {}

    fn shutdown_cancellation_signalled(&self) {}

    fn request_finished(&self) {}
}

#[cfg(feature = "test-seams")]
struct OwnerBoundedProbeAdapter {
    observer: Arc<dyn OwnerBoundedExecutionObserver>,
    operation: OwnerBoundedOperation,
    effective_limits: ReadLimits,
    cancellation: OwnerReadCancellation,
    started: AtomicBool,
}

#[cfg(feature = "test-seams")]
impl OwnerBoundedProbeAdapter {
    fn observe_start(&self) {
        if self
            .started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.observer.before_operation(
                self.operation,
                self.effective_limits,
                self.cancellation.clone(),
            );
        }
    }
}

#[cfg(feature = "test-seams")]
impl BoundedExecutionProbe for OwnerBoundedProbeAdapter {
    fn before_work(&self, source: BoundedWorkSource, completed_work: u64) {
        self.observe_start();
        self.observer.before_work(source.into(), completed_work);
    }

    fn before_source_touch(&self, touch: BoundedSourceTouch, completed_items: u64) {
        self.observe_start();
        self.observer
            .before_source_touch(touch.into(), completed_items);
    }

    fn before_hnsw_candidate_distance(
        &self,
        event: contextdb_vector::hnsw::HnswCandidateDistanceEvent,
    ) {
        self.observe_start();
        self.observer
            .before_source_touch(TestSourceTouch::HnswCandidate, event.completed_candidates());
    }

    fn before_temporary_reservation(
        &self,
        source: BoundedWorkSource,
        requested_bytes: u64,
        held_temporary_bytes: u64,
    ) {
        self.observe_start();
        self.observer.before_temporary_reservation(
            source.into(),
            requested_bytes,
            held_temporary_bytes,
        );
    }

    fn after_temporary_reservation(
        &self,
        source: BoundedWorkSource,
        reserved_bytes: u64,
        held_temporary_bytes: u64,
    ) {
        self.observe_start();
        self.observer.after_temporary_reservation(
            source.into(),
            reserved_bytes,
            held_temporary_bytes,
        );
    }

    fn cancellation_observed(&self, completed_work: u64) {
        self.observe_start();
        self.observer.cancellation_observed(completed_work);
    }
}

/// Path facts which must pass the shared local-channel validator before the
/// real Unix carrier is allowed to bind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedOwnerListener {
    facts: ChannelPathFacts,
}

impl ValidatedOwnerListener {
    pub fn new(facts: ChannelPathFacts) -> Self {
        Self { facts }
    }

    pub fn path(&self) -> &std::path::Path {
        &self.facts.path
    }

    fn validate(&self, owner_user: LocalUserIdentity) -> OwnerReadScaffoldResult<()> {
        validate_channel_path(&self.facts, owner_user)?;
        Ok(())
    }

    #[cfg(unix)]
    fn bind(&self) -> OwnerReadScaffoldResult<std::os::unix::net::UnixListener> {
        Ok(UnixLocalCarrier.listen(self.path())?)
    }
}

/// Complete construction input for a live owner service. It owns the database
/// handle through `Arc`, the declared policy, monotonic clock, expected local
/// identity, status, validated listener facts, and optional custom handler.
#[derive(Clone)]
pub struct OwnerServiceSpec {
    database: Arc<Database>,
    listener: ValidatedOwnerListener,
    expected_handshake: LocalHandshake,
    status: OwnerReadStatus,
    config: OwnerReadConfig,
    configuration_source: LocalConfigurationSource,
    clock: Arc<dyn DeadlineClock>,
    cursor_identifiers: Arc<dyn CursorIdentifierAllocator>,
    #[cfg(feature = "test-seams")]
    execution_observer: Option<Arc<dyn OwnerBoundedExecutionObserver>>,
}

impl OwnerServiceSpec {
    pub fn new(
        database: Arc<Database>,
        listener: ValidatedOwnerListener,
        expected_handshake: LocalHandshake,
        status: OwnerReadStatus,
        config: OwnerReadConfig,
        configuration_source: LocalConfigurationSource,
        clock: Arc<dyn DeadlineClock>,
    ) -> Self {
        Self {
            database,
            listener,
            expected_handshake,
            status,
            config,
            configuration_source,
            clock,
            cursor_identifiers: Arc::new(SequenceCursorIdentifierAllocator::default()),
            #[cfg(feature = "test-seams")]
            execution_observer: None,
        }
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn with_cursor_identifier_allocator_for_test(
        mut self,
        allocator: Arc<dyn CursorIdentifierAllocator>,
    ) -> Self {
        self.cursor_identifiers = allocator;
        self
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn with_execution_observer_for_test(
        mut self,
        observer: Arc<dyn OwnerBoundedExecutionObserver>,
    ) -> Self {
        self.execution_observer = Some(observer);
        self
    }

    /// Invoke the existing validators rather than reproducing their rules.
    pub fn validate(&self) -> OwnerReadScaffoldResult<()> {
        self.config.limits.validate()?;
        self.config.timeouts.validate()?;
        self.status.validate()?;
        self.listener.validate(self.expected_handshake.owner_user)?;
        validate_handshake(
            &self.expected_handshake,
            &self.expected_handshake,
            self.expected_handshake.owner_user,
        )?;
        Ok(())
    }

    pub fn database(&self) -> &Arc<Database> {
        &self.database
    }

    pub fn config(&self) -> &OwnerReadConfig {
        &self.config
    }

    pub fn clock(&self) -> &Arc<dyn DeadlineClock> {
        &self.clock
    }

    #[cfg(feature = "test-seams")]
    fn bounded_probe(
        &self,
        operation: OwnerBoundedOperation,
        limits: ReadLimits,
        cancellation: OwnerReadCancellation,
    ) -> Option<Arc<dyn BoundedExecutionProbe>> {
        // Work asked for over the channel runs HERE, on the owner's own
        // thread, so an observer watching this store has to be handed the
        // kernel directly -- a reader's own thread never touches it. The
        // service's execution observer is a different question, about what
        // this owner did, and answers when nobody is watching the store.
        if let Some(watcher) = self.database.kernel_observer_for_test() {
            return Some(crate::read_session::owner_route_kernel_probe(
                &watcher,
                match operation {
                    OwnerBoundedOperation::Execute => {
                        crate::read_session::ReadSessionOperation::Execute
                    }
                    OwnerBoundedOperation::CursorOpen => {
                        crate::read_session::ReadSessionOperation::CursorOpen
                    }
                    OwnerBoundedOperation::CursorFetch => {
                        crate::read_session::ReadSessionOperation::CursorFetch
                    }
                },
                &cancellation,
            ));
        }
        self.execution_observer.as_ref().map(|observer| {
            Arc::new(OwnerBoundedProbeAdapter {
                observer: Arc::clone(observer),
                operation,
                effective_limits: limits,
                cancellation,
                started: AtomicBool::new(false),
            }) as Arc<dyn BoundedExecutionProbe>
        })
    }
}

/// Authenticated state for one local connection. Authentication and all
/// handshake identity checks happen before this value can reach admission.
#[derive(Debug)]
pub struct ConnectionState {
    connection_id: [u8; 16],
    principal: ReadPrincipal,
    effective_limits: ReadLimits,
    cancellation: OwnerReadCancellation,
}

impl ConnectionState {
    fn authenticated(
        connection_id: [u8; 16],
        principal: ReadPrincipal,
        requested: ReadLimits,
        owner: OwnerReadLimits,
    ) -> OwnerReadScaffoldResult<Self> {
        let effective_limits = OwnerAdmission::effective_limits(requested, owner)?;
        Ok(Self {
            connection_id,
            principal,
            effective_limits,
            cancellation: OwnerReadCancellation::new(),
        })
    }

    pub const fn connection_id(&self) -> [u8; 16] {
        self.connection_id
    }

    pub const fn effective_limits(&self) -> ReadLimits {
        self.effective_limits
    }

    pub const fn cancellation(&self) -> &OwnerReadCancellation {
        &self.cancellation
    }
}

/// One decoded request plus the carrier-supplied peer identity.
#[derive(Debug, Clone)]
struct InboundOwnerRequest {
    connection_id: [u8; 16],
    principal: ReadPrincipal,
    envelope: LocalRequestEnvelope,
}

/// The complete service-side cursor resource bundle between fetches. The
/// current bounded handle is lifetime-free and privately owns its kernel
/// snapshot registration, continuation, and request-memory reservation.
/// Taking a second database snapshot here could observe a different concurrent
/// commit, so this entry owns the compiled handle and records only the
/// service's additional registry metadata and admission lease.
/// This cursor's retained charge after a fetch moved the engine's own total.
///
/// The engine accountant is shared, so the signed difference across this one
/// fetch is what THIS cursor's page did; applying it to the number the cursor
/// was already reported as holding keeps the entry and the fleet total honest
/// without either one re-deriving the other.
fn adjusted_retained_bytes(previous: u64, memory_before: usize, memory_after: usize) -> u64 {
    if memory_after >= memory_before {
        let taken = u64::try_from(memory_after.saturating_sub(memory_before)).unwrap_or(u64::MAX);
        previous.saturating_add(taken)
    } else {
        let returned =
            u64::try_from(memory_before.saturating_sub(memory_after)).unwrap_or(u64::MAX);
        previous.saturating_sub(returned)
    }
}

pub struct CursorEntry {
    cursor_id: [u8; 16],
    connection_id: [u8; 16],
    opened_at_ms: u64,
    last_activity_at_ms: u64,
    effective_limits: ReadLimits,
    retained_memory_bytes: u64,
    cursor: Option<BoundedCursorHandle>,
    lease: RequestLease,
}

impl CursorEntry {
    #[allow(clippy::too_many_arguments)]
    fn from_bounded_open(
        cursor_id: [u8; 16],
        connection_id: [u8; 16],
        opened: BoundedCursorOpen,
        effective_limits: ReadLimits,
        retained_memory_bytes: u64,
        lease: RequestLease,
        clock: &dyn DeadlineClock,
    ) -> Self {
        let now_ms = clock.now_ms();
        Self {
            cursor_id,
            connection_id,
            opened_at_ms: now_ms,
            last_activity_at_ms: now_ms,
            effective_limits,
            retained_memory_bytes,
            cursor: Some(opened.cursor),
            lease,
        }
    }

    fn fetch(
        &mut self,
        rows: Option<NonZeroUsize>,
        cancellation: OwnerReadCancellation,
        clock: &dyn DeadlineClock,
        #[cfg(feature = "test-seams")] probe: Option<Arc<dyn BoundedExecutionProbe>>,
    ) -> std::result::Result<CursorPage, BoundedExecutionError> {
        let outcome = fetch_bounded_cursor(
            self.cursor
                .as_mut()
                .expect("a live owner cursor retains its bounded handle"),
            rows,
            cancellation,
            #[cfg(feature = "test-seams")]
            probe,
        )?;
        self.last_activity_at_ms = clock.now_ms();
        Ok(outcome.page)
    }

    fn close(mut self) -> OwnerReadScaffoldResult<()> {
        let mut cursor = self
            .cursor
            .take()
            .expect("a live owner cursor retains its bounded handle");
        close_bounded_cursor(&mut cursor).map_err(map_bounded_error)
    }

    fn expiry_at(&self, now_ms: u64) -> Option<CursorExpiryKind> {
        if now_ms.saturating_sub(self.opened_at_ms) > self.effective_limits.cursor_lifetime_ms {
            return Some(CursorExpiryKind::Lifetime);
        }
        if now_ms.saturating_sub(self.last_activity_at_ms) > self.effective_limits.cursor_idle_ms {
            return Some(CursorExpiryKind::Idle);
        }
        None
    }

    /// Exhaustion is also a production terminal transition. If publication of
    /// a non-terminal page fails, explicitly close the still-live handle.
    fn discard_unpublished_page(self, has_more: bool) -> OwnerReadScaffoldResult<()> {
        if has_more {
            self.close()
        } else {
            drop(self);
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServiceState {
    Serving,
    Draining,
}

enum PreparedOwnerMetadata {
    CanonicalPage(MetadataPage),
    CanonicalComplete(Vec<u8>),
}

/// Resource counters used by deterministic leak assertions. A live cursor is
/// the sole documented non-baseline state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnerReadResourceSnapshot {
    pub active_slots: u64,
    pub cursor_count: u64,
    pub cursor_retained_bytes: u64,
    pub buffered_bytes: u64,
    pub active_cancellations: u64,
    pub accountant_used_bytes: u64,
}

/// Owner-side administrative read service. It owns only owner-read resources;
/// database lifecycle remains outside this module.
pub struct OwnerReadService {
    spec: OwnerServiceSpec,
    #[cfg(unix)]
    channel: std::os::unix::net::UnixListener,
    /// The filesystem identity of the socket THIS service bound. Taking the
    /// channel down at shutdown is bound to it, so a socket another process
    /// has since bound at the same pathname is never removed with ours.
    #[cfg(unix)]
    bound_channel_identity: Option<crate::local_transport::ChannelFilesystemIdentity>,
    admission: Arc<OwnerAdmission>,
    state: Mutex<ServiceState>,
    cursors: Mutex<HashMap<[u8; 16], CursorEntry>>,
    cursor_count: AtomicU64,
    cursor_retained_bytes: AtomicU64,
    exhausted_cursors: Mutex<BTreeSet<([u8; 16], [u8; 16])>>,
    disconnected_connections: Mutex<BTreeSet<[u8; 16]>>,
    /// Connections whose in-flight request the reader itself interrupted.
    /// A request that ends this way ended because its caller said so, and
    /// answers with the engine's own cancelled read rather than a deadline
    /// the owner never crossed.
    reader_cancelled_connections: Mutex<BTreeSet<[u8; 16]>>,
    next_connection_sequence: AtomicU64,
    buffered_bytes: AtomicU64,
    cancellations: Mutex<BTreeMap<[u8; 16], OwnerReadCancellation>>,
    cancellation_signals: AtomicU64,
    /// The narrowed handle each connection that DECLARED a visibility reads
    /// through, for the whole life of that connection.
    ///
    /// It is made once, from the writer's own handle: contexts and scope
    /// labels by intersection -- so a reader asking for a broader set than the
    /// writer holds is answered the writer's set -- and the declared identity
    /// by adoption, which only a writer opened as no identity or as the same
    /// one ever reaches, because a writer opened as a different one refuses
    /// the session instead of entering it here. So no reader can ever be
    /// served a row the writer serving it may not see, and none is served the
    /// writer's wider identity in place of the one it declared. A connection
    /// that declared nothing has no entry and reads through the writer's
    /// handle exactly as it always has.
    declared_connections: Mutex<HashMap<[u8; 16], Arc<Database>>>,
    /// How many request frames this service has taken off a connection,
    /// counted the moment one is read and before anything is decided about
    /// it. It answers "did this ever reach the owner at all" -- a question
    /// nothing else here can answer, because a request the owner refuses and
    /// a request that was never sent leave the same trace in every other
    /// meter. Every accepted frame counts, an owner-status probe and an
    /// interrupt included.
    #[cfg(feature = "test-seams")]
    received_requests: Arc<AtomicU64>,
    /// What this owner's kernel has finished inspecting for work asked for
    /// over its channel. A reader on the owner route asks in its own thread
    /// and is answered in the owner's, so the counter the reader installed on
    /// its own thread never sees these items; this one is published for the
    /// channel and read by any reader of it in this process.
    #[cfg(feature = "test-seams")]
    channel_source_items: Arc<AtomicU64>,
}

impl OwnerReadService {
    /// Start remains RED after every shared validator and carrier seam is
    /// reached. No database lifecycle method wires this scaffold yet.
    pub fn start(spec: OwnerServiceSpec) -> OwnerReadScaffoldResult<Arc<Self>> {
        #[cfg(feature = "test-seams")]
        crate::read_probe::note_owner_service_start();
        spec.validate()?;
        let admission = OwnerAdmission::new(spec.config.limits)?;
        #[cfg(unix)]
        let channel = spec.listener.bind()?;
        #[cfg(unix)]
        let bound_channel_identity =
            crate::local_transport::channel_filesystem_identity(spec.listener.path()).ok();
        let service = Arc::new(Self {
            spec,
            #[cfg(unix)]
            channel,
            #[cfg(unix)]
            bound_channel_identity,
            admission,
            state: Mutex::new(ServiceState::Serving),
            cursors: Mutex::new(HashMap::new()),
            cursor_count: AtomicU64::new(0),
            cursor_retained_bytes: AtomicU64::new(0),
            exhausted_cursors: Mutex::new(BTreeSet::new()),
            disconnected_connections: Mutex::new(BTreeSet::new()),
            reader_cancelled_connections: Mutex::new(BTreeSet::new()),
            next_connection_sequence: AtomicU64::new(0),
            buffered_bytes: AtomicU64::new(0),
            cancellations: Mutex::new(BTreeMap::new()),
            cancellation_signals: AtomicU64::new(0),
            declared_connections: Mutex::new(HashMap::new()),
            #[cfg(feature = "test-seams")]
            received_requests: Arc::new(AtomicU64::new(0)),
            #[cfg(feature = "test-seams")]
            channel_source_items: Arc::new(AtomicU64::new(0)),
        });
        #[cfg(feature = "test-seams")]
        if let Some(address) = service.spec.database.store_channel_address_for_test() {
            crate::read_probe::publish_owner_route_source_counter(
                address,
                Arc::clone(&service.channel_source_items),
            );
        }
        service.start_accept_loop()?;
        Ok(service)
    }

    /// A started service cannot return while its socket is dormant. Accepting
    /// runs on its own thread so `start` returns to the opening writer, and
    /// every accepted connection gets its own worker: one blocked reader may
    /// never stall another reader's request.
    fn start_accept_loop(self: &Arc<Self>) -> OwnerReadScaffoldResult<()> {
        #[cfg(unix)]
        {
            let listener = self.channel.try_clone().map_err(|_| {
                OwnerReadScaffoldError::unimplemented("owner listener clone for its accept worker")
            })?;
            let service = Arc::clone(self);
            std::thread::Builder::new()
                .name("contextdb-owner-accept".to_owned())
                .spawn(move || {
                    for accepted in listener.incoming() {
                        let Ok(mut stream) = accepted else {
                            break;
                        };
                        let connection_id = service.next_connection_id();
                        let connection_service = Arc::clone(&service);
                        let spawned = std::thread::Builder::new()
                            .name("contextdb-owner-connection".to_owned())
                            .spawn(move || {
                                let _served = wait_for_local_completion(
                                    connection_service
                                        .serve_accepted_stream(&mut stream, connection_id),
                                );
                                let _released = connection_service.disconnect(connection_id);
                            });
                        if spawned.is_err() {
                            break;
                        }
                    }
                })
                .map_err(|_| {
                    OwnerReadScaffoldError::unimplemented("owner listener accept worker thread")
                })?;
        }
        Ok(())
    }

    /// Take the visibility one authenticated reader declared for its whole
    /// session and turn it into the handle that connection will read through,
    /// or refuse the session outright when the declaration is one this writer
    /// cannot serve.
    ///
    /// Contexts and scope labels are sets, so a declaration over them is an
    /// INTERSECTION with what this writer may itself see and can only take
    /// rows away: a reader that asks for every context and every scope is
    /// answered the writer's own, and a reader that declares nothing is not
    /// narrowed at all and keeps reading through the writer's handle.
    ///
    /// Identities are not sets, so the principal axis has its own rule. A
    /// writer opened as no principal HONORS the declared one; a writer opened
    /// as the SAME one serves it unchanged; a writer opened as a DIFFERENT one
    /// REFUSES the session here, because keeping the writer's identity would
    /// answer the reader every row that identity's grants open up -- strictly
    /// more than the reader declared -- and there is no intersection of two
    /// identities to serve instead.
    ///
    /// It is decided once, here, at connection time -- the visibility belongs
    /// to the session, so no later request can move it.
    fn declare_connection_visibility(
        &self,
        connection_id: [u8; 16],
        declared: Option<crate::local_transport::LocalReadDeclaration>,
    ) -> Result<(), contextdb_core::read_contract::ReadFailure> {
        let Some(declared) = declared else {
            return Ok(());
        };
        if declared.contexts.is_none()
            && declared.scope_labels.is_none()
            && declared.principal.is_none()
        {
            return Ok(());
        }
        let narrowed = self
            .spec
            .database
            .scoped_for_read_declaration(
                declared.contexts,
                declared.scope_labels,
                declared.principal,
            )
            .map_err(|refusal| {
                contextdb_core::read_contract::ReadFailure::declared_principal_refused(
                    refusal.stated(),
                )
            })?;
        self.declared_connections
            .lock()
            .expect("owner declared-connection registry")
            .insert(connection_id, Arc::new(narrowed));
        Ok(())
    }

    /// The handle one connection's ROWS come from: its own narrowed handle
    /// when it declared a visibility, and the writer's own when it did not.
    fn reading_handle(&self, connection_id: [u8; 16]) -> Arc<Database> {
        self.declared_connections
            .lock()
            .expect("owner declared-connection registry")
            .get(&connection_id)
            .map_or_else(|| Arc::clone(&self.spec.database), Arc::clone)
    }

    /// Connection identity is minted from the writer's own run number, so two
    /// runs of the same database never hand out the same connection name.
    fn next_connection_id(&self) -> [u8; 16] {
        let sequence = self.next_connection_sequence.fetch_add(1, Ordering::SeqCst);
        let mut identity = self.spec.expected_handshake.writer_run.0;
        for (target, source) in identity[..8].iter_mut().zip(sequence.to_be_bytes()) {
            *target ^= source;
        }
        identity
    }

    /// Shape one accepted connection through OS-principal authentication and
    /// the shared request deadline before any admission or dispatch. Reading
    /// continues on its own worker for the whole life of the connection, so a
    /// reader that goes away mid-statement, or interrupts one, is observed
    /// while the owner is still inside that statement.
    #[cfg(unix)]
    async fn serve_accepted_stream(
        self: &Arc<Self>,
        stream: &mut std::os::unix::net::UnixStream,
        connection_id: [u8; 16],
    ) -> OwnerReadScaffoldResult<()> {
        let owner_limits = self.spec.config.limits.limits;
        let carrier = UnixLocalCarrier;
        let handshake_deadline_ms = self.request_deadline_ms()?;
        let handshake = authenticate_framed_stream_handshake(
            stream,
            &self.spec.expected_handshake,
            self.spec.clock.as_ref(),
            handshake_deadline_ms,
        )
        .await;
        let principal = match handshake {
            Ok(admitted) => {
                // A declaration this writer will not serve ends THIS session
                // and nothing else: the reader is told, in a refusal it can
                // read, that the identity it declared is the reason, and the
                // owner goes on answering every other reader.
                if let Err(refusal) =
                    self.declare_connection_visibility(connection_id, admitted.declared)
                {
                    let boundary = LocalProtocolBoundary::with_effective_limits(owner_limits);
                    let _answered = UnixLocalCarrier::send_message(
                        &carrier,
                        stream,
                        &boundary,
                        LocalOutboundMessage::Response {
                            response: &LocalResponse::Failure { failure: refusal },
                            expectation: &LocalResponseExpectation::Custom,
                        },
                    );
                    return Ok(());
                }
                admitted.principal
            }
            Err(error) => {
                // A refused peer still gets told which of its recorded owner
                // facts did not match; silence would read to the caller as an
                // owner that went away.
                let response = refusal_response(OwnerReadScaffoldError::from_local(error));
                let boundary = LocalProtocolBoundary::with_effective_limits(owner_limits);
                let _answered = UnixLocalCarrier::send_message(
                    &carrier,
                    stream,
                    &boundary,
                    LocalOutboundMessage::Response {
                        response: &response,
                        expectation: &LocalResponseExpectation::Custom,
                    },
                );
                return Ok(());
            }
        };

        let inbox = Arc::new(ConnectionInbox::default());
        let reader_stream = stream.try_clone().map_err(|_| {
            OwnerReadScaffoldError::unimplemented("owner connection reader carrier clone")
        })?;
        let reader_inbox = Arc::clone(&inbox);
        #[cfg(feature = "test-seams")]
        let reader_received = Arc::clone(&self.received_requests);
        std::thread::Builder::new()
            .name("contextdb-owner-reader".to_owned())
            .spawn(move || {
                let mut reader_stream = reader_stream;
                let boundary = LocalProtocolBoundary::with_effective_limits(owner_limits);
                let carrier = UnixLocalCarrier;
                loop {
                    let inbound = UnixLocalCarrier::receive_message(
                        &carrier,
                        &mut reader_stream,
                        &boundary,
                        LocalInboundKind::Request,
                    );
                    match inbound {
                        Ok(LocalInboundMessage::Request(envelope)) => {
                            #[cfg(feature = "test-seams")]
                            let _received = reader_received.fetch_add(1, Ordering::SeqCst);
                            reader_inbox.push(ConnectionEvent::Request(envelope));
                        }
                        _ => {
                            reader_inbox.push(ConnectionEvent::Ended);
                            break;
                        }
                    }
                }
            })
            .map_err(|_| {
                OwnerReadScaffoldError::unimplemented("owner connection reader worker thread")
            })?;

        let mut request_ordinal = 0_u64;
        loop {
            match inbox.blocking_next() {
                ConnectionEvent::Ended => {
                    self.disconnect(connection_id)?;
                    return Ok(());
                }
                ConnectionEvent::Request(envelope) => {
                    if let LocalRequest::CancelInFlight {
                        request_ordinal: named,
                    } = envelope.request
                    {
                        // Nothing is in flight on this connection, so the
                        // named request is already over. It is not counted:
                        // an interrupt that arrives too late must not shift
                        // the number the NEXT request is known by, or every
                        // later interrupt would name the wrong read.
                        //
                        // It is still ANSWERED. The caller that cancelled is
                        // waiting to hear that the owning process has stopped
                        // that statement, and a statement already over is
                        // stopped -- leaving this one silent would hold that
                        // caller for its whole patience for no reason, and
                        // which loop happens to take the frame off the inbox
                        // is not something a caller can see or influence.
                        let boundary = LocalProtocolBoundary::with_effective_limits(owner_limits);
                        let carrier = UnixLocalCarrier;
                        let _answered = UnixLocalCarrier::send_message(
                            &carrier,
                            stream,
                            &boundary,
                            LocalOutboundMessage::Response {
                                response: &LocalResponse::CancelApplied {
                                    request_ordinal: named,
                                },
                                expectation: &LocalResponseExpectation::Custom,
                            },
                        );
                        continue;
                    }
                    request_ordinal = request_ordinal.saturating_add(1);
                    let served = self
                        .serve_one_request(
                            stream,
                            connection_id,
                            request_ordinal,
                            principal,
                            envelope,
                            &inbox,
                        )
                        .await;
                    if !matches!(served, Ok(ServedRequest::Continue)) {
                        self.disconnect(connection_id)?;
                        return Ok(());
                    }
                }
            }
        }
    }

    fn request_deadline_ms(&self) -> OwnerReadScaffoldResult<u64> {
        self.spec
            .clock
            .now_ms()
            .checked_add(self.spec.config.timeouts.request_ms)
            .ok_or_else(|| {
                OwnerReadScaffoldError::unimplemented("validated owner request deadline arithmetic")
            })
    }

    /// One request, from admission through publication, bounded by the
    /// configured request deadline. The work itself runs on its own worker so
    /// a handler that blocks cannot also hold the deadline, the interrupt
    /// frame, or end-of-file away from this connection.
    #[cfg(unix)]
    async fn serve_one_request(
        self: &Arc<Self>,
        stream: &mut std::os::unix::net::UnixStream,
        connection_id: [u8; 16],
        request_ordinal: u64,
        principal: ReadPrincipal,
        envelope: LocalRequestEnvelope,
        inbox: &Arc<ConnectionInbox>,
    ) -> OwnerReadScaffoldResult<ServedRequest> {
        let carrier = UnixLocalCarrier;
        let expectation = response_expectation(&envelope.request);
        let effective_limits =
            OwnerAdmission::effective_limits(envelope.limits, self.spec.config.limits)
                .unwrap_or(envelope.limits);
        let boundary = LocalProtocolBoundary::with_effective_limits(effective_limits);
        let deadline_ms = self.request_deadline_ms()?;

        let slot = Arc::new(RequestSlot::default());
        let worker_slot = Arc::clone(&slot);
        let worker_service = Arc::clone(self);
        let progress = Arc::new(ProgressRelay::default());
        let worker_progress: Arc<dyn crate::read_progress::ReadProgressObserver> =
            Arc::clone(&progress) as Arc<dyn crate::read_progress::ReadProgressObserver>;
        let request = InboundOwnerRequest {
            connection_id,
            principal,
            envelope,
        };
        std::thread::Builder::new()
            .name("contextdb-owner-request".to_owned())
            .spawn(move || {
                // The read runs to completion on this thread, so this is
                // where the reader's interest in it is put in force -- the
                // same installation an in-process caller makes, around the
                // same work.
                // A read that dies mid-flight still owes its caller an
                // answer. The connection waits for this slot and for nothing
                // else, so a worker that unwinds without filling it leaves the
                // reader waiting on a reply that will never be written and the
                // connection waiting on a wake that will never come -- one
                // panic anywhere inside a read, in a trigger callback, a
                // plugin, or an observer, and that connection is hung until
                // its deadline. The unwind is caught here and answered as the
                // engine failure it is.
                let responses = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    crate::read_progress::with_progress_observer(Some(&worker_progress), || {
                        // The items this read finishes are charged to the
                        // channel the reader asked over, because the reader's
                        // own thread is not the thread doing the work.
                        with_channel_source_counter(&worker_service, || {
                            worker_service.dispatch_authenticated_for_carrier(request)
                        })
                    })
                }))
                .unwrap_or_else(|payload| {
                    vec![refusal_response(OwnerReadScaffoldError::Database(
                        contextdb_core::Error::Other(format!(
                            "the owner's read ended unexpectedly: {}",
                            panic_reason(&payload)
                        )),
                    ))]
                });
                worker_slot.complete(responses);
            })
            .map_err(|_| OwnerReadScaffoldError::unimplemented("owner request worker thread"))?;

        let mut publish_stream = stream.try_clone().map_err(|_| {
            OwnerReadScaffoldError::unimplemented("owner connection publication carrier clone")
        })?;
        let ended = AtomicBool::new(false);
        let operation: LocalDeadlineOperation<'_> = Box::pin(async {
            let responses = loop {
                let outcome = std::future::poll_fn(|context| {
                    // A report the read already made is news the caller asked
                    // for, so it goes out even if the answer landed while it
                    // was waiting to be written -- otherwise a read that
                    // finishes quickly tells its caller nothing at all, and
                    // whether a caller hears from a read at all would depend
                    // on how the two threads happened to be scheduled.
                    if let Some(progress) = progress.take_reported(context) {
                        return Poll::Ready(RequestOutcome::Reported(progress));
                    }
                    if let Some(responses) = slot.take_ready(context) {
                        return Poll::Ready(RequestOutcome::Answered(responses));
                    }
                    match inbox.poll_interrupt(context) {
                        // The reader is gone. The work it started stops now,
                        // and the answer it gets is the disconnected refusal
                        // that this request's own worker produces once it has
                        // released everything it held.
                        Poll::Ready(ConnectionInterrupt::Ended) => {
                            ended.store(true, Ordering::SeqCst);
                            let _released = self.disconnect(connection_id);
                            Poll::Pending
                        }
                        Poll::Ready(ConnectionInterrupt::Cancel {
                            request_ordinal: named,
                        }) => {
                            if named == request_ordinal {
                                self.cancel_in_flight(connection_id);
                            }
                            // Applied, not merely received. The caller that
                            // cancelled is waiting to be told the owning
                            // process has really stopped the statement it
                            // named, so the acknowledgement goes out after the
                            // token is cancelled and never before -- and an
                            // interrupt naming a statement that is NOT the one
                            // in flight is answered too, because that
                            // statement is already not running and its caller
                            // would otherwise wait for its whole deadline.
                            Poll::Ready(RequestOutcome::CancelApplied(named))
                        }
                        Poll::Pending => Poll::Pending,
                    }
                })
                .await;
                match outcome {
                    RequestOutcome::Answered(responses) => break responses,
                    RequestOutcome::CancelApplied(request_ordinal) => {
                        UnixLocalCarrier::send_message(
                            &carrier,
                            &mut publish_stream,
                            &boundary,
                            LocalOutboundMessage::Response {
                                response: &LocalResponse::CancelApplied { request_ordinal },
                                expectation: &expectation,
                            },
                        )?;
                    }
                    // A nonterminal frame about the request in flight. The
                    // result itself is still withheld: only the terminal
                    // frames below carry any of the answer.
                    RequestOutcome::Reported(progress) => {
                        UnixLocalCarrier::send_message(
                            &carrier,
                            &mut publish_stream,
                            &boundary,
                            LocalOutboundMessage::Response {
                                response: &LocalResponse::Progress { progress },
                                expectation: &expectation,
                            },
                        )?;
                    }
                }
            };
            // An answer larger than one local frame goes out as leading
            // pieces followed by the response that ends the exchange. The
            // boundary is told so before the first piece leaves: without that,
            // a piece offered for a cursor or metadata exchange is a crossed
            // pairing and stays refused, which is what keeps this apart from a
            // response variant wandering into the wrong operation.
            if responses.len() > 1 {
                boundary.answer_spans_frames();
            }
            for response in &responses {
                UnixLocalCarrier::send_message(
                    &carrier,
                    &mut publish_stream,
                    &boundary,
                    LocalOutboundMessage::Response {
                        response,
                        expectation: &expectation,
                    },
                )?;
            }
            Ok(())
        });
        let served = serve_request_with_deadline(self.spec.clock.as_ref(), deadline_ms, operation)
            .await
            .map_err(OwnerReadScaffoldError::from_local);
        if ended.load(Ordering::SeqCst) {
            return Ok(ServedRequest::ConnectionEnded);
        }
        match served {
            Ok(()) => Ok(ServedRequest::Continue),
            Err(error) => {
                let timed_out = matches!(
                    &error,
                    OwnerReadScaffoldError::Refused(failure)
                        if failure.kind() == ReadFailureKind::OwnerTimeout
                );
                if !timed_out {
                    return Err(error);
                }
                let refusal = self.signal_request_timeout(connection_id);
                let response = refusal_response(refusal);
                let boundary = LocalProtocolBoundary::with_effective_limits(effective_limits);
                UnixLocalCarrier::send_message(
                    &carrier,
                    stream,
                    &boundary,
                    LocalOutboundMessage::Response {
                        response: &response,
                        expectation: &expectation,
                    },
                )?;
                // The work still holds its slot until it returns. Waiting here
                // keeps one request in flight per connection, so a later
                // request cannot overtake the one that was abandoned.
                let _abandoned = slot.blocking_take();
                Ok(ServedRequest::Continue)
            }
        }
    }

    #[cfg(unix)]
    fn dispatch_authenticated_for_carrier(
        &self,
        request: InboundOwnerRequest,
    ) -> Vec<LocalResponse> {
        match self.handle_authenticated(request) {
            Ok(responses) => responses,
            Err(error) => vec![refusal_response(error)],
        }
    }

    pub fn status(&self) -> OwnerReadStatus {
        match *self.state.lock().expect("owner service state") {
            ServiceState::Serving => self.spec.status.clone(),
            ServiceState::Draining => OwnerReadStatus {
                state: OwnerServingState::NotServing,
                reason: Some(OwnerServingReason::ShutdownDraining),
            },
        }
    }

    pub fn resources(&self) -> OwnerReadResourceSnapshot {
        let usage = self.spec.database.accountant().usage();
        OwnerReadResourceSnapshot {
            active_slots: self.admission.counters().active_readers,
            cursor_count: self.cursor_count.load(Ordering::SeqCst),
            cursor_retained_bytes: self.cursor_retained_bytes.load(Ordering::SeqCst),
            buffered_bytes: self.buffered_bytes.load(Ordering::SeqCst),
            active_cancellations: u64::try_from(
                self.cancellations
                    .lock()
                    .expect("owner cancellation registry")
                    .len(),
            )
            .unwrap_or(u64::MAX),
            accountant_used_bytes: u64::try_from(usage.used).unwrap_or(u64::MAX),
        }
    }

    pub fn cancellation_signal_count(&self) -> u64 {
        self.cancellation_signals.load(Ordering::SeqCst)
    }

    /// How many request frames have arrived at this owner since it started.
    #[cfg(feature = "test-seams")]
    pub fn received_request_count(&self) -> u64 {
        self.received_requests.load(Ordering::SeqCst)
    }

    pub fn channel_path(&self) -> &std::path::Path {
        self.spec.listener.path()
    }

    #[cfg(all(unix, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn channel_is_bound_for_test(&self) -> bool {
        self.channel.local_addr().is_ok()
    }

    /// The journey is fixed here: authenticate peer and every handshake field;
    /// calculate stricter limits; let status bypass slots; immediately admit
    /// all other work; classify SQL before touching the database.
    fn handle_authenticated(
        &self,
        request: InboundOwnerRequest,
    ) -> OwnerReadScaffoldResult<Vec<LocalResponse>> {
        let local_request = request.envelope.request;
        let connection = ConnectionState::authenticated(
            request.connection_id,
            request.principal,
            request.envelope.limits,
            self.spec.config.limits,
        )?;

        if matches!(&local_request, LocalRequest::OwnerStatus) {
            return Ok(vec![LocalResponse::OwnerStatus {
                status: self.status_response()?,
            }]);
        }
        if matches!(&local_request, LocalRequest::CancelInFlight { .. }) {
            self.cancel_in_flight(connection.connection_id());
            return Ok(Vec::new());
        }
        // Give an already-draining owner the typed refusal before asking for a
        // slot. OwnerAdmission repeats the accepting check across its CAS so a
        // close racing after this service-state read cannot retain one more
        // admitted request.
        let draining = matches!(
            *self.state.lock().expect("owner service state"),
            ServiceState::Draining
        );
        if draining || !self.admission.is_accepting() {
            return Err(OwnerReadScaffoldError::Refused(simple_failure(
                ReadFailureKind::OwnerNotServing,
            )));
        }

        match local_request {
            LocalRequest::CursorFetch { cursor_id, rows } => {
                self.fetch_cursor(&connection, cursor_id, rows)
            }
            LocalRequest::CursorClose { cursor_id } => self.close_cursor(&connection, cursor_id),
            request => {
                let lease = self.admission.try_acquire(
                    connection.effective_limits(),
                    connection.cancellation().clone(),
                )?;
                self.register_active_request(&connection)?;
                let connection_id = connection.connection_id();
                // One routine ends the request however it ends. A request that
                // unwinds leaves the connection as free as one that answers --
                // otherwise the first panic makes every later statement on that
                // session refuse with "one in-flight request per authenticated
                // local connection", and the caller's session is finished even
                // though the caller did nothing wrong.
                let _active = ActiveRequest {
                    service: self,
                    connection_id,
                };
                self.handle_admitted(connection, lease, request)
            }
        }
    }

    fn register_active_request(&self, connection: &ConnectionState) -> OwnerReadScaffoldResult<()> {
        self.register_active_cancellation(
            connection.connection_id(),
            connection.cancellation().clone(),
        )
    }

    fn register_active_cancellation(
        &self,
        connection_id: [u8; 16],
        cancellation: OwnerReadCancellation,
    ) -> OwnerReadScaffoldResult<()> {
        let mut active = self
            .cancellations
            .lock()
            .expect("owner cancellation registry");
        if active.contains_key(&connection_id) {
            return Err(OwnerReadScaffoldError::unimplemented(
                "one in-flight request per authenticated local connection",
            ));
        }
        let cancellation_after_registration = cancellation.clone();
        let previous = active.insert(connection_id, cancellation);
        debug_assert!(previous.is_none());
        // A request may reserve its slot immediately before shutdown closes
        // admission, then reach this registry after shutdown took its first
        // cancellation snapshot. Register first while holding the same registry
        // lock shutdown uses, then re-read the admission fence. Either shutdown
        // sees this entry, or this side observes the closed fence and cancels it;
        // there is no uncancelled gap between the two.
        if !self.admission.is_accepting() && !cancellation_after_registration.is_cancelled() {
            cancellation_after_registration.cancel();
        }
        Ok(())
    }

    /// Holds a connection's in-flight registration for exactly as long as its
    /// request runs, and clears it however that request ends.
    fn finish_active_request(&self, connection_id: [u8; 16]) {
        let _ = self
            .cancellations
            .lock()
            .expect("owner cancellation registry")
            .remove(&connection_id);
        let _ = self
            .reader_cancelled_connections
            .lock()
            .expect("owner reader-interrupt registry")
            .remove(&connection_id);
        #[cfg(feature = "test-seams")]
        if let Some(observer) = &self.spec.execution_observer {
            observer.request_finished();
        }
    }

    /// Move the fleet total when ONE cursor's retained charge changes.
    ///
    /// A cursor's cost is not fixed at open: a page that defers a row leaves
    /// that row charged, and the next page that publishes it gives the bytes
    /// back. Owner status is only true if the number it reports moves with the
    /// cursor, so the entry and the aggregate are updated together, at the one
    /// moment the cursor is handed back to the registry.
    fn adjust_cursor_retained(&self, previous: u64, current: u64) {
        match current.cmp(&previous) {
            std::cmp::Ordering::Greater => {
                let _ = self
                    .cursor_retained_bytes
                    .fetch_add(current.saturating_sub(previous), Ordering::SeqCst);
            }
            std::cmp::Ordering::Less => {
                let returned = previous.saturating_sub(current);
                let released = self.cursor_retained_bytes.fetch_update(
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    |bytes| bytes.checked_sub(returned),
                );
                debug_assert!(
                    released.is_ok(),
                    "owner cursor retained-memory count cannot underflow",
                );
            }
            std::cmp::Ordering::Equal => {}
        }
    }

    fn release_cursor_resources(&self, retained_memory_bytes: u64) {
        let released =
            self.cursor_count
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                    count.checked_sub(1)
                });
        debug_assert!(released.is_ok(), "owner cursor count cannot underflow");
        let released_memory =
            self.cursor_retained_bytes
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |bytes| {
                    bytes.checked_sub(retained_memory_bytes)
                });
        debug_assert!(
            released_memory.is_ok(),
            "owner cursor retained-memory count cannot underflow",
        );
    }

    fn status_response(&self) -> OwnerReadScaffoldResult<LocalOwnerStatusResponse> {
        let usage = self.spec.database.accountant().usage();
        Ok(LocalOwnerStatusResponse {
            status: self.status(),
            effective_limits: LocalEffectiveLimits::from_owner_limits(
                self.spec.config.limits,
                self.spec.configuration_source,
            )?,
            timeouts: LocalOwnerTimeouts::from_owner_timeouts(
                self.spec.config.timeouts,
                self.spec.configuration_source,
            )?,
            admission: self.admission.counters(),
            memory: OwnerMemoryCounters {
                used_bytes: u64::try_from(usage.used).unwrap_or(u64::MAX),
                available_bytes: usage
                    .available
                    .map(|bytes| u64::try_from(bytes).unwrap_or(u64::MAX)),
            },
        })
    }

    fn handle_admitted(
        &self,
        connection: ConnectionState,
        lease: RequestLease,
        request: LocalRequest,
    ) -> OwnerReadScaffoldResult<Vec<LocalResponse>> {
        // ROWS come from the handle this connection declared itself into, so
        // every statement it runs -- of every shape -- and every page of every
        // cursor it opens meets the same row gate. Nothing here inspects the
        // statement.
        let reading = self.reading_handle(connection.connection_id());
        match request {
            LocalRequest::Query { statement, params } => {
                require_read_statement(&statement)?;
                let params: HashMap<_, _> = params.into_iter().collect();
                let outcome = execute_bounded(
                    &reading,
                    &statement,
                    &params,
                    connection.effective_limits(),
                    Arc::clone(&self.spec.clock),
                    connection.cancellation().clone(),
                    #[cfg(feature = "test-seams")]
                    self.spec.bounded_probe(
                        OwnerBoundedOperation::Execute,
                        connection.effective_limits(),
                        connection.cancellation().clone(),
                    ),
                )
                .map_err(|error| {
                    self.map_request_bounded_error(connection.connection_id(), error)
                })?;
                let canonical = encode_query_result(&outcome.result).map_err(|_| {
                    OwnerReadScaffoldError::unimplemented("canonical ordinary-result encoding")
                })?;
                let responses = split_canonical_result(&canonical)?;
                drop(lease);
                Ok(responses)
            }
            LocalRequest::CursorOpen { statement, params } => {
                require_read_statement(&statement)?;
                let cursor_id = self
                    .spec
                    .cursor_identifiers
                    .allocate(self.spec.expected_handshake.writer_run.0)?;
                let params: HashMap<_, _> = params.into_iter().collect();
                let memory_before = self.spec.database.accountant().usage().used;
                let mut opened = open_bounded_cursor(
                    Arc::clone(&reading),
                    &statement,
                    &params,
                    connection.effective_limits(),
                    Arc::clone(&self.spec.clock),
                    connection.cancellation().clone(),
                    #[cfg(feature = "test-seams")]
                    self.spec.bounded_probe(
                        OwnerBoundedOperation::CursorOpen,
                        connection.effective_limits(),
                        connection.cancellation().clone(),
                    ),
                )
                .map_err(|error| {
                    self.map_request_bounded_error(connection.connection_id(), error)
                })?;
                let memory_after = self.spec.database.accountant().usage().used;
                let retained_memory_bytes =
                    u64::try_from(memory_after.saturating_sub(memory_before)).unwrap_or(u64::MAX);
                let payload = match encode_cursor_page(&opened.first_page) {
                    Ok(payload) => payload,
                    Err(_) => {
                        close_unpublished_open(&mut opened)?;
                        return Err(OwnerReadScaffoldError::unimplemented(
                            "canonical cursor-page encoding",
                        ));
                    }
                };
                // A page inside the budget this session declared is served,
                // whatever it costs in frames: the frame ceiling is how the
                // channel moves bytes, never a ceiling the caller declared.
                let responses =
                    match split_payload_answer(payload, |payload| LocalResponse::CursorOpened {
                        opened: CursorOpenedResponse { cursor_id, payload },
                    }) {
                        Ok(responses) => responses,
                        Err(error) => {
                            close_unpublished_open(&mut opened)?;
                            return Err(OwnerReadScaffoldError::from_local(error));
                        }
                    };
                for response in &responses {
                    if let Err(error) = response_is_encodable(response) {
                        close_unpublished_open(&mut opened)?;
                        return Err(OwnerReadScaffoldError::from_local(error));
                    }
                }

                let has_more = opened.first_page.has_more;
                if has_more && retained_memory_bytes == 0 {
                    close_unpublished_open(&mut opened)?;
                    return Err(OwnerReadScaffoldError::unimplemented(
                        "strict bounded-cursor retained-memory accountant charge",
                    ));
                }
                self.retire_prior_cursor_identity(cursor_id);
                let entry = CursorEntry::from_bounded_open(
                    cursor_id,
                    connection.connection_id(),
                    opened,
                    connection.effective_limits(),
                    retained_memory_bytes,
                    lease,
                    self.spec.clock.as_ref(),
                );
                if has_more {
                    let retained_memory_bytes = entry.retained_memory_bytes;
                    self.store_cursor(entry)?;
                    let _ = self.cursor_count.fetch_add(1, Ordering::SeqCst);
                    let _ = self
                        .cursor_retained_bytes
                        .fetch_add(retained_memory_bytes, Ordering::SeqCst);
                } else {
                    let _ = self
                        .exhausted_cursors
                        .lock()
                        .expect("owner exhausted cursor registry")
                        .insert((connection.connection_id(), cursor_id));
                    drop(entry);
                }
                Ok(responses)
            }
            LocalRequest::CursorFetch { .. } | LocalRequest::CursorClose { .. } => {
                let _ = (connection, lease);
                unreachable!("cursor fetch/close bypass new slot admission")
            }
            LocalRequest::Metadata { request } => self.metadata_response(request, lease),
            LocalRequest::Explain { statement, params } => {
                // A write is PLANNED here, never run, so explaining one is
                // not a write and is not refused as one. Only running it
                // would be.
                if !is_read_statement(&statement)? {
                    let plan = self
                        .spec
                        .database
                        .explain(&statement)
                        .map_err(OwnerReadScaffoldError::Database)?;
                    let payload = crate::read_contract::encode_metadata_body(
                        &crate::direct_file_reader::DirectMetadataBody::Explain {
                            sql: statement,
                            physical_plan: plan,
                            index: None,
                        },
                    )
                    .map_err(|_| {
                        OwnerReadScaffoldError::unimplemented("canonical explain encoding")
                    })?;
                    self.admit_metadata_payload(&payload, lease.effective_limits())?;
                    let responses =
                        split_payload_answer(payload, |payload| LocalResponse::Explain { payload })
                            .map_err(OwnerReadScaffoldError::from_local)?;
                    for response in &responses {
                        response_is_encodable(response)
                            .map_err(OwnerReadScaffoldError::from_local)?;
                    }
                    drop(lease);
                    return Ok(responses);
                }
                let params: HashMap<_, _> = params.into_iter().collect();
                let outcome = execute_bounded(
                    &reading,
                    &statement,
                    &params,
                    connection.effective_limits(),
                    Arc::clone(&self.spec.clock),
                    connection.cancellation().clone(),
                    #[cfg(feature = "test-seams")]
                    self.spec.bounded_probe(
                        OwnerBoundedOperation::Execute,
                        connection.effective_limits(),
                        connection.cancellation().clone(),
                    ),
                )
                .map_err(|error| {
                    self.map_request_bounded_error(connection.connection_id(), error)
                })?;
                // Both routes answer this question through the same canonical
                // writer, so a plan read over the channel is the plan a direct
                // reader would have printed.
                let payload = crate::read_contract::encode_metadata_body(
                    &crate::direct_file_reader::DirectMetadataBody::Explain {
                        sql: statement,
                        physical_plan: outcome.result.trace.physical_plan.to_string(),
                        index: outcome.result.trace.index_used.clone(),
                    },
                )
                .map_err(|_| OwnerReadScaffoldError::unimplemented("canonical explain encoding"))?;
                self.admit_metadata_payload(&payload, lease.effective_limits())?;
                let responses =
                    split_payload_answer(payload, |payload| LocalResponse::Explain { payload })
                        .map_err(OwnerReadScaffoldError::from_local)?;
                for response in &responses {
                    response_is_encodable(response).map_err(OwnerReadScaffoldError::from_local)?;
                }
                drop(lease);
                Ok(responses)
            }
            LocalRequest::Custom { namespace, payload } => {
                let handler = self.spec.config.handler.as_ref().ok_or_else(|| {
                    // No custom handler is registered for this owner, so the
                    // requested namespace is an inspection kind this route
                    // does not answer -- the same refusal read_image.rs emits
                    // for image-state metadata, naming what was asked for
                    // rather than saying "not implemented".
                    OwnerReadScaffoldError::Refused(ReadFailure::owner_route_unsupported(
                        namespace.clone(),
                    ))
                })?;
                let response = handler.handle(&namespace, &payload, lease.cancellation())?;
                // An answer is thrown away only when there is nobody left who
                // wants it: the caller withdrew the request, or the caller is
                // gone. An owner that is winding down cancelled this work
                // itself, and the handler answered anyway -- that answer is
                // the caller's, and a caller in the owner's own process is
                // already given it, so a caller on the channel gets the same.
                if lease.cancellation().is_cancelled()
                    && self.caller_withdrew(connection.connection_id())
                {
                    return Err(self.cancellation_failure(connection.connection_id()));
                }
                let envelope = LocalResponse::Custom { payload: response };
                response_is_encodable(&envelope).map_err(OwnerReadScaffoldError::from_local)?;
                drop(lease);
                Ok(vec![envelope])
            }
            LocalRequest::OwnerStatus => unreachable!("status bypasses owner admission"),
            LocalRequest::CancelInFlight { .. } => {
                unreachable!("an interrupt names an in-flight request and takes no slot")
            }
        }
    }

    fn retire_prior_cursor_identity(&self, cursor_id: [u8; 16]) {
        self.exhausted_cursors
            .lock()
            .expect("owner exhausted cursor registry")
            .retain(|(_, exhausted_id)| *exhausted_id != cursor_id);
    }

    fn is_disconnected(&self, connection_id: [u8; 16]) -> bool {
        self.disconnected_connections
            .lock()
            .expect("owner disconnected connection registry")
            .contains(&connection_id)
    }

    /// Whether the caller of this connection has stopped wanting the answer --
    /// it interrupted the request, or it went away. An owner's own shutdown
    /// cancellation is neither.
    fn caller_withdrew(&self, connection_id: [u8; 16]) -> bool {
        if self.is_disconnected(connection_id) {
            return true;
        }
        self.reader_cancelled_connections
            .lock()
            .expect("owner reader-interrupt registry")
            .contains(&connection_id)
    }

    fn cancellation_failure(&self, connection_id: [u8; 16]) -> OwnerReadScaffoldError {
        if self.is_disconnected(connection_id) {
            return OwnerReadScaffoldError::Refused(simple_failure(
                ReadFailureKind::OwnerDisconnected,
            ));
        }
        if self
            .reader_cancelled_connections
            .lock()
            .expect("owner reader-interrupt registry")
            .contains(&connection_id)
        {
            return OwnerReadScaffoldError::Database(contextdb_core::Error::ReadCancelled);
        }
        OwnerReadScaffoldError::Refused(simple_failure(ReadFailureKind::OwnerTimeout))
    }

    fn registered_cancellation(&self, connection_id: [u8; 16]) -> Option<OwnerReadCancellation> {
        self.cancellations
            .lock()
            .expect("owner cancellation registry")
            .get(&connection_id)
            .cloned()
    }

    /// A reader that interrupts the statement it is waiting on reaches the
    /// same token the work already holds, so the engine stops where it is
    /// rather than running to completion for an answer nobody will read.
    fn cancel_in_flight(&self, connection_id: [u8; 16]) {
        if let Some(cancellation) = self.registered_cancellation(connection_id)
            && !cancellation.is_cancelled()
        {
            let _ = self
                .reader_cancelled_connections
                .lock()
                .expect("owner reader-interrupt registry")
                .insert(connection_id);
            cancellation.cancel();
            let _ = self.cancellation_signals.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// The accepted-stream worker calls this once when the shared request
    /// waiter wins. Keeping the signal here pins cancellation, status
    /// accounting, and the typed terminal failure to one production point.
    fn signal_request_timeout(&self, connection_id: [u8; 16]) -> OwnerReadScaffoldError {
        if let Some(cancellation) = self.registered_cancellation(connection_id)
            && !cancellation.is_cancelled()
        {
            cancellation.cancel();
            let _ = self.cancellation_signals.fetch_add(1, Ordering::SeqCst);
            #[cfg(feature = "test-seams")]
            if let Some(observer) = &self.spec.execution_observer {
                observer.request_deadline_cancellation_signalled();
            }
        }
        OwnerReadScaffoldError::Refused(simple_failure(ReadFailureKind::OwnerTimeout))
    }

    fn map_request_bounded_error(
        &self,
        connection_id: [u8; 16],
        error: BoundedExecutionError,
    ) -> OwnerReadScaffoldError {
        match error {
            BoundedExecutionError::Cancelled => self.cancellation_failure(connection_id),
            other => map_bounded_error(other),
        }
    }

    fn store_cursor(&self, entry: CursorEntry) -> OwnerReadScaffoldResult<()> {
        let cursor_id = entry.cursor_id;
        let mut cursors = self.cursors.lock().expect("owner cursor registry");
        if cursors.contains_key(&cursor_id) {
            drop(cursors);
            entry.close()?;
            return Err(OwnerReadScaffoldError::unimplemented(
                "collision-free owner cursor identifier allocation",
            ));
        }
        let previous = cursors.insert(cursor_id, entry);
        debug_assert!(previous.is_none());
        Ok(())
    }

    fn take_cursor(
        &self,
        connection_id: [u8; 16],
        cursor_id: [u8; 16],
    ) -> OwnerReadScaffoldResult<CursorEntry> {
        let mut cursors = self.cursors.lock().expect("owner cursor registry");
        let Some(entry) = cursors.remove(&cursor_id) else {
            return Err(OwnerReadScaffoldError::Refused(simple_failure(
                ReadFailureKind::CursorNotFound,
            )));
        };
        if entry.connection_id != connection_id {
            let previous = cursors.insert(cursor_id, entry);
            debug_assert!(previous.is_none());
            return Err(OwnerReadScaffoldError::Refused(simple_failure(
                ReadFailureKind::CursorNotFound,
            )));
        }
        Ok(entry)
    }

    fn fetch_cursor(
        &self,
        connection: &ConnectionState,
        cursor_id: [u8; 16],
        rows: Option<NonZeroU64>,
    ) -> OwnerReadScaffoldResult<Vec<LocalResponse>> {
        if self
            .exhausted_cursors
            .lock()
            .expect("owner exhausted cursor registry")
            .contains(&(connection.connection_id(), cursor_id))
        {
            return self.empty_exhausted_cursor_page();
        }

        let mut entry = self.take_cursor(connection.connection_id(), cursor_id)?;
        let retained_memory_bytes = entry.retained_memory_bytes;
        if let Some(expiry) = entry.expiry_at(self.spec.clock.now_ms()) {
            let close = entry.close();
            self.release_cursor_resources(retained_memory_bytes);
            close?;
            return Err(OwnerReadScaffoldError::Refused(
                ReadFailure::cursor_expired(expiry),
            ));
        }
        // An explicit count above the effective ceiling is a request-shape
        // validation refusal: it is answered before the cursor is asked for
        // anything, so no row and no continuation position is consumed. The
        // read is therefore exactly where the caller left it, and the entry
        // goes back into the registry untouched -- the same move a cancelled
        // fetch makes below. Closing here instead would desynchronize the
        // client's own cursor slot, which would then answer `cursor_not_found`
        // to a fetch and `cursor_already_open` to a reopen, leaving a session
        // with no cursor and no way to get one.
        if rows.is_some_and(|requested| requested.get() > entry.effective_limits.result_rows) {
            let effective = entry.effective_limits.result_rows;
            if let Err(error) = self.store_cursor(entry) {
                self.release_cursor_resources(retained_memory_bytes);
                return Err(error);
            }
            return Err(OwnerReadScaffoldError::Refused(
                ReadFailure::owner_limit_exceeded(OwnerLimitExceededDetail {
                    limit: ReadFailureLimit::ResultRows,
                    value: effective,
                    required: None,
                    statement: None,
                }),
            ));
        }
        let rows = match rows {
            Some(rows) => {
                let converted = usize::try_from(rows.get()).map_err(|_| {
                    OwnerReadScaffoldError::unimplemented(
                        "checked cursor row-count conversion for this platform",
                    )
                });
                match converted {
                    Ok(rows) => NonZeroUsize::new(rows),
                    Err(error) => {
                        let close = entry.close();
                        self.release_cursor_resources(retained_memory_bytes);
                        close?;
                        return Err(error);
                    }
                }
            }
            None => None,
        };

        let fetch_cancellation = OwnerReadCancellation::new();
        if let Err(error) = self
            .register_active_cancellation(connection.connection_id(), fetch_cancellation.clone())
        {
            let close = entry.close();
            self.release_cursor_resources(retained_memory_bytes);
            close?;
            return Err(error);
        }
        #[cfg(feature = "test-seams")]
        let bounded_probe = self.spec.bounded_probe(
            OwnerBoundedOperation::CursorFetch,
            entry.effective_limits,
            fetch_cancellation.clone(),
        );
        let memory_before = self.spec.database.accountant().usage().used;
        let fetched = entry.fetch(
            rows,
            fetch_cancellation,
            self.spec.clock.as_ref(),
            #[cfg(feature = "test-seams")]
            bounded_probe,
        );
        // What this cursor holds after the page is what it must now be
        // reported as holding: a deferred row raises it, a published one
        // lowers it. The entry and the fleet total move together here, before
        // any path below stores, drops, or releases this cursor.
        let memory_after = self.spec.database.accountant().usage().used;
        let retained_memory_bytes =
            adjusted_retained_bytes(retained_memory_bytes, memory_before, memory_after);
        self.adjust_cursor_retained(entry.retained_memory_bytes, retained_memory_bytes);
        entry.retained_memory_bytes = retained_memory_bytes;
        self.finish_active_request(connection.connection_id());
        let page = match fetched {
            Ok(page) => page,
            Err(BoundedExecutionError::Refused(failure)) => {
                drop(entry);
                self.release_cursor_resources(retained_memory_bytes);
                return Err(map_bounded_error(BoundedExecutionError::Refused(failure)));
            }
            Err(BoundedExecutionError::Cancelled) => {
                if self.is_disconnected(connection.connection_id()) {
                    let close = entry.close();
                    self.release_cursor_resources(retained_memory_bytes);
                    close?;
                    return Err(OwnerReadScaffoldError::Refused(simple_failure(
                        ReadFailureKind::OwnerDisconnected,
                    )));
                }
                if let Err(error) = self.store_cursor(entry) {
                    self.release_cursor_resources(retained_memory_bytes);
                    return Err(error);
                }
                return Err(OwnerReadScaffoldError::Database(
                    contextdb_core::Error::ReadCancelled,
                ));
            }
            Err(error) => {
                let mapped = map_bounded_error(error);
                let close = entry.close();
                self.release_cursor_resources(retained_memory_bytes);
                close?;
                return Err(mapped);
            }
        };

        let responses = match self.cursor_page_response(&page) {
            Ok(responses) => responses,
            Err(error) => {
                let release = entry.discard_unpublished_page(page.has_more);
                self.release_cursor_resources(retained_memory_bytes);
                release?;
                return Err(error);
            }
        };
        if page.has_more {
            if let Err(error) = self.store_cursor(entry) {
                self.release_cursor_resources(retained_memory_bytes);
                return Err(error);
            }
        } else {
            let _ = self
                .exhausted_cursors
                .lock()
                .expect("owner exhausted cursor registry")
                .insert((connection.connection_id(), cursor_id));
            drop(entry);
            self.release_cursor_resources(retained_memory_bytes);
        }
        Ok(responses)
    }

    fn close_cursor(
        &self,
        connection: &ConnectionState,
        cursor_id: [u8; 16],
    ) -> OwnerReadScaffoldResult<Vec<LocalResponse>> {
        if self
            .exhausted_cursors
            .lock()
            .expect("owner exhausted cursor registry")
            .contains(&(connection.connection_id(), cursor_id))
        {
            return self.cursor_closed_response();
        }
        let entry = self.take_cursor(connection.connection_id(), cursor_id)?;
        let retained_memory_bytes = entry.retained_memory_bytes;
        let close = entry.close();
        self.release_cursor_resources(retained_memory_bytes);
        close?;
        self.cursor_closed_response()
    }

    fn cursor_page_response(
        &self,
        page: &CursorPage,
    ) -> OwnerReadScaffoldResult<Vec<LocalResponse>> {
        let payload = encode_cursor_page(page)
            .map_err(|_| OwnerReadScaffoldError::unimplemented("canonical cursor-page encoding"))?;
        let responses = split_payload_answer(payload, |payload| LocalResponse::CursorPage {
            page: CursorPageResponse { payload },
        })?;
        for response in &responses {
            response_is_encodable(response)?;
        }
        Ok(responses)
    }

    fn empty_exhausted_cursor_page(&self) -> OwnerReadScaffoldResult<Vec<LocalResponse>> {
        let page = CursorPage {
            columns: Vec::new(),
            rows: Vec::new(),
            has_more: false,
        };
        self.cursor_page_response(&page)
    }

    fn cursor_closed_response(&self) -> OwnerReadScaffoldResult<Vec<LocalResponse>> {
        let response = LocalResponse::CursorClosed {
            acknowledgement: CursorCloseAcknowledgement { closed: true },
        };
        response_is_encodable(&response)?;
        Ok(vec![response])
    }

    fn metadata_response(
        &self,
        request: LocalMetadataRequest,
        lease: RequestLease,
    ) -> OwnerReadScaffoldResult<Vec<LocalResponse>> {
        let prepared = self.prepare_metadata(request, lease.effective_limits())?;
        let payload = match prepared {
            PreparedOwnerMetadata::CanonicalPage(page) => {
                encode_metadata_page(&page).map_err(|_| {
                    OwnerReadScaffoldError::unimplemented("canonical metadata-page encoding")
                })?
            }
            PreparedOwnerMetadata::CanonicalComplete(payload) => payload,
        };
        self.admit_metadata_payload(&payload, lease.effective_limits())?;
        let responses = split_payload_answer(payload, |payload| LocalResponse::Metadata {
            metadata: MetadataResponse { payload },
        })?;
        for response in &responses {
            response_is_encodable(response)?;
        }
        drop(lease);
        Ok(responses)
    }

    /// Table and event inventories are paged: the caller's byte ceiling
    /// decides how many complete items travel, and the continuation is the
    /// last item published, so resuming can neither repeat nor skip one even
    /// though the inventory itself moved between pages.
    fn prepare_metadata(
        &self,
        request: LocalMetadataRequest,
        effective_limits: ReadLimits,
    ) -> OwnerReadScaffoldResult<PreparedOwnerMetadata> {
        match request {
            LocalMetadataRequest::Tables { continuation } => {
                let items = crate::metadata_page::table_items(self.spec.database.table_names());
                Ok(PreparedOwnerMetadata::CanonicalPage(
                    crate::metadata_page::continuation_page(
                        MetadataPageVocabulary::Tables,
                        items,
                        continuation.as_deref(),
                        effective_limits.result_bytes,
                        crate::metadata_page::metadata_table_key,
                    )
                    .map_err(Self::paging_error)?,
                ))
            }
            LocalMetadataRequest::Schema { table } => {
                let meta = self.spec.database.table_meta(&table).ok_or_else(|| {
                    OwnerReadScaffoldError::Database(contextdb_core::Error::TableNotFound(
                        table.clone(),
                    ))
                })?;
                let payload = crate::read_contract::encode_metadata_body(
                    &crate::direct_file_reader::DirectMetadataBody::Schema {
                        schema: crate::read_image::project_schema(&table, &meta),
                    },
                )
                .map_err(|_| OwnerReadScaffoldError::unimplemented("canonical schema encoding"))?;
                Ok(PreparedOwnerMetadata::CanonicalComplete(payload))
            }
            LocalMetadataRequest::EventsStatus { continuation } => {
                let items = crate::metadata_page::event_status_items(
                    &self.spec.database.event_bus_status(),
                    &self.spec.database.cron_status(),
                );
                Ok(PreparedOwnerMetadata::CanonicalPage(
                    crate::metadata_page::continuation_page(
                        MetadataPageVocabulary::EventsStatus,
                        items,
                        continuation.as_deref(),
                        effective_limits.result_bytes,
                        crate::metadata_page::metadata_event_key,
                    )
                    .map_err(Self::paging_error)?,
                ))
            }
            LocalMetadataRequest::MaintenanceStatus => {
                let status = self.spec.database.maintenance_status();
                let payload = crate::read_contract::encode_metadata_body(
                    &crate::direct_file_reader::DirectMetadataBody::MaintenanceStatus {
                        status: crate::direct_file_reader::DirectMaintenanceStatus {
                            policy: match status.policy {
                                crate::MaintenancePolicy::EngineOwned => "engine_owned".to_owned(),
                                crate::MaintenancePolicy::CallerDriven => {
                                    "caller_driven".to_owned()
                                }
                            },
                            running: status.running,
                            retention_enabled: status.retention_enabled,
                            currency_compaction_enabled: status.currency_compaction_enabled,
                            active_maintenance_loops: status.active_maintenance_loops,
                        },
                    },
                )
                .map_err(|_| {
                    OwnerReadScaffoldError::unimplemented("canonical maintenance-status encoding")
                })?;
                Ok(PreparedOwnerMetadata::CanonicalComplete(payload))
            }
        }
    }

    /// A complete document that does not fit says so with the number the
    /// caller would have to raise, not with a bare refusal they cannot act on.
    /// Say what stopped an inventory from being cut into the page that was
    /// asked for, in the caller's own vocabulary.
    fn paging_error(error: crate::metadata_page::MetadataPagingError) -> OwnerReadScaffoldError {
        match error {
            crate::metadata_page::MetadataPagingError::Continuation(failure)
            | crate::metadata_page::MetadataPagingError::Oversized(failure) => {
                OwnerReadScaffoldError::Refused(failure)
            }
            crate::metadata_page::MetadataPagingError::Encoding(_) => {
                OwnerReadScaffoldError::unimplemented("canonical metadata page size")
            }
        }
    }

    fn admit_metadata_payload(
        &self,
        payload: &[u8],
        effective_limits: ReadLimits,
    ) -> OwnerReadScaffoldResult<()> {
        // Measured by the same function the file route measures with, so an
        // answer refused here is refused there, in the same words.
        match crate::metadata_page::admit_complete_metadata(
            payload.len(),
            effective_limits.result_bytes,
        ) {
            Some(failure) => Err(OwnerReadScaffoldError::Refused(failure)),
            None => Ok(()),
        }
    }

    /// EOF/HUP cancellation is idempotent per registered connection and does
    /// not choose another owner or direct route.
    /// Serve an administrative request from inside the process that owns this
    /// database.
    ///
    /// An embedded caller already holds the owner. It has no channel
    /// connection to authenticate and takes no admission slot, so an owner
    /// whose slots are all held by external readers still answers its own
    /// process. What it does share with an external caller is the shutdown
    /// contract: the request is registered while it runs, so a drain cancels
    /// it and a handler still inside sees that it was withdrawn.
    pub fn request_in_process(
        &self,
        namespace: &str,
        request: &[u8],
    ) -> OwnerReadScaffoldResult<Vec<u8>> {
        let Some(handler) = self.spec.config.handler.clone() else {
            return Err(OwnerReadScaffoldError::Refused(simple_failure(
                ReadFailureKind::OwnerNotRunning,
            )));
        };
        // An identity of its own, so an in-flight in-process request neither
        // collides with an authenticated connection nor blocks a second one.
        let request_identity = *uuid::Uuid::new_v4().as_bytes();
        let cancellation = OwnerReadCancellation::new();
        self.register_active_cancellation(request_identity, cancellation.clone())?;
        let answered = handler.handle(namespace, request, &cancellation);
        self.finish_active_request(request_identity);
        answered.map_err(OwnerReadScaffoldError::Database)
    }

    pub fn disconnect(&self, connection_id: [u8; 16]) -> OwnerReadScaffoldResult<()> {
        // The narrowed handle exists for this connection and nothing else, so
        // it goes when the connection does. It is a derived handle over the
        // writer's own stores and holds none of them open.
        let _released = self
            .declared_connections
            .lock()
            .expect("owner declared-connection registry")
            .remove(&connection_id);
        let newly_disconnected = self
            .disconnected_connections
            .lock()
            .expect("owner disconnected connection registry")
            .insert(connection_id);
        let mut signalled = false;
        if let Some(cancellation) = self
            .cancellations
            .lock()
            .expect("owner cancellation registry")
            .get(&connection_id)
            .cloned()
            && !cancellation.is_cancelled()
        {
            cancellation.cancel();
            signalled = true;
        }

        let cursor_ids: Vec<_> = self
            .cursors
            .lock()
            .expect("owner cursor registry")
            .iter()
            .filter_map(|(cursor_id, entry)| {
                (entry.connection_id == connection_id).then_some(*cursor_id)
            })
            .collect();
        for cursor_id in cursor_ids {
            let entry = self.take_cursor(connection_id, cursor_id)?;
            signalled = true;
            let retained_memory_bytes = entry.retained_memory_bytes;
            let close = entry.close();
            self.release_cursor_resources(retained_memory_bytes);
            close?;
        }
        self.exhausted_cursors
            .lock()
            .expect("owner exhausted cursor registry")
            .retain(|(owner_connection, _)| *owner_connection != connection_id);
        if newly_disconnected && signalled {
            let _ = self.cancellation_signals.fetch_add(1, Ordering::SeqCst);
            #[cfg(feature = "test-seams")]
            if let Some(observer) = &self.spec.execution_observer {
                observer.disconnect_cancellation_signalled();
            }
        }
        Ok(())
    }

    /// Stop serving, without waiting for anything.
    ///
    /// New work is refused, everything in flight is cancelled and told so, the
    /// suspended cursors are released, and the channel comes down. What this
    /// deliberately does NOT do is wait: waiting for the work already admitted
    /// is what a caller asks for by calling close, and a handle that is simply
    /// going away has no way to report a deadline it could not meet and no
    /// business blocking on one — least of all on a deadline whose clock the
    /// program may never advance again.
    ///
    /// Answers whether work was still in flight when serving stopped, so a
    /// caller that cannot wait knows not to release what that work is holding.
    pub fn begin_shutdown(&self) -> OwnerReadScaffoldResult<bool> {
        self.stop_serving()?;
        // "In flight" is every request this owner is still inside, not only
        // the ones holding an admission slot: a request served in-process
        // takes no slot, and an owner that is still answering one is still an
        // owner a reader may dial and be told about.
        // "In flight" is every request this owner is still inside, not only
        // the ones holding an admission slot: a request served in-process
        // takes no slot, and an owner that is still answering one is still an
        // owner a reader may dial and be told about.
        let still_in_flight = self.admission.counters().active_readers > 0
            || !self
                .cancellations
                .lock()
                .expect("owner cancellation registry")
                .is_empty();
        Ok(still_in_flight)
    }

    /// Stop admission, cancel owner work, and drive the shared shutdown
    /// deadline. This method never closes the database.
    pub fn shutdown_and_drain(&self) -> OwnerReadScaffoldResult<()> {
        let deadline_ms = self
            .spec
            .clock
            .now_ms()
            .saturating_add(self.spec.config.timeouts.shutdown_drain_ms);
        self.stop_serving()?;
        // Draining waits for the work already admitted to put its slot down.
        // Crossing the deadline is not a failure of the database: the channel
        // stays bound and every retained resource stays owned, so a later
        // retry can finish what this one could not.
        let operation: LocalDeadlineOperation<'_> = Box::pin(std::future::poll_fn(|context| {
            self.admission.poll_drained(context).map(|()| Ok(()))
        }));
        let drained = wait_for_local_completion(drain_shutdown_with_deadline(
            self.spec.clock.as_ref(),
            deadline_ms,
            operation,
        ))
        .map_err(|_| {
            OwnerReadScaffoldError::Database(contextdb_core::Error::OwnerReadDrainTimeout)
        });
        // The work this owner was serving is finished, so the pathname it was
        // reachable at goes too. Leaving the socket behind hands the next
        // reader something that looks like a live owner and only fails when it
        // dials, which is how an idle store ends up unreadable. A drain that
        // could not finish keeps the channel: readers still have an owner to
        // ask, and a later close finishes what this one could not.
        if drained.is_ok() {
            self.remove_own_channel();
        }
        drained
    }

    fn stop_serving(&self) -> OwnerReadScaffoldResult<()> {
        *self.state.lock().expect("owner service state") = ServiceState::Draining;
        #[cfg(feature = "test-seams")]
        if let Some(hooks) = &self.spec.config.test_hooks {
            let _ = hooks.drain_started.send(());
        }
        self.admission.close_to_new_work()?;
        let mut signalled = false;
        for cancellation in self
            .cancellations
            .lock()
            .expect("owner cancellation registry")
            .values()
        {
            if !cancellation.is_cancelled() {
                cancellation.cancel();
                signalled = true;
            }
        }
        let cursor_ids: Vec<_> = self
            .cursors
            .lock()
            .expect("owner cursor registry")
            .keys()
            .copied()
            .collect();
        for cursor_id in cursor_ids {
            let entry = self
                .cursors
                .lock()
                .expect("owner cursor registry")
                .remove(&cursor_id)
                .expect("shutdown cursor id came from the same registry");
            signalled = true;
            let retained_memory_bytes = entry.retained_memory_bytes;
            let close = entry.close();
            self.release_cursor_resources(retained_memory_bytes);
            close?;
        }
        #[cfg(feature = "test-seams")]
        if signalled && let Some(observer) = &self.spec.execution_observer {
            observer.shutdown_cancellation_signalled();
        }
        if signalled {
            let _ = self.cancellation_signals.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }

    /// Take this owner's channel down, at the moment its store is released.
    ///
    /// A channel outlives serving for exactly as long as the store does: an
    /// owner that has stopped admitting work but is still holding a store
    /// stays reachable, so a reader that dials it is told it is draining
    /// rather than finding nothing and concluding the store is idle. Once the
    /// store is let go there is nothing left to tell anybody about, and a
    /// pathname left behind would be read as a live owner by the next reader.
    pub fn release_channel(&self) {
        self.remove_own_channel();
    }

    /// Take down the channel this service bound, once and only ours.
    #[cfg(unix)]
    fn remove_own_channel(&self) {
        #[cfg(feature = "test-seams")]
        if let Some(address) = self.spec.database.store_channel_address_for_test() {
            crate::read_probe::withdraw_owner_route_source_counter(address);
        }
        let Some(identity) = self.bound_channel_identity else {
            return;
        };
        let _ = crate::local_transport::remove_own_bound_channel(self.channel_path(), identity);
    }

    #[cfg(not(unix))]
    fn remove_own_channel(&self) {}

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn translate_bounded_refusal_for_test(failure: ReadFailure) -> OwnerReadScaffoldError {
        map_bounded_error(BoundedExecutionError::Refused(failure))
    }
}

/// Run one served request with this owner's channel counter in force, so the
/// kernel charges the items it finishes where a reader of that channel can
/// read them. The counter is put back even if the read unwinds.
#[cfg(feature = "test-seams")]
fn with_channel_source_counter<T>(service: &OwnerReadService, operation: impl FnOnce() -> T) -> T {
    struct Restore {
        previous: Option<Arc<AtomicU64>>,
    }
    impl Drop for Restore {
        fn drop(&mut self) {
            crate::read_probe::install_session_source_counter(self.previous.take());
        }
    }
    let previous = crate::read_probe::install_session_source_counter(Some(Arc::clone(
        &service.channel_source_items,
    )));
    let _restore = Restore { previous };
    operation()
}

#[cfg(not(feature = "test-seams"))]
fn with_channel_source_counter<T>(_service: &OwnerReadService, operation: impl FnOnce() -> T) -> T {
    operation()
}

/// Whether this connection keeps serving after the request it just answered.
enum ServedRequest {
    Continue,
    ConnectionEnded,
}

/// One inbound frame, as the connection's reader saw it.
enum ConnectionEvent {
    Request(LocalRequestEnvelope),
    Ended,
}

/// The two things that can reach a request already in flight.
enum ConnectionInterrupt {
    Cancel { request_ordinal: u64 },
    Ended,
}

#[derive(Default)]
struct ConnectionInboxState {
    events: VecDeque<ConnectionEvent>,
    waker: Option<Waker>,
}

/// Reading never stops while a request is being served, so end-of-file and an
/// interrupt both arrive during the statement they are about. A request that
/// arrives early stays queued in arrival order and is served next.
#[derive(Default)]
struct ConnectionInbox {
    state: Mutex<ConnectionInboxState>,
    arrived: Condvar,
}

impl ConnectionInbox {
    fn push(&self, event: ConnectionEvent) {
        let waker = {
            let mut state = self.state.lock().expect("owner connection inbox");
            state.events.push_back(event);
            state.waker.take()
        };
        self.arrived.notify_all();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn blocking_next(&self) -> ConnectionEvent {
        let mut state = self.state.lock().expect("owner connection inbox");
        loop {
            if let Some(event) = state.events.pop_front() {
                return event;
            }
            state = self
                .arrived
                .wait(state)
                .expect("owner connection inbox wait");
        }
    }

    fn poll_interrupt(&self, context: &mut std::task::Context<'_>) -> Poll<ConnectionInterrupt> {
        let mut state = self.state.lock().expect("owner connection inbox");
        let interrupt = match state.events.front() {
            Some(ConnectionEvent::Ended) => Some(ConnectionInterrupt::Ended),
            Some(ConnectionEvent::Request(envelope)) => match &envelope.request {
                LocalRequest::CancelInFlight { request_ordinal } => {
                    Some(ConnectionInterrupt::Cancel {
                        request_ordinal: *request_ordinal,
                    })
                }
                _ => None,
            },
            None => None,
        };
        match interrupt {
            Some(interrupt) => {
                let _consumed = state.events.pop_front();
                Poll::Ready(interrupt)
            }
            None => {
                state.waker = Some(context.waker().clone());
                Poll::Pending
            }
        }
    }
}

#[derive(Default)]
struct ProgressRelayState {
    /// Only the newest report is kept. A reader wants to know what the read
    /// has done NOW, so a backlog of superseded reports would cost the
    /// connection frames to publish news the reader no longer needs, and a
    /// bounded queue that filled would have to block the read to stay
    /// truthful. Keeping one value does neither.
    latest: Option<crate::read_progress::ReadProgress>,
    waker: Option<Waker>,
}

/// Where the request worker leaves what its read has done so far, for the
/// connection to publish while that read is still running.
///
/// The read is charged nothing for a slow reader: publishing replaces one
/// value and takes no frame, no allocation, and no wait. The connection takes
/// the value out from under the lock and writes the frame with nothing held,
/// so a blocked write cannot stop the read from reporting again.
#[derive(Default)]
struct ProgressRelay {
    state: Mutex<ProgressRelayState>,
}

impl ProgressRelay {
    fn take_reported(
        &self,
        context: &mut std::task::Context<'_>,
    ) -> Option<crate::read_progress::ReadProgress> {
        let mut state = self.state.lock().expect("owner request progress relay");
        match state.latest.take() {
            Some(progress) => Some(progress),
            None => {
                state.waker = Some(context.waker().clone());
                None
            }
        }
    }
}

impl crate::read_progress::ReadProgressObserver for ProgressRelay {
    fn progress(&self, progress: crate::read_progress::ReadProgress) {
        let waker = {
            let mut state = self.state.lock().expect("owner request progress relay");
            state.latest = Some(progress);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// What the connection is waiting for while one request is in flight.
/// The in-flight registration of one request, released when it ends -- by
/// answering, by refusing, or by unwinding.
struct ActiveRequest<'a> {
    service: &'a OwnerReadService,
    connection_id: [u8; 16],
}

impl Drop for ActiveRequest<'_> {
    fn drop(&mut self) {
        self.service.finish_active_request(self.connection_id);
    }
}

enum RequestOutcome {
    /// The read reported what it has done so far and is still running.
    Reported(crate::read_progress::ReadProgress),
    /// The read is over and these are its frames.
    Answered(Vec<LocalResponse>),
    /// An interrupt for the request in flight has been APPLIED -- the token
    /// the owner's own execution watches is cancelled. The caller that
    /// cancelled is told so before it goes on.
    CancelApplied(u64),
}

#[derive(Default)]
struct RequestSlotState {
    responses: Option<Vec<LocalResponse>>,
    waker: Option<Waker>,
}

/// Where the request worker leaves its answer for the connection.
#[derive(Default)]
struct RequestSlot {
    state: Mutex<RequestSlotState>,
    completed: Condvar,
}

impl RequestSlot {
    fn complete(&self, responses: Vec<LocalResponse>) {
        let waker = {
            let mut state = self.state.lock().expect("owner request slot");
            state.responses = Some(responses);
            state.waker.take()
        };
        self.completed.notify_all();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn take_ready(&self, context: &mut std::task::Context<'_>) -> Option<Vec<LocalResponse>> {
        let mut state = self.state.lock().expect("owner request slot");
        match state.responses.take() {
            Some(responses) => Some(responses),
            None => {
                state.waker = Some(context.waker().clone());
                None
            }
        }
    }

    fn blocking_take(&self) -> Vec<LocalResponse> {
        let mut state = self.state.lock().expect("owner request slot");
        loop {
            if let Some(responses) = state.responses.take() {
                return responses;
            }
            state = self.completed.wait(state).expect("owner request slot wait");
        }
    }
}

/// Drive one owner-side future to completion on the calling thread. The owner
/// runs no executor; each connection and each drain owns exactly one thread.
fn wait_for_local_completion<F: std::future::Future>(future: F) -> F::Output {
    struct ParkedThread(std::thread::Thread);

    impl std::task::Wake for ParkedThread {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(ParkedThread(std::thread::current())));
    let mut context = std::task::Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match std::pin::Pin::as_mut(&mut future).poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
}

/// The one place an internal owner outcome becomes a frame a reader can act
/// on: a read refusal keeps its typed detail, and an engine answer keeps the
/// engine's own error rather than collapsing into a channel failure.
/// What a caught unwind can be told about itself, in one line.
fn panic_reason(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(reason) = payload.downcast_ref::<&str>() {
        return (*reason).to_owned();
    }
    if let Some(reason) = payload.downcast_ref::<String>() {
        return reason.clone();
    }
    "no reason recorded".to_owned()
}

fn refusal_response(error: OwnerReadScaffoldError) -> LocalResponse {
    match error {
        OwnerReadScaffoldError::Refused(failure) => LocalResponse::Failure { failure },
        OwnerReadScaffoldError::Database(error) => LocalResponse::EngineFailure {
            failure: LocalEngineFailure::from_error(&error),
        },
        OwnerReadScaffoldError::LocalTransport(error) => {
            match OwnerReadScaffoldError::from_local(error) {
                OwnerReadScaffoldError::Refused(failure) => LocalResponse::Failure { failure },
                other => engine_failure_response(&other),
            }
        }
        other => engine_failure_response(&other),
    }
}

/// An owner-side outcome with no engine error of its own still reaches the
/// caller as an engine answer, carrying what happened in the one variant that
/// holds prose.
fn engine_failure_response(error: &OwnerReadScaffoldError) -> LocalResponse {
    LocalResponse::EngineFailure {
        failure: LocalEngineFailure::from_error(&contextdb_core::Error::Other(error.to_string())),
    }
}

/// Failed publication must not turn an already exhausted first page into an
/// explicit second close. A page with continuation still owns live kernel
/// state and therefore goes through the production close seam.
fn close_unpublished_open(opened: &mut BoundedCursorOpen) -> OwnerReadScaffoldResult<()> {
    if opened.first_page.has_more {
        close_bounded_cursor(&mut opened.cursor).map_err(map_bounded_error)
    } else {
        Ok(())
    }
}

fn require_read_statement(sql: &str) -> OwnerReadScaffoldResult<()> {
    if is_read_statement(sql)? {
        return Ok(());
    }
    Err(OwnerReadScaffoldError::Refused(simple_failure(
        ReadFailureKind::WriteRequiresFlag,
    )))
}

/// Whether running this statement would change the store.
fn is_read_statement(sql: &str) -> OwnerReadScaffoldResult<bool> {
    let statement = parse(sql)?;
    Ok(statement_effect(&statement) == StatementEffect::Read)
}

fn simple_failure(kind: ReadFailureKind) -> ReadFailure {
    ReadFailure::new(kind, ReadFailureDetail::None)
        .expect("the selected owner failure uses canonical empty detail")
}

fn map_bounded_error(error: BoundedExecutionError) -> OwnerReadScaffoldError {
    match error {
        BoundedExecutionError::Unimplemented => {
            OwnerReadScaffoldError::unimplemented("bounded execute/open/fetch/close kernel")
        }
        BoundedExecutionError::Refused(failure) => {
            if matches!(
                failure.detail(),
                ReadFailureDetail::OwnerLimitExceeded(detail)
                    if detail.limit == ReadFailureLimit::ActiveMs
            ) {
                OwnerReadScaffoldError::Refused(simple_failure(ReadFailureKind::OwnerTimeout))
            } else {
                OwnerReadScaffoldError::Refused(failure)
            }
        }
        BoundedExecutionError::Cancelled => OwnerReadScaffoldError::unimplemented(
            "timeout/disconnect cancellation-to-failure mapping",
        ),
        BoundedExecutionError::Engine(error) => OwnerReadScaffoldError::Database(error),
    }
}
