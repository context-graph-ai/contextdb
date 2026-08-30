#![cfg(unix)]

use contextdb_core::read_contract::{
    CursorExpiryKind, DatabaseIdentity, DeadlineClock, LocalUserIdentity, MetadataItem,
    MetadataPageVocabulary, OwnerLimitExceededDetail, OwnerReadCancellation, OwnerReadLimits,
    OwnerReadStatus, OwnerRequestHandler, OwnerRouteUnsupportedDetail, OwnerServiceTimeouts,
    OwnerServingReason, OwnerServingState, ReadClientTimeouts, ReadFailure, ReadFailureDetail,
    ReadFailureKind, ReadFailureLimit, ReadLimitField, ReadLimits, RequiredBytesSetting,
    WriterRunNumber,
};
use contextdb_core::{Error, Result, Value};
use contextdb_engine::executor::bounded_read_test_support::{TestSourceTouch, TestWorkSource};
use contextdb_engine::local_transport::{
    ChannelPathFacts, CursorOpenedResponse, CursorPageResponse, LocalConfigurationSource,
    LocalHandshake, LocalMetadataRequest, LocalOwnerStatusResponse, LocalRequest,
    LocalRequestEnvelope, LocalResponse, MAX_FRAME_BYTES, ManualDeadlineClock,
    OrdinaryResultReceiver, ResultReceiveOutcome,
};
use contextdb_engine::owner_read::{
    CursorEntry, CursorIdentifierAllocator, OwnerBoundedExecutionObserver, OwnerBoundedOperation,
    OwnerClient, OwnerReadResourceSnapshot, OwnerReadScaffoldError, OwnerReadService,
    OwnerServiceSpec, ValidatedOwnerListener,
};
use contextdb_engine::read_contract::{
    cursor_page_encoded_size, decode_cursor_page, decode_metadata_page, decode_query_result,
    query_result_encoded_size,
};
use contextdb_engine::{Database, OwnerReadConfig, OwnerReadTestHooks};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::num::NonZeroU64;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Wake, Waker};

struct FutureSignal {
    ready: Mutex<bool>,
    changed: Condvar,
}

impl Wake for FutureSignal {
    fn wake(self: Arc<Self>) {
        let mut ready = self.ready.lock().expect("future signal state");
        *ready = true;
        self.changed.notify_one();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let mut ready = self.ready.lock().expect("future signal state");
        *ready = true;
        self.changed.notify_one();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let signal = Arc::new(FutureSignal {
        ready: Mutex::new(true),
        changed: Condvar::new(),
    });
    let waker = Waker::from(Arc::clone(&signal));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        if let Poll::Ready(output) = Pin::as_mut(&mut future).poll(&mut context) {
            return output;
        }
        let mut ready = signal.ready.lock().expect("future signal state");
        while !*ready {
            ready = signal.changed.wait(ready).expect("future signal wait");
        }
        *ready = false;
    }
}

fn limits() -> ReadLimits {
    ReadLimits {
        result_rows: 500,
        result_bytes: 4 * 1024 * 1024,
        work: 50_000,
        active_ms: 5_000,
        memory: 16 * 1024 * 1024,
        cursor_page_rows: 100,
        cursor_page_bytes: 1024 * 1024,
        cursor_idle_ms: 300_000,
        cursor_lifetime_ms: 1_800_000,
    }
}

fn owner_config(
    owner_limits: ReadLimits,
    concurrency: u64,
    timeouts: OwnerServiceTimeouts,
    handler: Option<Arc<dyn OwnerRequestHandler>>,
) -> OwnerReadConfig {
    OwnerReadConfig {
        enabled: true,
        limits: OwnerReadLimits {
            limits: owner_limits,
            concurrency,
        },
        timeouts,
        runtime_dir: None,
        handler,
        test_hooks: None,
    }
}

#[derive(Default)]
struct HandlerState {
    invocations: usize,
    entered: usize,
    released: bool,
    cancellation_observed: usize,
}

#[derive(Clone, Default)]
struct GatedHandler {
    state: Arc<(Mutex<HandlerState>, Condvar)>,
}

impl GatedHandler {
    fn wait_for_entered(&self, count: usize) {
        let (lock, changed) = &*self.state;
        let mut state = lock.lock().expect("custom handler state");
        while state.entered < count {
            state = changed.wait(state).expect("custom handler entry wait");
        }
    }

    fn release(&self) {
        let (lock, changed) = &*self.state;
        let mut state = lock.lock().expect("custom handler state");
        state.released = true;
        changed.notify_all();
    }

    fn pulse(&self) {
        self.state.1.notify_all();
    }

    fn wait_for_cancellation(&self, count: usize) {
        let (lock, changed) = &*self.state;
        let mut state = lock.lock().expect("custom handler state");
        while state.cancellation_observed < count {
            state = changed.wait(state).expect("custom cancellation wait");
        }
    }

    fn invocations(&self) -> usize {
        self.state
            .0
            .lock()
            .expect("custom handler state")
            .invocations
    }

    fn cancellation_observed(&self) -> usize {
        self.state
            .0
            .lock()
            .expect("custom handler state")
            .cancellation_observed
    }
}

impl OwnerRequestHandler for GatedHandler {
    fn handle(
        &self,
        namespace: &str,
        request: &[u8],
        cancellation: &OwnerReadCancellation,
    ) -> Result<Vec<u8>> {
        let (lock, changed) = &*self.state;
        let mut state = lock.lock().expect("custom handler state");
        state.invocations += 1;
        if namespace == "proof.fail" {
            return Err(Error::Other(
                "owner custom handler fixture failed".to_owned(),
            ));
        }
        if namespace == "proof.oversized-response" {
            return Ok(vec![0x6f; MAX_FRAME_BYTES + 1]);
        }
        if namespace == "proof.hold" {
            state.entered += 1;
            changed.notify_all();
            let mut reported_cancellation = false;
            while !state.released {
                if cancellation.is_cancelled() && !reported_cancellation {
                    state.cancellation_observed += 1;
                    reported_cancellation = true;
                    changed.notify_all();
                }
                state = changed.wait(state).expect("custom handler release wait");
            }
            if cancellation.is_cancelled() && !reported_cancellation {
                state.cancellation_observed += 1;
                changed.notify_all();
            }
        }
        Ok(request.to_vec())
    }
}

struct Harness {
    _directory: tempfile::TempDir,
    database: Arc<Database>,
    service: Arc<OwnerReadService>,
    handshake: LocalHandshake,
    owner_user: LocalUserIdentity,
    clock: ManualDeadlineClock,
    drain_started: std::sync::mpsc::Receiver<()>,
    clients: Mutex<BTreeMap<u8, Arc<Mutex<OwnerClient>>>>,
    client_timeouts: ReadClientTimeouts,
}

impl Harness {
    fn new(
        owner_limits: ReadLimits,
        concurrency: u64,
        handler: Option<Arc<dyn OwnerRequestHandler>>,
    ) -> Self {
        let config = OwnerReadConfig {
            enabled: true,
            limits: OwnerReadLimits {
                limits: owner_limits,
                concurrency,
            },
            timeouts: OwnerServiceTimeouts {
                request_ms: 10,
                shutdown_drain_ms: 10,
            },
            runtime_dir: None,
            handler,
            test_hooks: None,
        };
        Self::start(
            config,
            LocalConfigurationSource::Override,
            ManualDeadlineClock::at(100),
            ReadClientTimeouts {
                connect_ms: 1_000,
                routing_retry_ms: 1_000,
                response_ms: 11_000,
            },
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start(
        mut config: OwnerReadConfig,
        configuration_source: LocalConfigurationSource,
        clock: ManualDeadlineClock,
        client_timeouts: ReadClientTimeouts,
        observer: Option<Arc<dyn OwnerBoundedExecutionObserver>>,
        cursor_identifiers: Option<Arc<dyn CursorIdentifierAllocator>>,
    ) -> Self {
        let directory = tempfile::tempdir().expect("task-scoped owner-read directory");
        let database_path = directory.path().join("owner-service.db");
        let database = Arc::new(Database::open(&database_path).expect("open owner database"));
        database
            .set_memory_limit(Some(64 * 1024 * 1024))
            .expect("declare database memory for complete owner status proof");
        let owner_user = LocalUserIdentity(nix::unistd::Uid::effective().as_raw() as u64);
        let handshake = LocalHandshake::current(
            DatabaseIdentity([0x31; 16]),
            WriterRunNumber([0x42; 16]),
            owner_user,
        );
        let (drain_started, drain_started_at) = std::sync::mpsc::channel();
        config.runtime_dir = Some(directory.path().to_path_buf());
        config.test_hooks = Some(OwnerReadTestHooks {
            clock: Arc::new(clock.clone()),
            drain_started,
        });
        let listener = ValidatedOwnerListener::new(ChannelPathFacts {
            path: directory.path().join("owner.sock"),
            runtime_directory: directory.path().to_path_buf(),
            is_socket: true,
            owner: owner_user,
            mode: 0o700,
        });
        let mut spec = OwnerServiceSpec::new(
            Arc::clone(&database),
            listener,
            handshake.clone(),
            OwnerReadStatus {
                state: OwnerServingState::Serving,
                reason: None,
            },
            config,
            configuration_source,
            Arc::new(clock.clone()),
        );
        if let Some(observer) = observer {
            spec = spec.with_execution_observer_for_test(observer);
        }
        if let Some(cursor_identifiers) = cursor_identifiers {
            spec = spec.with_cursor_identifier_allocator_for_test(cursor_identifiers);
        }
        let service = OwnerReadService::start(spec)
            .expect("all production prerequisites must start the owner service");
        Self {
            _directory: directory,
            database,
            service,
            handshake,
            owner_user,
            clock,
            drain_started: drain_started_at,
            clients: Mutex::new(BTreeMap::new()),
            client_timeouts,
        }
    }

    fn client(&self, connection: u8) -> Arc<Mutex<OwnerClient>> {
        if let Some(client) = self
            .clients
            .lock()
            .expect("owner client registry")
            .get(&connection)
            .cloned()
        {
            return client;
        }
        let clock: Arc<dyn DeadlineClock> = Arc::new(self.clock.clone());
        let client = Arc::new(Mutex::new(
            block_on(OwnerClient::connect(
                self.service.channel_path(),
                self.handshake.clone(),
                self.client_timeouts,
                clock,
            ))
            .expect("connect through the production owner carrier"),
        ));
        self.clients
            .lock()
            .expect("owner client registry")
            .insert(connection, Arc::clone(&client));
        client
    }

    fn request(
        &self,
        connection: u8,
        request_limits: ReadLimits,
        request: LocalRequest,
    ) -> std::result::Result<Vec<LocalResponse>, OwnerReadScaffoldError> {
        let client = self.client(connection);
        block_on(
            client
                .lock()
                .expect("selected owner client")
                .request(LocalRequestEnvelope {
                    limits: request_limits,
                    request,
                }),
        )
    }

    fn request_with_handshake(
        &self,
        presented: LocalHandshake,
        request_limits: ReadLimits,
        request: LocalRequest,
    ) -> std::result::Result<Vec<LocalResponse>, OwnerReadScaffoldError> {
        let clock: Arc<dyn DeadlineClock> = Arc::new(self.clock.clone());
        let mut client = block_on(OwnerClient::connect(
            self.service.channel_path(),
            presented,
            self.client_timeouts,
            clock,
        ))?;
        block_on(client.request(LocalRequestEnvelope {
            limits: request_limits,
            request,
        }))
    }
}

fn query(statement: &str) -> LocalRequest {
    LocalRequest::Query {
        statement: statement.to_owned(),
        params: BTreeMap::new(),
    }
}

fn custom(namespace: &str, payload: &[u8]) -> LocalRequest {
    LocalRequest::Custom {
        namespace: namespace.to_owned(),
        payload: payload.to_vec(),
    }
}

fn fetch_rows(rows: u64) -> Option<NonZeroU64> {
    Some(NonZeroU64::new(rows).expect("cursor fetch rows must be positive"))
}

fn refusal(result: std::result::Result<Vec<LocalResponse>, OwnerReadScaffoldError>) -> ReadFailure {
    match result.expect_err("request must return a typed refusal") {
        OwnerReadScaffoldError::Refused(failure) => failure,
        other => panic!("expected typed owner refusal, got {other:?}"),
    }
}

fn assert_simple_refusal(failure: &ReadFailure, kind: ReadFailureKind) {
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.detail(), &ReadFailureDetail::None);
}

fn assert_limit_refusal(failure: &ReadFailure, limit: ReadFailureLimit, value: u64) {
    assert_eq!(failure.kind(), ReadFailureKind::OwnerLimitExceeded);
    let ReadFailureDetail::OwnerLimitExceeded(detail) = failure.detail() else {
        panic!("owner limit refusal must retain typed detail: {failure:?}");
    };
    assert_eq!(detail.limit, limit);
    assert_eq!(detail.value, value);
}

fn keep_limit_relationships_valid(limits: &mut ReadLimits) {
    limits.cursor_page_rows = limits.cursor_page_rows.min(limits.result_rows);
    limits.cursor_page_bytes = limits.cursor_page_bytes.min(limits.result_bytes);
    limits.cursor_idle_ms = limits.cursor_idle_ms.min(limits.cursor_lifetime_ms);
}

fn owner_status(responses: Vec<LocalResponse>) -> LocalOwnerStatusResponse {
    let [LocalResponse::OwnerStatus { status }] = responses.as_slice() else {
        panic!("owner status must be one complete status response");
    };
    status.clone()
}

fn assert_complete_status_configuration(
    status: &LocalOwnerStatusResponse,
    expected: ReadLimits,
    concurrency: u64,
    timeouts: OwnerServiceTimeouts,
    source: LocalConfigurationSource,
) {
    // A limit is this deployment's choice only where its value differs from
    // what ships; a configuration declared as overridden still reports each
    // untouched ceiling as the shipped default.
    let shipped = contextdb_core::read_contract::OwnerReadLimits::default();
    let shipped_timeouts = OwnerServiceTimeouts::default();
    let expected_source = |expected_value: u64, shipped_value: u64| {
        if source == LocalConfigurationSource::Override && expected_value != shipped_value {
            LocalConfigurationSource::Override
        } else {
            LocalConfigurationSource::Default
        }
    };
    for (configured, expected_value, shipped_value) in [
        (
            &status.effective_limits.result_rows,
            expected.result_rows,
            shipped.limits.result_rows,
        ),
        (
            &status.effective_limits.result_bytes,
            expected.result_bytes,
            shipped.limits.result_bytes,
        ),
        (
            &status.effective_limits.work,
            expected.work,
            shipped.limits.work,
        ),
        (
            &status.effective_limits.active_ms,
            expected.active_ms,
            shipped.limits.active_ms,
        ),
        (
            &status.effective_limits.memory,
            expected.memory,
            shipped.limits.memory,
        ),
        (
            &status.effective_limits.cursor_page_rows,
            expected.cursor_page_rows,
            shipped.limits.cursor_page_rows,
        ),
        (
            &status.effective_limits.cursor_page_bytes,
            expected.cursor_page_bytes,
            shipped.limits.cursor_page_bytes,
        ),
        (
            &status.effective_limits.cursor_idle_ms,
            expected.cursor_idle_ms,
            shipped.limits.cursor_idle_ms,
        ),
        (
            &status.effective_limits.cursor_lifetime_ms,
            expected.cursor_lifetime_ms,
            shipped.limits.cursor_lifetime_ms,
        ),
        (
            &status.effective_limits.concurrency,
            concurrency,
            shipped.concurrency,
        ),
    ] {
        assert_eq!(configured.value, expected_value);
        assert_eq!(
            configured.source,
            expected_source(expected_value, shipped_value)
        );
    }
    assert_eq!(status.timeouts.request_ms.value, timeouts.request_ms);
    assert_eq!(
        status.timeouts.request_ms.source,
        expected_source(timeouts.request_ms, shipped_timeouts.request_ms)
    );
    assert_eq!(
        status.timeouts.shutdown_drain_ms.value,
        timeouts.shutdown_drain_ms,
    );
    assert_eq!(
        status.timeouts.shutdown_drain_ms.source,
        expected_source(
            timeouts.shutdown_drain_ms,
            shipped_timeouts.shutdown_drain_ms
        )
    );
    assert_eq!(status.admission.capacity, concurrency);
}

fn cursor_opened(responses: Vec<LocalResponse>) -> CursorOpenedResponse {
    let [LocalResponse::CursorOpened { opened }] = responses.as_slice() else {
        panic!("cursor open must publish one complete first-page response");
    };
    opened.clone()
}

fn cursor_page(responses: Vec<LocalResponse>) -> CursorPageResponse {
    let [LocalResponse::CursorPage { page }] = responses.as_slice() else {
        panic!("cursor fetch must publish one complete page response");
    };
    page.clone()
}

fn metadata_payload(responses: Vec<LocalResponse>) -> Vec<u8> {
    let [LocalResponse::Metadata { metadata }] = responses.as_slice() else {
        panic!("metadata must publish one complete response");
    };
    metadata.payload.clone()
}

fn assert_resources(service: &OwnerReadService, expected: OwnerReadResourceSnapshot) {
    assert_eq!(service.resources(), expected);
}

/// A cursor that keeps paging keeps its slot, its count, its buffers and its
/// cancellation state, while the memory it retains may only FALL as the
/// answer drains out of it -- a sort hands back each buffered row's charge
/// as the row leaves. Returns the snapshot so the next page compares
/// against what this one left.
fn assert_resources_draining(
    service: &OwnerReadService,
    previous: OwnerReadResourceSnapshot,
) -> OwnerReadResourceSnapshot {
    let now = service.resources();
    assert_eq!(now.active_slots, previous.active_slots);
    assert_eq!(now.cursor_count, previous.cursor_count);
    assert_eq!(now.buffered_bytes, previous.buffered_bytes);
    assert_eq!(now.active_cancellations, previous.active_cancellations);
    assert!(
        now.cursor_retained_bytes <= previous.cursor_retained_bytes,
        "a paging cursor never retains more after a page than before it: {} after {}",
        now.cursor_retained_bytes,
        previous.cursor_retained_bytes
    );
    now
}

fn published_result(
    responses: Vec<LocalResponse>,
) -> contextdb_engine::read_contract::CanonicalQueryResult {
    let mut receiver = OrdinaryResultReceiver::new();
    let mut published = None;
    for response in responses {
        match OwnerClient::receive_ordinary(&mut receiver, response)
            .expect("ordinary response uses the terminal receiver")
        {
            ResultReceiveOutcome::Pending => assert!(published.is_none()),
            ResultReceiveOutcome::Published(result) => published = Some(result.bytes),
            other => panic!("successful ordinary result ended as {other:?}"),
        }
    }
    decode_query_result(&published.expect("terminal success publishes all bytes"))
        .expect("decode canonical query result")
}

#[derive(Debug, Clone, Copy)]
struct OperationRecord {
    operation: OwnerBoundedOperation,
    limits: ReadLimits,
    operation_event: u64,
    first_work_event: Option<u64>,
}

#[derive(Default)]
struct ObserverState {
    next_event: u64,
    records: Vec<OperationRecord>,
    current: Option<usize>,
    latest_cancellation: Option<OwnerReadCancellation>,
    blocked_operation: Option<OwnerBoundedOperation>,
    blocked: bool,
    released: bool,
    max_work: u64,
    peak_temporary_bytes: u64,
    cancellation_observed: u64,
    disconnect_signals: u64,
    request_deadline_signals: u64,
    shutdown_signals: u64,
    finished_requests: u64,
    active_clock_base_ms: u64,
}

#[derive(Clone, Default)]
struct ProductionObserver {
    state: Arc<(Mutex<ObserverState>, Condvar)>,
    active_clock: Option<ManualDeadlineClock>,
}

impl ProductionObserver {
    fn advancing(clock: ManualDeadlineClock) -> Self {
        Self {
            state: Arc::default(),
            active_clock: Some(clock),
        }
    }

    fn records(&self) -> Vec<OperationRecord> {
        self.state
            .0
            .lock()
            .expect("bounded observer state")
            .records
            .clone()
    }

    fn clear(&self) {
        let mut state = self.state.0.lock().expect("bounded observer state");
        state.records.clear();
        state.current = None;
        state.max_work = 0;
        state.peak_temporary_bytes = 0;
        state.cancellation_observed = 0;
    }

    fn arm_block(&self, operation: OwnerBoundedOperation) {
        let mut state = self.state.0.lock().expect("bounded observer state");
        state.blocked_operation = Some(operation);
        state.blocked = false;
        state.released = false;
    }

    fn wait_until_blocked(&self) {
        let (lock, changed) = &*self.state;
        let mut state = lock.lock().expect("bounded observer state");
        while !state.blocked {
            state = changed.wait(state).expect("bounded observer block wait");
        }
    }

    fn release_block(&self) {
        let (lock, changed) = &*self.state;
        let mut state = lock.lock().expect("bounded observer state");
        state.released = true;
        state.blocked_operation = None;
        changed.notify_all();
    }

    fn cancel_current(&self) {
        self.state
            .0
            .lock()
            .expect("bounded observer state")
            .latest_cancellation
            .clone()
            .expect("a production bounded operation supplied cancellation")
            .cancel();
    }

    fn current_is_cancelled(&self) -> bool {
        self.state
            .0
            .lock()
            .expect("bounded observer state")
            .latest_cancellation
            .as_ref()
            .expect("a production bounded operation supplied cancellation")
            .is_cancelled()
    }

    fn wait_for_disconnect_signal(&self, expected: u64) {
        let (lock, changed) = &*self.state;
        let mut state = lock.lock().expect("bounded observer state");
        while state.disconnect_signals < expected {
            state = changed.wait(state).expect("disconnect signal wait");
        }
    }

    fn wait_for_shutdown_signal(&self, expected: u64) {
        let (lock, changed) = &*self.state;
        let mut state = lock.lock().expect("bounded observer state");
        while state.shutdown_signals < expected {
            state = changed.wait(state).expect("shutdown signal wait");
        }
    }

    fn wait_for_request_deadline_signal(&self, expected: u64) {
        let (lock, changed) = &*self.state;
        let mut state = lock.lock().expect("bounded observer state");
        while state.request_deadline_signals < expected {
            state = changed.wait(state).expect("request deadline signal wait");
        }
    }

    fn wait_for_finished_requests(&self, expected: u64) {
        let (lock, changed) = &*self.state;
        let mut state = lock.lock().expect("bounded observer state");
        while state.finished_requests < expected {
            state = changed.wait(state).expect("request finish wait");
        }
    }

    fn max_work(&self) -> u64 {
        self.state
            .0
            .lock()
            .expect("bounded observer state")
            .max_work
    }

    fn peak_temporary_bytes(&self) -> u64 {
        self.state
            .0
            .lock()
            .expect("bounded observer state")
            .peak_temporary_bytes
    }

    fn cancellation_observed(&self) -> u64 {
        self.state
            .0
            .lock()
            .expect("bounded observer state")
            .cancellation_observed
    }
}

impl OwnerBoundedExecutionObserver for ProductionObserver {
    fn before_operation(
        &self,
        operation: OwnerBoundedOperation,
        effective_limits: ReadLimits,
        cancellation: OwnerReadCancellation,
    ) {
        let mut state = self.state.0.lock().expect("bounded observer state");
        state.next_event = state.next_event.saturating_add(1);
        let operation_event = state.next_event;
        state.records.push(OperationRecord {
            operation,
            limits: effective_limits,
            operation_event,
            first_work_event: None,
        });
        state.current = Some(state.records.len() - 1);
        state.latest_cancellation = Some(cancellation);
        state.active_clock_base_ms = self.active_clock.as_ref().map_or(0, |clock| clock.now_ms());
    }

    fn before_work(&self, _source: TestWorkSource, completed_work: u64) {
        let (clock_target, should_block) = {
            let (lock, changed) = &*self.state;
            let mut state = lock.lock().expect("bounded observer state");
            state.next_event = state.next_event.saturating_add(1);
            let event = state.next_event;
            let current = state.current.expect("work follows a bounded operation");
            if state.records[current].first_work_event.is_none() {
                state.records[current].first_work_event = Some(event);
            }
            state.max_work = state.max_work.max(completed_work.saturating_add(1));
            let should_block = state.blocked_operation == Some(state.records[current].operation)
                && !state.released;
            if should_block {
                state.blocked = true;
                changed.notify_all();
            }
            (
                self.active_clock.as_ref().map(|_| {
                    state
                        .active_clock_base_ms
                        .saturating_add(completed_work.saturating_add(1))
                }),
                should_block,
            )
        };
        if let (Some(clock), Some(target)) = (&self.active_clock, clock_target)
            && target > clock.now_ms()
        {
            clock.advance_to(target);
        }
        if should_block {
            let (lock, changed) = &*self.state;
            let mut state = lock.lock().expect("bounded observer state");
            while !state.released {
                state = changed.wait(state).expect("bounded operation release wait");
            }
        }
    }

    fn before_source_touch(&self, _touch: TestSourceTouch, _completed_items: u64) {}

    fn before_temporary_reservation(
        &self,
        _source: TestWorkSource,
        requested_bytes: u64,
        held_temporary_bytes: u64,
    ) {
        let mut state = self.state.0.lock().expect("bounded observer state");
        state.peak_temporary_bytes = state
            .peak_temporary_bytes
            .max(held_temporary_bytes.saturating_add(requested_bytes));
    }

    fn after_temporary_reservation(
        &self,
        _source: TestWorkSource,
        reserved_bytes: u64,
        held_temporary_bytes: u64,
    ) {
        let mut state = self.state.0.lock().expect("bounded observer state");
        state.peak_temporary_bytes = state
            .peak_temporary_bytes
            .max(held_temporary_bytes)
            .max(reserved_bytes);
    }

    fn cancellation_observed(&self, _completed_work: u64) {
        let (lock, changed) = &*self.state;
        let mut state = lock.lock().expect("bounded observer state");
        state.cancellation_observed = state.cancellation_observed.saturating_add(1);
        changed.notify_all();
    }

    fn disconnect_cancellation_signalled(&self) {
        let (lock, changed) = &*self.state;
        let mut state = lock.lock().expect("bounded observer state");
        state.disconnect_signals = state.disconnect_signals.saturating_add(1);
        changed.notify_all();
    }

    fn shutdown_cancellation_signalled(&self) {
        let (lock, changed) = &*self.state;
        let mut state = lock.lock().expect("bounded observer state");
        state.shutdown_signals = state.shutdown_signals.saturating_add(1);
        changed.notify_all();
    }

    fn request_deadline_cancellation_signalled(&self) {
        let (lock, changed) = &*self.state;
        let mut state = lock.lock().expect("bounded observer state");
        state.request_deadline_signals = state.request_deadline_signals.saturating_add(1);
        changed.notify_all();
    }

    fn request_finished(&self) {
        let (lock, changed) = &*self.state;
        let mut state = lock.lock().expect("bounded observer state");
        state.finished_requests = state.finished_requests.saturating_add(1);
        changed.notify_all();
    }
}

#[test]
fn cursor_registry_value_is_an_owned_lifetime_free_handoff() {
    fn assert_registry_value<T: Send + 'static>() {}
    fn assert_shared_service<T: Send + Sync + 'static>() {}

    assert_registry_value::<CursorEntry>();
    assert_shared_service::<OwnerReadService>();
}

#[test]
fn service_source_keeps_database_lifecycle_storage_and_raw_time_out_of_scope() {
    let source = include_str!("../src/owner_read/service.rs");
    let crate_root = include_str!("../src/lib.rs");
    for forbidden in [
        "self.spec.database.execute(",
        "self.spec.database.close(",
        "Database::execute",
        "Database::close",
        "redb::",
        "persistence::",
        "companion",
        "std::time::",
        "thread::sleep",
    ] {
        assert!(
            !source.contains(forbidden),
            "the owner service must not cross its frozen boundary: {forbidden}",
        );
    }
    for required in [
        "open_bounded_cursor(",
        "fetch_bounded_cursor(",
        "close_bounded_cursor(",
        "execute_bounded(",
        "authenticate_framed_stream_handshake(",
        "receive_message(",
        "LocalOutboundMessage::Response",
        "send_message(",
        "serve_request_with_deadline(",
        "service.start_accept_loop()?",
        "BoundedCursorHandle",
    ] {
        assert!(
            source.contains(required),
            "the owner service must retain production seam {required}",
        );
    }
    assert!(crate_root.contains("#[cfg(not(feature = \"test-seams\"))]\nmod owner_read;"));
    assert!(
        crate_root
            .contains("#[cfg(feature = \"test-seams\")]\n#[doc(hidden)]\npub mod owner_read;",)
    );
}

fn prove_service_capacity_boundary(mut config: OwnerReadConfig, source: LocalConfigurationSource) {
    let handler = GatedHandler::default();
    config.handler = Some(Arc::new(handler.clone()));
    let expected_limits = config.limits.limits;
    let expected_concurrency = config.limits.concurrency;
    let expected_timeouts = config.timeouts;
    let harness = Harness::start(
        config,
        source,
        ManualDeadlineClock::at(100),
        ReadClientTimeouts {
            connect_ms: 1_000,
            routing_retry_ms: 1_000,
            response_ms: expected_timeouts.request_ms.saturating_add(1_000),
        },
        None,
        None,
    );
    let baseline = harness.service.resources();
    let mut held = Vec::new();
    for active in 0..expected_concurrency {
        let connection = u8::try_from(active + 1).expect("capacity fixture fits client labels");
        let client = harness.client(connection);
        held.push(std::thread::spawn(move || {
            block_on(
                client
                    .lock()
                    .expect("held owner client")
                    .request(LocalRequestEnvelope {
                        limits: expected_limits,
                        request: custom("proof.hold", b"held"),
                    }),
            )
        }));
    }
    handler.wait_for_entered(
        usize::try_from(expected_concurrency).expect("capacity fits handler count"),
    );

    let refused_connection =
        u8::try_from(expected_concurrency + 1).expect("capacity fixture fits client labels");
    let full = refusal(harness.request(
        refused_connection,
        expected_limits,
        custom("proof.hold", b"N+1"),
    ));
    assert_simple_refusal(&full, ReadFailureKind::OwnerAtCapacity);
    assert_eq!(
        handler.invocations(),
        usize::try_from(expected_concurrency).expect("capacity fits handler count"),
        "N+1 refuses before handler invocation and cannot queue",
    );

    let status = owner_status(
        harness
            .request(
                refused_connection.saturating_add(1),
                expected_limits,
                LocalRequest::OwnerStatus,
            )
            .expect("authenticated status bypasses full reader admission"),
    );
    assert_eq!(
        status.status,
        OwnerReadStatus {
            state: OwnerServingState::Serving,
            reason: None,
        },
    );
    assert_eq!(status.admission.active_readers, expected_concurrency);
    assert_complete_status_configuration(
        &status,
        expected_limits,
        expected_concurrency,
        expected_timeouts,
        source,
    );
    assert_eq!(status.memory.used_bytes, baseline.accountant_used_bytes);
    assert_eq!(
        status.memory.available_bytes,
        Some((64 * 1024 * 1024_u64).saturating_sub(status.memory.used_bytes)),
    );
    assert_eq!(
        harness.service.resources().active_slots,
        expected_concurrency
    );

    handler.release();
    for request in held {
        request
            .join()
            .expect("held request thread joins")
            .expect("held request completes after release");
    }
    assert_resources(&harness.service, baseline);
}

#[test]
fn shipped_default_four_and_raised_capacity_refuse_n_plus_one_without_queueing() {
    let shipped = OwnerReadConfig::default();
    assert_eq!(shipped.limits.limits, limits());
    assert_eq!(shipped.limits.concurrency, 4);
    assert_eq!(
        shipped.timeouts,
        OwnerServiceTimeouts {
            request_ms: 10_000,
            shutdown_drain_ms: 10_000,
        },
    );
    prove_service_capacity_boundary(shipped, LocalConfigurationSource::Default);

    prove_service_capacity_boundary(
        owner_config(
            limits(),
            7,
            OwnerServiceTimeouts {
                request_ms: 17_000,
                shutdown_drain_ms: 19_000,
            },
            None,
        ),
        LocalConfigurationSource::Override,
    );
}

#[test]
fn eof_hup_cancels_once_at_bounded_work_discards_publication_and_releases_every_resource() {
    let handler = GatedHandler::default();
    let observer = Arc::new(ProductionObserver::default());
    let observer_seam: Arc<dyn OwnerBoundedExecutionObserver> = observer.clone();
    let harness = Harness::start(
        owner_config(
            limits(),
            4,
            OwnerServiceTimeouts {
                request_ms: 10_000,
                shutdown_drain_ms: 10_000,
            },
            Some(Arc::new(handler.clone())),
        ),
        LocalConfigurationSource::Override,
        ManualDeadlineClock::at(100),
        ReadClientTimeouts {
            connect_ms: 1_000,
            routing_retry_ms: 1_000,
            response_ms: 11_000,
        },
        Some(observer_seam),
        None,
    );
    let baseline = harness.service.resources();
    let client = harness.client(7);
    let (disconnect, repeated_disconnect) = {
        let selected = client.lock().expect("disconnecting owner client");
        (
            selected
                .disconnect_handle()
                .expect("clone selected carrier for EOF/HUP"),
            selected
                .disconnect_handle()
                .expect("clone selected carrier for repeated EOF/HUP"),
        )
    };
    let request_client = Arc::clone(&client);
    let held = std::thread::spawn(move || {
        block_on(
            request_client
                .lock()
                .expect("disconnecting request client")
                .request(LocalRequestEnvelope {
                    limits: limits(),
                    request: custom("proof.hold", b"disconnect-before-publication"),
                }),
        )
    });
    handler.wait_for_entered(1);

    let mut receiver = OrdinaryResultReceiver::new();
    OwnerClient::receive_ordinary(
        &mut receiver,
        LocalResponse::ResultChunk {
            chunk: contextdb_engine::local_transport::ResultChunk {
                bytes: vec![0x51, 0x52],
            },
        },
    )
    .expect("stage an ordinary result fragment before EOF/HUP");
    assert_eq!(receiver.buffered_byte_count(), 2);
    assert_eq!(
        receiver
            .disconnect()
            .expect("EOF/HUP discards staged ordinary bytes"),
        ResultReceiveOutcome::Disconnected
    );
    disconnect
        .disconnect()
        .expect("EOF/HUP closes only the selected owner carrier");
    observer.wait_for_disconnect_signal(1);
    handler.pulse();
    handler.wait_for_cancellation(1);
    assert_eq!(receiver.buffered_byte_count(), 0);
    assert_eq!(harness.service.cancellation_signal_count(), 1);
    repeated_disconnect
        .disconnect()
        .expect("a repeated carrier shutdown is an idempotent no-op");
    assert_eq!(harness.service.cancellation_signal_count(), 1);
    let status = owner_status(
        harness
            .request(8, limits(), LocalRequest::OwnerStatus)
            .expect("status remains independent while the ignoring handler holds its slot"),
    );
    assert_eq!(status.admission.active_readers, 1);
    let while_disconnected_handler_ignores = harness.service.resources();
    assert_eq!(while_disconnected_handler_ignores.active_slots, 1);
    assert_eq!(
        while_disconnected_handler_ignores.cursor_count,
        baseline.cursor_count
    );
    assert_eq!(
        while_disconnected_handler_ignores.cursor_retained_bytes,
        baseline.cursor_retained_bytes,
    );
    assert_eq!(while_disconnected_handler_ignores.buffered_bytes, 0);
    assert_eq!(while_disconnected_handler_ignores.active_cancellations, 1);
    assert_eq!(
        while_disconnected_handler_ignores.accountant_used_bytes,
        baseline.accountant_used_bytes,
    );

    handler.release();
    let failure = held
        .join()
        .expect("disconnecting request thread joins")
        .expect_err("cancelled work must never publish its handler response");
    assert!(matches!(
        failure,
        OwnerReadScaffoldError::Refused(failure)
            if failure.kind() == ReadFailureKind::OwnerDisconnected
    ));
    assert_eq!(handler.cancellation_observed(), 1);
    assert_resources(&harness.service, baseline);
}

fn prove_request_and_response_deadline_jump(jump_to_ms: u64) {
    let handler = GatedHandler::default();
    let observer = Arc::new(ProductionObserver::default());
    let observer_seam: Arc<dyn OwnerBoundedExecutionObserver> = observer.clone();
    let harness = Harness::start(
        owner_config(
            limits(),
            4,
            OwnerServiceTimeouts {
                request_ms: 10,
                shutdown_drain_ms: 10_000,
            },
            Some(Arc::new(handler.clone())),
        ),
        LocalConfigurationSource::Override,
        ManualDeadlineClock::at(100),
        ReadClientTimeouts {
            connect_ms: 1_000,
            routing_retry_ms: 1_000,
            response_ms: 11,
        },
        Some(observer_seam),
        None,
    );
    let baseline = harness.service.resources();
    let client = harness.client(93);
    let worker_client = Arc::clone(&client);
    let worker = std::thread::spawn(move || {
        block_on(
            worker_client
                .lock()
                .expect("deadline owner client")
                .request(LocalRequestEnvelope {
                    limits: limits(),
                    request: custom("proof.hold", b"never-published"),
                }),
        )
    });
    handler.wait_for_entered(1);

    harness.clock.advance_to(jump_to_ms);
    observer.wait_for_request_deadline_signal(1);
    handler.pulse();
    handler.wait_for_cancellation(1);
    assert_eq!(harness.service.cancellation_signal_count(), 1);
    assert_eq!(harness.service.resources().active_slots, 1);

    if jump_to_ms == 110 {
        harness.clock.advance_to(111);
    }
    let timeout = worker
        .join()
        .expect("deadline request joins")
        .expect_err("request/response deadline publishes no custom response");
    assert!(matches!(
        timeout,
        OwnerReadScaffoldError::Refused(failure)
            if failure.kind() == ReadFailureKind::OwnerTimeout
                && failure.detail() == &ReadFailureDetail::None
    ));
    assert_eq!(handler.cancellation_observed(), 1);
    assert_eq!(harness.service.resources().active_slots, 1);

    handler.release();
    observer.wait_for_finished_requests(1);
    assert_resources(&harness.service, baseline);
    assert_eq!(harness.service.cancellation_signal_count(), 1);
}

#[test]
fn exact_and_plus_one_request_response_deadlines_timeout_once_without_publication() {
    prove_request_and_response_deadline_jump(110);
    prove_request_and_response_deadline_jump(112);
}

#[test]
fn peer_and_every_handshake_identity_field_are_refused_before_admission_or_handler() {
    let handler = GatedHandler::default();
    let harness = Harness::new(limits(), 4, Some(Arc::new(handler.clone())));
    let baseline = harness.service.resources();
    let expected = harness.handshake.clone();
    let mut marker = expected.clone();
    marker.marker[0] ^= 1;
    let mut version = expected.clone();
    version.version = version.version.saturating_add(1);
    let mut database = expected.clone();
    database.database_identity.0[0] ^= 1;
    let mut writer = expected.clone();
    writer.writer_run.0[0] ^= 1;
    let mut recorded_user = expected.clone();
    recorded_user.owner_user = LocalUserIdentity(harness.owner_user.0 + 1);
    let cases = [
        (
            marker,
            ReadFailureKind::LocalProtocolMismatch,
            "protocol marker",
        ),
        (
            version,
            ReadFailureKind::LocalProtocolMismatch,
            "protocol version",
        ),
        (
            database,
            ReadFailureKind::OwnerMismatch,
            "database identity",
        ),
        (writer, ReadFailureKind::OwnerMismatch, "writer run"),
        (
            recorded_user,
            ReadFailureKind::OwnerUserMismatch,
            "recorded owner user",
        ),
    ];

    for (presented, expected_kind, field) in cases {
        let failure = refusal(harness.request_with_handshake(
            presented,
            limits(),
            custom("proof.echo", b"must-not-enter"),
        ));
        assert_simple_refusal(&failure, expected_kind);
        assert_resources(&harness.service, baseline);
        assert_eq!(
            handler.invocations(),
            0,
            "the {field} refusal happens before admission and handler invocation"
        );
    }

    #[cfg(unix)]
    {
        assert!(harness.service.channel_is_bound_for_test());
        assert!(harness.service.channel_path().exists());
    }
    let echoed = harness
        .request(90, limits(), custom("proof.echo", b"same-os-principal"))
        .expect("the carrier-derived same-user principal authorizes a custom request");
    assert_eq!(
        echoed,
        vec![LocalResponse::Custom {
            payload: b"same-os-principal".to_vec(),
        }],
    );
    assert_eq!(handler.invocations(), 1);

    let oversized_request = refusal(harness.request(
        91,
        limits(),
        custom("proof.echo", &vec![0x71; MAX_FRAME_BYTES + 1]),
    ));
    assert_simple_refusal(&oversized_request, ReadFailureKind::InvalidChannelData);
    assert_eq!(
        handler.invocations(),
        1,
        "oversized request framing refuses before handler invocation",
    );

    let oversized_response = refusal(harness.request(
        92,
        limits(),
        custom("proof.oversized-response", b"small-request"),
    ));
    assert_simple_refusal(&oversized_response, ReadFailureKind::InvalidChannelData);
    assert_eq!(handler.invocations(), 2);
    assert_resources(&harness.service, baseline);
}

#[test]
fn writes_are_refused_before_engine_and_uncommitted_writer_rows_are_invisible() {
    let observer = Arc::new(ProductionObserver::default());
    let observer_seam: Arc<dyn OwnerBoundedExecutionObserver> = observer.clone();
    let harness = Harness::start(
        owner_config(
            limits(),
            4,
            OwnerServiceTimeouts {
                request_ms: 10_000,
                shutdown_drain_ms: 10_000,
            },
            None,
        ),
        LocalConfigurationSource::Override,
        ManualDeadlineClock::at(100),
        ReadClientTimeouts {
            connect_ms: 1_000,
            routing_retry_ms: 1_000,
            response_ms: 11_000,
        },
        Some(observer_seam),
        None,
    );
    harness
        .database
        .execute(
            "CREATE TABLE owner_visibility (id INTEGER PRIMARY KEY, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create visibility fixture");
    let baseline = harness.service.resources();

    for sql in [
        "INSERT INTO owner_visibility VALUES (1, 'blocked')",
        "BEGIN",
        "COMMIT",
    ] {
        let failure = refusal(harness.request(1, limits(), query(sql)));
        assert_simple_refusal(&failure, ReadFailureKind::WriteRequiresFlag);
        assert!(
            observer.records().is_empty(),
            "write classification refuses before entering the bounded engine",
        );
        assert_resources(&harness.service, baseline);
    }

    let tx = harness.database.begin().expect("begin writer transaction");
    harness
        .database
        .execute_in_tx(
            tx,
            "INSERT INTO owner_visibility VALUES (1, 'uncommitted')",
            &HashMap::new(),
        )
        .expect("stage writer row without committing");
    let result = published_result(
        harness
            .request(
                2,
                limits(),
                query("SELECT id, payload FROM owner_visibility"),
            )
            .expect("owner read sees committed state only"),
    );
    assert!(result.rows.is_empty());
    assert_eq!(observer.records().len(), 1);
    harness
        .database
        .rollback(tx)
        .expect("discard writer fixture");
    assert_resources(&harness.service, baseline);
}

fn set_limit_field(limits: &mut ReadLimits, field: ReadLimitField, value: u64) {
    match field {
        ReadLimitField::ResultRows => limits.result_rows = value,
        ReadLimitField::ResultBytes => limits.result_bytes = value,
        ReadLimitField::Work => limits.work = value,
        ReadLimitField::ActiveMs => limits.active_ms = value,
        ReadLimitField::Memory => limits.memory = value,
        ReadLimitField::CursorPageRows => limits.cursor_page_rows = value,
        ReadLimitField::CursorPageBytes => limits.cursor_page_bytes = value,
        ReadLimitField::CursorIdleMs => limits.cursor_idle_ms = value,
        ReadLimitField::CursorLifetimeMs => limits.cursor_lifetime_ms = value,
        ReadLimitField::Concurrency => panic!("concurrency is not a caller ReadLimits field"),
    }
}

fn limit_field(limits: ReadLimits, field: ReadLimitField) -> u64 {
    match field {
        ReadLimitField::ResultRows => limits.result_rows,
        ReadLimitField::ResultBytes => limits.result_bytes,
        ReadLimitField::Work => limits.work,
        ReadLimitField::ActiveMs => limits.active_ms,
        ReadLimitField::Memory => limits.memory,
        ReadLimitField::CursorPageRows => limits.cursor_page_rows,
        ReadLimitField::CursorPageBytes => limits.cursor_page_bytes,
        ReadLimitField::CursorIdleMs => limits.cursor_idle_ms,
        ReadLimitField::CursorLifetimeMs => limits.cursor_lifetime_ms,
        ReadLimitField::Concurrency => panic!("concurrency is not a caller ReadLimits field"),
    }
}

fn intersection_values(field: ReadLimitField) -> (u64, u64) {
    match field {
        ReadLimitField::ResultRows => (200, 300),
        ReadLimitField::ResultBytes => (2 * 1024 * 1024, 3 * 1024 * 1024),
        ReadLimitField::Work => (20_000, 30_000),
        ReadLimitField::ActiveMs => (2_000, 3_000),
        ReadLimitField::Memory => (8 * 1024 * 1024, 12 * 1024 * 1024),
        ReadLimitField::CursorPageRows => (20, 30),
        ReadLimitField::CursorPageBytes => (256 * 1024, 512 * 1024),
        ReadLimitField::CursorIdleMs => (120_000, 180_000),
        ReadLimitField::CursorLifetimeMs => (900_000, 1_200_000),
        ReadLimitField::Concurrency => panic!("concurrency is not intersected per request"),
    }
}

fn prove_service_intersection(field: ReadLimitField, owner_value: u64, caller_value: u64) {
    let mut owner_limits = limits();
    set_limit_field(&mut owner_limits, field, owner_value);
    let mut caller_limits = limits();
    set_limit_field(&mut caller_limits, field, caller_value);
    let observer = Arc::new(ProductionObserver::default());
    let observer_seam: Arc<dyn OwnerBoundedExecutionObserver> = observer.clone();
    let harness = Harness::start(
        owner_config(
            owner_limits,
            4,
            OwnerServiceTimeouts {
                request_ms: 10_000,
                shutdown_drain_ms: 10_000,
            },
            None,
        ),
        LocalConfigurationSource::Override,
        ManualDeadlineClock::at(100),
        ReadClientTimeouts {
            connect_ms: 1_000,
            routing_retry_ms: 1_000,
            response_ms: 11_000,
        },
        Some(observer_seam),
        None,
    );
    harness
        .database
        .execute(
            "CREATE TABLE intersection_probe (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("create intersection fixture");
    harness
        .database
        .execute("INSERT INTO intersection_probe VALUES (1)", &HashMap::new())
        .expect("insert intersection fixture");
    let baseline = harness.service.resources();
    harness
        .request(1, caller_limits, query("SELECT id FROM intersection_probe"))
        .expect("intersection reaches the production bounded loop");
    let records = observer.records();
    let [record] = records.as_slice() else {
        panic!("one owner request must reach one production bounded operation");
    };
    assert_eq!(record.operation, OwnerBoundedOperation::Execute);
    assert_eq!(
        limit_field(record.limits, field),
        owner_value.min(caller_value),
    );
    assert!(
        record.operation_event < record.first_work_event.expect("real work was observed"),
        "stricter limits must exist before the first production work unit",
    );
    assert_resources(&harness.service, baseline);
}

#[test]
fn all_nine_limit_intersections_reach_owner_client_service_and_precede_real_work() {
    let fields = [
        ReadLimitField::ResultRows,
        ReadLimitField::ResultBytes,
        ReadLimitField::Work,
        ReadLimitField::ActiveMs,
        ReadLimitField::Memory,
        ReadLimitField::CursorPageRows,
        ReadLimitField::CursorPageBytes,
        ReadLimitField::CursorIdleMs,
        ReadLimitField::CursorLifetimeMs,
    ];
    for field in fields {
        let (lower, higher) = intersection_values(field);
        prove_service_intersection(field, lower, higher);
        prove_service_intersection(field, higher, lower);
    }
}

fn populate_limit_fixture(harness: &Harness) {
    harness
        .database
        .execute(
            "CREATE TABLE owner_limits (id INTEGER PRIMARY KEY, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create limit fixture");
    for id in 1..=3_i64 {
        harness
            .database
            .execute(
                "INSERT INTO owner_limits VALUES ($id, $payload)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    (
                        "payload".to_owned(),
                        Value::Text(format!("owner-limit-row-{id}")),
                    ),
                ]),
            )
            .expect("insert limit fixture row");
    }
}

#[test]
fn six_typed_request_limits_and_active_timeout_use_their_frozen_boundaries() {
    let observer = Arc::new(ProductionObserver::default());
    let observer_seam: Arc<dyn OwnerBoundedExecutionObserver> = observer.clone();
    let harness = Harness::start(
        owner_config(
            limits(),
            4,
            OwnerServiceTimeouts {
                request_ms: 10_000,
                shutdown_drain_ms: 10_000,
            },
            None,
        ),
        LocalConfigurationSource::Override,
        ManualDeadlineClock::at(100),
        ReadClientTimeouts {
            connect_ms: 1_000,
            routing_retry_ms: 1_000,
            response_ms: 11_000,
        },
        Some(observer_seam),
        None,
    );
    populate_limit_fixture(&harness);
    let sql = "SELECT id, payload FROM owner_limits ORDER BY id";
    let calibration = harness
        .database
        .execute(sql, &HashMap::new())
        .expect("calibrate canonical result from the same committed rows");
    let exact_rows = u64::try_from(calibration.rows.len()).expect("row count fits u64");
    let exact_bytes =
        u64::try_from(query_result_encoded_size(&calibration).expect("canonical result size"))
            .expect("canonical bytes fit u64");
    let baseline = harness.service.resources();

    observer.clear();
    harness
        .request(10, limits(), query(sql))
        .expect("calibration itself traverses OwnerClient and the service");
    let exact_work = observer.max_work();
    let exact_memory = observer.peak_temporary_bytes();
    assert!(exact_work > 1);
    assert!(exact_memory > 1);

    for (limit, exact, set) in [
        (
            ReadFailureLimit::ResultRows,
            exact_rows,
            (|value: &mut ReadLimits, exact| value.result_rows = exact) as fn(&mut ReadLimits, u64),
        ),
        (
            ReadFailureLimit::ResultBytes,
            exact_bytes,
            (|value: &mut ReadLimits, exact| value.result_bytes = exact)
                as fn(&mut ReadLimits, u64),
        ),
        (
            ReadFailureLimit::Work,
            exact_work,
            (|value: &mut ReadLimits, exact| value.work = exact) as fn(&mut ReadLimits, u64),
        ),
        (
            ReadFailureLimit::Memory,
            exact_memory,
            (|value: &mut ReadLimits, exact| value.memory = exact) as fn(&mut ReadLimits, u64),
        ),
    ] {
        let mut exact_limits = limits();
        set(&mut exact_limits, exact);
        keep_limit_relationships_valid(&mut exact_limits);
        harness
            .request(11, exact_limits, query(sql))
            .expect("the exact production charge boundary succeeds");
        assert_resources(&harness.service, baseline);

        let mut one_short = limits();
        set(&mut one_short, exact - 1);
        keep_limit_relationships_valid(&mut one_short);
        let failure = refusal(harness.request(12, one_short, query(sql)));
        assert_limit_refusal(&failure, limit, exact - 1);
        assert_resources(&harness.service, baseline);
    }

    let active_clock = ManualDeadlineClock::at(100);
    let active_observer = Arc::new(ProductionObserver::advancing(active_clock.clone()));
    let active_seam: Arc<dyn OwnerBoundedExecutionObserver> = active_observer.clone();
    let active = Harness::start(
        owner_config(
            limits(),
            4,
            OwnerServiceTimeouts {
                request_ms: 10_000,
                shutdown_drain_ms: 10_000,
            },
            None,
        ),
        LocalConfigurationSource::Override,
        active_clock,
        ReadClientTimeouts {
            connect_ms: 1_000,
            routing_retry_ms: 1_000,
            response_ms: 11_000,
        },
        Some(active_seam),
        None,
    );
    populate_limit_fixture(&active);
    let active_baseline = active.service.resources();
    let mut exact_active = limits();
    exact_active.active_ms = exact_work;
    active
        .request(13, exact_active, query(sql))
        .expect("active_ms succeeds when work completes exactly at the boundary");
    assert_resources(&active.service, active_baseline);
    let mut one_over_active = limits();
    one_over_active.active_ms = exact_work - 1;
    let timed_out = refusal(active.request(14, one_over_active, query(sql)));
    assert_simple_refusal(&timed_out, ReadFailureKind::OwnerTimeout);
    assert_resources(&active.service, active_baseline);
}

#[test]
fn metadata_is_continuation_aware_custom_namespaces_coexist_and_oversize_never_partially_publishes()
{
    let handler = GatedHandler::default();
    let harness = Harness::new(limits(), 4, Some(Arc::new(handler.clone())));
    harness
        .database
        .execute(
            "CREATE TABLE owner_metadata (id INTEGER PRIMARY KEY, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create metadata fixture");
    let mut expected_tables = BTreeSet::from(["owner_metadata".to_owned()]);
    for suffix in 0..8 {
        let table =
            format!("owner_metadata_continuation_{suffix}_with_a_deliberately_long_stable_name");
        harness
            .database
            .execute(
                &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY)"),
                &HashMap::new(),
            )
            .expect("create enough table metadata to require continuation");
        expected_tables.insert(table);
    }
    let baseline = harness.service.resources();

    let mut paged_limits = limits();
    paged_limits.result_bytes = 256;
    paged_limits.cursor_page_bytes = 256;
    let mut continuation = None;
    let mut observed_tables = BTreeSet::new();
    for page_number in 0..32 {
        let payload = metadata_payload(
            harness
                .request(
                    20,
                    paged_limits,
                    LocalRequest::Metadata {
                        request: LocalMetadataRequest::Tables {
                            continuation: continuation.clone(),
                        },
                    },
                )
                .expect("issued table continuation returns one complete page"),
        );
        let page = decode_metadata_page(&payload).expect("decode canonical table metadata page");
        assert_eq!(page.vocabulary, MetadataPageVocabulary::Tables);
        for item in page.items {
            let MetadataItem::Table(table) = item else {
                panic!("table metadata page contains only table items");
            };
            assert!(
                observed_tables.insert(table),
                "stable continuation never duplicates a table"
            );
        }
        assert_resources(&harness.service, baseline);
        if page.has_more {
            continuation = Some(
                page.continuation
                    .expect("a resumable metadata page carries its issued continuation"),
            );
            assert!(
                page_number < 31,
                "table metadata must terminate deterministically"
            );
        } else {
            assert!(page.continuation.is_none());
            continuation = None;
            break;
        }
    }
    assert!(
        continuation.is_none(),
        "table metadata reaches has_more false"
    );
    assert!(expected_tables.is_subset(&observed_tables));

    let mut event_continuation = None;
    for page_number in 0..32 {
        let payload = metadata_payload(
            harness
                .request(
                    20,
                    paged_limits,
                    LocalRequest::Metadata {
                        request: LocalMetadataRequest::EventsStatus {
                            continuation: event_continuation.clone(),
                        },
                    },
                )
                .expect("issued event-status continuation returns one complete page"),
        );
        let page = decode_metadata_page(&payload).expect("decode event-status metadata page");
        assert_eq!(page.vocabulary, MetadataPageVocabulary::EventsStatus);
        assert!(
            page.items
                .iter()
                .all(|item| matches!(item, MetadataItem::EventStatus(_))),
        );
        if page.has_more {
            event_continuation = Some(
                page.continuation
                    .expect("a resumable event page carries its issued continuation"),
            );
            assert!(
                page_number < 31,
                "event metadata must terminate deterministically"
            );
        } else {
            assert!(page.continuation.is_none());
            event_continuation = None;
            break;
        }
    }
    assert!(event_continuation.is_none());
    assert_resources(&harness.service, baseline);

    let metadata_requests = [
        LocalMetadataRequest::Schema {
            table: "owner_metadata".to_owned(),
        },
        LocalMetadataRequest::MaintenanceStatus,
    ];
    for request in metadata_requests {
        let responses = harness
            .request(20, limits(), LocalRequest::Metadata { request })
            .expect("metadata publishes one complete bounded response");
        assert!(matches!(
            responses.as_slice(),
            [LocalResponse::Metadata { .. }]
        ));
        assert_resources(&harness.service, baseline);
    }

    let custom_response = harness
        .request(21, limits(), custom("proof.echo", b"custom-owner-payload"))
        .expect("registered custom namespace works beside metadata and SQL");
    assert!(matches!(
        custom_response.as_slice(),
        [LocalResponse::Custom { payload }] if payload == b"custom-owner-payload"
    ));
    assert_eq!(handler.invocations(), 1);
    assert_resources(&harness.service, baseline);

    let handler_error = harness
        .request(23, limits(), custom("proof.fail", b"never-published"))
        .expect_err("custom handler failure remains terminal and typed");
    assert!(matches!(
        handler_error,
        OwnerReadScaffoldError::Database(Error::Other(message))
            if message == "owner custom handler fixture failed"
    ));
    assert_resources(&harness.service, baseline);

    let mut one_byte = limits();
    one_byte.result_bytes = 1;
    one_byte.cursor_page_bytes = 1;
    let oversized = refusal(harness.request(
        22,
        one_byte,
        LocalRequest::Metadata {
            request: LocalMetadataRequest::Schema {
                table: "owner_metadata".to_owned(),
            },
        },
    ));
    assert_eq!(oversized.kind(), ReadFailureKind::OwnerLimitExceeded);
    let ReadFailureDetail::OwnerLimitExceeded(OwnerLimitExceededDetail {
        limit: ReadFailureLimit::ResultBytes,
        value: 1,
        required:
            Some(RequiredBytesSetting {
                required_bytes,
                required_setting,
            }),
        statement: None,
    }) = oversized.detail()
    else {
        panic!("oversized single metadata payload needs actionable byte detail");
    };
    assert!(*required_bytes > 1);
    assert_eq!(
        required_setting,
        &format!("effective result_bytes >= {required_bytes}")
    );
    assert_resources(&harness.service, baseline);
}

#[test]
fn custom_namespace_against_a_handler_less_owner_answers_a_named_owner_route_unsupported_refusal() {
    let harness = Harness::new(limits(), 4, None);
    let failure = refusal(harness.request(
        30,
        limits(),
        custom("proof.unregistered", b"no handler is configured"),
    ));
    assert_eq!(failure.kind(), ReadFailureKind::OwnerRouteUnsupported);
    assert_eq!(
        failure.detail(),
        &ReadFailureDetail::OwnerRouteUnsupported(OwnerRouteUnsupportedDetail {
            inspection: "proof.unregistered".to_owned(),
        })
    );
}

#[test]
fn cursor_page_rows_bytes_identity_exhaustion_close_and_expiry_follow_the_frozen_contract() {
    let mut owner_limits = limits();
    owner_limits.result_rows = 10;
    owner_limits.cursor_page_rows = 2;
    owner_limits.cursor_idle_ms = 10;
    owner_limits.cursor_lifetime_ms = 100;
    let harness = Harness::new(owner_limits, 4, None);
    harness
        .database
        .execute(
            "CREATE TABLE owner_cursor (id INTEGER PRIMARY KEY, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create cursor fixture");
    for id in 1..=3_i64 {
        harness
            .database
            .execute(
                "INSERT INTO owner_cursor VALUES ($id, $payload)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    (
                        "payload".to_owned(),
                        Value::Text(format!("cursor-row-{id}")),
                    ),
                ]),
            )
            .expect("insert cursor fixture row");
    }
    let baseline = harness.service.resources();
    let sql = "SELECT id, payload FROM owner_cursor ORDER BY id";
    let mut original_cursor_limits = owner_limits;
    original_cursor_limits.result_rows = 3;
    original_cursor_limits.cursor_page_rows = 3;
    let opened = cursor_opened(
        harness
            .request(
                30,
                original_cursor_limits,
                LocalRequest::CursorOpen {
                    statement: sql.to_owned(),
                    params: BTreeMap::new(),
                },
            )
            .expect("cursor open returns its first complete page"),
    );
    let first = decode_cursor_page(&opened.payload).expect("decode first cursor page");
    assert_eq!(
        first.rows.len(),
        2,
        "the owner's lower cursor_page_rows clamps the default page"
    );
    assert!(first.has_more);
    let full_two_row_bytes =
        u64::try_from(cursor_page_encoded_size(&first).expect("canonical two-row page size"))
            .expect("cursor page size fits u64");
    let live = harness.service.resources();
    assert_eq!(live.active_slots, baseline.active_slots + 1);
    assert_eq!(live.cursor_count, baseline.cursor_count + 1);
    assert!(
        live.cursor_retained_bytes > 0,
        "a live cursor has a strict independently visible retained charge",
    );
    assert_eq!(
        live.accountant_used_bytes - baseline.accountant_used_bytes,
        live.cursor_retained_bytes,
    );
    let live_status = owner_status(
        harness
            .request(42, owner_limits, LocalRequest::OwnerStatus)
            .expect("owner status exposes live cursor memory"),
    );
    assert_eq!(live_status.memory.used_bytes, live.accountant_used_bytes);

    let mut caller_lower_page_rows = owner_limits;
    caller_lower_page_rows.cursor_page_rows = 1;
    let caller_shaped = cursor_opened(
        harness
            .request(
                41,
                caller_lower_page_rows,
                LocalRequest::CursorOpen {
                    statement: sql.to_owned(),
                    params: BTreeMap::new(),
                },
            )
            .expect("the caller may lower the owner's default cursor page rows"),
    );
    let caller_shaped_page =
        decode_cursor_page(&caller_shaped.payload).expect("decode caller-shaped cursor page");
    assert_eq!(caller_shaped_page.rows.len(), 1);
    assert!(caller_shaped_page.has_more);
    harness
        .request(
            41,
            caller_lower_page_rows,
            LocalRequest::CursorClose {
                cursor_id: caller_shaped.cursor_id,
            },
        )
        .expect("close the caller-shaped cursor");
    assert_eq!(harness.service.resources(), live);

    let mut exact_page_shape_limits = owner_limits;
    exact_page_shape_limits.cursor_page_bytes = full_two_row_bytes;
    let exact_page_shape = cursor_opened(
        harness
            .request(
                39,
                exact_page_shape_limits,
                LocalRequest::CursorOpen {
                    statement: sql.to_owned(),
                    params: BTreeMap::new(),
                },
            )
            .expect("the exact complete two-row page byte boundary succeeds"),
    );
    let exact_page =
        decode_cursor_page(&exact_page_shape.payload).expect("decode exact-byte cursor page");
    assert_eq!(exact_page.rows.len(), 2);
    harness
        .request(
            39,
            exact_page_shape_limits,
            LocalRequest::CursorClose {
                cursor_id: exact_page_shape.cursor_id,
            },
        )
        .expect("close the exact-byte proof cursor");
    assert_eq!(harness.service.resources(), live);

    let mut byte_stopped_limits = owner_limits;
    byte_stopped_limits.cursor_page_bytes = full_two_row_bytes - 1;
    let byte_stopped = cursor_opened(
        harness
            .request(
                38,
                byte_stopped_limits,
                LocalRequest::CursorOpen {
                    statement: sql.to_owned(),
                    params: BTreeMap::new(),
                },
            )
            .expect("page bytes stop at the last complete row instead of refusing"),
    );
    let byte_stopped_page =
        decode_cursor_page(&byte_stopped.payload).expect("decode byte-stopped cursor page");
    assert_eq!(byte_stopped_page.rows.len(), 1);
    assert!(byte_stopped_page.has_more);
    harness
        .request(
            38,
            byte_stopped_limits,
            LocalRequest::CursorClose {
                cursor_id: byte_stopped.cursor_id,
            },
        )
        .expect("close the byte-stopped proof cursor");
    assert_eq!(harness.service.resources(), live);

    for (connection, cursor_id) in [(31, opened.cursor_id), (30, [0x99; 16])] {
        let missing = refusal(harness.request(
            connection,
            owner_limits,
            LocalRequest::CursorFetch {
                cursor_id,
                rows: None,
            },
        ));
        assert_simple_refusal(&missing, ReadFailureKind::CursorNotFound);
        assert_eq!(harness.service.resources(), live);
    }

    let exact_explicit = cursor_opened(
        harness
            .request(
                45,
                original_cursor_limits,
                LocalRequest::CursorOpen {
                    statement: sql.to_owned(),
                    params: BTreeMap::new(),
                },
            )
            .expect("open a cursor for the exact explicit-fetch row boundary"),
    );
    let exact_explicit_page = cursor_page(
        harness
            .request(
                45,
                owner_limits,
                LocalRequest::CursorFetch {
                    cursor_id: exact_explicit.cursor_id,
                    rows: fetch_rows(original_cursor_limits.result_rows),
                },
            )
            .expect("an explicit fetch equal to result_rows succeeds"),
    );
    assert!(
        !decode_cursor_page(&exact_explicit_page.payload)
            .expect("decode exact explicit-fetch page")
            .has_more,
    );
    assert_eq!(harness.service.resources(), live);

    // An explicit count above the effective `result_rows` is a request-shape
    // validation refusal, not the end of the read: it is answered
    // before anything is pulled, so no row and no continuation
    // position is consumed. The owner therefore keeps the cursor registered
    // with its accounting untouched, and the caller pages on from exactly
    // where it left off. Terminal refusals still release everything -- the
    // expiry, exhaustion, and explicit-close legs around this one hold that.
    let too_many_rows = refusal(harness.request(
        30,
        owner_limits,
        LocalRequest::CursorFetch {
            cursor_id: opened.cursor_id,
            rows: fetch_rows(original_cursor_limits.result_rows + 1),
        },
    ));
    assert_limit_refusal(
        &too_many_rows,
        ReadFailureLimit::ResultRows,
        original_cursor_limits.result_rows,
    );
    assert_resources(&harness.service, live);
    let resumed = cursor_page(
        harness
            .request(
                30,
                owner_limits,
                LocalRequest::CursorFetch {
                    cursor_id: opened.cursor_id,
                    rows: None,
                },
            )
            .expect("the refused request consumed nothing, so the cursor pages on"),
    );
    let resumed = decode_cursor_page(&resumed.payload).expect("decode the page after the refusal");
    assert_eq!(
        resumed.rows,
        vec![vec![
            Value::Int64(3),
            Value::Text("cursor-row-3".to_owned())
        ]],
        "the page after the refusal is the next row, with nothing skipped and nothing repeated"
    );
    assert!(!resumed.has_more);
    assert_resources(&harness.service, baseline);

    let exhaustion = cursor_opened(
        harness
            .request(
                43,
                original_cursor_limits,
                LocalRequest::CursorOpen {
                    statement: sql.to_owned(),
                    params: BTreeMap::new(),
                },
            )
            .expect("open a separate cursor for genuine exhaustion"),
    );

    let final_page = cursor_page(
        harness
            .request(
                43,
                owner_limits,
                LocalRequest::CursorFetch {
                    cursor_id: exhaustion.cursor_id,
                    rows: None,
                },
            )
            .expect("owner cursor fetch resumes the retained bounded cursor"),
    );
    let final_page = decode_cursor_page(&final_page.payload).expect("decode final cursor page");
    assert_eq!(final_page.rows.len(), 1);
    assert!(!final_page.has_more);
    assert_resources(&harness.service, baseline);

    let past_end = cursor_page(
        harness
            .request(
                43,
                owner_limits,
                LocalRequest::CursorFetch {
                    cursor_id: exhaustion.cursor_id,
                    rows: None,
                },
            )
            .expect("fetch after exhaustion is one empty success page"),
    );
    let past_end = decode_cursor_page(&past_end.payload).expect("decode exhausted cursor page");
    assert!(past_end.rows.is_empty());
    assert!(!past_end.has_more);
    let close_after_end = harness
        .request(
            43,
            owner_limits,
            LocalRequest::CursorClose {
                cursor_id: exhaustion.cursor_id,
            },
        )
        .expect("close after exhaustion is a no-op success");
    let [LocalResponse::CursorClosed { acknowledgement }] = close_after_end.as_slice() else {
        panic!("cursor close returns one acknowledgement");
    };
    assert_eq!(
        acknowledgement,
        &contextdb_engine::local_transport::CursorCloseAcknowledgement { closed: true }
    );
    assert_resources(&harness.service, baseline);

    let explicitly_closed = cursor_opened(
        harness
            .request(
                36,
                owner_limits,
                LocalRequest::CursorOpen {
                    statement: sql.to_owned(),
                    params: BTreeMap::new(),
                },
            )
            .expect("open cursor for explicit-close identity proof"),
    );
    for (connection, cursor_id) in [(37, explicitly_closed.cursor_id), (36, [0x98; 16])] {
        let failure = refusal(harness.request(
            connection,
            owner_limits,
            LocalRequest::CursorClose { cursor_id },
        ));
        assert_simple_refusal(&failure, ReadFailureKind::CursorNotFound);
    }
    harness
        .request(
            36,
            owner_limits,
            LocalRequest::CursorClose {
                cursor_id: explicitly_closed.cursor_id,
            },
        )
        .expect("owner connection explicitly closes its cursor");
    for request in [
        LocalRequest::CursorFetch {
            cursor_id: explicitly_closed.cursor_id,
            rows: None,
        },
        LocalRequest::CursorClose {
            cursor_id: explicitly_closed.cursor_id,
        },
    ] {
        let reused = refusal(harness.request(36, owner_limits, request));
        assert_simple_refusal(&reused, ReadFailureKind::CursorNotFound);
    }
    assert_resources(&harness.service, baseline);

    let single_row_sql = "SELECT id, payload FROM owner_cursor WHERE id = 1";
    let calibration = cursor_opened(
        harness
            .request(
                44,
                owner_limits,
                LocalRequest::CursorOpen {
                    statement: single_row_sql.to_owned(),
                    params: BTreeMap::new(),
                },
            )
            .expect("calibration traverses OwnerClient and the service cursor open"),
    );
    let calibration_page =
        decode_cursor_page(&calibration.payload).expect("decode calibration cursor page");
    let required_page_bytes = u64::try_from(
        cursor_page_encoded_size(&calibration_page).expect("canonical cursor page encoded size"),
    )
    .expect("cursor page size fits u64");
    assert!(!calibration_page.has_more);
    assert_resources(&harness.service, baseline);

    let mut exact_page_bytes = owner_limits;
    exact_page_bytes.cursor_page_bytes = required_page_bytes;
    harness
        .request(
            32,
            exact_page_bytes,
            LocalRequest::CursorOpen {
                statement: single_row_sql.to_owned(),
                params: BTreeMap::new(),
            },
        )
        .expect("an exact complete-row page byte boundary succeeds");
    assert_resources(&harness.service, baseline);

    let mut one_short_page = owner_limits;
    assert!(required_page_bytes > 0);
    one_short_page.cursor_page_bytes = required_page_bytes - 1;
    let too_wide = refusal(harness.request(
        33,
        one_short_page,
        LocalRequest::CursorOpen {
            statement: single_row_sql.to_owned(),
            params: BTreeMap::new(),
        },
    ));
    assert_limit_refusal(
        &too_wide,
        ReadFailureLimit::CursorPageBytes,
        required_page_bytes - 1,
    );
    let ReadFailureDetail::OwnerLimitExceeded(detail) = too_wide.detail() else {
        unreachable!();
    };
    assert_eq!(
        detail.required,
        Some(RequiredBytesSetting {
            required_bytes: required_page_bytes,
            required_setting: format!("effective cursor_page_bytes >= {required_page_bytes}"),
        })
    );
    let remedy = detail
        .statement
        .as_ref()
        .expect("an oversized single cursor row carries an actionable remedy");
    assert_eq!(remedy.statement, single_row_sql);
    assert_eq!(remedy.remedy_command, "select fewer columns");
    assert_resources(&harness.service, baseline);

    for id in 4..=30_i64 {
        harness
            .database
            .execute(
                "INSERT INTO owner_cursor VALUES ($id, $payload)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    (
                        "payload".to_owned(),
                        Value::Text(format!("cursor-row-{id}")),
                    ),
                ]),
            )
            .expect("extend cursor fixture for lifetime proof");
    }
    let expanded_baseline = harness.service.resources();
    assert_eq!(expanded_baseline.active_slots, baseline.active_slots);
    assert_eq!(expanded_baseline.cursor_count, baseline.cursor_count);
    assert_eq!(
        expanded_baseline.cursor_retained_bytes,
        baseline.cursor_retained_bytes,
    );
    assert_eq!(expanded_baseline.buffered_bytes, baseline.buffered_bytes);
    assert_eq!(
        expanded_baseline.active_cancellations,
        baseline.active_cancellations,
    );

    let idle = cursor_opened(
        harness
            .request(
                34,
                owner_limits,
                LocalRequest::CursorOpen {
                    statement: sql.to_owned(),
                    params: BTreeMap::new(),
                },
            )
            .expect("open idle-boundary cursor"),
    );
    harness.clock.advance_to(110);
    harness
        .request(
            34,
            owner_limits,
            LocalRequest::CursorFetch {
                cursor_id: idle.cursor_id,
                rows: fetch_rows(1),
            },
        )
        .expect("exact idle boundary succeeds");
    harness.clock.advance_to(121);
    let expired = refusal(harness.request(
        34,
        owner_limits,
        LocalRequest::CursorFetch {
            cursor_id: idle.cursor_id,
            rows: fetch_rows(1),
        },
    ));
    assert_eq!(expired, ReadFailure::cursor_expired(CursorExpiryKind::Idle));
    assert_resources(&harness.service, expanded_baseline);
    let reused_idle = refusal(harness.request(
        34,
        owner_limits,
        LocalRequest::CursorFetch {
            cursor_id: idle.cursor_id,
            rows: fetch_rows(1),
        },
    ));
    assert_simple_refusal(&reused_idle, ReadFailureKind::CursorNotFound);
    assert_resources(&harness.service, expanded_baseline);

    let lifetime = cursor_opened(
        harness
            .request(
                35,
                owner_limits,
                LocalRequest::CursorOpen {
                    statement: sql.to_owned(),
                    params: BTreeMap::new(),
                },
            )
            .expect("open lifetime-boundary cursor"),
    );
    for now_ms in (130..=220).step_by(9) {
        harness.clock.advance_to(now_ms);
        harness
            .request(
                35,
                owner_limits,
                LocalRequest::CursorFetch {
                    cursor_id: lifetime.cursor_id,
                    rows: fetch_rows(1),
                },
            )
            .expect("periodic fetch keeps idle time below its boundary");
    }
    harness.clock.advance_to(221);
    harness
        .request(
            35,
            owner_limits,
            LocalRequest::CursorFetch {
                cursor_id: lifetime.cursor_id,
                rows: fetch_rows(1),
            },
        )
        .expect("exact total lifetime boundary succeeds");
    harness.clock.advance_to(222);
    let expired = refusal(harness.request(
        35,
        owner_limits,
        LocalRequest::CursorFetch {
            cursor_id: lifetime.cursor_id,
            rows: fetch_rows(1),
        },
    ));
    assert_eq!(
        expired,
        ReadFailure::cursor_expired(CursorExpiryKind::Lifetime)
    );
    assert_resources(&harness.service, expanded_baseline);
}

fn observed_cursor_harness(observer: Arc<ProductionObserver>) -> Harness {
    let mut cursor_limits = limits();
    cursor_limits.result_rows = 4;
    cursor_limits.cursor_page_rows = 1;
    let observer_seam: Arc<dyn OwnerBoundedExecutionObserver> = observer;
    let harness = Harness::start(
        owner_config(
            cursor_limits,
            4,
            OwnerServiceTimeouts {
                request_ms: 10_000,
                shutdown_drain_ms: 10_000,
            },
            None,
        ),
        LocalConfigurationSource::Override,
        ManualDeadlineClock::at(100),
        ReadClientTimeouts {
            connect_ms: 1_000,
            routing_retry_ms: 1_000,
            response_ms: 11_000,
        },
        Some(observer_seam),
        None,
    );
    harness
        .database
        .execute(
            "CREATE TABLE cancellation_cursor (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("create cancellation fixture");
    for id in 1..=4_i64 {
        harness
            .database
            .execute(
                "INSERT INTO cancellation_cursor VALUES ($id)",
                &HashMap::from([("id".to_owned(), Value::Int64(id))]),
            )
            .expect("insert cancellation fixture");
    }
    harness
}

fn prove_disconnect_reaches_production_blocker(operation: OwnerBoundedOperation) {
    let observer = Arc::new(ProductionObserver::default());
    let harness = observed_cursor_harness(Arc::clone(&observer));
    let request_limits = {
        let mut value = limits();
        value.result_rows = 4;
        value.cursor_page_rows = 1;
        value
    };
    let baseline = harness.service.resources();
    let connection = match operation {
        OwnerBoundedOperation::Execute => 70,
        OwnerBoundedOperation::CursorOpen => 71,
        OwnerBoundedOperation::CursorFetch => 72,
    };
    let request = match operation {
        OwnerBoundedOperation::Execute => query("SELECT id FROM cancellation_cursor ORDER BY id"),
        OwnerBoundedOperation::CursorOpen => LocalRequest::CursorOpen {
            statement: "SELECT id FROM cancellation_cursor ORDER BY id".to_owned(),
            params: BTreeMap::new(),
        },
        OwnerBoundedOperation::CursorFetch => {
            let opened = cursor_opened(
                harness
                    .request(
                        connection,
                        request_limits,
                        LocalRequest::CursorOpen {
                            statement: "SELECT id FROM cancellation_cursor ORDER BY id".to_owned(),
                            params: BTreeMap::new(),
                        },
                    )
                    .expect("open cursor before blocking its fetch"),
            );
            observer.clear();
            LocalRequest::CursorFetch {
                cursor_id: opened.cursor_id,
                rows: None,
            }
        }
    };
    observer.arm_block(operation);
    let client = harness.client(connection);
    let disconnect = client
        .lock()
        .expect("selected blocker client")
        .disconnect_handle()
        .expect("clone selected blocker carrier");
    let worker_client = Arc::clone(&client);
    let worker = std::thread::spawn(move || {
        block_on(
            worker_client
                .lock()
                .expect("blocked production client")
                .request(LocalRequestEnvelope {
                    limits: request_limits,
                    request,
                }),
        )
    });
    observer.wait_until_blocked();
    disconnect
        .disconnect()
        .expect("EOF/HUP reaches the blocked production operation");
    observer.wait_for_disconnect_signal(1);
    assert!(observer.current_is_cancelled());
    observer.release_block();
    let failure = worker
        .join()
        .expect("blocked production request joins")
        .expect_err("disconnect cannot publish a production result");
    assert!(matches!(
        failure,
        OwnerReadScaffoldError::Refused(failure)
            if failure.kind() == ReadFailureKind::OwnerDisconnected
    ));
    assert_eq!(observer.cancellation_observed(), 1);
    assert_eq!(harness.service.cancellation_signal_count(), 1);
    assert_resources(&harness.service, baseline);
}

#[test]
fn eof_hup_reaches_execute_open_and_fetch_production_blockers() {
    for operation in [
        OwnerBoundedOperation::Execute,
        OwnerBoundedOperation::CursorOpen,
        OwnerBoundedOperation::CursorFetch,
    ] {
        prove_disconnect_reaches_production_blocker(operation);
    }
}

#[test]
fn a_cancelled_fetch_is_nonterminal_and_resumes_original_state() {
    let observer = Arc::new(ProductionObserver::default());
    let harness = observed_cursor_harness(Arc::clone(&observer));
    let mut original_limits = limits();
    original_limits.result_rows = 4;
    original_limits.work = 1_234;
    original_limits.cursor_page_rows = 1;
    let baseline = harness.service.resources();
    let opened = cursor_opened(
        harness
            .request(
                75,
                original_limits,
                LocalRequest::CursorOpen {
                    statement: "SELECT id FROM cancellation_cursor ORDER BY id".to_owned(),
                    params: BTreeMap::new(),
                },
            )
            .expect("open cursor before statement cancellation"),
    );
    let live = harness.service.resources();
    assert_eq!(live.active_slots, baseline.active_slots + 1);
    assert_eq!(live.cursor_count, baseline.cursor_count + 1);
    assert!(live.cursor_retained_bytes > 0);
    assert_eq!(
        live.accountant_used_bytes - baseline.accountant_used_bytes,
        live.cursor_retained_bytes,
    );
    observer.clear();
    observer.arm_block(OwnerBoundedOperation::CursorFetch);
    let client = harness.client(75);
    let worker_client = Arc::clone(&client);
    let cursor_id = opened.cursor_id;
    let cancelled = std::thread::spawn(move || {
        block_on(
            worker_client
                .lock()
                .expect("cancelled fetch client")
                .request(LocalRequestEnvelope {
                    limits: limits(),
                    request: LocalRequest::CursorFetch {
                        cursor_id,
                        rows: None,
                    },
                }),
        )
    });
    observer.wait_until_blocked();
    let during_fetch = harness.service.resources();
    assert_eq!(during_fetch.active_slots, live.active_slots);
    assert_eq!(during_fetch.cursor_count, live.cursor_count);
    assert_eq!(
        during_fetch.cursor_retained_bytes,
        live.cursor_retained_bytes
    );
    assert_eq!(
        during_fetch.accountant_used_bytes,
        live.accountant_used_bytes
    );
    assert_eq!(during_fetch.active_cancellations, 1);
    let status = owner_status(
        harness
            .request(76, limits(), LocalRequest::OwnerStatus)
            .expect("status bypasses admission during a blocked cursor fetch"),
    );
    assert_eq!(status.admission.active_readers, 1);
    assert_eq!(status.memory.used_bytes, live.accountant_used_bytes);
    observer.cancel_current();
    assert!(observer.current_is_cancelled());
    observer.release_block();
    assert!(matches!(
        cancelled
            .join()
            .expect("cancelled fetch joins")
            .expect_err("cancelled fetch publishes no page"),
        OwnerReadScaffoldError::Database(Error::ReadCancelled)
    ));
    assert_eq!(observer.cancellation_observed(), 1);
    assert_resources(&harness.service, live);

    harness
        .database
        .execute(
            "INSERT INTO cancellation_cursor VALUES (5)",
            &HashMap::new(),
        )
        .expect("commit a later row between cancellation and resume");
    let live_after_later_commit = harness.service.resources();
    assert_eq!(live_after_later_commit.active_slots, live.active_slots);
    assert_eq!(live_after_later_commit.cursor_count, live.cursor_count);
    assert_eq!(
        live_after_later_commit.cursor_retained_bytes,
        live.cursor_retained_bytes,
    );
    assert_eq!(live_after_later_commit.buffered_bytes, live.buffered_bytes);
    assert_eq!(
        live_after_later_commit.active_cancellations,
        live.active_cancellations,
    );

    observer.clear();
    let resumed = cursor_page(
        harness
            .request(
                75,
                limits(),
                LocalRequest::CursorFetch {
                    cursor_id: opened.cursor_id,
                    rows: None,
                },
            )
            .expect("a statement-cancelled fetch resumes the same cursor"),
    );
    let resumed = decode_cursor_page(&resumed.payload).expect("decode resumed cursor page");
    assert_eq!(resumed.rows, vec![vec![Value::Int64(2)]]);
    assert!(resumed.has_more);
    let records = observer.records();
    let [record] = records.as_slice() else {
        panic!("resumed fetch reaches one production bounded operation");
    };
    assert_eq!(record.operation, OwnerBoundedOperation::CursorFetch);
    assert_eq!(record.limits, original_limits);
    let after_resume = assert_resources_draining(&harness.service, live_after_later_commit);

    let third = cursor_page(
        harness
            .request(
                75,
                limits(),
                LocalRequest::CursorFetch {
                    cursor_id: opened.cursor_id,
                    rows: None,
                },
            )
            .expect("the resumed snapshot advances to its third original row"),
    );
    let third = decode_cursor_page(&third.payload).expect("decode third snapshot page");
    assert_eq!(third.rows, vec![vec![Value::Int64(3)]]);
    assert!(third.has_more);
    assert_resources_draining(&harness.service, after_resume);

    let final_page = cursor_page(
        harness
            .request(
                75,
                limits(),
                LocalRequest::CursorFetch {
                    cursor_id: opened.cursor_id,
                    rows: None,
                },
            )
            .expect("the resumed snapshot exhausts at its fourth original row"),
    );
    let final_page = decode_cursor_page(&final_page.payload).expect("decode final snapshot page");
    assert_eq!(final_page.rows, vec![vec![Value::Int64(4)]]);
    assert!(!final_page.has_more);
    assert!(
        observer
            .records()
            .iter()
            .all(|record| record.limits == original_limits),
        "every resumed fetch retains the limits frozen at cursor open",
    );
    let expected_after_exhaustion = OwnerReadResourceSnapshot {
        active_slots: live_after_later_commit.active_slots - 1,
        cursor_count: live_after_later_commit.cursor_count - 1,
        cursor_retained_bytes: live_after_later_commit.cursor_retained_bytes
            - live.cursor_retained_bytes,
        buffered_bytes: live_after_later_commit.buffered_bytes,
        active_cancellations: live_after_later_commit.active_cancellations,
        accountant_used_bytes: live_after_later_commit.accountant_used_bytes
            - live.cursor_retained_bytes,
    };
    assert_resources(&harness.service, expected_after_exhaustion);
    let current = harness
        .database
        .execute(
            "SELECT id FROM cancellation_cursor ORDER BY id",
            &HashMap::new(),
        )
        .expect("ordinary database reads include the commit made after cursor open");
    assert_eq!(current.rows.len(), 5);
}

struct ReusedCursorIdentifier([u8; 16]);

impl CursorIdentifierAllocator for ReusedCursorIdentifier {
    fn allocate(
        &self,
        _writer_run: [u8; 16],
    ) -> std::result::Result<[u8; 16], OwnerReadScaffoldError> {
        Ok(self.0)
    }
}

#[test]
fn actual_identifier_reuse_after_every_release_path_has_no_tombstone_or_resource_leak() {
    let cursor_id = [0x5a; 16];
    let allocator: Arc<dyn CursorIdentifierAllocator> = Arc::new(ReusedCursorIdentifier(cursor_id));
    let mut cursor_limits = limits();
    cursor_limits.result_rows = 2;
    cursor_limits.cursor_page_rows = 1;
    let harness = Harness::start(
        owner_config(
            cursor_limits,
            4,
            OwnerServiceTimeouts {
                request_ms: 10_000,
                shutdown_drain_ms: 10_000,
            },
            None,
        ),
        LocalConfigurationSource::Override,
        ManualDeadlineClock::at(100),
        ReadClientTimeouts {
            connect_ms: 1_000,
            routing_retry_ms: 1_000,
            response_ms: 11_000,
        },
        None,
        Some(allocator),
    );
    harness
        .database
        .execute(
            "CREATE TABLE reused_cursor (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("create cursor reuse fixture");
    for id in 1..=3_i64 {
        harness
            .database
            .execute(
                "INSERT INTO reused_cursor VALUES ($id)",
                &HashMap::from([("id".to_owned(), Value::Int64(id))]),
            )
            .expect("insert cursor reuse fixture");
    }
    let baseline = harness.service.resources();

    let exhausted = cursor_opened(
        harness
            .request(
                60,
                cursor_limits,
                LocalRequest::CursorOpen {
                    statement: "SELECT id FROM reused_cursor WHERE id = 1".to_owned(),
                    params: BTreeMap::new(),
                },
            )
            .expect("single-row cursor exhausts on its first page"),
    );
    assert_eq!(exhausted.cursor_id, cursor_id);
    assert!(
        !decode_cursor_page(&exhausted.payload)
            .expect("decode exhausted page")
            .has_more
    );
    assert_resources(&harness.service, baseline);

    let after_exhaustion = cursor_opened(
        harness
            .request(
                61,
                cursor_limits,
                LocalRequest::CursorOpen {
                    statement: "SELECT id FROM reused_cursor ORDER BY id".to_owned(),
                    params: BTreeMap::new(),
                },
            )
            .expect("a new cursor reuses the exhaustion-drained identifier"),
    );
    assert_eq!(after_exhaustion.cursor_id, cursor_id);
    let first = decode_cursor_page(&after_exhaustion.payload).expect("decode reused first page");
    assert_eq!(first.rows, vec![vec![Value::Int64(1)]]);
    let live = harness.service.resources();
    assert_eq!(live.active_slots, 1);
    assert_eq!(live.cursor_count, 1);
    assert!(live.cursor_retained_bytes > 0);
    for request in [
        LocalRequest::CursorFetch {
            cursor_id,
            rows: None,
        },
        LocalRequest::CursorClose { cursor_id },
    ] {
        let stale_owner = refusal(harness.request(60, cursor_limits, request));
        assert_simple_refusal(&stale_owner, ReadFailureKind::CursorNotFound);
        assert_resources(&harness.service, live);
    }

    // An over-count fetch does NOT free the identifier: it is a request-shape
    // validation refusal that keeps the cursor and its accounting exactly as
    // they were, so the identifier is still taken and an explicit close is
    // what releases it.
    let fetch_refusal = refusal(harness.request(
        61,
        cursor_limits,
        LocalRequest::CursorFetch {
            cursor_id,
            rows: fetch_rows(cursor_limits.result_rows + 1),
        },
    ));
    assert_limit_refusal(
        &fetch_refusal,
        ReadFailureLimit::ResultRows,
        cursor_limits.result_rows,
    );
    assert_resources(&harness.service, live);
    harness
        .request(61, cursor_limits, LocalRequest::CursorClose { cursor_id })
        .expect("the refused cursor is still registered, so it is still closable");
    assert_resources(&harness.service, baseline);

    let after_refusal = cursor_opened(
        harness
            .request(
                62,
                cursor_limits,
                LocalRequest::CursorOpen {
                    statement: "SELECT id FROM reused_cursor ORDER BY id".to_owned(),
                    params: BTreeMap::new(),
                },
            )
            .expect("a new cursor reuses the released cursor identifier"),
    );
    assert_eq!(after_refusal.cursor_id, cursor_id);
    assert_eq!(
        decode_cursor_page(&after_refusal.payload)
            .expect("decode cursor reused after refusal")
            .rows,
        vec![vec![Value::Int64(1)]],
    );
    let live_after_refusal_reuse = harness.service.resources();
    assert_eq!(live_after_refusal_reuse.cursor_count, 1);
    assert!(live_after_refusal_reuse.cursor_retained_bytes > 0);
    for request in [
        LocalRequest::CursorFetch {
            cursor_id,
            rows: None,
        },
        LocalRequest::CursorClose { cursor_id },
    ] {
        let stale_owner = refusal(harness.request(61, cursor_limits, request));
        assert_simple_refusal(&stale_owner, ReadFailureKind::CursorNotFound);
        assert_resources(&harness.service, live_after_refusal_reuse);
    }
    harness
        .request(62, cursor_limits, LocalRequest::CursorClose { cursor_id })
        .expect("the new cursor alone controls explicit release");
    assert_resources(&harness.service, baseline);

    let after_close = cursor_opened(
        harness
            .request(
                63,
                cursor_limits,
                LocalRequest::CursorOpen {
                    statement: "SELECT id FROM reused_cursor ORDER BY id".to_owned(),
                    params: BTreeMap::new(),
                },
            )
            .expect("a new cursor reuses the explicitly closed identifier"),
    );
    assert_eq!(after_close.cursor_id, cursor_id);
    assert_eq!(
        decode_cursor_page(&after_close.payload)
            .expect("decode cursor reused after explicit close")
            .rows,
        vec![vec![Value::Int64(1)]],
    );
    let live_after_close_reuse = harness.service.resources();
    for request in [
        LocalRequest::CursorFetch {
            cursor_id,
            rows: None,
        },
        LocalRequest::CursorClose { cursor_id },
    ] {
        let stale_owner = refusal(harness.request(62, cursor_limits, request));
        assert_simple_refusal(&stale_owner, ReadFailureKind::CursorNotFound);
        assert_resources(&harness.service, live_after_close_reuse);
    }
    let status = owner_status(
        harness
            .request(64, cursor_limits, LocalRequest::OwnerStatus)
            .expect("status sees only the newest reused cursor"),
    );
    assert_eq!(status.admission.active_readers, 1);
    assert_eq!(
        status.memory.used_bytes,
        harness.service.resources().accountant_used_bytes
    );
    harness
        .request(63, cursor_limits, LocalRequest::CursorClose { cursor_id })
        .expect("release the newest reused cursor");
    assert_resources(&harness.service, baseline);
}

#[test]
fn cursor_snapshot_stays_stable_while_writer_and_maintenance_progress_then_releases_everything() {
    let mut owner_limits = limits();
    owner_limits.cursor_page_rows = 1;
    let harness = Harness::new(owner_limits, 4, None);
    harness
        .database
        .execute(
            "CREATE TABLE snapshot_rows (id INTEGER PRIMARY KEY, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create snapshot fixture");
    for id in 1..=2_i64 {
        harness
            .database
            .execute(
                "INSERT INTO snapshot_rows VALUES ($id, $payload)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    ("payload".to_owned(), Value::Text(format!("before-{id}"))),
                ]),
            )
            .expect("insert committed snapshot row");
    }
    let baseline = harness.service.resources();
    let opened = cursor_opened(
        harness
            .request(
                40,
                owner_limits,
                LocalRequest::CursorOpen {
                    statement: "SELECT id, payload FROM snapshot_rows ORDER BY id".to_owned(),
                    params: BTreeMap::new(),
                },
            )
            .expect("open one-row snapshot cursor"),
    );
    let first = decode_cursor_page(&opened.payload).expect("decode snapshot first page");
    assert_eq!(first.rows.len(), 1);
    let live = harness.service.resources();
    assert_eq!(live.active_slots, baseline.active_slots + 1);
    assert!(live.cursor_retained_bytes > 0);

    let writer_done = Arc::new((Mutex::new(false), Condvar::new()));
    let writer_database = Arc::clone(&harness.database);
    let writer_signal = Arc::clone(&writer_done);
    let writer = std::thread::spawn(move || {
        writer_database
            .execute(
                "INSERT INTO snapshot_rows VALUES (3, 'after-open')",
                &HashMap::new(),
            )
            .expect("writer commits while cursor waits between fetches");
        let (lock, changed) = &*writer_signal;
        *lock.lock().expect("writer progress state") = true;
        changed.notify_one();
    });
    let (lock, changed) = &*writer_done;
    let mut done = lock.lock().expect("writer progress state");
    while !*done {
        done = changed.wait(done).expect("writer progress wait");
    }
    drop(done);
    writer.join().expect("writer progress thread joins");
    harness
        .database
        .run_maintenance_cycle()
        .expect("maintenance and pruning can run while the snapshot pin is live");
    let live_after_writer = harness.service.resources();
    assert_eq!(live_after_writer.active_slots, live.active_slots);
    assert_eq!(live_after_writer.cursor_count, live.cursor_count);
    assert_eq!(
        live_after_writer.cursor_retained_bytes,
        live.cursor_retained_bytes,
    );
    assert_eq!(live_after_writer.buffered_bytes, live.buffered_bytes);
    assert_eq!(
        live_after_writer.active_cancellations,
        live.active_cancellations,
    );

    let second = cursor_page(
        harness
            .request(
                40,
                owner_limits,
                LocalRequest::CursorFetch {
                    cursor_id: opened.cursor_id,
                    rows: None,
                },
            )
            .expect("fetch retained snapshot without a file/store lock"),
    );
    let second = decode_cursor_page(&second.payload).expect("decode snapshot second page");
    assert_eq!(second.rows.len(), 1);
    assert!(
        !second.has_more,
        "later committed row is outside the cursor snapshot"
    );
    assert_resources(
        &harness.service,
        OwnerReadResourceSnapshot {
            active_slots: live_after_writer.active_slots - 1,
            cursor_count: live_after_writer.cursor_count - 1,
            cursor_retained_bytes: live_after_writer.cursor_retained_bytes
                - live.cursor_retained_bytes,
            buffered_bytes: live_after_writer.buffered_bytes,
            active_cancellations: live_after_writer.active_cancellations,
            accountant_used_bytes: live_after_writer.accountant_used_bytes
                - live.cursor_retained_bytes,
        },
    );

    let current = harness
        .database
        .execute("SELECT id FROM snapshot_rows ORDER BY id", &HashMap::new())
        .expect("ordinary writer handle remains fully usable");
    assert_eq!(current.rows.len(), 3);
}

#[test]
fn shutdown_timeout_retains_service_and_database_then_retry_drains_without_closing_database() {
    let handler = GatedHandler::default();
    let observer = Arc::new(ProductionObserver::default());
    let observer_seam: Arc<dyn OwnerBoundedExecutionObserver> = observer.clone();
    let harness = Harness::start(
        owner_config(
            limits(),
            4,
            OwnerServiceTimeouts {
                request_ms: 10,
                shutdown_drain_ms: 10,
            },
            Some(Arc::new(handler.clone())),
        ),
        LocalConfigurationSource::Override,
        ManualDeadlineClock::at(100),
        ReadClientTimeouts {
            connect_ms: 1_000,
            routing_retry_ms: 1_000,
            response_ms: 11_000,
        },
        Some(observer_seam),
        None,
    );
    harness
        .database
        .execute(
            "CREATE TABLE shutdown_rows (id INTEGER PRIMARY KEY, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create shutdown fixture");
    for id in 1..=2_i64 {
        harness
            .database
            .execute(
                "INSERT INTO shutdown_rows VALUES ($id, $payload)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    ("payload".to_owned(), Value::Text(format!("shutdown-{id}"))),
                ]),
            )
            .expect("insert shutdown cursor fixture");
    }
    let baseline = harness.service.resources();
    #[cfg(unix)]
    {
        assert!(harness.service.channel_is_bound_for_test());
        assert!(harness.service.channel_path().exists());
    }
    let mut cursor_limits = limits();
    cursor_limits.cursor_page_rows = 1;
    let shutdown_cursor = cursor_opened(
        harness
            .request(
                53,
                cursor_limits,
                LocalRequest::CursorOpen {
                    statement: "SELECT id FROM shutdown_rows ORDER BY id".to_owned(),
                    params: BTreeMap::new(),
                },
            )
            .expect("open a live cursor before shutdown"),
    );
    assert_ne!(shutdown_cursor.cursor_id, [0; 16]);
    assert!(harness.service.resources().cursor_retained_bytes > 0);
    let client = harness.client(50);
    let held =
        std::thread::spawn(move || {
            block_on(client.lock().expect("shutdown handler client").request(
                LocalRequestEnvelope {
                    limits: limits(),
                    request: custom("proof.hold", b"ignore-cancellation"),
                },
            ))
        });
    handler.wait_for_entered(1);

    let draining = Arc::clone(&harness.service);
    let drain = std::thread::spawn(move || draining.shutdown_and_drain());
    harness
        .drain_started
        .recv()
        .expect("shutdown captures its drain deadline before the first blocking seam");
    observer.wait_for_shutdown_signal(1);
    handler.pulse();
    handler.wait_for_cancellation(1);
    let while_ignoring = harness.service.resources();
    assert_eq!(while_ignoring.active_slots, baseline.active_slots + 1);
    assert_eq!(while_ignoring.cursor_count, baseline.cursor_count);
    assert_eq!(
        while_ignoring.cursor_retained_bytes,
        baseline.cursor_retained_bytes
    );
    assert_eq!(
        while_ignoring.accountant_used_bytes,
        baseline.accountant_used_bytes
    );
    harness.clock.advance_to(111);
    let timed_out = drain
        .join()
        .expect("drain thread joins")
        .expect_err("cancellation-ignoring handler crosses the drain deadline");
    assert!(matches!(
        timed_out,
        OwnerReadScaffoldError::Database(Error::OwnerReadDrainTimeout)
    ));
    assert_eq!(
        harness.service.status().state,
        OwnerServingState::NotServing
    );
    let status = owner_status(
        harness
            .request(52, limits(), LocalRequest::OwnerStatus)
            .expect("authenticated status remains available after admission closes"),
    );
    assert_eq!(status.status.state, OwnerServingState::NotServing);
    assert_eq!(
        status.status.reason,
        Some(OwnerServingReason::ShutdownDraining),
    );
    assert_eq!(status.admission.active_readers, 1);
    assert_complete_status_configuration(
        &status,
        limits(),
        4,
        OwnerServiceTimeouts {
            request_ms: 10,
            shutdown_drain_ms: 10,
        },
        LocalConfigurationSource::Override,
    );
    for request in [
        LocalRequest::CursorFetch {
            cursor_id: shutdown_cursor.cursor_id,
            rows: None,
        },
        LocalRequest::CursorClose {
            cursor_id: shutdown_cursor.cursor_id,
        },
    ] {
        let closed_cursor_request = refusal(harness.request(53, cursor_limits, request));
        assert_simple_refusal(&closed_cursor_request, ReadFailureKind::OwnerNotServing);
        assert_eq!(harness.service.resources(), while_ignoring);
    }
    let closed = refusal(harness.request(51, limits(), custom("proof.echo", b"new")));
    assert_simple_refusal(&closed, ReadFailureKind::OwnerNotServing);

    harness
        .database
        .execute("SELECT id FROM shutdown_rows", &HashMap::new())
        .expect("ordinary database work remains usable after owner drain timeout");
    let retained = harness.service.resources();
    assert_eq!(retained.active_slots, baseline.active_slots + 1);
    assert_eq!(retained.cursor_count, baseline.cursor_count);
    assert_eq!(
        retained.cursor_retained_bytes,
        baseline.cursor_retained_bytes
    );
    assert_eq!(retained.buffered_bytes, baseline.buffered_bytes);
    #[cfg(unix)]
    {
        assert!(harness.service.channel_is_bound_for_test());
        assert!(harness.service.channel_path().exists());
    }

    handler.release();
    let request_failure = held
        .join()
        .expect("held handler request joins")
        .expect_err("timed-out custom response is never published after cancellation");
    assert!(matches!(
        request_failure,
        OwnerReadScaffoldError::Refused(failure)
            if failure.kind() == ReadFailureKind::OwnerTimeout
    ));
    assert_eq!(handler.cancellation_observed(), 1);
    assert_eq!(harness.service.cancellation_signal_count(), 1);

    harness
        .service
        .shutdown_and_drain()
        .expect("retry succeeds after cancellation-ignoring handler releases");
    assert_resources(&harness.service, baseline);
    assert_eq!(harness.service.cancellation_signal_count(), 1);
    harness
        .database
        .execute("SELECT id FROM shutdown_rows", &HashMap::new())
        .expect("owner service drain never closes Database");
}
