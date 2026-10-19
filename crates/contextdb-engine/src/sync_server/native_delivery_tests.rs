//! One authentic SyncServer delivery request through the native route:
//! original funding, release, refusal, cancellation and cursor scaling.
use super::*;
use crate::database::DeliveryManifest;
use crate::protocol::{FetchDeliveryOutcomesRequest, MessageType, encode_named};
use crate::subjects::{binding_subject, delivery_outcomes_subject};
use crate::sync_client::{ApplicationTablePolicyExpectation, SyncClient};
use crate::sync_types::NaturalKey;
use crate::transport::NativeStop;
use crate::transport::in_process::InProcessBroker;
use crate::vector_read_memory::DeclaredReadMemory;
use contextdb_core::Value;
use std::time::Duration;

const TENANT: &str = "native-delivery";
const ROOT_TABLE: &str = "records";
const MEMBER_TABLE: &str = "record_parts";
const ROOT_DDL: &str = "CREATE TABLE records (id UUID PRIMARY KEY, body TEXT) \
     SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST DELIVERY MANIFEST OVER record_parts";
const MEMBER_DDL: &str = "CREATE TABLE record_parts \
     (id UUID PRIMARY KEY, record_id UUID REFERENCES records(id), body TEXT) \
     SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST";
const UNRELATED_DDL: &str = "CREATE TABLE unrelated_rows (id UUID PRIMARY KEY, body TEXT) \
     SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST";
const ROOT_CLAUSES: &str =
    "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST DELIVERY MANIFEST OVER record_parts";
const MEMBER_CLAUSES: &str = "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST";
const LIMIT: u64 = 1 << 30;

fn expectation() -> ApplicationTablePolicyExpectation {
    ApplicationTablePolicyExpectation::new()
        .expect_table(ROOT_TABLE, ROOT_CLAUSES)
        .expect("root clauses parse")
        .expect_table(MEMBER_TABLE, MEMBER_CLAUSES)
        .expect("member clauses parse")
}

fn open(path: &std::path::Path) -> Arc<Database> {
    open_with_page_bytes(path, None)
}

/// `open`, with the reader's declared page bytes set when given.
fn open_with_page_bytes(path: &std::path::Path, page_bytes: Option<u64>) -> Arc<Database> {
    open_with_limits(path, page_bytes, None)
}

/// `open_with_page_bytes`, with the reader's declared active time raised when
/// given: a debug build verifies a large outcome slower than the default.
fn open_with_limits(
    path: &std::path::Path,
    page_bytes: Option<u64>,
    active_ms: Option<u64>,
) -> Arc<Database> {
    let mut options = crate::DatabaseOpenOptions::default();
    if let Some(active_ms) = active_ms {
        options.owner_reads.limits.limits.active_ms = active_ms;
    }
    // The Legacy seeding fixture and the Legacy fetch of 10,010 outcomes hold
    // whole custody history on the reader; the native request does not.
    options.owner_reads.limits.limits.memory = 8 * 1024 * 1024 * 1024;
    if let Some(page_bytes) = page_bytes {
        options.owner_reads.limits.limits.cursor_page_bytes = page_bytes;
    }
    let db = Arc::new(Database::open_with_options(path, options).expect("open"));
    let p = HashMap::new();
    db.execute(ROOT_DDL, &p).expect("root table");
    db.execute(MEMBER_DDL, &p).expect("member table");
    db.execute(UNRELATED_DDL, &p).expect("unrelated table");
    db
}

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(600), future)
        .await
        .expect("bounded in-process exchange")
}

struct World {
    hub_db: Arc<Database>,
    hub_node: String,
    hub_identity: Arc<crate::identity::FabricIdentity>,
    broker: InProcessBroker,
    edge_db: Arc<Database>,
    edge_node: String,
    edge_identity: Arc<crate::identity::FabricIdentity>,
    client: SyncClient,
    // Dropped last: every store above closes before its directory goes.
    _dir: tempfile::TempDir,
}

impl World {
    fn new() -> Self {
        Self::with_hub_store(false)
    }

    /// `memory_hub` opens the hub as an in-memory store instead of a file.
    fn with_hub_store(memory_hub: bool) -> Self {
        Self::with_hub(memory_hub, None)
    }

    /// A file-backed hub whose reader declares `page_bytes` per page.
    fn with_page_bytes(page_bytes: u64) -> Self {
        Self::with_hub(false, Some(page_bytes))
    }

    fn with_hub(memory_hub: bool, page_bytes: Option<u64>) -> Self {
        let dir = tempfile::tempdir().expect("store directory");
        let hub_db = if memory_hub {
            open_with_page_bytes(std::path::Path::new(":memory:"), page_bytes)
        } else {
            open_with_page_bytes(&dir.path().join("hub.db"), page_bytes)
        };
        hub_db
            .__seed_tenant_table_policies_for_test(TenantId::from(TENANT), expectation())
            .expect("declare the tenant policy");
        let hub_identity = Arc::new(crate::identity::FabricIdentity::generate());
        let hub_node = hub_identity.node_id();
        let broker = InProcessBroker::new();
        let edge_db = open(&dir.path().join("edge.db"));
        let edge_identity = Arc::new(crate::identity::FabricIdentity::generate());
        let edge_node = edge_identity.node_id();
        let client = SyncClient::with_authenticated_transport_and_identity_for_test(
            edge_db.clone(),
            broker.client_as(&edge_node),
            TenantId::from(TENANT),
            edge_identity.clone(),
        )
        .expect("edge client");
        Self {
            _dir: dir,
            hub_db,
            hub_node,
            hub_identity,
            broker,
            edge_db,
            edge_node,
            edge_identity,
            client,
        }
    }

    /// A hub over the same store whose delivery requests are funded by `requests`.
    fn server(&self, requests: &Arc<DeclaredReadMemory>) -> Arc<SyncServer> {
        Arc::new(
            SyncServer::with_authenticated_transport_and_identity_for_test_with_read_budget_provider(
                self.hub_db.clone(),
                self.broker.server_as(&self.hub_node),
                TenantId::from(TENANT),
                self.hub_node.clone(),
                self.hub_identity.clone(),
                Arc::new(requests.clone()),
            )
            .expect("hub"),
        )
    }

    fn edge_reader(&self) -> Arc<dyn contextdb_core::read_memory::ReadMemoryBudget> {
        self.edge_db.declared_reader_memory().expect("edge reader")
    }

    /// Commit `count` manifested roots on the edge, at most 100 per
    /// transaction, and have the hub record one terminal outcome for each of
    /// them. Returns the new roots' keys.
    async fn seed_outcomes(&self, count: usize) -> Vec<NaturalKey> {
        let started = std::time::Instant::now();
        let mut all = Vec::with_capacity(count);
        let mut remaining = count;
        while remaining > 0 {
            let batch = remaining.min(100);
            remaining -= batch;
            let tx = self.edge_db.begin().expect("begin");
            let mut keys = Vec::with_capacity(batch);
            for _ in 0..batch {
                let id = uuid::Uuid::new_v4();
                self.edge_db
                    .insert_row(
                        tx,
                        ROOT_TABLE,
                        HashMap::from([
                            ("id".to_string(), Value::Uuid(id)),
                            ("body".to_string(), Value::Text("unit".into())),
                        ]),
                    )
                    .expect("root row");
                let key = NaturalKey::single("id".to_string(), Value::Uuid(id));
                self.client
                    .__stage_delivery_manifest_for_test(
                        tx,
                        DeliveryManifest {
                            root_table: ROOT_TABLE,
                            root_key: key.clone(),
                            members: Vec::new(),
                        },
                        self.edge_reader(),
                    )
                    .expect("signed manifest");
                keys.push(key);
            }
            eprintln!(
                "S4_SEED staged batch of {batch} elapsed_ms={}",
                started.elapsed().as_millis()
            );
            self.edge_db.commit(tx).expect("commit");
            eprintln!(
                "S4_SEED committed batch elapsed_ms={}",
                started.elapsed().as_millis()
            );
            let seeded = within(self.client.__seed_hub_delivery_roots_for_test(
                ROOT_TABLE,
                Some(&keys),
                self.edge_reader(),
            ))
            .await
            .expect("hub records the outcomes");
            assert_eq!(seeded, batch);
            all.extend(keys);
            eprintln!(
                "S4_SEED recorded={} elapsed_ms={}",
                all.len(),
                started.elapsed().as_millis()
            );
        }
        all
    }

    /// Advance both stores' commit positions to at least `target` with
    /// unrelated local rows, so every position the delivery protocol encodes
    /// has the same MessagePack width in worlds of different history length.
    fn pad_positions(&self, target: u64) {
        for db in [&self.hub_db, &self.edge_db] {
            let mut batch = 1_000usize;
            while db.current_lsn().0 < target {
                let before = db.current_lsn().0;
                let tx = db.begin().expect("begin");
                for _ in 0..batch {
                    db.insert_row(
                        tx,
                        "unrelated_rows",
                        HashMap::from([
                            ("id".to_string(), Value::Uuid(uuid::Uuid::new_v4())),
                            ("body".to_string(), Value::Text("pad".into())),
                        ]),
                    )
                    .expect("pad row");
                }
                db.commit(tx).expect("pad commit");
                let advanced = db.current_lsn().0 - before;
                // Positions advance per commit, not per row: commit singly.
                if advanced < batch as u64 / 2 {
                    batch = 1;
                }
            }
        }
    }

    async fn bind(&self) {
        within(
            self.client
                .__seed_application_table_policy_binding_for_test(
                    expectation(),
                    self.edge_reader(),
                ),
        )
        .await
        .expect("binding");
    }

    fn request_bytes(&self, nonce: u8, since: Option<Vec<u8>>) -> Vec<u8> {
        let edge_incarnation = self
            .edge_db
            .existing_sync_incarnation(&TenantId::from(TENANT), self.edge_reader())
            .expect("edge incarnation read")
            .expect("bound edge has an incarnation");
        encode_named(
            MessageType::FetchDeliveryOutcomesRequest,
            &FetchDeliveryOutcomesRequest {
                format: 1,
                tenant_id: TENANT.into(),
                edge_incarnation,
                request_nonce: [nonce; 16],
                since,
            },
        )
        .expect("encode request")
    }

    /// The exact bytes the Legacy outcomes handler answers for `request`.
    fn legacy_reply(&self, reader: &Arc<DeclaredReadMemory>, request: &[u8]) -> Vec<u8> {
        legacy_delivery_reply(
            &self.hub_db,
            &TenantId::from(TENANT),
            Some(self.hub_node.clone()),
            reader.child().expect("legacy request reader"),
            request,
            Some(self.edge_node.clone()),
        )
        .expect("legacy reply")
    }
}

/// The hub served with `run_until` on a task of the calling runtime.
struct LegacyHub {
    flag: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}
async fn start_legacy(world: &World, server: &Arc<SyncServer>) -> LegacyHub {
    let flag = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let flag = flag.clone();
        async move { server.run_until(flag).await }
    });
    within(
        world
            .broker
            .wait_for_registered_route_for_test(&push_subject(TENANT)),
    )
    .await;
    LegacyHub { flag, task }
}
impl LegacyHub {
    async fn stop(self) {
        self.flag.store(true, Ordering::SeqCst);
        self.task.await.expect("legacy hub");
    }
}

/// The hub served with `run_until_stopped` on its own thread: the native
/// delivery root and the four Legacy companions beside it.
struct NativeHub {
    stop: NativeStop,
    thread: std::thread::JoinHandle<()>,
}
async fn start_native(world: &World, server: &Arc<SyncServer>) -> NativeHub {
    let stop = server
        .native_stop(Arc::new(AtomicBool::new(false)))
        .expect("prepare the native delivery installation");
    let thread = std::thread::spawn({
        let server = server.clone();
        let stop = stop.clone();
        move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("hub runtime")
                .block_on(server.run_until_stopped(stop))
        }
    });
    within(
        world
            .broker
            .wait_for_registered_route_for_test(&push_subject(TENANT)),
    )
    .await;
    NativeHub { stop, thread }
}
impl NativeHub {
    fn stop(self) {
        self.stop.request_stop();
        self.thread.join().expect("native hub thread");
    }
}

fn installation(server: &SyncServer) -> crate::transport::ReadBoundStreamInstallation {
    let _stop = server
        .native_stop(Arc::new(AtomicBool::new(false)))
        .expect("prepare the native delivery installation");
    server
        .native_delivery
        .lock()
        .unwrap()
        .take()
        .expect("installed native delivery route")
}

/// Drive one native delivery request on this thread through the in-process
/// installation. Returns the worker result and the reply body, if any.
fn drive(
    world: &World,
    installation: &crate::transport::ReadBoundStreamInstallation,
    request: &[u8],
    before_poll: impl FnOnce(&contextdb_core::read_contract::NativeReadCancellation),
) -> (crate::transport::WireResult<()>, Option<Vec<u8>>) {
    let (cancellation, future, reply) = crate::transport::in_process::begin_native_in_process(
        installation,
        &delivery_outcomes_subject(TENANT),
        request,
        Some(&world.edge_node),
    )
    .expect("begin one native delivery request");
    before_poll(&cancellation);
    // The native worker is synchronous: polling it on this thread runs the
    // whole selection here, with no executor and no wake registration.
    let mut future = std::pin::pin!(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    let mut polls = 0u64;
    let result = loop {
        polls += 1;
        if let std::task::Poll::Ready(result) = future.as_mut().poll(&mut context) {
            break result;
        }
        assert!(polls < 1_000_000, "native delivery worker stayed pending");
        std::thread::yield_now();
    };
    let body = reply.lock().unwrap().take().map(|body| {
        body.native_contiguous_bytes()
            .expect("contiguous reply")
            .to_vec()
    });
    (result, body)
}

/// The serving side drops a finished request's last allocation just after the
/// reply is published; wait for its release event, then read the exact count.
fn settled(parent: &DeclaredReadMemory, expected: u64) -> u64 {
    crate::transport::release_events::wait_until(|| parent.retained_bytes_for_test() == expected);
    parent.retained_bytes_for_test()
}

fn decode_outcomes(reply: &[u8]) -> usize {
    let envelope = crate::protocol::decode(reply).expect("reply envelope");
    assert_eq!(
        envelope.message_type,
        MessageType::FetchDeliveryOutcomesResponse
    );
    let response: crate::protocol::FetchDeliveryOutcomesResponse =
        rmp_serde::from_slice(&envelope.payload).expect("reply payload");
    response.result.expect("outcome page").outcomes.len()
}

/// The error a native request's result reaches its caller as: a native
/// terminal is converted as the caller handoff converts it; a Legacy resource
/// error is already that error.
fn handed_off(result: &crate::transport::WireResult<()>) -> Option<contextdb_core::Error> {
    match result {
        Err(crate::transport::WireError::Native(terminal)) => Some(match terminal {
            crate::transport::NativeTerminal::Refused(refusal) => {
                crate::transport::NativeTerminal::Refused(*refusal).into_error()
            }
            crate::transport::NativeTerminal::Kernel(error) => match error {
                contextdb_core::Error::ReadFailure(failure) => {
                    contextdb_core::Error::ReadFailure(failure.clone())
                }
                contextdb_core::Error::MemoryBudgetExceeded {
                    subsystem,
                    operation,
                    requested_bytes,
                    available_bytes,
                    budget_limit_bytes,
                    hint,
                } => contextdb_core::Error::MemoryBudgetExceeded {
                    subsystem: subsystem.clone(),
                    operation: operation.clone(),
                    requested_bytes: *requested_bytes,
                    available_bytes: *available_bytes,
                    budget_limit_bytes: *budget_limit_bytes,
                    hint: hint.clone(),
                },
                other => contextdb_core::Error::SyncError(other.to_string().into()),
            },
            crate::transport::NativeTerminal::Cancelled => contextdb_core::Error::ReadCancelled,
            crate::transport::NativeTerminal::Class(class) => {
                contextdb_core::Error::SyncError((*class).into())
            }
        }),
        Err(crate::transport::WireError::Resource(error)) => Some(match error {
            contextdb_core::Error::ReadCancelled => contextdb_core::Error::ReadCancelled,
            other => contextdb_core::Error::SyncError(other.to_string().into()),
        }),
        _ => None,
    }
}

// Step 3: one request with three committed outcomes, through SyncServer and
// SyncClient, served by the native arm with no Legacy fetch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_native_delivery_request_serves_three_outcomes_and_releases_every_byte() {
    let world = World::new();
    let requests = DeclaredReadMemory::from_reader_limit(LIMIT);
    let server = world.server(&requests);
    let hub = start_native(&world, &server).await;
    world.bind().await;
    world.seed_outcomes(3).await;

    let installation_parent = world.hub_db.declared_read_memory_for_test();
    let installation_before = installation_parent.retained_bytes_for_test();
    let request_before = requests.retained_bytes_for_test();
    let fetched = within(world.client.fetch_delivery_outcomes(None))
        .await
        .expect("native delivery fetch");
    assert_eq!(fetched.len(), 3);
    assert_eq!(server.delivery_route_counts_for_test(), (1, 0));
    assert_eq!(settled(&requests, request_before), request_before);
    assert_eq!(
        installation_parent.retained_bytes_for_test(),
        installation_before
    );
    println!(
        "S4_STEP3 outcomes={} native={} legacy={} installation_delta={} request_parent_before={} request_parent_after={} request_peak={}",
        fetched.len(),
        server.delivery_route_counts_for_test().0,
        server.delivery_route_counts_for_test().1,
        installation_parent.retained_bytes_for_test() as i128 - installation_before as i128,
        request_before,
        requests.retained_bytes_for_test(),
        requests.file_child_high_water(),
    );

    // The same request bytes answered by the native route and by the Legacy
    // outcomes handler are byte-for-byte identical.
    let request = world.request_bytes(7, None);
    let transport = world.broker.client_as(&world.edge_node);
    let body = crate::transport::BoundedWireBody::from_legacy_reply(
        request.clone(),
        contextdb_core::read_memory::ReadMemoryOwner::from_legacy(world.edge_reader()),
    )
    .expect("request body");
    let native = transport
        .request_stream_admitted(
            &delivery_outcomes_subject(TENANT),
            body,
            Duration::from_secs(60),
        )
        .expect("admitted native request")
        .await
        .expect("native reply");
    let reply = native;
    let native = reply.contiguous_bytes().expect("contiguous").to_vec();
    drop(reply);
    let legacy = world.legacy_reply(&DeclaredReadMemory::from_reader_limit(LIMIT), &request);
    assert_eq!(decode_outcomes(&native), 3);
    assert_eq!(native, legacy);
    assert_eq!(server.delivery_route_counts_for_test(), (2, 0));
    println!(
        "S4_STEP3_BYTES native_len={} legacy_len={} equal={}",
        native.len(),
        legacy.len(),
        native == legacy
    );
    hub.stop();
    assert_eq!(settled(&requests, request_before), request_before);
}

// Step 4: binding, push, pull and status keep their Legacy handlers beside the
// native delivery root, answering exactly what `run_until` answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_companions_serve_beside_the_native_delivery_root_byte_for_byte() {
    let world = World::new();
    let requests = DeclaredReadMemory::from_reader_limit(LIMIT);
    let server = world.server(&requests);
    let native = start_native(&world, &server).await;
    world.bind().await;
    world.seed_outcomes(2).await;
    within(world.client.bind_application_table_policy(expectation()))
        .await
        .expect("binding companion");
    let tx = world.edge_db.begin().expect("begin");
    world
        .edge_db
        .insert_row(
            tx,
            "unrelated_rows",
            HashMap::from([
                ("id".to_string(), Value::Uuid(uuid::Uuid::new_v4())),
                ("body".to_string(), Value::Text("beside".into())),
            ]),
        )
        .expect("edge row");
    world.edge_db.commit(tx).expect("commit");
    within(world.client.push()).await.expect("push companion");
    within(world.client.pull_default())
        .await
        .expect("pull companion");
    let fetched = within(world.client.fetch_delivery_outcomes(None))
        .await
        .expect("native delivery");
    assert_eq!(fetched.len(), 2);
    assert_eq!(server.delivery_route_counts_for_test(), (1, 0));
    native.stop();

    let companions = [
        binding_subject(TENANT),
        push_subject(TENANT),
        pull_subject(TENANT),
        status_subject(TENANT),
    ];
    let recorded = world.broker.recorded_exchanges();
    let mut served = [0usize; 4];
    for exchange in &recorded {
        if let Some(index) = companions.iter().position(|s| *s == exchange.subject) {
            served[index] += 1;
        }
    }
    assert!(served.iter().all(|count| *count >= 1), "{served:?}");
    assert!(
        recorded
            .iter()
            .all(|exchange| exchange.subject != delivery_outcomes_subject(TENANT)),
        "delivery never reached the Legacy route map"
    );

    // The journey above changed hub state between exchanges, so its replies
    // are not a like-for-like baseline. Replay every recorded companion
    // request, in order, first beside the native root and then on run_until:
    // both replays see the same settled state and must answer identically.
    let requests: Vec<_> = recorded
        .iter()
        .filter(|exchange| companions.contains(&exchange.subject))
        .map(|exchange| (exchange.subject.clone(), exchange.request_bytes.clone()))
        .collect();
    let transport = world.broker.client_as(&world.edge_node);
    let mut beside = Vec::new();
    let native = start_native(&world, &server).await;
    for (subject, request) in &requests {
        beside.push(
            within(transport.request(subject, request.clone(), Duration::from_secs(60)))
                .await
                .expect("companion beside the native root"),
        );
    }
    native.stop();
    let legacy = start_legacy(&world, &server).await;
    let mut identical = [0usize; 4];
    let mut equivalent = [0usize; 4];
    let mut differing = Vec::new();
    for ((subject, request), beside) in requests.iter().zip(&beside) {
        let index = companions
            .iter()
            .position(|s| s == subject)
            .expect("companion");
        let replay = within(transport.request(subject, request.clone(), Duration::from_secs(60)))
            .await
            .expect("run_until replay");
        if &replay == beside {
            identical[index] += 1;
        } else {
            differing.push(subject.clone());
        }
        // Every reply, identical or not, decodes to the same answer once the
        // hub's own clock fields are set aside; nothing else may differ.
        assert_eq!(
            without_hub_clock(subject, &replay),
            without_hub_clock(subject, beside),
            "{subject}: replies differ beyond the hub clock fields"
        );
        equivalent[index] += 1;
    }
    // Control: the same replay twice more on run_until alone.
    let mut control_identical = [0usize; 4];
    let mut first = Vec::new();
    for (subject, request) in &requests {
        first.push(
            within(transport.request(subject, request.clone(), Duration::from_secs(60)))
                .await
                .expect("control replay"),
        );
    }
    for ((subject, request), first) in requests.iter().zip(&first) {
        let index = companions
            .iter()
            .position(|s| s == subject)
            .expect("companion");
        let again = within(transport.request(subject, request.clone(), Duration::from_secs(60)))
            .await
            .expect("control replay");
        if &again == first {
            control_identical[index] += 1;
        } else {
            let (a, b) = (
                crate::protocol::decode(first).expect("envelope"),
                crate::protocol::decode(&again).expect("envelope"),
            );
            println!(
                "S4_STEP4_CONTROL_DIFF {subject} type={:?} first={:?} again={:?}",
                a.message_type,
                rmp_serde::from_slice::<serde_json::Value>(&a.payload).ok(),
                rmp_serde::from_slice::<serde_json::Value>(&b.payload).ok()
            );
        }
    }
    println!(
        "S4_STEP4_CONTROL run_until_vs_run_until identical(binding,push,pull,status)={control_identical:?}"
    );
    legacy.stop().await;
    println!(
        "S4_STEP4 served(binding,push,pull,status)={served:?} replayed={} identical(binding,push,pull,status)={identical:?} equivalent_without_hub_clock={equivalent:?} differing={differing:?} native={} legacy={}",
        requests.len(),
        server.delivery_route_counts_for_test().0,
        server.delivery_route_counts_for_test().1
    );
    assert_eq!(server.delivery_route_counts_for_test(), (1, 0));
    // Every replay itself records contact and advances the hub position, so
    // status, pull and repeated pushes carry a newer hub LSN on any second
    // ask. The native-beside arm must match run_until exactly as run_until
    // matches itself: the same subjects identical, the same subjects moved.
    assert_eq!(identical, control_identical, "{differing:?}");
    assert_eq!(identical[0], served[0], "binding is byte-identical");
}

/// A companion reply decoded with exactly the fields that carry the hub's
/// current position cleared: status `server_current_lsn`, push
/// `result.new_lsn`, pull `cursor`. Any other difference stays visible.
fn without_hub_clock(subject: &str, reply: &[u8]) -> String {
    use crate::protocol::*;
    let envelope = decode(reply).expect("companion envelope");
    let payload = &envelope.payload;
    match envelope.message_type {
        MessageType::StatusResponse => {
            let mut status: SyncStatusResponse = rmp_serde::from_slice(payload).expect("status");
            status.server_current_lsn = None;
            format!("{status:?}")
        }
        MessageType::PushResponse => {
            let mut push: PushResponse = rmp_serde::from_slice(payload).expect("push");
            if let Some(result) = push.result.as_mut() {
                result.new_lsn = contextdb_core::Lsn(0);
            }
            format!("{push:?}")
        }
        MessageType::PullResponse => {
            let mut pull: PullResponse = rmp_serde::from_slice(payload).expect("pull");
            pull.cursor = None;
            format!("{pull:?}")
        }
        MessageType::DependencyCompletePullResponse => {
            let mut pull: DependencyCompletePullResponse =
                rmp_serde::from_slice(payload).expect("dependency-complete pull");
            pull.ordinary.cursor = None;
            format!("{pull:?}")
        }
        other => {
            assert_eq!(subject, binding_subject(TENANT), "unexpected {other:?}");
            format!("{other:?}:{payload:?}")
        }
    }
}

// Step 6: refusal at the reply, cancellation before the first poll and after
// selection; every request byte returns and the installation is untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refusal_and_cancellation_release_every_request_byte() {
    let world = World::new();
    let seeding = DeclaredReadMemory::from_reader_limit(LIMIT);
    let seeder = world.server(&seeding);
    let hub = start_legacy(&world, &seeder).await;
    world.bind().await;
    world.seed_outcomes(3).await;
    hub.stop().await;
    let request = world.request_bytes(9, None);
    let installation_parent = world.hub_db.declared_read_memory_for_test();

    // Measure one successful request: its peak child charge and reply size.
    let measured = DeclaredReadMemory::from_reader_limit(LIMIT);
    let route = installation(&world.server(&measured));
    let (result, reply) = drive(&world, &route, &request, |_| {});
    result.expect("measured request");
    let reply = reply.expect("measured reply");
    assert_eq!(decode_outcomes(&reply), 3);
    let peak = measured.file_child_high_water();
    let control = ReadMemoryOwnerLayout::control_bytes();
    assert_eq!(measured.retained_bytes_for_test(), 0);

    // Refusal: the request parent holds one byte less than the request's peak.
    let tight = DeclaredReadMemory::from_reader_limit(control + peak - 1);
    let route = installation(&world.server(&tight));
    let installation_before = installation_parent.retained_bytes_for_test();
    let (refused, body) = drive(&world, &route, &request, |_| {});
    let refusal = match handed_off(&refused) {
        Some(error @ contextdb_core::Error::MemoryBudgetExceeded { .. }) => error.to_string(),
        other => panic!("expected a typed memory refusal, got {other:?} from {refused:?}"),
    };
    assert!(body.is_none(), "no partial reply");
    assert_eq!(tight.retained_bytes_for_test(), 0);
    assert_eq!(
        installation_parent.retained_bytes_for_test(),
        installation_before
    );

    // Cancellation before the first poll.
    let before = DeclaredReadMemory::from_reader_limit(LIMIT);
    let route = installation(&world.server(&before));
    let installation_before = installation_parent.retained_bytes_for_test();
    let (cancelled_early, body) = drive(&world, &route, &request, |cancel| cancel.cancel());
    assert!(matches!(
        handed_off(&cancelled_early),
        Some(contextdb_core::Error::ReadCancelled)
    ));
    assert!(body.is_none());
    assert_eq!(before.retained_bytes_for_test(), 0);
    assert_eq!(
        installation_parent.retained_bytes_for_test(),
        installation_before
    );

    // Cancellation after custody selection, sampling the installation parent
    // while the selected request is still alive.
    let after = DeclaredReadMemory::from_reader_limit(LIMIT);
    let route = installation(&world.server(&after));
    let installation_before = installation_parent.retained_bytes_for_test();
    let during = Arc::new(std::sync::Mutex::new(None));
    let (cancelled_late, body) = drive(&world, &route, &request, |cancel| {
        let cancel = cancel.clone();
        let during = during.clone();
        let parent = installation_parent.clone();
        let request_parent = after.clone();
        crate::sync_native_delivery::AFTER_SELECTION.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                *during.lock().unwrap() = Some((
                    parent.retained_bytes_for_test(),
                    request_parent.retained_bytes_for_test(),
                ));
                cancel.cancel();
            }));
        });
    });
    assert!(matches!(
        handed_off(&cancelled_late),
        Some(contextdb_core::Error::ReadCancelled)
    ));
    assert!(body.is_none());
    let (installation_during, request_during) = during.lock().unwrap().expect("hook ran");
    assert_eq!(installation_during, installation_before);
    assert!(request_during > 0);
    assert_eq!(after.retained_bytes_for_test(), 0);
    assert_eq!(
        installation_parent.retained_bytes_for_test(),
        installation_before
    );
    println!(
        "S4_STEP6 reply_len={} request_peak={} control={} tight_limit={} refused={:?} cancelled_before_poll=ReadCancelled cancelled_after_selection=ReadCancelled request_bytes_during_selection={} installation_delta=0 retained_after=[{},{},{}]",
        reply.len(),
        peak,
        control,
        control + peak - 1,
        refusal,
        request_during,
        tight.retained_bytes_for_test(),
        before.retained_bytes_for_test(),
        after.retained_bytes_for_test(),
    );
}

/// The parent-side control charge of one native request owner.
struct ReadMemoryOwnerLayout;
impl ReadMemoryOwnerLayout {
    fn control_bytes() -> u64 {
        let parent = DeclaredReadMemory::from_reader_limit(LIMIT);
        let owner = parent.native_child().expect("one owner");
        let bytes = parent.retained_bytes_for_test();
        drop(owner);
        bytes
    }
}

/// One native page as the hub served it.
struct Page {
    examined: (u64, u64),
    outcomes: usize,
    more: bool,
    peak: u64,
    released: bool,
    last: Option<Vec<u8>>,
}

fn decode_page(reply: &[u8]) -> (Vec<crate::protocol::WireDeliveryOutcome>, bool) {
    let envelope = crate::protocol::decode(reply).expect("reply envelope");
    let response: crate::protocol::FetchDeliveryOutcomesResponse =
        rmp_serde::from_slice(&envelope.payload).expect("reply payload");
    let page = response.result.expect("outcome page");
    (page.outcomes, page.more)
}

/// The resumable cursor after one served outcome, as the edge derives it.
fn cursor_after(world: &World, outcome: &crate::protocol::WireDeliveryOutcome) -> Vec<u8> {
    let reader = DeclaredReadMemory::from_reader_limit(LIMIT);
    let memory = crate::read_image_memory::ImageMemory::charging_reader_for(
        world.hub_db.bounded_read_accountant(),
        reader.child().expect("decode reader"),
        "reader",
        "decode_page_cursor",
        "test fixture",
    );
    let packet = crate::custody::delivery::read_packet(outcome, &memory).expect("signed packet");
    rmp_serde::to_vec_named(
        &crate::custody::inspection::projection(&packet.record, &packet.block).cursor(),
    )
    .expect("cursor bytes")
}

/// Serve native pages from `since` until the hub says no more follow. Each
/// page runs on a fresh request parent so its peak and release are its own.
fn native_pages(world: &World, since: Option<Vec<u8>>) -> Vec<Page> {
    let mut pages = Vec::new();
    let mut since = since;
    loop {
        let parent = DeclaredReadMemory::from_reader_limit(LIMIT);
        let route = installation(&world.server(&parent));
        crate::database::native_custody_selection::NATIVE_ORDERS_EXAMINED
            .with(|seen| seen.set((0, 0)));
        let request = world.request_bytes(5, since.take());
        let (result, reply) = drive(world, &route, &request, |_| {});
        result.expect("one native page");
        let examined = crate::database::native_custody_selection::NATIVE_ORDERS_EXAMINED
            .with(|seen| seen.get());
        drop(route);
        let (outcomes, more) = decode_page(&reply.expect("page reply"));
        let last = outcomes.last().map(|outcome| cursor_after(world, outcome));
        pages.push(Page {
            examined,
            outcomes: outcomes.len(),
            more,
            peak: parent.file_child_high_water(),
            released: parent.retained_bytes_for_test() == 0,
            last: last.clone(),
        });
        if !more {
            return pages;
        }
        since = Some(last.expect("a page that says more has outcomes"));
    }
}

struct Scaling {
    examined: (u64, u64),
    request_len: usize,
    reply_len: usize,
    native_peak: u64,
    legacy_peak: u64,
    outcomes: usize,
    catch_up: Vec<Page>,
    /// Peak bytes freed before selection returns: its working set.
    transient: u64,
    /// The bytes whose width depends on random key material: the request
    /// envelope, its decoded payload and the selected outcomes' byte fields.
    own_bytes: u64,
}

/// The encoded bytes one cursor request holds through selection whose widths
/// follow random key material rather than history: the request envelope, the
/// decoded request payload (its cursor rides as a MessagePack sequence of u8,
/// one or two bytes per byte value) and every selected outcome's byte fields.
fn own_bytes(request: &[u8], reply: &[u8]) -> u64 {
    let payload = crate::protocol::decode(request)
        .expect("request envelope")
        .payload
        .len();
    let envelope = crate::protocol::decode(reply).expect("reply envelope");
    let response: crate::protocol::FetchDeliveryOutcomesResponse =
        rmp_serde::from_slice(&envelope.payload).expect("reply payload");
    let outcome_bytes: usize = response
        .result
        .expect("outcome page")
        .outcomes
        .iter()
        .map(|outcome| {
            outcome.lookup_submission.len()
                + outcome.lookup_source.len()
                + outcome.signed_core.len()
                + outcome.diagnostic_body.len()
        })
        .sum();
    (request.len() + payload + outcome_bytes) as u64
}

/// Both worlds start from the same position class (MessagePack u32), so the
/// ten pending outcomes after the cursor encode to the same number of bytes.
const PADDED_POSITION: u64 = 1 << 16;

async fn cursor_scaling(prior: usize) -> Scaling {
    eprintln!("S4_SCALE prior={prior} world");
    let world = World::new();
    world.pad_positions(PADDED_POSITION);
    eprintln!(
        "S4_SCALE prior={prior} padded hub_lsn={} edge_lsn={}",
        world.hub_db.current_lsn().0,
        world.edge_db.current_lsn().0
    );
    let seeding = DeclaredReadMemory::from_reader_limit(LIMIT);
    let seeder = world.server(&seeding);
    let hub = start_legacy(&world, &seeder).await;
    world.bind().await;
    world.seed_outcomes(prior).await;
    hub.stop().await;
    // A fresh edge catches up page by page under the shipped limits.
    eprintln!("S4_SCALE prior={prior} paged catch-up");
    let catch_up = native_pages(&world, None);
    let cursor = catch_up
        .last()
        .and_then(|page| page.last.clone())
        .expect("prior cursor");
    let hub = start_legacy(&world, &seeder).await;
    world.seed_outcomes(10).await;
    hub.stop().await;
    eprintln!("S4_SCALE prior={prior} seeded; driving native");
    let request = world.request_bytes(3, Some(cursor));

    let measured = DeclaredReadMemory::from_reader_limit(LIMIT);
    let route = installation(&world.server(&measured));
    crate::database::native_custody_selection::NATIVE_ORDERS_EXAMINED.with(|seen| seen.set((0, 0)));
    // The peak is reached inside selection. Split it there into what the
    // request still holds and the selection's transient working set.
    let at_selection = std::rc::Rc::new(std::cell::Cell::new((0u64, 0u64)));
    crate::sync_native_delivery::AFTER_SELECTION.with(|hook| {
        let at_selection = at_selection.clone();
        let measured = measured.clone();
        *hook.borrow_mut() = Some(Box::new(move || {
            at_selection.set((
                measured.retained_bytes_for_test(),
                measured.file_child_high_water(),
            ))
        }));
    });
    let (result, reply) = drive(&world, &route, &request, |_| {});
    result.expect("native cursor request");
    let (held_at_selection, high_at_selection) = at_selection.get();
    let examined =
        crate::database::native_custody_selection::NATIVE_ORDERS_EXAMINED.with(|seen| seen.get());
    let reply = reply.expect("reply");
    let reply_len = reply.len();
    let outcomes = decode_outcomes(&reply);

    let legacy = DeclaredReadMemory::from_reader_limit(LIMIT);
    let control = world.legacy_reply(&legacy, &request);
    assert_eq!(decode_outcomes(&control), outcomes);
    let native_peak = measured.file_child_high_water();
    assert_eq!(
        native_peak, high_at_selection,
        "the peak is reached in selection"
    );
    Scaling {
        examined,
        request_len: request.len(),
        reply_len,
        native_peak,
        legacy_peak: legacy.file_child_high_water(),
        outcomes,
        catch_up,
        transient: native_peak - held_at_selection,
        own_bytes: own_bytes(&request, &reply),
    }
}

fn describe(pages: &[Page]) -> String {
    let full: Vec<_> = pages.iter().filter(|page| page.more).collect();
    let (min, max) = full.iter().fold((u64::MAX, 0), |(lo, hi), page| {
        (lo.min(page.peak), hi.max(page.peak))
    });
    format!(
        "pages={} delivered={} examined_total(count,collect)=({},{}) max_examined_per_page={:?} full_page_peak_min={} full_page_peak_max={} last_page_peak={} all_released={}",
        pages.len(),
        pages.iter().map(|page| page.outcomes).sum::<usize>(),
        pages.iter().map(|page| page.examined.0).sum::<u64>(),
        pages.iter().map(|page| page.examined.1).sum::<u64>(),
        pages.iter().map(|page| page.examined).max(),
        if full.is_empty() { 0 } else { min },
        max,
        pages.last().map_or(0, |page| page.peak),
        pages.iter().all(|page| page.released),
    )
}

// Step 7 and the paging follow-up: a fresh edge catches up in pages of the
// declared cursor page size; each page examines at most page + 1 orders,
// releases every request byte, and charges the same peak wherever it sits in
// history. The ten outcomes after a cursor examine exactly ten orders whether
// 10 or 10,010 precede it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cursor_request_examines_only_the_orders_after_it() {
    let small = cursor_scaling(10).await;
    let large = cursor_scaling(10_010).await;
    println!(
        "S4_STEP7 prior=10 examined(count,collect)={:?} outcomes={} request_len={} reply_len={} native_peak={} transient={} own_bytes={} legacy_peak={} | prior=10010 examined(count,collect)={:?} outcomes={} request_len={} reply_len={} native_peak={} transient={} own_bytes={} legacy_peak={} | legacy_growth={}",
        small.examined,
        small.outcomes,
        small.request_len,
        small.reply_len,
        small.native_peak,
        small.transient,
        small.own_bytes,
        small.legacy_peak,
        large.examined,
        large.outcomes,
        large.request_len,
        large.reply_len,
        large.native_peak,
        large.transient,
        large.own_bytes,
        large.legacy_peak,
        large.legacy_peak as i128 - small.legacy_peak as i128,
    );
    println!("S4_PAGED prior=10 {}", describe(&small.catch_up));
    println!("S4_PAGED prior=10010 {}", describe(&large.catch_up));
    assert_eq!(small.outcomes, 10);
    assert_eq!(large.outcomes, 10);
    assert_eq!(small.examined, (10, 10));
    assert_eq!(large.examined, (10, 10));
    // The two worlds cannot share key material: node keys, incarnations,
    // signatures and digests are minted fresh, and a MessagePack u8 sequence
    // spends one or two bytes per byte value. So the raw peaks differ run to
    // run even at equal history. The exact invariant: selection's working set
    // is the same at both depths, and the peaks differ by exactly the
    // difference in the request's and selected outcomes' own encoded bytes,
    // with nothing charged per prior outcome.
    assert_eq!(
        large.transient, small.transient,
        "selection's working set grew with history"
    );
    assert_eq!(
        large.native_peak as i128 - small.native_peak as i128,
        large.own_bytes as i128 - small.own_bytes as i128,
        "native peak moved with history beyond the request's own bytes"
    );
    let page = crate::DatabaseOpenOptions::default()
        .owner_reads
        .limits
        .limits
        .cursor_page_rows as usize;
    for (prior, pages) in [(10, &small.catch_up), (10_010, &large.catch_up)] {
        assert_eq!(pages.iter().map(|p| p.outcomes).sum::<usize>(), prior);
        assert_eq!(pages.len(), prior.div_ceil(page));
        assert!(
            pages.iter().all(|p| p.released),
            "every page releases its bytes"
        );
        assert!(pages.iter().all(|p| p.outcomes <= page));
        assert!(
            pages
                .iter()
                .all(|p| p.examined.0 <= page as u64 + 1 && p.examined.1 <= page as u64),
            "a page examines at most page + 1 orders"
        );
    }
    let full: Vec<u64> = large
        .catch_up
        .iter()
        .filter(|p| p.more)
        .map(|p| p.peak)
        .collect();
    let (lo, hi) = (*full.iter().min().unwrap(), *full.iter().max().unwrap());
    assert!(
        (hi - lo) * 100 <= lo,
        "full-page peak moved with position in history: {lo}..{hi}"
    );
}

// Step 9: the production Iroh transport serves delivery on the native route,
// with binding, push, pull and status on their Legacy handlers beside it.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_iroh_hub_serves_delivery_natively_beside_its_legacy_companions() {
    let dir = tempfile::tempdir().expect("store directory");
    let tenant = TenantId::from(TENANT);
    let endpoint = crate::transport::iroh::IrohServer::bind(&format!(
        "iroh:?identity={}",
        dir.path().join("hub.key").display()
    ))
    .await
    .expect("bind the hub endpoint");
    let hub_db = open(&dir.path().join("hub.db"));
    hub_db
        .__seed_tenant_table_policies_for_test(tenant.clone(), expectation())
        .expect("declare the tenant policy");
    let server = Arc::new(SyncServer::new(hub_db.clone(), &endpoint, tenant.clone()).expect("hub"));
    let stop = server
        .native_stop(Arc::new(AtomicBool::new(false)))
        .expect("prepare the native delivery installation");
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until_stopped(stop).await }
    });

    let edge_db = open(&dir.path().join("edge.db"));
    let client = SyncClient::new(
        edge_db.clone(),
        &crate::transport::peer_dial_spec(&endpoint.ticket(), &dir.path().join("edge.key")),
        tenant.clone(),
    )
    .expect("edge client");
    within(client.bind_application_table_policy(expectation()))
        .await
        .expect("binding over Iroh");
    let reader = edge_db.declared_reader_memory().expect("edge reader");
    let tx = edge_db.begin().expect("begin");
    let mut keys = Vec::new();
    for _ in 0..3 {
        let id = uuid::Uuid::new_v4();
        edge_db
            .insert_row(
                tx,
                ROOT_TABLE,
                HashMap::from([
                    ("id".to_string(), Value::Uuid(id)),
                    ("body".to_string(), Value::Text("unit".into())),
                ]),
            )
            .expect("root row");
        let key = NaturalKey::single("id".to_string(), Value::Uuid(id));
        client
            .__stage_delivery_manifest_for_test(
                tx,
                DeliveryManifest {
                    root_table: ROOT_TABLE,
                    root_key: key.clone(),
                    members: Vec::new(),
                },
                reader.clone(),
            )
            .expect("signed manifest");
        keys.push(key);
    }
    edge_db
        .insert_row(
            tx,
            "unrelated_rows",
            HashMap::from([
                ("id".to_string(), Value::Uuid(uuid::Uuid::new_v4())),
                ("body".to_string(), Value::Text("beside".into())),
            ]),
        )
        .expect("edge row");
    edge_db.commit(tx).expect("commit");
    assert_eq!(
        within(client.__seed_hub_delivery_roots_for_test(ROOT_TABLE, Some(&keys), reader.clone()))
            .await
            .expect("hub records the outcomes"),
        3
    );
    within(client.push()).await.expect("push over Iroh");
    within(client.pull_default()).await.expect("pull over Iroh");
    let fetched = within(client.fetch_delivery_outcomes(None))
        .await
        .expect("native delivery over Iroh");
    let tail = within(client.fetch_delivery_outcomes(Some(fetched[0].cursor())))
        .await
        .expect("native cursor delivery over Iroh");
    println!(
        "S4_STEP9 outcomes={} tail_after_first={} native={} legacy={}",
        fetched.len(),
        tail.len(),
        server.delivery_route_counts_for_test().0,
        server.delivery_route_counts_for_test().1
    );
    assert_eq!(fetched.len(), 3);
    assert_eq!(tail.len(), 2);
    assert_eq!(server.delivery_route_counts_for_test(), (2, 0));
    within(client.shutdown()).await;
    stop.request_stop();
    within(task).await.expect("hub stops");
}

/// The broker's own edge client, except that delivery requests after the
/// first `allowed` are lost on the way to the hub.
struct LoseDeliveryAfter {
    inner: Arc<dyn crate::transport::ClientTransport>,
    allowed: std::sync::atomic::AtomicUsize,
}
impl crate::transport::ClientTransport for LoseDeliveryAfter {
    fn local_node_id(&self) -> Option<String> {
        self.inner.local_node_id()
    }
    fn peer_node_id(&self) -> Option<String> {
        self.inner.peer_node_id()
    }
    fn has_stable_edge_identity(&self) -> bool {
        self.inner.has_stable_edge_identity()
    }
    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> crate::transport::TransportFuture<'a, Vec<u8>> {
        self.inner.request(subject, request_bytes, timeout)
    }
    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> crate::transport::TransportFuture<'a, Vec<u8>> {
        self.inner
            .request_single_reply(subject, request_bytes, timeout)
    }
    fn request_stream_admitted<'a>(
        &'a self,
        subject: &'a str,
        body: crate::transport::BoundedWireBody,
        timeout: Duration,
    ) -> crate::transport::WireResult<
        crate::transport::DispatchedWireFuture<'a, crate::transport::BoundedWireReply>,
    > {
        if subject == delivery_outcomes_subject(TENANT)
            && self
                .allowed
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_err()
        {
            return Err(TransportError::Unreachable("lost between delivery pages".into()).into());
        }
        self.inner.request_stream_admitted(subject, body, timeout)
    }
}

// Paging through SyncClient: a lagging edge persists whole pages only. Losing
// the hub between pages leaves the pages already delivered, no half page and no
// request bytes on the hub; asking again completes the continuation once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lagging_edge_catches_up_in_whole_pages_and_resumes_after_a_lost_page() {
    let world = World::new();
    let requests = DeclaredReadMemory::from_reader_limit(LIMIT);
    let server = world.server(&requests);
    let hub = start_native(&world, &server).await;
    world.bind().await;
    let page = crate::DatabaseOpenOptions::default()
        .owner_reads
        .limits
        .limits
        .cursor_page_rows as usize;
    let backlog = page * 2 + page / 2;
    world.seed_outcomes(backlog).await;
    let accepted = || {
        world
            .edge_db
            .delivery_status(ROOT_TABLE)
            .expect("edge delivery status")
            .accepted
    };
    let before = accepted();

    let lossy = SyncClient::with_authenticated_transport_and_identity_for_test(
        world.edge_db.clone(),
        Arc::new(LoseDeliveryAfter {
            inner: world.broker.client_as(&world.edge_node),
            allowed: std::sync::atomic::AtomicUsize::new(1),
        }),
        TenantId::from(TENANT),
        world.edge_identity.clone(),
    )
    .expect("lossy edge client");
    let lost = within(lossy.fetch_delivery_outcomes(None)).await;
    assert!(lost.is_err(), "the second page was lost");
    let after_loss = accepted();
    let served_once = server.delivery_route_counts_for_test();
    assert_eq!(settled(&requests, 0), 0, "no request bytes stay on the hub");

    let fetched = within(world.client.fetch_delivery_outcomes(None))
        .await
        .expect("resumed catch-up");
    let served = server.delivery_route_counts_for_test();
    println!(
        "S4_PAGED_CLIENT page={page} backlog={backlog} accepted_before={before} accepted_after_lost_page={after_loss} fetched_after_resume={} accepted_after_resume={} native_requests(lossy,total)=({},{}) legacy={} request_parent_after={}",
        fetched.len(),
        accepted(),
        served_once.0,
        served.0,
        served.1,
        requests.retained_bytes_for_test(),
    );
    assert_eq!(before, 0);
    assert_eq!(after_loss, page as u64, "exactly one whole page persisted");
    assert_eq!(served_once, (1, 0));
    assert_eq!(fetched.len(), backlog);
    assert_eq!(accepted(), backlog as u64);
    assert_eq!(served, (1 + backlog.div_ceil(page), 0));
    hub.stop();
    assert_eq!(settled(&requests, 0), 0);
}

// Iroh native path charges: the request body is read straight into the
// operation's admitted allocation and the reply writer rides inside the paid
// finish callback, so both are on the request parent while selection runs and
// every byte returns afterwards. The Iroh client writes an admitted body from
/// One native reply-control event of the Iroh serve loop with the request
/// parent's retained bytes at that moment.
#[cfg(feature = "iroh")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplyStep {
    BeforeAdmission,
    Admitted { subject: usize, body: usize },
    BeforeCharge,
    Charged { control: usize, edge: usize },
    Held,
}

#[cfg(feature = "iroh")]
type ReplyControls = Vec<(ReplyStep, u64)>;

/// Further holders of live reply controls the probe was handed.
#[cfg(feature = "iroh")]
type HeldControls = Arc<std::sync::Mutex<Vec<Box<dyn std::any::Any + Send>>>>;

/// Record each native reply-control event with the request parent's retained
/// bytes. With `held`, every live control handed to the probe is kept there
/// as a further holder; otherwise it is dropped at once.
#[cfg(feature = "iroh")]
fn record_reply_controls(
    requests: &Arc<DeclaredReadMemory>,
    held: Option<HeldControls>,
) -> Arc<std::sync::Mutex<ReplyControls>> {
    use crate::transport::iroh::NativeReplyEvent as Event;
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    *crate::transport::iroh::NATIVE_REPLY_PROBE.lock().unwrap() = Some(Box::new({
        let seen = seen.clone();
        let requests = requests.clone();
        move |event| {
            let step = match event {
                Event::BeforeAdmission => ReplyStep::BeforeAdmission,
                Event::Admitted { subject, body } => ReplyStep::Admitted { subject, body },
                Event::BeforeCharge => ReplyStep::BeforeCharge,
                Event::Charged { control, edge } => ReplyStep::Charged { control, edge },
                Event::Hold(control) => {
                    if let Some(held) = &held {
                        held.lock().unwrap().push(control);
                    }
                    ReplyStep::Held
                }
            };
            seen.lock()
                .unwrap()
                .push((step, requests.retained_bytes_for_test()))
        }
    }));
    seen
}

/// Stop recording and check one native request: its subject and body, then
/// its reply-control block and the request's copy of the authenticated edge, are
/// each charged to the request before they exist, by exactly their sizes.
#[cfg(feature = "iroh")]
fn settled_reply_controls(seen: &std::sync::Mutex<ReplyControls>, edge_node: &str) -> String {
    *crate::transport::iroh::NATIVE_REPLY_PROBE.lock().unwrap() = None;
    let seen = std::mem::take(&mut *seen.lock().unwrap());
    let &[
        (ReplyStep::BeforeAdmission, before_admission),
        (ReplyStep::Admitted { subject, body }, admitted),
        (ReplyStep::BeforeCharge, before_charge),
        (ReplyStep::Charged { control, edge }, charged),
        (ReplyStep::Held, _),
    ] = seen.as_slice()
    else {
        panic!("one native request admits and charges its controls once: {seen:?}");
    };
    assert_eq!(subject, delivery_outcomes_subject(TENANT).len());
    assert_eq!(
        admitted - before_admission,
        (subject + body) as u64,
        "the subject and body are charged to the request"
    );
    assert_eq!(
        control,
        crate::transport::iroh::native_reply_control_bytes_for_test()
    );
    assert_eq!(edge, edge_node.len());
    assert_eq!(
        charged - before_charge,
        (control + edge) as u64,
        "the control block and the request's edge copy are charged to the request"
    );
    format!("subject={subject} body={body} control_block={control} edge_copy={edge}")
}

// its own allocation, without the Legacy branch's materialized second copy.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_iroh_native_path_charges_its_body_and_reply_writer_to_the_request() {
    let dir = tempfile::tempdir().expect("store directory");
    let tenant = TenantId::from(TENANT);
    let endpoint = crate::transport::iroh::IrohServer::bind(&format!(
        "iroh:?identity={}",
        dir.path().join("hub.key").display()
    ))
    .await
    .expect("bind the hub endpoint");
    let hub_db = open(&dir.path().join("hub.db"));
    hub_db
        .__seed_tenant_table_policies_for_test(tenant.clone(), expectation())
        .expect("declare the tenant policy");
    let requests = DeclaredReadMemory::from_reader_limit(LIMIT);
    let server = Arc::new(
        SyncServer::new_with_read_budget_provider(
            hub_db.clone(),
            &endpoint,
            tenant.clone(),
            Arc::new(requests.clone()),
        )
        .expect("hub"),
    );
    let stop = server
        .native_stop(Arc::new(AtomicBool::new(false)))
        .expect("prepare the native delivery installation");
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until_stopped(stop).await }
    });
    let edge_db = open(&dir.path().join("edge.db"));
    let dial = crate::transport::peer_dial_spec(&endpoint.ticket(), &dir.path().join("edge.key"));
    let client = SyncClient::new(edge_db.clone(), &dial, tenant.clone()).expect("edge client");
    within(client.bind_application_table_policy(expectation()))
        .await
        .expect("binding over Iroh");
    let reader = edge_db.declared_reader_memory().expect("edge reader");
    let tx = edge_db.begin().expect("begin");
    let mut keys = Vec::new();
    for _ in 0..3 {
        let id = uuid::Uuid::new_v4();
        edge_db
            .insert_row(
                tx,
                ROOT_TABLE,
                HashMap::from([
                    ("id".to_string(), Value::Uuid(id)),
                    ("body".to_string(), Value::Text("unit".into())),
                ]),
            )
            .expect("root row");
        let key = NaturalKey::single("id".to_string(), Value::Uuid(id));
        client
            .__stage_delivery_manifest_for_test(
                tx,
                DeliveryManifest {
                    root_table: ROOT_TABLE,
                    root_key: key.clone(),
                    members: Vec::new(),
                },
                reader.clone(),
            )
            .expect("signed manifest");
        keys.push(key);
    }
    edge_db.commit(tx).expect("commit");
    within(client.__seed_hub_delivery_roots_for_test(ROOT_TABLE, Some(&keys), reader.clone()))
        .await
        .expect("hub records the outcomes");

    // The request the edge sends: same fields and sizes as the client's own.
    let edge_incarnation = edge_db
        .existing_sync_incarnation(&tenant, reader.clone())
        .expect("edge incarnation read")
        .expect("bound edge has an incarnation");
    let request_len = encode_named(
        MessageType::FetchDeliveryOutcomesRequest,
        &FetchDeliveryOutcomesRequest {
            format: 1,
            tenant_id: TENANT.into(),
            edge_incarnation,
            request_nonce: [0; 16],
            since: None,
        },
    )
    .expect("encode request")
    .len() as u64;
    let during = Arc::new(std::sync::Mutex::new(None));
    *crate::sync_native_delivery::AFTER_SELECTION_ANY_THREAD
        .lock()
        .unwrap() = Some(Box::new({
        let during = during.clone();
        let requests = requests.clone();
        move |_: &crate::owner_read::NativeReadRequest| {
            *during.lock().unwrap() = Some(requests.retained_bytes_for_test())
        }
    }));
    let controls = record_reply_controls(&requests, None);
    let fetched = within(client.fetch_delivery_outcomes(None))
        .await
        .expect("native delivery over Iroh");
    let during = during.lock().unwrap().expect("selection ran");
    let writer = crate::transport::iroh::reply_writer_bytes_for_test() as u64;
    let after = settled(&requests, 0);
    let edge_node = crate::identity::FabricIdentity::load_or_generate(&dir.path().join("edge.key"))
        .expect("edge identity")
        .node_id();
    let controls = settled_reply_controls(&controls, &edge_node);

    // Client side: the same 64 KiB body through the Legacy branch and through
    // the admitted request, each against an unrouted subject on a fresh reader.
    let transport = crate::transport::client_transport(&crate::transport::peer_dial_spec(
        &endpoint.ticket(),
        &dir.path().join("probe.key"),
    ));
    let probe = vec![7u8; 64 * 1024];
    let mut peaks = Vec::new();
    for admitted in [false, true] {
        let parent = DeclaredReadMemory::from_reader_limit(LIMIT);
        let child = parent.child().expect("probe reader");
        let body = crate::transport::BoundedWireBody::from_legacy_reply(
            probe.clone(),
            contextdb_core::read_memory::ReadMemoryOwner::from_legacy(child.clone()),
        )
        .expect("probe body");
        let result = if admitted {
            match transport.request_stream_admitted("s4.unrouted", body, Duration::from_secs(30)) {
                Ok(future) => future.await.map(drop),
                Err(error) => Err(error),
            }
        } else {
            transport
                .request_stream("s4.unrouted", body, Duration::from_secs(30))
                .await
                .map(drop)
        };
        assert!(result.is_err(), "an unrouted subject has no responder");
        drop(child);
        peaks.push(parent.file_child_high_water());
    }
    println!(
        "S4_IROH_CHARGES outcomes={} request_len={} reply_writer_inline={} {controls} request_parent_during_selection={} request_parent_after={} client_probe_len={} client_peak(legacy_branch,admitted)=({},{})",
        fetched.len(),
        request_len,
        writer,
        during,
        after,
        probe.len(),
        peaks[0],
        peaks[1],
    );
    assert_eq!(fetched.len(), 3);
    assert!(
        during >= request_len + writer,
        "body and writer are on the request"
    );
    assert_eq!(after, 0);
    // The admitted request holds its body plus its own admitted driver frame,
    // never a second body-sized copy.
    assert!(
        peaks[1] >= probe.len() as u64 && peaks[1] - (probe.len() as u64) < 16 * 1024,
        "admitted request holds only its body and frame"
    );
    assert_eq!(
        peaks[0],
        2 * probe.len() as u64,
        "Legacy branch materializes a copy"
    );
    assert_eq!(server.delivery_route_counts_for_test(), (1, 0));
    within(client.shutdown()).await;
    within(transport.shutdown()).await.ok();
    stop.request_stop();
    within(task).await.expect("hub stops");
}

/// Companions keep a roomy reader; native delivery can be pointed at a tight
/// one, so a refusal is exercised on the real route without starving binding.
#[derive(Debug)]
struct SwitchableProvider {
    roomy: Arc<DeclaredReadMemory>,
    tight: std::sync::Mutex<Arc<DeclaredReadMemory>>,
    use_tight: AtomicBool,
}
impl crate::vector_read_memory::ReadRequestBudgetProvider for SwitchableProvider {
    fn request_budget(
        &self,
    ) -> contextdb_core::Result<Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>> {
        crate::vector_read_memory::ReadRequestBudgetProvider::request_budget(&self.roomy)
    }
    fn request_budget_owner(
        &self,
    ) -> contextdb_core::Result<crate::vector_read_memory::RequestReadOwner> {
        let parent = if self.use_tight.load(Ordering::SeqCst) {
            self.tight.lock().unwrap().clone()
        } else {
            self.roomy.clone()
        };
        crate::vector_read_memory::ReadRequestBudgetProvider::request_budget_owner(&parent)
    }
    fn request_budget_owner_native(
        &self,
    ) -> std::result::Result<
        crate::vector_read_memory::RequestReadOwner,
        crate::vector_read_memory::RequestOwnerFailure,
    > {
        let parent = if self.use_tight.load(Ordering::SeqCst) {
            self.tight.lock().unwrap().clone()
        } else {
            self.roomy.clone()
        };
        crate::vector_read_memory::ReadRequestBudgetProvider::request_budget_owner_native(&parent)
    }
}

// Finding 4: over the real Iroh connection, a memory refusal before the body
// is held, a memory refusal during selection and a cancellation after
// selection each reach the edge as the same typed error, with no success
// body, and the hub keeps none of the request's bytes.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_iroh_native_path_answers_refusal_and_cancellation_with_typed_errors() {
    let dir = tempfile::tempdir().expect("store directory");
    let tenant = TenantId::from(TENANT);
    let endpoint = crate::transport::iroh::IrohServer::bind(&format!(
        "iroh:?identity={}",
        dir.path().join("hub.key").display()
    ))
    .await
    .expect("bind the hub endpoint");
    let hub_db = open(&dir.path().join("hub.db"));
    hub_db
        .__seed_tenant_table_policies_for_test(tenant.clone(), expectation())
        .expect("declare the tenant policy");
    let provider = Arc::new(SwitchableProvider {
        roomy: DeclaredReadMemory::from_reader_limit(LIMIT),
        tight: std::sync::Mutex::new(DeclaredReadMemory::from_reader_limit(LIMIT)),
        use_tight: AtomicBool::new(false),
    });
    let server = Arc::new(
        SyncServer::new_with_read_budget_provider(
            hub_db.clone(),
            &endpoint,
            tenant.clone(),
            provider.clone(),
        )
        .expect("hub"),
    );
    let stop = server
        .native_stop(Arc::new(AtomicBool::new(false)))
        .expect("prepare the native delivery installation");
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until_stopped(stop).await }
    });
    let edge_db = open(&dir.path().join("edge.db"));
    let dial = crate::transport::peer_dial_spec(&endpoint.ticket(), &dir.path().join("edge.key"));
    let client = SyncClient::new(edge_db.clone(), &dial, tenant.clone()).expect("edge client");
    within(client.bind_application_table_policy(expectation()))
        .await
        .expect("binding over Iroh");
    let reader = edge_db.declared_reader_memory().expect("edge reader");
    let tx = edge_db.begin().expect("begin");
    let mut keys = Vec::new();
    for _ in 0..3 {
        let id = uuid::Uuid::new_v4();
        edge_db
            .insert_row(
                tx,
                ROOT_TABLE,
                HashMap::from([
                    ("id".to_string(), Value::Uuid(id)),
                    ("body".to_string(), Value::Text("unit".into())),
                ]),
            )
            .expect("root row");
        let key = NaturalKey::single("id".to_string(), Value::Uuid(id));
        client
            .__stage_delivery_manifest_for_test(
                tx,
                DeliveryManifest {
                    root_table: ROOT_TABLE,
                    root_key: key.clone(),
                    members: Vec::new(),
                },
                reader.clone(),
            )
            .expect("signed manifest");
        keys.push(key);
    }
    edge_db.commit(tx).expect("commit");
    within(client.__seed_hub_delivery_roots_for_test(ROOT_TABLE, Some(&keys), reader.clone()))
        .await
        .expect("hub records the outcomes");
    let accepted = || {
        edge_db
            .delivery_status(ROOT_TABLE)
            .expect("edge delivery status")
            .accepted
    };

    let mut lines = Vec::new();
    // 1. Memory refusal during selection, then before the body is held.
    for (case, limit) in [("selection", 20_000u64), ("body", 100u64)] {
        let tight = DeclaredReadMemory::from_reader_limit(limit);
        *provider.tight.lock().unwrap() = tight.clone();
        provider.use_tight.store(true, Ordering::SeqCst);
        let before = server.delivery_route_counts_for_test();
        let refused = within(client.fetch_delivery_outcomes(None)).await;
        provider.use_tight.store(false, Ordering::SeqCst);
        let after_counts = server.delivery_route_counts_for_test();
        let kind = match &refused {
            Err(contextdb_core::Error::MemoryBudgetExceeded {
                subsystem,
                operation,
                ..
            }) => format!("MemoryBudgetExceeded({subsystem}/{operation})"),
            other => panic!("{case}: expected a typed memory refusal, got {other:?}"),
        };
        let retained = settled(&tight, 0);
        lines.push(format!(
            "{case}: limit={limit} error={kind} native_dispatches={} accepted={} tight_parent_after={retained}",
            after_counts.0 - before.0,
            accepted(),
        ));
        assert_eq!(retained, 0);
        assert_eq!(accepted(), 0, "no success body was admitted");
    }

    // 2. Cancellation after selection on the hub's own worker thread.
    *crate::sync_native_delivery::AFTER_SELECTION_ANY_THREAD
        .lock()
        .unwrap() = Some(Box::new(
        |operation: &crate::owner_read::NativeReadRequest| operation.cancellation().cancel(),
    ));
    let controls = record_reply_controls(&provider.roomy, None);
    let cancelled = within(client.fetch_delivery_outcomes(None)).await;
    assert!(
        matches!(cancelled, Err(contextdb_core::Error::ReadCancelled)),
        "expected a typed cancellation, got {cancelled:?}"
    );
    let roomy_after = settled(&provider.roomy, 0);
    let edge_node = crate::identity::FabricIdentity::load_or_generate(&dir.path().join("edge.key"))
        .expect("edge identity")
        .node_id();
    let controls = settled_reply_controls(&controls, &edge_node);
    lines.push(format!(
        "cancel: error=ReadCancelled accepted={} roomy_parent_after={roomy_after} {controls}",
        accepted()
    ));
    assert_eq!(accepted(), 0);
    assert_eq!(roomy_after, 0);

    // 3. The same hub then serves the request normally.
    let fetched = within(client.fetch_delivery_outcomes(None))
        .await
        .expect("native delivery after the refusals");
    lines.push(format!(
        "after: outcomes={} accepted={} roomy_parent_after={}",
        fetched.len(),
        accepted(),
        settled(&provider.roomy, 0)
    ));
    println!("S4_IROH_TYPED {}", lines.join(" | "));
    assert_eq!(fetched.len(), 3);
    assert_eq!(server.delivery_route_counts_for_test().1, 0);
    within(client.shutdown()).await;
    stop.request_stop();
    within(task).await.expect("hub stops");
}

// Finding 5: the Iroh native root is retired by whichever way its serve call
// ends: dropped unpolled, refused because another root is live, cancelled
// mid-serve, or stopped normally. Installation bytes return to the baseline
// while the endpoint itself stays alive, and a refused second serve never
// retires the live one.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_iroh_native_root_is_retired_by_every_way_its_serve_ends() {
    let dir = tempfile::tempdir().expect("store directory");
    let tenant = TenantId::from(TENANT);
    let hub_db = open(&dir.path().join("hub.db"));
    hub_db
        .__seed_tenant_table_policies_for_test(tenant.clone(), expectation())
        .expect("declare the tenant policy");
    let installs = hub_db.declared_read_memory_for_test();
    let requests = DeclaredReadMemory::from_reader_limit(LIMIT);
    let bind = |name: &str| {
        let path = dir.path().join(format!("{name}.key"));
        async move {
            crate::transport::iroh::IrohServer::bind(&format!("iroh:?identity={}", path.display()))
                .await
                .expect("bind a hub endpoint")
        }
    };
    let server_on = |endpoint: &crate::transport::iroh::IrohServer| {
        Arc::new(
            SyncServer::new_with_read_budget_provider(
                hub_db.clone(),
                endpoint,
                tenant.clone(),
                Arc::new(requests.clone()),
            )
            .expect("hub"),
        )
    };
    let prepared = |server: &SyncServer| {
        let stop = server
            .native_stop(Arc::new(AtomicBool::new(false)))
            .expect("prepare a native delivery installation");
        let installation = server
            .native_delivery
            .lock()
            .unwrap()
            .take()
            .expect("installed native delivery route");
        (stop, installation)
    };
    let edge_db = open(&dir.path().join("edge.db"));
    let mut lines = Vec::new();

    let endpoint = bind("hub").await;
    let server = server_on(&endpoint);
    let transport = endpoint.transport();
    let baseline = installs.retained_bytes_for_test();

    // 1. Dropped before its first poll.
    let (stop, installation) = prepared(&server);
    let charged = installs.retained_bytes_for_test();
    let unpolled = transport
        .serve_native_installation(Vec::new(), installation, stop)
        .expect("serve future");
    drop(unpolled);
    lines.push(format!(
        "unpolled: installation_charge={} after_drop_delta={}",
        charged - baseline,
        installs.retained_bytes_for_test() as i128 - baseline as i128
    ));
    assert_eq!(installs.retained_bytes_for_test(), baseline);

    // 2. A live serve, then a second serve refused while the first root lives.
    let (stop, installation) = prepared(&server);
    let live = tokio::spawn({
        let transport = transport.clone();
        let stop = stop.clone();
        async move {
            transport
                .serve_native_installation(Vec::new(), installation, stop)?
                .await
        }
    });
    let client = SyncClient::new(
        edge_db.clone(),
        &crate::transport::peer_dial_spec(&endpoint.ticket(), &dir.path().join("edge.key")),
        tenant.clone(),
    )
    .expect("edge client");
    // An unbound edge is answered (and refused) by the native route itself;
    // the native dispatch count is what proves the root is live.
    let first = within(client.fetch_delivery_outcomes(None)).await;
    let live_charge = installs.retained_bytes_for_test();
    let (second_stop, second) = prepared(&server);
    let refused = transport
        .serve_native_installation(Vec::new(), second, second_stop)
        .expect("serve future")
        .await;
    assert!(
        refused.is_err(),
        "a second root is refused while one is live"
    );
    let after_refusal = installs.retained_bytes_for_test();
    let again = within(client.fetch_delivery_outcomes(None)).await;
    lines.push(format!(
        "refused_second: live_root_charge={} after_refusal_delta={} answers=({:?},{:?}) native_requests={}",
        live_charge - baseline,
        after_refusal as i128 - live_charge as i128,
        first.map(|outcomes| outcomes.len()),
        again.map(|outcomes| outcomes.len()),
        server.delivery_route_counts_for_test().0
    ));
    assert_eq!(after_refusal, live_charge);
    assert_eq!(server.delivery_route_counts_for_test().0, 2);

    // 3. Cancelled mid-serve: the task is aborted while the root is live.
    live.abort();
    let aborted = live.await;
    assert!(aborted.is_err_and(|error| error.is_cancelled()));
    drop(stop);
    lines.push(format!(
        "cancelled: after_abort_delta={} endpoint_alive={}",
        installs.retained_bytes_for_test() as i128 - baseline as i128,
        !endpoint.node_id().is_empty()
    ));
    assert_eq!(installs.retained_bytes_for_test(), baseline);
    within(client.shutdown()).await;

    // 4. Stopped normally through run_until_stopped on a fresh endpoint.
    let endpoint = bind("hub-2").await;
    let server = server_on(&endpoint);
    let stop = server
        .native_stop(Arc::new(AtomicBool::new(false)))
        .expect("prepare the native delivery installation");
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until_stopped(stop).await }
    });
    let client = SyncClient::new(
        edge_db.clone(),
        &crate::transport::peer_dial_spec(&endpoint.ticket(), &dir.path().join("edge-2.key")),
        tenant.clone(),
    )
    .expect("edge client");
    let _answered = within(client.fetch_delivery_outcomes(None)).await;
    assert_eq!(server.delivery_route_counts_for_test().0, 1);
    within(client.shutdown()).await;
    stop.request_stop();
    drop(stop);
    within(task).await.expect("hub stops");
    lines.push(format!(
        "stopped: after_stop_delta={}",
        installs.retained_bytes_for_test() as i128 - baseline as i128
    ));
    println!("S4_IROH_RETIRE {}", lines.join(" | "));
    assert_eq!(installs.retained_bytes_for_test(), baseline);
}

// Finding 9: native delivery is scoped to file-backed hubs, because native
// custody selection reads the durable committed root. An in-memory hub driven
// by run_until_stopped keeps its Legacy delivery handler and answers exactly
// as before; it gets no native installation and no native dispatch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_in_memory_hub_keeps_legacy_delivery_under_run_until_stopped() {
    let world = World::with_hub_store(true);
    assert!(world.hub_db.is_memory_backed());
    let requests = DeclaredReadMemory::from_reader_limit(LIMIT);
    let server = world.server(&requests);
    let hub = start_native(&world, &server).await;
    assert!(
        server.native_delivery.lock().unwrap().is_none(),
        "no native installation is prepared for an in-memory hub"
    );
    world.bind().await;
    world.seed_outcomes(3).await;
    let fetched = within(world.client.fetch_delivery_outcomes(None))
        .await
        .expect("Legacy delivery from an in-memory hub");
    let counts = server.delivery_route_counts_for_test();
    let legacy_exchanges = world
        .broker
        .recorded_exchanges()
        .iter()
        .filter(|exchange| exchange.subject == delivery_outcomes_subject(TENANT))
        .count();
    println!(
        "S4_MEMORY_HUB outcomes={} native={} legacy={} legacy_delivery_exchanges={legacy_exchanges}",
        fetched.len(),
        counts.0,
        counts.1
    );
    assert_eq!(fetched.len(), 3);
    assert_eq!(counts, (0, 1));
    assert_eq!(legacy_exchanges, 1);
    hub.stop();
}

// Policy (owner ruling): a sync hub is a durable store; a memory-backed hub is
// a test fixture, not a supported deployment shape. Each hub logs at
// registration which route serves its delivery and why: an in-memory hub is
// served by Legacy, and native delivery requires a durable store.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_hub_names_its_delivery_route_and_only_a_durable_hub_is_served_natively() {
    let mut lines = Vec::new();
    for (memory_hub, route, counts) in [(true, "legacy", (0, 1)), (false, "native", (1, 0))] {
        let world = World::with_hub_store(memory_hub);
        let requests = DeclaredReadMemory::from_reader_limit(LIMIT);
        let server = world.server(&requests);
        let hub = start_native(&world, &server).await;
        let (chosen, reason) = server.delivery_route_for_test();
        world.bind().await;
        world.seed_outcomes(3).await;
        let fetched = within(world.client.fetch_delivery_outcomes(None))
            .await
            .expect("delivery");
        let served = server.delivery_route_counts_for_test();
        lines.push(format!(
            "memory_hub={memory_hub} route={chosen} reason=\"{reason}\" outcomes={} served(native,legacy)={served:?}",
            fetched.len()
        ));
        assert_eq!(chosen, route);
        assert_eq!(fetched.len(), 3);
        assert_eq!(served, counts, "the logged route is the one that served");
        hub.stop();
    }
    println!("S4_HUB_ROUTE {}", lines.join(" | "));
}

/// A file-backed hub served natively over a real Iroh endpoint, with one
/// bound edge and three recorded outcomes.
#[cfg(feature = "iroh")]
struct IrohHub {
    dir: tempfile::TempDir,
    server: Arc<SyncServer>,
    stop: NativeStop,
    task: tokio::task::JoinHandle<()>,
    client: SyncClient,
    _edge_db: Arc<Database>,
    controller: crate::transport::iroh::LargeRequestTestController,
    ticket: String,
}

#[cfg(feature = "iroh")]
impl IrohHub {
    async fn start(requests: &Arc<DeclaredReadMemory>) -> Self {
        Self::start_with(Arc::new(requests.clone()), None).await
    }

    /// A hub whose requests are funded by `provider`, its reader declaring
    /// `page_bytes` per page when given.
    async fn start_with(
        provider: Arc<dyn crate::vector_read_memory::ReadRequestBudgetProvider>,
        page_bytes: Option<u64>,
    ) -> Self {
        Self::start_seeded(provider, page_bytes, 0).await
    }

    /// `start_with`, with one more outcome after the three when
    /// `large_members` is not zero: a root with that many member rows, whose
    /// diagnostics name every row, so it is much larger than the others.
    async fn start_seeded(
        provider: Arc<dyn crate::vector_read_memory::ReadRequestBudgetProvider>,
        page_bytes: Option<u64>,
        large_members: usize,
    ) -> Self {
        let dir = tempfile::tempdir().expect("store directory");
        let tenant = TenantId::from(TENANT);
        let endpoint = crate::transport::iroh::IrohServer::bind(&format!(
            "iroh:?identity={}",
            dir.path().join("hub.key").display()
        ))
        .await
        .expect("bind the hub endpoint");
        let hub_db = open_with_limits(
            &dir.path().join("hub.db"),
            page_bytes,
            (large_members > 0).then_some(600_000),
        );
        hub_db
            .__seed_tenant_table_policies_for_test(tenant.clone(), expectation())
            .expect("declare the tenant policy");
        let server = Arc::new(
            SyncServer::new_with_read_budget_provider(hub_db, &endpoint, tenant.clone(), provider)
                .expect("hub"),
        );
        let stop = server
            .native_stop(Arc::new(AtomicBool::new(false)))
            .expect("prepare the native delivery installation");
        let controller = endpoint.large_request_test_controller();
        let ticket = endpoint.ticket();
        let task = tokio::spawn({
            let server = server.clone();
            let stop = stop.clone();
            async move { server.run_until_stopped(stop).await }
        });
        let edge_db = open(&dir.path().join("edge.db"));
        let dial =
            crate::transport::peer_dial_spec(&endpoint.ticket(), &dir.path().join("edge.key"));
        let client = SyncClient::new(edge_db.clone(), &dial, tenant).expect("edge client");
        within(client.bind_application_table_policy(expectation()))
            .await
            .expect("binding over Iroh");
        let reader = edge_db.declared_reader_memory().expect("edge reader");
        let tx = edge_db.begin().expect("begin");
        let mut keys = Vec::new();
        for _ in 0..3 {
            let id = uuid::Uuid::new_v4();
            edge_db
                .insert_row(
                    tx,
                    ROOT_TABLE,
                    HashMap::from([
                        ("id".to_string(), Value::Uuid(id)),
                        ("body".to_string(), Value::Text("unit".into())),
                    ]),
                )
                .expect("root row");
            let key = NaturalKey::single("id".to_string(), Value::Uuid(id));
            client
                .__stage_delivery_manifest_for_test(
                    tx,
                    DeliveryManifest {
                        root_table: ROOT_TABLE,
                        root_key: key.clone(),
                        members: Vec::new(),
                    },
                    reader.clone(),
                )
                .expect("signed manifest");
            keys.push(key);
        }
        edge_db.commit(tx).expect("commit");
        within(client.__seed_hub_delivery_roots_for_test(ROOT_TABLE, Some(&keys), reader.clone()))
            .await
            .expect("hub records the outcomes");
        // A later batch, so its outcome follows the others in delivery order.
        if large_members > 0 {
            let tx = edge_db.begin().expect("begin");
            let root = uuid::Uuid::new_v4();
            edge_db
                .insert_row(
                    tx,
                    ROOT_TABLE,
                    HashMap::from([
                        ("id".to_string(), Value::Uuid(root)),
                        ("body".to_string(), Value::Text("large".into())),
                    ]),
                )
                .expect("large root row");
            let mut parts = Vec::with_capacity(large_members);
            for _ in 0..large_members {
                let id = uuid::Uuid::new_v4();
                edge_db
                    .insert_row(
                        tx,
                        MEMBER_TABLE,
                        HashMap::from([
                            ("id".to_string(), Value::Uuid(id)),
                            ("record_id".to_string(), Value::Uuid(root)),
                            ("body".to_string(), Value::Text("part".into())),
                        ]),
                    )
                    .expect("member row");
                parts.push((
                    MEMBER_TABLE,
                    NaturalKey::single("id".to_string(), Value::Uuid(id)),
                ));
            }
            let key = NaturalKey::single("id".to_string(), Value::Uuid(root));
            client
                .__stage_delivery_manifest_for_test(
                    tx,
                    DeliveryManifest {
                        root_table: ROOT_TABLE,
                        root_key: key.clone(),
                        members: parts,
                    },
                    reader.clone(),
                )
                .expect("signed large manifest");
            edge_db.commit(tx).expect("commit");
            within(client.__seed_hub_delivery_roots_for_test(ROOT_TABLE, Some(&[key]), reader))
                .await
                .expect("hub records the large outcome");
        }
        Self {
            dir,
            server,
            stop,
            task,
            client,
            _edge_db: edge_db,
            controller,
            ticket,
        }
    }

    fn edge_node(&self) -> String {
        crate::identity::FabricIdentity::load_or_generate(&self.dir.path().join("edge.key"))
            .expect("edge identity")
            .node_id()
    }

    async fn stop(self) {
        self.stop.request_stop();
        within(self.task).await.expect("hub stops");
        drop(self.server);
    }
}

// Review 2, finding 13: a native request's reply control is one block its
// operation paid for, and the charge follows the block's last holder. A
// further holder keeps the control alive after the request has answered and
// after the edge's connection has gone; the charge stays exactly that long and
// returns to zero only when that holder drops.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_iroh_native_reply_control_is_charged_until_its_last_holder_drops() {
    let requests = DeclaredReadMemory::from_reader_limit(LIMIT);
    let hub = IrohHub::start(&requests).await;
    let held: HeldControls = Arc::default();
    let controls = record_reply_controls(&requests, Some(held.clone()));
    let fetched = within(hub.client.fetch_delivery_outcomes(None))
        .await
        .expect("native delivery over Iroh");
    let edge_node = hub.edge_node();
    let steps = settled_reply_controls(&controls, &edge_node);
    // What the held control still owns: its block, the paid subject, and the
    // request's own reader owner, which the paid subject keeps alive.
    let owned = (crate::transport::iroh::native_reply_control_bytes_for_test()
        + delivery_outcomes_subject(TENANT).len()) as u64
        + ReadMemoryOwnerLayout::control_bytes();
    let while_held = settled(&requests, owned);
    within(hub.client.shutdown()).await;
    let after_disconnect = requests.retained_bytes_for_test();
    let holders = held.lock().unwrap().len();
    held.lock().unwrap().clear();
    let after_last_holder = requests.retained_bytes_for_test();
    println!(
        "S4_IROH_FINAL_HOLDER outcomes={} {steps} holders={holders} owned={owned} while_held={while_held} after_disconnect={after_disconnect} after_last_holder={after_last_holder}",
        fetched.len()
    );
    assert_eq!(fetched.len(), 3);
    assert_eq!(holders, 1);
    assert_eq!(while_held, owned, "the held control keeps its charge");
    assert_eq!(
        after_disconnect, owned,
        "the charge outlives the connection while a holder survives"
    );
    assert_eq!(after_last_holder, 0, "the last holder settles the charge");
    // The fixed per-connection residue (ruling E2) is measured and bounded.
    let connection_task = crate::transport::iroh::connection_task_bytes_for_test();
    println!(
        "S4_CONNECTION_RESIDUE task_frame={connection_task} bound={}",
        crate::transport::iroh::MAX_CONNECTION_TASK_BYTES
    );
    assert!(connection_task > 0);
    assert!(connection_task <= crate::transport::iroh::MAX_CONNECTION_TASK_BYTES);
    hub.stop().await;
}

// Review 2, finding 12: the Iroh client admits each reply frame on the
// request's own reader from its length header before allocating or reading
// it. A reader too small for the reply refuses with a typed memory refusal
// and the reply is never read or acknowledged; nothing stays charged after an
// answered, refused, unpolled or cancelled request.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_iroh_admitted_client_refuses_a_reply_its_reader_cannot_hold_before_reading_it() {
    use crate::transport::{BoundedWireBody, HandlerRegistration, IncomingRequest};
    use contextdb_core::read_memory::ReadMemoryOwner;
    const REPLY: usize = 256 * 1024;
    let dir = tempfile::tempdir().expect("store directory");
    let endpoint = crate::transport::iroh::IrohServer::bind(&format!(
        "iroh:?identity={}",
        dir.path().join("hub.key").display()
    ))
    .await
    .expect("bind the hub endpoint");
    let controller = endpoint.large_request_test_controller();
    let reached = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let large: crate::transport::RequestHandler = Arc::new(|incoming: IncomingRequest| {
        Box::pin(async move { (incoming.responder)(vec![7u8; REPLY]).await })
            as crate::transport::TransportFuture<'static, ()>
    });
    let stall: crate::transport::RequestHandler = Arc::new({
        let reached = reached.clone();
        let release = release.clone();
        move |incoming: IncomingRequest| {
            let reached = reached.clone();
            let release = release.clone();
            Box::pin(async move {
                reached.notify_one();
                release.notified().await;
                (incoming.responder)(vec![1u8; 8]).await
            }) as crate::transport::TransportFuture<'static, ()>
        }
    });
    let shutdown = Arc::new(AtomicBool::new(false));
    let serve = tokio::spawn({
        let transport = endpoint.transport();
        let shutdown = shutdown.clone();
        async move {
            transport
                .serve(
                    vec![
                        HandlerRegistration {
                            subject: "s4.large".into(),
                            handler: large,
                        },
                        HandlerRegistration {
                            subject: "s4.stall".into(),
                            handler: stall,
                        },
                    ],
                    shutdown,
                )
                .await
        }
    });
    let client = crate::transport::client_transport(&crate::transport::peer_dial_spec(
        &endpoint.ticket(),
        &dir.path().join("edge.key"),
    ));
    let body = |parent: &Arc<DeclaredReadMemory>| {
        let child = parent.child().expect("request reader");
        BoundedWireBody::from_legacy_reply(vec![1u8; 64], ReadMemoryOwner::from_legacy(child))
            .expect("request body")
    };
    let receipts = || controller.observations_for_test().successful_reply_receipts;

    // A roomy reader receives the reply, charged to it, and releases it.
    let roomy = DeclaredReadMemory::from_reader_limit(LIMIT);
    let reply = within(async {
        match client.request_stream_admitted("s4.large", body(&roomy), Duration::from_secs(60)) {
            Ok(future) => future.await,
            Err(error) => Err(error),
        }
    })
    .await
    .expect("the reply over Iroh");
    let roomy_reply = reply.contiguous_bytes().map(<[u8]>::len);
    let roomy_held = roomy.retained_bytes_for_test();
    drop(reply);
    let roomy_after = roomy.retained_bytes_for_test();

    // A tight reader refuses before the reply is read or acknowledged. The
    // hub records a receipt only after it reads the edge's acknowledgement,
    // which can trail the edge's own return, so the count settles first.
    within(controller.wait_until_requests_idle_for_test()).await;
    let before = receipts();
    let tight = DeclaredReadMemory::from_reader_limit(32 * 1024);
    let refused = within(async {
        match client.request_stream_admitted("s4.large", body(&tight), Duration::from_secs(60)) {
            Ok(future) => future.await.map(drop),
            Err(error) => Err(error),
        }
    })
    .await;
    let tight_peak = tight.file_child_high_water();
    let tight_after = tight.retained_bytes_for_test();
    // The connection serves its streams in order, so once the next request
    // answers, the hub has finished with the refused reply.
    let next = DeclaredReadMemory::from_reader_limit(LIMIT);
    within(async {
        match client.request_stream_admitted("s4.large", body(&next), Duration::from_secs(60)) {
            Ok(future) => future.await.map(drop),
            Err(error) => Err(error),
        }
    })
    .await
    .expect("the next reply on the same connection");
    within(controller.wait_until_requests_idle_for_test()).await;
    let receipts_for_refused_and_next = receipts() - before;

    // Dropped unpolled, and cancelled while the hub withholds its reply.
    let unpolled = DeclaredReadMemory::from_reader_limit(LIMIT);
    drop(
        client
            .request_stream_admitted("s4.large", body(&unpolled), Duration::from_secs(60))
            .expect("admit the request"),
    );
    let unpolled_after = unpolled.retained_bytes_for_test();
    let cancelled = DeclaredReadMemory::from_reader_limit(LIMIT);
    let mut pending = Box::pin(
        client
            .request_stream_admitted("s4.stall", body(&cancelled), Duration::from_secs(600))
            .expect("admit the request"),
    );
    tokio::select! {
        () = reached.notified() => {}
        answered = &mut pending => panic!("a withheld reply answered: {:?}", answered.map(drop)),
    }
    drop(pending);
    let cancelled_after = cancelled.retained_bytes_for_test();
    release.notify_one();

    println!(
        "S4_IROH_CLIENT_ADMISSION reply={REPLY} roomy(reply,held,after)=({roomy_reply:?},{roomy_held},{roomy_after}) tight(peak,after)=({tight_peak},{tight_after}) refused={refused:?} receipts_for_refused_and_next={receipts_for_refused_and_next} unpolled_after={unpolled_after} cancelled_after={cancelled_after}"
    );
    assert_eq!(roomy_reply, Some(REPLY));
    assert!(
        roomy_held >= REPLY as u64,
        "the reply is charged to its reader"
    );
    assert_eq!(roomy_after, 0);
    assert!(
        matches!(
            &refused,
            Err(crate::transport::WireError::Resource(contextdb_core::Error::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes: REPLY,
                ..
            })) if subsystem == "sync_transport" && operation == "materialize_body"
        ),
        "the reply length is refused on the reader before allocation: {refused:?}"
    );
    assert!(
        tight_peak < REPLY as u64,
        "the refused reply was never held"
    );
    assert_eq!(tight_after, 0);
    assert_eq!(
        receipts_for_refused_and_next, 1,
        "only the next reply was read and acknowledged"
    );
    assert_eq!(unpolled_after, 0);
    assert_eq!(cancelled_after, 0);
    within(client.shutdown()).await.ok();
    shutdown.store(true, Ordering::SeqCst);
    within(serve).await.expect("serve task").expect("serve");
}

// Review 2, finding 4 (round 3 shape): a native read the owner's route cannot
// answer, such as a durable custody index without its completion marker, is a
// heap-free native terminal. Its borrowed wire encoding is byte-identical to
// the owned typed refusal, and it decodes to the shared typed read refusal
// naming what was refused.
#[test]
fn an_unsupported_native_custody_read_refuses_with_a_typed_owner_route_refusal() {
    use contextdb_core::read_contract::{ReadFailure, ReadFailureKind};
    use contextdb_core::read_memory::NativeRefusal;
    let error = crate::transport::native_delivery_error(
        crate::custody::native_selection::Failure::Refused(NativeRefusal::NativeUnsupported {
            subsystem: "custody",
            operation: "durable_group_index",
        }),
    );
    let crate::transport::WireError::Native(terminal) = &error else {
        panic!("expected a native terminal, got {error:?}");
    };
    assert!(matches!(
        terminal,
        crate::transport::NativeTerminal::Refused(NativeRefusal::NativeUnsupported {
            subsystem: "custody",
            operation: "durable_group_index",
        })
    ));
    let mut encoded = [0u8; crate::transport::TERMINAL_BYTES];
    let length = crate::transport::encode_terminal(terminal, &mut encoded).expect("typed");
    let expected = ReadFailure::owner_route_unsupported("custody/durable_group_index".into());
    let owned = crate::transport::WireRefusal::from_error(&contextdb_core::Error::ReadFailure(
        expected.clone(),
    ))
    .expect("a typed refusal")
    .encode()
    .expect("encoded refusal");
    assert_eq!(
        &encoded[..length],
        owned.as_slice(),
        "byte-identical to the owned refusal"
    );
    let crossed = crate::transport::WireRefusal::decode(&encoded[..length])
        .expect("decoded refusal")
        .into_error();
    let contextdb_core::Error::ReadFailure(crossed) = crossed else {
        panic!("the refusal lost its kind across the wire: {crossed:?}");
    };
    println!(
        "S4_TYPED_OWNER_ROUTE kind={:?} wire_len={length}",
        crossed.kind()
    );
    assert_eq!(crossed.kind(), ReadFailureKind::OwnerRouteUnsupported);
    assert_eq!(crossed, expected);
}

// Round 3, R4: every typed native terminal reachable on the route encodes,
// borrowed, to exactly the owned typed refusal's bytes, and within
// TERMINAL_BYTES even when its strings reach the audited bound.
#[test]
fn every_typed_native_terminal_encodes_like_the_owned_refusal_within_the_bound() {
    use crate::transport::{NativeTerminal, TERMINAL_BYTES, WireRefusal, encode_terminal};
    use contextdb_core::read_contract::{OwnerLimitExceededDetail, ReadFailure, ReadFailureLimit};
    use contextdb_core::read_memory::NativeRefusal;
    const LONG: &str = concat!(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
    assert_eq!(LONG.len(), crate::transport::MAX_TERMINAL_STR);
    let limit = ReadFailure::owner_limit_exceeded(OwnerLimitExceededDetail {
        limit: ReadFailureLimit::CursorPageBytes,
        value: u64::MAX,
        required: None,
        statement: None,
    });
    let cases: Vec<(&str, NativeTerminal, contextdb_core::Error)> = vec![
        (
            "memory",
            NativeTerminal::Refused(NativeRefusal::MemoryBudgetExceeded {
                subsystem: LONG,
                operation: LONG,
                requested_bytes: usize::MAX,
                available_bytes: usize::MAX,
                budget_limit_bytes: usize::MAX,
                hint: LONG,
            }),
            contextdb_core::Error::MemoryBudgetExceeded {
                subsystem: LONG.into(),
                operation: LONG.into(),
                requested_bytes: usize::MAX,
                available_bytes: usize::MAX,
                budget_limit_bytes: usize::MAX,
                hint: LONG.into(),
            },
        ),
        (
            "process",
            NativeTerminal::Refused(NativeRefusal::ProcessMemoryBudgetExceeded {
                operation: LONG,
                requested_bytes: usize::MAX,
                available_bytes: usize::MAX,
                budget_limit_bytes: usize::MAX,
                hint: LONG,
            }),
            contextdb_core::Error::MemoryBudgetExceeded {
                subsystem: "process_native_ceiling".into(),
                operation: LONG.into(),
                requested_bytes: usize::MAX,
                available_bytes: usize::MAX,
                budget_limit_bytes: usize::MAX,
                hint: LONG.into(),
            },
        ),
        (
            "owner_route",
            NativeTerminal::Refused(NativeRefusal::NativeUnsupported {
                subsystem: LONG,
                operation: LONG,
            }),
            contextdb_core::Error::ReadFailure(ReadFailure::owner_route_unsupported(format!(
                "{LONG}/{LONG}"
            ).into())),
        ),
        (
            "owner_limit",
            NativeTerminal::Kernel(contextdb_core::Error::ReadFailure(limit.clone())),
            contextdb_core::Error::ReadFailure(limit),
        ),
        (
            "cancelled",
            NativeTerminal::Cancelled,
            contextdb_core::Error::ReadCancelled,
        ),
    ];
    let mut lines = Vec::new();
    for (name, terminal, expected) in cases {
        let mut encoded = [0u8; TERMINAL_BYTES];
        let length = encode_terminal(&terminal, &mut encoded)
            .unwrap_or_else(|| panic!("{name}: typed terminal fits TERMINAL_BYTES"));
        let owned = WireRefusal::from_error(&expected)
            .expect("typed")
            .encode()
            .expect("encoded");
        assert_eq!(
            &encoded[..length],
            owned.as_slice(),
            "{name}: byte-identical"
        );
        lines.push(format!("{name}={length}"));
    }
    println!(
        "S4_TERMINAL_BYTES bound={TERMINAL_BYTES} {}",
        lines.join(" ")
    );
    // A class has no typed refusal: the encoder declines and the class answers.
    let mut encoded = [0u8; TERMINAL_BYTES];
    let class = NativeTerminal::Class(crate::transport::terminal_class::FAULT);
    assert_eq!(encode_terminal(&class, &mut encoded), None);
    assert_eq!(
        crate::transport::terminal_class_of(&class),
        "sync.native.fault"
    );
}

// Round 3, R4 audit: every string a native refusal producer can supply stays
// within MAX_TERMINAL_STR. The audit reads the literal arguments of every
// native admission call and NativeRefusal construction, and every `&str` const,
// in the crates that produce native refusals.
#[test]
fn native_refusal_strings_fit_the_terminal_bound() {
    let engine = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let crates = engine.parent().expect("crates directory");
    let roots = [
        crates.join("contextdb-core/src"),
        crates.join("contextdb-engine/src"),
        crates.join("contextdb-redb/src"),
        crates.join("contextdb-vector/src"),
    ];
    const CALLS: [&str; 6] = [
        "try_new_owned_native",
        "try_reserve_native",
        "try_grow_for_native",
        "reserve_copy_native",
        "admit_on",
        "NativeRefusal::",
    ];
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("read source directory") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }
    fn literals(text: &str) -> Vec<String> {
        let bytes = text.as_bytes();
        let mut found = Vec::new();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'"' && (i == 0 || (bytes[i - 1] != b'\'' && bytes[i - 1] != b'#')) {
                let mut j = i + 1;
                let mut value = String::new();
                while j < bytes.len() && bytes[j] != b'"' {
                    if bytes[j] == b'\\' {
                        j += 1;
                    }
                    if j < bytes.len() {
                        value.push(bytes[j] as char);
                    }
                    j += 1;
                }
                found.push(value);
                i = j + 1;
            } else {
                i += 1;
            }
        }
        found
    }
    let mut files = Vec::new();
    for root in &roots {
        if root.is_dir() {
            walk(root, &mut files);
        }
    }
    assert!(files.len() > 50, "the audit walks the producing crates");
    let mut longest = (0usize, String::new());
    let mut sites = 0usize;
    for path in files {
        let name = path.to_string_lossy().to_string();
        if name.contains("test") {
            continue;
        }
        let source = std::fs::read_to_string(&path).expect("read source");
        for call in CALLS {
            let mut from = 0;
            while let Some(found) = source[from..].find(call) {
                let start = from + found + call.len();
                let rest = &source[start..];
                let Some(open) = rest.find(['(', '{']) else {
                    break;
                };
                if rest[..open]
                    .trim()
                    .contains(|c: char| c.is_whitespace() || c == ';')
                {
                    from = start;
                    continue;
                }
                let mut depth = 0i32;
                let mut end = start + open;
                for (offset, c) in source[start + open..].char_indices() {
                    match c {
                        '(' | '{' | '[' => depth += 1,
                        ')' | '}' | ']' => {
                            depth -= 1;
                            if depth == 0 {
                                end = start + open + offset;
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                sites += 1;
                for literal in literals(&source[start + open..end]) {
                    if literal.len() > longest.0 {
                        longest = (literal.len(), format!("{name}: {literal}"));
                    }
                }
                from = end.max(start);
            }
        }
        for line in source.lines() {
            let line = line.trim_start();
            if (line.starts_with("const ")
                || line.starts_with("pub(crate) const ")
                || line.starts_with("pub const "))
                && (line.contains(": &str") || line.contains(": &'static str"))
            {
                for literal in literals(line) {
                    if literal.len() > longest.0 {
                        longest = (literal.len(), format!("{name}: {literal}"));
                    }
                }
            }
        }
    }
    println!(
        "S4_TERMINAL_AUDIT sites={sites} longest={} bound={}",
        longest.0,
        crate::transport::MAX_TERMINAL_STR
    );
    assert!(sites > 30, "the audit finds the native admission sites");
    assert!(
        longest.0 <= crate::transport::MAX_TERMINAL_STR,
        "a native refusal string exceeds the terminal bound: {}",
        longest.1
    );
}

// Round 3, R3: a native page is the longest prefix, in delivery order, whose
// reply stays within the reader's declared page bytes. With room for one
// outcome per page, three outcomes arrive as three pages, in order, each
// releasing every request byte, the last reporting no more.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_declared_page_byte_bound_cuts_native_pages_in_delivery_order() {
    let world = World::with_page_bytes(9_000);
    let requests = DeclaredReadMemory::from_reader_limit(LIMIT);
    let server = world.server(&requests);
    let hub = start_legacy(&world, &server).await;
    world.bind().await;
    world.seed_outcomes(3).await;
    hub.stop().await;
    let pages = native_pages(&world, None);
    println!(
        "S4_PAGE_BYTES bound=9000 pages={} outcomes={:?} more={:?} released={:?}",
        pages.len(),
        pages.iter().map(|page| page.outcomes).collect::<Vec<_>>(),
        pages.iter().map(|page| page.more).collect::<Vec<_>>(),
        pages.iter().map(|page| page.released).collect::<Vec<_>>(),
    );
    assert_eq!(pages.len(), 3, "one outcome per page");
    assert!(pages.iter().all(|page| page.outcomes == 1));
    assert_eq!(
        pages.iter().map(|page| page.more).collect::<Vec<_>>(),
        vec![true, true, false]
    );
    assert!(pages.iter().all(|page| page.released));
}

// Round 3, R3: a page that cannot fit refuses with the typed CursorPageBytes
// limit — when even the empty reply exceeds the bound, and when the first
// outcome alone does — never an empty page that reports more.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_page_that_cannot_fit_refuses_with_the_typed_page_byte_limit() {
    let mut lines = Vec::new();
    for page_bytes in [100u64, 1_500] {
        let world = World::with_page_bytes(page_bytes);
        let seeding = DeclaredReadMemory::from_reader_limit(LIMIT);
        let seeder = world.server(&seeding);
        let hub = start_legacy(&world, &seeder).await;
        world.bind().await;
        world.seed_outcomes(1).await;
        hub.stop().await;
        let requests = DeclaredReadMemory::from_reader_limit(LIMIT);
        let route = installation(&world.server(&requests));
        let request = world.request_bytes(9, None);
        let (result, reply) = drive(&world, &route, &request, |_| {});
        let refused = handed_off(&result);
        lines.push(format!("bound={page_bytes} refused={refused:?}"));
        assert!(
            matches!(
                &refused,
                Some(contextdb_core::Error::ReadFailure(failure))
                    if failure == &contextdb_core::read_contract::ReadFailure::owner_limit_exceeded(
                        contextdb_core::read_contract::OwnerLimitExceededDetail {
                            limit: contextdb_core::read_contract::ReadFailureLimit::CursorPageBytes,
                            value: page_bytes,
                            required: None,
                            statement: None,
                        }
                    )
            ),
            "bound {page_bytes}: {refused:?}"
        );
        assert!(reply.is_none(), "no success body");
        assert_eq!(settled(&requests, 0), 0);
    }
    println!("S4_PAGE_BYTES_REFUSAL {}", lines.join(" | "));
}

// Round 3, P2: the page bound is the declared page bytes, unless the
// transport's frame ceiling binds tighter, which refuses as ResultBytes.
#[test]
fn the_page_bound_is_the_declared_page_bytes_unless_the_frame_ceiling_binds() {
    use contextdb_core::read_contract::ReadFailureLimit;
    let frame = crate::transport::MAX_FRAME_BYTES as u64;
    assert_eq!(
        crate::sync_native_delivery::page_bound(1 << 20),
        (1 << 20, ReadFailureLimit::CursorPageBytes, 1 << 20)
    );
    assert_eq!(
        crate::sync_native_delivery::page_bound(frame + 1),
        (frame, ReadFailureLimit::ResultBytes, frame)
    );
}

/// A provider over one declared reader whose native acquisition can be
/// switched to select the Legacy route.
#[cfg(feature = "iroh")]
#[derive(Debug)]
struct RouteSwitchProvider {
    parent: Arc<DeclaredReadMemory>,
    legacy: AtomicBool,
}
#[cfg(feature = "iroh")]
impl crate::vector_read_memory::ReadRequestBudgetProvider for RouteSwitchProvider {
    fn request_budget(
        &self,
    ) -> contextdb_core::Result<Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>> {
        self.parent.child()
    }
    fn request_budget_owner(
        &self,
    ) -> contextdb_core::Result<crate::vector_read_memory::RequestReadOwner> {
        if self.legacy.load(Ordering::SeqCst) {
            self.parent
                .child()
                .map(crate::vector_read_memory::RequestReadOwner::legacy)
        } else {
            crate::vector_read_memory::ReadRequestBudgetProvider::request_budget_owner(&self.parent)
        }
    }
    fn request_budget_owner_native(
        &self,
    ) -> std::result::Result<
        crate::vector_read_memory::RequestReadOwner,
        crate::vector_read_memory::RequestOwnerFailure,
    > {
        if self.legacy.load(Ordering::SeqCst) {
            self.parent
                .child()
                .map(crate::vector_read_memory::RequestReadOwner::legacy)
                .map_err(crate::vector_read_memory::RequestOwnerFailure::Legacy)
        } else {
            crate::vector_read_memory::ReadRequestBudgetProvider::request_budget_owner_native(
                &self.parent,
            )
        }
    }
}

/// Stop a started Iroh hub and hand back its store, its directory and its
/// edge, for re-serving the same store on a re-bound endpoint.
#[cfg(feature = "iroh")]
async fn stop_keeping_store(hub: IrohHub) -> (tempfile::TempDir, Arc<Database>, Arc<Database>) {
    let IrohHub {
        dir,
        server,
        stop,
        task,
        client,
        _edge_db: edge_db,
        ..
    } = hub;
    let hub_db = server.db.clone();
    within(client.shutdown()).await;
    stop.request_stop();
    within(task).await.expect("hub stops");
    drop(server);
    (dir, hub_db, edge_db)
}

// Round 3, R6: classification is per reply. On one client connection, a
// native delivery reply is classified native, a legitimate oversized Legacy
// reply on another subject is assembled through today's Legacy path and
// classified Legacy, and a provider-selected Legacy reply on the native
// subject is classified Legacy. No reply is refused for what came before.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_admitted_reply_is_classified_by_its_own_status_on_one_connection() {
    use crate::transport::iroh::{CLIENT_REPLY_CLASSES, ClientReplyClass};
    use crate::transport::{BoundedWireBody, HandlerRegistration, IncomingRequest};
    use contextdb_core::read_memory::ReadMemoryOwner;
    const OVERSIZED: usize = 69 * 1024 * 1024 + 137;
    let requests = DeclaredReadMemory::from_reader_limit(LIMIT);
    let (dir, hub_db, edge_db) = stop_keeping_store(IrohHub::start(&requests).await).await;
    let endpoint = crate::transport::iroh::IrohServer::bind(&format!(
        "iroh:?identity={}",
        dir.path().join("hub.key").display()
    ))
    .await
    .expect("rebind the hub endpoint");
    let provider = Arc::new(RouteSwitchProvider {
        parent: requests.clone(),
        legacy: AtomicBool::new(false),
    });
    let server = Arc::new(
        SyncServer::new_with_read_budget_provider(
            hub_db,
            &endpoint,
            TenantId::from(TENANT),
            provider.clone(),
        )
        .expect("hub"),
    );
    let stop = server
        .native_stop(Arc::new(AtomicBool::new(false)))
        .expect("prepare the native delivery installation");
    let native = server
        .native_delivery
        .lock()
        .unwrap()
        .take()
        .expect("native installation");
    let oversized: crate::transport::RequestHandler = Arc::new(|incoming: IncomingRequest| {
        Box::pin(async move { (incoming.responder)(vec![3u8; OVERSIZED]).await })
            as crate::transport::TransportFuture<'static, ()>
    });
    let controller = endpoint.large_request_test_controller();
    let transport = endpoint.transport();
    let serve = tokio::spawn({
        let stop = stop.clone();
        async move {
            match transport.serve_native_installation(
                vec![HandlerRegistration {
                    subject: "s4.legacy.oversized".into(),
                    handler: oversized,
                }],
                native,
                stop,
            ) {
                Ok(future) => future.await,
                Err(error) => Err(error),
            }
        }
    });
    within(controller.wait_until_routes_ready_for_test()).await;
    let client = crate::transport::client_transport(&crate::transport::peer_dial_spec(
        &endpoint.ticket(),
        &dir.path().join("edge.key"),
    ));
    let edge_reader = edge_db.declared_reader_memory().expect("edge reader");
    let incarnation = edge_db
        .existing_sync_incarnation(&TenantId::from(TENANT), edge_reader.clone())
        .expect("edge incarnation read")
        .expect("bound edge has an incarnation");
    let delivery = || {
        encode_named(
            MessageType::FetchDeliveryOutcomesRequest,
            &FetchDeliveryOutcomesRequest {
                format: 1,
                tenant_id: TENANT.into(),
                edge_incarnation: incarnation,
                request_nonce: [6; 16],
                since: None,
            },
        )
        .expect("encode request")
    };
    let reader = DeclaredReadMemory::from_reader_limit(LIMIT);
    let ask = |subject: String, bytes: Vec<u8>| {
        let child = reader.child().expect("request reader");
        let body = BoundedWireBody::from_legacy_reply(bytes, ReadMemoryOwner::from_legacy(child))
            .expect("request body");
        let client = client.clone();
        async move {
            match client.request_stream_admitted(&subject, body, Duration::from_secs(300)) {
                Ok(future) => future.await,
                Err(error) => Err(error),
            }
        }
    };
    CLIENT_REPLY_CLASSES.lock().unwrap().clear();
    let first = within(ask(delivery_outcomes_subject(TENANT), delivery()))
        .await
        .expect("native delivery reply");
    let first = decode_outcomes(first.contiguous_bytes().expect("contiguous"));
    let large = within(ask("s4.legacy.oversized".into(), vec![1]))
        .await
        .expect("oversized Legacy reply");
    let large_len = large.contiguous_bytes().map(<[u8]>::len);
    drop(large);
    provider.legacy.store(true, Ordering::SeqCst);
    let legacy = within(ask(delivery_outcomes_subject(TENANT), delivery()))
        .await
        .expect("provider-selected Legacy reply");
    let legacy = decode_outcomes(legacy.contiguous_bytes().expect("contiguous"));
    let classes = CLIENT_REPLY_CLASSES.lock().unwrap().clone();
    println!(
        "S4_REPLY_CLASSES native_outcomes={first} oversized_len={large_len:?} legacy_outcomes={legacy} classes={classes:?}"
    );
    assert_eq!(first, 3);
    assert_eq!(large_len, Some(OVERSIZED));
    assert_eq!(legacy, 3);
    assert_eq!(
        classes,
        vec![
            ClientReplyClass::Native,
            ClientReplyClass::LegacyOversized,
            ClientReplyClass::Legacy
        ]
    );
    within(client.shutdown()).await.ok();
    stop.request_stop();
    within(serve).await.expect("serve task").expect("serve");
}

// Round 3, C11: a malformed page refuses on the edge's native decode with the
// static codec class, and the edge's declared reader retains nothing of it.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_malformed_delivery_page_refuses_with_the_codec_class_and_retains_nothing() {
    use crate::transport::{HandlerRegistration, IncomingRequest};
    let requests = DeclaredReadMemory::from_reader_limit(LIMIT);
    let (dir, _hub_db, edge_db) = stop_keeping_store(IrohHub::start(&requests).await).await;
    let endpoint = crate::transport::iroh::IrohServer::bind(&format!(
        "iroh:?identity={}",
        dir.path().join("hub.key").display()
    ))
    .await
    .expect("rebind the hub endpoint");
    // A truncated envelope: an array of three with only its version present.
    let garbage: crate::transport::RequestHandler = Arc::new(|incoming: IncomingRequest| {
        Box::pin(async move { (incoming.responder)(vec![0x93, 0x07]).await })
            as crate::transport::TransportFuture<'static, ()>
    });
    let controller = endpoint.large_request_test_controller();
    let shutdown = Arc::new(AtomicBool::new(false));
    let serve = tokio::spawn({
        let transport = endpoint.transport();
        let shutdown = shutdown.clone();
        async move {
            transport
                .serve(
                    vec![HandlerRegistration {
                        subject: delivery_outcomes_subject(TENANT),
                        handler: garbage,
                    }],
                    shutdown,
                )
                .await
        }
    });
    within(controller.wait_until_routes_ready_for_test()).await;
    let dial = crate::transport::peer_dial_spec(&endpoint.ticket(), &dir.path().join("edge.key"));
    let client = SyncClient::new(edge_db.clone(), &dial, TenantId::from(TENANT)).expect("edge");
    let declared = edge_db.declared_read_memory_for_test();
    let before = declared.retained_bytes_for_test();
    let refused = within(client.fetch_delivery_outcomes(None)).await;
    let after = declared.retained_bytes_for_test();
    println!(
        "S4_MALFORMED_PAGE refused={refused:?} edge_retained(before,after)=({before},{after})"
    );
    assert!(
        matches!(&refused, Err(contextdb_core::Error::SyncError(class)) if class == "sync.decode.codec"),
        "{refused:?}"
    );
    assert_eq!(
        after, before,
        "the edge retains nothing of the malformed page"
    );
    within(client.shutdown()).await;
    shutdown.store(true, Ordering::SeqCst);
    within(serve).await.expect("serve task").expect("serve");
}

/// The native admissions one parent saw, in order.
#[cfg(feature = "iroh")]
fn witnesses_of(
    parent: &DeclaredReadMemory,
    all: Vec<crate::vector_read_memory::NativeAdmissionWitness>,
) -> Vec<crate::vector_read_memory::NativeAdmissionWitness> {
    let id = parent.witness_identity_for_test();
    all.into_iter().filter(|w| w.parent == id).collect()
}

// Round 3, finding 7 (hub): every admission site the native route owns is
// witnessed, in order, with its exact bytes; and a parent one byte short at
// any site refuses exactly there, with that site's own typed refusal, admits
// nothing after it, publishes no outcome, and retains nothing.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_hub_admission_site_refuses_before_its_allocation() {
    use crate::vector_read_memory::{
        take_native_admissions_for_test, watch_native_admissions_for_test,
    };
    const ROUTE_SITES: [&str; 8] = [
        "request_owner",
        "native cancellation token",
        "materialize_body",
        "reply_stream_controls",
        "native_finish_callback",
        "native owned future",
        "native_control_cell",
        "native_control_payload",
    ];
    let provider = Arc::new(SwitchableProvider {
        roomy: DeclaredReadMemory::from_reader_limit(LIMIT),
        tight: std::sync::Mutex::new(DeclaredReadMemory::from_reader_limit(LIMIT)),
        use_tight: AtomicBool::new(false),
    });
    let hub = IrohHub::start_with(provider.clone(), None).await;
    let accepted = || {
        hub._edge_db
            .delivery_status(ROOT_TABLE)
            .expect("edge delivery status")
            .accepted
    };
    // The witnessed run.
    let probe = DeclaredReadMemory::from_reader_limit(LIMIT);
    *provider.tight.lock().unwrap() = probe.clone();
    provider.use_tight.store(true, Ordering::SeqCst);
    watch_native_admissions_for_test();
    let fetched = within(hub.client.fetch_delivery_outcomes(None))
        .await
        .expect("witnessed delivery");
    let seen = witnesses_of(&probe, take_native_admissions_for_test());
    assert_eq!(fetched.len(), 3);
    assert!(seen.iter().all(|w| w.admitted));
    let sites: Vec<_> = seen
        .iter()
        .copied()
        .filter(|w| ROUTE_SITES.contains(&w.operation))
        .collect();
    for operation in ROUTE_SITES {
        assert!(
            sites.iter().any(|w| w.operation == operation),
            "site {operation} is witnessed: {sites:?}"
        );
    }
    let accepted_before = accepted();
    let mut lines = Vec::new();
    // The request's own bytes vary run to run (a random nonce and incarnation
    // widths), so each site is approached from just below its witnessed total
    // until the refusal lands exactly there.
    for operation in ROUTE_SITES {
        let site = *sites
            .iter()
            .find(|w| w.operation == operation)
            .expect("site witnessed");
        let mut landed = None;
        for back in 0..256u64 {
            let limit = site.retained_after.saturating_sub(1 + back);
            let tight = DeclaredReadMemory::from_reader_limit(limit);
            *provider.tight.lock().unwrap() = tight.clone();
            watch_native_admissions_for_test();
            let refused = within(hub.client.fetch_delivery_outcomes(None)).await;
            let witnessed = witnesses_of(&tight, take_native_admissions_for_test());
            let retained = settled(&tight, 0);
            assert_eq!(
                retained, 0,
                "{operation}: a refused request retains nothing"
            );
            assert_eq!(accepted(), accepted_before, "no success body was published");
            let Some(at) = witnessed.iter().position(|w| !w.admitted) else {
                continue;
            };
            assert!(
                witnessed[at + 1..].iter().all(|w| !w.admitted),
                "nothing is admitted after the refusal: {witnessed:?}"
            );
            let refusal = witnessed[at];
            assert!(
                matches!(
                    &refused,
                    Err(contextdb_core::Error::MemoryBudgetExceeded { subsystem, operation, .. })
                        if subsystem == refusal.subsystem && operation == refusal.operation
                ),
                "the typed refusal names the refused site {refusal:?}: {refused:?}"
            );
            if refusal.operation == operation {
                landed = Some((limit, refusal.bytes));
                break;
            }
        }
        let (limit, bytes) =
            landed.unwrap_or_else(|| panic!("{operation}: no limit refused exactly at the site"));
        lines.push(format!("{operation}:{bytes}@{limit}"));
    }
    println!("S4_HUB_SITES {} | {}", ROUTE_SITES.len(), lines.join(" | "));
    provider.use_tight.store(false, Ordering::SeqCst);
    hub.stop().await;
}

// Round 3, finding 7 and C14 (edge): a three-page native fetch witnesses each
// edge admission site in order on the edge's own request owner; a caller
// reader too small for the second page's result refuses there with the typed
// result refusal, returns nothing, and retains nothing.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_edge_admits_each_fetch_site_and_refuses_a_second_page_it_cannot_hold() {
    use crate::vector_read_memory::{
        take_native_admissions_for_test, watch_native_admissions_for_test,
    };
    const EDGE_SITES: [&str; 6] = [
        "materialize_body",
        "wire_future_frame",
        "decode_sync_response",
        "delivery_packet_slots",
        "delivery_result",
        "delivery_cursor",
    ];
    let requests = DeclaredReadMemory::from_reader_limit(LIMIT);
    let hub = IrohHub::start_with(Arc::new(requests.clone()), Some(9_000)).await;
    let edge = hub._edge_db.declared_read_memory_for_test();
    watch_native_admissions_for_test();
    let fetched = within(hub.client.fetch_delivery_outcomes(None))
        .await
        .expect("three pages");
    let seen = witnesses_of(&edge, take_native_admissions_for_test());
    let operations: Vec<&str> = seen.iter().map(|w| w.operation).collect();
    println!(
        "S4_EDGE_SITES outcomes={} witnessed={}",
        fetched.len(),
        operations
            .iter()
            .filter(|op| EDGE_SITES.contains(op))
            .copied()
            .collect::<Vec<_>>()
            .join(",")
    );
    assert_eq!(fetched.len(), 3);
    for operation in EDGE_SITES {
        assert!(
            operations.contains(&operation),
            "edge site {operation} is witnessed: {operations:?}"
        );
    }
    assert_eq!(
        operations
            .iter()
            .filter(|op| **op == "delivery_cursor")
            .count(),
        2,
        "one admitted continuation cursor per further page"
    );

    // A caller reader sized from a recorded caller run refuses on page two.
    let caller = DeclaredReadMemory::from_reader_limit(LIMIT);
    let child = caller.child().expect("caller reader");
    let roomy = within(
        hub.client
            .fetch_delivery_outcomes_with_read_memory(None, child.clone()),
    )
    .await
    .expect("caller-reader fetch");
    assert_eq!(roomy.len(), 3);
    drop(roomy);
    drop(child);
    assert_eq!(caller.retained_bytes_for_test(), 0);
    let mut refused_on_page_two = None;
    for limit in (2_000..2_000_000u64).step_by(2_000) {
        let tight = DeclaredReadMemory::from_reader_limit(limit);
        let child = tight.child().expect("tight reader");
        let served_before = hub.server.delivery_route_counts_for_test().0;
        let result = within(
            hub.client
                .fetch_delivery_outcomes_with_read_memory(None, child.clone()),
        )
        .await;
        drop(child);
        let pages_asked = hub.server.delivery_route_counts_for_test().0 - served_before;
        assert_eq!(
            tight.retained_bytes_for_test(),
            0,
            "nothing retained at {limit}"
        );
        match result {
            Ok(outcomes) => {
                drop(outcomes);
                break;
            }
            Err(contextdb_core::Error::MemoryBudgetExceeded { operation, .. })
                if pages_asked >= 2 =>
            {
                refused_on_page_two = Some((limit, pages_asked, operation));
                break;
            }
            Err(_) => {}
        }
    }
    println!("S4_EDGE_RESULT_REFUSAL {refused_on_page_two:?}");
    assert!(
        refused_on_page_two.is_some(),
        "a caller reader refuses after the first page, returning nothing and retaining nothing"
    );
    hub.stop().await;
}

// Round 3, design §8 (abort exits): a hub connection task aborted while a
// native request is held after admission (receive), after selection
// (dispatch) or after its reply is written but before the receipt (write)
// releases every charge that request held, while the endpoint stays up; the
// edge redials once and fetches every outcome.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_aborted_hub_task_releases_every_charge_in_receive_dispatch_and_write() {
    use crate::transport::native_pause::{self, Point};
    let requests = DeclaredReadMemory::from_reader_limit(LIMIT);
    let hub = IrohHub::start(&requests).await;
    let edge = hub._edge_db.declared_read_memory_for_test();
    let mut lines = Vec::new();
    for point in [
        Point::AfterAdmission,
        Point::AfterSelection,
        Point::BeforeReceipt,
    ] {
        let edge_before = edge.retained_bytes_for_test();
        let gate = native_pause::arm(point);
        let abort = async {
            gate.reached().await;
            let held = requests.retained_bytes_for_test();
            let aborted = crate::transport::iroh::abort_connection_tasks_for_test();
            (held, aborted)
        };
        let (fetched, (held, aborted)) =
            within(async { tokio::join!(hub.client.fetch_delivery_outcomes(None), abort) }).await;
        let fetched = fetched.map(|outcomes| outcomes.len());
        let after = settled(&requests, 0);
        let edge_after = edge.retained_bytes_for_test();
        lines.push(format!(
            "{point:?}: held={held} aborted={aborted} fetched={fetched:?} hub_after={after} edge(before,after)=({edge_before},{edge_after})"
        ));
        assert!(held > 0, "{point:?}: the held request carries its charges");
        assert!(
            aborted >= 1,
            "{point:?}: the hub connection task was aborted"
        );
        assert_eq!(
            after, 0,
            "{point:?}: the aborted task released every charge"
        );
        assert_eq!(
            fetched.ok(),
            Some(3),
            "{point:?}: the edge redials and fetches"
        );
        assert_eq!(
            edge_after, edge_before,
            "{point:?}: the edge retains nothing"
        );
    }
    println!("S4_ABORT_EXITS {}", lines.join(" | "));
    hub.stop().await;
}

// Round 3, design §8 (drain exit): a drain while a native request is held
// after selection drops its dispatch and closes its stream unanswered — no
// terminal is written — and every charge is released on both ends.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drain_mid_dispatch_closes_the_request_unanswered_and_releases_every_charge() {
    use crate::transport::iroh::TERMINAL_WRITES;
    use crate::transport::native_pause::{self, Point};
    let requests = DeclaredReadMemory::from_reader_limit(LIMIT);
    let hub = IrohHub::start(&requests).await;
    let edge = hub._edge_db.declared_read_memory_for_test();
    let edge_before = edge.retained_bytes_for_test();
    let terminals_before = TERMINAL_WRITES.lock().unwrap().len();
    let gate = native_pause::arm(Point::AfterSelection);
    let drain = async {
        gate.reached().await;
        let held = requests.retained_bytes_for_test();
        // The drain deadline is forced so the drain cancels reply owners at
        // once instead of waiting out the graceful window.
        hub.controller.force_graceful_drain_timeout_for_test();
        hub.stop.request_stop();
        held
    };
    let (fetched, held) =
        within(async { tokio::join!(hub.client.fetch_delivery_outcomes(None), drain) }).await;
    let fetched = fetched.map(|outcomes| outcomes.len());
    let terminals = TERMINAL_WRITES.lock().unwrap().len() - terminals_before;
    hub.stop().await;
    let after = settled(&requests, 0);
    let edge_after = edge.retained_bytes_for_test();
    println!(
        "S4_DRAIN_EXIT held={held} fetched={fetched:?} terminals_written={terminals} hub_after={after} edge(before,after)=({edge_before},{edge_after})"
    );
    assert!(held > 0, "the held request carries its charges");
    assert!(fetched.is_err(), "a drained request is not answered");
    assert_eq!(
        terminals, 0,
        "the drained request's stream closes unanswered"
    );
    assert_eq!(after, 0, "the drain released every charge");
    // The edge's redial meets a hub that stopped serving, so the edge may also
    // let go of standing state it held before the request; it never holds more.
    assert!(
        edge_after <= edge_before,
        "the edge retains nothing of the drained request: ({edge_before},{edge_after})"
    );
}

// Round 3, design §8 (terminal-write failure): when the edge stops reading
// before the hub writes a native terminal, the terminal write fails, every
// charge is released on both ends, and the next request on the same hub is
// answered with its typed refusal.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_terminal_the_edge_no_longer_reads_fails_its_write_and_releases_every_charge() {
    use crate::transport::iroh::TERMINAL_WRITES;
    use crate::transport::native_pause::{self, Point};
    let requests = DeclaredReadMemory::from_reader_limit(LIMIT);
    // A 100-byte page cannot hold one outcome: every delivery request fails in
    // dispatch with the typed page-byte refusal, while its control is alive.
    let hub = IrohHub::start_with(Arc::new(requests.clone()), Some(100)).await;
    let edge = hub._edge_db.declared_read_memory_for_test();
    let edge_before = edge.retained_bytes_for_test();
    TERMINAL_WRITES.lock().unwrap().clear();
    let gate = native_pause::arm(Point::BeforeTerminal);
    // The edge gives up on the request while the hub holds its terminal: the
    // fetch future, and with it the edge's streams, is dropped.
    let answered = within(async {
        tokio::select! {
            result = hub.client.fetch_delivery_outcomes(None) => Some(result.map(|o| o.len())),
            () = gate.reached() => None,
        }
    })
    .await;
    let held = requests.retained_bytes_for_test();
    let edge_abandoned = edge.retained_bytes_for_test();
    gate.release();
    crate::transport::release_events::wait_until(|| !TERMINAL_WRITES.lock().unwrap().is_empty());
    let first = TERMINAL_WRITES.lock().unwrap().clone();
    let after = settled(&requests, 0);
    // The hub keeps serving: the next request is refused, typed.
    let again = within(hub.client.fetch_delivery_outcomes(None)).await;
    let writes = TERMINAL_WRITES.lock().unwrap().clone();
    let after_again = settled(&requests, 0);
    let edge_after = edge.retained_bytes_for_test();
    println!(
        "S4_TERMINAL_WRITE_FAILURE answered={answered:?} held={held} first_writes={first:?} hub_after={after} again={again:?} writes={writes:?} hub_after_again={after_again} edge(before,abandoned,after)=({edge_before},{edge_abandoned},{edge_after})"
    );
    assert!(
        answered.is_none(),
        "the hub holds the terminal before the edge has an answer"
    );
    assert!(
        held > 0,
        "the failed request's control is alive while its terminal waits"
    );
    assert_eq!(
        first,
        vec![false],
        "the terminal write to an abandoned stream fails"
    );
    assert_eq!(after, 0, "the failed terminal write released every charge");
    assert!(
        matches!(&again, Err(contextdb_core::Error::ReadFailure(_))),
        "the next request is answered with its typed refusal: {again:?}"
    );
    assert_eq!(writes, vec![false, true]);
    assert_eq!(after_again, 0);
    assert_eq!(
        edge_abandoned, edge_before,
        "the abandoned fetch retains nothing"
    );
    assert_eq!(edge_after, edge_before);
    hub.stop().await;
}

// Round 3, design §8 (edge timeout, retry and drop): an admitted edge request
// whose hub holds it past the request timeout times out, drops its connection
// and redials once; the retry is answered with every outcome. The edge
// retains nothing, and the held hub request releases every charge once its
// write to the dropped connection fails.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_edge_timeout_retries_once_and_both_ends_release_every_charge() {
    use crate::transport::BoundedWireBody;
    use crate::transport::native_pause::{self, Point};
    use contextdb_core::read_memory::ReadMemoryOwner;
    // A parameter of the exchange, not a synchronization: the hub holds the
    // first attempt until the test releases it, however long that is.
    const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);
    let requests = DeclaredReadMemory::from_reader_limit(LIMIT);
    let hub = IrohHub::start(&requests).await;
    let client = crate::transport::client_transport(&crate::transport::peer_dial_spec(
        &hub.ticket,
        &hub.dir.path().join("edge.key"),
    ));
    let edge_reader = hub._edge_db.declared_reader_memory().expect("edge reader");
    let incarnation = hub
        ._edge_db
        .existing_sync_incarnation(&TenantId::from(TENANT), edge_reader)
        .expect("edge incarnation read")
        .expect("bound edge has an incarnation");
    let request = encode_named(
        MessageType::FetchDeliveryOutcomesRequest,
        &FetchDeliveryOutcomesRequest {
            format: 1,
            tenant_id: TENANT.into(),
            edge_incarnation: incarnation,
            request_nonce: [5; 16],
            since: None,
        },
    )
    .expect("encode request");
    let reader = DeclaredReadMemory::from_reader_limit(LIMIT);
    let gate = native_pause::arm(Point::AfterSelection);
    let child = reader.child().expect("request reader");
    let body = BoundedWireBody::from_legacy_reply(request, ReadMemoryOwner::from_legacy(child))
        .expect("request body");
    let subject = delivery_outcomes_subject(TENANT);
    let reply = within(async {
        match client.request_stream_admitted(&subject, body, ATTEMPT_TIMEOUT) {
            Ok(future) => future.await,
            Err(error) => Err(error),
        }
    })
    .await;
    let held_first = tokio::select! {
        biased;
        () = gate.reached() => true,
        () = std::future::ready(()) => false,
    };
    let outcomes = reply
        .as_ref()
        .ok()
        .and_then(|reply| reply.contiguous_bytes())
        .map(decode_outcomes);
    let reply_ok = reply.is_ok();
    drop(reply);
    let edge_after = settled(&reader, 0);
    let held = requests.retained_bytes_for_test();
    gate.release();
    let hub_after = settled(&requests, 0);
    println!(
        "S4_EDGE_TIMEOUT held_first={held_first} reply_ok={reply_ok} outcomes={outcomes:?} edge_after={edge_after} hub_held={held} hub_after={hub_after}"
    );
    assert!(
        held_first,
        "the first attempt was held at the hub past its timeout"
    );
    assert_eq!(outcomes, Some(3), "the single retry is answered");
    assert_eq!(edge_after, 0, "the edge retains nothing");
    assert!(
        held > 0,
        "the abandoned first attempt still holds its charges"
    );
    assert_eq!(hub_after, 0, "its failed write released every charge");
    within(client.shutdown()).await.ok();
    hub.stop().await;
}

/// A provider that funds every hub request from its own fresh declared
/// reader of the current limit, and keeps each one for inspection.
#[cfg(feature = "iroh")]
#[derive(Debug)]
struct PerRequestProvider {
    limit: std::sync::atomic::AtomicU64,
    /// Limits for the next requests, in order; the default once spent.
    planned: std::sync::Mutex<std::collections::VecDeque<u64>>,
    parents: std::sync::Mutex<Vec<Arc<DeclaredReadMemory>>>,
}
#[cfg(feature = "iroh")]
impl PerRequestProvider {
    fn new(limit: u64) -> Self {
        Self {
            limit: std::sync::atomic::AtomicU64::new(limit),
            planned: std::sync::Mutex::new(std::collections::VecDeque::new()),
            parents: std::sync::Mutex::new(Vec::new()),
        }
    }
    fn next(&self) -> Arc<DeclaredReadMemory> {
        let limit = self
            .planned
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| self.limit.load(Ordering::SeqCst));
        let parent = DeclaredReadMemory::from_reader_limit(limit);
        self.parents.lock().unwrap().push(parent.clone());
        parent
    }
    fn served(&self) -> usize {
        self.parents.lock().unwrap().len()
    }
    fn served_since(&self, from: usize) -> Vec<Arc<DeclaredReadMemory>> {
        self.parents.lock().unwrap()[from..].to_vec()
    }
}
#[cfg(feature = "iroh")]
impl crate::vector_read_memory::ReadRequestBudgetProvider for PerRequestProvider {
    fn request_budget(
        &self,
    ) -> contextdb_core::Result<Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>> {
        self.next().child()
    }
    fn request_budget_owner(
        &self,
    ) -> contextdb_core::Result<crate::vector_read_memory::RequestReadOwner> {
        crate::vector_read_memory::ReadRequestBudgetProvider::request_budget_owner(&self.next())
    }
    fn request_budget_owner_native(
        &self,
    ) -> std::result::Result<
        crate::vector_read_memory::RequestReadOwner,
        crate::vector_read_memory::RequestOwnerFailure,
    > {
        crate::vector_read_memory::ReadRequestBudgetProvider::request_budget_owner_native(
            &self.next(),
        )
    }
}

// Slice review 04, finding 1: the page cut counts each candidate from its
// borrowed views and builds only the included prefix. Three small outcomes
// are followed by one much larger one, with one small outcome per page: the
// page whose excluded next candidate is the large outcome is built from its
// fitting outcome alone and succeeds, the large outcome is counted but never
// built, and as the first outcome of its own page it refuses with the typed
// page-byte limit, never a memory refusal from building it. Run in release:
// a debug build verifies the large outcome slower than the request timeout.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_page_that_fits_is_built_alone_before_a_much_larger_excluded_outcome() {
    use crate::sync_native_delivery::PAGE_CUT_SIZES;
    use crate::vector_read_memory::{
        take_native_admissions_for_test, watch_native_admissions_for_test,
    };
    use contextdb_core::read_contract::{OwnerLimitExceededDetail, ReadFailure, ReadFailureLimit};
    const PAGE_BYTES: u64 = 9_000;
    let page_refusal = ReadFailure::owner_limit_exceeded(OwnerLimitExceededDetail {
        limit: ReadFailureLimit::CursorPageBytes,
        value: PAGE_BYTES,
        required: None,
        statement: None,
    });
    let provider = Arc::new(PerRequestProvider::new(LIMIT));
    let started = std::time::Instant::now();
    let hub = IrohHub::start_seeded(provider.clone(), Some(PAGE_BYTES), 150).await;
    eprintln!(
        "S4_EXCLUDED_PHASE seeded elapsed_ms={}",
        started.elapsed().as_millis()
    );
    let accepted = || {
        hub._edge_db
            .delivery_status(ROOT_TABLE)
            .expect("edge delivery status")
            .accepted
    };
    let accepted_before = accepted();

    // Every request on its own roomy reader, each one's peak witnessed.
    PAGE_CUT_SIZES.lock().unwrap().clear();
    let from = provider.served();
    watch_native_admissions_for_test();
    let observed = within(hub.client.fetch_delivery_outcomes(None)).await;
    eprintln!(
        "S4_EXCLUDED_PHASE observed elapsed_ms={}",
        started.elapsed().as_millis()
    );
    let witnesses = take_native_admissions_for_test();
    let sizes = PAGE_CUT_SIZES.lock().unwrap().clone();
    let accepted_after = accepted();
    let pages = provider.served_since(from);
    let peaks: Vec<u64> = pages
        .iter()
        .map(|parent| {
            witnesses_of(parent, witnesses.clone())
                .iter()
                .map(|w| w.retained_after)
                .max()
                .unwrap_or(0)
        })
        .collect();
    let built: Vec<usize> = sizes.iter().filter(|(b, _)| *b).map(|(_, n)| *n).collect();
    let largest_counted = sizes
        .iter()
        .filter(|(b, _)| !*b)
        .map(|(_, n)| *n)
        .max()
        .unwrap_or(0);
    println!(
        "S4_EXCLUDED_LARGE observed={observed:?} sizes={sizes:?} peaks={peaks:?} accepted(before,after)=({accepted_before},{accepted_after})"
    );
    assert!(
        matches!(&observed, Err(contextdb_core::Error::ReadFailure(f)) if *f == page_refusal),
        "the large outcome, first on its page, refuses with the typed page limit: {observed:?}"
    );
    assert_eq!(
        accepted_after,
        accepted_before + 3,
        "every fitting page succeeded, including the one before the large outcome"
    );
    assert!(
        largest_counted > PAGE_BYTES as usize,
        "the excluded outcome is larger than a whole page"
    );
    assert_eq!(
        built.len(),
        3,
        "exactly the three fitting outcomes were built"
    );
    assert!(
        built.iter().all(|n| *n <= PAGE_BYTES as usize),
        "no built outcome exceeds the page: the large one was never materialized"
    );
    assert!(pages.len() >= 4);
    let peaks = &peaks[peaks.len() - 4..];

    // Each page again, on a reader of exactly its own measured peak plus a
    // small allowance for the request's run-to-run variance: every fitting
    // page still succeeds and the large outcome still refuses typed. A page's
    // peak is its selection holds, and building a packet happens beneath it,
    // so a reader limit cannot tell a built excluded outcome from an unbuilt
    // one; the build record above is what proves it was never materialized.
    const ALLOWANCE: u64 = 256;
    *provider.planned.lock().unwrap() = peaks.iter().map(|peak| peak + ALLOWANCE).collect();
    let constrained = within(hub.client.fetch_delivery_outcomes(None)).await;
    println!(
        "S4_EXCLUDED_LARGE_TIGHT limits={:?} result={constrained:?}",
        peaks
            .iter()
            .map(|peak| peak + ALLOWANCE)
            .collect::<Vec<_>>()
    );
    assert!(
        matches!(&constrained, Err(contextdb_core::Error::ReadFailure(f)) if *f == page_refusal),
        "on readers of their own peaks the fitting pages succeed and the large one refuses typed: {constrained:?}"
    );
    hub.stop().await;
}

// Slice review 04, finding 3: the page's result memory is admitted before its
// custody effects commit. A caller reader stepped up from nothing refuses at
// each site in turn and never succeeds first. On page one, the refusal lands
// at delivery_result and the edge's delivery status is unchanged. Every
// refused probe, on any page, leaves durable status only for the pages that
// completed before the refused one, and retains nothing.
//
// Page two's delivery_result is not reachable with one reader limit when the
// outcomes are of one size: the page's reply frame peaks above its later
// result admission, so any limit that admits the frame admits the result. The
// two-page walk therefore checks the invariant at every refusal up to its
// first success and reports whether it reached that site.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_result_refusal_leaves_its_page_without_durable_delivery_status() {
    // Room past a refused site, for the request's own run-to-run variance.
    const SLACK: u64 = 64;
    let mut lines = Vec::new();
    for (page_bytes, target_page) in [(None, 1usize), (Some(9_000u64), 2usize)] {
        let requests = DeclaredReadMemory::from_reader_limit(LIMIT);
        let hub = IrohHub::start_with(Arc::new(requests.clone()), page_bytes).await;
        let accepted = || {
            hub._edge_db
                .delivery_status(ROOT_TABLE)
                .expect("edge delivery status")
                .accepted
        };
        let before = accepted();
        let mut limit = 1u64;
        let mut steps = 0;
        let mut last_operation = String::new();
        let mut result_refusal = None;
        loop {
            steps += 1;
            assert!(steps < 400, "page {target_page}: the walk did not finish");
            let tight = DeclaredReadMemory::from_reader_limit(limit);
            // Taking the caller's reader is itself charged; a limit below it
            // steps past it like any other site.
            let child = match tight.child() {
                Ok(child) => child,
                Err(contextdb_core::Error::MemoryBudgetExceeded {
                    requested_bytes,
                    available_bytes,
                    ..
                }) => {
                    limit += requested_bytes.saturating_sub(available_bytes) as u64 + SLACK;
                    continue;
                }
                Err(other) => panic!("caller reader: {other:?}"),
            };
            let served = hub.server.delivery_route_counts_for_test().0;
            let result = within(
                hub.client
                    .fetch_delivery_outcomes_with_read_memory(None, child.clone()),
            )
            .await;
            drop(child);
            let pages = hub.server.delivery_route_counts_for_test().0 - served;
            eprintln!(
                "S4_RESULT_PROBE page_target={target_page} limit={limit} pages={pages} result={:?}",
                result.as_ref().map(Vec::len)
            );
            assert_eq!(
                tight.retained_bytes_for_test(),
                0,
                "limit {limit}: a refused fetch retains nothing"
            );
            match result {
                Err(contextdb_core::Error::MemoryBudgetExceeded {
                    operation,
                    requested_bytes,
                    available_bytes,
                    ..
                }) => {
                    let durable = accepted() - before;
                    assert!(
                        durable <= pages.saturating_sub(1) as u64,
                        "limit {limit}: a refusal on page {pages} left {durable} pages durable"
                    );
                    if operation == "delivery_result" && pages == target_page {
                        result_refusal = Some((limit, durable));
                        break;
                    }
                    // A site that admits in many small steps (the page decode)
                    // is climbed by a sixteenth of the limit at a time.
                    let mut step = requested_bytes.saturating_sub(available_bytes) as u64 + SLACK;
                    if operation == last_operation {
                        step = step.max(limit / 16);
                    }
                    last_operation = operation.to_string();
                    limit += step;
                }
                Ok(outcomes) if target_page > 1 => {
                    assert_eq!(outcomes.len(), 3);
                    break;
                }
                other => panic!(
                    "page {target_page}, limit {limit}: a memory refusal precedes success: {other:?}"
                ),
            }
        }
        lines.push(format!(
            "page {target_page}: delivery_result refusal (limit, pages durable)={result_refusal:?} after {steps} probes"
        ));
        if target_page == 1 {
            assert_eq!(
                result_refusal.map(|(_, durable)| durable),
                Some(0),
                "page one's result refusal leaves no durable delivery status"
            );
        }
        assert_eq!(settled(&requests, 0), 0);
        hub.stop().await;
    }
    println!("S4_RESULT_REFUSAL {}", lines.join(" | "));
}

impl World {
    /// Close the hub store and open it again from its file, as a restarted
    /// hub process would. Every server over the old handle must be gone.
    fn reopen_hub(&mut self) {
        let path = self._dir.path().join("hub.db");
        let old = std::mem::replace(&mut self.hub_db, Arc::new(Database::open_memory()));
        Arc::into_inner(old)
            .expect("no server still holds the hub store")
            .close()
            .expect("close the hub store");
        let mut options = crate::DatabaseOpenOptions::default();
        options.owner_reads.limits.limits.memory = 8 * 1024 * 1024 * 1024;
        self.hub_db =
            Arc::new(Database::open_with_options(&path, options).expect("reopen the hub"));
    }
}

// Verdict 1: a file-backed hub whose custody group index lost its completion
// marker refuses native delivery. Transaction repair rebuilds the index
// complete; after a reopen the hub, served exactly as the shipped binary
// serves it (native_stop + run_until_stopped), answers natively again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transaction_repair_restores_native_delivery_after_reopen() {
    let mut world = World::new();
    let requests = DeclaredReadMemory::from_reader_limit(LIMIT);
    let mut lines = Vec::new();
    let serve = |world: &World| world.server(&requests);

    let server = serve(&world);
    let hub = start_native(&world, &server).await;
    world.bind().await;
    world.seed_outcomes(3).await;
    let fetched = within(world.client.fetch_delivery_outcomes(None))
        .await
        .expect("native delivery from a complete index");
    let counts = server.delivery_route_counts_for_test();
    lines.push(format!(
        "complete: outcomes={} served={counts:?}",
        fetched.len()
    ));
    assert_eq!(fetched.len(), 3);
    assert_eq!(counts, (1, 0));
    hub.stop();
    drop(server);

    world
        .hub_db
        .__invalidate_custody_groups_for_test()
        .expect("drop the completion marker");
    world.reopen_hub();
    assert!(!world.hub_db.__custody_groups_complete_for_test().unwrap());
    let server = serve(&world);
    let hub = start_native(&world, &server).await;
    let refused = within(world.client.fetch_delivery_outcomes(None)).await;
    let counts = server.delivery_route_counts_for_test();
    lines.push(format!(
        "marker_lost: refused={} served={counts:?}",
        refused.is_err()
    ));
    assert!(
        matches!(
            &refused,
            Err(contextdb_core::Error::ReadFailure(failure))
                if failure == &contextdb_core::read_contract::ReadFailure::owner_route_unsupported(
                    "custody/durable_group_index".into()
                )
        ),
        "an incomplete index is refused by the native route, never served: {refused:?}"
    );
    assert_eq!(
        counts,
        (1, 0),
        "refused on the native route; no Legacy answer in its place"
    );
    hub.stop();
    drop(server);

    world
        .hub_db
        .__repair_transaction_order_for_test()
        .expect("transaction repair");
    assert!(world.hub_db.__custody_groups_complete_for_test().unwrap());
    world.reopen_hub();
    let complete_after_reopen = world.hub_db.__custody_groups_complete_for_test().unwrap();
    let server = serve(&world);
    let hub = start_native(&world, &server).await;
    let fetched = within(world.client.fetch_delivery_outcomes(None))
        .await
        .expect("native delivery after repair and reopen");
    let counts = server.delivery_route_counts_for_test();
    lines.push(format!(
        "repaired: complete_after_reopen={complete_after_reopen} outcomes={} served={counts:?} request_parent_after={}",
        fetched.len(),
        settled(&requests, 0)
    ));
    println!("S4_REPAIR {}", lines.join(" | "));
    assert!(complete_after_reopen);
    assert_eq!(fetched.len(), 3, "every recorded outcome, exactly once");
    assert_eq!(counts, (1, 0), "one request, served by the native route");
    hub.stop();
}

// Review 2, finding 4, over real Iroh: a hub whose custody index has lost its
// completion marker, reopened as a restarted hub process would be, refuses the
// native request with the shared typed owner-route refusal, intact across the
// wire. No success body is admitted on the edge and the hub retains nothing of
// the request.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_custody_marker_refuses_over_iroh_with_the_typed_owner_route_refusal() {
    let requests = DeclaredReadMemory::from_reader_limit(LIMIT);
    let IrohHub {
        dir,
        server,
        stop,
        task,
        client,
        _edge_db: edge_db,
        ..
    } = IrohHub::start(&requests).await;
    let hub_db = server.db.clone();
    hub_db
        .__invalidate_custody_groups_for_test()
        .expect("drop the completion marker");
    stop.request_stop();
    within(task).await.expect("hub stops");
    drop(server);
    Arc::into_inner(hub_db)
        .expect("no server still holds the hub store")
        .close()
        .expect("close the hub store");

    // The restarted hub: the same store file and the same fabric identity.
    let mut options = crate::DatabaseOpenOptions::default();
    options.owner_reads.limits.limits.memory = 8 * 1024 * 1024 * 1024;
    let hub_db = Arc::new(
        Database::open_with_options(dir.path().join("hub.db"), options).expect("reopen the hub"),
    );
    assert!(!hub_db.__custody_groups_complete_for_test().unwrap());
    let endpoint = crate::transport::iroh::IrohServer::bind(&format!(
        "iroh:?identity={}",
        dir.path().join("hub.key").display()
    ))
    .await
    .expect("rebind the hub endpoint");
    let server = Arc::new(
        SyncServer::new_with_read_budget_provider(
            hub_db,
            &endpoint,
            TenantId::from(TENANT),
            Arc::new(requests.clone()),
        )
        .expect("restarted hub"),
    );
    let stop = server
        .native_stop(Arc::new(AtomicBool::new(false)))
        .expect("prepare the native delivery installation");
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until_stopped(stop).await }
    });

    let refused = within(client.fetch_delivery_outcomes(None)).await;
    let counts = server.delivery_route_counts_for_test();
    let accepted = edge_db
        .delivery_status(ROOT_TABLE)
        .expect("edge delivery status")
        .accepted;
    let retained = settled(&requests, 0);
    println!(
        "S4_IROH_LOST_MARKER refused={refused:?} served={counts:?} accepted={accepted} request_parent_after={retained}"
    );
    assert!(
        matches!(
            &refused,
            Err(contextdb_core::Error::ReadFailure(failure))
                if failure == &contextdb_core::read_contract::ReadFailure::owner_route_unsupported(
                    "custody/durable_group_index".into()
                )
        ),
        "the lost marker refuses with the typed owner-route refusal: {refused:?}"
    );
    assert_eq!(
        counts,
        (1, 0),
        "refused on the native route; no Legacy answer"
    );
    assert_eq!(accepted, 0, "no success body was admitted");
    assert_eq!(retained, 0);
    within(client.shutdown()).await;
    stop.request_stop();
    within(task).await.expect("restarted hub stops");
}
