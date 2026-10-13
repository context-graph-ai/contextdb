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
    let mut options = crate::DatabaseOpenOptions::default();
    // The Legacy seeding fixture and the Legacy fetch of 10,010 outcomes hold
    // whole custody history on the reader; the native request does not.
    options.owner_reads.limits.limits.memory = 8 * 1024 * 1024 * 1024;
    // Reading the cursor after 10,010 prior outcomes selects all of them in
    // one unpaged native request, which outlasts the shipped 5 s active limit.
    options.owner_reads.limits.limits.active_ms = 600_000;
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
    client: SyncClient,
    // Dropped last: every store above closes before its directory goes.
    _dir: tempfile::TempDir,
}

impl World {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("store directory");
        let hub_db = open(&dir.path().join("hub.db"));
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
            edge_identity,
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

/// The serving thread drops its finished request node right after publishing
/// the reply; wait for that release, then compare the exact byte count.
fn settled(parent: &DeclaredReadMemory, expected: u64) -> u64 {
    for _ in 0..10_000 {
        if parent.retained_bytes_for_test() == expected {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
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
        "S4_STEP4 served(binding,push,pull,status)={served:?} replayed={} identical(binding,push,pull,status)={identical:?} differing={differing:?} native={} legacy={}",
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
    let refusal = match &refused {
        Err(crate::transport::WireError::Resource(
            error @ contextdb_core::Error::MemoryBudgetExceeded { .. },
        )) => error.to_string(),
        other => panic!("expected a typed memory refusal, got {other:?}"),
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
        cancelled_early,
        Err(crate::transport::WireError::Resource(
            contextdb_core::Error::ReadCancelled
        ))
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
        cancelled_late,
        Err(crate::transport::WireError::Resource(
            contextdb_core::Error::ReadCancelled
        ))
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

struct Scaling {
    examined: (u64, u64),
    request_len: usize,
    reply_len: usize,
    native_peak: u64,
    legacy_peak: u64,
    outcomes: usize,
}

async fn cursor_scaling(prior: usize) -> Scaling {
    eprintln!("S4_SCALE prior={prior} world");
    let world = World::new();
    let seeding = DeclaredReadMemory::from_reader_limit(LIMIT);
    let seeder = world.server(&seeding);
    let hub = start_legacy(&world, &seeder).await;
    eprintln!("S4_SCALE prior={prior} hub serving");
    world.bind().await;
    eprintln!("S4_SCALE prior={prior} bound");
    world.seed_outcomes(prior).await;
    hub.stop().await;
    // The cursor after the last prior outcome, read from one native request
    // (the Legacy SyncClient fetch of 10,010 outcomes ran over 30 minutes).
    eprintln!("S4_SCALE prior={prior} reading the prior cursor natively");
    let everything = DeclaredReadMemory::from_reader_limit(LIMIT);
    let route = installation(&world.server(&everything));
    let (result, reply) = drive(&world, &route, &world.request_bytes(1, None), |_| {});
    result.expect("native request for the prior outcomes");
    drop(route);
    let reply = reply.expect("prior outcomes reply");
    let envelope = crate::protocol::decode(&reply).expect("reply envelope");
    let page: crate::protocol::FetchDeliveryOutcomesResponse =
        rmp_serde::from_slice(&envelope.payload).expect("reply payload");
    let outcomes = page.result.expect("outcome page").outcomes;
    assert_eq!(outcomes.len(), prior);
    let memory = crate::read_image_memory::ImageMemory::charging_reader_for(
        world.hub_db.bounded_read_accountant(),
        everything.child().expect("decode reader"),
        "reader",
        "decode_prior_cursor",
        "test fixture",
    );
    let last = crate::custody::delivery::read_packet(outcomes.last().expect("prior"), &memory)
        .expect("signed packet");
    let cursor = rmp_serde::to_vec_named(
        &crate::custody::inspection::projection(&last.record, &last.block).cursor(),
    )
    .expect("cursor bytes");
    let hub = start_legacy(&world, &seeder).await;
    world.seed_outcomes(10).await;
    hub.stop().await;
    eprintln!("S4_SCALE prior={prior} seeded; driving native");
    let request = world.request_bytes(3, Some(cursor));

    let measured = DeclaredReadMemory::from_reader_limit(LIMIT);
    let route = installation(&world.server(&measured));
    crate::database::native_custody_selection::NATIVE_ORDERS_EXAMINED.with(|seen| seen.set((0, 0)));
    let (result, reply) = drive(&world, &route, &request, |_| {});
    result.expect("native cursor request");
    let examined =
        crate::database::native_custody_selection::NATIVE_ORDERS_EXAMINED.with(|seen| seen.get());
    let reply = reply.expect("reply");
    let reply_len = reply.len();
    let outcomes = decode_outcomes(&reply);
    eprintln!("S4_SCALE prior={prior} native done examined={examined:?}; legacy control");

    let legacy = DeclaredReadMemory::from_reader_limit(LIMIT);
    let control = world.legacy_reply(&legacy, &request);
    eprintln!("S4_SCALE prior={prior} legacy control done");
    assert_eq!(decode_outcomes(&control), outcomes);
    Scaling {
        examined,
        request_len: request.len(),
        reply_len,
        native_peak: measured.file_child_high_water(),
        legacy_peak: legacy.file_child_high_water(),
        outcomes,
    }
}

// Step 7: delivering the ten outcomes after a cursor examines exactly those ten
// orders, and charges the same peak, whether 10 or 10,010 outcomes precede it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cursor_request_examines_only_the_orders_after_it() {
    let small = cursor_scaling(10).await;
    let large = cursor_scaling(10_010).await;
    println!(
        "S4_STEP7 prior=10 examined(count,collect)={:?} outcomes={} request_len={} reply_len={} native_peak={} legacy_peak={} | prior=10010 examined(count,collect)={:?} outcomes={} request_len={} reply_len={} native_peak={} legacy_peak={} | legacy_growth={}",
        small.examined,
        small.outcomes,
        small.request_len,
        small.reply_len,
        small.native_peak,
        small.legacy_peak,
        large.examined,
        large.outcomes,
        large.request_len,
        large.reply_len,
        large.native_peak,
        large.legacy_peak,
        large.legacy_peak as i128 - small.legacy_peak as i128,
    );
    assert_eq!(small.outcomes, 10);
    assert_eq!(large.outcomes, 10);
    assert_eq!(small.examined, (10, 10));
    assert_eq!(large.examined, (10, 10));
    // The selection is the same ten orders in both worlds. Fresh random node
    // keys, identifiers and incarnations change those ten records' encoded
    // sizes by a few bytes per run, in either direction; history adds nothing.
    assert!(
        large.native_peak.abs_diff(small.native_peak) * 1000 <= small.native_peak,
        "native peak moved with history: {} -> {}",
        small.native_peak,
        large.native_peak
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
