//! Per-edge receipt confirmation.
//!
//! The defect these tests pin: the hub's received-up-to record is kept per
//! TENANT (`sync_server.rs` `applied_push_watermark`, raised at the end of any
//! edge's apply) while the authenticated edge identity rides every request. An
//! edge compares its own private position against that shared number, so with
//! TWO edges on one tenant a busy edge's progress answers the quiet edge's
//! question. Every test here therefore runs two distinct edges against one hub:
//! the single-edge version of each scenario passes today and proves nothing.
//!
//! Before this change, the two-edge fault scenarios below fail; the ordinary
//! two-edge steady-state scenario already passes as a regression guard.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads. Time
//! moves through `Wallclock::test_clock_guard` and pruning is driven
//! synchronously on the test thread.

use contextdb_core::{Incarnation, Lsn, TenantId, Value, Wallclock};
use contextdb_engine::Database;
use contextdb_engine::work_ledger::{advertise_capability, install_work_ledger_schema};
use contextdb_server::protocol::{
    MessageType, SyncStatusRequest, SyncStatusResponse, decode, encode,
};
use contextdb_server::subjects::{pull_subject, push_subject, status_subject};
use contextdb_server::transport::{
    ClientTransport, TransportError, TransportFuture, TransportResult, TransportStatusFuture,
};
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

const T0: u64 = 1_700_000_000_000;
const TENANT: &str = "per-edge-receipts";
/// The hub's authenticated node id. An edge dials its hub by key, so it always
/// knows who answered; the in-process broker presents this exactly as the p2p
/// transport path presents the dialed endpoint's node id.
const HUB_NODE: &str = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a";
/// The two edges. These are the identities the transport authenticates on the
/// connection — the hub reads them off the request, the request bytes never
/// carry them.
const EDGE_A: &str = "3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c";
const EDGE_B: &str = "ec172b93ad5e563bf4932c70e1245034c35467ef2efd4d64ebf819683467e2bf";
/// Two tenants served by ONE hub database (the multi-tenant hub shape). The
/// per-(tenant, edge) test drives the SAME authenticated edge into both.
const TENANT_1: &str = "per-edge-tenant-one";
const TENANT_2: &str = "per-edge-tenant-two";

const NOTES_DDL: &str =
    "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP FIRST";
/// Retention plus the delivery promise, with NO direction word: this spelling
/// parses identically regardless of the direction-clause grammar, so the
/// expired-rows-survive scenario pins the deletion gate rather than the grammar
/// in flight beside it.
const RETAINED_DDL: &str = "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) RETAIN 1 HOURS SYNC SAFE SYNC CONFLICT KEEP FIRST";

const HUB_SEED: [u8; 32] = [
    0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec, 0x2c, 0xc4,
    0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b, 0xac, 0x03, 0x1c, 0xae, 0x7f, 0x60,
];
const EDGE_A_SEED: [u8; 32] = [
    0x4c, 0xcd, 0x08, 0x9b, 0x28, 0xff, 0x96, 0xda, 0x9d, 0xb6, 0xc3, 0x46, 0xec, 0x11, 0x4e, 0x0f,
    0x5b, 0x8a, 0x31, 0x9f, 0x35, 0xab, 0xa6, 0x24, 0xda, 0x8c, 0xf6, 0xed, 0x4f, 0xb8, 0xa6, 0xfb,
];
const EDGE_B_SEED: [u8; 32] = [
    0x83, 0x3f, 0xe6, 0x24, 0x09, 0x23, 0x7b, 0x9d, 0x62, 0xec, 0x77, 0x58, 0x75, 0x20, 0x91, 0x1e,
    0x9a, 0x75, 0x9c, 0xec, 0x1d, 0x19, 0x75, 0x5b, 0x7d, 0xa9, 0x01, 0xb9, 0x6d, 0xca, 0x3d, 0x42,
];

fn identity_from_seed(node_id: &str, seed: [u8; 32]) -> Arc<FabricIdentity> {
    let mut key = tempfile::NamedTempFile::new().expect("temporary fabric identity");
    key.write_all(&seed).expect("write test fabric identity");
    key.flush().expect("flush test fabric identity");
    let identity =
        Arc::new(FabricIdentity::load_or_generate(key.path()).expect("load test fabric identity"));
    assert_eq!(
        identity.node_id(),
        node_id,
        "test identity must match its node id"
    );
    identity
}

fn identity_for_node(node_id: &str) -> Arc<FabricIdentity> {
    static HUB: OnceLock<Arc<FabricIdentity>> = OnceLock::new();
    static EDGE_A_IDENTITY: OnceLock<Arc<FabricIdentity>> = OnceLock::new();
    static EDGE_B_IDENTITY: OnceLock<Arc<FabricIdentity>> = OnceLock::new();
    match node_id {
        HUB_NODE => HUB
            .get_or_init(|| identity_from_seed(HUB_NODE, HUB_SEED))
            .clone(),
        EDGE_A => EDGE_A_IDENTITY
            .get_or_init(|| identity_from_seed(EDGE_A, EDGE_A_SEED))
            .clone(),
        EDGE_B => EDGE_B_IDENTITY
            .get_or_init(|| identity_from_seed(EDGE_B, EDGE_B_SEED))
            .clone(),
        other => panic!("unknown test fabric node identity: {other}"),
    }
}

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

/// Bounded wrapper so a red state can never hang the suite.
async fn within<T>(fut: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(60), fut)
        .await
        .expect("sync operation must complete within 60s")
}

struct MockClock(Arc<AtomicU64>);

impl MockClock {
    fn install(start_millis: u64) -> (Self, contextdb_core::WallclockTestClockGuard) {
        let cell = Arc::new(AtomicU64::new(start_millis));
        let guard = {
            let cell = Arc::clone(&cell);
            Wallclock::test_clock_guard(move || cell.load(Ordering::SeqCst))
        };
        (Self(cell), guard)
    }

    fn advance(&self, millis: u64) {
        self.0.fetch_add(millis, Ordering::SeqCst);
    }
}

fn create_tables(db: &Database) {
    db.execute(NOTES_DDL, &p()).expect("notes table");
    db.execute(RETAINED_DDL, &p()).expect("retained table");
}

fn insert_notes(db: &Database, ids: std::ops::Range<i64>) {
    for id in ids {
        let mut row = p();
        row.insert("id".to_string(), Value::Int64(id));
        row.insert("body".to_string(), Value::Text(format!("note-{id}")));
        db.execute("INSERT INTO notes (id, body) VALUES ($id, $body)", &row)
            .expect("note insert");
    }
}

fn insert_windows(db: &Database, ids: std::ops::Range<i64>) {
    for id in ids {
        let mut row = p();
        row.insert("id".to_string(), Value::Int64(id));
        row.insert("body".to_string(), Value::Text(format!("window-{id}")));
        db.execute("INSERT INTO windows (id, body) VALUES ($id, $body)", &row)
            .expect("window insert");
    }
}

fn row_count(db: &Database, table: &str) -> usize {
    db.execute(&format!("SELECT * FROM {table}"), &p())
        .unwrap_or_else(|err| panic!("{table} scan must succeed: {err}"))
        .rows
        .len()
}

/// The exact `body` values a table holds — value assertions, never counts
/// alone, so a row that arrived from the wrong edge cannot pass.
fn bodies(db: &Database, table: &str) -> std::collections::BTreeSet<String> {
    let result = db
        .execute(&format!("SELECT id, body FROM {table}"), &p())
        .unwrap_or_else(|err| panic!("{table} scan must succeed: {err}"));
    let idx = result
        .columns
        .iter()
        .position(|c| c == "body")
        .expect("result must project column 'body'");
    result
        .rows
        .iter()
        .map(|row| match &row[idx] {
            Value::Text(t) => t.clone(),
            other => panic!("body column must be TEXT, got {other:?}"),
        })
        .collect()
}

fn expect_bodies(prefix: &str, ids: std::ops::Range<i64>) -> std::collections::BTreeSet<String> {
    ids.map(|id| format!("{prefix}-{id}")).collect()
}

fn both(
    left: std::collections::BTreeSet<String>,
    right: std::collections::BTreeSet<String>,
) -> std::collections::BTreeSet<String> {
    left.union(&right).cloned().collect()
}

struct RunningHub {
    db: Arc<Database>,
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl RunningHub {
    async fn stop(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = self.task.await;
    }
}

fn start_hub_on_tenant(broker: &InProcessBroker, db: Arc<Database>, tenant: &str) -> RunningHub {
    let identity = identity_for_node(HUB_NODE);
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            db.clone(),
            broker.server_as(HUB_NODE),
            TenantId::from(tenant),
            HUB_NODE.to_string(),
            identity,
        ),
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    RunningHub { db, shutdown, task }
}

fn start_hub_on(broker: &InProcessBroker, db: Arc<Database>) -> RunningHub {
    start_hub_on_tenant(broker, db, TENANT)
}

fn start_hub(broker: &InProcessBroker) -> RunningHub {
    let db = Arc::new(Database::open_memory());
    create_tables(&db);
    start_hub_on(broker, db)
}

fn open_edge() -> Arc<Database> {
    let db = Arc::new(Database::open_memory());
    create_tables(&db);
    db
}

fn edge_client_on_tenant(
    db: &Arc<Database>,
    broker: &InProcessBroker,
    node_id: &str,
    tenant: &str,
) -> SyncClient {
    let identity = identity_for_node(node_id);
    SyncClient::with_authenticated_transport_and_identity_for_test(
        db.clone(),
        broker.client_as(node_id),
        TenantId::from(tenant),
        identity,
    )
}

fn edge_client(db: &Arc<Database>, broker: &InProcessBroker, node_id: &str) -> SyncClient {
    edge_client_on_tenant(db, broker, node_id, TENANT)
}

/// Subjects of the exchanges recorded on `broker` after `from` (exclusive), in
/// order. The ordinary two-edge push test feeds them to
/// `assert_confirms_from_direct_reply` to prove an ordinary push confirms from
/// its direct reply and not a status round-trip.
fn new_exchange_subjects(broker: &InProcessBroker, from: usize) -> Vec<String> {
    broker
        .recorded_exchanges()
        .into_iter()
        .skip(from)
        .map(|exchange| exchange.subject)
        .collect()
}

/// A successful ordinary push confirms from its direct `PushResponse`, never
/// from a post-push status probe. Asserting only that the LAST exchange is the
/// push subject is too weak — a `push → status(confirm) → push` path also ends
/// on a push. So this pins the WHOLE sequence: an optional pre-push
/// stale-restore status probe, then one or more push exchanges, and NO status
/// exchange after any push. A confirmation obtained by a status round-trip
/// wedged between pushes is exactly what it rejects.
fn assert_confirms_from_direct_reply(subjects: &[String], tenant: &str) {
    let push = push_subject(tenant);
    let status = status_subject(tenant);
    assert!(
        subjects.contains(&push),
        "a successful ordinary push must exchange on the push subject: {subjects:?}"
    );
    let mut seen_push = false;
    for subject in subjects {
        if *subject == push {
            seen_push = true;
        } else if *subject == status {
            assert!(
                !seen_push,
                "no status exchange may follow a push on the ordinary path and \
                 stand in for its acknowledgement — confirmation comes from the \
                 direct reply, so any status probe is only the pre-push \
                 stale-restore check: {subjects:?}"
            );
        } else {
            panic!(
                "unexpected exchange on the ordinary push path (only the \
                 pre-push status probe and the push itself are allowed): {subjects:?}"
            );
        }
    }
}

/// Ask a tenant's hub for sync status AS a named edge, over the ordinary status
/// subject with the ordinary request bytes. The asking identity is carried by
/// the transport connection alone — this is the whole point of the per-edge
/// status test, so the helper deliberately has no way to name an edge inside the
/// payload; the tenant
/// selects the subject, exactly as it does on the shipped path.
async fn status_for(
    broker: &InProcessBroker,
    tenant: &str,
    node_id: &str,
    incarnation: Incarnation,
) -> (Vec<u8>, SyncStatusResponse) {
    let request_bytes = encode(
        MessageType::StatusRequest,
        &SyncStatusRequest { incarnation },
    )
    .expect("encode status request");
    let transport = broker.client_as(node_id);
    let reply = within(transport.request_single_reply(
        &status_subject(tenant),
        request_bytes.clone(),
        Duration::from_secs(5),
    ))
    .await
    .unwrap_or_else(|err| panic!("hub must answer the status subject for {node_id}: {err}"));
    let envelope = decode(&reply).expect("decode status envelope");
    assert!(
        matches!(envelope.message_type, MessageType::StatusResponse),
        "the status subject must answer with a status response"
    );
    let response: SyncStatusResponse =
        rmp_serde::from_slice(&envelope.payload).expect("decode status payload");
    (request_bytes, response)
}

/// The single-tenant convenience used by the per-edge status test. The incarnation is the asking
/// edge's own — read from its database — so the hub answers with the record for
/// THIS life of the edge, exactly as the real client would stamp it.
async fn status_as(
    broker: &InProcessBroker,
    edge_db: &Arc<Database>,
    node_id: &str,
) -> (Vec<u8>, SyncStatusResponse) {
    let incarnation = edge_db
        .sync_incarnation(&TenantId::from(TENANT))
        .expect("read edge incarnation");
    status_for(broker, TENANT, node_id, incarnation).await
}

/// A transport whose push request is LOST ON THE WAY OUT — the hub never sees
/// the bytes, so it never stores the batch, and the edge cannot tell that from
/// a hub that applied and then died before acknowledging. That
/// indistinguishability is the scenario: the edge must not resolve it by
/// trusting a number another edge moved.
///
/// Honest-double notes: the status subject stays wired to the live hub (the hub
/// is up and answering — only this batch was dropped), the failure surfaces as
/// an ordinary `TransportError`, and `peer_node_id` is forwarded from the
/// wrapped transport so the authenticated identity is unchanged. Nothing here
/// returns a shaped reply.
struct DropPushRequestOnTheWire {
    inner: Arc<dyn ClientTransport>,
    push_subject: String,
}

impl DropPushRequestOnTheWire {
    fn dropped() -> TransportError {
        TransportError::Unreachable(
            "push request dropped on the wire before it reached the hub".to_string(),
        )
    }
}

impl ClientTransport for DropPushRequestOnTheWire {
    fn peer_node_id(&self) -> Option<String> {
        self.inner.peer_node_id()
    }

    fn local_node_id(&self) -> Option<String> {
        self.inner.local_node_id()
    }

    fn has_stable_edge_identity(&self) -> bool {
        self.inner.has_stable_edge_identity()
    }

    fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.ensure_connected()
    }

    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        self.inner.is_connected()
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        if subject == self.push_subject {
            return Box::pin(async { Err(Self::dropped()) });
        }
        self.inner.request(subject, request_bytes, timeout)
    }

    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        if subject == self.push_subject {
            return Box::pin(async { Err(Self::dropped()) });
        }
        self.inner
            .request_single_reply(subject, request_bytes, timeout)
    }

    fn ensure_single_reply_retry_safe(&self, request_bytes: &[u8]) -> TransportResult<()> {
        self.inner.ensure_single_reply_retry_safe(request_bytes)
    }

    fn shutdown<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.shutdown()
    }
}

fn dropped_push_client(db: &Arc<Database>, broker: &InProcessBroker, node_id: &str) -> SyncClient {
    let identity = identity_for_node(node_id);
    SyncClient::with_authenticated_transport_and_identity_for_test(
        db.clone(),
        Arc::new(DropPushRequestOnTheWire {
            inner: broker.client_as(node_id),
            push_subject: push_subject(TENANT),
        }),
        TenantId::from(TENANT),
        identity,
    )
}

/// The busy edge, whose only job is to leave the tenant's shared received-up-to
/// number far ahead of the quiet edge's own position. Returns what the hub
/// confirmed to it.
async fn busy_edge_pushes_ahead(
    broker: &InProcessBroker,
    db: &Arc<Database>,
    ids: std::ops::Range<i64>,
) -> (SyncClient, Lsn) {
    let client = edge_client(db, broker, EDGE_B);
    insert_notes(db, ids);
    within(client.push()).await.expect("busy edge push");
    let confirmed = client.push_watermark();
    (client, confirmed)
}

// ---------------------------------------------------------------------------
// The status answer belongs to the edge that asked
// ---------------------------------------------------------------------------

/// Edge A pushed a small, early batch; edge B then pushed a much larger one and
/// carried the tenant's shared number far past it. Asked "what do you hold from
/// me?", the hub must answer A with A's position — not with B's.
#[tokio::test]
async fn c3a_status_answers_the_asking_edge_not_the_tenant_high_water() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);

    let edge_a = open_edge();
    let client_a = edge_client(&edge_a, &broker, EDGE_A);
    insert_notes(&edge_a, 1..4);
    within(client_a.push()).await.expect("edge A push");
    let a_confirmed = client_a.push_watermark();
    assert!(a_confirmed > Lsn(0), "the hub really took edge A's batch");

    let edge_b = open_edge();
    let (_client_b, b_confirmed) = busy_edge_pushes_ahead(&broker, &edge_b, 100..300).await;
    assert!(
        b_confirmed > a_confirmed,
        "fixture: edge B must end up ahead of edge A, otherwise the shared \
         number and the per-edge number coincide and prove nothing \
         (A={a_confirmed:?}, B={b_confirmed:?})"
    );

    let (a_request, a_status) = status_as(&broker, &edge_a, EDGE_A).await;
    let (b_request, b_status) = status_as(&broker, &edge_b, EDGE_B).await;

    // The asking NODE identity still comes from the authenticated connection, not
    // the payload: the request carries only the edge's per-life incarnation, so
    // neither request contains its node id. That is what keeps the per-edge answer
    // un-spoofable from the wire.
    for (request, node) in [(&a_request, EDGE_A), (&b_request, EDGE_B)] {
        assert!(
            !request.windows(node.len()).any(|w| w == node.as_bytes()),
            "the status request payload must not carry the asking node id ({node}); \
             the hub selects the per-edge record from the authenticated connection"
        );
    }
    assert_eq!(
        a_status.applied_push_watermark,
        Some(a_confirmed),
        "the hub must tell edge A what it holds FROM EDGE A \
         ({a_confirmed:?}); before this change it answered with the tenant's shared \
         received-up-to record, which edge B raised to {b_confirmed:?}"
    );
    assert_eq!(
        b_status.applied_push_watermark,
        Some(b_confirmed),
        "and edge B is still told its own, unchanged position"
    );
    assert_ne!(
        a_status.applied_push_watermark, b_status.applied_push_watermark,
        "two edges at genuinely different positions must not receive the same \
         answer to the same question"
    );

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// A lost reply on a batch the hub never stored is never success
// ---------------------------------------------------------------------------

/// Edge A's batch never reaches the hub and its reply never comes back. Because
/// edge B has pushed the shared number past A's batch, today's reconciliation
/// reads that number as confirmation, reports success, and abandons rows the
/// hub has never seen. The edge must instead resend or report the outcome
/// unknown — and the rows must end up on the hub.
#[tokio::test]
async fn c3b_a_lost_reply_on_a_batch_the_hub_never_stored_is_never_reported_as_success() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);

    let edge_b = open_edge();
    let (_client_b, b_confirmed) = busy_edge_pushes_ahead(&broker, &edge_b, 100..300).await;

    let edge_a = open_edge();
    insert_notes(&edge_a, 1..6);
    assert!(
        edge_a.current_lsn() < b_confirmed,
        "fixture: edge A's batch must sit BELOW the shared received-up-to number \
         edge B raised ({:?} vs {b_confirmed:?}) — that is what makes the hub's \
         answer a false confirmation",
        edge_a.current_lsn()
    );

    let unlucky = dropped_push_client(&edge_a, &broker, EDGE_A);
    let outcome = within(unlucky.push()).await;

    assert_eq!(
        row_count(&hub.db, "notes"),
        200,
        "premise: the hub holds edge B's rows and NOT ONE of edge A's — the \
         batch never reached it"
    );
    assert!(
        outcome.is_err(),
        "a batch the hub never stored must never be reported as a \
         successful push. Before this change edge B's progress on the shared record \
         answers edge A's question and this returns Ok, got {outcome:?}"
    );
    assert!(
        matches!(
            outcome,
            Err(contextdb_core::Error::SyncPushUnconfirmed { .. })
        ),
        "and the outcome the edge reports is the distinct unconfirmed one, got \
         {outcome:?}"
    );

    // The consequence that matters: the backlog is still the edge's to send.
    let healthy = edge_client(&edge_a, &broker, EDGE_A);
    within(healthy.push()).await.expect("edge A re-push");
    assert_eq!(
        bodies(&hub.db, "notes"),
        both(expect_bodies("note", 1..6), expect_bodies("note", 100..300)),
        "the hub must end up holding edge A's rows by value, alongside edge B's"
    );

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// Expired rows survive until the hub genuinely holds them
// ---------------------------------------------------------------------------

/// The same lost batch, on a `SYNC SAFE` retained table, with the rows past
/// their window. A false confirmation opens the engine's deletion gate, so the
/// edge deletes rows nothing else in the world holds. This is the data-loss
/// face of the defect.
#[tokio::test]
async fn c3c_expired_rows_survive_until_the_hub_genuinely_holds_them() {
    let (clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);

    let edge_b = open_edge();
    let (_client_b, b_confirmed) = busy_edge_pushes_ahead(&broker, &edge_b, 100..300).await;

    let edge_a = open_edge();
    insert_windows(&edge_a, 1..6);
    assert!(
        edge_a.current_lsn() < b_confirmed,
        "fixture: edge A's retained batch must sit below the shared number \
         ({:?} vs {b_confirmed:?})",
        edge_a.current_lsn()
    );
    assert_eq!(
        edge_a.sync_watermark(),
        Lsn(0),
        "nothing is confirmed yet, so the SYNC SAFE deletion gate is fully closed"
    );

    // How the edge REPORTS this push is the previous scenario's subject; this
    // test's subject is what the edge then does to its own data, so the outcome
    // is deliberately not asserted here — the test must reach the retention
    // cycle either way.
    let unlucky = dropped_push_client(&edge_a, &broker, EDGE_A);
    let _outcome = within(unlucky.push()).await;
    assert_eq!(
        row_count(&hub.db, "windows"),
        0,
        "premise: the batch never reached the hub, so it holds none of these rows"
    );

    // Age the whole batch well past its one-hour window and drive retention
    // synchronously.
    clock.advance(48 * 60 * 60 * 1000);
    let report = edge_a
        .run_pruning_cycle_checked()
        .expect("a prune cycle still runs");

    assert_eq!(
        report.pruned_rows, 0,
        "not one row may be deleted while the hub does not hold it. \
         Before this change the lost batch was 'confirmed' by edge B's progress on \
         the shared record, the deletion gate opened, and retention took rows that \
         exist nowhere else: {report:?}"
    );
    assert_eq!(
        bodies(&edge_a, "windows"),
        expect_bodies("window", 1..6),
        "the rows must still be on the edge, by value"
    );
    assert_eq!(
        edge_a.sync_watermark(),
        Lsn(0),
        "and the deletion gate must not have opened at all"
    );

    // Once the hub genuinely holds them, ordinary retention resumes.
    let healthy = edge_client(&edge_a, &broker, EDGE_A);
    within(healthy.push()).await.expect("edge A re-push");
    assert_eq!(
        bodies(&hub.db, "windows"),
        expect_bodies("window", 1..6),
        "the hub now holds the rows by value"
    );
    assert_eq!(
        edge_a
            .run_pruning_cycle_checked()
            .expect("prune after genuine delivery")
            .pruned_rows,
        5,
        "and only now may the edge delete its expired copies"
    );

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// A hub restored from an older copy is noticed
// ---------------------------------------------------------------------------

/// The hub is replaced by an older copy of itself, losing commits it had
/// already acknowledged to edge A, while edge B keeps pushing. Edge B's traffic
/// holds the shared received-up-to number above edge A's position, so today
/// edge A's regression check sees nothing wrong and its lost rows are never
/// re-uploaded.
#[tokio::test]
async fn c3d_a_hub_restored_from_an_older_copy_is_noticed_while_another_edge_pushes() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let broker = InProcessBroker::new();
    let hub_db = Arc::new(Database::open(tmp.path().join("hub-gen1.db")).expect("open hub db"));
    create_tables(&hub_db);
    let gen1 = start_hub_on(&broker, hub_db.clone());

    // Edge A's first batch, acknowledged.
    let edge_a = open_edge();
    let client_a = edge_client(&edge_a, &broker, EDGE_A);
    insert_notes(&edge_a, 1..4);
    within(client_a.push()).await.expect("edge A first push");

    // Edge B pushes a much larger batch, carrying the shared number far ahead.
    let edge_b = open_edge();
    let (_client_b, b_confirmed) = busy_edge_pushes_ahead(&broker, &edge_b, 100..300).await;

    // The artifact is taken HERE: it holds everything above and nothing below.
    let artifact = tmp.path().join("hub-checkpoint.cdb");
    hub_db.export_snapshot(&artifact).expect("export artifact");

    // Edge A's second batch, also acknowledged — and the commits the restore
    // will silently lose.
    insert_notes(&edge_a, 4..7);
    within(client_a.push()).await.expect("edge A second push");
    let a_confirmed = client_a.push_watermark();
    assert!(
        a_confirmed < b_confirmed,
        "fixture: edge A stays below the shared number edge B set \
         ({a_confirmed:?} vs {b_confirmed:?}) — that is what hides the regression"
    );
    assert_eq!(
        bodies(&hub_db, "notes"),
        both(expect_bodies("note", 1..7), expect_bodies("note", 100..300)),
        "precondition: the live hub acknowledged and holds everything"
    );

    // The hub comes back from the older copy.
    gen1.stop().await;
    let restored = Arc::new(Database::open(&artifact).expect("open restored hub"));
    assert_eq!(
        bodies(&restored, "notes"),
        both(expect_bodies("note", 1..4), expect_bodies("note", 100..300)),
        "precondition: the artifact predates edge A's second batch"
    );
    let gen2 = start_hub_on(&broker, restored.clone());

    // Edge B carries on as if nothing happened, keeping the shared number high.
    insert_notes(&edge_b, 300..305);
    let client_b_again = edge_client(&edge_b, &broker, EDGE_B);
    within(client_b_again.push())
        .await
        .expect("edge B carries on");

    // Edge A's next ordinary push is where it must notice.
    within(client_a.push()).await.expect("edge A next push");

    assert_eq!(
        bodies(&restored, "notes"),
        both(expect_bodies("note", 1..7), expect_bodies("note", 100..305)),
        "edge A's acknowledged commits were lost by the restore and \
         must be re-uploaded on its next push. Before this change edge B's traffic \
         keeps the shared received-up-to record above edge A's position, so edge A \
         sees no regression and its rows are gone for good"
    );

    gen2.stop().await;
}

/// A restored hub must re-order an edge's lost acknowledged value in its NEW
/// history. If the edge keeps the discarded old hub arrival, a later edit on
/// the restored hub loses numerically even though it committed later there.
#[tokio::test]
async fn restored_hub_reorders_lost_accepted_value_before_a_later_same_key_edit() {
    let ddl = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST";
    let temp = tempfile::TempDir::new().expect("tempdir");
    let broker = InProcessBroker::new();
    let live_path = temp.path().join("hub-live.db");
    let edge_path = temp.path().join("edge-a.db");
    let live_db = Arc::new(Database::open(&live_path).expect("open live hub"));
    live_db.execute(ddl, &p()).expect("create latest hub table");
    let first_hub = start_hub_on(&broker, live_db.clone());
    let edge_a = Arc::new(Database::open(&edge_path).expect("open persistent edge A"));
    edge_a.execute(ddl, &p()).expect("create latest A table");
    let edge_b = Arc::new(Database::open_memory());
    edge_b.execute(ddl, &p()).expect("create latest B table");
    let client_a = edge_client(&edge_a, &broker, EDGE_A);
    let client_b = edge_client(&edge_b, &broker, EDGE_B);

    insert_notes(&edge_a, 1..2);
    within(client_a.push()).await.expect("seed A value");
    within(client_b.pull_default()).await.expect("seed B value");
    let artifact = temp.path().join("hub-before-lost-value.cdb");
    live_db
        .export_snapshot(&artifact)
        .expect("export pre-loss hub");

    edge_a
        .execute("UPDATE notes SET body = 'a-lost' WHERE id = 1", &p())
        .expect("A writes value later lost by restore");
    within(client_a.push())
        .await
        .expect("live hub accepts A lost value");
    assert_eq!(
        bodies(&live_db, "notes"),
        std::collections::BTreeSet::from(["a-lost".to_string()]),
        "fixture: live hub has A's later acknowledged value"
    );

    first_hub.stop().await;
    let restored = Arc::new(Database::open(&artifact).expect("open restored hub"));
    let second_hub = start_hub_on(&broker, restored.clone());
    within(client_a.push())
        .await
        .expect("A detects restore and re-sends its lost value");
    assert_eq!(
        bodies(&restored, "notes"),
        std::collections::BTreeSet::from(["a-lost".to_string()]),
        "the restored hub holds A's re-ordered lost value before B edits it"
    );

    within(client_b.pull_default())
        .await
        .expect("B receives restored-hub A value");
    edge_b
        .execute("UPDATE notes SET body = 'b-later' WHERE id = 1", &p())
        .expect("B writes later value on restored hub history");
    within(client_b.push())
        .await
        .expect("restored hub accepts B later value");
    within(client_a.pull_default())
        .await
        .expect("A pulls B later value");

    let expected = std::collections::BTreeSet::from(["b-later".to_string()]);
    for (name, db) in [
        ("restored hub", &restored),
        ("edge A", &edge_a),
        ("edge B", &edge_b),
    ] {
        assert_eq!(
            bodies(db, "notes"),
            expected,
            "{name} must retain the later restored-hub edit, not old discarded order"
        );
    }
    second_hub.stop().await;
}

/// A restored-hub resend can land while its acknowledgement and first recovery
/// pull both fail. The next push sees a status that covers the pending batch;
/// it must use the explicit confirmed reconciliation mode so the identical echo
/// refreshes Pending provenance, then later restored-hub truth can win.
#[tokio::test]
async fn landed_ack_lost_after_restore_recovers_pending_on_next_push_before_later_edit() {
    let ddl = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST";
    let temp = tempfile::TempDir::new().expect("tempdir");
    let broker = InProcessBroker::new();
    let live_path = temp.path().join("hub-live.db");
    let edge_path = temp.path().join("edge-a.db");
    let live_db = Arc::new(Database::open(&live_path).expect("open live hub"));
    live_db.execute(ddl, &p()).expect("create latest hub table");
    let first_hub = start_hub_on(&broker, live_db.clone());
    let edge_a = Arc::new(Database::open(&edge_path).expect("open persistent edge A"));
    edge_a.execute(ddl, &p()).expect("create latest A table");
    let edge_b = Arc::new(Database::open_memory());
    edge_b.execute(ddl, &p()).expect("create latest B table");
    let client_a = edge_client(&edge_a, &broker, EDGE_A);
    let client_b = edge_client(&edge_b, &broker, EDGE_B);

    insert_notes(&edge_a, 1..2);
    within(client_a.push()).await.expect("seed A value");
    within(client_b.pull_default()).await.expect("seed B value");
    let artifact = temp.path().join("hub-before-lost-value.cdb");
    live_db
        .export_snapshot(&artifact)
        .expect("export pre-loss hub");

    edge_a
        .execute("UPDATE notes SET body = 'a-lost' WHERE id = 1", &p())
        .expect("A writes value later lost by restore");
    let lost_lsn = edge_a.current_lsn();
    within(client_a.push())
        .await
        .expect("live hub accepts A lost value");

    first_hub.stop().await;
    let restored = Arc::new(Database::open(&artifact).expect("open restored hub"));
    let second_hub = start_hub_on(&broker, restored.clone());
    let landed_but_unreconciled =
        landed_ack_lost_with_recovery_pull_dropped_client(&edge_a, &broker, EDGE_A);
    let interrupted = within(landed_but_unreconciled.push()).await;
    assert!(
        interrupted.is_err(),
        "the lost acknowledgement plus lost recovery pull leaves the batch pending: {interrupted:?}"
    );
    assert_eq!(
        bodies(&restored, "notes"),
        std::collections::BTreeSet::from(["a-lost".to_string()]),
        "fixture: the restored hub received the resend before its acknowledgement was lost"
    );
    assert_eq!(
        edge_a
            .persisted_sync_pending_push_confirmation(&TenantId::from(TENANT))
            .expect("read durable pending confirmation"),
        Some(lost_lsn),
        "the failed recovery leaves a durable no-send gate for the next push"
    );

    let recovered_a = edge_client(&edge_a, &broker, EDGE_A);
    within(recovered_a.push())
        .await
        .expect("next push status-confirms and reconciles the landed resend");
    assert_eq!(
        edge_a
            .persisted_sync_pending_push_confirmation(&TenantId::from(TENANT))
            .expect("read cleared pending confirmation"),
        None,
        "only the completed confirmed pull may retire the durable no-send gate"
    );
    let (_, arrivals) = edge_a.changes_since_with_arrivals(Lsn(0));
    assert!(
        matches!(arrivals.get(&lost_lsn), Some(Some(_))),
        "the confirmed identical echo must replace Pending's missing arrival: {arrivals:?}"
    );

    within(client_b.pull_default())
        .await
        .expect("B receives restored A value");
    edge_b
        .execute("UPDATE notes SET body = 'b-later' WHERE id = 1", &p())
        .expect("B writes later restored-hub value");
    within(client_b.push())
        .await
        .expect("restored hub accepts B later value");
    within(recovered_a.pull_default())
        .await
        .expect("A pulls B later value after pending refresh");

    let expected = std::collections::BTreeSet::from(["b-later".to_string()]);
    for (name, db) in [
        ("restored hub", &restored),
        ("edge A", &edge_a),
        ("edge B", &edge_b),
    ] {
        assert_eq!(
            bodies(db, "notes"),
            expected,
            "{name} must accept the later edit after the lost-ack recovery refresh"
        );
    }
    second_hub.stop().await;
}

/// A hub can be restored below a batch that landed but whose acknowledgement
/// and recovery pull were lost. Status is then below the durable pending
/// target: the reconciliation pull must remain ordinary and the next send must
/// re-deliver the local value without treating the stale hub history as an ack.
#[tokio::test]
async fn restored_before_pending_preserves_pending_until_the_resend_is_redelivered() {
    let ddl = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST";
    let temp = tempfile::TempDir::new().expect("tempdir");
    let broker = InProcessBroker::new();
    let live_path = temp.path().join("hub-live.db");
    let edge_path = temp.path().join("edge-a.db");
    let live_db = Arc::new(Database::open(&live_path).expect("open live hub"));
    live_db.execute(ddl, &p()).expect("create latest hub table");
    let first_hub = start_hub_on(&broker, live_db.clone());
    let edge_a = Arc::new(Database::open(&edge_path).expect("open persistent edge A"));
    edge_a.execute(ddl, &p()).expect("create latest A table");
    let edge_b = Arc::new(Database::open_memory());
    edge_b.execute(ddl, &p()).expect("create latest B table");
    let client_a = edge_client(&edge_a, &broker, EDGE_A);
    let client_b = edge_client(&edge_b, &broker, EDGE_B);

    insert_notes(&edge_a, 1..2);
    within(client_a.push()).await.expect("seed A value");
    let artifact = temp.path().join("hub-before-lost-value.cdb");
    live_db
        .export_snapshot(&artifact)
        .expect("export pre-loss hub");
    edge_a
        .execute("UPDATE notes SET body = 'a-pending' WHERE id = 1", &p())
        .expect("write value later lost by restore");
    let pending_lsn = edge_a.current_lsn();
    let landed_but_unreconciled =
        landed_ack_lost_with_recovery_pull_dropped_client(&edge_a, &broker, EDGE_A);
    assert!(
        within(landed_but_unreconciled.push()).await.is_err(),
        "fixture: the live hub accepted the update but the acknowledgement/recovery were lost"
    );
    assert_eq!(
        edge_a
            .persisted_sync_pending_push_confirmation(&TenantId::from(TENANT))
            .expect("read durable pending target"),
        Some(pending_lsn),
        "fixture: the landed live update has a durable confirmation target"
    );

    first_hub.stop().await;
    let restored = Arc::new(Database::open(&artifact).expect("open restored hub"));
    let second_hub = start_hub_on(&broker, restored.clone());
    let final_retry = edge_client(&edge_a, &broker, EDGE_A);
    within(final_retry.push())
        .await
        .expect("below-target status pulls ordinarily then re-delivers the pending value");
    assert_eq!(
        bodies(&edge_a, "notes"),
        std::collections::BTreeSet::from(["a-pending".to_string()]),
        "the stale restored hub value cannot replace the local pending value"
    );
    assert_eq!(
        bodies(&restored, "notes"),
        std::collections::BTreeSet::from(["a-pending".to_string()]),
        "the below-target branch re-delivers rather than falsely confirming the lost batch"
    );
    assert_eq!(
        edge_a
            .persisted_sync_pending_push_confirmation(&TenantId::from(TENANT))
            .expect("read cleared pending target"),
        None,
        "the ordinary below-target pull clears only the confirmation target before re-delivery"
    );
    within(client_b.pull_default())
        .await
        .expect("B receives A re-delivered value");
    edge_b
        .execute("UPDATE notes SET body = 'b-later' WHERE id = 1", &p())
        .expect("B writes later restored-hub value");
    within(client_b.push())
        .await
        .expect("restored hub accepts B later value");
    within(final_retry.pull_default())
        .await
        .expect("A receives later hub value");
    let expected = std::collections::BTreeSet::from(["b-later".to_string()]);
    for (name, db) in [("restored hub", &restored), ("edge A", &edge_a)] {
        assert_eq!(
            bodies(db, "notes"),
            expected,
            "{name} must accept the later restored-hub edit after re-delivery"
        );
    }
    second_hub.stop().await;
}

/// Even when status recovery succeeds immediately, its identical pull echo
/// must leave A's local value AcceptedLocal rather than Pulled. A later hub
/// restore therefore re-uploads it, and B's subsequent restored-history edit
/// still wins everywhere.
#[tokio::test]
async fn successful_lost_ack_recovery_survives_later_restore_and_later_edit() {
    let ddl = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST";
    let temp = tempfile::TempDir::new().expect("tempdir");
    let broker = InProcessBroker::new();
    let live_path = temp.path().join("hub-live.db");
    let edge_path = temp.path().join("edge-a.db");
    let live = Arc::new(Database::open(&live_path).expect("open live hub"));
    live.execute(ddl, &p()).expect("create latest hub table");
    let first_hub = start_hub_on(&broker, live.clone());
    let edge_a = Arc::new(Database::open(&edge_path).expect("open persistent edge A"));
    let edge_b = Arc::new(Database::open_memory());
    edge_a.execute(ddl, &p()).expect("create latest A table");
    edge_b.execute(ddl, &p()).expect("create latest B table");
    let client_a = edge_client(&edge_a, &broker, EDGE_A);
    let client_b = edge_client(&edge_b, &broker, EDGE_B);

    insert_notes(&edge_a, 1..2);
    within(client_a.push()).await.expect("seed A value");
    within(client_b.pull_default()).await.expect("seed B value");
    let artifact = temp.path().join("hub-before-ack-loss.cdb");
    live.export_snapshot(&artifact)
        .expect("export pre-loss hub");
    edge_a
        .execute("UPDATE notes SET body = 'a-recovered' WHERE id = 1", &p())
        .expect("A writes lost-ack value");
    let recovered_lsn = edge_a.current_lsn();
    let lost_ack = landed_ack_lost_client(&edge_a, &broker, EDGE_A);
    within(lost_ack.push())
        .await
        .expect("status recovery pulls the exact live hub order");
    assert_eq!(
        edge_a
            .persisted_sync_pending_push_confirmation(&TenantId::from(TENANT))
            .expect("read cleared recovery marker"),
        None,
        "a completed status recovery retires its durable pending marker"
    );
    let (outbound, arrivals) = edge_a.changes_since_with_arrivals(Lsn(0));
    let recovered_row = outbound
        .rows
        .iter()
        .find(|row| row.lsn == recovered_lsn)
        .expect("recovered local row remains in history");
    assert!(
        matches!(arrivals.get(&recovered_lsn), Some(Some(_)))
            && !edge_a.row_change_arrived_by_sync(recovered_row),
        "the successful identical echo records exact AcceptedLocal order, never Pulled"
    );

    first_hub.stop().await;
    let restored = Arc::new(Database::open(&artifact).expect("open restored hub"));
    let second_hub = start_hub_on(&broker, restored.clone());
    let restored_a = edge_client(&edge_a, &broker, EDGE_A);
    within(restored_a.push())
        .await
        .expect("A re-uploads recovered value after hub restore");
    assert_eq!(
        bodies(&restored, "notes"),
        std::collections::BTreeSet::from(["a-recovered".to_string()]),
        "the restored hub receives the status-recovered local update again"
    );
    within(client_b.pull_default())
        .await
        .expect("B receives A re-upload");
    edge_b
        .execute("UPDATE notes SET body = 'b-later' WHERE id = 1", &p())
        .expect("B writes later restored-history value");
    within(client_b.push())
        .await
        .expect("hub accepts B later value");
    within(restored_a.pull_default())
        .await
        .expect("A receives B later value");
    let expected = std::collections::BTreeSet::from(["b-later".to_string()]);
    for (name, db) in [
        ("restored hub", &restored),
        ("edge A", &edge_a),
        ("edge B", &edge_b),
    ] {
        assert_eq!(bodies(db, "notes"), expected, "{name} keeps B's later edit");
    }
    second_hub.stop().await;
}

// ---------------------------------------------------------------------------
// The ordinary push path is untouched
// ---------------------------------------------------------------------------

/// Regression guard. The single-edge happy path is already covered by
/// `bounded_tables_sync_tests::c3_confirmed_push_advances_the_engine_sync_watermark`
/// (direct-reply confirmation drives the deletion gate) and
/// `stale_restore_tests::sr4_guard_steady_state_push_counts_and_no_duplicates`
/// (counts, watermark, no duplicates). What is NOT covered anywhere is the
/// two-edge steady state — the exact configuration the fix changes — so this
/// pins it: with no fault at all, each edge confirms from its own direct reply,
/// each watermark tracks ITS OWN batch frontier (asserted against the frontier
/// LSN each edge is KNOWN to have written, never against a saved copy of the
/// watermark itself), and the confirmation comes from the `PushResponse` — the
/// whole exchange sequence of a successful push is an optional pre-push status
/// probe then the push, with no status round-trip wedged in to confirm it.
#[tokio::test]
async fn c3e_guard_ordinary_two_edge_push_confirms_from_the_direct_reply() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);

    // Edge A writes exactly three rows; its batch frontier is the db's own LSN
    // AFTER those writes and BEFORE the push — a value the push does not
    // produce, so asserting the watermark against it is not self-referential.
    let edge_a = open_edge();
    let client_a = edge_client(&edge_a, &broker, EDGE_A);
    insert_notes(&edge_a, 1..4);
    let a_frontier = edge_a.current_lsn();
    assert!(
        a_frontier > Lsn(0),
        "edge A really has local writes to push"
    );

    let before_a = broker.recorded_exchanges().len();
    let a_push = within(client_a.push()).await.expect("edge A push");
    assert_eq!(
        a_push.applied_rows, 3,
        "edge A's three rows are applied and confirmed by the hub's own reply: {a_push:?}"
    );
    assert_eq!(
        client_a.push_watermark(),
        a_frontier,
        "edge A's push watermark is its own confirmed batch frontier {a_frontier:?}, \
         not zero and not some shared number"
    );
    assert_eq!(
        edge_a.sync_watermark(),
        Lsn(a_frontier.0 + 1),
        "and the direct-reply confirmation drove edge A's engine deletion gate to \
         first-unconfirmed-LSN"
    );

    // The confirmation came from the PushResponse: the last exchange of the
    // push is on the push subject. A pre-push stale-restore status probe may
    // precede it, but NO status exchange may follow the push and stand in for
    // its acknowledgement.
    let a_exchanges = new_exchange_subjects(&broker, before_a);
    assert_confirms_from_direct_reply(&a_exchanges, TENANT);

    // Edge B, a genuinely different frontier, must confirm against its OWN batch.
    let edge_b = open_edge();
    let client_b = edge_client(&edge_b, &broker, EDGE_B);
    insert_notes(&edge_b, 100..105);
    let b_frontier = edge_b.current_lsn();
    assert_ne!(
        a_frontier, b_frontier,
        "fixture: the two edges must sit at different frontiers so a shared number \
         cannot satisfy both ({a_frontier:?} vs {b_frontier:?})"
    );

    let before_b = broker.recorded_exchanges().len();
    let b_push = within(client_b.push()).await.expect("edge B push");
    assert_eq!(
        b_push.applied_rows, 5,
        "edge B's five rows are applied and confirmed by its own reply: {b_push:?}"
    );
    assert_eq!(
        client_b.push_watermark(),
        b_frontier,
        "edge B's watermark is ITS OWN frontier {b_frontier:?} — never edge A's, \
         never a shared value"
    );
    let b_exchanges = new_exchange_subjects(&broker, before_b);
    assert_confirms_from_direct_reply(&b_exchanges, TENANT);

    // Edge B's push left edge A's confirmed position untouched.
    assert_eq!(
        client_a.push_watermark(),
        a_frontier,
        "edge B's push must not move edge A's position"
    );
    assert_eq!(
        bodies(&hub.db, "notes"),
        both(expect_bodies("note", 1..4), expect_bodies("note", 100..105)),
        "the hub holds the union of both edges' rows by value"
    );

    // Repeated no-op pushes stay inert on both edges, each watermark pinned to
    // its own known frontier.
    for _ in 0..2 {
        let a = within(client_a.push()).await.expect("edge A no-op push");
        let b = within(client_b.push()).await.expect("edge B no-op push");
        assert_eq!(a.applied_rows, 0, "nothing-new push applies nothing");
        assert_eq!(b.applied_rows, 0, "nothing-new push applies nothing");
    }
    assert_eq!(
        client_a.push_watermark(),
        a_frontier,
        "no watermark churn for edge A on the ordinary path"
    );
    assert_eq!(
        client_b.push_watermark(),
        b_frontier,
        "no watermark churn for edge B on the ordinary path"
    );
    assert_eq!(
        bodies(&hub.db, "notes"),
        both(expect_bodies("note", 1..4), expect_bodies("note", 100..105)),
        "repeated pushes leave each row exactly once"
    );

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// Tenant dimension — the record is per (tenant, edge), not per edge alone
// ---------------------------------------------------------------------------

/// The received-up-to record is keyed by (tenant, edge). The edge-dimension
/// tests above vary the edge on one tenant; this varies the tenant on one edge:
/// the SAME authenticated node pushes different batches into TWO tenants served
/// by ONE hub database (the multi-tenant hub shape). Each tenant's status must
/// answer with that tenant's position for the node — a record keyed by edge
/// alone (or persisted under a key that omits the tenant) collapses the two and
/// answers both with whichever tenant wrote the record last, false-confirming a
/// batch in the other tenant: the same data-loss class these tests exist to close.
///
/// This passes on today's per-tenant record and guards the fix: the older-copy
/// restore scenario already
/// forces the per-edge record to be PERSISTED (a restored older copy must
/// present a lower one), so an in-memory-per-server key is not an option — the
/// key that survives is the one this test constrains to include the tenant.
#[tokio::test]
async fn c3a_the_received_up_to_record_is_per_tenant_and_edge_not_per_edge_alone() {
    let broker = InProcessBroker::new();
    // ONE hub database serving both tenants.
    let hub_db = Arc::new(Database::open_memory());
    create_tables(&hub_db);
    let hub1 = start_hub_on_tenant(&broker, hub_db.clone(), TENANT_1);
    let hub2 = start_hub_on_tenant(&broker, hub_db.clone(), TENANT_2);

    // The same authenticated edge pushes a small batch into tenant 1 ...
    let edge_t1 = open_edge();
    let client_t1 = edge_client_on_tenant(&edge_t1, &broker, EDGE_A, TENANT_1);
    insert_notes(&edge_t1, 1..4);
    within(client_t1.push())
        .await
        .expect("edge push into tenant 1");
    let w1 = client_t1.push_watermark();

    // ... and a much larger batch into tenant 2.
    let edge_t2 = open_edge();
    let client_t2 = edge_client_on_tenant(&edge_t2, &broker, EDGE_A, TENANT_2);
    insert_notes(&edge_t2, 100..300);
    within(client_t2.push())
        .await
        .expect("edge push into tenant 2");
    let w2 = client_t2.push_watermark();

    assert!(
        w1 > Lsn(0) && w2 > Lsn(0) && w1 != w2,
        "fixture: the same edge sits at genuinely different positions in the two \
         tenants ({w1:?} vs {w2:?}), so a per-edge-only record cannot satisfy both"
    );

    let i1 = edge_t1
        .sync_incarnation(&TenantId::from(TENANT_1))
        .expect("read tenant-1 edge incarnation");
    let i2 = edge_t2
        .sync_incarnation(&TenantId::from(TENANT_2))
        .expect("read tenant-2 edge incarnation");
    let (t1_request, t1_status) = status_for(&broker, TENANT_1, EDGE_A, i1).await;
    let (t2_request, t2_status) = status_for(&broker, TENANT_2, EDGE_A, i2).await;

    // Neither request carries the asking node id: the tenant rides the subject and
    // the node identity rides the authenticated connection, so only the per-life
    // incarnation is on the wire.
    for request in [&t1_request, &t2_request] {
        assert!(
            !request
                .windows(EDGE_A.len())
                .any(|w| w == EDGE_A.as_bytes()),
            "the status request payload must not carry the asking node id ({EDGE_A})"
        );
    }
    assert_eq!(
        t1_status.applied_push_watermark,
        Some(w1),
        "tenant 1 must be told what the hub holds from this edge IN TENANT 1 \
         ({w1:?}); a record keyed by edge alone answers with tenant 2's {w2:?}"
    );
    assert_eq!(
        t2_status.applied_push_watermark,
        Some(w2),
        "tenant 2 must be told its own position for the same edge ({w2:?})"
    );
    assert_ne!(
        t1_status.applied_push_watermark, t2_status.applied_push_watermark,
        "one authenticated edge at two positions across two tenants must not receive \
         the same answer to the same question"
    );

    hub1.stop().await;
    hub2.stop().await;
}

// ---------------------------------------------------------------------------
// A wiped-and-recreated edge that reuses its identity is not
// false-confirmed by the hub's stale per-edge watermark
// ---------------------------------------------------------------------------

/// A `SYNC SAFE SYNC PUSH ONLY` retained table, so the deletion gate governs
/// whether the edge may delete its own undelivered rows. Push-only, because that
/// is the exact shape the reincarnation defect names.
const FIXB_DDL: &str = "CREATE TABLE fixb_windows (id INTEGER PRIMARY KEY, body TEXT) \
     RETAIN 1 HOURS SYNC SAFE SYNC PUSH ONLY";

fn insert_fixb(db: &Database, ids: std::ops::Range<i64>) {
    for id in ids {
        let mut row = p();
        row.insert("id".to_string(), Value::Int64(id));
        row.insert("body".to_string(), Value::Text(format!("fixb-{id}")));
        db.execute(
            "INSERT INTO fixb_windows (id, body) VALUES ($id, $body)",
            &row,
        )
        .expect("fixb insert");
    }
}

fn fixb_bodies(db: &Database) -> std::collections::BTreeSet<String> {
    bodies(db, "fixb_windows")
}

/// The wipe-and-recreate recovery path. The hub records a high per-edge
/// watermark for an edge. That edge's database is deleted and recreated against
/// the SAME node identity (the transport identity is a sibling key-file that
/// survives the wipe), so its local LSN counter is back at zero. The recreated edge writes
/// a new row at LSN 1 and its push acknowledgement is lost. Pre-fix,
/// `finish_interrupted_push` reads the hub's stale per-edge watermark, sees it
/// already past the tiny new batch, false-confirms, opens the deletion gate, and
/// the SYNC SAFE row is pruned before the hub ever holds it — data loss on the
/// SYNC SAFE promise. The confirmation must instead reject a watermark that
/// exceeds anything THIS incarnation produced.
#[tokio::test]
async fn fixb_a_reincarnated_edge_is_not_false_confirmed_by_the_stale_per_edge_watermark() {
    let (clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();

    // An empty hub that will learn the push-only table from the first push.
    let hub_db = Arc::new(Database::open_memory());
    let hub = start_hub_on(&broker, hub_db.clone());

    // The ORIGINAL incarnation delivers several rows, so the hub records a high
    // per-edge watermark for EDGE_A.
    let original = Arc::new(Database::open_memory());
    original.execute(FIXB_DDL, &p()).expect("original table");
    insert_fixb(&original, 1..6);
    let original_client = edge_client(&original, &broker, EDGE_A);
    within(original_client.push()).await.expect("original push");
    let recorded = hub_db
        .persisted_sync_applied_push_watermark_for_node(&TenantId::from(TENANT), EDGE_A)
        .expect("read per-edge watermark")
        .expect("the hub recorded a watermark for EDGE_A");
    assert!(
        recorded > Lsn(1),
        "fixture: the hub holds a high watermark for EDGE_A: {recorded:?}"
    );

    // The RECREATED incarnation: a brand-new database (LSN reset to zero) reusing
    // EDGE_A's identity, writing one fresh row.
    let reborn = Arc::new(Database::open_memory());
    reborn.execute(FIXB_DDL, &p()).expect("reborn table");
    insert_fixb(&reborn, 99..100);
    assert!(
        reborn.current_lsn() < recorded,
        "fixture: the reborn edge's own LSN ({:?}) is below the hub's stale watermark ({recorded:?}) \
         — that is what makes the stale watermark a false confirmation",
        reborn.current_lsn(),
    );
    assert_eq!(
        reborn.sync_watermark(),
        Lsn(0),
        "the reborn edge's SYNC SAFE deletion gate starts fully closed",
    );

    // Its push acknowledgement is lost on the way out.
    let unlucky = dropped_push_client(&reborn, &broker, EDGE_A);
    let outcome = within(unlucky.push()).await;

    assert!(
        matches!(
            outcome,
            Err(contextdb_core::Error::SyncPushUnconfirmed { .. })
        ),
        "the hub's stale per-edge watermark must not confirm a batch this incarnation's \
         own LSN cannot account for; the outcome is the distinct unconfirmed one, got {outcome:?}",
    );
    assert_eq!(
        reborn.sync_watermark(),
        Lsn(0),
        "and the SYNC SAFE deletion gate must not have opened at all",
    );

    // Age the fresh row past its window and prune: it must survive, because the
    // hub never actually received it.
    clock.advance(48 * 60 * 60 * 1000);
    let report = reborn
        .run_pruning_cycle_checked()
        .expect("a prune cycle still runs");
    assert_eq!(
        report.pruned_rows, 0,
        "no SYNC SAFE row may be deleted while the hub does not hold it: {report:?}",
    );
    assert_eq!(
        fixb_bodies(&reborn),
        std::iter::once("fixb-99".to_string()).collect(),
        "the reborn edge's row is still present by value",
    );

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// The confirmation ceiling is what THIS incarnation could TRANSMIT,
// not the whole-database committed LSN
// ---------------------------------------------------------------------------

/// A table that never delivers outbound: writes to it advance the database's
/// committed LSN but are never transmitted to any hub. It exists to inflate
/// `current_lsn` past the hub's stale per-edge watermark WITHOUT raising what
/// this edge could actually have pushed.
const LOCAL_ONLY_DDL: &str =
    "CREATE TABLE local_scratch (id INTEGER PRIMARY KEY, body TEXT) SYNC OFF";

fn insert_scratch(db: &Database, ids: std::ops::Range<i64>) {
    for id in ids {
        let mut row = p();
        row.insert("id".to_string(), Value::Int64(id));
        row.insert("body".to_string(), Value::Text(format!("scratch-{id}")));
        db.execute(
            "INSERT INTO local_scratch (id, body) VALUES ($id, $body)",
            &row,
        )
        .expect("scratch insert");
    }
}

fn expect_fixb(ids: std::ops::Range<i64>) -> std::collections::BTreeSet<String> {
    ids.map(|id| format!("fixb-{id}")).collect()
}

/// A transport that FORWARDS the push batch to the real hub (so it applies and
/// commits it) and then loses the acknowledgement on the way back — the
/// hub-committed-then-died-before-ack shape. Every other subject forwards
/// untouched so the edge can still reach the hub to reconcile.
struct ForwardPushThenDropAck {
    inner: Arc<dyn ClientTransport>,
    push_subject: String,
    /// Test-only fault: the recovery pull after status confirmation fails once,
    /// leaving the durable confirmation marker for the next ordinary push.
    drop_first_pull: Option<Arc<AtomicBool>>,
}

impl ClientTransport for ForwardPushThenDropAck {
    fn peer_node_id(&self) -> Option<String> {
        self.inner.peer_node_id()
    }

    fn local_node_id(&self) -> Option<String> {
        self.inner.local_node_id()
    }

    fn has_stable_edge_identity(&self) -> bool {
        self.inner.has_stable_edge_identity()
    }

    fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.ensure_connected()
    }

    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        self.inner.is_connected()
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        if subject == pull_subject(TENANT)
            && self
                .drop_first_pull
                .as_ref()
                .is_some_and(|drop| drop.swap(false, Ordering::SeqCst))
        {
            return Box::pin(async {
                Err(TransportError::Unreachable(
                    "first status-confirmed recovery pull lost on the wire".to_string(),
                ))
            });
        }
        self.inner.request(subject, request_bytes, timeout)
    }

    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        if subject == pull_subject(TENANT)
            && self
                .drop_first_pull
                .as_ref()
                .is_some_and(|drop| drop.swap(false, Ordering::SeqCst))
        {
            return Box::pin(async {
                Err(TransportError::Unreachable(
                    "first status-confirmed recovery pull lost on the wire".to_string(),
                ))
            });
        }
        if subject == self.push_subject {
            let inner = self.inner.clone();
            let subject = subject.to_string();
            return Box::pin(async move {
                // Let the hub apply + commit the batch first ...
                let _ = inner
                    .request_single_reply(&subject, request_bytes, timeout)
                    .await;
                // ... then lose the acknowledgement on the way back.
                Err(TransportError::Unreachable(
                    "acknowledgement lost after the hub committed the batch".to_string(),
                ))
            });
        }
        self.inner
            .request_single_reply(subject, request_bytes, timeout)
    }

    fn ensure_single_reply_retry_safe(&self, request_bytes: &[u8]) -> TransportResult<()> {
        self.inner.ensure_single_reply_retry_safe(request_bytes)
    }

    fn shutdown<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.shutdown()
    }
}

fn landed_ack_lost_client(
    db: &Arc<Database>,
    broker: &InProcessBroker,
    node_id: &str,
) -> SyncClient {
    let identity = identity_for_node(node_id);
    SyncClient::with_authenticated_transport_and_identity_for_test(
        db.clone(),
        Arc::new(ForwardPushThenDropAck {
            inner: broker.client_as(node_id),
            push_subject: push_subject(TENANT),
            drop_first_pull: None,
        }),
        TenantId::from(TENANT),
        identity,
    )
}

fn landed_ack_lost_with_recovery_pull_dropped_client(
    db: &Arc<Database>,
    broker: &InProcessBroker,
    node_id: &str,
) -> SyncClient {
    let identity = identity_for_node(node_id);
    SyncClient::with_authenticated_transport_and_identity_for_test(
        db.clone(),
        Arc::new(ForwardPushThenDropAck {
            inner: broker.client_as(node_id),
            push_subject: push_subject(TENANT),
            drop_first_pull: Some(Arc::new(AtomicBool::new(true))),
        }),
        TenantId::from(TENANT),
        identity,
    )
}

/// A reincarnated edge (fresh DB, reused identity, LSN counter reset to zero)
/// writes a small SYNC SAFE batch, then does NON-DELIVERING (`SYNC OFF`) work
/// that pushes its committed LSN PAST the hub's stale per-edge watermark. The
/// old lost-ack reconciliation ceiling was the whole-database `current_lsn`,
/// which the sync-off work inflated, so the stale watermark satisfied
/// `stale >= batch_max_lsn && stale <= current_lsn` and false-confirmed a batch
/// the hub never received — opening the SYNC SAFE deletion gate on rows that
/// exist nowhere else. The ceiling must instead be the greatest LSN this
/// incarnation could have TRANSMITTED, which sync-off work cannot raise.
#[tokio::test]
async fn fix1_reincarnated_edge_is_not_false_confirmed_when_local_only_work_inflates_current_lsn() {
    let (clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();

    let hub_db = Arc::new(Database::open_memory());
    let hub = start_hub_on(&broker, hub_db.clone());

    // ORIGINAL incarnation pushes MANY rows, so the hub records a HIGH per-edge
    // watermark for EDGE_A.
    let original = Arc::new(Database::open_memory());
    original.execute(FIXB_DDL, &p()).expect("original table");
    insert_fixb(&original, 1..60);
    within(edge_client(&original, &broker, EDGE_A).push())
        .await
        .expect("original push");
    let recorded = hub_db
        .persisted_sync_applied_push_watermark_for_node(&TenantId::from(TENANT), EDGE_A)
        .expect("read per-edge watermark")
        .expect("the hub recorded a watermark for EDGE_A");
    assert!(
        recorded > Lsn(5),
        "fixture: the hub holds a high watermark for EDGE_A: {recorded:?}"
    );

    // RECREATED incarnation: fresh DB reusing EDGE_A. A SMALL SYNC SAFE batch,
    // below the stale watermark.
    let reborn = Arc::new(Database::open_memory());
    reborn.execute(FIXB_DDL, &p()).expect("reborn table");
    reborn
        .execute(LOCAL_ONLY_DDL, &p())
        .expect("reborn scratch table");
    insert_fixb(&reborn, 90..95);
    let safe_batch_frontier = reborn.current_lsn();
    assert!(
        safe_batch_frontier < recorded,
        "fixture: the SYNC SAFE batch sits below the stale watermark \
         ({safe_batch_frontier:?} vs {recorded:?})"
    );

    // NON-DELIVERING work that inflates the committed LSN PAST the stale
    // watermark — the exact thing `current_lsn` overstates.
    insert_scratch(&reborn, 1..300);
    assert!(
        reborn.current_lsn() > recorded,
        "fixture: local-only work inflated current_lsn past the stale watermark \
         ({:?} vs {recorded:?})",
        reborn.current_lsn()
    );
    assert_eq!(
        reborn.sync_watermark(),
        Lsn(0),
        "the reborn edge's SYNC SAFE deletion gate starts fully closed"
    );

    // The SYNC SAFE push's acknowledgement is lost on the way out.
    let outcome = within(dropped_push_client(&reborn, &broker, EDGE_A).push()).await;

    assert!(
        matches!(
            outcome,
            Err(contextdb_core::Error::SyncPushUnconfirmed { .. })
        ),
        "the stale per-edge watermark — reachable only because SYNC OFF work \
         inflated current_lsn — must not confirm the SYNC SAFE batch; got {outcome:?}"
    );
    assert_eq!(
        reborn.sync_watermark(),
        Lsn(0),
        "the SYNC SAFE deletion gate must not have opened"
    );

    // Age the SYNC SAFE rows past their window and prune: they survive, because
    // the hub never received them.
    clock.advance(48 * 60 * 60 * 1000);
    let report = reborn
        .run_pruning_cycle_checked()
        .expect("a prune cycle still runs");
    assert_eq!(
        report.pruned_rows, 0,
        "no SYNC SAFE row may be deleted while the hub does not hold it: {report:?}"
    );
    assert_eq!(
        fixb_bodies(&reborn),
        expect_fixb(90..95),
        "the reborn edge's SYNC SAFE rows survive by value"
    );

    hub.stop().await;
}

/// The reincarnation defect with the startup-distrust guard NEUTRALISED: the
/// reborn edge first makes ONE ordinary, directly-confirmed push (so its push
/// watermark is non-zero and the fresh-incarnation distrust no longer applies).
/// Only the transmittable-LSN ceiling then stands between the hub's stale
/// per-edge watermark and a false confirmation — this isolates that ceiling.
#[tokio::test]
async fn fix1_transmittable_ceiling_blocks_false_confirm_after_a_direct_push() {
    let (clock, _guard) = MockClock::install(T0);
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let broker = InProcessBroker::new();

    let hub_db = Arc::new(Database::open_memory());
    let hub = start_hub_on(&broker, hub_db.clone());

    let original = Arc::new(Database::open_memory());
    original.execute(FIXB_DDL, &p()).expect("original table");
    insert_fixb(&original, 1..60);
    within(edge_client(&original, &broker, EDGE_A).push())
        .await
        .expect("original push");
    let recorded = hub_db
        .persisted_sync_applied_push_watermark_for_node(&TenantId::from(TENANT), EDGE_A)
        .expect("read per-edge watermark")
        .expect("the hub recorded a watermark for EDGE_A");

    // File-backed so the warmup client's confirmed push watermark PERSISTS and a
    // later client on the same edge reloads a non-zero position.
    let reborn = Arc::new(Database::open(tmp.path().join("reborn.db")).expect("open reborn"));
    reborn.execute(FIXB_DDL, &p()).expect("reborn table");
    reborn
        .execute(LOCAL_ONLY_DDL, &p())
        .expect("reborn scratch table");

    // ONE directly-confirmed push re-establishes a non-zero push watermark.
    insert_fixb(&reborn, 90..91);
    let warmup = edge_client(&reborn, &broker, EDGE_A);
    within(warmup.push()).await.expect("reborn warmup push");
    assert!(
        warmup.push_watermark() > Lsn(0),
        "the reborn edge has a directly-confirmed push, so the fresh-incarnation \
         distrust guard no longer applies"
    );
    let gate_after_warmup = reborn.sync_watermark();

    // A further SYNC SAFE batch (still below the stale watermark) plus
    // non-delivering inflation.
    insert_fixb(&reborn, 91..96);
    let safe_frontier = reborn.current_lsn();
    assert!(
        safe_frontier < recorded,
        "fixture: the SYNC SAFE batch sits below the stale watermark \
         ({safe_frontier:?} vs {recorded:?})"
    );
    insert_scratch(&reborn, 1..300);
    assert!(
        reborn.current_lsn() > recorded,
        "fixture: local-only work inflated current_lsn past the stale watermark"
    );

    let dropped = dropped_push_client(&reborn, &broker, EDGE_A);
    assert!(
        dropped.push_watermark() > Lsn(0),
        "fixture: the dropped-ack client inherits the warmup's confirmed position"
    );
    let outcome = within(dropped.push()).await;

    assert!(
        matches!(
            outcome,
            Err(contextdb_core::Error::SyncPushUnconfirmed { .. })
        ),
        "with distrust neutralised, only the transmittable ceiling blocks the \
         stale watermark; current_lsn would false-confirm; got {outcome:?}"
    );
    assert_eq!(
        reborn.sync_watermark(),
        gate_after_warmup,
        "the SYNC SAFE deletion gate must not advance past the warmup confirmation"
    );

    clock.advance(48 * 60 * 60 * 1000);
    let report = reborn
        .run_pruning_cycle_checked()
        .expect("a prune cycle still runs");
    assert!(
        fixb_bodies(&reborn).is_superset(&expect_fixb(91..96)),
        "the undelivered SYNC SAFE batch survives by value: {report:?}"
    );

    hub.stop().await;
}

/// The no-false-negative guard: an ORDINARY push whose batch reached the hub and
/// committed, but whose acknowledgement was lost, must still reconcile to
/// success. The fix must reject a stale cross-incarnation watermark WITHOUT
/// rejecting a genuine landed batch.
#[tokio::test]
async fn fix1_ordinary_landed_but_ack_lost_push_still_confirms() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);

    let edge = open_edge();
    insert_notes(&edge, 1..4);
    let frontier = edge.current_lsn();

    let client = landed_ack_lost_client(&edge, &broker, EDGE_A);
    let outcome = within(client.push()).await;

    assert_eq!(
        bodies(&hub.db, "notes"),
        expect_bodies("note", 1..4),
        "the batch reached the hub and committed"
    );
    assert!(
        outcome.is_ok(),
        "a landed-but-ack-lost push must reconcile to success, got {outcome:?}"
    );
    assert_eq!(
        client.push_watermark(),
        frontier,
        "the confirmed push advances the edge watermark to its own frontier"
    );

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// A concurrent direction change between send and reconcile must not
// wrongly reject a landed push (the ceiling is frozen from the transmitted set)
// ---------------------------------------------------------------------------

const RELAY_PUSH_ONLY_DDL: &str =
    "CREATE TABLE relay_notes (id INTEGER PRIMARY KEY, body TEXT) SYNC PUSH ONLY";

fn insert_relay(db: &Database, ids: std::ops::Range<i64>) {
    for id in ids {
        let mut row = p();
        row.insert("id".to_string(), Value::Int64(id));
        row.insert("body".to_string(), Value::Text(format!("relay-{id}")));
        db.execute(
            "INSERT INTO relay_notes (id, body) VALUES ($id, $body)",
            &row,
        )
        .expect("relay insert");
    }
}

fn expect_relay(ids: std::ops::Range<i64>) -> std::collections::BTreeSet<String> {
    ids.map(|id| format!("relay-{id}")).collect()
}

/// Forwards the push to the hub (which applies and commits it), then — before the
/// acknowledgement returns — flips the edge's delivering table to `SYNC OFF`, then
/// loses the acknowledgement. This is the concurrent-direction-change scenario:
/// if the lost-ack ceiling is RECOMPUTED after the await, the now
/// non-delivering table is filtered out, the ceiling collapses below what actually
/// shipped, and a batch the hub genuinely holds is wrongly reported unconfirmed.
struct ForwardPushThenAlterThenDropAck {
    inner: Arc<dyn ClientTransport>,
    edge_db: Arc<Database>,
    alter_sql: String,
    push_subject: String,
    altered: AtomicBool,
}

impl ClientTransport for ForwardPushThenAlterThenDropAck {
    fn peer_node_id(&self) -> Option<String> {
        self.inner.peer_node_id()
    }

    fn local_node_id(&self) -> Option<String> {
        self.inner.local_node_id()
    }

    fn has_stable_edge_identity(&self) -> bool {
        self.inner.has_stable_edge_identity()
    }

    fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.ensure_connected()
    }

    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        self.inner.is_connected()
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        self.inner.request(subject, request_bytes, timeout)
    }

    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        if subject == self.push_subject {
            let inner = self.inner.clone();
            let subject = subject.to_string();
            let edge_db = self.edge_db.clone();
            let alter_sql = self.alter_sql.clone();
            let already = self.altered.swap(true, Ordering::SeqCst);
            return Box::pin(async move {
                // Let the hub apply + commit the batch first ...
                let _ = inner
                    .request_single_reply(&subject, request_bytes, timeout)
                    .await;
                // ... then flip the delivering table to SYNC OFF, concurrently
                // with the still-in-flight push, exactly once ...
                if !already {
                    edge_db
                        .execute(&alter_sql, &HashMap::new())
                        .expect("concurrent direction change");
                }
                // ... then lose the acknowledgement on the way back.
                Err(TransportError::Unreachable(
                    "acknowledgement lost after the hub committed the batch".to_string(),
                ))
            });
        }
        self.inner
            .request_single_reply(subject, request_bytes, timeout)
    }

    fn ensure_single_reply_retry_safe(&self, request_bytes: &[u8]) -> TransportResult<()> {
        self.inner.ensure_single_reply_retry_safe(request_bytes)
    }

    fn shutdown<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.shutdown()
    }
}

/// An ordinary, non-reincarnated edge pushes a delivering batch that the hub
/// applies and commits; the acknowledgement is lost AND the table is switched to
/// `SYNC OFF` between the send and the lost-ack reconciliation. The reconciliation
/// must still confirm the landed batch: its ceiling has to be the set that was
/// actually transmitted (captured before the send), not a value recomputed from
/// the table's post-change direction.
#[tokio::test]
async fn fixa_concurrent_direction_change_does_not_reject_a_landed_push() {
    let broker = InProcessBroker::new();
    // Empty hub; it learns the delivering table from the push itself.
    let hub_db = Arc::new(Database::open_memory());
    let hub = start_hub_on(&broker, hub_db.clone());

    let edge = Arc::new(Database::open_memory());
    edge.execute(RELAY_PUSH_ONLY_DDL, &p())
        .expect("relay table");
    // Rows created AFTER the table so the batch's max LSN is a row LSN, matching
    // the hub's per-edge applied watermark exactly.
    insert_relay(&edge, 1..4);
    let frontier = edge.current_lsn();
    assert!(frontier > Lsn(0), "the edge has delivering rows to push");

    let identity = identity_for_node(EDGE_A);
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        Arc::new(ForwardPushThenAlterThenDropAck {
            inner: broker.client_as(EDGE_A),
            edge_db: edge.clone(),
            alter_sql: "ALTER TABLE relay_notes SET SYNC OFF".to_string(),
            push_subject: push_subject(TENANT),
            altered: AtomicBool::new(false),
        }),
        TenantId::from(TENANT),
        identity,
    );
    let outcome = within(client.push()).await;

    assert_eq!(
        bodies(&hub_db, "relay_notes"),
        expect_relay(1..4),
        "the batch reached the hub and committed"
    );
    assert!(
        outcome.is_ok(),
        "the batch landed and was committed on the hub; a direction change \
         racing the lost acknowledgement must not turn a genuine landing into an \
         unconfirmed push, got {outcome:?}"
    );
    assert_eq!(
        client.push_watermark(),
        frontier,
        "the confirmed push advances the edge watermark to its own transmitted \
         frontier"
    );

    hub.stop().await;
}

// ===========================================================================
// Wire-incarnation reincarnation guarantees (G1, G2, G3, G5, G6)
//
// The reincarnation defect: an edge that WIPES its local database but REUSES its
// transport identity (a sibling key-file that survives the wipe) presents the
// same node id, so the hub still answers with the PRIOR incarnation's high
// per-edge watermark while the rebuilt edge's own LSNs are back near zero. The
// pre-fix code mitigates this WITHOUT a wire change — a pre-push status probe
// snapshots the stale value and arms a persisted "distrust" — but two holes
// remained in that mitigation. These tests pin the behavioral guarantees; G2
// and G3 reproduce the two holes and therefore FAIL before this change (a false
// confirmation prunes SYNC SAFE rows the hub never held).
//
// G4: hub-keys-by-incarnation — lands with the implementation. The assertion
// that the hub keys its per-edge watermark by (node_id, incarnation) — a status
// probe carrying a NEW incarnation returns Lsn(0) even though the OLD
// incarnation still holds a high watermark, while a probe carrying the OLD
// incarnation still returns the old value — needs the not-yet-existing
// incarnation API, so it is added here alongside the wire change.
// ===========================================================================

/// A transport for the probe-failure reincarnation scenario (G3). It makes the
/// edge's PRE-PUSH status probe fail — the FIRST status exchange returns a
/// transport error, which `fetch_sync_status` degrades to `None` — while
/// dropping the push on the way OUT so the hub never stores the batch, and
/// FORWARDING every LATER status exchange so the lost-ack reconciliation still
/// reads the hub's stale per-edge watermark. A pre-push probe that returns None
/// is exactly the transient miss that leaves the no-wire distrust unarmed: the
/// tip arms distrust only when the probe returns `Some`.
struct ProbeFailsThenPushDropped {
    inner: Arc<dyn ClientTransport>,
    push_subject: String,
    status_subject: String,
    status_calls: AtomicU64,
}

impl ClientTransport for ProbeFailsThenPushDropped {
    fn peer_node_id(&self) -> Option<String> {
        self.inner.peer_node_id()
    }

    fn local_node_id(&self) -> Option<String> {
        self.inner.local_node_id()
    }

    fn has_stable_edge_identity(&self) -> bool {
        self.inner.has_stable_edge_identity()
    }

    fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.ensure_connected()
    }

    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        self.inner.is_connected()
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        if subject == self.push_subject {
            return Box::pin(async {
                Err(TransportError::Unreachable(
                    "push request dropped on the wire before it reached the hub".to_string(),
                ))
            });
        }
        self.inner.request(subject, request_bytes, timeout)
    }

    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        if subject == self.push_subject {
            return Box::pin(async {
                Err(TransportError::Unreachable(
                    "push request dropped on the wire before it reached the hub".to_string(),
                ))
            });
        }
        if subject == self.status_subject && self.status_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            // The pre-push probe transiently fails; `fetch_sync_status` maps every
            // failure mode to `None`, so distrust is never armed. Later status
            // exchanges (the lost-ack reconciliation's own probe) are forwarded.
            return Box::pin(async {
                Err(TransportError::Unreachable(
                    "pre-push status probe transiently failed".to_string(),
                ))
            });
        }
        self.inner
            .request_single_reply(subject, request_bytes, timeout)
    }

    fn ensure_single_reply_retry_safe(&self, request_bytes: &[u8]) -> TransportResult<()> {
        self.inner.ensure_single_reply_retry_safe(request_bytes)
    }

    fn shutdown<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.shutdown()
    }
}

fn probe_failing_dropped_push_client(
    db: &Arc<Database>,
    broker: &InProcessBroker,
    node_id: &str,
) -> SyncClient {
    let identity = identity_for_node(node_id);
    SyncClient::with_authenticated_transport_and_identity_for_test(
        db.clone(),
        Arc::new(ProbeFailsThenPushDropped {
            inner: broker.client_as(node_id),
            push_subject: push_subject(TENANT),
            status_subject: status_subject(TENANT),
            status_calls: AtomicU64::new(0),
        }),
        TenantId::from(TENANT),
        identity,
    )
}

/// A transport for the in-memory reincarnation scenario (G2). The test arms
/// dropping only AFTER the complete warmup `push()` returns. Exact receipt
/// stamping legitimately sends more than one PushRequest when DDL and row
/// commits have distinct source LSNs, so counting requests would drop part of
/// the warmup instead of the subsequent backlog. Status exchanges always
/// forward.
struct WarmupConfirmsThenPushDropped {
    inner: Arc<dyn ClientTransport>,
    push_subject: String,
    drop_after_warmup: Arc<AtomicBool>,
}

impl ClientTransport for WarmupConfirmsThenPushDropped {
    fn peer_node_id(&self) -> Option<String> {
        self.inner.peer_node_id()
    }

    fn local_node_id(&self) -> Option<String> {
        self.inner.local_node_id()
    }

    fn has_stable_edge_identity(&self) -> bool {
        self.inner.has_stable_edge_identity()
    }

    fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.ensure_connected()
    }

    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        self.inner.is_connected()
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        self.inner.request(subject, request_bytes, timeout)
    }

    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        if subject == self.push_subject && self.drop_after_warmup.load(Ordering::SeqCst) {
            // Every backlog batch is lost on the way out, so the hub never
            // stores it. The test arms this only after warmup completion.
            return Box::pin(async {
                Err(TransportError::Unreachable(
                    "push request dropped on the wire before it reached the hub".to_string(),
                ))
            });
        }
        self.inner
            .request_single_reply(subject, request_bytes, timeout)
    }

    fn ensure_single_reply_retry_safe(&self, request_bytes: &[u8]) -> TransportResult<()> {
        self.inner.ensure_single_reply_retry_safe(request_bytes)
    }

    fn shutdown<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.shutdown()
    }
}

fn warmup_then_dropped_push_client(
    db: &Arc<Database>,
    broker: &InProcessBroker,
    node_id: &str,
) -> (SyncClient, Arc<AtomicBool>) {
    let drop_after_warmup = Arc::new(AtomicBool::new(false));
    let identity = identity_for_node(node_id);
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        db.clone(),
        Arc::new(WarmupConfirmsThenPushDropped {
            inner: broker.client_as(node_id),
            push_subject: push_subject(TENANT),
            drop_after_warmup: drop_after_warmup.clone(),
        }),
        TenantId::from(TENANT),
        identity,
    );
    (client, drop_after_warmup)
}

/// G3 — the reincarnation false confirmation must be refused EVEN IF the pre-push
/// status probe fails. The hub holds a high per-edge watermark for EDGE_A from
/// the original incarnation. A wiped-and-recreated edge (fresh file-backed DB,
/// reused identity) pushes a delivering backlog that splits at the 100-LSN-group
/// boundary — its first batch below the stale watermark, its tail above it, so
/// the transmittable-ceiling guard cannot block the confirmation. The pre-push
/// probe fails, so the pre-fix code never arms its distrust; the push is dropped
/// on the way out; and the lost-ack reconciliation's own (forwarded) probe reads
/// the stale watermark and — in the pre-fix code — false-confirms the first batch,
/// opening the SYNC SAFE deletion gate on rows the hub never received.
///
/// Before this change: the confirmation must be refused (SyncPushUnconfirmed) and
/// the rows must survive; the pre-fix code instead returns Ok and prunes them.
#[tokio::test]
async fn g3_probe_failure_reincarnation_is_not_false_confirmed() {
    let (clock, _guard) = MockClock::install(T0);
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let broker = InProcessBroker::new();

    let hub_db = Arc::new(Database::open_memory());
    let hub = start_hub_on(&broker, hub_db.clone());

    // ORIGINAL incarnation delivers enough rows that the hub's per-edge watermark
    // for EDGE_A sits ABOVE a first batch's ~100-LSN-group ceiling.
    let original = Arc::new(Database::open_memory());
    original.execute(FIXB_DDL, &p()).expect("original table");
    insert_fixb(&original, 1..150);
    within(edge_client(&original, &broker, EDGE_A).push())
        .await
        .expect("original push");
    let recorded = hub_db
        .persisted_sync_applied_push_watermark_for_node(&TenantId::from(TENANT), EDGE_A)
        .expect("read per-edge watermark")
        .expect("the hub recorded a watermark for EDGE_A");
    assert!(
        recorded > Lsn(110),
        "fixture: the stale watermark must sit above a first batch's ~100-LSN \
         ceiling so that first batch lands below it: {recorded:?}"
    );

    // RECREATED incarnation: a fresh file-backed database reusing EDGE_A's
    // identity, so BOTH its local push position and its persisted
    // stale-incarnation marker are zero.
    let reborn = Arc::new(Database::open(tmp.path().join("reborn.db")).expect("open reborn"));
    reborn.execute(FIXB_DDL, &p()).expect("reborn table");
    // A DELIVERING backlog that splits at the 100-group boundary: the first
    // batch's max LSN lands BELOW the stale watermark, the whole backlog's max
    // (the transmitted ceiling) crosses ABOVE it — so the ceiling guard cannot be
    // what blocks the false confirmation; only distrust could, and the failed
    // pre-push probe leaves it unarmed.
    insert_fixb(&reborn, 600..830);
    assert!(
        reborn.current_lsn() > recorded,
        "fixture: the backlog's tail crosses above the stale watermark ({:?} vs \
         {recorded:?})",
        reborn.current_lsn()
    );
    assert_eq!(
        reborn.sync_watermark(),
        Lsn(0),
        "the reborn edge's SYNC SAFE deletion gate starts fully closed"
    );

    // The pre-push status probe fails (→ None) AND the push is dropped on the way
    // out; the hub never stores the batch. The lost-ack reconciliation's own
    // status probe is forwarded and reads the stale per-edge watermark.
    let unlucky = probe_failing_dropped_push_client(&reborn, &broker, EDGE_A);
    let outcome = within(unlucky.push()).await;

    assert!(
        matches!(
            outcome,
            Err(contextdb_core::Error::SyncPushUnconfirmed { .. })
        ),
        "with the pre-push probe failed the no-wire distrust is \
         never armed, so the hub's stale per-edge watermark false-confirms a batch \
         it never received; the outcome must be the distinct unconfirmed one, got \
         {outcome:?}"
    );
    assert_eq!(
        reborn.sync_watermark(),
        Lsn(0),
        "the SYNC SAFE deletion gate must not have opened at all"
    );

    // Age the undelivered backlog past its window and prune: it must survive,
    // because the hub never received it.
    clock.advance(48 * 60 * 60 * 1000);
    let report = reborn
        .run_pruning_cycle_checked()
        .expect("a prune cycle still runs");
    assert_eq!(
        report.pruned_rows, 0,
        "no SYNC SAFE row may be deleted while the hub does not \
         hold it: {report:?}"
    );
    assert_eq!(
        fixb_bodies(&reborn),
        expect_fixb(600..830),
        "the reborn edge's undelivered SYNC SAFE backlog must survive by value"
    );

    hub.stop().await;
}

/// G2 — the reincarnation false confirmation must be refused on an IN-MEMORY
/// edge, where the persisted stale-incarnation marker is inert. One long-lived
/// client on a reborn `open_memory` database first makes a directly-confirmed
/// warmup push, which raises its LOCAL push position off zero and so clears the
/// tip's per-push boolean distrust for every later push. The persisted marker
/// that is supposed to keep distrust alive across that warmup never persists on
/// an in-memory DB (persistence is None), so the pre-fix code is left with NO
/// distrust. The client then pushes a delivering backlog that splits at the
/// 100-group boundary (first batch below the stale watermark, tail above it) whose
/// first batch is dropped on the way out; the lost-ack reconciliation's forwarded
/// probe reads the stale watermark and — in the pre-fix code — false-confirms it.
///
/// Before this change: the confirmation must be refused and the SYNC SAFE deletion
/// gate must not advance past the warmup; the pre-fix code instead returns Ok and
/// prunes undelivered rows.
#[tokio::test]
async fn g2_in_memory_reincarnation_is_not_false_confirmed_across_a_warmup() {
    let (clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();

    let hub_db = Arc::new(Database::open_memory());
    let hub = start_hub_on(&broker, hub_db.clone());

    // ORIGINAL incarnation records a high per-edge watermark for EDGE_A.
    let original = Arc::new(Database::open_memory());
    original.execute(FIXB_DDL, &p()).expect("original table");
    insert_fixb(&original, 1..150);
    within(edge_client(&original, &broker, EDGE_A).push())
        .await
        .expect("original push");
    let recorded = hub_db
        .persisted_sync_applied_push_watermark_for_node(&TenantId::from(TENANT), EDGE_A)
        .expect("read per-edge watermark")
        .expect("the hub recorded a watermark for EDGE_A");
    assert!(
        recorded > Lsn(110),
        "fixture: the stale watermark must sit above a first batch's ~100-LSN \
         ceiling: {recorded:?}"
    );

    // RECREATED incarnation: an in-memory database reusing EDGE_A, driven by ONE
    // long-lived client so its in-memory local push position carries across the
    // two pushes (a fresh client would reload zero and re-arm the boolean).
    let reborn = Arc::new(Database::open_memory());
    reborn.execute(FIXB_DDL, &p()).expect("reborn table");
    let (client, drop_after_warmup) = warmup_then_dropped_push_client(&reborn, &broker, EDGE_A);

    // The warmup: one directly-confirmed push whose single row sits BELOW the
    // stale watermark. It raises the local position off zero (clearing the
    // boolean distrust) but does not move the hub's monotonic per-edge watermark.
    insert_fixb(&reborn, 500..501);
    within(client.push()).await.expect("warmup push confirmed");
    assert!(
        client.push_watermark() > Lsn(0) && client.push_watermark() < recorded,
        "fixture: the warmup confirmed a position below the stale watermark \
         ({:?} vs {recorded:?})",
        client.push_watermark()
    );
    let gate_after_warmup = reborn.sync_watermark();
    drop_after_warmup.store(true, Ordering::SeqCst);

    // A DELIVERING backlog that splits at the 100-group boundary: its first
    // batch's max LSN sits BELOW the stale watermark, its tail ABOVE it, so the
    // transmittable-ceiling guard cannot block the confirmation.
    insert_fixb(&reborn, 600..830);
    assert!(
        reborn.current_lsn() > recorded,
        "fixture: the backlog's tail crosses above the stale watermark ({:?} vs \
         {recorded:?})",
        reborn.current_lsn()
    );

    // The backlog's first batch is dropped on the way out; the hub never stores
    // it. Its acknowledgement is therefore lost.
    let outcome = within(client.push()).await;

    assert!(
        matches!(
            outcome,
            Err(contextdb_core::Error::SyncPushUnconfirmed { .. })
        ),
        "on an in-memory edge the persisted stale marker is inert, \
         so the warmup leaves the pre-fix code with no distrust and the hub's stale \
         per-edge watermark false-confirms a batch it never received; the outcome \
         must be the distinct unconfirmed one, got {outcome:?}"
    );
    assert_eq!(
        reborn.sync_watermark(),
        gate_after_warmup,
        "the SYNC SAFE deletion gate must not advance past the \
         warmup confirmation"
    );

    // Age everything past its window and prune. The warmup row (500) was
    // DELIVERED — the hub provably holds it — so SYNC SAFE's delete-after-delivery
    // rule lets that one row go; what must never be touched is the UNDELIVERED
    // backlog (600..830), which the hub never received. On the buggy tip the false
    // confirmation opened the gate past that backlog and pruned it, so an intact
    // backlog is exactly what proves the reincarnation was refused. (The sibling
    // fix1_transmittable_ceiling checks its own backlog the same way, because its
    // warmup row is delivered and prunable too.)
    clock.advance(48 * 60 * 60 * 1000);
    let report = reborn
        .run_pruning_cycle_checked()
        .expect("a prune cycle still runs");
    let survivors = fixb_bodies(&reborn);
    assert!(
        survivors.is_superset(&expect_fixb(600..830)),
        "no SYNC SAFE row may be deleted while the hub does not \
         hold it — the undelivered backlog must survive entirely: {report:?} \
         survivors={survivors:?}"
    );

    hub.stop().await;
}

/// G1 — file-backed reincarnation retention (regression guard). A wiped-and-recreated
/// file-backed edge reusing its identity writes one fresh SYNC SAFE row and loses
/// its push acknowledgement on the way out. The stale per-edge watermark the hub
/// still holds must not confirm the batch, and the row — held nowhere else — must
/// survive retention. This is the already-protected form (the pre-push probe arms
/// the boolean distrust) so it holds before this change and must keep holding after
/// the wire change.
#[tokio::test]
async fn g1_file_backed_reincarnation_retains_sync_safe_rows_when_ack_is_lost() {
    let (clock, _guard) = MockClock::install(T0);
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let broker = InProcessBroker::new();

    let hub_db = Arc::new(Database::open_memory());
    let hub = start_hub_on(&broker, hub_db.clone());

    let original = Arc::new(Database::open_memory());
    original.execute(FIXB_DDL, &p()).expect("original table");
    insert_fixb(&original, 1..6);
    within(edge_client(&original, &broker, EDGE_A).push())
        .await
        .expect("original push");
    let recorded = hub_db
        .persisted_sync_applied_push_watermark_for_node(&TenantId::from(TENANT), EDGE_A)
        .expect("read per-edge watermark")
        .expect("the hub recorded a watermark for EDGE_A");
    assert!(
        recorded > Lsn(1),
        "fixture: the hub holds a high watermark for EDGE_A: {recorded:?}"
    );

    let reborn = Arc::new(Database::open(tmp.path().join("reborn.db")).expect("open reborn"));
    reborn.execute(FIXB_DDL, &p()).expect("reborn table");
    insert_fixb(&reborn, 99..100);
    assert!(
        reborn.current_lsn() < recorded,
        "fixture: the reborn edge's own LSN ({:?}) is below the stale watermark \
         ({recorded:?})",
        reborn.current_lsn()
    );
    assert_eq!(
        reborn.sync_watermark(),
        Lsn(0),
        "the reborn edge's SYNC SAFE deletion gate starts fully closed"
    );

    let unlucky = dropped_push_client(&reborn, &broker, EDGE_A);
    let outcome = within(unlucky.push()).await;
    assert!(
        matches!(
            outcome,
            Err(contextdb_core::Error::SyncPushUnconfirmed { .. })
        ),
        "a file-backed reincarnation whose ack is lost must not be confirmed by the \
         stale per-edge watermark, got {outcome:?}"
    );
    assert_eq!(
        reborn.sync_watermark(),
        Lsn(0),
        "the SYNC SAFE deletion gate must not have opened"
    );

    clock.advance(48 * 60 * 60 * 1000);
    let report = reborn
        .run_pruning_cycle_checked()
        .expect("a prune cycle still runs");
    assert_eq!(
        report.pruned_rows, 0,
        "no SYNC SAFE row may be deleted while the hub does not hold it: {report:?}"
    );
    assert_eq!(
        fixb_bodies(&reborn),
        expect_fixb(99..100),
        "the reborn edge's row survives by value"
    );

    hub.stop().await;
}

/// G5 — non-regression guard. An ESTABLISHED edge (same identity, same
/// life) whose second batch GENUINELY LANDS on the hub but whose acknowledgement
/// is lost must still CONFIRM: no false negative, no spurious re-upload. This is
/// the guard that the reincarnation refusal above does not over-reach into
/// rejecting an honest landed batch. It passes before this change and must keep passing.
#[tokio::test]
async fn g5_established_edge_landed_but_ack_lost_still_confirms() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let broker = InProcessBroker::new();

    let hub_db = Arc::new(Database::open_memory());
    let hub = start_hub_on(&broker, hub_db.clone());

    // A file-backed edge with an ESTABLISHED position: its first batch lands and
    // is directly confirmed, and the confirmed push watermark persists so a later
    // client on the same edge reloads a non-zero position (no reincarnation).
    let edge = Arc::new(Database::open(tmp.path().join("edge.db")).expect("open edge"));
    edge.execute(FIXB_DDL, &p()).expect("edge table");
    insert_fixb(&edge, 1..4);
    within(edge_client(&edge, &broker, EDGE_A).push())
        .await
        .expect("first push confirmed");
    let established = hub_db
        .persisted_sync_applied_push_watermark_for_node(&TenantId::from(TENANT), EDGE_A)
        .expect("read per-edge watermark")
        .expect("the hub recorded a watermark for EDGE_A");
    assert!(
        established > Lsn(0),
        "fixture: the edge has an established per-edge watermark: {established:?}"
    );

    // A second batch that GENUINELY lands on the hub, but whose acknowledgement is
    // lost on the way back. Same identity, same life — it must confirm.
    insert_fixb(&edge, 10..14);
    let frontier = edge.current_lsn();
    let landed = landed_ack_lost_client(&edge, &broker, EDGE_A);
    let outcome = within(landed.push()).await;

    assert_eq!(
        fixb_bodies(&hub_db),
        both(expect_fixb(1..4), expect_fixb(10..14)),
        "the batch reached the hub and committed"
    );
    assert!(
        outcome.is_ok(),
        "an established edge whose landed batch loses only its ack must still \
         confirm — no false negative, no spurious re-upload, got {outcome:?}"
    );
    assert_eq!(
        landed.push_watermark(),
        frontier,
        "the confirmed push advances the edge watermark to its own transmitted \
         frontier"
    );

    hub.stop().await;
}

/// G6 — change_destination reset behavior (regression guard). Pointing an edge at a new
/// retained-data destination forgets what the previous destination confirmed in
/// the SAME operation: both watermarks reset to zero so the next push rebuilds
/// the new destination from everything the edge still holds, and no local row is
/// skipped against a destination that never received it. The reset is stable
/// across the wire change; only the swallowed marker-clear inside it is removed.
#[tokio::test]
async fn g6_change_destination_resets_watermarks() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let broker = InProcessBroker::new();

    let hub_db = Arc::new(Database::open_memory());
    let hub = start_hub_on(&broker, hub_db.clone());

    let edge = Arc::new(Database::open(tmp.path().join("edge.db")).expect("open edge"));
    edge.execute(FIXB_DDL, &p()).expect("edge table");
    insert_fixb(&edge, 1..4);
    let client = edge_client(&edge, &broker, EDGE_A);
    within(client.push()).await.expect("push confirmed");
    assert!(
        client.push_watermark() > Lsn(0),
        "fixture: the edge has a confirmed position against its first destination"
    );

    client
        .change_destination(EDGE_B)
        .expect("change destination to a new hub");
    assert_eq!(
        client.push_watermark(),
        Lsn(0),
        "change_destination resets the push watermark so no row is skipped against \
         a destination that never received it"
    );
    assert_eq!(
        client.pull_watermark(),
        Lsn(0),
        "and the pull watermark resets in the same operation"
    );

    hub.stop().await;
}

/// G4 — the hub keys its per-edge record by (node_id, incarnation). This is the
/// keying the whole reincarnation refusal rests on, so it is pinned directly
/// through the status exchange. After the original life of an edge records a high
/// watermark under its incarnation, a status probe carrying that SAME incarnation
/// still reads the recorded value back, while a probe carrying a DIFFERENT
/// (rebuilt-life) incarnation for the SAME node id is answered Lsn(0) — the hub
/// holds nothing for that life, so an interrupted push on it can never be
/// false-confirmed by the original life's stale-high watermark. Lands with the
/// wire change.
#[tokio::test]
async fn g4_hub_keys_the_per_edge_watermark_by_node_and_incarnation() {
    let broker = InProcessBroker::new();

    let hub_db = Arc::new(Database::open_memory());
    let hub = start_hub_on(&broker, hub_db.clone());

    // The original life of EDGE_A records a high per-edge watermark under ITS
    // incarnation.
    let original = Arc::new(Database::open_memory());
    original.execute(FIXB_DDL, &p()).expect("original table");
    insert_fixb(&original, 1..60);
    within(edge_client(&original, &broker, EDGE_A).push())
        .await
        .expect("original push");
    let old_incarnation = original
        .sync_incarnation(&TenantId::from(TENANT))
        .expect("read the original life's incarnation");
    let recorded = hub_db
        .persisted_sync_applied_push_watermark_for_node(&TenantId::from(TENANT), EDGE_A)
        .expect("read per-edge watermark")
        .expect("the hub recorded a watermark for EDGE_A");
    assert!(
        recorded > Lsn(1),
        "fixture: the hub holds a high watermark for EDGE_A: {recorded:?}"
    );

    // A probe carrying the ORIGINAL incarnation reads its own recorded watermark.
    let (_old_request, old_status) = status_for(&broker, TENANT, EDGE_A, old_incarnation).await;
    assert_eq!(
        old_status.applied_push_watermark,
        Some(recorded),
        "the original incarnation still reads back its own recorded watermark \
         ({recorded:?})"
    );

    // A probe carrying a NEW (rebuilt-life) incarnation for the SAME node id is an
    // unknown edge life, answered Lsn(0) — never the original life's watermark.
    let new_incarnation = Incarnation::mint();
    assert_ne!(
        new_incarnation, old_incarnation,
        "a rebuilt life mints a distinct incarnation"
    );
    let (_new_request, new_status) = status_for(&broker, TENANT, EDGE_A, new_incarnation).await;
    assert_eq!(
        new_status.applied_push_watermark,
        Some(Lsn(0)),
        "a fresh incarnation is an unknown edge life: the hub answers Lsn(0), never \
         the original life's stale-high watermark of {recorded:?}"
    );

    hub.stop().await;
}

/// A memory-only hub can restart its SyncServer over the same `Database`.
/// The server's own cache is new, so this proves the committed receipt was
/// published into the database-owned memory frontier only after the apply.
#[tokio::test]
async fn memory_hub_server_restart_keeps_the_authenticated_edge_receipt() {
    let first_broker = InProcessBroker::new();
    let hub_db = Arc::new(Database::open_memory());
    let first_hub = start_hub_on(&first_broker, hub_db.clone());
    let edge = Arc::new(Database::open_memory());
    edge.execute(FIXB_DDL, &p()).expect("edge table");
    insert_fixb(&edge, 1..4);
    within(edge_client(&edge, &first_broker, EDGE_A).push())
        .await
        .expect("initial push");
    let incarnation = edge
        .sync_incarnation(&TenantId::from(TENANT))
        .expect("edge incarnation");
    let recorded = hub_db
        .persisted_sync_applied_push_watermark_for_node_incarnation(
            &TenantId::from(TENANT),
            EDGE_A,
            incarnation,
        )
        .expect("read database-owned memory receipt")
        .expect("initial push committed a receipt");
    first_hub.stop().await;

    let restarted_broker = InProcessBroker::new();
    let restarted_hub = start_hub_on(&restarted_broker, hub_db);
    let (_request, status) = status_for(&restarted_broker, TENANT, EDGE_A, incarnation).await;
    assert_eq!(
        status.applied_push_watermark,
        Some(recorded),
        "a new SyncServer cache must reload the same database-owned receipt"
    );
    restarted_hub.stop().await;
}

/// Reopen-retains-incarnation. A file-backed database reloads the SAME incarnation
/// after a full close and reopen — so an established edge keeps its identity across a
/// restart and a later landed-but-ack-lost push still confirms against its own record —
/// while a fresh database at a new path is a new life and mints a distinct incarnation.
/// This pins the persistence reopen path that G5 exercises only through a reconstructed
/// same-handle client rather than an actual close and reopen.
#[test]
fn reopen_retains_the_edge_incarnation_while_a_fresh_database_mints_a_new_one() {
    let tenant = TenantId::from(TENANT);
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = dir.path().join("edge.db");

    let first = {
        let db = Database::open(path.clone()).expect("open file-backed database");
        db.sync_incarnation(&tenant).expect("mint incarnation")
    }; // the database is dropped/closed here

    let reopened = Database::open(path.clone()).expect("reopen the same database");
    assert_eq!(
        reopened
            .sync_incarnation(&tenant)
            .expect("reload incarnation"),
        first,
        "a file-backed database must retain its incarnation across close/reopen, so an \
         established edge keeps its identity and a later lost-ack still confirms"
    );

    let other_dir = tempfile::TempDir::new().expect("tempdir");
    let fresh = Database::open(other_dir.path().join("other.db")).expect("open a fresh database");
    assert_ne!(
        fresh
            .sync_incarnation(&tenant)
            .expect("mint a distinct incarnation"),
        first,
        "a freshly created database is a new life and mints a distinct incarnation"
    );
}

// ---------------------------------------------------------------------------
// A row committed via the work-ledger surface, dropped without ever pushing,
// still pushes after a fresh reopen. A restarted authenticated client loads
// its push watermark from durable engine state, not an in-process-only
// counter. This belongs beside
// `reopen_retains_the_edge_incarnation_while_a_fresh_database_mints_a_new_one`
// above, which pins the sibling incarnation-reload half of the same restart
// story.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_row_committed_before_an_unclean_restart_still_pushes_after_reopen() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = dir.path().join("edge.db");

    let broker = InProcessBroker::new();
    let hub_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&hub_db).expect("hub work ledger schema");
    let hub = start_hub_on(&broker, hub_db);

    {
        // First "process": commit a row via the work-ledger surface, then
        // drop the handle WITHOUT ever pushing — simulating a crash or
        // unclean shutdown before the push ran.
        let db = Database::open(path.clone()).expect("open edge db");
        install_work_ledger_schema(&db).expect("edge work ledger schema");
        advertise_capability(
            &db,
            "edge-restart",
            "cap.demo",
            &["demo".to_string()],
            T0 as i64,
        )
        .expect("advertise capability");
        db.close().expect("close");
        // `db` drops here — nothing has been pushed yet.
    }

    // Reopen fresh, exactly as a new process would after the restart.
    let db = Arc::new(Database::open(path.clone()).expect("reopen edge db"));
    let client = edge_client(&db, &broker, EDGE_A);

    assert!(
        client
            .has_pending_push_changes()
            .expect("pending-changes check"),
        "the row committed before the unclean restart must still read as pending after reopen"
    );

    let applied = within(client.push()).await.expect("push after reopen");
    assert!(
        applied.applied_rows >= 1,
        "the reopened client must actually push the pre-restart row, got {applied:?}"
    );

    let hub_rows = hub
        .db
        .execute(
            "SELECT * FROM work_capabilities WHERE node_id = 'edge-restart'",
            &p(),
        )
        .expect("hub scan")
        .rows
        .len();
    assert_eq!(
        hub_rows, 1,
        "the hub must hold the row the reopened edge pushed"
    );

    hub.stop().await;
}
