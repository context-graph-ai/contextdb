//! Bounded tables — the transport-side acceptance tests: a synced retained
//! table starts maintaining itself, rows delete only after confirmed delivery,
//! a push-only table is never sent back, a pruned row never comes back, and
//! transfer receipts count per peer and direction.
//!
//! Before this change: the sync client never advanced the engine's `SYNC SAFE`
//! watermark, the one-way table policy did not exist, and there were no per-peer
//! transfer receipts on either plane.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads. Time
//! moves through `Wallclock::test_clock_guard`; pruning is driven synchronously
//! on the test thread.

use contextdb_core::{TenantId, Value, Wallclock};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy, SyncDirection};
use contextdb_engine::work_ledger::{BlobHash, MovementPolicy, install_work_ledger_schema};
use contextdb_server::blob_resolver::BlobStore;
use contextdb_server::subjects::{push_subject, status_subject};
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::transport::{
    ClientTransport, TransportError, TransportFuture, TransportResult, TransportStatusFuture,
};
use contextdb_server::{
    InProcessBroker, SyncClient, SyncServer, TransferDirection, TransferPlane, TransferReceipt,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

#[path = "media_support/mod.rs"]
mod media_support;
use media_support::{bind_spec, identity_file, node_id_of, seed_entitlement, within};

const T0: u64 = 1_700_000_000_000;
const TENANT: &str = "bounded";
/// The hub's authenticated node id. The client dials the hub by key, so it
/// always knows who it is talking to; the in-process broker presents this the
/// same way the iroh path presents the dialed endpoint's node id.
const HUB_NODE: &str = "hub-node-a";
const OTHER_HUB_NODE: &str = "hub-node-b";
/// A retained table for a downstream application, engine-generically named: a
/// small window row that ages out and only ever travels edge -> hub.
const RETAINED_DDL: &str = "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) \
     RETAIN 1 HOURS SYNC SAFE SYNC PUSH ONLY";
const CONTROL_DDL: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)";

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

fn row_count(db: &Database, table: &str) -> usize {
    db.execute(&format!("SELECT * FROM {table}"), &p())
        .unwrap_or_else(|err| panic!("{table} scan must succeed: {err}"))
        .rows
        .len()
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

fn insert_windows(db: &Database, ids: std::ops::Range<i64>) {
    for id in ids {
        let mut row = p();
        row.insert("id".to_string(), Value::Int64(id));
        row.insert("body".to_string(), Value::Text(format!("window-{id}")));
        db.execute("INSERT INTO windows (id, body) VALUES ($id, $body)", &row)
            .expect("window insert");
    }
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

/// A second writer's content at the SAME keys `insert_notes` uses, but
/// genuinely DIFFERENT (`x` for `n`) — a real competing write a conflict
/// policy must actually decide on, not a coincidental re-delivery of the same
/// bytes. Kept the same byte length as `insert_notes`'s body ("note-{id}" vs
/// "xote-{id}", both 6 bytes for the single-digit ids this file uses) on
/// purpose: a receipt's `payload_bytes` is asserted equal between the two
/// writers below, and a length mismatch would fail that equality for a reason
/// that has nothing to do with what the test is actually about.
fn insert_divergent_notes(db: &Database, ids: std::ops::Range<i64>) {
    for id in ids {
        let mut row = p();
        row.insert("id".to_string(), Value::Int64(id));
        row.insert("body".to_string(), Value::Text(format!("xote-{id}")));
        db.execute("INSERT INTO notes (id, body) VALUES ($id, $body)", &row)
            .expect("divergent note insert");
    }
}

struct RunningHub {
    db: Arc<Database>,
    server: Arc<SyncServer>,
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl RunningHub {
    async fn stop(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = self.task.await;
    }
}

fn start_hub(broker: &InProcessBroker) -> RunningHub {
    start_hub_as(broker, HUB_NODE)
}

fn start_hub_with_policies(
    broker: &InProcessBroker,
    node_id: &str,
    policies: ConflictPolicies,
) -> RunningHub {
    let db = Arc::new(Database::open_memory());
    db.execute(RETAINED_DDL, &p()).expect("hub retained table");
    db.execute(CONTROL_DDL, &p()).expect("hub control table");
    let server = Arc::new(SyncServer::with_transport(
        db.clone(),
        broker.server_as(node_id),
        TenantId::from(TENANT),
        policies,
    ));
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    RunningHub {
        db,
        server,
        shutdown,
        task,
    }
}

fn start_hub_as(broker: &InProcessBroker, node_id: &str) -> RunningHub {
    let db = Arc::new(Database::open_memory());
    db.execute(RETAINED_DDL, &p()).expect("hub retained table");
    db.execute(CONTROL_DDL, &p()).expect("hub control table");
    let server = Arc::new(SyncServer::with_transport(
        db.clone(),
        broker.server_as(node_id),
        TenantId::from(TENANT),
        ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    ));
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    RunningHub {
        db,
        server,
        shutdown,
        task,
    }
}

fn open_edge(path: Option<&std::path::Path>) -> Arc<Database> {
    let db = match path {
        Some(path) => Arc::new(Database::open(path).expect("open edge db")),
        None => Arc::new(Database::open_memory()),
    };
    if db.table_meta("windows").is_none() {
        db.execute(RETAINED_DDL, &p()).expect("edge retained table");
        db.execute(CONTROL_DDL, &p()).expect("edge control table");
    }
    db
}

fn edge_client(db: &Arc<Database>, broker: &InProcessBroker, node_id: &str) -> SyncClient {
    SyncClient::with_transport(
        db.clone(),
        broker.client_as(node_id),
        TenantId::from(TENANT),
    )
}

/// Receipts exist ONLY for authenticated peers: `peer_node_id` is a plain
/// `String`, and a transport that authenticates nobody produces no receipt at
/// all rather than an unkeyed one (asserted in
/// `c9_an_unauthenticated_peer_produces_no_transfer_receipt`).
fn receipt<'a>(
    receipts: &'a [TransferReceipt],
    peer: &str,
    plane: TransferPlane,
    direction: TransferDirection,
) -> &'a TransferReceipt {
    receipts
        .iter()
        .find(|r| r.peer_node_id == peer && r.plane == plane && r.direction == direction)
        .unwrap_or_else(|| {
            panic!("no {plane:?}/{direction:?} receipt for peer {peer} in {receipts:?}")
        })
}

/// A transport that is simply not there — the worker-offline condition, with no
/// sleeping and no timing.
struct OfflineTransport;

impl ClientTransport for OfflineTransport {
    fn request<'a>(
        &'a self,
        _subject: &'a str,
        _request_bytes: Vec<u8>,
        _timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        Box::pin(async {
            TransportResult::Err(TransportError::Other("edge is offline".to_string()))
        })
    }
}

// ---------------------------------------------------------------------------
// Retention runs by itself, including for a table that ARRIVED by sync
// ---------------------------------------------------------------------------

#[tokio::test]
async fn c1_synced_ddl_arrival_starts_maintenance_on_the_receiving_edge() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);

    // A fresh edge that has never heard of the retained table.
    let edge = Arc::new(Database::open_memory());
    assert!(
        !edge.maintenance_status().running,
        "a bare edge maintains nothing yet"
    );

    let client = edge_client(&edge, &broker, "edge-a");
    within(client.pull_default()).await.expect("edge pulls DDL");

    assert!(
        edge.table_meta("windows").is_some(),
        "the retained table must arrive by synced DDL"
    );
    let status = edge.maintenance_status();
    assert!(
        status.retention_enabled && status.running,
        "a retained table arriving by sync must start maintenance with no consumer call: {status:?}"
    );

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// Delete only after delivery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn c3_confirmed_push_advances_the_engine_sync_watermark() {
    let (_clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);
    let edge = open_edge(None);
    insert_windows(&edge, 1..6);

    assert_eq!(
        edge.sync_watermark(),
        contextdb_core::Lsn(0),
        "nothing has been confirmed yet, so the SYNC SAFE gate is fully closed"
    );

    let client = edge_client(&edge, &broker, "edge-a");
    let applied = within(client.push()).await.expect("push");
    assert!(applied.applied_rows >= 5, "the hub really took the rows");

    // The engine's prune predicate is `row.lsn >= sync_watermark` blocks
    // (database.rs:17187-17190), so the watermark is the EXCLUSIVE frontier:
    // the first LSN that is NOT yet confirmed. Confirmed through L therefore
    // means exactly L+1 — one more would open the gate on an unconfirmed row,
    // one less would strand a confirmed one forever.
    let confirmed = client.push_watermark();
    assert!(confirmed.0 > 0, "the hub confirmed a real batch");
    assert_eq!(
        edge.sync_watermark(),
        contextdb_core::Lsn(confirmed.0 + 1),
        "the REAL sync client must set the engine gate to first-unconfirmed-LSN \
         (confirmed through {confirmed:?}) — no app code calls set_sync_watermark"
    );

    hub.stop().await;
}

/// The decisive row: a write made AFTER the confirmed batch sits beyond
/// the frontier and must survive a prune that takes every confirmed row, however
/// far past its window it is. This is what a watermark that ran ahead of
/// confirmation would delete.
#[tokio::test]
async fn c3_a_row_beyond_the_confirmed_batch_is_never_pruned() {
    let (clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);
    let edge = open_edge(None);

    insert_windows(&edge, 1..6);
    let client = edge_client(&edge, &broker, "edge-a");
    within(client.push()).await.expect("push");
    let confirmed = client.push_watermark();

    // Written after the push: never confirmed by anyone.
    insert_windows(&edge, 100..101);
    let unconfirmed_lsn = edge.current_lsn();
    assert!(
        unconfirmed_lsn > confirmed,
        "the new row must genuinely sit beyond the confirmed batch \
         (row={unconfirmed_lsn:?}, confirmed={confirmed:?})"
    );

    clock.advance(48 * 60 * 60 * 1000);
    let report = edge.run_pruning_cycle_checked().expect("prune");

    assert_eq!(
        report.pruned_rows, 5,
        "exactly the confirmed rows prune: {report:?}"
    );
    assert_eq!(
        row_count(&edge, "windows"),
        1,
        "the row beyond the confirmation frontier survives, whatever its age"
    );
    assert_eq!(
        edge.sync_watermark(),
        contextdb_core::Lsn(confirmed.0 + 1),
        "and a prune cycle never moves the gate itself"
    );

    hub.stop().await;
}

#[tokio::test]
async fn c3_unconfirmed_rows_are_never_pruned() {
    let (clock, _guard) = MockClock::install(T0);
    let edge = open_edge(None);
    insert_windows(&edge, 1..21);

    let offline = SyncClient::with_transport(
        edge.clone(),
        Arc::new(OfflineTransport),
        TenantId::from(TENANT),
    );
    within(offline.push())
        .await
        .expect_err("an offline push cannot be confirmed");

    // Age the whole backlog well past its retention window.
    clock.advance(48 * 60 * 60 * 1000);
    let report = edge
        .run_pruning_cycle_checked()
        .expect("a prune cycle still runs");

    assert_eq!(
        report.pruned_rows, 0,
        "not one unconfirmed row may be deleted, however old: {report:?}"
    );
    assert_eq!(
        row_count(&edge, "windows"),
        20,
        "the offline worker keeps its ENTIRE backlog"
    );
    assert_eq!(
        edge.sync_watermark(),
        contextdb_core::Lsn(0),
        "a failed push must not move the gate"
    );
}

#[tokio::test]
async fn c3_offline_backlog_prunes_only_after_reconnect_and_confirm() {
    let (clock, _guard) = MockClock::install(T0);
    let edge = open_edge(None);
    insert_windows(&edge, 1..21);

    let offline = SyncClient::with_transport(
        edge.clone(),
        Arc::new(OfflineTransport),
        TenantId::from(TENANT),
    );
    within(offline.push())
        .await
        .expect_err("offline push fails");
    clock.advance(48 * 60 * 60 * 1000);
    assert_eq!(
        edge.run_pruning_cycle_checked().expect("prune").pruned_rows,
        0,
        "still nothing prunes while offline"
    );

    // Reconnect: same database, a hub that is now reachable.
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);
    let client = edge_client(&edge, &broker, "edge-a");
    within(client.push()).await.expect("reconnected push");
    assert_eq!(
        row_count(&hub.db, "windows"),
        20,
        "the whole backlog reached the hub before anything was deleted"
    );

    let report = edge
        .run_pruning_cycle_checked()
        .expect("prune after confirm");
    assert_eq!(
        report.pruned_rows, 20,
        "confirmed rows past their window become prunable: {report:?}"
    );
    assert_eq!(row_count(&edge, "windows"), 0);

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// The hub never sends a retained table back
// ---------------------------------------------------------------------------

#[tokio::test]
async fn c4_hub_never_sends_a_push_only_retained_table_back() {
    let (_clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);

    let edge_a = open_edge(None);
    insert_windows(&edge_a, 1..6);
    insert_notes(&edge_a, 1..4);
    let client_a = edge_client(&edge_a, &broker, "edge-a");
    within(client_a.push()).await.expect("edge-a push");
    assert_eq!(
        row_count(&hub.db, "windows"),
        5,
        "the hub does hold the retained rows — this is not an empty-hub tautology"
    );
    assert_eq!(row_count(&hub.db, "notes"), 3);

    // A second edge pulls everything the hub has to offer.
    let edge_b = open_edge(None);
    let client_b = edge_client(&edge_b, &broker, "edge-b");
    within(client_b.pull_default()).await.expect("edge-b pull");

    assert_eq!(
        row_count(&edge_b, "notes"),
        3,
        "the ordinary table proves the pull path works"
    );
    assert_eq!(
        row_count(&edge_b, "windows"),
        0,
        "a PUSH ONLY retained table is never sent back by the hub"
    );

    hub.stop().await;
}

#[tokio::test]
async fn c4_one_way_policy_holds_after_restart_without_app_reregistration() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("edge.db");
    let (_clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);

    {
        let edge = open_edge(Some(&path));
        insert_windows(&edge, 1..6);
        let client = edge_client(&edge, &broker, "edge-a");
        within(client.push()).await.expect("push");
    }

    // Restart: a brand-new SyncClient whose runtime direction map starts empty.
    // Nothing re-registers the policy; it comes off the table meta.
    let edge = open_edge(Some(&path));
    edge.execute("DELETE FROM windows", &p())
        .expect("clear local rows");
    assert_eq!(row_count(&edge, "windows"), 0);

    let client = edge_client(&edge, &broker, "edge-a");
    within(client.pull_default()).await.expect("pull");
    assert_eq!(
        row_count(&edge, "windows"),
        0,
        "after restart the declared one-way policy still refuses inbound rows for this table"
    );

    hub.stop().await;
}

/// The multi-hub refusal must run on the REAL client path, not only when an app
/// calls the engine API by hand: pushing a retained table registers the hub the
/// client is authenticated against, so a second hub is refused by the push
/// itself, with no cooperation from the embedding application.
#[tokio::test]
async fn c4_pushing_a_retained_table_registers_the_hub_and_a_second_hub_is_refused() {
    let (_clock, _guard) = MockClock::install(T0);
    let first_broker = InProcessBroker::new();
    let first_hub = start_hub_as(&first_broker, HUB_NODE);
    let edge = open_edge(None);
    insert_windows(&edge, 1..6);

    assert_eq!(
        edge.retention_sync_peer(),
        None,
        "no hub is established before the first push"
    );
    let client = edge_client(&edge, &first_broker, "edge-a");
    within(client.push()).await.expect("first hub push");
    assert_eq!(
        edge.retention_sync_peer().as_deref(),
        Some(HUB_NODE),
        "the client itself registers the authenticated hub it delivered to"
    );

    within(client.push())
        .await
        .expect("pushing again to the SAME hub is unaffected");

    // A different hub, same database, same retained table.
    let second_broker = InProcessBroker::new();
    let second_hub = start_hub_as(&second_broker, OTHER_HUB_NODE);
    let second_client = edge_client(&edge, &second_broker, "edge-a");
    insert_windows(&edge, 100..103);
    let err = within(second_client.push())
        .await
        .expect_err("a retained table may not be delivered to a second hub");
    let message = err.to_string();
    assert!(
        message.contains(OTHER_HUB_NODE) && message.contains(HUB_NODE),
        "the refusal must name both hubs, got: {message}"
    );
    assert_eq!(
        row_count(&second_hub.db, "windows"),
        0,
        "and not one retained row may reach the second hub"
    );
    assert_eq!(
        edge.retention_sync_peer().as_deref(),
        Some(HUB_NODE),
        "the established hub is unchanged"
    );

    first_hub.stop().await;
    second_hub.stop().await;
}

// ---------------------------------------------------------------------------
// A pruned row never comes back
// ---------------------------------------------------------------------------

#[tokio::test]
async fn c5_pull_does_not_replant_pruned_rows_the_hub_still_holds() {
    let (clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);
    let edge = open_edge(None);

    insert_windows(&edge, 1..11);
    let client = edge_client(&edge, &broker, "edge-a");
    within(client.push()).await.expect("push");
    assert_eq!(
        row_count(&hub.db, "windows"),
        10,
        "the hub genuinely holds the rows AND their history"
    );

    clock.advance(2 * 60 * 60 * 1000);
    assert_eq!(
        edge.run_pruning_cycle_checked().expect("prune").pruned_rows,
        10
    );
    assert_eq!(row_count(&edge, "windows"), 0);

    within(client.pull_default()).await.expect("pull");
    assert_eq!(
        row_count(&edge, "windows"),
        0,
        "a pull against a hub that still holds the rows must not replant them"
    );
    assert_eq!(
        row_count(&hub.db, "windows"),
        10,
        "the edge's expiry must not delete the hub's copy either — the hub ages on its own clock"
    );

    hub.stop().await;
}

#[tokio::test]
async fn c5_restart_does_not_resurrect_pruned_rows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("edge.db");
    let (clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);

    {
        let edge = open_edge(Some(&path));
        insert_windows(&edge, 1..11);
        let client = edge_client(&edge, &broker, "edge-a");
        within(client.push()).await.expect("push");
        clock.advance(2 * 60 * 60 * 1000);
        assert_eq!(
            edge.run_pruning_cycle_checked().expect("prune").pruned_rows,
            10
        );
    }

    let edge = open_edge(Some(&path));
    assert_eq!(
        row_count(&edge, "windows"),
        0,
        "a restart must not bring pruned rows back from persistence"
    );

    hub.stop().await;
}

#[tokio::test]
async fn c5_wipe_and_restore_pull_does_not_resurrect_pruned_rows() {
    let (clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);

    let edge = open_edge(None);
    insert_windows(&edge, 1..11);
    insert_notes(&edge, 1..4);
    let client = edge_client(&edge, &broker, "edge-a");
    within(client.push()).await.expect("push");
    clock.advance(2 * 60 * 60 * 1000);
    assert_eq!(
        edge.run_pruning_cycle_checked().expect("prune").pruned_rows,
        10
    );

    // Wipe the machine and restore from the hub, watermark zero: the full
    // restore path, with the hub still holding every pruned row.
    let restored = open_edge(None);
    let restore_client = edge_client(&restored, &broker, "edge-a-restored");
    within(restore_client.pull_default())
        .await
        .expect("restore pull");

    assert_eq!(
        row_count(&restored, "notes"),
        3,
        "the restore genuinely restored what it should — control"
    );
    assert_eq!(
        row_count(&restored, "windows"),
        0,
        "wipe-and-restore must not replant rows that aged out"
    );

    hub.stop().await;
}

#[tokio::test]
async fn c5_hub_ages_retained_rows_on_its_own_clock() {
    let (clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);
    let edge = open_edge(None);

    insert_windows(&edge, 1..11);
    let client = edge_client(&edge, &broker, "edge-a");
    within(client.push()).await.expect("push");
    clock.advance(2 * 60 * 60 * 1000);

    // The edge's prune says nothing about the hub: expiry is not replicated.
    edge.run_pruning_cycle_checked().expect("edge prune");
    assert_eq!(row_count(&hub.db, "windows"), 10);

    // The hub ages the rows when ITS OWN cycle runs.
    let hub_report = hub.db.run_pruning_cycle_checked().expect("hub prune");
    assert_eq!(
        hub_report.pruned_rows, 10,
        "the hub prunes on its own cycle: {hub_report:?}"
    );
    assert_eq!(row_count(&hub.db, "windows"), 0);

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// Transfer receipts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn c9_sync_receipts_count_items_and_bytes_per_peer_and_direction() {
    let (_clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);

    let edge_a = open_edge(None);
    insert_notes(&edge_a, 1..8);
    let client_a = edge_client(&edge_a, &broker, "edge-a");
    within(client_a.push()).await.expect("edge-a push");

    let edge_b = open_edge(None);
    insert_notes(&edge_b, 100..102);
    let client_b = edge_client(&edge_b, &broker, "edge-b");
    within(client_b.push()).await.expect("edge-b push");
    within(client_b.pull_default()).await.expect("edge-b pull");

    let receipts = hub.server.transfer_receipts();
    let from_a = receipt(
        &receipts,
        "edge-a",
        TransferPlane::Sync,
        TransferDirection::Received,
    );
    let from_b = receipt(
        &receipts,
        "edge-b",
        TransferPlane::Sync,
        TransferDirection::Received,
    );
    assert_eq!(
        from_a.counters.items, 7,
        "items are counted per authenticated peer: {receipts:?}"
    );
    assert_eq!(from_b.counters.items, 2, "and never merged: {receipts:?}");
    assert!(
        from_a.counters.payload_bytes > from_b.counters.payload_bytes,
        "payload bytes track the peer's own traffic: {receipts:?}"
    );

    let sent_to_b = receipt(
        &receipts,
        "edge-b",
        TransferPlane::Sync,
        TransferDirection::Sent,
    );
    assert!(
        sent_to_b.counters.items >= 7,
        "the pull direction is counted separately, on the same peer key: {receipts:?}"
    );

    hub.stop().await;
}

/// The client end of the same exchange. The client dials the hub by key, so its
/// receipts are keyed by the hub's authenticated node id, with Sent and Received
/// counted separately and exactly.
#[tokio::test]
async fn c9_client_receipts_record_both_directions_against_the_hub_node_id() {
    let (_clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);

    // A DIFFERENT edge seeds the hub with four rows, so the pull has a known,
    // non-zero size made entirely of FOREIGN rows.
    let seeder = open_edge(None);
    insert_notes(&seeder, 500..504);
    let seed_client = edge_client(&seeder, &broker, "edge-seed");
    within(seed_client.push()).await.expect("seed push");

    let edge = open_edge(None);
    insert_notes(&edge, 1..7);
    let client = edge_client(&edge, &broker, "edge-a");
    // Pull BEFORE push, deliberately: whether a pull echoes a client's own
    // pushed rows back is an open pull-watermark question this test leaves
    // untouched. Pulling first means the hub holds only the four foreign rows
    // at that moment, so the count is exact without depending on either answer
    // to that open question.
    within(client.pull_default()).await.expect("pull");
    within(client.push()).await.expect("push");

    let receipts = client.transfer_receipts();
    let sent = receipt(
        &receipts,
        HUB_NODE,
        TransferPlane::Sync,
        TransferDirection::Sent,
    );
    let received = receipt(
        &receipts,
        HUB_NODE,
        TransferPlane::Sync,
        TransferDirection::Received,
    );
    assert_eq!(
        sent.counters.items, 6,
        "the client counts exactly the rows it pushed: {receipts:?}"
    );
    assert_eq!(
        received.counters.items, 4,
        "and exactly the rows it pulled back: {receipts:?}"
    );
    assert!(
        sent.counters.payload_bytes > 0 && received.counters.payload_bytes > 0,
        "both directions carry payload bytes: {receipts:?}"
    );
    assert!(
        receipts.iter().all(|r| r.peer_node_id == HUB_NODE),
        "the client talks to exactly one hub, so every receipt names it: {receipts:?}"
    );

    hub.stop().await;
}

/// The contract behind a non-optional `peer_node_id`: a transport that
/// authenticates no peer produces NO transfer receipt, rather than an unkeyed
/// one that would quietly aggregate every anonymous peer together.
#[tokio::test]
async fn c9_an_unauthenticated_peer_produces_no_transfer_receipt() {
    let (_clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);

    let known = open_edge(None);
    insert_notes(&known, 1..4);
    let known_client = edge_client(&known, &broker, "edge-a");
    within(known_client.push())
        .await
        .expect("authenticated push");
    let after_known = hub.server.transfer_receipts();
    assert_eq!(
        after_known.len(),
        1,
        "one authenticated peer, one receipt: {after_known:?}"
    );

    // `broker.client()` presents no node id — the deprecated broker path.
    let anonymous = open_edge(None);
    insert_notes(&anonymous, 100..104);
    let anonymous_client =
        SyncClient::with_transport(anonymous.clone(), broker.client(), TenantId::from(TENANT));
    within(anonymous_client.push())
        .await
        .expect("the push itself still works");
    assert!(
        row_count(&hub.db, "notes") >= 7,
        "the rows really did land — this is not a failed-transfer tautology"
    );

    let after_anonymous = hub.server.transfer_receipts();
    assert_eq!(
        after_anonymous.len(),
        1,
        "an unauthenticated transfer adds no receipt at all: {after_anonymous:?}"
    );
    assert_eq!(
        receipt(
            &after_anonymous,
            "edge-a",
            TransferPlane::Sync,
            TransferDirection::Received,
        )
        .counters
        .items,
        3,
        "and never inflates the authenticated peer's counters: {after_anonymous:?}"
    );

    hub.stop().await;
}

#[tokio::test]
async fn c9_sync_receipts_count_payload_bytes_only_never_transport_framing() {
    let (_clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);

    let edge = open_edge(None);
    insert_notes(&edge, 1..21);
    let client = edge_client(&edge, &broker, "edge-a");
    within(client.push()).await.expect("push");

    let wire_bytes: usize = broker
        .recorded_exchanges()
        .iter()
        .map(|exchange| exchange.request_bytes.len())
        .sum();
    let receipts = hub.server.transfer_receipts();
    let received = receipt(
        &receipts,
        "edge-a",
        TransferPlane::Sync,
        TransferDirection::Received,
    );

    assert!(
        received.counters.payload_bytes > 0,
        "payload bytes must actually be counted: {receipts:?}"
    );
    assert!(
        (received.counters.payload_bytes as usize) < wire_bytes,
        "receipts count payload only — envelope and framing overhead is excluded \
         (payload={}, wire={wire_bytes})",
        received.counters.payload_bytes
    );

    hub.stop().await;
}

#[tokio::test]
async fn c9_receipts_are_monotonic_and_never_restored_from_the_database() {
    let (_clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);
    let edge = open_edge(None);

    insert_notes(&edge, 1..6);
    let client = edge_client(&edge, &broker, "edge-a");
    within(client.push()).await.expect("first push");
    let first = receipt(
        &hub.server.transfer_receipts(),
        "edge-a",
        TransferPlane::Sync,
        TransferDirection::Received,
    )
    .counters
    .clone();

    insert_notes(&edge, 6..11);
    within(client.push()).await.expect("second push");
    let second = receipt(
        &hub.server.transfer_receipts(),
        "edge-a",
        TransferPlane::Sync,
        TransferDirection::Received,
    )
    .counters
    .clone();
    assert!(
        second.items > first.items && second.payload_bytes > first.payload_bytes,
        "counters are monotonic across transfers ({first:?} -> {second:?})"
    );

    // The engine persists nothing here: a fresh client on the SAME database
    // starts from zero, because there is nothing on disk to restore.
    let fresh = edge_client(&edge, &broker, "edge-a");
    assert!(
        fresh.transfer_receipts().is_empty(),
        "receipts are in-memory only — the engine persists none of this: {:?}",
        fresh.transfer_receipts()
    );

    hub.stop().await;
}

#[tokio::test]
async fn c9_blob_receipts_split_serve_and_fetch_by_peer() {
    let holder_dir = tempfile::tempdir().expect("holder dir");
    let one_dir = tempfile::tempdir().expect("consumer dir");
    let two_dir = tempfile::tempdir().expect("consumer dir");
    let holder_key = identity_file(&holder_dir);
    let one_key = identity_file(&one_dir);
    let two_key = identity_file(&two_dir);
    let holder_node = node_id_of(&holder_key);
    let one_node = node_id_of(&one_key);
    let two_node = node_id_of(&two_key);

    let content: Vec<u8> = (0..(256 * 1024)).map(|i| (i % 251) as u8).collect();
    let hash = BlobHash::of(&content);

    let holder_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&holder_db).expect("holder schema");
    seed_entitlement(&holder_db, "job-1", &holder_node, &one_node, &hash);
    seed_entitlement(&holder_db, "job-2", &holder_node, &two_node, &hash);
    let holder = BlobStore::new(
        holder_db,
        MovementPolicy {
            auto_propagate: true,
        },
        holder_key.clone(),
    );
    holder.set_test_clock(T0 as i64);
    assert_eq!(holder.ingest_bytes(&content).expect("ingest"), hash);
    let endpoint = within(IrohServer::bind(&bind_spec(&holder_key)))
        .await
        .expect("bind holder");
    holder.serve_on(&endpoint);
    let ticket = endpoint.ticket();

    let mut fetched_by_one = Vec::new();
    let consumer_one = {
        let db = Arc::new(Database::open_memory());
        install_work_ledger_schema(&db).expect("consumer schema");
        seed_entitlement(&db, "job-1", &holder_node, &one_node, &hash);
        let service = BlobStore::new(
            db,
            MovementPolicy {
                auto_propagate: true,
            },
            one_key,
        );
        service.set_test_clock(T0 as i64);
        within(service.resolve_blob_ref(&hash, &ticket, &mut fetched_by_one))
            .await
            .expect("consumer one resolve");
        service
    };
    assert_eq!(fetched_by_one.len(), content.len());

    let holder_receipts = holder.transfer_receipts();
    let served_to_one = receipt(
        &holder_receipts,
        &one_node,
        TransferPlane::Blob,
        TransferDirection::Sent,
    );
    assert_eq!(
        served_to_one.counters.items, 1,
        "one blob served to one peer: {holder_receipts:?}"
    );
    assert_eq!(
        served_to_one.counters.payload_bytes,
        content.len() as u64,
        "blob payload bytes are the blob's own bytes, framing excluded: {holder_receipts:?}"
    );

    let fetched_by_one_receipts = consumer_one.transfer_receipts();
    let received_from_holder = receipt(
        &fetched_by_one_receipts,
        &holder_node,
        TransferPlane::Blob,
        TransferDirection::Received,
    );
    assert_eq!(
        received_from_holder.counters.payload_bytes,
        content.len() as u64,
        "the fetching end counts the same payload against the holder: {fetched_by_one_receipts:?}"
    );

    // A second consumer must land on its OWN key, not aggregate with the first —
    // the existing `payload_bytes_emitted_for_test` seam cannot tell them apart.
    let mut fetched_by_two = Vec::new();
    {
        let db = Arc::new(Database::open_memory());
        install_work_ledger_schema(&db).expect("consumer schema");
        seed_entitlement(&db, "job-2", &holder_node, &two_node, &hash);
        let service = BlobStore::new(
            db,
            MovementPolicy {
                auto_propagate: true,
            },
            two_key,
        );
        service.set_test_clock(T0 as i64);
        within(service.resolve_blob_ref(&hash, &ticket, &mut fetched_by_two))
            .await
            .expect("consumer two resolve");
    }

    let holder_receipts = holder.transfer_receipts();
    assert_eq!(
        receipt(
            &holder_receipts,
            &one_node,
            TransferPlane::Blob,
            TransferDirection::Sent,
        )
        .counters
        .payload_bytes,
        content.len() as u64,
        "the first peer's counter is untouched by the second transfer: {holder_receipts:?}"
    );
    assert_eq!(
        receipt(
            &holder_receipts,
            &two_node,
            TransferPlane::Blob,
            TransferDirection::Sent,
        )
        .counters
        .payload_bytes,
        content.len() as u64,
        "the second peer gets its own counter: {holder_receipts:?}"
    );
}

// ---------------------------------------------------------------------------
// The delivery promise cannot be revoked from the side
//
// `set_table_direction` is public and unguarded. Setting `None` or `Pull` on a
// `SYNC SAFE` table drops its rows from the outbound changeset while the GLOBAL
// watermark keeps advancing on other tables' confirmations — so the gate opens
// on rows the hub never received and retention deletes them. The call is
// self-contradictory (a table that promises delete-after-DELIVERY, configured
// not to deliver), and its keyless sibling already earns a loud refusal, so this
// one may not stay silent and destructive.
//
// Registration order, the honest semantics: the client refuses at the FIRST
// point it can see the conflict. If the table's meta is already known, that is
// the `set_table_direction` call itself; if the direction was set before the
// table existed, nothing is knowable then and the conflict first becomes visible
// at push — so the push refuses rather than silently shipping an incomplete
// changeset. Both doors are covered below.
// ---------------------------------------------------------------------------

fn assert_delivery_promise_refusal(message: &str, table: &str) {
    let lower = message.to_lowercase();
    assert!(
        lower.contains(table),
        "the refusal must name the table it protects, got: {message}"
    );
    assert!(
        lower.contains("sync safe"),
        "the refusal must name the declaration that forbids it, got: {message}"
    );
    assert!(
        lower.contains("deliver"),
        "the refusal must say WHY — the table promises deletion only after \
         DELIVERY, so a direction that stops delivering would let retention \
         delete undelivered rows — got: {message}"
    );
}

#[tokio::test]
async fn c3_a_non_push_direction_on_a_sync_safe_table_is_refused() {
    let (_clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);
    let edge = open_edge(None);
    insert_windows(&edge, 1..6);

    let client = edge_client(&edge, &broker, "edge-a");
    for refused in [SyncDirection::None, SyncDirection::Pull] {
        let err = client
            .set_table_direction("windows", refused)
            .expect_err("a non-Push direction on a SYNC SAFE table must be refused");
        assert_delivery_promise_refusal(&err.to_string(), "windows");
    }

    // The refusals changed nothing: the table still delivers.
    within(client.push()).await.expect("push");
    assert_eq!(
        row_count(&hub.db, "windows"),
        5,
        "a refused direction change must leave the table delivering exactly as before"
    );

    hub.stop().await;
}

/// The allowed calls must stay allowed, or the guard is just a wall: `Push` on a
/// `SYNC SAFE` table is the one consistent setting, and a table that made no
/// delivery promise may still be configured however the app likes — including
/// out of the outbound changeset entirely.
#[tokio::test]
async fn c3_push_on_a_sync_safe_table_and_any_direction_elsewhere_stay_allowed() {
    let (_clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);
    let edge = open_edge(None);
    insert_windows(&edge, 1..6);
    insert_notes(&edge, 1..4);

    let client = edge_client(&edge, &broker, "edge-a");
    client
        .set_table_direction("windows", SyncDirection::Push)
        .expect("Push is the consistent setting for a SYNC SAFE table");
    client
        .set_table_direction("notes", SyncDirection::None)
        .expect("a table with no delivery promise may be configured freely");

    within(client.push()).await.expect("push");
    assert_eq!(
        row_count(&hub.db, "windows"),
        5,
        "the promising table still delivers"
    );
    assert_eq!(
        row_count(&hub.db, "notes"),
        0,
        "and the ordinary table's direction still takes effect"
    );

    hub.stop().await;
}

/// The registration-order case: the direction is set before the table exists, so
/// the client cannot see the conflict yet. It becomes visible at push — and the
/// push must refuse rather than ship a changeset that silently omits the table
/// while the watermark advances over everything else in it.
#[tokio::test]
async fn c3_a_direction_set_before_the_table_existed_is_refused_at_push() {
    let (clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);
    let edge = Arc::new(Database::open_memory());
    edge.execute(CONTROL_DDL, &p()).expect("control table");

    let client = edge_client(&edge, &broker, "edge-a");
    client
        .set_table_direction("windows", SyncDirection::None)
        .expect("nothing is knowable about a table that does not exist yet");

    // Now the table arrives, carrying the promise the direction contradicts.
    edge.execute(RETAINED_DDL, &p()).expect("retained table");
    insert_windows(&edge, 1..6);
    insert_notes(&edge, 1..4);

    let err = within(client.push())
        .await
        .expect_err("the push must refuse the contradiction it can now see");
    assert_delivery_promise_refusal(&err.to_string(), "windows");

    assert_eq!(
        edge.sync_watermark(),
        contextdb_core::Lsn(0),
        "a refused push may not advance the gate on ANY table — that is the \
         mechanism that would strand the undelivered rows"
    );
    clock.advance(48 * 60 * 60 * 1000);
    let report = edge.run_pruning_cycle_checked().expect("prune cycle");
    assert_eq!(
        report.pruned_rows, 0,
        "so nothing may be pruned, however old: {report:?}"
    );
    assert_eq!(row_count(&edge, "windows"), 5);

    hub.stop().await;
}

/// Receipts count what MOVED, not what was kept — and the hub end must obey that
/// as strictly as the pulling end does. The hub's push handler took its item
/// count from the APPLY result while its byte count covered every transmitted
/// row, so a push whose rows were skipped by conflict policy reported bytes for
/// rows its own item count denied: two figures drawn from different row sets,
/// which is exactly what the counters' contract forbids.
///
/// Only the hub side is tested here. The pulling side already draws both figures
/// from the same pre-apply row set (`sync_client.rs:612-614`), so its twin would
/// assert behaviour that is already correct; the hub's handler
/// (`sync_server.rs:579`) is the remaining half.
#[tokio::test]
async fn c9_hub_receipts_count_transmitted_rows_even_when_conflict_policy_skips_them() {
    let (_clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub_with_policies(
        &broker,
        HUB_NODE,
        ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
    );

    // First writer wins the keys.
    let first = open_edge(None);
    insert_notes(&first, 1..4);
    let first_client = edge_client(&first, &broker, "edge-a");
    let first_result = within(first_client.push()).await.expect("first push");
    assert_eq!(
        first_result.applied_rows, 3,
        "the first push genuinely lands its three rows"
    );

    // Second writer sends the SAME natural keys with GENUINELY DIFFERENT
    // content — a real competing write, so `InsertIfNotExists` has an actual
    // decision to make and actually declines it. (An identical-content
    // resend at the same keys is a re-delivery the apply layer now recognizes
    // as a no-op — see `Database::sync_incoming_row_is_already_local` — and
    // correctly counts nowhere; that is a DIFFERENT case from this one and is
    // covered by the `self_echo_pull_after_push_tests.rs` suite.) The rows
    // still cross the wire in full and are then skipped at apply — the case
    // the item count got wrong.
    let second = open_edge(None);
    insert_divergent_notes(&second, 1..4);
    let second_client = edge_client(&second, &broker, "edge-b");
    let second_result = within(second_client.push()).await.expect("second push");
    assert_eq!(
        second_result.applied_rows, 0,
        "the fixture's premise: nothing from the second push applied"
    );
    assert_eq!(
        second_result.skipped_rows, 3,
        "because the conflict policy skipped all three: {second_result:?}"
    );
    assert_eq!(
        row_count(&hub.db, "notes"),
        3,
        "so the hub still holds exactly the first writer's rows"
    );

    let receipts = hub.server.transfer_receipts();
    let from_first = receipt(
        &receipts,
        "edge-a",
        TransferPlane::Sync,
        TransferDirection::Received,
    );
    let from_second = receipt(
        &receipts,
        "edge-b",
        TransferPlane::Sync,
        TransferDirection::Received,
    );

    assert_eq!(
        from_second.counters.items, 3,
        "the hub must count the rows that CROSSED THE WIRE, not the subset it \
         chose to keep — a skipped row still moved: {receipts:?}"
    );
    assert_eq!(
        from_second.counters, from_first.counters,
        "and both peers shipped an equally-sized row set (same item count, same \
         byte length per row, genuinely different content), so their receipts \
         must be identical — the outcome of applying differs, what moved does \
         not: {receipts:?}"
    );
    assert!(
        from_second.counters.payload_bytes > 0,
        "with the bytes drawn from that same set: {receipts:?}"
    );

    hub.stop().await;
}

/// A transport that forwards the push batch to the real hub (so it applies and
/// commits) and then LOSES the acknowledgement on the way back — the shape of a
/// hub killed a few milliseconds into applying: the write landed durably, the
/// edge never saw the ack. `confirmable` decides whether the edge can then reach
/// hub status to discover the truth.
///
/// This is a sibling of `DropPushAckAfterApply` in transport_boundary_tests.rs
/// rather than a reuse of it, for one load-bearing reason: receipts are keyed by
/// the authenticated peer, so the fault wrapper MUST forward `peer_node_id` from
/// the transport it wraps. A wrapper that inherits the trait default reports no
/// peer, records nothing, and would make these tests pass for the wrong reason.
struct LoseAckAfterApply {
    inner: Arc<dyn ClientTransport>,
    push_subject: String,
    status_subject: String,
    confirmable: bool,
}

impl ClientTransport for LoseAckAfterApply {
    fn peer_node_id(&self) -> Option<String> {
        self.inner.peer_node_id()
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
            return Box::pin(async move {
                // The hub applies and commits ...
                let _ = inner
                    .request_single_reply(&subject, request_bytes, timeout)
                    .await;
                // ... and then the acknowledgement is lost coming back.
                Err(TransportError::Unreachable(
                    "ack lost after the hub committed the batch".to_string(),
                ))
            });
        }
        if !self.confirmable && subject == self.status_subject {
            return Box::pin(async {
                Err(TransportError::Unreachable(
                    "hub status unreachable".to_string(),
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

fn lost_ack_client(
    db: &Arc<Database>,
    broker: &InProcessBroker,
    node_id: &str,
    confirmable: bool,
) -> SyncClient {
    SyncClient::with_transport(
        db.clone(),
        Arc::new(LoseAckAfterApply {
            inner: broker.client_as(node_id),
            push_subject: push_subject(TENANT),
            status_subject: status_subject(TENANT),
            confirmable,
        }),
        TenantId::from(TENANT),
    )
}

/// A push whose ack was lost but whose batch landed returns through
/// `finish_interrupted_push`, which reconciles against hub status and reports
/// the success it actually was — advancing both watermarks. That return happens
/// BEFORE the normal recording site, so the delivered batch was counted as
/// nothing: a confirmed-delivered push with a zero receipt. The bytes moved; the
/// receipt must say so.
#[tokio::test]
async fn c9_a_push_whose_ack_was_lost_but_landed_records_its_sent_receipt_once() {
    let (_clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);
    let edge = open_edge(None);
    insert_notes(&edge, 1..6);

    let client = lost_ack_client(&edge, &broker, "edge-a", true);
    let push = within(client.push()).await;

    assert!(
        push.is_ok(),
        "the fixture's premise: the batch landed and the edge reconciled to \
         success, got {push:?}"
    );
    assert_eq!(
        row_count(&hub.db, "notes"),
        5,
        "and the rows really are on the hub — this is a DELIVERED batch"
    );

    let receipts = client.transfer_receipts();
    let sent = receipt(
        &receipts,
        HUB_NODE,
        TransferPlane::Sync,
        TransferDirection::Sent,
    );
    assert_eq!(
        sent.counters.items, 5,
        "a batch that was confirmed delivered must be counted — reconciling \
         through the lost-ack path may not lose the receipt with the ack: \
         {receipts:?}"
    );
    assert!(
        sent.counters.payload_bytes > 0,
        "with the bytes that moved alongside: {receipts:?}"
    );
    assert_eq!(
        receipts
            .iter()
            .filter(|r| r.plane == TransferPlane::Sync && r.direction == TransferDirection::Sent)
            .count(),
        1,
        "and exactly ONE Sent receipt, not one per attempt: {receipts:?}"
    );

    // Recorded exactly once, and the WATERMARK is what enforces it — there is
    // no de-dup flag and none is asserted here. The confirmed branch advanced
    // and persisted the push watermark, so the batch can never re-enter a
    // changeset; the retry below therefore sends nothing to count.
    let after_first = sent.counters.clone();
    within(client.push())
        .await
        .expect("a retry after a reconciled push is a no-op");
    let after_retry = receipt(
        &client.transfer_receipts(),
        HUB_NODE,
        TransferPlane::Sync,
        TransferDirection::Sent,
    )
    .counters
    .clone();
    assert_eq!(
        after_retry, after_first,
        "a retry that discovers the batch already landed sends nothing, so it \
         must add nothing: {after_first:?} -> {after_retry:?}"
    );

    hub.stop().await;
}

/// The mirror: when the hub cannot be reached to confirm, the outcome is
/// genuinely unknown — the push surfaces the distinct unconfirmed error and the
/// watermark stays put. Nothing may be recorded, because "what moved" is exactly
/// what this end does not know.
#[tokio::test]
async fn c9_a_lost_ack_push_the_hub_cannot_confirm_records_nothing() {
    let (_clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);
    let edge = open_edge(None);
    insert_notes(&edge, 1..6);

    let client = lost_ack_client(&edge, &broker, "edge-a", false);
    let push = within(client.push()).await;

    assert!(
        matches!(push, Err(contextdb_core::Error::SyncPushUnconfirmed { .. })),
        "an unconfirmable lost-ack push must surface the distinct unconfirmed \
         outcome, got {push:?}"
    );
    assert_eq!(
        client.push_watermark(),
        contextdb_core::Lsn(0),
        "and must not advance the watermark"
    );
    assert!(
        client.transfer_receipts().is_empty(),
        "an unconfirmed transfer records nothing — the receipt would be a claim \
         this end cannot make: {:?}",
        client.transfer_receipts()
    );

    // The honest exactly-once form for this branch: nothing recorded, and the
    // watermark did not move, so the SAME backlog re-pushes over a healthy
    // transport and records once at the normal site. Across the whole sequence
    // the edge has recorded exactly ONE batch's worth.
    let healthy = edge_client(&edge, &broker, "edge-a");
    within(healthy.push()).await.expect("re-push the backlog");
    let recorded = receipt(
        &healthy.transfer_receipts(),
        HUB_NODE,
        TransferPlane::Sync,
        TransferDirection::Sent,
    )
    .counters
    .clone();
    assert_eq!(
        recorded.items, 5,
        "the re-push records the batch once, at the normal site: {recorded:?}"
    );
    assert!(
        client.transfer_receipts().is_empty(),
        "and the unconfirmed attempt still claims nothing, so the two together \
         total one batch — not two, not zero: {:?}",
        client.transfer_receipts()
    );

    hub.stop().await;
}
