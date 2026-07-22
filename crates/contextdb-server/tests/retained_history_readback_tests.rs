//! Retained history reads back from the server.
//!
//! The contract these pin: a retained table declared `SYNC TWO WAY` is served
//! back to edges, minus the rows whose retention window has already passed, so
//! a wiped machine recovers its still-live history and an ordinary edge reads
//! rows other machines wrote. `SYNC PUSH ONLY` keeps the never-sent-back
//! behaviour — as a consequence of the declared direction, never of `RETAIN`.
//!
//! Before this change, two layers fail: the direction clause `SYNC TWO WAY`
//! does not parse (the direction-clause grammar lands it), and beneath that the
//! hub drops every retained table's rows from the wire
//! (`sync_server.rs` `handle_pull` → `drop_push_only_retained_rows`) with no
//! serve-time expiry filter anywhere.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads. Time
//! moves through `Wallclock::test_clock_guard`; pruning is driven synchronously
//! on the test thread. Every history assertion is by VALUE — the expired row's
//! body differs from every live row's, so no value assertion can pass by
//! accident on a count.

use contextdb_core::{Lsn, TenantId, Value, Wallclock};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{
    ChangeSet, ConflictPolicies, ConflictPolicy, NaturalKey, RowChange,
};
use contextdb_server::protocol::{MessageType, PullRequest, PullResponse, decode, encode};
use contextdb_server::subjects::pull_subject;
use contextdb_server::{InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// Bounded wrapper so a red state can never hang the suite.
async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), fut)
        .await
        .expect("sync operation must complete within 60s")
}

const T0: u64 = 1_700_000_000_000;
const TENANT: &str = "readback";
const HUB_NODE: &str = "hub-node-a";
const MINUTE: u64 = 60 * 1000;

/// The retained table this item exists for: history that ages out on a window
/// AND is readable back from the server — the recovery/dashboard shape.
const TWO_WAY_DDL: &str = "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) \
     RETAIN 1 HOURS SYNC SAFE SYNC TWO WAY";
/// The same retention window, opposite declared direction: never sent back.
const PUSH_ONLY_DDL: &str = "CREATE TABLE outbox (id INTEGER PRIMARY KEY, body TEXT) \
     RETAIN 1 HOURS SYNC SAFE SYNC PUSH ONLY";
/// A retained two-way table with NO `SYNC SAFE`. `RETAIN` bounds when its rows
/// expire; `SYNC TWO WAY` alone says they are served back. Serve-back must
/// follow from the declared direction, never from a delivery-safety promise —
/// a serve path gated on `meta.sync_safe && direction == Both` would strand
/// exactly this table, so it gets its own by-value readback proof.
const PLAIN_TWO_WAY_DDL: &str = "CREATE TABLE ledger (id INTEGER PRIMARY KEY, body TEXT) \
     RETAIN 1 HOURS SYNC TWO WAY";
/// Non-retained control: proves the pull path itself worked in every test that
/// asserts a retained table is absent.
const CONTROL_DDL: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)";

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

/// Every history assertion goes through this: the sorted `body` values a
/// database actually holds, read through the ordinary local query surface.
fn bodies(db: &Database, table: &str) -> Vec<String> {
    let result = db
        .execute(&format!("SELECT * FROM {table}"), &p())
        .unwrap_or_else(|err| panic!("{table} scan must succeed: {err}"));
    let body_index = result
        .columns
        .iter()
        .position(|column| column == "body")
        .unwrap_or_else(|| panic!("{table} must expose a body column: {:?}", result.columns));
    let mut out: Vec<String> = result
        .rows
        .iter()
        .map(|row| match row.get(body_index) {
            Some(Value::Text(text)) => text.clone(),
            other => panic!("{table} body must be text, got {other:?}"),
        })
        .collect();
    out.sort();
    out
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

fn insert(db: &Database, table: &str, id: i64, body: &str) {
    let mut row = p();
    row.insert("id".to_string(), Value::Int64(id));
    row.insert("body".to_string(), Value::Text(body.to_string()));
    db.execute(
        &format!("INSERT INTO {table} (id, body) VALUES ($id, $body)"),
        &row,
    )
    .unwrap_or_else(|err| panic!("{table} insert must succeed: {err}"));
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

/// The shared schema for the readback scenarios: the SYNC SAFE two-way and
/// push-only retained tables plus the control. The plain-two-way proof does
/// NOT use this — it must run against a schema with no SYNC SAFE table anywhere
/// (see `declare_plain_only`).
fn declare_tables(db: &Database) {
    db.execute(TWO_WAY_DDL, &p())
        .expect("a retained SYNC TWO WAY table must be declarable");
    db.execute(PUSH_ONLY_DDL, &p())
        .expect("a retained SYNC PUSH ONLY table must be declarable");
    db.execute(CONTROL_DDL, &p()).expect("control table");
}

/// The isolated schema for the plain-two-way proof: ONE retained `SYNC TWO WAY` table with
/// no `SYNC SAFE`, plus a non-retained control — and nothing else. No `SYNC
/// SAFE` table exists on this hub/edge pair, so an implementation that flips
/// retained readback on globally the moment ANY table is `SYNC SAFE` cannot
/// pass this test on the back of the shared fixture's safe tables.
fn declare_plain_only(db: &Database) {
    db.execute(PLAIN_TWO_WAY_DDL, &p())
        .expect("a retained SYNC TWO WAY table without SYNC SAFE must be declarable");
    db.execute(CONTROL_DDL, &p()).expect("control table");
}

fn start_hub_with(broker: &InProcessBroker, declare: fn(&Database)) -> RunningHub {
    let db = Arc::new(Database::open_memory());
    declare(&db);
    let server = Arc::new(SyncServer::with_transport(
        db.clone(),
        broker.server_as(HUB_NODE),
        TenantId::from(TENANT),
        ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    ));
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    RunningHub { db, shutdown, task }
}

fn start_hub(broker: &InProcessBroker) -> RunningHub {
    start_hub_with(broker, declare_tables)
}

/// A machine: its own database, its own declared schema. A "wiped and
/// recreated" machine is simply a fresh one against the same tenant.
fn open_edge_with(declare: fn(&Database)) -> Arc<Database> {
    let db = Arc::new(Database::open_memory());
    declare(&db);
    db
}

fn open_edge() -> Arc<Database> {
    open_edge_with(declare_tables)
}

fn edge_client(db: &Arc<Database>, broker: &InProcessBroker, node_id: &str) -> SyncClient {
    SyncClient::with_transport(
        db.clone(),
        broker.client_as(node_id),
        TenantId::from(TENANT),
    )
}

// ---------------------------------------------------------------------------
// A SYNC TWO WAY retained table's rows are served back; SYNC PUSH ONLY
//       keeps never-sent-back
// ---------------------------------------------------------------------------

#[tokio::test]
async fn c2a_a_two_way_retained_table_is_served_back_and_a_push_only_one_is_not() {
    let (_clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);

    let edge_a = open_edge();
    insert(&edge_a, "windows", 1, "two-way-alpha");
    insert(&edge_a, "windows", 2, "two-way-beta");
    insert(&edge_a, "outbox", 1, "push-only-alpha");
    insert(&edge_a, "notes", 1, "control-alpha");
    let client_a = edge_client(&edge_a, &broker, "edge-a");
    within(client_a.push()).await.expect("edge-a push");

    assert_eq!(
        bodies(&hub.db, "windows"),
        vec!["two-way-alpha", "two-way-beta"],
        "the hub genuinely holds the two-way retained rows — this is not an empty-hub tautology"
    );
    assert_eq!(
        bodies(&hub.db, "outbox"),
        vec!["push-only-alpha"],
        "and it holds the push-only retained row too"
    );

    let edge_b = open_edge();
    let client_b = edge_client(&edge_b, &broker, "edge-b");
    within(client_b.pull_default()).await.expect("edge-b pull");

    assert_eq!(
        bodies(&edge_b, "notes"),
        vec!["control-alpha"],
        "the ordinary table proves the pull path itself worked"
    );
    assert_eq!(
        bodies(&edge_b, "windows"),
        vec!["two-way-alpha", "two-way-beta"],
        "a SYNC TWO WAY retained table's rows are served back to an edge, by value"
    );
    assert_eq!(
        bodies(&edge_b, "outbox"),
        Vec::<String>::new(),
        "a SYNC PUSH ONLY retained table is still never sent back — direction says so, not RETAIN"
    );

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// Every retained row carries a creation time; a restored row ages from
//       the ORIGINAL one
// ---------------------------------------------------------------------------

/// A peer that records no creation stamp still gets one here, so the row
/// expires on this node's schedule instead of living forever: `row_is_prunable`
/// treats a stamp-less row as never-prunable, so "prunes exactly when the
/// window passes" is the observable proof that a stamp was written at insert.
///
/// Driven on the test thread through the same public apply path the sync client
/// uses, deliberately: the hub applies on its own task thread, where the
/// thread-local test clock does not reach, so a hub-applied row would be stamped
/// from the real clock and the schedule assertion below would mean nothing. The
/// rule under test is an engine rule about the insert, not about the transport.
#[test]
fn c2b_a_retained_row_that_arrives_without_a_creation_stamp_is_stamped_at_insert() {
    let (clock, _guard) = MockClock::install(T0);
    let db = Database::open_memory();
    declare_tables(&db);

    let mut values = HashMap::new();
    values.insert("id".to_string(), Value::Int64(7));
    values.insert(
        "body".to_string(),
        Value::Text("stampless-arrival".to_string()),
    );
    let changes = ChangeSet {
        rows: vec![RowChange {
            table: "windows".to_string(),
            natural_key: NaturalKey::single("id".to_string(), Value::Int64(7)),
            values,
            deleted: false,
            lsn: Lsn(1),
            created_at: None,
        }],
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: Vec::new(),
        ddl_lsn: Vec::new(),
    };
    let result = db
        .apply_changes(
            changes,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .expect("a row carrying no creation stamp must be accepted, not refused");
    assert_eq!(
        result.applied_rows, 1,
        "the stamp-less row landed: {result:?}"
    );
    assert_eq!(
        bodies(&db, "windows"),
        vec!["stampless-arrival"],
        "the receiving node holds the row the peer sent without a creation stamp"
    );

    // Half a window in: still live.
    clock.advance(30 * MINUTE);
    let report = db.run_pruning_cycle_checked().expect("prune");
    assert_eq!(
        report.pruned_rows, 0,
        "half a window in, nothing has expired: {report:?}"
    );
    assert_eq!(bodies(&db, "windows"), vec!["stampless-arrival"]);

    // Past the window: it must go. A row with no creation time never would.
    clock.advance(40 * MINUTE);
    let report = db.run_pruning_cycle_checked().expect("prune");
    assert_eq!(
        report.pruned_rows, 1,
        "past the window the stamped row expires on schedule; a stamp-less row would live \
         forever: {report:?}"
    );
    assert_eq!(bodies(&db, "windows"), Vec::<String>::new());
}

/// The row is 50 minutes old when it is restored onto a wiped machine. It must
/// die 10 minutes later, on its ORIGINAL birth time — not 60 minutes later, on
/// a window restarted at arrival.
#[tokio::test]
async fn c2b_a_restored_row_ages_from_its_original_creation_time_not_from_arrival() {
    let (clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);

    let edge_a = open_edge();
    insert(&edge_a, "windows", 1, "born-at-t0");
    let client_a = edge_client(&edge_a, &broker, "edge-a");
    within(client_a.push()).await.expect("edge-a push");

    // The machine is wiped and recreated 50 minutes later; the row is still
    // live, so it comes back.
    clock.advance(50 * MINUTE);
    let restored = open_edge();
    let restored_client = edge_client(&restored, &broker, "edge-a-restored");
    within(restored_client.pull_default())
        .await
        .expect("restore pull");
    assert_eq!(
        bodies(&restored, "windows"),
        vec!["born-at-t0"],
        "a still-live row returns to the recreated machine"
    );

    // 65 minutes after the row was WRITTEN, 15 after it arrived here.
    clock.advance(15 * MINUTE);
    let report = restored
        .run_pruning_cycle_checked()
        .expect("restored machine prune");
    assert_eq!(
        report.pruned_rows, 1,
        "the restored row ages from its original creation time, so 65 minutes after it was \
         written it expires; a row re-stamped on arrival would still have 45 minutes to live: \
         {report:?}"
    );
    assert_eq!(bodies(&restored, "windows"), Vec::<String>::new());

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// The expiry filter is for row content only: deletes still travel
// ---------------------------------------------------------------------------

#[tokio::test]
async fn c2c_a_delete_on_a_two_way_retained_table_still_reaches_the_other_machine() {
    let (_clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);

    let edge_a = open_edge();
    insert(&edge_a, "windows", 1, "kept-one");
    insert(&edge_a, "windows", 2, "deleted-later");
    insert(&edge_a, "windows", 3, "kept-three");
    let client_a = edge_client(&edge_a, &broker, "edge-a");
    within(client_a.push()).await.expect("edge-a push");

    let edge_b = open_edge();
    let client_b = edge_client(&edge_b, &broker, "edge-b");
    within(client_b.pull_default()).await.expect("edge-b pull");
    assert_eq!(
        bodies(&edge_b, "windows"),
        vec!["deleted-later", "kept-one", "kept-three"],
        "edge-b holds the history before the delete"
    );

    edge_a
        .execute("DELETE FROM windows WHERE id = 2", &p())
        .expect("delete on edge-a");
    within(client_a.push()).await.expect("edge-a push delete");
    assert_eq!(
        bodies(&hub.db, "windows"),
        vec!["kept-one", "kept-three"],
        "the delete reached the hub"
    );

    within(client_b.pull_default())
        .await
        .expect("edge-b pull delete");
    assert_eq!(
        bodies(&edge_b, "windows"),
        vec!["kept-one", "kept-three"],
        "a row deleted on one machine disappears on the other: the expiry filter applies to row \
         content only and must never eat a delete record"
    );

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// RECOVERY: a wiped machine gets still-live rows back and never the
//       expired ones, with the server holding both at the moment of the read
// ---------------------------------------------------------------------------

#[tokio::test]
async fn c2d_a_wiped_machine_recovers_live_rows_and_provably_not_expired_ones() {
    let (clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);

    let edge_a = open_edge();
    insert(&edge_a, "windows", 1, "expired-window-one");
    clock.advance(50 * MINUTE);
    insert(&edge_a, "windows", 2, "live-window-alpha");
    insert(&edge_a, "windows", 3, "live-window-beta");
    insert(&edge_a, "notes", 1, "control-note");
    let client_a = edge_client(&edge_a, &broker, "edge-a");
    within(client_a.push()).await.expect("edge-a push");

    // 65 minutes after the first row was written, 15 after the other two.
    clock.advance(15 * MINUTE);
    assert_eq!(
        bodies(&hub.db, "windows"),
        vec![
            "expired-window-one",
            "live-window-alpha",
            "live-window-beta"
        ],
        "the server still HOLDS the expired row at the moment of the read — the exclusion below \
         is serve-time filtering, not a server that pruned first"
    );

    let restored = open_edge();
    let restored_client = edge_client(&restored, &broker, "edge-a-restored");
    within(restored_client.pull_default())
        .await
        .expect("recovery pull");

    assert_eq!(
        bodies(&restored, "notes"),
        vec!["control-note"],
        "the recovery genuinely restored what it should — control"
    );
    assert_eq!(
        bodies(&restored, "windows"),
        vec!["live-window-alpha", "live-window-beta"],
        "a wiped machine recreated against the same tenant gets every still-live row back, by \
         value, and the row whose window has passed provably does not return"
    );

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// ORDINARY READ: a machine reads rows another machine wrote and it never
//       held, no wipe involved
// ---------------------------------------------------------------------------

#[tokio::test]
async fn c2e_a_machine_reads_rows_another_machine_wrote_and_it_never_held() {
    let (_clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);

    let edge_a = open_edge();
    insert(&edge_a, "windows", 1, "written-on-a-one");
    insert(&edge_a, "windows", 2, "written-on-a-two");
    let client_a = edge_client(&edge_a, &broker, "edge-a");
    within(client_a.push()).await.expect("edge-a push");

    // A second machine that has never held these rows and is not recovering
    // from anything — the ordinary dashboard read.
    let edge_b = open_edge();
    assert_eq!(
        bodies(&edge_b, "windows"),
        Vec::<String>::new(),
        "edge-b starts having never held a single one of them"
    );
    let client_b = edge_client(&edge_b, &broker, "edge-b");
    within(client_b.pull_default()).await.expect("edge-b pull");

    assert_eq!(
        bodies(&edge_b, "windows"),
        vec!["written-on-a-one", "written-on-a-two"],
        "the ordinary two-way read returns rows another machine wrote, by value"
    );

    // Edge B writes its own row; edge A reads it back the same way. Neither
    // machine is privileged and nothing here involves a wipe.
    insert(&edge_b, "windows", 3, "written-on-b-three");
    within(client_b.push()).await.expect("edge-b push");
    within(client_a.pull_default()).await.expect("edge-a pull");
    assert_eq!(
        bodies(&edge_a, "windows"),
        vec!["written-on-a-one", "written-on-a-two", "written-on-b-three"],
        "and the read works in the other direction too"
    );

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// Serve-back follows from SYNC TWO WAY alone, not from SYNC SAFE
// ---------------------------------------------------------------------------

/// The whole shared fixture above declares `SYNC SAFE` on its two-way table,
/// so an implementation that served rows only when `meta.sync_safe && direction
/// == Both` would pass every one of those tests while plain retained two-way
/// history stayed unreadable. This proves readback AND delete propagation by
/// value on a retained `SYNC TWO WAY` table that carries NO `SYNC SAFE`, so the
/// serve path cannot be coupled to the delivery-safety promise.
///
/// The hub/edge pair here declares an ISOLATED schema (`declare_plain_only`):
/// the plain two-way `ledger` and a non-retained control, with no `SYNC SAFE`
/// table anywhere. So an implementation that flips retained readback on globally
/// the instant ANY table on the node is `SYNC SAFE` cannot ride the shared
/// fixture's safe tables to pass this — this node has none.
#[tokio::test]
async fn c2a_a_retained_two_way_table_without_sync_safe_is_still_served_back() {
    let (_clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub_with(&broker, declare_plain_only);

    let edge_a = open_edge_with(declare_plain_only);
    insert(&edge_a, "ledger", 1, "ledger-alpha");
    insert(&edge_a, "ledger", 2, "ledger-beta");
    insert(&edge_a, "ledger", 3, "ledger-gamma");
    insert(&edge_a, "notes", 1, "control-alpha");
    let client_a = edge_client(&edge_a, &broker, "edge-a");
    within(client_a.push()).await.expect("edge-a push");

    assert_eq!(
        bodies(&hub.db, "ledger"),
        vec!["ledger-alpha", "ledger-beta", "ledger-gamma"],
        "the hub holds the plain two-way retained rows — not an empty-hub tautology"
    );

    let edge_b = open_edge_with(declare_plain_only);
    let client_b = edge_client(&edge_b, &broker, "edge-b");
    within(client_b.pull_default()).await.expect("edge-b pull");

    assert_eq!(
        bodies(&edge_b, "notes"),
        vec!["control-alpha"],
        "the ordinary table proves the pull path itself worked"
    );
    assert_eq!(
        bodies(&edge_b, "ledger"),
        vec!["ledger-alpha", "ledger-beta", "ledger-gamma"],
        "a retained SYNC TWO WAY table with NO SYNC SAFE is served back by value: serve-back \
         follows from the declared direction alone, never from the delivery-safety promise"
    );

    // Delete propagation on the same non-SYNC-SAFE table.
    edge_a
        .execute("DELETE FROM ledger WHERE id = 2", &p())
        .expect("delete on edge-a");
    within(client_a.push()).await.expect("edge-a push delete");
    assert_eq!(
        bodies(&hub.db, "ledger"),
        vec!["ledger-alpha", "ledger-gamma"],
        "the delete reached the hub"
    );
    within(client_b.pull_default())
        .await
        .expect("edge-b pull delete");
    assert_eq!(
        bodies(&edge_b, "ledger"),
        vec!["ledger-alpha", "ledger-gamma"],
        "a delete on a plain two-way retained table still reaches the other machine"
    );

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// The expiry exclusion is on the SERVE side, in the hub's response
//            bytes, not a client-side inbound drop
// ---------------------------------------------------------------------------

/// The wiped-machine recovery test above observes only the recovered local
/// database, which a wrong
/// implementation could satisfy by sending the expired row over the wire and
/// dropping it client-side before apply — and that inbound drop is exactly the
/// defect this change removes, so the test must not accept it as the fix. This
/// issues a RAW pull (the raw-pull pattern used in the stale-restore suite) and decodes the
/// hub's PullResponse directly: the expired row must be ABSENT from the hub's
/// response bytes while the live rows are present, proving the filter runs at
/// serve time. The hub's own pruning cycle is never run, so it still stores the
/// expired row the whole time — the exclusion is a serve filter, not a prune.
#[tokio::test]
async fn c2d_the_expired_row_is_excluded_from_the_hubs_pull_response_itself() {
    let (clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);

    let edge_a = open_edge();
    insert(&edge_a, "windows", 1, "expired-window-one");
    clock.advance(50 * MINUTE);
    insert(&edge_a, "windows", 2, "live-window-alpha");
    insert(&edge_a, "windows", 3, "live-window-beta");
    let client_a = edge_client(&edge_a, &broker, "edge-a");
    within(client_a.push()).await.expect("edge-a push");

    // 65 minutes after row 1 was written, 15 after the other two. The hub's own
    // pruning cycle is deliberately never driven.
    clock.advance(15 * MINUTE);
    assert_eq!(
        bodies(&hub.db, "windows"),
        vec![
            "expired-window-one",
            "live-window-alpha",
            "live-window-beta"
        ],
        "the hub still STORES the expired row at the moment of the read — the exclusion below is \
         a serve-time filter, never a hub that pruned first"
    );

    // Raw pull from the beginning, decoded straight off the wire.
    let request = PullRequest {
        since_lsn: Lsn(0),
        max_entries: None,
    };
    let encoded = encode(MessageType::PullRequest, &request).expect("encode pull request");
    let reply = within(broker.client_as("edge-reader").request(
        &pull_subject(TENANT),
        encoded,
        Duration::from_secs(10),
    ))
    .await
    .expect("the hub must answer the pull");
    let envelope = decode(&reply).expect("decode pull reply");
    let response: PullResponse =
        rmp_serde::from_slice(&envelope.payload).expect("decode pull response");

    // The bodies the HUB actually put on the wire for the windows table.
    let mut served: Vec<String> = response
        .changeset
        .rows
        .iter()
        .filter(|row| row.table == "windows" && !row.deleted)
        .map(|row| match row.values.get("body") {
            Some(Value::Text(text)) => text.clone(),
            other => panic!("served windows row must carry a text body, got {other:?}"),
        })
        .collect();
    served.sort();

    assert_eq!(
        served,
        vec!["live-window-alpha", "live-window-beta"],
        "the hub's own PullResponse carries the live rows by value and NOT the expired one: the \
         expiry exclusion runs at serve time, so a client-side inbound drop cannot be what \
         satisfies it"
    );
    assert!(
        !served.contains(&"expired-window-one".to_string()),
        "the expired row must never leave the hub"
    );
    assert!(
        response.cursor.is_some(),
        "a served pull still carries a cursor so the reader advances its watermark"
    );

    hub.stop().await;
}
