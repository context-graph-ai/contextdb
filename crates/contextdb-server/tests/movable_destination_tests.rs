//! The server is movable — criteria C4a–C4d of
//! `contextdb-sync-policy-decoupling-criteria.md`, plus the guard the seed keeps:
//! one destination AT A TIME.
//!
//! An edge persists the first hub it pushes retained data to
//! (`database.rs:10441-10460`, armed on every push at `sync_client.rs:170-183`).
//! `Database::change_retention_sync_peer` moves that binding to another hub and,
//! in the same operation, clears what the previous hub confirmed. At the RED
//! (`dev` @ `7991099`) no such operation existed, so a second, different hub was
//! refused with `Error::SchemaInvalid` and every test died on the second bind or
//! on the confirmation record that was never cleared.
//!
//! Surface boundary: these are the DETERMINISTIC half of the proof. They drive
//! the destination change at the client/engine level, because a gate test cannot
//! reach the REPL — `contextdb-cli`'s `handle_sync_command` is private and the
//! CLI depends on the server, not the reverse. The operator-facing spelling
//! `.sync destination <server>` is proven by live smoke S4.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads. Time
//! moves through `Wallclock::test_clock_guard`; pruning is driven synchronously
//! on the test thread; every assertion is by VALUE, not by count alone.

use contextdb_core::{Lsn, TenantId, Value, Wallclock};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use contextdb_server::{InProcessBroker, SyncClient, SyncServer};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

const T0: u64 = 1_700_000_000_000;
const TENANT: &str = "movable";
/// Two hubs with DISTINCT transport-authenticated identities. The client dials
/// by key, so the edge always knows which one it is talking to; the in-process
/// broker presents the identity exactly as the p2p transport presents the
/// dialed endpoint's node id.
const HUB_ONE: &str = "hub-node-one";
const HUB_TWO: &str = "hub-node-two";
/// Retained + delivery-promising, declared with NO direction word so this file
/// does not depend on the new direction clause landing.
const RETAINED_DDL: &str = "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) \
     RETAIN 1 HOURS SYNC SAFE";

// ---------------------------------------------------------------------------
// THE OPERATION UNDER TEST
// ---------------------------------------------------------------------------

/// Change the retained-data destination of `edge` to `new_destination`, in ONE
/// operation that both rebinds the persisted peer AND clears what the previous
/// destination confirmed (persisted push watermark and the engine deletion-gate
/// watermark, which only ever moves forward through
/// `advance_engine_sync_watermark`).
///
/// This drives the shipped operation `Database::change_retention_sync_peer`.
/// The body was a documented NO-OP at the RED (no setter, clear, or command
/// existed at `dev` @ `7991099`, so every test below ran into the one-hub-forever
/// refusal); this is the pre-registered author-owned swap in its own `tests:`
/// commit, made once the public API landed — implementation commits never touch
/// test files. Two things the operation may not do, still pinned by the tests
/// below: make an ordinary push rebind implicitly (pinned by
/// `one_destination_at_a_time_the_previous_server_receives_nothing_new`, which
/// still relies on the arm-on-push registration refusing a second hub), and
/// clear the confirmation record anywhere other than inside this one operation
/// (pinned by `c4b_...`, which drives the deletion gate back to `Lsn(0)`).
fn change_destination(edge: &Arc<Database>, new_destination: &str) {
    edge.change_retention_sync_peer(&TenantId::from(TENANT), new_destination)
        .expect("change destination");
}

// ---------------------------------------------------------------------------
// scaffolding
// ---------------------------------------------------------------------------

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), fut)
        .await
        .expect("bounded sync operation exceeded 60s")
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

fn start_hub_as(broker: &InProcessBroker, node_id: &str) -> RunningHub {
    let db = Arc::new(Database::open_memory());
    db.execute(RETAINED_DDL, &p()).expect("hub retained table");
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
    RunningHub { db, shutdown, task }
}

fn open_edge(path: Option<&std::path::Path>) -> Arc<Database> {
    let db = match path {
        Some(path) => Arc::new(Database::open(path).expect("open edge db")),
        None => Arc::new(Database::open_memory()),
    };
    if db.table_meta("windows").is_none() {
        db.execute(RETAINED_DDL, &p()).expect("edge retained table");
    }
    db
}

fn edge_client(db: &Arc<Database>, broker: &InProcessBroker) -> SyncClient {
    SyncClient::with_transport(
        db.clone(),
        broker.client_as("edge-a"),
        TenantId::from(TENANT),
    )
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

/// Every row's VALUE, so convergence and loss are judged on content and never on
/// a row count that a silent overwrite would satisfy.
fn bodies(db: &Database) -> BTreeSet<String> {
    let result = db
        .execute("SELECT * FROM windows", &p())
        .expect("windows scan");
    let body_col = result
        .columns
        .iter()
        .position(|column| column == "body")
        .expect("the windows table has a body column");
    result
        .rows
        .into_iter()
        .map(|row| match row.into_iter().nth(body_col) {
            Some(Value::Text(body)) => body,
            other => panic!("every window row carries a text body, got {other:?}"),
        })
        .collect()
}

fn expected(ids: std::ops::Range<i64>) -> BTreeSet<String> {
    ids.map(|id| format!("window-{id}")).collect()
}

// ---------------------------------------------------------------------------
// C4a — the destination changes, any number of times, both directions, and
// every rebind survives an edge restart
// ---------------------------------------------------------------------------

/// RED today at the FIRST repoint: `client.push()` against hub two returns
/// `Error::SchemaInvalid` ("already established with hub hub-node-one, so hub
/// hub-node-two is refused") because `change_destination` is unshipped.
#[tokio::test]
async fn c4a_the_destination_moves_both_ways_and_each_binding_survives_a_restart() {
    let (_clock, _guard) = MockClock::install(T0);
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("edge.db");

    let broker_one = InProcessBroker::new();
    let hub_one = start_hub_as(&broker_one, HUB_ONE);
    let broker_two = InProcessBroker::new();
    let hub_two = start_hub_as(&broker_two, HUB_TWO);

    // Bind to hub one by delivering to it, exactly as an edge does in the field.
    {
        let edge = open_edge(Some(&path));
        insert_windows(&edge, 1..4);
        let client = edge_client(&edge, &broker_one);
        within(client.push()).await.expect("first delivery");
        assert_eq!(
            edge.retention_sync_peer().as_deref(),
            Some(HUB_ONE),
            "the first delivery binds the edge to hub one"
        );
    }
    // Restart: the binding is read back from disk, not from process memory.
    {
        let edge = open_edge(Some(&path));
        assert_eq!(
            edge.retention_sync_peer().as_deref(),
            Some(HUB_ONE),
            "the binding survives a restart"
        );
    }

    // Move it: hub one -> hub two -> hub one -> hub two. Three changes prove
    // "any number of times, both directions"; each is followed by a restart, so
    // a change that lived only in memory fails here.
    for (round, (destination, broker)) in [
        (HUB_TWO, &broker_two),
        (HUB_ONE, &broker_one),
        (HUB_TWO, &broker_two),
    ]
    .into_iter()
    .enumerate()
    {
        {
            let edge = open_edge(Some(&path));
            change_destination(&edge, destination);
            assert_eq!(
                edge.retention_sync_peer().as_deref(),
                Some(destination),
                "round {round}: the shipped operation rebinds the edge to {destination}"
            );
            let client = edge_client(&edge, broker);
            insert_windows(&edge, (100 + round as i64 * 10)..(103 + round as i64 * 10));
            within(client.push())
                .await
                .unwrap_or_else(|err| panic!("round {round}: delivery to {destination}: {err}"));
            assert_eq!(
                edge.retention_sync_peer().as_deref(),
                Some(destination),
                "round {round}: delivering to the new destination keeps the new binding"
            );
        }
        let edge = open_edge(Some(&path));
        assert_eq!(
            edge.retention_sync_peer().as_deref(),
            Some(destination),
            "round {round}: the new binding survives a restart"
        );
    }

    hub_one.stop().await;
    hub_two.stop().await;
}

// ---------------------------------------------------------------------------
// C4b — the change clears what the PREVIOUS destination confirmed
// ---------------------------------------------------------------------------

/// RED today twice over: the deletion gate keeps hub one's frontier, so the
/// prune deletes rows hub two has never seen, and the rows are gone by value.
#[tokio::test]
async fn c4b_repointing_clears_what_the_previous_destination_confirmed() {
    let (clock, _guard) = MockClock::install(T0);
    let broker_one = InProcessBroker::new();
    let hub_one = start_hub_as(&broker_one, HUB_ONE);
    let broker_two = InProcessBroker::new();
    let hub_two = start_hub_as(&broker_two, HUB_TWO);

    let edge = open_edge(None);
    insert_windows(&edge, 1..6);
    let first_client = edge_client(&edge, &broker_one);
    within(first_client.push()).await.expect("first delivery");
    assert_eq!(
        bodies(&hub_one.db),
        expected(1..6),
        "hub one really confirmed these rows"
    );
    assert!(
        edge.sync_watermark() > Lsn(0),
        "hub one's confirmation opened the deletion gate"
    );

    change_destination(&edge, HUB_TWO);

    // Retention runs BEFORE the new destination has confirmed anything.
    clock.advance(48 * 60 * 60 * 1000);
    let report = edge.run_pruning_cycle_checked().expect("prune");
    assert_eq!(
        report.pruned_rows, 0,
        "not one row may be deleted while the new destination holds none of them: {report:?}"
    );
    assert_eq!(
        bodies(&edge),
        expected(1..6),
        "and the rows are still there by value"
    );
    assert_eq!(
        bodies(&hub_two.db),
        BTreeSet::<String>::new(),
        "the new destination is genuinely empty at this point"
    );
    assert_eq!(
        edge.sync_watermark(),
        Lsn(0),
        "and the mechanism behind that: the SAME operation drove the deletion \
         gate back — the forward-only advance path cannot express this, so \
         nothing else can have done it"
    );

    // Only the NEW destination's confirmation reopens the gate.
    let second_client = edge_client(&edge, &broker_two);
    within(second_client.push())
        .await
        .expect("delivery to the new destination");
    assert_eq!(
        bodies(&hub_two.db),
        expected(1..6),
        "the new destination now holds them"
    );
    let report = edge
        .run_pruning_cycle_checked()
        .expect("prune after confirm");
    assert_eq!(
        report.pruned_rows, 5,
        "confirmed-and-expired rows prune once the NEW destination holds them: {report:?}"
    );

    hub_one.stop().await;
    hub_two.stop().await;
}

// ---------------------------------------------------------------------------
// C4c — everything the edge holds arrives at a new empty destination
// ---------------------------------------------------------------------------

/// RED today at the push to hub two (`SchemaInvalid`); with the operation
/// shipped but the confirmation record left in place, the rows written before
/// hub one's confirmation would silently stay behind — which is what the
/// pre-confirmation half of this assertion catches.
#[tokio::test]
async fn c4c_a_new_empty_destination_receives_everything_including_pre_confirmation_rows() {
    let (_clock, _guard) = MockClock::install(T0);
    let broker_one = InProcessBroker::new();
    let hub_one = start_hub_as(&broker_one, HUB_ONE);
    let broker_two = InProcessBroker::new();
    let hub_two = start_hub_as(&broker_two, HUB_TWO);

    let edge = open_edge(None);
    insert_windows(&edge, 1..6);
    let first_client = edge_client(&edge, &broker_one);
    within(first_client.push()).await.expect("first delivery");
    // Written after that confirmation, so the two age classes are both present.
    insert_windows(&edge, 6..9);

    change_destination(&edge, HUB_TWO);
    let second_client = edge_client(&edge, &broker_two);
    within(second_client.push())
        .await
        .expect("delivery to the new empty destination");

    assert_eq!(
        bodies(&hub_two.db),
        expected(1..9),
        "the new destination receives EVERYTHING the edge holds, including the \
         rows the previous destination had already confirmed"
    );
    assert_eq!(
        bodies(&edge),
        expected(1..9),
        "and the edge lost nothing in the move"
    );

    hub_one.stop().await;
    hub_two.stop().await;
}

// ---------------------------------------------------------------------------
// C4d — point back, converge, repeat, lose nothing
// ---------------------------------------------------------------------------

/// RED today at the first repoint. The cycle runs twice, so a change that works
/// once and leaves a binding behind fails on the repeat.
#[tokio::test]
async fn c4d_pointing_back_converges_with_no_binding_left_behind_and_repeats() {
    let (_clock, _guard) = MockClock::install(T0);
    let broker_one = InProcessBroker::new();
    let hub_one = start_hub_as(&broker_one, HUB_ONE);
    let broker_two = InProcessBroker::new();
    let hub_two = start_hub_as(&broker_two, HUB_TWO);

    let edge = open_edge(None);
    insert_windows(&edge, 1..4);
    within(edge_client(&edge, &broker_one).push())
        .await
        .expect("first delivery to hub one");

    let mut next_id = 10;
    for round in 0..2 {
        // Away to hub two.
        change_destination(&edge, HUB_TWO);
        insert_windows(&edge, next_id..next_id + 3);
        next_id += 3;
        within(edge_client(&edge, &broker_two).push())
            .await
            .unwrap_or_else(|err| panic!("round {round}: delivery to hub two: {err}"));
        let held = bodies(&edge);
        assert_eq!(
            bodies(&hub_two.db),
            held,
            "round {round}: hub two converges on everything the edge holds"
        );

        // Back to hub one, which still holds its own older copy.
        change_destination(&edge, HUB_ONE);
        insert_windows(&edge, next_id..next_id + 3);
        next_id += 3;
        within(edge_client(&edge, &broker_one).push())
            .await
            .unwrap_or_else(|err| panic!("round {round}: delivery back to hub one: {err}"));
        let held_now = bodies(&edge);
        assert!(
            held.is_subset(&held_now),
            "round {round}: the edge lost rows across the move back: {held:?} -> {held_now:?}"
        );
        assert_eq!(
            bodies(&hub_one.db),
            held_now,
            "round {round}: hub one converges on everything the edge holds, \
             including the rows it never saw while the edge was pointed elsewhere"
        );
        assert_eq!(
            edge.retention_sync_peer().as_deref(),
            Some(HUB_ONE),
            "round {round}: no binding from the other destination is left behind"
        );
    }

    assert_eq!(
        bodies(&edge),
        expected(1..4)
            .union(&expected(10..22))
            .cloned()
            .collect::<BTreeSet<_>>(),
        "every row written across the whole cycle is still on the edge, by value"
    );

    hub_one.stop().await;
    hub_two.stop().await;
}

// ---------------------------------------------------------------------------
// The guard the seed keeps: ONE destination at a time
// ---------------------------------------------------------------------------

/// Moving the destination must not turn into having two. RED today at the hub
/// two assertion — the destination never moved, so the delivery is refused.
#[tokio::test]
async fn one_destination_at_a_time_the_previous_server_receives_nothing_new() {
    let (_clock, _guard) = MockClock::install(T0);
    let broker_one = InProcessBroker::new();
    let hub_one = start_hub_as(&broker_one, HUB_ONE);
    let broker_two = InProcessBroker::new();
    let hub_two = start_hub_as(&broker_two, HUB_TWO);

    let edge = open_edge(None);
    insert_windows(&edge, 1..4);
    let first_client = edge_client(&edge, &broker_one);
    within(first_client.push()).await.expect("first delivery");
    let hub_one_at_change = bodies(&hub_one.db);

    change_destination(&edge, HUB_TWO);
    insert_windows(&edge, 200..203);
    let second_client = edge_client(&edge, &broker_two);
    within(second_client.push())
        .await
        .expect("delivery to the new destination");
    assert!(
        bodies(&hub_two.db).is_superset(&expected(200..203)),
        "the rows written after the change go to the new destination"
    );

    // The old client is still holding a live connection to hub one. Whether the
    // attempt is refused or simply carries nothing, hub one is no longer a
    // destination: it receives NOTHING written after the change.
    insert_windows(&edge, 300..303);
    let _ = within(first_client.push()).await;
    assert_eq!(
        bodies(&hub_one.db),
        hub_one_at_change,
        "the previous destination received nothing new after the change — \
         an edge delivers retained data to ONE destination at a time"
    );
    assert_eq!(
        edge.retention_sync_peer().as_deref(),
        Some(HUB_TWO),
        "and the binding still names only the current destination"
    );

    hub_one.stop().await;
    hub_two.stop().await;
}

// ---------------------------------------------------------------------------
// FIX C — a destination change that fails mid-operation lands on the safe side
// ---------------------------------------------------------------------------

/// The in-window failure c4b does not reach: c4b prunes only AFTER a clean
/// `change_destination` returns. Here the operation is interrupted by an I/O
/// failure between clearing the confirmation record and finishing the rebind.
/// Pre-fix the destination is rebound to the new (empty) hub while the deletion
/// gate still holds the OLD frontier, and the client's cached watermark is left
/// at the old value, so a prune deletes SYNC SAFE rows the new hub has never
/// received. Every observable must land on the safe side: gate closed, binding
/// unchanged, cached frontier reset, nothing pruned.
#[tokio::test]
async fn c4_a_failed_destination_change_leaves_the_edge_on_the_safe_side() {
    let (clock, _guard) = MockClock::install(T0);
    let broker_one = InProcessBroker::new();
    let hub_one = start_hub_as(&broker_one, HUB_ONE);
    let _broker_two = InProcessBroker::new();

    let edge = open_edge(None);
    insert_windows(&edge, 1..6);
    let client = edge_client(&edge, &broker_one);
    within(client.push())
        .await
        .expect("first delivery to hub one");
    assert_eq!(
        bodies(&hub_one.db),
        expected(1..6),
        "hub one really confirmed these rows",
    );
    assert!(
        edge.sync_watermark() > Lsn(0),
        "hub one's confirmation opened the deletion gate",
    );
    assert!(
        client.push_watermark() > Lsn(0),
        "and the client cached that confirmed frontier",
    );

    // Arm the one-shot injection so the destination change fails after it clears
    // the confirmation record, then attempt to move to hub two through the client.
    edge.__arm_retention_peer_persist_fault_for_test();
    let outcome = client.change_destination(HUB_TWO);
    assert!(
        outcome.is_err(),
        "the injected failure must surface as an error, got {outcome:?}",
    );

    // Safe side #1: the deletion gate was not left at the old frontier.
    assert_eq!(
        edge.sync_watermark(),
        Lsn(0),
        "a failed change must not leave the gate open at the old frontier",
    );
    // Safe side #2: the binding did not move to the new, empty hub.
    assert_eq!(
        edge.retention_sync_peer().as_deref(),
        Some(HUB_ONE),
        "a failed change must not rebind the edge to a hub that holds nothing",
    );
    // Safe side #3: the client's cached frontier was reset in lockstep, so the
    // next push cannot skip rows a new destination never received.
    assert_eq!(
        client.push_watermark(),
        Lsn(0),
        "the cached push frontier must not be stranded at the old value",
    );

    // Retention runs after the failed change: not one SYNC SAFE row may go.
    clock.advance(48 * 60 * 60 * 1000);
    let report = edge.run_pruning_cycle_checked().expect("prune");
    assert_eq!(
        report.pruned_rows, 0,
        "no SYNC SAFE row may be deleted after a failed destination change: {report:?}",
    );
    assert_eq!(
        bodies(&edge),
        expected(1..6),
        "and the rows are all still there by value",
    );

    hub_one.stop().await;
}
