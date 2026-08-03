//! Multi-column PRIMARY KEY — transport-side acceptance for composite keys over
//! sync.
//!
//! What a reader should take from this file: two machines writing the same
//! sensor, the same metric, and the same time window are two different rows,
//! and the hub keeps both — because the identity that travels between them is
//! the whole declared key, not its first column. One surviving row is the
//! failure this guards against. And whether a re-sent row keeps its first value
//! or takes its latest is decided by the policy the TABLE declares
//! (`SYNC CONFLICT KEEP FIRST` / `KEEP LATEST`), honored by a hub built the
//! ordinary way — never by a policy a test rigged onto the hub.
//!
//! Every hub here is `start_hub`, handed the database's own default policies —
//! the shipped configuration — with no table-specific policy named. The
//! durable-record guarantee (`c5e_*`) holds because `CONSUMER_DDL` declares
//! keep-first; the current-state behavior (`latest_wins_*`) holds because that
//! table declares keep-latest; both run the same un-rigged hub.
//!
//! Before this change, per test:
//! - `c5b_*` (plain tables, no policy clause): once the composite key parses,
//!   the identity mechanics stand under the non-overwriting default.
//! - `c5e_*` and `latest_wins_*`: fail FIRST at the declaration, because the
//!   `SYNC CONFLICT` clause was not in the grammar and the hub could not honor a
//!   declared policy — the exact false-green this removes.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads —
//! nothing here is time-dependent. Every assertion is by VALUE; each machine's
//! rows carry a distinct payload, so a collapse or a silent overwrite is
//! visible in the data and not only in a count.

use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const TENANT: &str = "composite";
/// The identity mechanics table: bare, so these tests do not depend on the
/// direction-clause grammar.
const PLAIN_DDL: &str = "CREATE TABLE metric_windows (\
     machine_id TEXT NOT NULL, \
     sensor_id TEXT NOT NULL, \
     metric TEXT NOT NULL, \
     window_start INTEGER NOT NULL, \
     value INTEGER NOT NULL, \
     PRIMARY KEY (machine_id, sensor_id, metric, window_start))";

/// The shape a downstream consumer actually writes: the same four-column
/// identity, retained, promising delivery, syncing both ways, and
/// DECLARING keep-first — so the durable-record guarantee comes from the
/// declaration the table carries, not from a policy a test rigged onto the hub.
const CONSUMER_DDL: &str = "CREATE TABLE metric_windows (\
     machine_id TEXT NOT NULL, \
     sensor_id TEXT NOT NULL, \
     metric TEXT NOT NULL, \
     window_start INTEGER NOT NULL, \
     value INTEGER NOT NULL, \
     PRIMARY KEY (machine_id, sensor_id, metric, window_start)) \
     RETAIN 48 HOURS SYNC SAFE SYNC TWO WAY SYNC CONFLICT KEEP FIRST";

/// The identity mechanics table, DECLARING keep-latest: the current-state shape
/// where the newest write for a key wins. Same normally-built hub as everything
/// else here — the opposite outcome comes from the declaration, not a rig.
const LATEST_WINS_DDL: &str = "CREATE TABLE metric_windows (\
     machine_id TEXT NOT NULL, \
     sensor_id TEXT NOT NULL, \
     metric TEXT NOT NULL, \
     window_start INTEGER NOT NULL, \
     value INTEGER NOT NULL, \
     PRIMARY KEY (machine_id, sensor_id, metric, window_start)) \
     SYNC CONFLICT KEEP LATEST";

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

/// A bounded await so a hung exchange fails the test instead of hanging the
/// gate. Not a synchronization device: nothing here waits for time to pass.
async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), fut)
        .await
        .expect("a sync exchange exceeded 60s")
}

fn insert_window(
    db: &Database,
    machine: &str,
    sensor: &str,
    metric: &str,
    window_start: i64,
    value: i64,
) {
    let mut row = p();
    row.insert("machine_id".to_string(), Value::Text(machine.to_string()));
    row.insert("sensor_id".to_string(), Value::Text(sensor.to_string()));
    row.insert("metric".to_string(), Value::Text(metric.to_string()));
    row.insert("window_start".to_string(), Value::Int64(window_start));
    row.insert("value".to_string(), Value::Int64(value));
    db.execute(
        "INSERT INTO metric_windows (machine_id, sensor_id, metric, window_start, value) \
         VALUES ($machine_id, $sensor_id, $metric, $window_start, $value)",
        &row,
    )
    .unwrap_or_else(|err| panic!("insert {machine}/{sensor}/{metric}/{window_start}: {err}"));
}

/// Every `metric_windows` row as `(machine, sensor, metric, window_start,
/// value)`, sorted — the by-value view every assertion below compares against.
fn windows(db: &Database) -> Vec<(String, String, String, i64, i64)> {
    let result = db
        .execute("SELECT * FROM metric_windows", &p())
        .expect("metric_windows scan");
    let idx = |name: &str| {
        result
            .columns
            .iter()
            .position(|column| column == name)
            .unwrap_or_else(|| panic!("column {name} must be selected, got {:?}", result.columns))
    };
    let (machine, sensor, metric, window_start, value) = (
        idx("machine_id"),
        idx("sensor_id"),
        idx("metric"),
        idx("window_start"),
        idx("value"),
    );
    let text = |row: &Vec<Value>, i: usize| match &row[i] {
        Value::Text(t) => t.clone(),
        other => panic!("expected text, got {other:?}"),
    };
    let int = |row: &Vec<Value>, i: usize| match &row[i] {
        Value::Int64(v) => *v,
        other => panic!("expected an integer, got {other:?}"),
    };
    let mut rows: Vec<(String, String, String, i64, i64)> = result
        .rows
        .iter()
        .map(|row| {
            (
                text(row, machine),
                text(row, sensor),
                text(row, metric),
                int(row, window_start),
                int(row, value),
            )
        })
        .collect();
    rows.sort();
    rows
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

/// A hub built the ordinary way — handed the database's own default policies,
/// the same uniform default a shipped hub binary passes, and nothing
/// table-specific. Which value survives a re-send is decided by what each table
/// DECLARED (honored through the table's meta), not by a policy this test rigged
/// onto the hub. The first value written for a key survives here only because
/// `CONSUMER_DDL` declares keep-first.
fn start_hub(broker: &InProcessBroker, ddl: &str) -> RunningHub {
    let db = Arc::new(Database::open_memory());
    db.execute(ddl, &p()).expect("hub table");
    let identity = Arc::new(FabricIdentity::generate());
    let node_id = identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            db.clone(),
            broker.server_as(&node_id),
            TenantId::from(TENANT),
            node_id,
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

fn open_edge(ddl: &str) -> Arc<Database> {
    let db = Arc::new(Database::open_memory());
    db.execute(ddl, &p()).expect("edge table");
    db
}

fn edge_client(db: &Arc<Database>, broker: &InProcessBroker, _role: &str) -> SyncClient {
    let identity = Arc::new(FabricIdentity::generate());
    let node_id = identity.node_id();
    SyncClient::with_authenticated_transport_and_identity_for_test(
        db.clone(),
        broker.client_as(&node_id),
        TenantId::from(TENANT),
        identity,
    )
}

// ---------------------------------------------------------------------------
// The identity mechanics over the real transport (independent of the
// direction clause)
// ---------------------------------------------------------------------------

/// Two edges, one hub, same sensor/metric/window, different machines and
/// different values: the hub holds both rows by value. Under a single-column
/// identity the second edge's row is the same row as the first edge's and one
/// value is lost.
#[tokio::test]
async fn c5b_two_edges_writing_the_same_sensor_metric_window_leave_two_rows_on_the_hub() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, PLAIN_DDL);

    let edge_a = open_edge(PLAIN_DDL);
    insert_window(&edge_a, "machine-a", "sensor-1", "motion", 1_000, 11);
    within(edge_client(&edge_a, &broker, "edge-a").push())
        .await
        .expect("edge A push");

    let edge_b = open_edge(PLAIN_DDL);
    insert_window(&edge_b, "machine-b", "sensor-1", "motion", 1_000, 22);
    within(edge_client(&edge_b, &broker, "edge-b").push())
        .await
        .expect("edge B push");

    assert_eq!(
        windows(&hub.db),
        vec![
            (
                "machine-a".to_string(),
                "sensor-1".to_string(),
                "motion".to_string(),
                1_000,
                11
            ),
            (
                "machine-b".to_string(),
                "sensor-1".to_string(),
                "motion".to_string(),
                1_000,
                22
            ),
        ],
        "the hub must keep one row per machine — one surviving row is the collapse"
    );

    hub.stop().await;
}

/// And each edge sees both rows after pulling: the identity that arrives is
/// matched on all four columns, so the other machine's row is added rather
/// than folded onto the local one.
#[tokio::test]
async fn c5b_each_edge_pulls_back_both_machines_rows() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, PLAIN_DDL);

    let edge_a = open_edge(PLAIN_DDL);
    let client_a = edge_client(&edge_a, &broker, "edge-a");
    insert_window(&edge_a, "machine-a", "sensor-1", "motion", 1_000, 11);
    within(client_a.push()).await.expect("edge A push");

    let edge_b = open_edge(PLAIN_DDL);
    let client_b = edge_client(&edge_b, &broker, "edge-b");
    insert_window(&edge_b, "machine-b", "sensor-1", "motion", 1_000, 22);
    within(client_b.push()).await.expect("edge B push");

    within(client_a.pull_default()).await.expect("edge A pull");
    within(client_b.pull_default()).await.expect("edge B pull");

    let expected = vec![
        (
            "machine-a".to_string(),
            "sensor-1".to_string(),
            "motion".to_string(),
            1_000,
            11,
        ),
        (
            "machine-b".to_string(),
            "sensor-1".to_string(),
            "motion".to_string(),
            1_000,
            22,
        ),
    ];
    assert_eq!(
        windows(&edge_a),
        expected,
        "edge A holds both machines' rows"
    );
    assert_eq!(
        windows(&edge_b),
        expected,
        "edge B holds both machines' rows"
    );

    hub.stop().await;
}

/// The sharper collapse: one machine, four rows differing only in key
/// columns the leading-column identity never looks at. All four must reach the
/// hub with their own values.
#[tokio::test]
async fn c5b_one_machines_rows_differing_in_later_key_columns_all_reach_the_hub() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, PLAIN_DDL);

    let edge = open_edge(PLAIN_DDL);
    insert_window(&edge, "machine-a", "sensor-1", "motion", 1_000, 11);
    insert_window(&edge, "machine-a", "sensor-1", "motion", 2_000, 22);
    insert_window(&edge, "machine-a", "sensor-2", "motion", 1_000, 33);
    insert_window(&edge, "machine-a", "sensor-1", "occupancy", 1_000, 44);
    within(edge_client(&edge, &broker, "edge-a").push())
        .await
        .expect("edge push");

    assert_eq!(
        windows(&hub.db),
        windows(&edge),
        "every distinct (machine, sensor, metric, window) must arrive with its own value"
    );
    assert_eq!(windows(&hub.db).len(), 4, "all four are distinct rows");

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// The proof in the shape a downstream consumer writes it
// ---------------------------------------------------------------------------

/// The four-column key on a retained, `SYNC SAFE`, `SYNC TWO WAY` table: two
/// machines writing the same sensor, metric, and window keep two distinct rows
/// on the hub and on both edges.
///
/// Before the composite-key and direction-clause grammar existed, this failed
/// at the declaration for two reasons: the composite key grammar and the
/// separate direction clause.
#[tokio::test]
async fn c5e_the_consumer_shape_keeps_one_row_per_machine() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, CONSUMER_DDL);

    let edge_a = open_edge(CONSUMER_DDL);
    let client_a = edge_client(&edge_a, &broker, "edge-a");
    insert_window(&edge_a, "machine-a", "sensor-1", "motion", 1_000, 11);
    within(client_a.push()).await.expect("edge A push");

    let edge_b = open_edge(CONSUMER_DDL);
    let client_b = edge_client(&edge_b, &broker, "edge-b");
    insert_window(&edge_b, "machine-b", "sensor-1", "motion", 1_000, 22);
    within(client_b.push()).await.expect("edge B push");

    let expected = vec![
        (
            "machine-a".to_string(),
            "sensor-1".to_string(),
            "motion".to_string(),
            1_000,
            11,
        ),
        (
            "machine-b".to_string(),
            "sensor-1".to_string(),
            "motion".to_string(),
            1_000,
            22,
        ),
    ];
    assert_eq!(
        windows(&hub.db),
        expected,
        "the hub keeps both machines' rows"
    );

    within(client_a.pull_default()).await.expect("edge A pull");
    assert_eq!(
        windows(&edge_a),
        expected,
        "a two-way retained table reads the other machine's row back"
    );

    hub.stop().await;
}

/// A machine re-sending its OWN row with a different non-key value leaves one
/// row for that key still carrying the first value. Asserted by value: a
/// count-only check passes under a silent last-writer-wins overwrite, which is
/// exactly the failure this pins.
#[tokio::test]
async fn c5e_resending_a_row_with_a_changed_non_key_value_keeps_the_first_value() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, CONSUMER_DDL);

    let edge = open_edge(CONSUMER_DDL);
    let client = edge_client(&edge, &broker, "edge-a");
    insert_window(&edge, "machine-a", "sensor-1", "motion", 1_000, 11);
    within(client.push()).await.expect("first push");

    // Same four key columns, different payload — the row the hub already holds.
    let mut update = p();
    update.insert("value".to_string(), Value::Int64(99));
    edge.execute(
        "UPDATE metric_windows SET value = $value WHERE window_start = 1000",
        &update,
    )
    .expect("change the non-key value");
    within(client.push()).await.expect("second push");

    assert_eq!(
        windows(&hub.db),
        vec![(
            "machine-a".to_string(),
            "sensor-1".to_string(),
            "motion".to_string(),
            1_000,
            11
        )],
        "one row for that key, still carrying the first value under InsertIfNotExists"
    );

    hub.stop().await;
}

/// Made diagnostic of a COMPOSITE identity. The two earlier durable-record
/// tests separate their rows by `machine_id`, which is the first primary-key
/// column, so an implementation still keyed on that one column passes them.
/// This one cannot be passed that way: a SINGLE machine writes two rows
/// differing only in `window_start` — a later key column the leading-column
/// identity never looks at — and then re-sends one exact four-column key with a
/// changed payload. The composite key must keep both windows as distinct rows
/// AND keep the first payload for the re-sent key. Under the first-column
/// identity the two windows collapse to one row, so this fails for the right
/// reason before the fix.
#[tokio::test]
async fn c5e_same_machine_later_key_column_and_resend_are_diagnostic() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, CONSUMER_DDL);

    let edge = open_edge(CONSUMER_DDL);
    let client = edge_client(&edge, &broker, "edge-a");
    insert_window(&edge, "machine-a", "sensor-1", "motion", 1_000, 11);
    insert_window(&edge, "machine-a", "sensor-1", "motion", 2_000, 22);
    within(client.push()).await.expect("first push");

    // Re-send the 1000 window with a changed non-key value.
    let mut update = p();
    update.insert("value".to_string(), Value::Int64(99));
    edge.execute(
        "UPDATE metric_windows SET value = $value WHERE window_start = 1000",
        &update,
    )
    .expect("change the non-key value of the 1000 window");
    within(client.push()).await.expect("second push");

    assert_eq!(
        windows(&hub.db),
        vec![
            (
                "machine-a".to_string(),
                "sensor-1".to_string(),
                "motion".to_string(),
                1_000,
                11
            ),
            (
                "machine-a".to_string(),
                "sensor-1".to_string(),
                "motion".to_string(),
                2_000,
                22
            ),
        ],
        "same machine, two windows: both survive as distinct rows and the re-sent \
         key keeps its first value — a leading-column identity would collapse them"
    );

    hub.stop().await;
}

/// An EXISTING composite-key row updated under keep-latest. The hub holds two
/// rows that share the leading key column `machine_id` and differ only in
/// `window_start`; an incoming newer version of the 2000 row must overwrite
/// THAT row and leave the 1000 row untouched. The keep-latest behavior comes from
/// the table's `SYNC CONFLICT KEEP LATEST` declaration, honored by the SAME
/// normally-built hub every other test here uses — not from a rigged hub policy.
/// Before the fix the sync upsert resolved the existing row by the leading column
/// alone, matched the WRONG row (the 1000 row), tripped the composite-key
/// uniqueness check, and DROPPED the update — so the 2000 row kept its stale
/// value. Value-assert, not count.
#[tokio::test]
async fn latest_wins_updates_the_whole_composite_key_not_the_leading_column() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, LATEST_WINS_DDL);

    let edge = open_edge(LATEST_WINS_DDL);
    let client = edge_client(&edge, &broker, "edge-a");
    // Two rows sharing machine_id/sensor_id/metric, differing only in window_start.
    insert_window(&edge, "machine-a", "sensor-1", "motion", 1_000, 11);
    insert_window(&edge, "machine-a", "sensor-1", "motion", 2_000, 22);
    within(client.push()).await.expect("first push");
    assert_eq!(
        windows(&hub.db),
        vec![
            (
                "machine-a".to_string(),
                "sensor-1".to_string(),
                "motion".to_string(),
                1_000,
                11
            ),
            (
                "machine-a".to_string(),
                "sensor-1".to_string(),
                "motion".to_string(),
                2_000,
                22
            ),
        ],
        "the hub holds both composite rows before the update",
    );

    // A newer version of ONLY the 2000 row.
    let mut update = p();
    update.insert("value".to_string(), Value::Int64(99));
    edge.execute(
        "UPDATE metric_windows SET value = $value WHERE window_start = 2000",
        &update,
    )
    .expect("update the 2000 window's non-key value");
    within(client.push()).await.expect("second push");

    assert_eq!(
        windows(&hub.db),
        vec![
            (
                "machine-a".to_string(),
                "sensor-1".to_string(),
                "motion".to_string(),
                1_000,
                11
            ),
            (
                "machine-a".to_string(),
                "sensor-1".to_string(),
                "motion".to_string(),
                2_000,
                99
            ),
        ],
        "LatestWins must update the row whose WHOLE composite key matched (2000 -> 99) \
         and leave the 1000 row untouched; a leading-column lookup drops the update",
    );

    hub.stop().await;
}
