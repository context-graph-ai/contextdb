//! Serve-back on a retained `SYNC TWO WAY` table that ALSO declares an
//! `EXPIRES` column must judge expiry EXACTLY as the local prune rule does —
//! the per-row `EXPIRES` timestamp takes precedence over the `RETAIN` window
//! (`i64::MAX` = never expires; a passed timestamp = expired), and only when
//! the row carries no such override does the `created_at` + window judgement
//! apply.
//!
//! This pins the two ways the serve filter and the local prune rule
//! (`row_is_prunable`) disagreed when it judged the `RETAIN` window ALONE and
//! ignored the `EXPIRES` column — a legal, tested `RETAIN` + `EXPIRES`
//! combination:
//!   * FORWARD (expired data would RETURN): a row whose `EXPIRES` timestamp has
//!     passed but whose `created_at` is still inside the window is prunable
//!     locally, yet the old serve filter kept it and served it back.
//!   * INVERSE (live data would be LOST on recovery): a never-expire
//!     (`EXPIRES` = `i64::MAX`) row with a `created_at` older than the window is
//!     NOT prunable locally, yet the old serve filter excluded it.
//!
//! Discipline mirrors `retained_history_readback_tests.rs`: no sleeps, no
//! elapsed-time assertions, no raw clock reads; time moves through
//! `Wallclock::test_clock_guard`; every assertion is by VALUE. The two suites
//! deliberately do not share a module — this one is additive and leaves the
//! frozen readback suite byte-for-byte untouched.

use contextdb_core::{TenantId, Value, Wallclock};
use contextdb_engine::Database;
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), fut)
        .await
        .expect("sync operation must complete within 60s")
}

const T0: u64 = 1_700_000_000_000;
const TENANT: &str = "expires-readback";
const MINUTE: u64 = 60 * 1000;

/// The table this suite exists for: a retained, two-way table that also names a
/// per-row `EXPIRES` override. One window (`RETAIN 1 HOURS`), plus an `EXPIRES`
/// timestamp column that can pin a row past the window or retire it early.
const WINDOWS_DDL: &str = "CREATE TABLE windows \
     (id INTEGER PRIMARY KEY, body TEXT, expires_at TIMESTAMP EXPIRES) \
     RETAIN 1 HOURS SYNC TWO WAY SYNC CONFLICT KEEP LATEST";
/// Non-retained control: proves the pull path itself worked in every test that
/// asserts a retained row is absent.
const CONTROL_DDL: &str =
    "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST";

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

/// The sorted `body` values a database actually holds, read through the
/// ordinary local query surface.
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

fn insert_note(db: &Database, id: i64, body: &str) {
    let mut row = p();
    row.insert("id".to_string(), Value::Int64(id));
    row.insert("body".to_string(), Value::Text(body.to_string()));
    db.execute("INSERT INTO notes (id, body) VALUES ($id, $body)", &row)
        .unwrap_or_else(|err| panic!("notes insert must succeed: {err}"));
}

/// Insert a `windows` row carrying an explicit `EXPIRES` timestamp: `i64::MAX`
/// to pin it forever, or a millisecond stamp that is judged against the clock.
fn insert_window(db: &Database, id: i64, body: &str, expires_at: i64) {
    let mut row = p();
    row.insert("id".to_string(), Value::Int64(id));
    row.insert("body".to_string(), Value::Text(body.to_string()));
    row.insert("expires_at".to_string(), Value::Timestamp(expires_at));
    db.execute(
        "INSERT INTO windows (id, body, expires_at) VALUES ($id, $body, $expires_at)",
        &row,
    )
    .unwrap_or_else(|err| panic!("windows insert must succeed: {err}"));
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

fn declare_tables(db: &Database) {
    db.execute(WINDOWS_DDL, &p())
        .expect("a retained SYNC TWO WAY table with an EXPIRES column must be declarable");
    db.execute(CONTROL_DDL, &p()).expect("control table");
}

fn start_hub(broker: &InProcessBroker) -> RunningHub {
    let db = Arc::new(Database::open_memory());
    declare_tables(&db);
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

fn open_edge() -> Arc<Database> {
    let db = Arc::new(Database::open_memory());
    declare_tables(&db);
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
// FORWARD — an EXPIRES-passed row is NOT served back even with a young created_at
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_expires_passed_row_is_not_served_back_despite_a_young_created_at() {
    let (_clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);

    let edge_a = open_edge();
    // Both rows are written NOW, so both have a young created_at well inside the
    // RETAIN window. The only difference is the EXPIRES override.
    insert_window(&edge_a, 1, "expires-already-passed", (T0 - MINUTE) as i64);
    insert_window(&edge_a, 2, "live-control", i64::MAX);
    insert_note(&edge_a, 1, "control-note");
    let client_a = edge_client(&edge_a, &broker, "edge-a");
    within(client_a.push()).await.expect("edge-a push");

    // The hub holds both retained rows at the moment of the read — no pruning
    // cycle is driven — so the exclusion below is a serve-time filter honoring
    // the EXPIRES column, not a hub that pruned first.
    assert_eq!(
        bodies(&hub.db, "windows"),
        vec!["expires-already-passed", "live-control"],
        "the hub genuinely holds both rows, expired override included"
    );

    let edge_b = open_edge();
    let client_b = edge_client(&edge_b, &broker, "edge-b");
    within(client_b.pull_default()).await.expect("edge-b pull");

    assert_eq!(
        bodies(&edge_b, "notes"),
        vec!["control-note"],
        "the non-retained control proves the pull path itself worked"
    );
    assert_eq!(
        bodies(&edge_b, "windows"),
        vec!["live-control"],
        "serve-back honors the EXPIRES column: the row whose EXPIRES timestamp has already \
         passed is withheld even though its created_at is young, while the never-expire row is \
         served — exactly as the local prune rule judges the same two rows"
    );

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// INVERSE — a never-expire row IS served back even with an aged created_at
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_never_expire_row_is_served_back_despite_an_aged_created_at() {
    let (clock, _guard) = MockClock::install(T0);
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);

    let edge_a = open_edge();
    // Born at T0, pinned to never expire via EXPIRES = i64::MAX.
    insert_window(&edge_a, 1, "never-expire", i64::MAX);
    let client_a = edge_client(&edge_a, &broker, "edge-a");
    within(client_a.push()).await.expect("edge-a push");

    // Age the row well past the RETAIN window. Its created_at is now older than
    // the window, but its EXPIRES override says it never dies.
    clock.advance(90 * MINUTE);

    // A young control on the same retained table proves the retained pull path
    // is live at this clock — its own EXPIRES is far in the future.
    insert_note(&edge_a, 1, "control-note");
    within(client_a.push()).await.expect("edge-a push control");

    // The hub still holds the aged never-expire row.
    assert_eq!(
        bodies(&hub.db, "windows"),
        vec!["never-expire"],
        "the hub still holds the aged never-expire row at the moment of the read"
    );

    let restored = open_edge();
    let restored_client = edge_client(&restored, &broker, "edge-a-restored");
    within(restored_client.pull_default())
        .await
        .expect("recovery pull");

    assert_eq!(
        bodies(&restored, "notes"),
        vec!["control-note"],
        "the non-retained control proves the pull path itself worked"
    );
    assert_eq!(
        bodies(&restored, "windows"),
        vec!["never-expire"],
        "serve-back honors the EXPIRES column: a never-expire (EXPIRES = i64::MAX) row is served \
         back on recovery even though its created_at is older than the RETAIN window — exactly as \
         the local prune rule refuses to prune it"
    );

    hub.stop().await;
}
