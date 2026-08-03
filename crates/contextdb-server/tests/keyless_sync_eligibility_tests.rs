//! A keyless table with no usable identity fallback must REFUSE sync
//! eligibility loudly instead of silently omitting its rows while push
//! reports success.
//!
//! Today `Database::persisted_state_since` / `full_state_snapshot`
//! (crates/contextdb-engine/src/database.rs) skip a keyless table's rows
//! with a bare `continue` when `natural_key_columns_for_meta` returns `None`
//! — no error, no signal, and `push()` still returns `Ok` with whatever
//! (possibly zero) rows a keyless table contributed. These tests pin the two
//! outcomes that behavior must instead produce: a table that WOULD sync (any
//! direction but `SYNC OFF`) and has no PRIMARY KEY / indexed `id` fallback
//! refuses loudly; a keyless table declared `SYNC OFF` is unaffected (green
//! guard — it already never reaches the hub, and must keep not erroring).
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads —
//! nothing here is time-dependent.

use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

const TENANT: &str = "keyless-eligibility";
/// A table with neither a declared PRIMARY KEY nor a column literally named
/// `id` — `natural_key_columns_for_meta` resolves to `None` for it. Default
/// sync direction (no `SYNC` clause) is `Both`, i.e. it WOULD sync.
const KEYLESS_SYNCED_DDL: &str =
    "CREATE TABLE keyless_events (payload TEXT NOT NULL, occurred_at INTEGER NOT NULL)";

/// The same shape, declared `SYNC OFF` — never eligible to leave the
/// machine that wrote it, keyless or not.
const KEYLESS_SYNC_OFF_DDL: &str =
    "CREATE TABLE keyless_local (payload TEXT NOT NULL, occurred_at INTEGER NOT NULL) SYNC OFF";

/// A column literally named `id` — `natural_key_columns_for_meta` resolves
/// `Some(["id"])` for it by name alone — but declared with NEITHER
/// `PRIMARY KEY` nor `UNIQUE` and with no `CREATE INDEX` naming it. The
/// engine's auto-index pass (`auto_indexes_for_table_meta`) only indexes a
/// `PRIMARY KEY`/`UNIQUE` column, so this `id` column carries no covering
/// index at all. Default sync direction (no `SYNC` clause) is `Both`.
const ID_COLUMN_WITHOUT_COVERING_INDEX_DDL: &str = "CREATE TABLE id_no_index_events (id UUID, payload TEXT NOT NULL, occurred_at INTEGER NOT NULL)";

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), fut)
        .await
        .expect("bounded sync operation exceeded 30s")
}

fn insert_event_with_id(
    db: &Database,
    table: &str,
    id: uuid::Uuid,
    payload: &str,
    occurred_at: i64,
) {
    let mut row = p();
    row.insert("id".to_string(), Value::Uuid(id));
    row.insert("payload".to_string(), Value::Text(payload.to_string()));
    row.insert("occurred_at".to_string(), Value::Int64(occurred_at));
    db.execute(
        &format!(
            "INSERT INTO {table} (id, payload, occurred_at) VALUES ($id, $payload, $occurred_at)"
        ),
        &row,
    )
    .unwrap_or_else(|err| panic!("{table} insert must succeed: {err}"));
}

fn insert_event(db: &Database, table: &str, payload: &str, occurred_at: i64) {
    let mut row = p();
    row.insert("payload".to_string(), Value::Text(payload.to_string()));
    row.insert("occurred_at".to_string(), Value::Int64(occurred_at));
    db.execute(
        &format!("INSERT INTO {table} (payload, occurred_at) VALUES ($payload, $occurred_at)"),
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
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = within(self.task).await;
    }
}

fn start_hub(broker: &InProcessBroker, ddl: &[&str]) -> RunningHub {
    let db = Arc::new(Database::open_memory());
    for stmt in ddl {
        db.execute(stmt, &p()).expect("hub table");
    }
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

fn open_edge(ddl: &[&str]) -> Arc<Database> {
    let db = Arc::new(Database::open_memory());
    for stmt in ddl {
        db.execute(stmt, &p()).expect("edge table");
    }
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

fn row_count(db: &Database, table: &str) -> usize {
    db.execute(&format!("SELECT * FROM {table}"), &p())
        .unwrap_or_else(|err| panic!("{table} scan must succeed: {err}"))
        .rows
        .len()
}

// ---------------------------------------------------------------------------
// A keyless table that WOULD sync refuses loudly, at eligibility time,
// before anything reaches the hub.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn keyless_table_with_no_fallback_refuses_push_instead_of_silently_dropping_rows() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, &[KEYLESS_SYNCED_DDL]);
    let edge = open_edge(&[KEYLESS_SYNCED_DDL]);
    insert_event(&edge, "keyless_events", "first", 1);
    insert_event(&edge, "keyless_events", "second", 2);

    let client = edge_client(&edge, &broker, "edge-b6b-a");
    let outcome = within(client.push()).await;

    let err = outcome.err().unwrap_or_else(|| {
        panic!(
            "push over a keyless syncable table must refuse, not silently report success \
             with rows dropped"
        )
    });
    assert!(
        matches!(err, contextdb_core::Error::SyncError(_)),
        "the refusal must surface as the distinct sync-error class, not an unrelated error \
         kind: {err:?}"
    );
    let msg = format!("{err}");
    assert!(
        msg.contains("PRIMARY KEY"),
        "the refusal must name the PRIMARY KEY remedy: {msg}"
    );
    assert!(
        msg.to_uppercase().contains("SYNC OFF"),
        "the refusal must name the SYNC OFF remedy: {msg}"
    );
    assert!(
        msg.contains("id"),
        "the refusal must name the indexed id-column fallback remedy: {msg}"
    );

    assert_eq!(
        row_count(&hub.db, "keyless_events"),
        0,
        "the eligibility check must fire before anything is sent — zero rows applied, \
         not a partial silent success"
    );

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// A table with a column literally named `id` still refuses if that column
// carries no covering index — the column's name alone is not a usable
// identity; the apply side's exact-key probe requires the same index.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn keyless_table_with_id_column_but_no_covering_index_still_refuses_push() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, &[ID_COLUMN_WITHOUT_COVERING_INDEX_DDL]);
    let edge = open_edge(&[ID_COLUMN_WITHOUT_COVERING_INDEX_DDL]);
    insert_event_with_id(
        &edge,
        "id_no_index_events",
        uuid::Uuid::new_v4(),
        "first",
        1,
    );
    insert_event_with_id(
        &edge,
        "id_no_index_events",
        uuid::Uuid::new_v4(),
        "second",
        2,
    );

    let client = edge_client(&edge, &broker, "edge-b6b-c");
    let outcome = within(client.push()).await;

    let err = outcome.err().unwrap_or_else(|| {
        panic!(
            "an `id` column with no covering index is not a usable sync identity — push must \
             refuse, not silently report success with rows dropped"
        )
    });
    assert!(
        matches!(err, contextdb_core::Error::SyncError(_)),
        "the refusal must surface as the distinct sync-error class, not an unrelated error \
         kind: {err:?}"
    );
    let msg = format!("{err}");
    assert!(
        msg.contains("PRIMARY KEY"),
        "the refusal must name the PRIMARY KEY remedy: {msg}"
    );
    assert!(
        msg.to_uppercase().contains("SYNC OFF"),
        "the refusal must name the SYNC OFF remedy: {msg}"
    );
    assert!(
        msg.contains("id"),
        "the refusal must name the indexed id-column fallback remedy: {msg}"
    );

    assert_eq!(
        row_count(&hub.db, "id_no_index_events"),
        0,
        "the eligibility check must fire before anything is sent — zero rows applied, \
         not a partial silent success"
    );

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// Existing keyless + SYNC OFF setups are unaffected — green guard, must stay
// green across the fix.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn keyless_table_declared_sync_off_is_unaffected_by_the_refusal() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, &[KEYLESS_SYNC_OFF_DDL]);
    let edge = open_edge(&[KEYLESS_SYNC_OFF_DDL]);
    insert_event(&edge, "keyless_local", "stays-local-1", 1);
    insert_event(&edge, "keyless_local", "stays-local-2", 2);

    let client = edge_client(&edge, &broker, "edge-b6b-b");
    let applied = within(client.push())
        .await
        .expect("a keyless SYNC OFF table must never trip the eligibility refusal");

    // SYNC OFF rows were never eligible to travel in the first place — this
    // push moved nothing, which is the pre-existing (and still correct)
    // outcome: there is no silently-dropped-while-reporting-success syncable
    // row here.
    assert_eq!(
        applied.applied_rows, 0,
        "a SYNC OFF table contributes no rows to a push either way"
    );
    assert_eq!(
        row_count(&hub.db, "keyless_local"),
        0,
        "SYNC OFF rows must never reach the hub"
    );

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// Pull-side: hub with keyless table must refuse pull, not silently omit rows
// ---------------------------------------------------------------------------

#[tokio::test]
async fn keyless_table_with_no_fallback_refuses_pull_instead_of_silently_dropping_rows() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, &[KEYLESS_SYNCED_DDL]);
    // Insert rows into the hub's keyless table to demonstrate they would be silently lost
    insert_event(&hub.db, "keyless_events", "hub-event-1", 1);
    insert_event(&hub.db, "keyless_events", "hub-event-2", 2);

    let edge = open_edge(&[KEYLESS_SYNCED_DDL]);
    let client = edge_client(&edge, &broker, "edge-pulling");

    // Attempt to pull from the hub containing keyless rows
    let outcome = within(client.pull_default()).await;

    let err = outcome.err().unwrap_or_else(|| {
        panic!(
            "pull from a hub with a keyless syncable table must refuse, not silently omit rows \
             from the changeset"
        )
    });
    assert!(
        matches!(err, contextdb_core::Error::SyncError(_)),
        "the refusal must surface as the distinct sync-error class, not an unrelated error \
         kind: {err:?}"
    );
    let msg = format!("{err}");
    assert!(
        msg.contains("PRIMARY KEY") || msg.contains("id"),
        "the refusal must name a remedy: {msg}"
    );

    // Verify that the edge received nothing (cursor did not advance)
    assert_eq!(
        row_count(&edge, "keyless_events"),
        0,
        "the pull refusal must fire before any rows are applied — zero rows on the edge, \
         even though the hub has 2 keyless rows"
    );

    hub.stop().await;
}

#[tokio::test]
async fn keyless_table_declared_sync_off_allows_pull_to_succeed() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, &[KEYLESS_SYNC_OFF_DDL]);
    // Even though we insert rows, they must stay local on the hub (SYNC OFF)
    insert_event(&hub.db, "keyless_local", "local-event-1", 1);
    insert_event(&hub.db, "keyless_local", "local-event-2", 2);

    let edge = open_edge(&[KEYLESS_SYNC_OFF_DDL]);
    let client = edge_client(&edge, &broker, "edge-pulling-local");

    // Pull should succeed — SYNC OFF tables are never synced
    let result = within(client.pull_default()).await;
    assert!(
        result.is_ok(),
        "pull must succeed when all sync-eligible tables have usable identities: {result:?}"
    );

    // No rows arrive on the edge (SYNC OFF means they never leave the hub)
    assert_eq!(
        row_count(&edge, "keyless_local"),
        0,
        "SYNC OFF rows must never arrive on a puller, even if the hub holds them"
    );

    hub.stop().await;
}
