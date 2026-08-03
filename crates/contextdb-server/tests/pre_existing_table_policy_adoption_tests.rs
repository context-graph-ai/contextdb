//! A hub adopts an edge's DECLARED conflict policy for a table that
//! already exists on the hub UNDECLARED.
//!
//! When a synced `CreateTable` arrives for a table that already exists, the apply
//! compared columns / FKs / unique constraints but NOT the conflict policy, so it
//! `continue`d and left the hub's existing (keep-first default) policy in place —
//! silently overriding the edge's declaration. The AlterTable path already adopts
//! it; the pre-existing-table CreateTable path did not.
//!
//! What a reader should take from this file: a hub holding `notes` undeclared,
//! receiving an edge's `notes ... SYNC CONFLICT KEEP LATEST`, must ADOPT
//! keep-latest — both in the stored table policy and in what survives a re-send.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads. The
//! surviving value is asserted by content, not by count.

use contextdb_core::{ConflictPolicy, SyncDirection, TenantId, Value};
use contextdb_engine::Database;
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const TENANT: &str = "policy-adopt";
/// The hub's table: bare, no conflict-policy clause — its policy is the engine's
/// non-overwriting keep-first default.
const UNDECLARED_DDL: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)";
/// The edge's table: the SAME shape, DECLARING keep-latest.
const DECLARED_LATEST_DDL: &str =
    "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST";
/// The edge's table declaring the full policy set — conflict, direction, and
/// retention — so the relay assertion covers everything the adoption carries.
const DECLARED_FULL_DDL: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) \
     RETAIN 48 HOURS SYNC SAFE SYNC TWO WAY SYNC CONFLICT KEEP LATEST";

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), fut)
        .await
        .expect("a sync exchange exceeded 60s")
}

fn insert_note(db: &Database, id: i64, body: &str) {
    let mut row = p();
    row.insert("id".to_string(), Value::Int64(id));
    row.insert("body".to_string(), Value::Text(body.to_string()));
    db.execute("INSERT INTO notes (id, body) VALUES ($id, $body)", &row)
        .expect("note insert");
}

fn update_note(db: &Database, id: i64, body: &str) {
    let mut row = p();
    row.insert("id".to_string(), Value::Int64(id));
    row.insert("body".to_string(), Value::Text(body.to_string()));
    db.execute("UPDATE notes SET body = $body WHERE id = $id", &row)
        .expect("note update");
}

fn body_of(db: &Database, id: i64) -> Option<String> {
    let mut params = p();
    params.insert("id".to_string(), Value::Int64(id));
    let result = db
        .execute("SELECT body FROM notes WHERE id = $id", &params)
        .expect("scan");
    result.rows.first().map(|row| match &row[0] {
        Value::Text(t) => t.clone(),
        other => panic!("body must be text, got {other:?}"),
    })
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

#[tokio::test]
async fn fix4_hub_adopts_a_declared_policy_for_a_pre_existing_undeclared_table() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, UNDECLARED_DDL);

    assert_eq!(
        hub.db
            .table_meta("notes")
            .expect("hub notes meta")
            .conflict_policy,
        None,
        "premise: the hub's pre-existing table declares no conflict policy"
    );

    let edge = open_edge(DECLARED_LATEST_DDL);
    let client = edge_client(&edge, &broker, "edge-a");

    // First write + push: the edge's declared CreateTable arrives at the hub's
    // pre-existing undeclared table.
    insert_note(&edge, 1, "first");
    within(client.push()).await.expect("edge push 1");

    assert_eq!(
        hub.db
            .table_meta("notes")
            .expect("hub notes meta")
            .conflict_policy,
        Some(ConflictPolicy::KEEP_LATEST),
        "the hub must ADOPT the edge's declared KEEP LATEST policy for its \
         pre-existing undeclared table; today it keeps its keep-first default"
    );
    assert_eq!(
        body_of(&hub.db, 1),
        Some("first".to_string()),
        "the first value landed on the hub"
    );

    // Update + re-send: keep-latest means the newest value wins on the hub. A
    // kept keep-first default would leave the first value.
    update_note(&edge, 1, "second");
    within(client.push()).await.expect("edge push 2");
    assert_eq!(
        body_of(&hub.db, 1),
        Some("second".to_string()),
        "under the adopted KEEP LATEST policy the hub takes the newest value"
    );

    hub.stop().await;
}

/// The adoption must TRAVEL to other edges, not stop at the hub. When the
/// hub adopts an edge's declaration for a pre-existing undeclared table it must
/// also allocate an LSN and log the change, so a DIFFERENT edge pulling afterward
/// receives the declared policy, direction, and retention — not the hub's stale
/// undeclared defaults.
#[tokio::test]
async fn fixc_the_adopted_declaration_travels_to_a_later_pulling_edge() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, UNDECLARED_DDL);
    assert_eq!(
        hub.db
            .table_meta("notes")
            .expect("hub notes meta")
            .conflict_policy,
        None,
        "premise: the hub's pre-existing table declares no policy"
    );

    // Edge A declares the full policy set and pushes; the hub adopts it.
    let edge_a = open_edge(DECLARED_FULL_DDL);
    let client_a = edge_client(&edge_a, &broker, "edge-a");
    insert_note(&edge_a, 1, "first");
    within(client_a.push()).await.expect("edge A push");
    assert_eq!(
        hub.db
            .table_meta("notes")
            .expect("hub notes meta")
            .conflict_policy,
        Some(ConflictPolicy::KEEP_LATEST),
        "premise: the hub adopted the declaration"
    );

    // Edge B — which never saw the declaration — pulls from the hub.
    let edge_b = Arc::new(Database::open_memory());
    let client_b = edge_client(&edge_b, &broker, "edge-b");
    within(client_b.pull_default()).await.expect("edge B pull");

    let meta_b = edge_b
        .table_meta("notes")
        .expect("edge B must have the notes table after pulling");
    assert_eq!(
        meta_b.conflict_policy,
        Some(ConflictPolicy::KEEP_LATEST),
        "before this change the hub's adoption stopped at the hub; edge B pulling \
         afterward must receive the relayed declaration and adopt KEEP LATEST, not \
         the undeclared default"
    );
    assert_eq!(
        meta_b.sync_direction,
        Some(SyncDirection::Both),
        "the declared direction travels with the adoption"
    );
    assert_eq!(
        meta_b.default_ttl_seconds,
        Some(48 * 60 * 60),
        "the declared retention window travels with the adoption"
    );
    assert!(
        meta_b.sync_safe,
        "the declared SYNC SAFE promise travels with the adoption"
    );

    hub.stop().await;
}
