//! Conflict resolution is the policy the TABLE declares and the hub HONORS —
//! never a rule the hub hardcodes.
//!
//! What a reader should take from this file: the hub these tests run is built the
//! ordinary way a shipped hub is — it is handed the database's own default
//! policies and nothing table-specific. What happens to a re-sent row is decided
//! entirely by what the TABLE declared. A table that declares nothing keeps the
//! first value (the non-overwriting default); a table that declares
//! `SYNC CONFLICT KEEP LATEST` takes the newest value on the SAME hub. Two
//! opposite outcomes on one un-rigged hub are the proof the declaration is what
//! the hub reads.
//!
//! Before this change: the hub hardcoded last-writer-wins, so an undeclared
//! re-send silently overwrote; and the `SYNC CONFLICT` clause did not parse, so a
//! table could not ask for keep-first at all.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads. Every
//! assertion is by VALUE — a count-only check passes under a silent overwrite,
//! which is exactly the failure this pins.

use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_server::{InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const TENANT: &str = "conflict";
const HUB_NODE: &str = "hub-node-a";

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

/// A bound on a hung exchange, not a timing assertion: every assertion below
/// reads state after the exchange returns.
async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), fut)
        .await
        .expect("bounded sync exchange exceeded 60s")
}

fn insert_note(db: &Database, id: i64, body: &str) {
    let mut row = p();
    row.insert("id".to_string(), Value::Int64(id));
    row.insert("body".to_string(), Value::Text(body.to_string()));
    db.execute("INSERT INTO notes (id, body) VALUES ($id, $body)", &row)
        .unwrap_or_else(|err| panic!("insert note {id}: {err}"));
}

fn update_body(db: &Database, id: i64, body: &str) {
    let mut row = p();
    row.insert("id".to_string(), Value::Int64(id));
    row.insert("body".to_string(), Value::Text(body.to_string()));
    db.execute("UPDATE notes SET body = $body WHERE id = $id", &row)
        .unwrap_or_else(|err| panic!("update note {id}: {err}"));
}

/// The single `notes` row's `body`, by value — the payload every assertion below
/// compares against. Fails loudly if the table does not hold exactly one row, so
/// a collapse or a duplicate cannot hide behind the value check.
fn only_body(db: &Database, id: i64) -> String {
    let mut params = p();
    params.insert("id".to_string(), Value::Int64(id));
    let result = db
        .execute("SELECT body FROM notes WHERE id = $id", &params)
        .expect("notes scan");
    assert_eq!(
        result.rows.len(),
        1,
        "exactly one row must carry id {id}, got {:?}",
        result.rows
    );
    match &result.rows[0][0] {
        Value::Text(body) => body.clone(),
        other => panic!("expected a text body, got {other:?}"),
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

/// A hub built the ordinary way: handed the database's OWN default policies —
/// the same uniform default a shipped hub binary passes — and nothing
/// table-specific. It honors each table's declared policy through the table's
/// meta, and falls back to the engine's non-overwriting default for a table that
/// declared none. This is NOT a per-test policy rig: no conflict policy is named
/// here.
fn start_hub(broker: &InProcessBroker, ddl: &str) -> RunningHub {
    let db = Arc::new(Database::open_memory());
    db.execute(ddl, &p()).expect("hub table");
    let server = Arc::new(SyncServer::with_transport(
        db.clone(),
        broker.server_as(HUB_NODE),
        TenantId::from(TENANT),
        db.conflict_policies(),
    ));
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

fn edge_client(db: &Arc<Database>, broker: &InProcessBroker, node_id: &str) -> SyncClient {
    SyncClient::with_transport(
        db.clone(),
        broker.client_as(node_id),
        TenantId::from(TENANT),
    )
}

/// An UNDECLARED table takes the non-overwriting default: a re-send of an
/// existing key does NOT overwrite it. Pre-correction the hub hardcoded
/// last-writer-wins, so this silently kept the second value.
#[tokio::test]
async fn an_undeclared_table_keeps_the_first_value_on_a_resend() {
    const DDL: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)";
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, DDL);

    let edge = open_edge(DDL);
    let client = edge_client(&edge, &broker, "edge-a");
    insert_note(&edge, 1, "first");
    within(client.push()).await.expect("first push");

    update_body(&edge, 1, "second");
    within(client.push()).await.expect("second push");

    assert_eq!(
        only_body(&hub.db, 1),
        "first",
        "an undeclared table's default is non-overwriting: the re-send must not \
         replace the first value"
    );

    hub.stop().await;
}

/// A table that DECLARES keep-first keeps the first value on a re-send — the
/// consumer's durable-record shape, decided by the declaration and honored by an
/// un-rigged hub. Pre-correction the clause did not parse.
#[tokio::test]
async fn a_declared_keep_first_table_keeps_the_first_value_on_a_resend() {
    const DDL: &str =
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP FIRST";
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, DDL);

    let edge = open_edge(DDL);
    let client = edge_client(&edge, &broker, "edge-a");
    insert_note(&edge, 1, "first");
    within(client.push()).await.expect("first push");

    update_body(&edge, 1, "second");
    within(client.push()).await.expect("second push");

    assert_eq!(
        only_body(&hub.db, 1),
        "first",
        "a keep-first declaration must keep the first value through the re-send"
    );

    hub.stop().await;
}

/// A table that DECLARES keep-latest takes the newest value on a re-send — on the
/// SAME hub whose default is non-overwriting. The outcome contradicts the
/// default, so it can only come from the hub reading the declaration: this is the
/// disambiguator that keep-first passing above is honoring and not a lucky
/// default. Pre-correction the clause did not parse.
#[tokio::test]
async fn a_declared_keep_latest_table_takes_the_latest_value_on_a_resend() {
    const DDL: &str =
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST";
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, DDL);

    let edge = open_edge(DDL);
    let client = edge_client(&edge, &broker, "edge-a");
    insert_note(&edge, 1, "first");
    within(client.push()).await.expect("first push");

    update_body(&edge, 1, "second");
    within(client.push()).await.expect("second push");

    assert_eq!(
        only_body(&hub.db, 1),
        "second",
        "a keep-latest declaration must take the newest value — proving the hub \
         honors the declaration, not its own non-overwriting default"
    );

    hub.stop().await;
}
