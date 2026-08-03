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

use contextdb_core::{Lsn, TenantId, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{
    ChangeSet, ConflictPolicies, ConflictPolicy, RowChange, SyncAdoption,
};
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const TENANT: &str = "conflict";
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
    only_table_body(db, "notes", id)
}

fn only_table_body(db: &Database, table: &str, id: i64) -> String {
    let mut params = p();
    params.insert("id".to_string(), Value::Int64(id));
    let result = db
        .execute(&format!("SELECT body FROM {table} WHERE id = $id"), &params)
        .expect("table scan");
    assert_eq!(
        result.rows.len(),
        1,
        "exactly one {table} row must carry id {id}, got {:?}",
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
    start_hub_with_ddls(broker, &[ddl])
}

fn start_hub_with_ddls(broker: &InProcessBroker, ddls: &[&str]) -> RunningHub {
    let db = Arc::new(Database::open_memory());
    for ddl in ddls {
        db.execute(ddl, &p()).expect("hub table");
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

fn open_edge(ddl: &str) -> Arc<Database> {
    open_edge_with_ddls(&[ddl])
}

fn open_edge_with_ddls(ddls: &[&str]) -> Arc<Database> {
    let db = Arc::new(Database::open_memory());
    for ddl in ddls {
        db.execute(ddl, &p()).expect("edge table");
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

#[tokio::test]
async fn refused_keep_first_delete_reconciles_forward_without_rewinding_pull_cursor() {
    const DDL: &str =
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP FIRST";
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, DDL);
    let first = open_edge(DDL);
    let second = open_edge(DDL);
    let first_client = edge_client(&first, &broker, "keep-first-origin");
    let second_client = edge_client(&second, &broker, "keep-first-refusal");

    insert_note(&first, 1, "first");
    within(first_client.push()).await.expect("seed hub");
    within(second_client.pull_default())
        .await
        .expect("receive seed");
    let before_refusal = second_client.pull_watermark();
    second
        .execute("DELETE FROM notes WHERE id = 1", &p())
        .expect("author delete that Keep First refuses");
    within(second_client.push())
        .await
        .expect("refused delete push completes");
    assert_eq!(
        second_client.pull_watermark(),
        before_refusal,
        "a policy refusal must not reset the global pull cursor"
    );

    within(second_client.pull_default())
        .await
        .expect("forward reconciliation pull");
    assert_eq!(only_body(&second, 1), "first");
    assert!(
        second_client.pull_watermark() > before_refusal,
        "the hub's targeted re-emission advances the ordinary cursor"
    );
    hub.stop().await;
}

#[tokio::test]
async fn mixed_group_stamps_the_accepted_sibling_before_refusal_reconciliation() {
    const ACCEPTED: &str =
        "CREATE TABLE accepted (id INTEGER PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST";
    const REFUSED: &str =
        "CREATE TABLE refused (id INTEGER PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP FIRST";
    let broker = InProcessBroker::new();
    let hub = start_hub_with_ddls(&broker, &[ACCEPTED, REFUSED]);
    let origin = open_edge_with_ddls(&[ACCEPTED, REFUSED]);
    let edge = open_edge_with_ddls(&[ACCEPTED, REFUSED]);
    let origin_client = edge_client(&origin, &broker, "mixed-origin");
    let edge_client = edge_client(&edge, &broker, "mixed-refusal");

    let mut seed = p();
    seed.insert("id".to_string(), Value::Int64(1));
    seed.insert("body".to_string(), Value::Text("first".to_string()));
    origin
        .execute("INSERT INTO refused (id, body) VALUES ($id, $body)", &seed)
        .expect("seed refused row");
    within(origin_client.push()).await.expect("push seed");
    within(edge_client.pull_default()).await.expect("pull seed");

    let tx = edge.begin().expect("begin one edge transaction");
    edge.execute_in_tx(tx, "INSERT INTO accepted VALUES (9, 'accepted')", &p())
        .expect("stage accepted sibling");
    edge.execute_in_tx(tx, "DELETE FROM refused WHERE id = 1", &p())
        .expect("stage refused sibling");
    edge.commit(tx).expect("commit mixed local group");
    within(edge_client.push())
        .await
        .expect("push mixed data-LSN group");

    let accepted_key =
        contextdb_engine::sync_types::NaturalKey::single("id".to_string(), Value::Int64(9));
    let (changes, arrivals) = edge.changes_since_with_arrivals(Lsn(0));
    let accepted = changes
        .rows
        .iter()
        .find(|row| row.table == "accepted" && row.natural_key == accepted_key)
        .expect("accepted sibling change");
    assert!(
        arrivals.get(&accepted.lsn).is_some_and(Option::is_some),
        "the accepted sibling carries the exact hub acknowledgement even though \
         its transaction also had a refused key"
    );
    assert!(
        !edge.row_version_arrived_by_sync("accepted", &accepted_key),
        "AcceptedLocal is acknowledged provenance, not a pulled echo: it stays \
         eligible for outbound repair if the hub later regresses"
    );
    within(edge_client.pull_default())
        .await
        .expect("pull targeted refused-key reconciliation");
    assert_eq!(only_table_body(&edge, "refused", 1), "first");
    assert_eq!(only_table_body(&hub.db, "accepted", 9), "accepted");
    hub.stop().await;
}

/// One source transaction is one accepting hub position. A delete must be
/// staged before an insert for constraint safety, but staging order may not
/// split the group into two commits: the acknowledgement/provenance has only
/// one position to represent the complete source transaction.
#[tokio::test]
async fn accepted_delete_and_insert_from_one_source_transaction_share_one_hub_commit() {
    const DDL: &str =
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST";
    let hub = Database::open_memory();
    hub.execute(DDL, &p()).expect("hub table");
    insert_note(&hub, 1, "old");
    let before = hub.current_lsn();
    let source_lsn = Lsn(77);
    let mut delete_values = p();
    delete_values.insert("__deleted".to_string(), Value::Bool(true));
    let mut insert_values = p();
    insert_values.insert("id".to_string(), Value::Int64(2));
    insert_values.insert("body".to_string(), Value::Text("new".to_string()));
    let changes = ChangeSet {
        rows: vec![
            RowChange {
                table: "notes".to_string(),
                natural_key: contextdb_engine::sync_types::NaturalKey::single(
                    "id".to_string(),
                    Value::Int64(1),
                ),
                values: delete_values,
                deleted: true,
                lsn: source_lsn,
                created_at: None,
            },
            RowChange {
                table: "notes".to_string(),
                natural_key: contextdb_engine::sync_types::NaturalKey::single(
                    "id".to_string(),
                    Value::Int64(2),
                ),
                values: insert_values,
                deleted: false,
                lsn: source_lsn,
                created_at: None,
            },
        ],
        ..ChangeSet::default()
    };
    // The source group is stampless, as an authored edge transaction is. The
    // receiver must assign one own-commit provenance position to both rows.
    let arrivals = HashMap::from([(source_lsn, None)]);
    hub.apply_synced_changes(
        changes,
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        &arrivals,
        SyncAdoption::Continuing,
    )
    .expect("apply one source LSN group");

    let after = hub.current_lsn();
    assert_eq!(
        after,
        Lsn(before.0 + 1),
        "the hub must accept the delete and insert in one commit, not split by staging order"
    );
    let (changes, arrivals) = hub.changes_since_with_arrivals(before);
    let group = changes
        .rows
        .iter()
        .filter(|row| row.lsn == after && row.table == "notes")
        .collect::<Vec<_>>();
    assert_eq!(
        group.len(),
        2,
        "one hub LSN carries both source mutations: {group:?}"
    );
    assert!(
        group.iter().any(|row| row.deleted) && group.iter().any(|row| !row.deleted),
        "the one group contains both the accepted delete and insert: {group:?}"
    );
    assert_eq!(
        arrivals.get(&after),
        Some(&Some(after)),
        "the shared hub commit is the exact provenance position served for this group"
    );
    let rows = hub
        .execute("SELECT id, body FROM notes", &p())
        .expect("hub read");
    assert_eq!(
        rows.rows,
        vec![vec![Value::Int64(2), Value::Text("new".to_string())]]
    );
}
