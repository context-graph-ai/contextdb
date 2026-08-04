//! A table an edge only ever learned about BY SYNC must arbitrate the way its
//! author declared it.
//!
//! The declaration travels with the schema, so an edge that never typed the
//! `CREATE TABLE` itself still knows the table keeps the first accepted value
//! and which column is its identity. Losing that on the receiving side would be
//! silent: rows would still apply, but every same-key disagreement would be
//! resolved as if the table had no declared arbitration at all, and two
//! machines racing one key would both believe they won.
//!
//! This is the shape the work-fabric tables have in production — an edge never
//! declares them locally, it receives them. So the race below is run entirely
//! on a received table: neither edge ever types the `CREATE`.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads; every
//! assertion reads state after a bounded exchange returns.

use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Declared exactly as the engine-owned work-fabric tables are: two-way sync,
/// keep the first accepted value, identity on a column-level primary key.
const DDL: &str = "CREATE TABLE work_claims (claim_key TEXT PRIMARY KEY, holder TEXT) \
     SYNC TWO WAY SYNC CONFLICT KEEP FIRST";

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), fut)
        .await
        .expect("bounded sync exchange exceeded 60s")
}

fn claim(db: &Database, key: &str, holder: &str) {
    db.execute(
        "INSERT INTO work_claims (claim_key, holder) VALUES ($claim_key, $holder)",
        &HashMap::from([
            ("claim_key".to_string(), Value::Text(key.to_string())),
            ("holder".to_string(), Value::Text(holder.to_string())),
        ]),
    )
    .expect("insert claim");
}

fn holder(db: &Database, key: &str) -> Option<String> {
    let result = db
        .execute(
            "SELECT holder FROM work_claims WHERE claim_key = $claim_key",
            &HashMap::from([("claim_key".to_string(), Value::Text(key.to_string()))]),
        )
        .expect("claim scan");
    result.rows.first().map(|row| match &row[0] {
        Value::Text(holder) => holder.clone(),
        other => panic!("expected a text holder, got {other:?}"),
    })
}

/// What this database believes each table's declared arbitration is — the same
/// declaration the sync path resolves a same-key disagreement against.
fn declared_policies(db: &Database) -> Vec<String> {
    let result = db
        .execute("SHOW SYNC_CONFLICT_POLICY", &p())
        .expect("show declared conflict policy");
    result
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Text(policy) => policy.clone(),
            other => panic!("policy column must be text, got {other:?}"),
        })
        .collect()
}

struct RunningHub {
    db: Arc<Database>,
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl RunningHub {
    async fn stop(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.task.await.expect("hub task must stop");
    }
}

fn start_hub(broker: &InProcessBroker, tenant: &str) -> RunningHub {
    let db = Arc::new(Database::open_memory());
    db.execute(DDL, &p()).expect("hub declares the table");
    let identity = Arc::new(FabricIdentity::generate());
    let hub_node_id = identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            db.clone(),
            broker.server_as(&hub_node_id),
            TenantId::from(tenant),
            hub_node_id,
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

/// An edge that has NEVER been told about this table locally. Everything it
/// knows about `work_claims` it learns from the hub.
fn open_undeclared_edge() -> Arc<Database> {
    Arc::new(Database::open_memory())
}

fn edge_client(db: &Arc<Database>, broker: &InProcessBroker, tenant: &str) -> SyncClient {
    let identity = Arc::new(FabricIdentity::generate());
    let node_id = identity.node_id();
    SyncClient::with_authenticated_transport_and_identity_for_test(
        db.clone(),
        broker.client_as(&node_id),
        TenantId::from(tenant),
        identity,
    )
}

#[tokio::test]
async fn a_table_learned_by_sync_carries_its_authors_declared_arbitration_and_key() {
    let tenant = "received-table-keeps-declared-arbitration";
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, tenant);
    claim(&hub.db, "job-1", "edge-one");

    let edge = open_undeclared_edge();
    let client = edge_client(&edge, &broker, tenant);
    within(client.pull_default())
        .await
        .expect("the edge learns the table and its row by sync");
    assert_eq!(
        holder(&edge, "job-1").as_deref(),
        Some("edge-one"),
        "the received table is usable: its row arrived"
    );

    // The declaration travelled with the schema, so this edge knows the table
    // keeps the first accepted value even though it never typed the CREATE.
    assert!(
        declared_policies(&edge)
            .iter()
            .any(|policy| policy == "work_claims=keep_first"),
        "a table learned by sync must report the arbitration its author \
         declared; got {:?}",
        declared_policies(&edge)
    );

    // And the identity that arbitration resolves against: the receiver must
    // agree with the author about which column names a row, or it cannot tell
    // a same-key disagreement from two unrelated rows.
    let hub_meta = hub.db.table_meta("work_claims").expect("hub table meta");
    let edge_meta = edge.table_meta("work_claims").expect("edge table meta");
    assert_eq!(
        edge_meta.conflict_policy, hub_meta.conflict_policy,
        "the receiver's arbitration must be the author's"
    );
    assert_eq!(
        edge_meta
            .columns
            .iter()
            .filter(|column| column.primary_key)
            .map(|column| column.name.clone())
            .collect::<Vec<_>>(),
        vec!["claim_key".to_string()],
        "the receiver must know which column names a row"
    );
    assert_eq!(
        edge_meta.primary_key_columns, hub_meta.primary_key_columns,
        "the receiver's key identity must be the author's, however it is spelled"
    );
    assert_eq!(
        edge_meta.natural_key_column, hub_meta.natural_key_column,
        "the receiver's key identity must be the author's, however it is spelled"
    );

    hub.stop().await;
}

/// The race that arbitration exists for, run entirely on a table NEITHER edge
/// declared: both learn it by sync, both claim the same key, and exactly one
/// must end up believing it holds the claim.
#[tokio::test]
async fn two_edges_racing_one_key_on_a_received_table_agree_on_the_winner() {
    let tenant = "received-table-claim-race";
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, tenant);

    let winner = open_undeclared_edge();
    let winner_client = edge_client(&winner, &broker, tenant);
    let loser = open_undeclared_edge();
    let loser_client = edge_client(&loser, &broker, tenant);

    // Both edges learn the table by sync and nothing else.
    within(winner_client.pull_default())
        .await
        .expect("the winner learns the table by sync");
    within(loser_client.pull_default())
        .await
        .expect("the loser learns the table by sync");

    claim(&winner, "job-1", "edge-one");
    within(winner_client.push())
        .await
        .expect("the winning claim reaches the hub");
    assert_eq!(
        holder(&hub.db, "job-1").as_deref(),
        Some("edge-one"),
        "the hub holds the first accepted claim"
    );

    claim(&loser, "job-1", "edge-two");
    let refusal = within(loser_client.push())
        .await
        .expect("the losing push is a handled refusal, not a transport error");
    assert!(
        !refusal.conflicts.is_empty(),
        "the hub refuses the second claim on a table it only ever sent: {refusal:?}"
    );

    let pulled = within(loser_client.pull_default())
        .await
        .expect("the refused pusher can pull");
    assert_eq!(
        holder(&loser, "job-1").as_deref(),
        Some("edge-one"),
        "the loser must read the WINNER as holder — otherwise both edges run \
         the same job: {pulled:?}"
    );

    hub.stop().await;
}
