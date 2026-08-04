//! `KEEP LATEST` means the last value the hub accepted, not the last value this
//! machine happened to write.
//!
//! Two edges write the same key and both push. The hub decides an order and one
//! of the two values ends up as the accepted one. Every edge must then converge
//! on that value. If an edge instead compares the incoming row against its OWN
//! recent commit, the edge that wrote last keeps its own value forever while the
//! hub and every other edge hold a different one — a permanent split that no
//! later sync repairs, on the table shape that carries mutable status.
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

/// The mutable-status shape: a composite identity and last-write-wins
/// arbitration.
const DDL: &str = "CREATE TABLE outcomes (context_id TEXT, decision_id TEXT, status TEXT, \
     PRIMARY KEY (context_id, decision_id)) SYNC TWO WAY SYNC CONFLICT KEEP LATEST";

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), fut)
        .await
        .expect("bounded sync exchange exceeded 60s")
}

fn write_status(db: &Database, status: &str) {
    db.execute(
        "INSERT INTO outcomes (context_id, decision_id, status) \
         VALUES ($context_id, $decision_id, $status)",
        &HashMap::from([
            ("context_id".to_string(), Value::Text("ctx-1".to_string())),
            ("decision_id".to_string(), Value::Text("dec-1".to_string())),
            ("status".to_string(), Value::Text(status.to_string())),
        ]),
    )
    .expect("write the status row");
}

fn status(db: &Database) -> Option<String> {
    let result = db
        .execute(
            "SELECT status FROM outcomes WHERE context_id = $context_id \
             AND decision_id = $decision_id",
            &HashMap::from([
                ("context_id".to_string(), Value::Text("ctx-1".to_string())),
                ("decision_id".to_string(), Value::Text("dec-1".to_string())),
            ]),
        )
        .expect("status scan");
    result.rows.first().map(|row| match &row[0] {
        Value::Text(status) => status.clone(),
        other => panic!("expected a text status, got {other:?}"),
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
        self.task.await.expect("hub task must stop");
    }
}

fn start_hub(broker: &InProcessBroker, tenant: &str) -> RunningHub {
    let db = Arc::new(Database::open_memory());
    db.execute(DDL, &p()).expect("hub table");
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

fn open_edge() -> Arc<Database> {
    let db = Arc::new(Database::open_memory());
    db.execute(DDL, &p()).expect("edge table");
    db
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
async fn both_edges_converge_on_the_status_the_hub_accepted_last() {
    let tenant = "keep-latest-pull-order";
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, tenant);

    let first = open_edge();
    let first_client = edge_client(&first, &broker, tenant);
    let second = open_edge();
    let second_client = edge_client(&second, &broker, tenant);

    // Two machines write the same status row without having seen each other.
    write_status(&first, "in-progress");
    write_status(&second, "complete");

    within(first_client.push())
        .await
        .expect("the first status reaches the hub");
    within(second_client.push())
        .await
        .expect("the second status reaches the hub");

    // The hub decided; whatever it now holds is the accepted value.
    let accepted = status(&hub.db).expect("the hub holds a status");

    // Both edges pull. Neither may keep a value the hub did not accept —
    // including the edge that wrote most recently on its own clock.
    let first_pull = within(first_client.pull_default())
        .await
        .expect("the first edge can pull");
    let second_pull = within(second_client.pull_default())
        .await
        .expect("the second edge can pull");

    assert_eq!(
        status(&first).as_deref(),
        Some(accepted.as_str()),
        "the first edge must hold the status the hub accepted: {first_pull:?}"
    );
    assert_eq!(
        status(&second).as_deref(),
        Some(accepted.as_str()),
        "the second edge must hold the status the hub accepted — its own later \
         local write is not a later hub acceptance: {second_pull:?}"
    );

    hub.stop().await;
}

/// The same contract from the other direction: an edge that wrote its own
/// value but never got it accepted must still take the hub's when it pulls.
/// A local write is not an acceptance, however recent it is.
#[tokio::test]
async fn an_unaccepted_local_status_yields_to_the_one_the_hub_holds() {
    let tenant = "keep-latest-unaccepted-local";
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, tenant);

    let accepted_edge = open_edge();
    let accepted_client = edge_client(&accepted_edge, &broker, tenant);
    let local_only = open_edge();
    let local_only_client = edge_client(&local_only, &broker, tenant);

    write_status(&accepted_edge, "in-progress");
    within(accepted_client.push())
        .await
        .expect("the accepted status reaches the hub");
    assert_eq!(
        status(&hub.db).as_deref(),
        Some("in-progress"),
        "the hub holds the accepted status"
    );

    // Written AFTER the hub already settled, and never pushed.
    write_status(&local_only, "complete");
    let pulled = within(local_only_client.pull_default())
        .await
        .expect("the local-only edge can pull");
    assert_eq!(
        status(&local_only).as_deref(),
        Some("in-progress"),
        "a local write the hub never accepted must yield on pull: {pulled:?}"
    );

    hub.stop().await;
}
