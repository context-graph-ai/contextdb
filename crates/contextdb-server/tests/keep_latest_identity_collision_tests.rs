//! `KEEP LATEST` on a row that carries its OWN identity as well as a natural
//! key: two edges each write a FRESH row under the same natural key.
//!
//! This is the replace-not-version shape a product uses for mutable status: the
//! row has a unique id of its own, a composite natural key that names the thing
//! the status is about, and a parent it references. Recording a new status is a
//! fresh insert under the same natural key, not an edit of the existing row's
//! id.
//!
//! Two machines doing that concurrently disagree on identity, not just on
//! content. The hub settles it, and every machine must end up holding the SAME
//! surviving row — same id, same content, one row. An edge that keeps its own
//! colliding row after pulling is permanently split from the hub and from its
//! peer, and no later sync repairs it.
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

/// The parent the status row references.
const DECISIONS_DDL: &str = "CREATE TABLE decisions (id TEXT PRIMARY KEY, \
     scope TEXT NOT NULL, description TEXT, UNIQUE (scope, id)) \
     SYNC TWO WAY SYNC CONFLICT KEEP FIRST";

/// The mutable-status shape: its own unique id, a composite natural key, a
/// composite reference to its parent, and last-accepted-wins arbitration.
const OUTCOMES_DDL: &str = "CREATE TABLE outcomes (id TEXT NOT NULL UNIQUE, \
     decision_id TEXT NOT NULL, scope TEXT NOT NULL, success BOOLEAN NOT NULL, \
     notes TEXT, PRIMARY KEY (scope, decision_id), UNIQUE (scope, id), \
     FOREIGN KEY (scope, decision_id) REFERENCES decisions(scope, id)) \
     SYNC TWO WAY SYNC CONFLICT KEEP LATEST";

const SCOPE: &str = "scope-1";
const DECISION: &str = "decision-1";

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), fut)
        .await
        .expect("bounded sync exchange exceeded 60s")
}

fn declare(db: &Database) {
    db.execute(DECISIONS_DDL, &p()).expect("declare decisions");
    db.execute(OUTCOMES_DDL, &p()).expect("declare outcomes");
}

fn seed_decision(db: &Database) {
    db.execute(
        "INSERT INTO decisions (id, scope, description) VALUES ($id, $scope, $description)",
        &HashMap::from([
            ("id".to_string(), Value::Text(DECISION.to_string())),
            ("scope".to_string(), Value::Text(SCOPE.to_string())),
            (
                "description".to_string(),
                Value::Text("the decision the status is about".to_string()),
            ),
        ]),
    )
    .expect("seed the parent decision");
}

/// Record a status: a FRESH row with its own id under the SAME natural key.
fn record_outcome(db: &Database, id: &str, success: bool, notes: &str) {
    db.execute(
        "INSERT INTO outcomes (id, decision_id, scope, success, notes) \
         VALUES ($id, $decision_id, $scope, $success, $notes)",
        &HashMap::from([
            ("id".to_string(), Value::Text(id.to_string())),
            ("decision_id".to_string(), Value::Text(DECISION.to_string())),
            ("scope".to_string(), Value::Text(SCOPE.to_string())),
            ("success".to_string(), Value::Bool(success)),
            ("notes".to_string(), Value::Text(notes.to_string())),
        ]),
    )
    .expect("record the outcome");
}

/// Every outcome row this database holds for the contended key, as
/// (id, notes) — identity AND content, because adopting the surviving row
/// means adopting both.
fn outcomes(db: &Database) -> Vec<(String, String)> {
    let result = db
        .execute(
            "SELECT id, notes FROM outcomes WHERE scope = $scope AND decision_id = $decision_id",
            &HashMap::from([
                ("scope".to_string(), Value::Text(SCOPE.to_string())),
                ("decision_id".to_string(), Value::Text(DECISION.to_string())),
            ]),
        )
        .expect("outcome scan");
    result
        .rows
        .iter()
        .map(|row| match (&row[0], &row[1]) {
            (Value::Text(id), Value::Text(notes)) => (id.clone(), notes.clone()),
            other => panic!("expected text id and notes, got {other:?}"),
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
    declare(&db);
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
    declare(&db);
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

async fn drain_push(client: &SyncClient) {
    for _ in 0..32 {
        match client.has_pending_push_changes() {
            Ok(true) => {}
            _ => break,
        }
        if within(client.push()).await.is_err() {
            break;
        }
    }
}

async fn drain_pull(client: &SyncClient) {
    for _ in 0..32 {
        let before = client.pull_watermark();
        within(client.pull_default()).await.expect("pull round");
        if client.pull_watermark() == before {
            break;
        }
    }
}

/// Both edges record a status for the same thing at the same time, each with
/// its own fresh identity. After both push and both pull, all three databases
/// must hold ONE outcome row, and it must be the SAME one.
#[tokio::test]
async fn two_edges_recording_the_same_status_converge_on_one_surviving_row() {
    let tenant = "keep-latest-identity-collision";
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, tenant);

    let edge1 = open_edge();
    let edge1_client = edge_client(&edge1, &broker, tenant);
    let edge2 = open_edge();
    let edge2_client = edge_client(&edge2, &broker, tenant);

    // The parent exists everywhere before the contention.
    seed_decision(&edge1);
    drain_push(&edge1_client).await;
    drain_pull(&edge2_client).await;
    drain_pull(&edge1_client).await;

    record_outcome(&edge1, "outcome-from-edge1", true, "recorded by edge one");
    record_outcome(&edge2, "outcome-from-edge2", false, "recorded by edge two");

    // Edge one offers its row first, so the hub decides on it first.
    drain_push(&edge1_client).await;
    drain_push(&edge2_client).await;
    drain_pull(&edge1_client).await;
    drain_pull(&edge2_client).await;

    let settled = outcomes(&hub.db);
    assert_eq!(
        settled.len(),
        1,
        "the hub holds exactly one status row for the contended key: {settled:?}"
    );
    let surviving = settled[0].clone();

    assert_eq!(
        outcomes(&edge1),
        vec![surviving.clone()],
        "edge one must hold exactly the row the hub settled on — same identity, \
         same content"
    );
    assert_eq!(
        outcomes(&edge2),
        vec![surviving.clone()],
        "edge two must hold exactly the row the hub settled on; keeping its own \
         colliding row is a split no later sync repairs"
    );

    hub.stop().await;
}

/// The same collision named concretely, on the question convergence alone does
/// not answer: WHICH row survives.
///
/// `KEEP LATEST` keeps the value the hub accepted LAST. On this shape "a value"
/// is a whole row, identity included: the status is replace-not-version, so
/// recording a new one means a new row with its own id under the same natural
/// key, and superseding the previous status necessarily supersedes its id too.
/// So the row accepted second is the surviving one everywhere — on the hub, on
/// the machine that wrote the earlier row once it pulls, and on the machine
/// that wrote the later one. Which competing status is truly CURRENT is a
/// judgment made above this layer; transport's job is only that all three
/// machines agree, deterministically, on the same surviving row.
#[tokio::test]
async fn the_row_the_hub_accepted_last_survives_on_every_machine() {
    let tenant = "keep-latest-identity-collision-last-accepted-survives";
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, tenant);

    let edge1 = open_edge();
    let edge1_client = edge_client(&edge1, &broker, tenant);
    let edge2 = open_edge();
    let edge2_client = edge_client(&edge2, &broker, tenant);

    seed_decision(&edge1);
    drain_push(&edge1_client).await;
    drain_pull(&edge2_client).await;
    drain_pull(&edge1_client).await;

    record_outcome(&edge1, "outcome-from-edge1", true, "recorded by edge one");
    within(edge1_client.push())
        .await
        .expect("edge one's status reaches the hub");
    assert_eq!(
        outcomes(&hub.db),
        vec![(
            "outcome-from-edge1".to_string(),
            "recorded by edge one".to_string()
        )],
        "the hub accepted edge one's status first"
    );

    record_outcome(&edge2, "outcome-from-edge2", false, "recorded by edge two");
    drain_push(&edge2_client).await;
    drain_pull(&edge2_client).await;
    drain_pull(&edge1_client).await;

    let settled = outcomes(&hub.db);
    assert_eq!(
        settled,
        vec![(
            "outcome-from-edge2".to_string(),
            "recorded by edge two".to_string()
        )],
        "the hub holds the status it accepted last, identity included: {settled:?}"
    );
    assert_eq!(
        outcomes(&edge1),
        settled,
        "the machine that wrote the earlier status adopts the later one by exact \
         id and content on its pull — keeping its own row would be a split no \
         later sync repairs"
    );
    assert_eq!(
        outcomes(&edge2),
        settled,
        "the machine that wrote the surviving status keeps it"
    );

    hub.stop().await;
}
