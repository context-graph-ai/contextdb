//! Two workers racing one claim on the work ledger AS THE PRODUCT INSTALLS IT.
//!
//! Every node here gets its work ledger from the shipped installer — the same
//! call a real node makes at open — and never types a `CREATE TABLE`. That is
//! the only difference from the races already covered: those author the claim
//! table by hand and spell the arbitration and the key out in the DDL.
//!
//! The promise under test is the exactly-once one the fabric rests on: two
//! machines that claim the same unit of work must not both believe they hold
//! it. The hub keeps the first accepted claim; the machine whose claim was
//! refused must, on its very next pull, read the WINNER as the holder. If it
//! instead re-reads its own refused row, both machines run the same job and no
//! error is raised anywhere.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads; every
//! assertion reads state after a bounded exchange returns.

use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_engine::work_ledger::{self as ledger, JobSpec};
use contextdb_server::work_ledger::{ClaimOutcome, claim_job};
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// A fixed logical clock. Nothing here reads a real one.
const NOW_MS: i64 = 1_000;
const LEASE_MS: i64 = 60_000;

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), fut)
        .await
        .expect("bounded sync exchange exceeded 60s")
}

/// A node exactly as the product opens one: the ledger arrives from the
/// installer, never from hand-typed DDL.
fn open_installed_node() -> Arc<Database> {
    let db = Arc::new(Database::open_memory());
    ledger::install_work_ledger_schema(&db).expect("the shipped installer creates the ledger");
    db
}

fn submit(db: &Database, job_id: &str) {
    let spec = JobSpec::builder(job_id, "understanding", "shared", "edge-one")
        .max_attempts(3)
        .submitted_at_ms(NOW_MS)
        .build();
    ledger::submit_job(db, &spec, &[b"one chunk of work".as_slice()]).expect("submit the job");
}

/// Who this database believes holds the claim — the read the Won/Lost verdict
/// is derived from.
fn holder(db: &Database, job_id: &str, attempt: i64) -> Option<String> {
    ledger::claim_holder(db, job_id, attempt).expect("claim holder read")
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
    let db = open_installed_node();
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

/// Drain a pull until it stops making progress, so "the edge did not converge"
/// can never be confused with "the edge pulled only one page".
async fn drain_pull(client: &SyncClient) {
    for _ in 0..32 {
        let before = client.pull_watermark();
        within(client.pull_default()).await.expect("pull round");
        if client.pull_watermark() == before {
            break;
        }
    }
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

/// An ordinary application table with a trigger on it, declared the way a
/// product declares its schema: on every node, before any sync.
fn declare_application_table_with_trigger(db: &Database) {
    db.execute(
        "CREATE TABLE observations (id TEXT PRIMARY KEY, note TEXT) SYNC TWO WAY",
        &p(),
    )
    .expect("declare the application table");
    db.execute(
        "CREATE TRIGGER observation_watch ON observations WHEN INSERT",
        &p(),
    )
    .expect("declare the trigger on it");
    db.register_trigger_callback("observation_watch", |_db, _context| Ok(()))
        .expect("register the trigger's callback");
    db.complete_initialization()
        .expect("the node is ready to serve and apply");
}

/// The exactly-once promise, run on the ledger the product actually installs.
#[tokio::test]
async fn two_installed_nodes_racing_one_claim_agree_on_the_holder() {
    let tenant = "installed-ledger-claim-race";
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, tenant);

    let winner = open_installed_node();
    let winner_client = edge_client(&winner, &broker, tenant);
    let loser = open_installed_node();
    let loser_client = edge_client(&loser, &broker, tenant);

    // The job is submitted on one node and reaches the other through the hub,
    // exactly as a peer's work becomes visible in production.
    submit(&winner, "job-1");
    drain_push(&winner_client).await;
    drain_pull(&loser_client).await;
    drain_pull(&winner_client).await;

    let won = within(claim_job(
        &winner_client,
        "job-1",
        1,
        "edge-one",
        NOW_MS + LEASE_MS,
        NOW_MS,
    ))
    .await
    .expect("the first claim is a handled outcome, not a transport error");
    assert_eq!(
        won,
        ClaimOutcome::Won { synced: true },
        "the first claim to reach the hub wins it"
    );
    assert_eq!(
        holder(&hub.db, "job-1", 1).as_deref(),
        Some("edge-one"),
        "the hub holds the first accepted claim"
    );

    let lost = within(claim_job(
        &loser_client,
        "job-1",
        1,
        "edge-two",
        NOW_MS + LEASE_MS,
        NOW_MS,
    ))
    .await
    .expect("the second claim is a handled outcome, not a transport error");
    assert_eq!(
        lost,
        ClaimOutcome::Lost {
            holder: Some("edge-one".to_string())
        },
        "the second machine must learn it lost and who won — believing it won \
         runs the job twice"
    );
    assert_eq!(
        holder(&loser, "job-1", 1).as_deref(),
        Some("edge-one"),
        "the refused machine must read the WINNER as holder after the pull the \
         claim path performs; reading itself means two executors on one job"
    );
    assert_eq!(
        holder(&hub.db, "job-1", 1).as_deref(),
        Some("edge-one"),
        "the refusal leaves the winner in place on the hub"
    );

    hub.stop().await;
}

/// The same race, isolating the DELIVERY half: after the refusal the loser
/// pulls on its own. That pull must serve it the winning claim.
#[tokio::test]
async fn a_refused_installed_node_is_served_the_winning_claim_on_its_next_pull() {
    let tenant = "installed-ledger-refused-delivery";
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, tenant);

    let winner = open_installed_node();
    let winner_client = edge_client(&winner, &broker, tenant);
    let loser = open_installed_node();
    let loser_client = edge_client(&loser, &broker, tenant);

    submit(&winner, "job-1");
    drain_push(&winner_client).await;
    drain_pull(&loser_client).await;

    // Both claim the same key by hand, so the race is unambiguous, then each
    // offers its claim to the hub in turn.
    ledger::insert_claim(&winner, "job-1", 1, "edge-one", NOW_MS + LEASE_MS, NOW_MS)
        .expect("winner records its claim locally");
    within(winner_client.push())
        .await
        .expect("the winning claim reaches the hub");
    assert_eq!(
        holder(&hub.db, "job-1", 1).as_deref(),
        Some("edge-one"),
        "the hub holds the first accepted claim"
    );

    ledger::insert_claim(&loser, "job-1", 1, "edge-two", NOW_MS + LEASE_MS, NOW_MS)
        .expect("loser records its own claim locally");
    let refusal = within(loser_client.push())
        .await
        .expect("the losing push is a handled refusal, not a transport error");
    assert!(
        refusal
            .conflicts
            .iter()
            .any(|conflict| conflict.table.as_deref() == Some("work_claims")),
        "the hub must visibly refuse the second claim: {refusal:?}"
    );

    drain_pull(&loser_client).await;
    assert_eq!(
        holder(&loser, "job-1", 1).as_deref(),
        Some("edge-one"),
        "the hub must serve the winning claim to the machine whose claim it \
         refused; delivering nothing leaves that machine believing it won"
    );

    hub.stop().await;
}

/// The same refusal-then-pull, on a hub that carries an application table with
/// a trigger and a history longer than one served page — the ordinary shape of
/// a product database, and the shape the fabric actually runs on.
///
/// The exactly-once promise is not qualified by how much unrelated history the
/// hub holds or by what else lives in its schema: the machine whose claim was
/// refused must be served the winner by the pull its claim path performs.
#[tokio::test]
async fn a_refused_node_is_served_the_winner_even_on_a_hub_with_triggered_history() {
    let tenant = "installed-ledger-refused-delivery-with-history";
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, tenant);

    // An application table with a trigger, alongside the ledger, on every node.
    declare_application_table_with_trigger(&hub.db);

    let winner = open_installed_node();
    declare_application_table_with_trigger(&winner);
    let winner_client = edge_client(&winner, &broker, tenant);
    let loser = open_installed_node();
    declare_application_table_with_trigger(&loser);
    let loser_client = edge_client(&loser, &broker, tenant);

    submit(&winner, "job-1");
    drain_push(&winner_client).await;

    // Ordinary history: enough separately-committed rows that a from-scratch
    // re-read of this hub cannot complete inside a single pull.
    for index in 0..40 {
        hub.db
            .execute(
                "INSERT INTO observations (id, note) VALUES ($id, $note)",
                &HashMap::from([
                    (
                        "id".to_string(),
                        Value::Text(format!("observation-{index}")),
                    ),
                    ("note".to_string(), Value::Text("ordinary work".to_string())),
                ]),
            )
            .expect("hub records ordinary history");
    }

    // Both machines are established edges: each carries a real cursor into that
    // history before the race.
    drain_pull(&loser_client).await;
    drain_pull(&winner_client).await;

    ledger::insert_claim(&winner, "job-1", 1, "edge-one", NOW_MS + LEASE_MS, NOW_MS)
        .expect("winner records its claim locally");
    within(winner_client.push())
        .await
        .expect("the winning claim reaches the hub");
    assert_eq!(
        holder(&hub.db, "job-1", 1).as_deref(),
        Some("edge-one"),
        "the hub holds the first accepted claim"
    );

    ledger::insert_claim(&loser, "job-1", 1, "edge-two", NOW_MS + LEASE_MS, NOW_MS)
        .expect("loser records its own claim locally");
    let refusal = within(loser_client.push())
        .await
        .expect("the losing push is a handled refusal, not a transport error");
    assert!(
        refusal
            .conflicts
            .iter()
            .any(|conflict| conflict.table.as_deref() == Some("work_claims")),
        "the hub must visibly refuse the second claim: {refusal:?}"
    );

    // The pull the claim path performs — one bounded exchange, exactly what a
    // worker does before deciding whether it holds the claim.
    let pulled = within(loser_client.pull_default())
        .await
        .expect("the refused pusher can pull");
    assert_eq!(
        holder(&loser, "job-1", 1).as_deref(),
        Some("edge-one"),
        "the refused machine must read the WINNER as holder after its next \
         pull; reading itself is two executors on one job: {pulled:?}"
    );

    hub.stop().await;
}

/// Even given many further pulls, the refused machine must converge on the
/// winner. This separates "one pull was not enough" from "these pulls never
/// deliver the winning row at all".
#[tokio::test]
async fn a_refused_node_converges_on_the_winner_within_bounded_pulls() {
    let tenant = "installed-ledger-refused-bounded-convergence";
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, tenant);

    // An application table with a trigger, alongside the ledger, on every node.
    declare_application_table_with_trigger(&hub.db);

    let winner = open_installed_node();
    declare_application_table_with_trigger(&winner);
    let winner_client = edge_client(&winner, &broker, tenant);
    let loser = open_installed_node();
    declare_application_table_with_trigger(&loser);
    let loser_client = edge_client(&loser, &broker, tenant);

    submit(&winner, "job-1");
    drain_push(&winner_client).await;
    for index in 0..40 {
        hub.db
            .execute(
                "INSERT INTO observations (id, note) VALUES ($id, $note)",
                &HashMap::from([
                    (
                        "id".to_string(),
                        Value::Text(format!("observation-{index}")),
                    ),
                    ("note".to_string(), Value::Text("ordinary work".to_string())),
                ]),
            )
            .expect("hub records ordinary history");
    }
    drain_pull(&loser_client).await;
    drain_pull(&winner_client).await;

    ledger::insert_claim(&winner, "job-1", 1, "edge-one", NOW_MS + LEASE_MS, NOW_MS)
        .expect("winner records its claim locally");
    within(winner_client.push())
        .await
        .expect("the winning claim reaches the hub");

    ledger::insert_claim(&loser, "job-1", 1, "edge-two", NOW_MS + LEASE_MS, NOW_MS)
        .expect("loser records its own claim locally");
    within(loser_client.push())
        .await
        .expect("the losing push is a handled refusal, not a transport error");

    drain_pull(&loser_client).await;
    assert_eq!(
        holder(&loser, "job-1", 1).as_deref(),
        Some("edge-one"),
        "draining every page the hub will serve must leave the refused machine \
         reading the winner; still reading itself means the winning row is \
         never delivered at all"
    );

    hub.stop().await;
}
