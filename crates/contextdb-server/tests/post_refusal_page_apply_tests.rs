//! The pull a refused pusher makes to find out who beat it must APPLY.
//!
//! A refused push is answered by re-reading the hub's history, which is how the
//! refused machine learns the value that won. That re-read repeats content the
//! machine already holds — including content that carries a vector and hangs off
//! a parent row. Repeating it must be a no-op, not a failure: if the apply dies
//! partway, the whole pull is rolled back, and the winning claim the machine was
//! re-reading history to find never lands.
//!
//! The cost is silent, which is what makes it serious. The caller pulls to learn
//! who won, gets an error it has no way to interpret, re-reads its OWN losing
//! claim, and runs work another machine is already running.
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

/// The parent every digest hangs off.
const CONTEXTS_DDL: &str = "CREATE TABLE contexts (id TEXT PRIMARY KEY, label TEXT) \
     SYNC TWO WAY SYNC CONFLICT KEEP FIRST";

/// Write-once content with a vector, hanging off its parent — so a page
/// carrying it is one connected, dependency-complete unit.
const DIGESTS_DDL: &str = "CREATE TABLE digests (id TEXT PRIMARY KEY, \
     context_id TEXT NOT NULL REFERENCES contexts(id), summary TEXT NOT NULL, \
     embedding VECTOR(3), UNIQUE (context_id, id)) \
     IMMUTABLE SYNC TWO WAY SYNC CONFLICT KEEP FIRST";

const NOW_MS: i64 = 1_000;
const LEASE_MS: i64 = 60_000;
const CONTEXT: &str = "context-1";

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), fut)
        .await
        .expect("bounded sync exchange exceeded 60s")
}

/// A node as the product opens one: the ledger from the shipped installer, plus
/// the application's own tables.
fn open_node() -> Arc<Database> {
    let db = Arc::new(Database::open_memory());
    ledger::install_work_ledger_schema(&db).expect("the shipped installer creates the ledger");
    db.execute(CONTEXTS_DDL, &p()).expect("declare contexts");
    db.execute(DIGESTS_DDL, &p()).expect("declare digests");
    db
}

fn write_context(db: &Database) {
    db.execute(
        "INSERT INTO contexts (id, label) VALUES ($id, $label)",
        &HashMap::from([
            ("id".to_string(), Value::Text(CONTEXT.to_string())),
            ("label".to_string(), Value::Text("shared".to_string())),
        ]),
    )
    .expect("write the parent context");
}

/// One digest and its vector, committed together with its parent already in
/// place — the connected unit a page carries.
fn write_digest(db: &Database, id: &str, summary: &str, vector: Vec<f32>) {
    db.execute(
        "INSERT INTO digests (id, context_id, summary, embedding) \
         VALUES ($id, $context_id, $summary, $embedding)",
        &HashMap::from([
            ("id".to_string(), Value::Text(id.to_string())),
            ("context_id".to_string(), Value::Text(CONTEXT.to_string())),
            ("summary".to_string(), Value::Text(summary.to_string())),
            ("embedding".to_string(), Value::Vector(vector)),
        ]),
    )
    .expect("write the digest");
}

fn digest_summaries(db: &Database) -> Vec<(String, String)> {
    let result = db
        .execute("SELECT id, summary FROM digests", &p())
        .expect("digest scan");
    let mut rows = result
        .rows
        .iter()
        .map(|row| match (&row[0], &row[1]) {
            (Value::Text(id), Value::Text(summary)) => (id.clone(), summary.clone()),
            other => panic!("expected text id and summary, got {other:?}"),
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

fn submit(db: &Database, job_id: &str) {
    let spec = JobSpec::builder(job_id, "understanding", "shared", "edge-one")
        .max_attempts(3)
        .submitted_at_ms(NOW_MS)
        .build();
    ledger::submit_job(db, &spec, &[b"one chunk of work".as_slice()]).expect("submit the job");
}

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
    let db = open_node();
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

/// Two machines that each authored their own vector-bearing content, converged
/// through the hub, and are now about to race one claim. Authoring on BOTH
/// edges is what makes each machine's row numbering its own: the hub's number
/// for a digest is not the number either edge gave it.
async fn converged_race(
    broker: &InProcessBroker,
    tenant: &str,
) -> (
    RunningHub,
    Arc<Database>,
    SyncClient,
    Arc<Database>,
    SyncClient,
) {
    let hub = start_hub(broker, tenant);
    write_context(&hub.db);

    let winner = open_node();
    let winner_client = edge_client(&winner, broker, tenant);
    let loser = open_node();
    let loser_client = edge_client(&loser, broker, tenant);

    drain_pull(&winner_client).await;
    drain_pull(&loser_client).await;

    // The loser authors first and alone, so its own numbering starts ahead of
    // anything it later receives.
    write_digest(&loser, "digest-loser", "authored here", vec![1.0, 0.0, 0.0]);
    drain_push(&loser_client).await;

    // The winner authors several, so the hub's numbering runs ahead of the
    // loser's for every one of them.
    for index in 0..4 {
        write_digest(
            &winner,
            &format!("digest-winner-{index}"),
            "authored elsewhere",
            vec![0.0, index as f32, 0.0],
        );
    }
    submit(&winner, "job-1");
    drain_push(&winner_client).await;

    drain_pull(&loser_client).await;
    drain_pull(&winner_client).await;

    (hub, winner, winner_client, loser, loser_client)
}

/// Facet one: the reconcile pull must apply.
#[tokio::test]
async fn the_pull_that_follows_a_refused_push_applies_instead_of_failing() {
    let tenant = "post-refusal-page-applies";
    let broker = InProcessBroker::new();
    let (hub, winner, winner_client, loser, loser_client) = converged_race(&broker, tenant).await;

    let before = digest_summaries(&loser);
    assert_eq!(
        before,
        digest_summaries(&hub.db),
        "both machines hold the same content before the race"
    );

    ledger::insert_claim(&winner, "job-1", 1, "edge-one", NOW_MS + LEASE_MS, NOW_MS)
        .expect("winner records its claim locally");
    within(winner_client.push())
        .await
        .expect("the winning claim reaches the hub");

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

    let pulled = within(loser_client.pull_default()).await;
    assert!(
        pulled.is_ok(),
        "the pull that follows a refusal must apply; failing it rolls back the \
         winning claim it was fetched to deliver: {pulled:?}"
    );
    assert_eq!(
        digest_summaries(&loser),
        before,
        "repeating content this machine already holds changes none of it"
    );
    assert_eq!(
        holder(&loser, "job-1", 1).as_deref(),
        Some("edge-one"),
        "and the winner it went to find has landed"
    );

    hub.stop().await;
}

/// Facet two: the claim verdict must never say this machine holds the claim on
/// the strength of a reconcile pull that did not succeed.
///
/// The verdict is read from a local row that only becomes trustworthy once the
/// hub's answer has been applied. When that pull fails, the local row still says
/// what this machine wrote before the refusal — itself. Reporting a win from it
/// turns any pull failure into two machines running one job, with no error
/// raised anywhere.
#[tokio::test]
async fn a_claim_verdict_never_reports_a_win_when_its_reconcile_pull_failed() {
    let tenant = "post-refusal-verdict-honesty";
    let broker = InProcessBroker::new();
    let (hub, _winner, winner_client, _loser, loser_client) = converged_race(&broker, tenant).await;

    let won = within(claim_job(
        &winner_client,
        "job-1",
        1,
        "edge-one",
        NOW_MS + LEASE_MS,
        NOW_MS,
    ))
    .await
    .expect("the first claim is a handled outcome");
    assert_eq!(
        won,
        ClaimOutcome::Won { synced: true },
        "the first claim to reach the hub wins it"
    );

    let verdict = within(claim_job(
        &loser_client,
        "job-1",
        1,
        "edge-two",
        NOW_MS + LEASE_MS,
        NOW_MS,
    ))
    .await
    .expect("the second claim is a handled outcome, not a transport error");
    assert_ne!(
        verdict,
        ClaimOutcome::Won { synced: true },
        "a machine the hub refused must never report a synced win: its own \
         claim row is not evidence, and the pull that would have corrected it \
         is the thing that failed"
    );
    assert_eq!(
        holder(&hub.db, "job-1", 1).as_deref(),
        Some("edge-one"),
        "the hub still holds exactly one winner"
    );

    hub.stop().await;
}
