//! A pusher whose write the hub refused must still be SERVED the value that
//! won.
//!
//! Exactly-once work depends on it. Two executors race a claim at the same
//! key; the hub accepts the first and refuses the second, correctly. The loser
//! then pulls. If that pull delivers nothing, the loser re-reads its own
//! refused row, still sees itself as the holder, and runs the job the winner is
//! already running — one job, two executors, no error anywhere.
//!
//! The single variable is the refused push. The control below is the same race
//! with the loser never pushing: there the pull converges. So anything the
//! refusal leaves behind in the loser's delivery bookkeeping is what decides
//! whether the winner is ever served.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads; every
//! assertion reads state after a bounded exchange returns.

use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// The engine-owned claim shape: the claim key is the identity, the holder is
/// content. `KEEP FIRST` is what makes the first accepted claim the winner.
const GROUP_DDL: &str = "CREATE TABLE claim_groups (id TEXT PRIMARY KEY, label TEXT) \
     SYNC TWO WAY SYNC CONFLICT KEEP FIRST";
/// The claim references its group, so a push carrying both is ONE connected,
/// dependency-complete unit — the shape whose refusal is decided as a single
/// acceptance and retires the resend obligation with a receipt.
const DDL: &str = "CREATE TABLE relay_claims (claim_key TEXT PRIMARY KEY, \
     group_id TEXT REFERENCES claim_groups(id), holder TEXT) \
     SYNC TWO WAY SYNC CONFLICT KEEP FIRST";

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), fut)
        .await
        .expect("bounded sync exchange exceeded 60s")
}

/// Claim the key as one connected unit: the group row and the claim that
/// references it, committed together.
fn claim(db: &Database, key: &str, holder: &str) {
    let tx = db.begin().expect("begin claim transaction");
    db.execute_in_tx(
        tx,
        "INSERT INTO claim_groups (id, label) VALUES ($id, $label)",
        &HashMap::from([
            (
                "id".to_string(),
                Value::Text(format!("{key}-{holder}-group")),
            ),
            ("label".to_string(), Value::Text(holder.to_string())),
        ]),
    )
    .expect("insert claim group");
    db.execute_in_tx(
        tx,
        "INSERT INTO relay_claims (claim_key, group_id, holder) \
         VALUES ($claim_key, $group_id, $holder)",
        &HashMap::from([
            ("claim_key".to_string(), Value::Text(key.to_string())),
            (
                "group_id".to_string(),
                Value::Text(format!("{key}-{holder}-group")),
            ),
            ("holder".to_string(), Value::Text(holder.to_string())),
        ]),
    )
    .expect("insert claim");
    db.commit(tx).expect("commit the claim unit");
}

/// Who this database believes holds the claim — the read the ledger's
/// Won/Lost verdict is derived from.
fn holder(db: &Database, key: &str) -> Option<String> {
    let result = db
        .execute(
            "SELECT holder FROM relay_claims WHERE claim_key = $claim_key",
            &HashMap::from([("claim_key".to_string(), Value::Text(key.to_string()))]),
        )
        .expect("claim scan");
    result.rows.first().map(|row| match &row[0] {
        Value::Text(holder) => holder.clone(),
        other => panic!("expected a text holder, got {other:?}"),
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
    db.execute(GROUP_DDL, &p()).expect("hub group table");
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
    db.execute(GROUP_DDL, &p()).expect("edge group table");
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
async fn a_refused_pusher_is_served_the_winning_claim_on_its_next_pull() {
    let tenant = "refused-pusher-receives-winner";
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, tenant);

    // The loser is an ESTABLISHED edge: it pulls BEFORE the race, so it carries
    // a real pull cursor rather than starting from nothing. A cursor the
    // refusal later advances past the winner's acceptance is invisible to an
    // edge whose cursor still sits at zero.
    let loser = open_edge();
    let loser_client = edge_client(&loser, &broker, tenant);
    within(loser_client.pull_default())
        .await
        .expect("the loser establishes its pull cursor before the race");

    // Winner: edge one claims and its push is accepted.
    let winner = open_edge();
    let winner_client = edge_client(&winner, &broker, tenant);
    claim(&winner, "job-1", "edge-one");
    within(winner_client.push())
        .await
        .expect("the winning claim reaches the hub");
    assert_eq!(
        holder(&hub.db, "job-1").as_deref(),
        Some("edge-one"),
        "the hub holds the first accepted claim"
    );

    // Loser: edge two claims the same key and pushes. The hub refuses, which
    // is correct — and is the single variable this test isolates.
    // The loser claims the same key concurrently — it has not seen the
    // winner's claim, which is what makes this a race.
    claim(&loser, "job-1", "edge-two");
    let refusal = within(loser_client.push())
        .await
        .expect("the losing push is a handled refusal, not a transport error");
    assert!(
        !refusal.conflicts.is_empty(),
        "the hub must visibly refuse the second claim: {refusal:?}"
    );
    assert_eq!(
        holder(&hub.db, "job-1").as_deref(),
        Some("edge-one"),
        "the refusal leaves the winner in place on the hub"
    );

    // The refused unit mixes two kinds of member. The claim collides with a
    // value the hub already accepted, so it reports who won and where that
    // acceptance sits. The loser's own group row is one the hub has never
    // seen: nobody won it, so it reports no winner at all and instead names
    // the claim whose winner refused the unit it belongs to. Inventing a
    // winner for it would be a lie; dropping it would leave the refusal
    // silent about a row the pusher still holds.
    let diagnostics = serde_json::to_value(&refusal.conflicts)
        .expect("serialize typed refusal diagnostics")
        .as_array()
        .cloned()
        .expect("diagnostics serialize as an ordered array");
    assert_eq!(
        diagnostics.len(),
        2,
        "both members of the refused unit are reported: {diagnostics:?}"
    );
    let claim_diagnostic = diagnostics
        .iter()
        .find(|entry| entry.get("table") == Some(&json!("relay_claims")))
        .expect("the colliding claim is reported");
    assert_eq!(
        claim_diagnostic.get("natural_key"),
        Some(&json!({ "column": "claim_key", "value": { "Text": "job-1" }, "rest": [] })),
        "the colliding member is identified by its claim key"
    );
    assert!(
        claim_diagnostic
            .get("winning_author_node_id")
            .is_some_and(|author| author.is_string()),
        "the colliding member names the edge whose claim the hub kept: {claim_diagnostic:?}"
    );
    assert!(
        claim_diagnostic.get("hub_acceptance_position").is_some(),
        "the colliding member reports where the winner was accepted: {claim_diagnostic:?}"
    );
    assert_eq!(
        claim_diagnostic.get("refusal_cause"),
        None,
        "a member with its own winner reports that winner, not a sibling cause"
    );

    let group_diagnostic = diagnostics
        .iter()
        .find(|entry| entry.get("table") == Some(&json!("claim_groups")))
        .expect("the loser's own group row is reported too");
    assert_eq!(
        group_diagnostic.get("natural_key"),
        Some(&json!({
            "column": "id",
            "value": { "Text": "job-1-edge-two-group" },
            "rest": [],
        })),
        "the winnerless member is identified by its own key"
    );
    assert_eq!(
        group_diagnostic.get("winning_author_node_id"),
        None,
        "a row no one else ever wrote has no winning author to report: \
         {group_diagnostic:?}"
    );
    assert_eq!(
        group_diagnostic.get("hub_acceptance_position"),
        None,
        "a row the hub never accepted has no acceptance position: \
         {group_diagnostic:?}"
    );
    assert_eq!(
        group_diagnostic.get("refusal_cause"),
        Some(&json!({
            "table": "relay_claims",
            "natural_key": { "column": "claim_key", "value": { "Text": "job-1" }, "rest": [] },
        })),
        "the winnerless member names the claim whose winner refused the unit: \
         {group_diagnostic:?}"
    );

    // The loser now pulls. It must be SERVED the winning claim: without it the
    // loser still reads itself as holder and runs a job that is already
    // running elsewhere.
    let pulled = within(loser_client.pull_default())
        .await
        .expect("the refused pusher can pull");

    assert!(
        pulled.applied_rows >= 1 || !pulled.conflicts.is_empty(),
        "the hub must serve the winning claim to the edge whose claim it \
         refused; delivering nothing leaves the loser believing it won: {pulled:?}"
    );
    assert_eq!(
        holder(&loser, "job-1").as_deref(),
        Some("edge-one"),
        "after the pull the loser must read the WINNER as holder — this is the \
         read the exactly-once verdict is derived from"
    );

    hub.stop().await;
}

/// The control: the identical race with the loser never pushing. Its pull
/// converges today, so a failure above is caused by what the refused push
/// leaves behind, not by two-edge divergence itself.
#[tokio::test]
async fn without_the_refused_push_the_same_race_converges_on_pull() {
    let tenant = "refused-pusher-control";
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, tenant);

    let winner = open_edge();
    let winner_client = edge_client(&winner, &broker, tenant);
    claim(&winner, "job-1", "edge-one");
    within(winner_client.push())
        .await
        .expect("the winning claim reaches the hub");

    let loser = open_edge();
    let loser_client = edge_client(&loser, &broker, tenant);
    claim(&loser, "job-1", "edge-two");

    let pulled = within(loser_client.pull_default())
        .await
        .expect("the never-pushing edge can pull");
    assert!(
        pulled.applied_rows >= 1 || !pulled.conflicts.is_empty(),
        "the control must receive the winning claim: {pulled:?}"
    );
    assert_eq!(
        holder(&loser, "job-1").as_deref(),
        Some("edge-one"),
        "without a refused push the loser converges on the winner"
    );

    hub.stop().await;
}
