//! Cross-sender `LatestWins` arbitration correctness.
//!
//! `LatestWins` today compares the INCOMING row's own sender-local LSN
//! against whatever LSN happens to be already stored (`database.rs`,
//! `sync_latest_wins_incoming`). Across two different senders — or an edge
//! and the hub that authored a row locally — those clocks are unrelated
//! counters. A quiet sender's genuinely later write can lose forever to a
//! chattier sender's earlier one, and the push watermark then steps over the
//! refusal (`has_retryable_legacy_lsn_conflict` never matches its own
//! producer's reason string), so the row is never re-offered: the fleet
//! converges on a stale value, permanently and silently. The same defect
//! ages a rebuilt node's capability advertisement out of the fleet, because
//! `work_capabilities` is baked `LatestWins` precisely so a re-advertise can
//! refresh it.
//!
//! Contract once fixed: arbitration compares one clock — the accepting
//! node's own ordering of arrivals — never two machines' independent
//! counters. A row that carries no prior ordering position (a fresh local
//! write, never before synced) always wins over whatever is already held. A
//! row that already carries an ordering position at or below what is stored
//! is a stale echo: it must not regress the current value, and it must not
//! be reported as a conflict — nothing was refused, there was nothing new.
//! A conflict that CAN legitimately still refuse (e.g. `ServerWins`) keeps
//! advancing the push watermark exactly once, so a refusal no later push
//! could ever win is never silently retried forever either.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads.
//! Every assertion is by VALUE, read back off the `ApplyResult` / row content
//! the library returns.

use contextdb_core::{Lsn, TenantId, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{
    ChangeSet, ConflictPolicies, ConflictPolicy, NaturalKey, RowChange,
};
use contextdb_server::{InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const DDL: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)";
const T0: i64 = 1_700_000_000_000;

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

struct RunningHub {
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl RunningHub {
    async fn stop(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = self.task.await;
    }
}

fn start_hub_with_policy(
    broker: &InProcessBroker,
    tenant: &str,
    hub_db: Arc<Database>,
    policies: ConflictPolicies,
) -> RunningHub {
    let server = Arc::new(SyncServer::with_transport(
        hub_db,
        broker.server(),
        TenantId::from(tenant),
        policies,
    ));
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    RunningHub { shutdown, task }
}

fn start_hub(broker: &InProcessBroker, tenant: &str, hub_db: Arc<Database>) -> RunningHub {
    start_hub_with_policy(
        broker,
        tenant,
        hub_db,
        ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
}

fn edge(broker: &InProcessBroker, tenant: &str) -> (Arc<Database>, SyncClient) {
    let db = Arc::new(Database::open_memory());
    db.execute(DDL, &p()).expect("edge ddl");
    let client = SyncClient::with_transport(db.clone(), broker.client(), TenantId::from(tenant));
    (db, client)
}

fn edge_with_ledger(broker: &InProcessBroker, tenant: &str) -> (Arc<Database>, SyncClient) {
    let db = Arc::new(Database::open_memory());
    contextdb_engine::work_ledger::install_work_ledger_schema(&db)
        .expect("install ledger schema on edge");
    let client = SyncClient::with_transport(db.clone(), broker.client(), TenantId::from(tenant));
    (db, client)
}

fn only_body(db: &Database, id: i64) -> Option<String> {
    let mut params = p();
    params.insert("id".to_string(), Value::Int64(id));
    let result = db
        .execute("SELECT body FROM notes WHERE id = $id", &params)
        .expect("notes scan");
    result.rows.into_iter().next().map(|row| match &row[0] {
        Value::Text(body) => body.clone(),
        other => panic!("expected a text body, got {other:?}"),
    })
}

fn write_filler(db: &Database, count: i64, prefix: &str) {
    for i in 0..count {
        let mut row = p();
        row.insert("id".to_string(), Value::Int64(1_000_000 + i));
        row.insert("body".to_string(), Value::Text(format!("{prefix}-{i}")));
        db.execute("INSERT INTO notes (id, body) VALUES ($id, $body)", &row)
            .unwrap_or_else(|err| panic!("filler write {i}: {err}"));
    }
}

fn write_key(db: &Database, id: i64, body: &str) {
    let mut row = p();
    row.insert("id".to_string(), Value::Int64(id));
    row.insert("body".to_string(), Value::Text(body.to_string()));
    db.execute("INSERT INTO notes (id, body) VALUES ($id, $body)", &row)
        .unwrap_or_else(|err| panic!("write key {id}: {err}"));
}

/// The `work_capabilities.tags` JSON array for `node_id`, as plain strings —
/// `None` when the hub holds no row for it.
fn capability_tags(db: &Database, node_id: &str) -> Option<Vec<String>> {
    let mut params = p();
    params.insert("node_id".to_string(), Value::Text(node_id.to_string()));
    let result = db
        .execute(
            "SELECT tags FROM work_capabilities WHERE node_id = $node_id",
            &params,
        )
        .expect("work_capabilities scan");
    result.rows.into_iter().next().map(|row| match &row[0] {
        Value::Json(serde_json::Value::Array(items)) => items
            .iter()
            .map(|item| item.as_str().expect("tag must be a string").to_string())
            .collect(),
        other => panic!("expected a JSON tag array, got {other:?}"),
    })
}

// ---------------------------------------------------------------------------
// Arbitration compares one clock, and the wire says which.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_write_from_a_low_clock_sender_lands_over_a_high_clock_senders_value() {
    let broker = InProcessBroker::new();
    let tenant = "xsender-two-edges";
    let hub_db = Arc::new(Database::open_memory());
    hub_db.execute(DDL, &p()).expect("hub ddl");
    let hub = start_hub(&broker, tenant, hub_db.clone());

    let (b_db, b_client) = edge(&broker, tenant);
    let (a_db, a_client) = edge(&broker, tenant);
    let (c_db, c_client) = edge(&broker, tenant);

    // B is chatty: its clock runs far ahead before it writes and pushes the
    // shared key.
    write_filler(&b_db, 200, "b-filler");
    write_key(&b_db, 1, "from-B");
    within(b_client.push()).await.expect("B push");

    // A is quiet: it writes the SAME key LATER in real time, at a low local
    // clock, and pushes after B's traffic already landed on the hub.
    write_key(&a_db, 1, "from-A-LATER");
    let a_push = within(a_client.push()).await.expect("A push");

    assert_eq!(
        only_body(&hub_db, 1).as_deref(),
        Some("from-A-LATER"),
        "A's genuinely later write must win over B's earlier one on the hub, \
         regardless of B's higher local clock. A's push report: {a_push:?}"
    );

    within(c_client.pull_default()).await.expect("C pull");
    assert_eq!(
        only_body(&c_db, 1).as_deref(),
        Some("from-A-LATER"),
        "a third edge pulling from the hub must see A's value too, never \
         B's stale one"
    );

    hub.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_edge_write_lands_over_a_row_the_hub_authored() {
    let broker = InProcessBroker::new();
    let tenant = "xsender-hub-authored";
    let hub_db = Arc::new(Database::open_memory());
    hub_db.execute(DDL, &p()).expect("hub ddl");
    // The hub's own clock runs ahead before it authors the shared key
    // locally — no sidecar exists for this row, so the comparison falls
    // back to the hub's own commit LSN.
    write_filler(&hub_db, 300, "hub-filler");
    write_key(&hub_db, 1, "hub-original");
    let hub = start_hub(&broker, tenant, hub_db.clone());

    // A fresh edge, never pulled, so its own clock stays low. It writes the
    // SAME key the hub already authored.
    let (a_db, a_client) = edge(&broker, tenant);
    write_key(&a_db, 1, "edge-authored-LATER");
    let rep = within(a_client.push()).await.expect("A push");

    assert_eq!(
        only_body(&hub_db, 1).as_deref(),
        Some("edge-authored-LATER"),
        "an edge's fresh write must win over a row the hub authored \
         locally, regardless of the edge's low local clock. push report: \
         {rep:?}"
    );

    hub.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rebuilt_node_re_advertises_its_capability_to_the_fleet() {
    let broker = InProcessBroker::new();
    let tenant = "xsender-rebuild";
    let hub_db = Arc::new(Database::open_memory());
    let hub = start_hub(&broker, tenant, hub_db.clone());

    let (b_db, b_client) = edge_with_ledger(&broker, tenant);
    for i in 0..60 {
        contextdb_engine::work_ledger::advertise_capability(
            &b_db,
            "node-b",
            "wl.demo",
            &["class:wl.demo".to_string(), format!("gen:{i}")],
            T0 + i,
        )
        .expect("advertise");
    }
    within(b_client.push()).await.expect("b push");
    assert_eq!(
        capability_tags(&hub_db, "node-b"),
        Some(vec!["class:wl.demo".to_string(), "gen:59".to_string()]),
        "fixture: the hub must hold node-b's pre-rebuild advertisement"
    );

    // The node is wiped and rebuilt: a fresh store, a fresh incarnation, a
    // clock restarted near zero.
    let (b2_db, b2_client) = edge_with_ledger(&broker, tenant);
    contextdb_engine::work_ledger::advertise_capability(
        &b2_db,
        "node-b",
        "wl.demo",
        &["class:wl.demo".to_string(), "gen:rebuilt".to_string()],
        T0 + 9_000,
    )
    .expect("advertise after rebuild");
    let rep = within(b2_client.push()).await.expect("b2 push");

    assert_eq!(
        capability_tags(&hub_db, "node-b"),
        Some(vec!["class:wl.demo".to_string(), "gen:rebuilt".to_string()]),
        "the hub must hold the REBUILT node's present capability tags, not \
         the dead incarnation's stale advertisement — push report: {rep:?}"
    );

    // The hub holding it is not enough — the promise is FLEET visibility. A
    // third edge that pulls after the rebuild must see the rebuilt node's
    // present offer too, both through its raw registry row and through the
    // work-ledger's own capability query, the one a submitter would consult
    // before routing work to node-b.
    let (c_db, c_client) = edge_with_ledger(&broker, tenant);
    within(c_client.pull_default())
        .await
        .expect("c pull after rebuild");

    assert_eq!(
        capability_tags(&c_db, "node-b"),
        Some(vec!["class:wl.demo".to_string(), "gen:rebuilt".to_string()]),
        "a third edge pulling from the hub after the rebuild must see the \
         REBUILT node's present capability tags, not the dead incarnation's \
         stale advertisement"
    );
    assert_eq!(
        contextdb_engine::work_ledger::advertised_tags(&c_db, "node-b")
            .expect("advertised_tags scan on the third edge"),
        vec!["class:wl.demo".to_string(), "gen:rebuilt".to_string()],
        "the work-ledger's own capability query — what a submitter would \
         consult before routing work to node-b — must also reflect the \
         rebuilt node's present offer on the third edge"
    );

    hub.stop().await;
}

/// Engine-level: a deliberately nonconforming peer offers a row whose
/// ordering position is at or below what is already stored for the same
/// key — a stale echo. This must be an inert no-op: the current value must
/// not regress, and the row must not be reported anywhere on the receipt —
/// not applied, not skipped, not a conflict. "Conflict" means genuine
/// divergence between two machines that never saw each other's write, never
/// an old copy re-arriving behind the position the accepting node already
/// holds.
#[test]
fn a_stale_echo_cannot_regress_a_served_value() {
    let db = Database::open_memory();
    db.execute(DDL, &p()).expect("ddl");
    let policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);

    let newer = ChangeSet {
        rows: vec![RowChange {
            table: "notes".to_string(),
            natural_key: NaturalKey::single("id".to_string(), Value::Int64(1)),
            values: HashMap::from([
                ("id".to_string(), Value::Int64(1)),
                ("body".to_string(), Value::Text("v-new".to_string())),
            ]),
            deleted: false,
            lsn: Lsn(200),
            created_at: None,
        }],
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: Vec::new(),
        ddl_lsn: Vec::new(),
    };
    let applied = db.apply_changes(newer, &policies).expect("apply newer");
    assert_eq!(applied.applied_rows, 1, "fixture: the newer row must land");

    let echo = ChangeSet {
        rows: vec![RowChange {
            table: "notes".to_string(),
            natural_key: NaturalKey::single("id".to_string(), Value::Int64(1)),
            values: HashMap::from([
                ("id".to_string(), Value::Int64(1)),
                ("body".to_string(), Value::Text("v-stale".to_string())),
            ]),
            deleted: false,
            lsn: Lsn(150),
            created_at: None,
        }],
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: Vec::new(),
        ddl_lsn: Vec::new(),
    };
    let echoed = db.apply_changes(echo, &policies).expect("apply stale echo");

    assert_eq!(
        only_body(&db, 1).as_deref(),
        Some("v-new"),
        "a stale echo (an ordering position at or below the stored one) \
         must never regress the current value"
    );
    assert_eq!(
        echoed.applied_rows, 0,
        "a stale echo applies nothing: {echoed:?}"
    );
    assert_eq!(
        echoed.skipped_rows, 0,
        "a stale echo must not be counted as skipped either — nothing was \
         refused, there is simply nothing new. Got {echoed:?}"
    );
    assert!(
        echoed.conflicts.is_empty(),
        "a stale echo must not be reported as a conflict — 'conflict' means \
         genuine divergence, not an old copy re-arriving behind the current \
         position. Got {:?}",
        echoed.conflicts
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn arrival_positions_stay_ordered_across_a_restart_of_the_accepting_node() {
    let broker = InProcessBroker::new();
    let tenant = "xsender-restart";
    let dir = tempfile::TempDir::new().expect("tempdir");
    let hub_path = dir.path().join("hub.db");

    let hub_db1 = Arc::new(Database::open(&hub_path).expect("open hub"));
    hub_db1.execute(DDL, &p()).expect("hub ddl");
    // Moved, not cloned: the file lock must be released when this generation
    // stops, so the reopen below can succeed.
    let hub1 = start_hub(&broker, tenant, hub_db1);

    // B is chatty: its clock runs far ahead, then it writes and pushes the
    // shared key BEFORE the restart, and pulls once to establish a
    // watermark it will later resume from.
    let (b_db, b_client) = edge(&broker, tenant);
    write_filler(&b_db, 200, "b-filler");
    write_key(&b_db, 1, "from-B-before-restart");
    within(b_client.push()).await.expect("B push");
    within(b_client.pull_default())
        .await
        .expect("B pull (establish a watermark)");
    let watermark_before_restart = b_client.pull_watermark();

    hub1.stop().await;

    // The accepting node restarts: same file, same data (including the
    // provenance behind the shared key), a fresh handle and server
    // generation over it.
    let hub_db2 = Arc::new(Database::open(&hub_path).expect("reopen hub"));
    let hub2 = start_hub(&broker, tenant, hub_db2.clone());

    // A is quiet and has never contacted the hub, so its clock stays low.
    // It writes the SAME key AFTER the restart.
    let (a_db, a_client) = edge(&broker, tenant);
    write_key(&a_db, 1, "from-A-after-restart");
    within(a_client.push()).await.expect("A push after restart");

    assert_eq!(
        only_body(&hub_db2, 1).as_deref(),
        Some("from-A-after-restart"),
        "A's write, arriving after the restart, must still win over B's \
         earlier value — the accepting node's ordering must survive its own \
         restart, not just live in memory"
    );

    // B resumes from its PRE-restart watermark and must receive exactly the
    // one row now above it.
    within(b_client.pull_default())
        .await
        .expect("B resume pull");
    assert!(
        b_client.pull_watermark() > watermark_before_restart,
        "a resuming puller must advance past its pre-restart cursor"
    );
    assert_eq!(
        only_body(&b_db, 1).as_deref(),
        Some("from-A-after-restart"),
        "a resuming puller must receive the post-restart accepted row, \
         correctly ordered above everything it already held"
    );

    hub2.stop().await;
}

// ---------------------------------------------------------------------------
// The push watermark stops stepping over a refusal it can never win.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_row_is_not_silently_stepped_over() {
    let broker = InProcessBroker::new();

    // Part A: with arbitration comparing a single clock, a LatestWins push is
    // never refused at all — the incoming write either wins outright (a
    // fresh local write) or is a stale echo, never a "skip" carrying a
    // conflict on the receipt.
    let tenant_a = "s2-latest-wins-no-refusal";
    let hub_db = Arc::new(Database::open_memory());
    hub_db.execute(DDL, &p()).expect("hub ddl");
    let hub = start_hub(&broker, tenant_a, hub_db.clone());
    let (b_db, b_client) = edge(&broker, tenant_a);
    let (a_db, a_client) = edge(&broker, tenant_a);
    write_filler(&b_db, 200, "b-filler");
    write_key(&b_db, 1, "from-B");
    within(b_client.push()).await.expect("B push");
    write_key(&a_db, 1, "from-A-LATER");
    let a_push = within(a_client.push()).await.expect("A push");
    assert!(
        a_push.conflicts.is_empty(),
        "a LatestWins push must never be refused once arbitration compares a \
         single clock — got {:?}",
        a_push.conflicts
    );
    assert_eq!(
        a_push.applied_rows, 1,
        "A's write must actually apply, not merely avoid a conflict report: \
         {a_push:?}"
    );
    hub.stop().await;

    // Part B: a policy that CAN legitimately refuse (ServerWins) still
    // advances the push watermark exactly once, with the refusal reported —
    // and the refused row is never re-offered on a later push.
    let tenant_b = "s2-server-wins-refusal-advances-once";
    let hub_db2 = Arc::new(Database::open_memory());
    hub_db2.execute(DDL, &p()).expect("hub ddl 2");
    write_key(&hub_db2, 1, "hub-keeps-this");
    let hub2 = start_hub_with_policy(
        &broker,
        tenant_b,
        hub_db2.clone(),
        ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    );
    let (edge_db, edge_client) = edge(&broker, tenant_b);
    write_key(&edge_db, 1, "edge-tries-to-overwrite");
    let refused = within(edge_client.push())
        .await
        .expect("edge push (refused)");
    assert_eq!(
        refused.skipped_rows, 1,
        "the ServerWins refusal must be reported: {refused:?}"
    );
    assert_eq!(refused.conflicts.len(), 1, "got {:?}", refused.conflicts);
    assert_eq!(refused.conflicts[0].resolution, ConflictPolicy::ServerWins);

    let watermark_after_refusal = edge_client.push_watermark();
    assert!(
        watermark_after_refusal.0 > 0,
        "the push watermark must advance past a refusal no later push \
         could ever win"
    );

    let retried = within(edge_client.push()).await.expect("edge push (retry)");
    assert_eq!(
        retried.applied_rows + retried.skipped_rows,
        0,
        "the refused row must not be re-transmitted on a later push: \
         {retried:?}"
    );
    assert!(retried.conflicts.is_empty(), "got {:?}", retried.conflicts);

    hub2.stop().await;
}

// ---------------------------------------------------------------------------
// Standing guard for what is NOT broken.
// ---------------------------------------------------------------------------

/// Pinned permanently: a single pull cursor is a complete cursor over the
/// hub's own outbound order — a low-write-volume sender's row reaches a
/// high-write-volume puller with no arbitration involved (distinct keys).
/// Green today and must stay green through every arbitration change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_low_write_volume_senders_row_reaches_a_high_write_volume_puller() {
    let broker = InProcessBroker::new();
    let tenant = "quiet-distinct-key";
    let hub_db = Arc::new(Database::open_memory());
    hub_db.execute(DDL, &p()).expect("hub ddl");
    let hub = start_hub(&broker, tenant, hub_db.clone());

    let (a_db, a_client) = edge(&broker, tenant);
    let (b_db, b_client) = edge(&broker, tenant);

    // B is chatty: 200 local writes, then push, then pull (its watermark
    // runs high).
    write_filler(&b_db, 200, "b-chatty");
    within(b_client.push()).await.expect("b push");
    within(b_client.pull_default()).await.expect("b pull");
    let b_wm_before = b_client.pull_watermark();

    // A is quiet: one write at a very low local LSN, pushed after B's
    // traffic.
    write_key(&a_db, 1, "from-quiet-a");
    within(a_client.push()).await.expect("a push");

    within(b_client.pull_default()).await.expect("b pull 2");
    assert_eq!(
        only_body(&b_db, 1).as_deref(),
        Some("from-quiet-a"),
        "the quiet sender's distinct-key row must reach the chatty puller \
         (b_wm_before={b_wm_before:?}, b_wm_after={:?})",
        b_client.pull_watermark()
    );

    hub.stop().await;
}
