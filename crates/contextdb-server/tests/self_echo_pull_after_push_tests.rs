//! An edge pulling right after its own push must never see its own
//! just-delivered rows come back as "conflicts."
//!
//! The contract: pulling data you already hold is a PURE
//! no-op. It counts in NONE of `applied_rows` / `conflicts` / `skipped_rows` —
//! `skipped_rows` stays reserved for genuine policy or context refusals (e.g.
//! `InsertIfNotExists` declining an existing key, or a row hidden by access
//! scope), never for a re-delivery of data the edge already holds unchanged.
//! "Conflict" means genuine divergence between two machines that never saw
//! each other's write — not an edge re-observing the row it just sent, unchanged,
//! a moment ago.
//!
//! Mechanism: a client's default conflict policy is
//! `ConflictPolicy::ServerWins` (`sync_client.rs::build`), and `pull()` remaps it
//! through `remap_pull_policies` to `ConflictPolicy::EdgeWins` for the apply
//! direction. The `(Some(local), ConflictPolicy::EdgeWins)` arm in
//! `apply_changes_single_lsn_group` (`contextdb-engine/src/database.rs`) must
//! never unconditionally push a `Conflict { reason: "edge_wins", .. }` and
//! count the row as applied for ANY natural-key collision with an existing
//! local row — it must check whether the incoming row is byte-identical to
//! what is already there (i.e. the edge's own prior write echoing back
//! through the hub) before treating it as a conflict.
//!
//! An identical-row check ahead of the conflict-policy arms must not merely
//! route the re-delivery into `skipped_rows` instead of counting it nowhere —
//! that is still wrong per the contract above, and this file's first test
//! pins that too, closing the gap a `skipped_rows`-blind assertion would let
//! through.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads;
//! assertions are on the `ApplyResult` / `VersionedRow` values the library
//! calls return, never on rendered text. Pull bookkeeping (the pull watermark)
//! may legitimately advance on a self-echo — only the row's own content/version
//! and the `ApplyResult` counters are asserted.

use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy, NaturalKey};
use contextdb_server::{InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const HUB_NODE: &str = "hub-node-a";
const DDL: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)";

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

/// The single `notes` row's `body`, by value — fails loudly if the table does
/// not hold exactly one row for `id`, so a collapse or duplicate cannot hide
/// behind the value check.
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

/// The complete, versioned local row for `id` — row_id, values, created_tx,
/// deleted_tx, lsn and created_at, all of it. Used to prove a self-echo pull
/// left the row COMPLETELY untouched: a delete+reinsert cycle (what the
/// conflict-policy upsert arms perform) changes `row_id`/`created_tx` even when
/// the visible column values end up identical, so comparing the full
/// `VersionedRow` — not just `body` — is what actually proves "never touched."
fn versioned_note(db: &Database, id: i64) -> contextdb_core::VersionedRow {
    db.point_lookup("notes", "id", &Value::Int64(id), db.snapshot())
        .expect("point lookup must succeed")
        .unwrap_or_else(|| panic!("row id={id} must exist"))
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

/// A hub built the ordinary way (uniform `InsertIfNotExists`, matching how the
/// other in-process sync test files in this crate stand up a hub) — the hub's
/// own policy is irrelevant to this defect, which lives entirely in what the
/// PULLING edge does with its own default conflict policy.
fn start_hub(broker: &InProcessBroker, tenant: &str) -> RunningHub {
    let db = Arc::new(Database::open_memory());
    db.execute(DDL, &p()).expect("hub table");
    let server = Arc::new(SyncServer::with_transport(
        db,
        broker.server_as(HUB_NODE),
        TenantId::from(tenant),
        ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
    ));
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    RunningHub { shutdown, task }
}

fn open_edge() -> Arc<Database> {
    let db = Arc::new(Database::open_memory());
    db.execute(DDL, &p()).expect("edge table");
    db
}

/// The plain constructor with NO explicit conflict-policy override — this is
/// the client's shipped default (`ServerWins`, remapped to `EdgeWins` on pull),
/// exactly what an ordinary `cg`/`contextdb-cli` session gets with no `.sync
/// policy` override.
fn edge_client(
    db: &Arc<Database>,
    broker: &InProcessBroker,
    node_id: &str,
    tenant: &str,
) -> SyncClient {
    SyncClient::with_transport(
        db.clone(),
        broker.client_as(node_id),
        TenantId::from(tenant),
    )
}

/// The self-echo: a single edge pushes its own new rows, then
/// immediately pulls — on the shipped default policy path (no explicit policy
/// override). The pulled rows are the edge's OWN data, already present locally
/// by value; this must be a PURE no-op — counted in NONE of applied, conflicts,
/// or skipped. `skipped_rows` is reserved for a genuine policy/context refusal,
/// which this is not: nothing was refused, there was simply nothing to do.
#[tokio::test]
async fn pull_right_after_push_does_not_self_echo_as_a_conflict() {
    let tenant = "self-echo-basic";
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, tenant);

    let edge = open_edge();
    let client = edge_client(&edge, &broker, "edge-a", tenant);
    insert_note(&edge, 1, "mine-1");
    insert_note(&edge, 2, "mine-2");

    let pushed = within(client.push()).await.expect("push must succeed");
    assert_eq!(
        pushed.applied_rows, 2,
        "fixture: the hub really took both new rows: {pushed:?}"
    );

    let pulled = within(client.pull_default())
        .await
        .expect("pull must succeed");

    assert_eq!(
        pulled.applied_rows, 0,
        "pulling data this edge already holds (its own just-pushed rows) must be \
         a pure no-op — nothing new to apply. Got {pulled:?}"
    );
    assert!(
        pulled.conflicts.is_empty(),
        "pulling back the edge's OWN just-pushed rows must never be reported as \
         a conflict — 'conflict' means genuine divergence between two machines \
         that never saw each other's write, not an edge re-observing its own \
         data through the hub. Got {:?}",
        pulled.conflicts
    );
    assert_eq!(
        pulled.skipped_rows, 0,
        "a self-echoed row must not be counted as SKIPPED either — skipped is \
         reserved for a genuine policy or context refusal (an InsertIfNotExists \
         decline, a row hidden by access scope), never for a re-delivery of data \
         the edge already holds unchanged, which is not a refusal of anything. \
         Got {pulled:?}"
    );

    hub.stop().await;
}

/// A single pull that mixes a genuine conflict with a self-echo, to
/// kill the "suppress the pull immediately following a push" cheat — a fix
/// that merely special-cases "this edge's own just-pushed batch" rather than
/// checking each row's content would zero everything in this pull, hiding the
/// genuine conflict too. Edge B pushes a value at key=1 first. Edge A
/// independently holds a DIFFERENT value at that same key=1 (a real,
/// never-synced divergence) and ALSO has an unrelated key=2 row it already
/// pushed to the hub itself (a genuine self-echo). One pull on A must resolve
/// them independently: exactly one conflict, naming key=1, and the key=2
/// self-echo contributing to NONE of applied/skipped/conflicts.
#[tokio::test]
async fn mixed_pull_isolates_the_genuine_conflict_from_the_self_echo() {
    let tenant = "self-echo-mixed";
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, tenant);

    // Edge B writes and pushes a value at key=1 first — the hub now holds B's
    // value, and edge A has never seen it.
    let edge_b = open_edge();
    let client_b = edge_client(&edge_b, &broker, "edge-b", tenant);
    insert_note(&edge_b, 1, "edge-b-version");
    within(client_b.push()).await.expect("edge B push");

    // Edge A pushes an UNRELATED row (key=2) that is entirely its own — the
    // genuine self-echo half of this scenario.
    let edge_a = open_edge();
    let client_a = edge_client(&edge_a, &broker, "edge-a", tenant);
    insert_note(&edge_a, 2, "edge-a-only");
    let pushed_a = within(client_a.push()).await.expect("edge A push");
    assert_eq!(
        pushed_a.applied_rows, 1,
        "fixture: the hub took edge A's own key=2 row: {pushed_a:?}"
    );

    // ONLY AFTER that push, edge A independently writes its OWN different
    // value at key=1 — never pushed, never pulled, a genuine divergence from
    // what edge B already put on the hub.
    insert_note(&edge_a, 1, "edge-a-version");

    // One pull now mixes both cases: key=1 (genuine two-edge divergence) and
    // key=2 (A's own row echoing back).
    let pulled = within(client_a.pull_default())
        .await
        .expect("edge A mixed pull must succeed");

    assert_eq!(
        pulled.conflicts.len(),
        1,
        "exactly one genuine conflict must surface — the self-echoed key=2 row \
         must not inflate this list. Got {:?}",
        pulled.conflicts
    );
    let conflict = &pulled.conflicts[0];
    assert_eq!(
        conflict.natural_key,
        NaturalKey::single("id".to_string(), Value::Int64(1)),
        "the one reported conflict must name the genuinely divergent key (id=1), \
         not the self-echoed one (id=2). Got {conflict:?}"
    );
    assert_eq!(
        conflict.resolution,
        ConflictPolicy::EdgeWins,
        "the conflict's resolution must be the pull's actual apply-direction \
         policy. Got {conflict:?}"
    );
    assert_eq!(
        conflict.reason.as_deref(),
        Some("edge_wins"),
        "the conflict's reason must name the policy that resolved it. Got {conflict:?}"
    );

    assert_eq!(
        pulled.applied_rows, 1,
        "only the genuine key=1 conflict resolution is real work; the key=2 \
         self-echo must not inflate this count. Got {pulled:?}"
    );
    assert_eq!(
        pulled.skipped_rows, 0,
        "the key=2 self-echo must not be counted as skipped either — nothing \
         was refused, there was nothing to do. Got {pulled:?}"
    );

    hub.stop().await;
}

/// Guards that the self-echo fix above does not just zero out conflict
/// counters wholesale. Two edges that independently write DIFFERENT content at
/// the SAME natural key — with no shared history, no push/pull between them —
/// is a genuine divergence, and must still surface as a pull conflict when the
/// second edge pulls the first edge's already-committed value, carrying the
/// right natural key and resolution/reason, and must still actually apply the
/// incoming value locally (never a silent no-op over real diverging content).
#[tokio::test]
async fn genuine_two_edge_divergence_still_reports_a_pull_conflict() {
    let tenant = "self-echo-divergence-guard";
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, tenant);

    // Edge A writes and pushes id=1 first — the hub now holds A's value.
    let edge_a = open_edge();
    let client_a = edge_client(&edge_a, &broker, "edge-a", tenant);
    insert_note(&edge_a, 1, "edge-a-version");
    within(client_a.push()).await.expect("edge A push");

    // Edge B, independently and with NO shared history, writes its OWN
    // different value at the same natural key — never pushed, never pulled.
    let edge_b = open_edge();
    let client_b = edge_client(&edge_b, &broker, "edge-b", tenant);
    insert_note(&edge_b, 1, "edge-b-version");

    // Edge B pulls: the incoming row (A's committed value) collides with B's
    // own different local value at the same key — a genuine two-machine
    // conflict, not a self-echo.
    let pulled = within(client_b.pull_default())
        .await
        .expect("edge B pull must succeed");

    assert_eq!(
        pulled.conflicts.len(),
        1,
        "a genuine divergence between two edges at the same natural key must \
         still be reported as a conflict on pull, got {:?}",
        pulled.conflicts
    );
    let conflict = &pulled.conflicts[0];
    assert_eq!(
        conflict.natural_key,
        NaturalKey::single("id".to_string(), Value::Int64(1)),
        "the reported conflict must name the genuinely divergent key. Got {conflict:?}"
    );
    assert_eq!(
        conflict.resolution,
        ConflictPolicy::EdgeWins,
        "the reported resolution must be the pull's actual apply-direction \
         policy. Got {conflict:?}"
    );
    assert_eq!(
        conflict.reason.as_deref(),
        Some("edge_wins"),
        "the reported reason must name the policy that resolved it. Got {conflict:?}"
    );

    // The genuine conflict must actually resolve, overwriting B's local value
    // with the hub's (A's) committed one — never a silent no-op that leaves
    // two machines disagreeing while reporting a conflict for show.
    assert_eq!(
        only_body(&edge_b, 1),
        "edge-a-version",
        "the genuine conflict must actually apply the hub's committed value \
         locally, not merely report itself"
    );

    hub.stop().await;
}

/// Row-level no-op observation. A self-echoed row is not merely
/// uncounted in the `ApplyResult` — its actual STORAGE identity must be
/// untouched. Capture the row's complete `VersionedRow` (row_id, created_tx,
/// deleted_tx, lsn, values, created_at) before the self-echo pull and again
/// after; they must be byte-for-byte identical. A delete+reinsert cycle (what
/// every conflict-policy upsert arm performs) changes `row_id`/`created_tx`
/// even when the visible column values end up the same, so this is strictly
/// stronger than comparing `body` alone. Pull BOOKKEEPING (the client's pull
/// watermark) legitimately advances on a self-echo and is deliberately not
/// asserted on here.
#[tokio::test]
async fn self_echo_leaves_the_row_completely_untouched() {
    let tenant = "self-echo-row-identity";
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, tenant);

    let edge = open_edge();
    let client = edge_client(&edge, &broker, "edge-a", tenant);
    insert_note(&edge, 1, "mine");
    within(client.push()).await.expect("push must succeed");

    let before = versioned_note(&edge, 1);

    within(client.pull_default())
        .await
        .expect("self-echo pull must succeed");

    let after = versioned_note(&edge, 1);

    assert_eq!(
        before, after,
        "a self-echoed row must be left COMPLETELY untouched by the pull that \
         re-delivers it — same row_id, same created_tx, same deleted_tx, same \
         lsn, same values, same created_at. Before: {before:?}\nAfter: {after:?}"
    );

    hub.stop().await;
}

/// Standing guard: a row that arrived at this edge BY SYNC must never be
/// offered back outbound on a later push — `drop_rows_that_arrived_by_sync`
/// (`sync_client.rs`) already suppresses this on the shipped client. Pinned
/// so every future arbitration or pull-cursor change keeps it true: both
/// touch the exact comparison this guard depends on staying unreachable
/// through the normal client path.
#[tokio::test]
async fn a_sync_arrived_row_is_never_offered_back_on_a_later_push() {
    let tenant = "no-outbound-echo";
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, tenant);

    let edge_a = open_edge();
    let client_a = edge_client(&edge_a, &broker, "edge-a", tenant);
    insert_note(&edge_a, 1, "from-a");
    within(client_a.push()).await.expect("A push");

    let edge_b = open_edge();
    let client_b = edge_client(&edge_b, &broker, "edge-b", tenant);
    // Push once first, purely so B's OWN local DDL (its `CREATE TABLE`) is
    // no longer pending — otherwise `has_pending_push_changes` would read
    // true for that unrelated reason and this guard would prove nothing
    // about row echo specifically.
    within(client_b.push())
        .await
        .expect("B push (own schema only)");
    within(client_b.pull_default())
        .await
        .expect("B pull (sync-arrived row)");

    assert!(
        !client_b
            .has_pending_push_changes()
            .expect("pending-changes probe"),
        "a row that arrived at this edge BY SYNC must never be offered back \
         on a later push"
    );

    let pushed = within(client_b.push())
        .await
        .expect("B push (should be empty)");
    assert_eq!(
        pushed.applied_rows + pushed.skipped_rows,
        0,
        "a sync-arrived row must not be transmitted outbound at all: {pushed:?}"
    );

    hub.stop().await;
}
