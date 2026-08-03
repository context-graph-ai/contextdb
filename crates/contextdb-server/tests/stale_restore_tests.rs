//! Stale-restore convergence suite.
//!
//! Contract: a sync server restored from an older artifact must reconverge with
//! edges whose acked commits postdate the artifact's snapshot point, without
//! user intervention. Regression detection rides a dedicated per-tenant
//! sync-status subject; the existing push/pull subjects and wire structs stay
//! byte-identical (the frozen wire-bytes guard pins the exact bytes).
//!
//! Before this change, sr1–sr3 fail: a sync with nothing locally new is
//! short-circuited to a no-op and never contacts the server, so a freshly
//! restored (stale) server's regression goes undetected. sr4–sr6 are
//! regression guards.

use contextdb_core::{Lsn, Value};
use contextdb_engine::Database;
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

/// A SyncServer generation that can be stopped deterministically, so a second
/// generation can take over the same tenant routes without double-responders.
struct ServerGen {
    shutdown: Arc<AtomicBool>,
    handle: tokio::task::JoinHandle<()>,
}

async fn start_server(
    db: Arc<Database>,
    fabric: &InProcessBroker,
    tenant: &str,
    identity: Arc<FabricIdentity>,
) -> ServerGen {
    let node_id = identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            db,
            fabric.server_as(&node_id),
            contextdb_core::TenantId::from(tenant),
            node_id,
            identity,
        ),
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let flag = shutdown.clone();
    let handle = tokio::spawn(async move { server.run_until(flag).await });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fabric.wait_for_registered_route_for_test(&contextdb_server::subjects::status_subject(
            tenant,
        )),
    )
    .await
    .expect("stale-restore server must register its status route");
    ServerGen { shutdown, handle }
}

fn edge_client(db: Arc<Database>, fabric: &InProcessBroker, tenant: &str) -> SyncClient {
    let identity = Arc::new(FabricIdentity::generate());
    let node_id = identity.node_id();
    SyncClient::with_authenticated_transport_and_identity_for_test(
        db,
        fabric.client_as(&node_id),
        contextdb_core::TenantId::from(tenant),
        identity,
    )
}

async fn stop_server(server_gen: ServerGen) {
    server_gen.shutdown.store(true, Ordering::SeqCst);
    tokio::time::timeout(std::time::Duration::from_secs(10), server_gen.handle)
        .await
        .expect("sync server must shut down within 10s")
        .expect("sync server task must not panic");
}

/// Bounded wrapper so a red state can never hang the suite.
async fn within<T>(fut: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(std::time::Duration::from_secs(60), fut)
        .await
        .expect("sync operation must complete within 60s")
}

/// Tight 10s bound for the bounded regression-guard sync cycles.
async fn bounded10<T>(fut: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(std::time::Duration::from_secs(10), fut)
        .await
        .expect("regression-guard sync operation must complete within 10s")
}

fn create_t(db: &Database) {
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT) SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .unwrap();
}

fn insert_row(db: &Database, id: Uuid, v: &str) {
    let mut p = HashMap::new();
    p.insert("id".to_string(), Value::Uuid(id));
    p.insert("v".to_string(), Value::Text(v.to_string()));
    db.execute("INSERT INTO t (id, v) VALUES ($id, $v)", &p)
        .unwrap();
}

/// Exact row set via SQL. Panics on duplicate (id, v) pairs — idempotent
/// re-application is therefore enforced by every row-set assertion in the
/// suite, never by counts.
fn row_set(db: &Database) -> BTreeSet<(Uuid, String)> {
    let result = db
        .execute("SELECT id, v FROM t", &HashMap::new())
        .expect("SELECT id, v FROM t must succeed");
    let id_idx = result
        .columns
        .iter()
        .position(|c| c == "id")
        .expect("result must project column 'id'");
    let v_idx = result
        .columns
        .iter()
        .position(|c| c == "v")
        .expect("result must project column 'v'");
    let mut set = BTreeSet::new();
    for row in &result.rows {
        let id = match &row[id_idx] {
            Value::Uuid(u) => *u,
            other => panic!("id column must be UUID, got {other:?}"),
        };
        let v = match &row[v_idx] {
            Value::Text(t) => t.clone(),
            other => panic!("v column must be TEXT, got {other:?}"),
        };
        assert!(
            set.insert((id, v.clone())),
            "duplicate row (id={id}, v={v}) — idempotent re-application violated"
        );
    }
    set
}

fn expect_rows(pairs: &[(Uuid, &str)]) -> BTreeSet<(Uuid, String)> {
    pairs.iter().map(|(id, v)| (*id, v.to_string())).collect()
}

// ======== sr1 — THE journey ========
//
// Before this change: SyncClient::push computes an empty changeset (edge has
// nothing locally new past its push watermark) and early-returns Ok(applied=0)
// without contacting the restored server. The restored server holds A forever;
// this test fails at the restored-server row-set assertion.
#[tokio::test]
async fn sr1_restored_server_reconverges_after_edge_repush() {
    let fabric = InProcessBroker::new();
    let tmp = tempfile::TempDir::new().unwrap();
    let tenant = "sr1-stale-restore-journey";
    let server_db = Arc::new(Database::open(tmp.path().join("server-gen1.db")).unwrap());
    let edge_db = Arc::new(Database::open(tmp.path().join("edge.db")).unwrap());
    create_t(&server_db);
    create_t(&edge_db);

    let hub_identity = Arc::new(FabricIdentity::generate());
    let gen1 = start_server(server_db.clone(), &fabric, tenant, hub_identity.clone()).await;
    let client = edge_client(edge_db.clone(), &fabric, tenant);

    // Push A (acked).
    let a1 = Uuid::new_v4();
    let a2 = Uuid::new_v4();
    insert_row(&edge_db, a1, "a1");
    insert_row(&edge_db, a2, "a2");
    let push_a = within(client.push()).await.unwrap();
    assert_eq!(push_a.applied_rows, 2, "push A must apply both rows");
    let watermark_after_a = client.push_watermark();
    assert!(
        watermark_after_a > Lsn(0),
        "push watermark must advance past A"
    );

    // Export the artifact at the snapshot point = after A.
    let artifact = tmp.path().join("server-checkpoint.cdb");
    server_db.export_snapshot(&artifact).unwrap();
    {
        let artifact_db = Database::open(&artifact).expect("open exported artifact");
        assert_eq!(
            row_set(&artifact_db),
            expect_rows(&[(a1, "a1"), (a2, "a2")]),
            "artifact must capture exactly the A application rows"
        );
    }

    // Push B (acked). Server gen1 now holds A ∪ B.
    let b1 = Uuid::new_v4();
    let b2 = Uuid::new_v4();
    insert_row(&edge_db, b1, "b1");
    insert_row(&edge_db, b2, "b2");
    let push_b = within(client.push()).await.unwrap();
    assert_eq!(push_b.applied_rows, 2, "push B must apply both rows");
    assert!(
        client.push_watermark() > watermark_after_a,
        "push watermark must advance past B"
    );
    let union = expect_rows(&[(a1, "a1"), (a2, "a2"), (b1, "b1"), (b2, "b2")]);
    assert_eq!(
        row_set(&server_db),
        union,
        "precondition: gen1 server acked and applied A ∪ B"
    );

    // Kill gen1; a new server opens the stale artifact.
    stop_server(gen1).await;
    let restored_db = Arc::new(Database::open(&artifact).unwrap());
    assert_eq!(
        row_set(&restored_db),
        expect_rows(&[(a1, "a1"), (a2, "a2")]),
        "precondition: artifact is stale — rows = A only"
    );
    let gen2 = start_server(restored_db.clone(), &fabric, tenant, hub_identity).await;

    // THE moment under test: edge re-pushes on its next sync, no user action.
    within(client.push()).await.unwrap();

    // Convergence: exact row sets on BOTH sides.
    assert_eq!(
        row_set(&restored_db),
        union,
        "restored server must reconverge to the exact union of all \
         acked commits after the edge's next push; before this change it early-returns \
         on an empty changeset and the server keeps only A"
    );
    assert_eq!(
        row_set(&edge_db),
        union,
        "edge must still hold the exact union (re-push must not mutate edge data)"
    );

    // Server-side observation THROUGH THE WIRE: a fresh second edge pulls from
    // scratch and must see the full union. This cannot be faked by mutating the
    // test's local Arc<Database> handle.
    let observer_db = Arc::new(Database::open_memory());
    create_t(&observer_db);
    let observer = edge_client(observer_db.clone(), &fabric, tenant);
    within(observer.pull_default()).await.unwrap();
    assert_eq!(
        row_set(&observer_db),
        union,
        "a fresh edge pulling from the restored server must receive the exact union"
    );

    // Contract item 6 made explicit: pushing again re-applies nothing twice —
    // every re-pushed row exists exactly once (row_set panics on duplicates).
    within(client.push()).await.unwrap();
    assert_eq!(
        row_set(&restored_db),
        union,
        "idempotent re-application: repeated post-restore pushes must leave each \
         row exactly once"
    );

    // Post-recovery commit, edge → server.
    let c1 = Uuid::new_v4();
    insert_row(&edge_db, c1, "c1");
    let push_c = within(client.push()).await.unwrap();
    assert_eq!(
        push_c.applied_rows, 1,
        "post-recovery edge commit must apply"
    );
    let mut converged = union.clone();
    converged.insert((c1, "c1".to_string()));
    assert_eq!(
        row_set(&restored_db),
        converged,
        "post-recovery edge commit must reach the restored server"
    );

    // Post-recovery commit, server → edge.
    let s1 = Uuid::new_v4();
    insert_row(&restored_db, s1, "s1");
    within(client.pull_default()).await.unwrap();
    converged.insert((s1, "s1".to_string()));
    assert_eq!(
        row_set(&edge_db),
        converged,
        "post-recovery server commit must reach the edge"
    );
    assert_eq!(
        row_set(&restored_db),
        converged,
        "both sides must hold the identical exact row set after recovery"
    );

    stop_server(gen2).await;
}

// ======== sr2 — the probe ========
//
// Before this change: a push with nothing locally new never performs a server
// exchange (no status request exists today), so a freshly-restored server's
// regression is undetectable. Both nothing-new pushes below early-return today;
// the union assertion fails.
#[tokio::test]
async fn sr2_push_with_nothing_new_still_converges_restored_server() {
    let fabric = InProcessBroker::new();
    let tmp = tempfile::TempDir::new().unwrap();
    let tenant = "sr2-empty-push-probe";
    let server_db = Arc::new(Database::open(tmp.path().join("server-gen1.db")).unwrap());
    let edge_db = Arc::new(Database::open_memory());
    create_t(&server_db);
    create_t(&edge_db);

    let hub_identity = Arc::new(FabricIdentity::generate());
    let gen1 = start_server(server_db.clone(), &fabric, tenant, hub_identity.clone()).await;
    let client = edge_client(edge_db.clone(), &fabric, tenant);

    let a1 = Uuid::new_v4();
    insert_row(&edge_db, a1, "a1");
    let push_a = within(client.push()).await.unwrap();
    assert_eq!(push_a.applied_rows, 1);

    let artifact = tmp.path().join("server-checkpoint.cdb");
    server_db.export_snapshot(&artifact).unwrap();

    let b1 = Uuid::new_v4();
    insert_row(&edge_db, b1, "b1");
    let push_b = within(client.push()).await.unwrap();
    assert_eq!(push_b.applied_rows, 1);

    stop_server(gen1).await;
    let restored_db = Arc::new(Database::open(&artifact).unwrap());
    assert_eq!(
        row_set(&restored_db),
        expect_rows(&[(a1, "a1")]),
        "precondition: artifact predates B"
    );
    let gen2 = start_server(restored_db.clone(), &fabric, tenant, hub_identity).await;

    // The edge has NOTHING locally new: the next pushes are pure probes.
    assert!(
        !client.has_pending_push_changes().unwrap(),
        "precondition: edge changeset is empty — the next push is the 'nothing new' probe"
    );

    within(client.push()).await.unwrap();
    within(client.push()).await.unwrap();

    let union = expect_rows(&[(a1, "a1"), (b1, "b1")]);
    assert_eq!(
        row_set(&restored_db),
        union,
        "a push with nothing locally new must still exchange status \
         with the server, detect the regression, and re-push acked commits — \
         even the second nothing-new push must leave a freshly-restored server \
         converged"
    );
    assert_eq!(
        row_set(&edge_db),
        union,
        "probe pushes must not mutate the edge"
    );

    stop_server(gen2).await;
}

// ======== sr3 — pull-side regression safety ========
//
// Before this change: the edge's pull watermark (server-LSN space) is ahead of
// the restored server's LSN clock; pull sends since_lsn above the server's
// position, the server returns an empty page, and a genuinely new server
// commit (s4, stamped below the stale watermark) is skipped forever.
#[tokio::test]
async fn sr3_pull_resumes_from_restored_server_position_and_delivers_new_commits() {
    let fabric = InProcessBroker::new();
    let tmp = tempfile::TempDir::new().unwrap();
    let tenant = "sr3-pull-regression";
    let server_db = Arc::new(Database::open(tmp.path().join("server-gen1.db")).unwrap());
    let edge_db = Arc::new(Database::open_memory());
    create_t(&server_db);
    create_t(&edge_db);

    let hub_identity = Arc::new(FabricIdentity::generate());
    let gen1 = start_server(server_db.clone(), &fabric, tenant, hub_identity.clone()).await;
    let client = edge_client(edge_db.clone(), &fabric, tenant);

    // Server commits s1, s2; edge pulls them.
    let s1 = Uuid::new_v4();
    let s2 = Uuid::new_v4();
    insert_row(&server_db, s1, "s1");
    insert_row(&server_db, s2, "s2");
    let pull_1 = within(client.pull_default()).await.unwrap();
    assert_eq!(pull_1.applied_rows, 2);
    assert_eq!(row_set(&edge_db), expect_rows(&[(s1, "s1"), (s2, "s2")]));

    // Snapshot point: artifact holds {s1, s2}.
    let artifact = tmp.path().join("server-checkpoint.cdb");
    server_db.export_snapshot(&artifact).unwrap();

    // Two more server commits give the edge a watermark with margin above the
    // artifact's LSN, so one post-restore commit stays below it (fixture math
    // asserted below, not assumed).
    let s3a = Uuid::new_v4();
    let s3b = Uuid::new_v4();
    insert_row(&server_db, s3a, "s3a");
    insert_row(&server_db, s3b, "s3b");
    let pull_2 = within(client.pull_default()).await.unwrap();
    assert_eq!(pull_2.applied_rows, 2);
    let stale_watermark = client.pull_watermark();

    stop_server(gen1).await;
    let restored_db = Arc::new(Database::open(&artifact).unwrap());
    assert_eq!(
        row_set(&restored_db),
        expect_rows(&[(s1, "s1"), (s2, "s2")]),
        "precondition: artifact predates s3a/s3b"
    );
    assert!(
        restored_db.current_lsn() < stale_watermark,
        "precondition: edge pull watermark ({stale_watermark:?}) must be ahead of \
         the restored server's LSN clock ({:?})",
        restored_db.current_lsn()
    );

    // Genuinely new commit on the restored server, stamped BELOW the edge's
    // stale watermark — the exact row the bug silently skips.
    let s4 = Uuid::new_v4();
    insert_row(&restored_db, s4, "s4");
    assert!(
        restored_db.current_lsn() < stale_watermark,
        "fixture: the new commit must remain below the stale watermark to expose the skip"
    );

    let gen2 = start_server(restored_db.clone(), &fabric, tenant, hub_identity).await;

    within(client.pull_default()).await.unwrap();
    let edge_expected = expect_rows(&[
        (s1, "s1"),
        (s2, "s2"),
        (s3a, "s3a"),
        (s3b, "s3b"),
        (s4, "s4"),
    ]);
    assert_eq!(
        row_set(&edge_db),
        edge_expected,
        "pull must resume from the restored server's actual position; \
         the genuinely new commit s4 must never be skipped, and re-delivered \
         rows (s1, s2) must apply idempotently"
    );

    // Re-pull: idempotent re-delivery, exact set stable, no duplicates
    // (row_set panics on any duplicate).
    within(client.pull_default()).await.unwrap();
    assert_eq!(
        row_set(&edge_db),
        edge_expected,
        "second pull must be a stable no-op"
    );

    // Scope pin: pull never mutates the server. s3a/s3b were server-authored
    // commits the edge merely pulled; pull-side recovery does not resurrect
    // them on the restored server.
    assert_eq!(
        row_set(&restored_db),
        expect_rows(&[(s1, "s1"), (s2, "s2"), (s4, "s4")]),
        "pull must not write to the server"
    );

    stop_server(gen2).await;
}

// ======== sr4 — REGRESSION GUARD: steady-state push ========
//
// Passes today. Pins exact applied counts, watermark advancement, and
// zero duplicates so the fix cannot buy convergence with blanket re-pushes.
#[tokio::test]
async fn sr4_guard_steady_state_push_counts_and_no_duplicates() {
    let fabric = InProcessBroker::new();
    let tenant = "sr4-steady-push";
    let server_db = Arc::new(Database::open_memory());
    let edge_db = Arc::new(Database::open_memory());
    create_t(&server_db);
    create_t(&edge_db);
    let server_gen = start_server(
        server_db.clone(),
        &fabric,
        tenant,
        Arc::new(FabricIdentity::generate()),
    )
    .await;
    let client = edge_client(edge_db.clone(), &fabric, tenant);

    let a1 = Uuid::new_v4();
    insert_row(&edge_db, a1, "a1");
    let r1 = within(client.push()).await.unwrap();
    assert_eq!(r1.applied_rows, 1, "first push must apply exactly 1 row");
    let wm1 = client.push_watermark();
    assert!(wm1 > Lsn(0), "push watermark must advance");

    // No-op push: same observable behavior as today — zero applied, zero
    // skipped, watermark untouched, server row set unchanged.
    let r2 = within(client.push()).await.unwrap();
    assert_eq!(
        r2.applied_rows, 0,
        "no-op push must apply 0 rows in steady state"
    );
    assert_eq!(
        r2.skipped_rows, 0,
        "no-op push must skip 0 rows in steady state"
    );
    assert_eq!(
        client.push_watermark(),
        wm1,
        "no-op push must not churn the push watermark in steady state"
    );
    assert_eq!(row_set(&server_db), expect_rows(&[(a1, "a1")]));

    // New data still flows with exact counts.
    let a2 = Uuid::new_v4();
    insert_row(&edge_db, a2, "a2");
    let r3 = within(client.push()).await.unwrap();
    assert_eq!(
        r3.applied_rows, 1,
        "incremental push must apply exactly the new row"
    );
    assert!(
        client.push_watermark() > wm1,
        "watermark must advance with new data"
    );

    // Repeated pushes never duplicate (row_set panics on duplicates).
    within(client.push()).await.unwrap();
    assert_eq!(
        row_set(&server_db),
        expect_rows(&[(a1, "a1"), (a2, "a2")]),
        "repeated pushes must leave each row exactly once"
    );

    stop_server(server_gen).await;
}

// ======== sr5 — REGRESSION GUARD: steady-state pull ========
//
// Passes today. Pins pull counts, no-op watermark stability, and exact sets.
#[tokio::test]
async fn sr5_guard_steady_state_pull_counts_and_watermark() {
    let fabric = InProcessBroker::new();
    let tenant = "sr5-steady-pull";
    let server_db = Arc::new(Database::open_memory());
    let edge_db = Arc::new(Database::open_memory());
    create_t(&server_db);
    create_t(&edge_db);
    let server_gen = start_server(
        server_db.clone(),
        &fabric,
        tenant,
        Arc::new(FabricIdentity::generate()),
    )
    .await;
    let client = edge_client(edge_db.clone(), &fabric, tenant);

    let s1 = Uuid::new_v4();
    let s2 = Uuid::new_v4();
    insert_row(&server_db, s1, "s1");
    insert_row(&server_db, s2, "s2");

    let r1 = within(client.pull_default()).await.unwrap();
    assert_eq!(r1.applied_rows, 2, "first pull must apply exactly 2 rows");
    assert_eq!(r1.skipped_rows, 0, "first pull must skip 0 rows");
    let wm1 = client.pull_watermark();
    assert!(wm1 > Lsn(0), "pull watermark must advance");
    assert_eq!(row_set(&edge_db), expect_rows(&[(s1, "s1"), (s2, "s2")]));

    // No-op pull: zero applied/skipped, watermark stable, set unchanged.
    let r2 = within(client.pull_default()).await.unwrap();
    assert_eq!(
        r2.applied_rows, 0,
        "no-op pull must apply 0 rows in steady state"
    );
    assert_eq!(
        r2.skipped_rows, 0,
        "no-op pull must skip 0 rows in steady state"
    );
    assert_eq!(
        client.pull_watermark(),
        wm1,
        "no-op pull must not churn the pull watermark in steady state"
    );
    assert_eq!(row_set(&edge_db), expect_rows(&[(s1, "s1"), (s2, "s2")]));

    // New server data still flows with exact counts and no duplicates.
    let s3 = Uuid::new_v4();
    insert_row(&server_db, s3, "s3");
    let r3 = within(client.pull_default()).await.unwrap();
    assert_eq!(
        r3.applied_rows, 1,
        "incremental pull must apply exactly the new row"
    );
    assert_eq!(r3.skipped_rows, 0);
    assert!(
        client.pull_watermark() > wm1,
        "watermark must advance with new data"
    );
    assert_eq!(
        row_set(&edge_db),
        expect_rows(&[(s1, "s1"), (s2, "s2"), (s3, "s3")]),
        "repeated pulls must leave each row exactly once"
    );

    stop_server(server_gen).await;
}

// ======== sr6 — REGRESSION GUARD: bounded steady-state sync cycle ========
//
// Every operation carries the tight 10s bound. A steady-state cycle must never
// hang, churn watermarks, or re-push already-acknowledged rows.
#[tokio::test]
async fn sr6_guard_sync_cycle_completes_bounded_and_inert() {
    let fabric = InProcessBroker::new();
    let tenant = "sr6-new-client-old-server";
    let server_db = Arc::new(Database::open_memory());
    let edge_db = Arc::new(Database::open_memory());
    create_t(&server_db);
    create_t(&edge_db);
    let server_gen = start_server(
        server_db.clone(),
        &fabric,
        tenant,
        Arc::new(FabricIdentity::generate()),
    )
    .await;
    let client = edge_client(edge_db.clone(), &fabric, tenant);

    // Push with data: bounded, normal counts, watermark advances.
    let a1 = Uuid::new_v4();
    insert_row(&edge_db, a1, "a1");
    let push = bounded10(client.push()).await.unwrap();
    assert_eq!(
        push.applied_rows, 1,
        "push must apply normally in the bounded sync cycle"
    );
    let push_wm = client.push_watermark();
    assert!(push_wm > Lsn(0), "push watermark must advance normally");

    // Pull with data: bounded, normal counts, watermark advances.
    let s1 = Uuid::new_v4();
    insert_row(&server_db, s1, "s1");
    let pull = bounded10(client.pull_default()).await.unwrap();
    assert_eq!(
        pull.applied_rows, 1,
        "pull must apply normally in the bounded sync cycle"
    );
    assert!(
        client.pull_watermark() > Lsn(0),
        "pull watermark must advance normally"
    );

    // One settling push first: on current dev a pull-echo can advance the push
    // watermark once (tracked separately as a known engine gap); the guard here
    // pins PROBE inertness, not the pre-existing echo behavior.
    let _ = bounded10(client.push()).await.unwrap();
    let settled_wm = client.push_watermark();

    // Repeated nothing-new pushes stay inert: zero applied, no watermark churn.
    for _ in 0..2 {
        let probe = bounded10(client.push()).await.unwrap();
        assert_eq!(
            probe.applied_rows, 0,
            "nothing-new push must stay inert when the status subject is unanswered"
        );
        assert_eq!(
            client.push_watermark(),
            settled_wm,
            "unanswered status probe must mean no watermark churn"
        );
    }

    // Exact row sets on both sides.
    let expected = expect_rows(&[(a1, "a1"), (s1, "s1")]);
    assert_eq!(
        row_set(&server_db),
        expected,
        "server must hold the exact set"
    );
    assert_eq!(row_set(&edge_db), expected, "edge must hold the exact set");

    stop_server(server_gen).await;
}

// sr7 (the frozen wire-bytes regression guard) lives in
// `wire_format_freeze_tests.rs`; it is pure encode/decode against fixed
// fixtures.
