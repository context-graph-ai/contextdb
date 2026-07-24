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
//! restored (stale) server's regression goes undetected. sr4–sr8 are
//! regression guards.

use contextdb_core::{Lsn, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use contextdb_server::protocol::{
    MessageType, PullRequest, PullResponse, PushRequest, PushResponse, WireApplyResult,
    WireChangeSet, decode, encode,
};
use contextdb_server::{SyncClient, SyncServer};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use uuid::Uuid;

struct NatsFixture {
    _container: ContainerAsync<GenericImage>,
    nats_url: String,
}

async fn start_nats() -> NatsFixture {
    let nats_conf = format!("{}/tests/nats.conf", env!("CARGO_MANIFEST_DIR"));

    let image = GenericImage::new("nats", "latest")
        .with_exposed_port(4222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"));

    let request = image
        .with_mount(Mount::bind_mount(nats_conf, "/etc/nats/nats.conf"))
        .with_cmd(["--js", "--config", "/etc/nats/nats.conf"]);

    let container: ContainerAsync<GenericImage> = request.start().await.unwrap();
    let nats_port = container.get_host_port_ipv4(4222.tcp()).await.unwrap();

    NatsFixture {
        _container: container,
        nats_url: format!("nats://127.0.0.1:{nats_port}"),
    }
}

/// A SyncServer generation that can be stopped deterministically, so a second
/// generation can take over the same tenant subjects without double-responders.
struct ServerGen {
    shutdown: Arc<AtomicBool>,
    handle: tokio::task::JoinHandle<()>,
}

async fn start_server(
    db: Arc<Database>,
    nats_url: &str,
    tenant: &str,
    policies: ConflictPolicies,
) -> ServerGen {
    let server = Arc::new(SyncServer::new(
        db,
        nats_url,
        contextdb_core::TenantId::from(tenant),
        policies,
    ));
    let shutdown = Arc::new(AtomicBool::new(false));
    let flag = shutdown.clone();
    let handle = tokio::spawn(async move { server.run_until(flag).await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    ServerGen { shutdown, handle }
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

/// Tight 10s bound: pins the no-hang property of the status probe against
/// servers that do not answer the status subject.
async fn bounded10<T>(fut: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(std::time::Duration::from_secs(10), fut)
        .await
        .expect("sync op against a status-less server must complete within 10s — the probe must never hang a sync")
}

fn create_t(db: &Database) {
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)",
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
    let nats = start_nats().await;
    let tmp = tempfile::TempDir::new().unwrap();
    let tenant = "sr1-stale-restore-journey";
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);

    let server_db = Arc::new(Database::open(tmp.path().join("server-gen1.db")).unwrap());
    let edge_db = Arc::new(Database::open(tmp.path().join("edge.db")).unwrap());
    create_t(&server_db);
    create_t(&edge_db);

    let gen1 = start_server(server_db.clone(), &nats.nats_url, tenant, policies.clone()).await;
    let client = SyncClient::new(
        edge_db.clone(),
        &nats.nats_url,
        contextdb_core::TenantId::from(tenant),
    );

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
    let report = server_db.export_snapshot(&artifact).unwrap();
    assert_eq!(report.rows, 2, "artifact must capture exactly the A rows");

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
    let gen2 = start_server(
        restored_db.clone(),
        &nats.nats_url,
        tenant,
        policies.clone(),
    )
    .await;

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
    let observer = SyncClient::new(
        observer_db.clone(),
        &nats.nats_url,
        contextdb_core::TenantId::from(tenant),
    );
    within(observer.pull(&policies)).await.unwrap();
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
    within(client.pull(&policies)).await.unwrap();
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
    let nats = start_nats().await;
    let tmp = tempfile::TempDir::new().unwrap();
    let tenant = "sr2-empty-push-probe";
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);

    let server_db = Arc::new(Database::open(tmp.path().join("server-gen1.db")).unwrap());
    let edge_db = Arc::new(Database::open_memory());
    create_t(&server_db);
    create_t(&edge_db);

    let gen1 = start_server(server_db.clone(), &nats.nats_url, tenant, policies.clone()).await;
    let client = SyncClient::new(
        edge_db.clone(),
        &nats.nats_url,
        contextdb_core::TenantId::from(tenant),
    );

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
    let gen2 = start_server(
        restored_db.clone(),
        &nats.nats_url,
        tenant,
        policies.clone(),
    )
    .await;

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
    let nats = start_nats().await;
    let tmp = tempfile::TempDir::new().unwrap();
    let tenant = "sr3-pull-regression";
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);

    let server_db = Arc::new(Database::open(tmp.path().join("server-gen1.db")).unwrap());
    let edge_db = Arc::new(Database::open_memory());
    create_t(&server_db);
    create_t(&edge_db);

    let gen1 = start_server(server_db.clone(), &nats.nats_url, tenant, policies.clone()).await;
    let client = SyncClient::new(
        edge_db.clone(),
        &nats.nats_url,
        contextdb_core::TenantId::from(tenant),
    );

    // Server commits s1, s2; edge pulls them.
    let s1 = Uuid::new_v4();
    let s2 = Uuid::new_v4();
    insert_row(&server_db, s1, "s1");
    insert_row(&server_db, s2, "s2");
    let pull_1 = within(client.pull(&policies)).await.unwrap();
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
    let pull_2 = within(client.pull(&policies)).await.unwrap();
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

    let gen2 = start_server(
        restored_db.clone(),
        &nats.nats_url,
        tenant,
        policies.clone(),
    )
    .await;

    within(client.pull(&policies)).await.unwrap();
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
    within(client.pull(&policies)).await.unwrap();
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
    let nats = start_nats().await;
    let tenant = "sr4-steady-push";
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);

    let server_db = Arc::new(Database::open_memory());
    let edge_db = Arc::new(Database::open_memory());
    create_t(&server_db);
    create_t(&edge_db);
    let server_gen =
        start_server(server_db.clone(), &nats.nats_url, tenant, policies.clone()).await;
    let client = SyncClient::new(
        edge_db.clone(),
        &nats.nats_url,
        contextdb_core::TenantId::from(tenant),
    );

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
    let nats = start_nats().await;
    let tenant = "sr5-steady-pull";
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);

    let server_db = Arc::new(Database::open_memory());
    let edge_db = Arc::new(Database::open_memory());
    create_t(&server_db);
    create_t(&edge_db);
    let server_gen =
        start_server(server_db.clone(), &nats.nats_url, tenant, policies.clone()).await;
    let client = SyncClient::new(
        edge_db.clone(),
        &nats.nats_url,
        contextdb_core::TenantId::from(tenant),
    );

    let s1 = Uuid::new_v4();
    let s2 = Uuid::new_v4();
    insert_row(&server_db, s1, "s1");
    insert_row(&server_db, s2, "s2");

    let r1 = within(client.pull(&policies)).await.unwrap();
    assert_eq!(r1.applied_rows, 2, "first pull must apply exactly 2 rows");
    assert_eq!(r1.skipped_rows, 0, "first pull must skip 0 rows");
    let wm1 = client.pull_watermark();
    assert!(wm1 > Lsn(0), "pull watermark must advance");
    assert_eq!(row_set(&edge_db), expect_rows(&[(s1, "s1"), (s2, "s2")]));

    // No-op pull: zero applied/skipped, watermark stable, set unchanged.
    let r2 = within(client.pull(&policies)).await.unwrap();
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
    let r3 = within(client.pull(&policies)).await.unwrap();
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

// ======== sr6 — REGRESSION GUARD: new client + old server ========
//
// "Old server" means: a server that does not answer the status subject. Before
// the fix the real SyncServer is exactly that, so this runs the genuine
// no-responders path today. After the fix the real SyncServer answers status
// and this becomes a bounded steady-state guard; sr8's hand-rolled fixture keeps
// the no-responder path covered durably. Every op carries the tight 10s
// bound: an unanswered probe must never hang, error, churn watermarks, or
// re-push.
#[tokio::test]
async fn sr6_guard_new_client_with_old_server_completes_bounded_and_inert() {
    let nats = start_nats().await;
    let tenant = "sr6-new-client-old-server";
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);

    let server_db = Arc::new(Database::open_memory());
    let edge_db = Arc::new(Database::open_memory());
    create_t(&server_db);
    create_t(&edge_db);
    let server_gen =
        start_server(server_db.clone(), &nats.nats_url, tenant, policies.clone()).await;
    let client = SyncClient::new(
        edge_db.clone(),
        &nats.nats_url,
        contextdb_core::TenantId::from(tenant),
    );

    // Push with data: bounded, normal counts, watermark advances.
    let a1 = Uuid::new_v4();
    insert_row(&edge_db, a1, "a1");
    let push = bounded10(client.push()).await.unwrap();
    assert_eq!(
        push.applied_rows, 1,
        "push must apply normally against an old server"
    );
    let push_wm = client.push_watermark();
    assert!(push_wm > Lsn(0), "push watermark must advance normally");

    // Pull with data: bounded, normal counts, watermark advances.
    let s1 = Uuid::new_v4();
    insert_row(&server_db, s1, "s1");
    let pull = bounded10(client.pull(&policies)).await.unwrap();
    assert_eq!(
        pull.applied_rows, 1,
        "pull must apply normally against an old server"
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

// sr7 (the frozen wire-bytes regression guard) moved to the default-run
// `wire_format_freeze_tests.rs`: it is pure encode/decode against fixed
// fixtures, needs no broker, and this file's `[[test]]` entry gates the
// WHOLE binary on `required-features = ["nats"]` for the tests below that
// genuinely need one -- so it never ran in the default suite.

// ======== sr8 — REGRESSION GUARD: no status responder, bounded + exactly-once ========
//
// Durable no-responder fixture: a hand-rolled old server answers push and pull
// and — by construction, forever — never subscribes to the status subject.
// Unlike sr6 (real SyncServer, which gains a status responder once the fix
// lands), this keeps the no-responders path covered after implementation.
// A full sync cycle must complete with every op bounded to 10s, the single
// real row must cross the wire exactly once, and the push watermark must not
// churn.
#[cfg(feature = "nats")]
#[tokio::test]
async fn sr8_guard_sync_cycle_bounded_when_status_subject_has_no_responder() {
    use contextdb_server::protocol::SyncStatusResponse;
    use futures_util::StreamExt;
    use std::sync::atomic::AtomicUsize;

    let nats = start_nats().await;
    let tenant = "sr8-no-status-responder";

    // The status subject is dedicated: it must never collide with the data
    // subjects (otherwise this fixture could accidentally answer probes).
    let status = contextdb_server::subjects::status_subject(tenant);
    assert_ne!(status, contextdb_server::subjects::push_subject(tenant));
    assert_ne!(status, contextdb_server::subjects::pull_subject(tenant));

    // A defaulted status reply carries no regression signal — absent = inert.
    let inert = SyncStatusResponse::default();
    assert_eq!(inert.applied_push_watermark, None);
    assert_eq!(inert.server_current_lsn, None);

    // Old server: push + pull responders only.
    let responder = async_nats::connect(&nats.nats_url).await.unwrap();
    let mut push_sub = responder
        .subscribe(contextdb_server::subjects::push_subject(tenant))
        .await
        .unwrap();
    let mut pull_sub = responder
        .subscribe(contextdb_server::subjects::pull_subject(tenant))
        .await
        .unwrap();

    let rows_seen = Arc::new(AtomicUsize::new(0));
    let rows_seen_push = rows_seen.clone();
    let push_conn = responder.clone();
    tokio::spawn(async move {
        while let Some(msg) = push_sub.next().await {
            // Decode exactly as a shipped old server would; a failure here
            // leaves the client unanswered and fails the test via its bound.
            let envelope = decode(&msg.payload).expect("old server: envelope decode");
            assert!(
                matches!(envelope.message_type, MessageType::PushRequest),
                "old server: only plain push requests expected on the push subject"
            );
            let request: PushRequest = rmp_serde::from_slice(&envelope.payload)
                .expect("old server must decode the client's push request unchanged");
            let row_count = request.changeset.rows.len();
            rows_seen_push.fetch_add(row_count, Ordering::SeqCst);
            if let Some(reply) = msg.reply {
                let response = PushResponse {
                    result: Some(WireApplyResult {
                        applied_rows: row_count,
                        skipped_rows: 0,
                        conflicts: Vec::new(),
                        new_lsn: Lsn(2),
                    }),
                    error: None,
                };
                let payload = encode(MessageType::PushResponse, &response).unwrap();
                push_conn.publish(reply, payload.into()).await.unwrap();
            }
        }
    });

    let pull_conn = responder.clone();
    tokio::spawn(async move {
        while let Some(msg) = pull_sub.next().await {
            let envelope = decode(&msg.payload).expect("old server: envelope decode");
            assert!(
                matches!(envelope.message_type, MessageType::PullRequest),
                "old server: only plain pull requests expected on the pull subject"
            );
            let _request: PullRequest = rmp_serde::from_slice(&envelope.payload)
                .expect("old server must decode the client's pull request unchanged");
            if let Some(reply) = msg.reply {
                let response = PullResponse {
                    changeset: WireChangeSet::default(),
                    has_more: false,
                    cursor: None,
                    source: None,
                };
                let payload = encode(MessageType::PullResponse, &response).unwrap();
                pull_conn.publish(reply, payload.into()).await.unwrap();
            }
        }
    });

    let edge_db = Arc::new(Database::open_memory());
    create_t(&edge_db);
    let a1 = Uuid::new_v4();
    insert_row(&edge_db, a1, "a1");

    let client = SyncClient::new(
        edge_db.clone(),
        &nats.nats_url,
        contextdb_core::TenantId::from(tenant),
    );
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);

    // Full sync cycle, every op bounded to 10s.
    let r1 = bounded10(client.push()).await.unwrap();
    assert_eq!(
        r1.applied_rows, 1,
        "real push must succeed against the old server"
    );
    let wm1 = client.push_watermark();
    assert!(wm1 > Lsn(0));

    let r2 = bounded10(client.push()).await.unwrap();
    assert_eq!(
        r2.applied_rows, 0,
        "nothing-new push must stay inert without a status responder"
    );
    assert_eq!(
        client.push_watermark(),
        wm1,
        "no watermark churn without a status responder"
    );
    let r3 = bounded10(client.push()).await.unwrap();
    assert_eq!(r3.applied_rows, 0);
    assert_eq!(client.push_watermark(), wm1);

    let pull = bounded10(client.pull(&policies)).await.unwrap();
    assert_eq!(
        pull.applied_rows, 0,
        "empty pull from the old server must apply nothing"
    );

    assert_eq!(
        rows_seen.load(Ordering::SeqCst),
        1,
        "the single real row must cross the wire exactly once — an unanswered \
         status probe must never trigger spurious re-pushes"
    );
}
