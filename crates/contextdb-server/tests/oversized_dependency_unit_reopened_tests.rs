//! A dependency-complete unit larger than one Iroh frame, committed by an
//! earlier run of the source and pushed after the source store is reopened,
//! still travels as resumable chunks and commits once at the hub. Reading the
//! pending unit back from the reopened store must not refuse the push before
//! any chunk is offered.

use contextdb_core::{TenantId, Value, VersionedRow};
use contextdb_engine::{Database, DatabaseOpenOptions};
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{FabricIdentity, SyncClient, SyncServer, peer_dial_spec};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

const TENANT: &str = "oversized-dependency-unit-reopened";
const IROH_FRAME_CEILING_BYTES: usize = 64 * 1024 * 1024;

fn bind_spec(identity_path: &Path) -> String {
    format!("iroh:?identity={}", identity_path.display())
}

fn declare_tables(db: &Database) {
    db.execute(
        "CREATE TABLE parents (id UUID PRIMARY KEY, body TEXT) \
         SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .expect("declare parent table");
    db.execute(
        "CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id), body TEXT) \
         SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .expect("declare child table");
}

fn insert_parent(db: &Database, id: Uuid, body: String) {
    db.execute(
        "INSERT INTO parents (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body)),
        ]),
    )
    .expect("insert parent");
}

fn update_parent(db: &Database, id: Uuid, body: String) {
    db.execute(
        "UPDATE parents SET body = $body WHERE id = $id",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body)),
        ]),
    )
    .expect("update parent");
}

fn insert_child(db: &Database, id: Uuid, parent_id: Uuid) {
    db.execute(
        "INSERT INTO children (id, parent_id, body) VALUES ($id, $parent_id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("parent_id".to_string(), Value::Uuid(parent_id)),
            ("body".to_string(), Value::Text("outcome".to_string())),
        ]),
    )
    .expect("insert child");
}

/// The test's own check reads the 64 MiB row back, so it funds that caller
/// read explicitly rather than with the 16 MiB default.
fn row(db: &Database, table: &str, id: Uuid) -> Option<VersionedRow> {
    let fixture_read = contextdb_engine::vector_read_memory::DeclaredReadMemory::from_reader_limit(
        512 * 1024 * 1024,
    );
    db.point_lookup_with_read_memory_for_test(
        table,
        "id",
        &Value::Uuid(id),
        fixture_read.child().expect("admit the fixture read"),
    )
    .expect("point lookup")
}

struct Hub {
    db: Arc<Database>,
    endpoint: IrohServer,
    ticket: String,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

async fn start_hub(root: &Path) -> Hub {
    start_hub_with_writer_limit(root, None).await
}

async fn start_hub_with_writer_limit(root: &Path, memory_limit: Option<usize>) -> Hub {
    let identity_path = root.join("hub.db.fabric-identity.key");
    let endpoint = IrohServer::bind(&bind_spec(&identity_path))
        .await
        .expect("bind authenticated Iroh hub");
    let ticket = endpoint.ticket();
    let db = Arc::new(
        Database::open_with_options(
            root.join("hub.db"),
            DatabaseOpenOptions {
                memory_limit,
                ..DatabaseOpenOptions::default()
            },
        )
        .expect("open hub database"),
    );
    declare_tables(&db);
    let server = Arc::new(
        SyncServer::new(db.clone(), &endpoint, TenantId::from(TENANT))
            .expect("construct SyncServer fixture"),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });
    Hub {
        db,
        endpoint,
        ticket,
        stop,
        task,
    }
}

impl Hub {
    async fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        // No test-authored stopwatch: the hub's own run loop reacts to the
        // `stop` flag and exits its task; a task that never exits is a real
        // hang, and nextest's own harness slow-timeout is the liveness bound
        // for that, not an assertion this test makes.
        self.task
            .await
            .expect("authenticated Iroh hub run task must not panic");
        self.endpoint.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_oversized_unit_committed_before_the_source_reopens_still_pushes_and_commits_once() {
    let root = tempfile::tempdir().expect("tempdir");
    let hub = start_hub(root.path()).await;
    let edge_path = root.path().join("edge.db");
    let identity_path = root.path().join("edge.db.fabric-identity.key");
    FabricIdentity::load_or_generate(&identity_path).expect("persist edge identity");
    let dial_spec = peer_dial_spec(&hub.ticket, &identity_path);
    let parent_id = Uuid::new_v4();
    let child_id = Uuid::new_v4();
    {
        // The first run of the source declares, bootstraps its DDL, and
        // commits the oversized dependency unit, then closes its store.
        let edge = Arc::new(Database::open(&edge_path).expect("open edge database"));
        declare_tables(&edge);
        let client = SyncClient::new(edge.clone(), &dial_spec, TenantId::from(TENANT))
            .expect("construct SyncClient fixture");
        client
            .push()
            .await
            .expect("bootstrap DDL reaches the authenticated hub before the oversized unit");
        insert_parent(&edge, parent_id, "decision-before-evidence".to_string());
        insert_child(&edge, child_id, parent_id);
        update_parent(&edge, parent_id, "x".repeat(IROH_FRAME_CEILING_BYTES));
        client.shutdown().await;
        edge.close().expect("close the seeded source");
    }
    // A later run reopens the store with its default limits and pushes the
    // still-pending unit, which it must read back from storage.
    let edge = Arc::new(Database::open(&edge_path).expect("reopen edge database"));
    let source_lsn = edge.current_lsn();
    let client = SyncClient::new(edge.clone(), &dial_spec, TenantId::from(TENANT))
        .expect("construct reopened SyncClient");
    let pushed = client.push().await;
    assert!(
        pushed.is_ok(),
        "the reopened source pushes its pending oversized unit as resumable chunks: {pushed:?}"
    );
    assert_eq!(
        client.push_watermark(),
        source_lsn,
        "the confirmed push advances the source watermark to the unit"
    );
    let hub_parent = row(&hub.db, "parents", parent_id).expect("hub committed the parent");
    assert_eq!(
        hub_parent.values.get("body"),
        Some(&Value::Text("x".repeat(IROH_FRAME_CEILING_BYTES))),
        "the hub holds the complete oversized revision"
    );
    assert!(
        row(&hub.db, "children", child_id).is_some(),
        "the hub committed the dependent child with its parent"
    );
    client.shutdown().await;
    edge.close().expect("close the reopened source");
    hub.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reopened_source_whose_writer_budget_cannot_fit_its_pending_unit_refuses_typed() {
    const WRITER_LIMIT: usize = 96 * 1024 * 1024;
    let root = tempfile::tempdir().expect("tempdir");
    let hub = start_hub(root.path()).await;
    let edge_path = root.path().join("edge.db");
    let identity_path = root.path().join("edge.db.fabric-identity.key");
    FabricIdentity::load_or_generate(&identity_path).expect("persist edge identity");
    let dial_spec = peer_dial_spec(&hub.ticket, &identity_path);
    let parent_id = Uuid::new_v4();
    {
        let edge = Arc::new(Database::open(&edge_path).expect("open edge database"));
        declare_tables(&edge);
        let client = SyncClient::new(edge.clone(), &dial_spec, TenantId::from(TENANT))
            .expect("construct SyncClient fixture");
        client.push().await.expect("bootstrap DDL");
        insert_parent(&edge, parent_id, "decision-before-evidence".to_string());
        insert_child(&edge, Uuid::new_v4(), parent_id);
        update_parent(&edge, parent_id, "x".repeat(IROH_FRAME_CEILING_BYTES));
        client.shutdown().await;
        edge.close().expect("close the seeded source");
    }
    // The stored revision needs a 128 MiB page window, more than this
    // writer budget holds: the push refuses by type before allocating it.
    let edge = Arc::new(
        Database::open_with_options(
            &edge_path,
            DatabaseOpenOptions {
                memory_limit: Some(WRITER_LIMIT),
                ..DatabaseOpenOptions::default()
            },
        )
        .expect("reopen edge database under a smaller writer budget"),
    );
    let client = SyncClient::new(edge.clone(), &dial_spec, TenantId::from(TENANT))
        .expect("construct reopened SyncClient");
    let watermark = client.push_watermark();
    let refused = client.push().await;
    assert!(
        matches!(&refused, Err(contextdb_core::Error::MemoryBudgetExceeded {
            subsystem, operation, requested_bytes, budget_limit_bytes, ..
        }) if subsystem == "storage_pages"
            && operation == "read_page"
            && *requested_bytes > IROH_FRAME_CEILING_BYTES
            && *budget_limit_bytes == WRITER_LIMIT),
        "the writer budget refuses the pending unit's page by type: {refused:?}"
    );
    assert_eq!(
        client.push_watermark(),
        watermark,
        "a refused push advances nothing"
    );
    assert!(
        row(&hub.db, "parents", parent_id).is_none(),
        "the hub received nothing"
    );
    client.shutdown().await;
    edge.close().expect("close the reopened source");
    hub.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_push_under_a_tight_read_budget_delivers_its_oversized_unit_and_records_the_outcome() {
    // Far below the unit's 64 MiB body: it funds only the push's status reads.
    const READ_LIMIT: u64 = 4 * 1024 * 1024;
    let root = tempfile::tempdir().expect("tempdir");
    let hub = start_hub(root.path()).await;
    let edge_path = root.path().join("edge.db");
    let identity_path = root.path().join("edge.db.fabric-identity.key");
    FabricIdentity::load_or_generate(&identity_path).expect("persist edge identity");
    let dial_spec = peer_dial_spec(&hub.ticket, &identity_path);
    let parent_id = Uuid::new_v4();
    let child_id = Uuid::new_v4();
    {
        let edge = Arc::new(Database::open(&edge_path).expect("open edge database"));
        declare_tables(&edge);
        let client = SyncClient::new(edge.clone(), &dial_spec, TenantId::from(TENANT))
            .expect("construct SyncClient fixture");
        client.push().await.expect("bootstrap DDL");
        insert_parent(&edge, parent_id, "decision-before-evidence".to_string());
        insert_child(&edge, child_id, parent_id);
        update_parent(&edge, parent_id, "x".repeat(IROH_FRAME_CEILING_BYTES));
        client.shutdown().await;
        edge.close().expect("close the seeded source");
    }
    let edge = Arc::new(Database::open(&edge_path).expect("reopen edge database"));
    let source_lsn = edge.current_lsn();
    let client = SyncClient::new(edge.clone(), &dial_spec, TenantId::from(TENANT))
        .expect("construct reopened SyncClient");
    let tight =
        contextdb_engine::vector_read_memory::DeclaredReadMemory::from_reader_limit(READ_LIMIT);
    let pushed = client
        .push_with_read_memory(tight.child().expect("tight read budget"))
        .await;
    assert!(
        pushed.is_ok(),
        "selecting, sending and recording the unit is funded by the writer budget: {pushed:?}"
    );
    // The hub committed the unit, and the edge recorded that outcome: its
    // watermark covers the unit and nothing is left pending.
    let hub_parent = row(&hub.db, "parents", parent_id).expect("hub committed the parent");
    assert_eq!(
        hub_parent.values.get("body"),
        Some(&Value::Text("x".repeat(IROH_FRAME_CEILING_BYTES))),
        "the hub holds the complete oversized revision"
    );
    assert!(row(&hub.db, "children", child_id).is_some());
    assert_eq!(
        client.push_watermark(),
        source_lsn,
        "the edge recorded the confirmed push"
    );
    assert!(
        !client
            .has_pending_push_changes()
            .expect("inspect pending work"),
        "nothing remains pending after the confirmed push"
    );
    client.shutdown().await;
    edge.close().expect("close the reopened source");
    drop(edge);
    let reopened = Arc::new(Database::open(&edge_path).expect("reopen after the push"));
    let after = SyncClient::new(reopened.clone(), &dial_spec, TenantId::from(TENANT))
        .expect("construct client on the reopened edge");
    assert_eq!(
        after.push_watermark(),
        source_lsn,
        "the confirmed outcome is durable across reopen"
    );
    after.shutdown().await;
    reopened.close().expect("close the reopened edge");
    hub.stop().await;
}

/// Seed a source whose pending dependency unit holds one 64 MiB parent and
/// its child, then close it so the push reads the unit back from storage.
async fn seed_reopened_source(edge_path: &Path, dial_spec: &str) -> (Uuid, Uuid) {
    let parent_id = Uuid::new_v4();
    let child_id = Uuid::new_v4();
    let edge = Arc::new(Database::open(edge_path).expect("open edge database"));
    declare_tables(&edge);
    let client = SyncClient::new(edge.clone(), dial_spec, TenantId::from(TENANT))
        .expect("construct SyncClient fixture");
    client.push().await.expect("bootstrap DDL");
    insert_parent(&edge, parent_id, "decision-before-evidence".to_string());
    insert_child(&edge, child_id, parent_id);
    update_parent(&edge, parent_id, "x".repeat(IROH_FRAME_CEILING_BYTES));
    client.shutdown().await;
    edge.close().expect("close the seeded source");
    (parent_id, child_id)
}

fn is_receive_refusal(error: &contextdb_core::Error, limit: usize) -> bool {
    matches!(error, contextdb_core::Error::MemoryBudgetExceeded {
        subsystem, operation, requested_bytes, budget_limit_bytes, ..
    } if subsystem == "sync_receive"
        && operation == "assemble_pushed_unit"
        && *requested_bytes >= 2 * IROH_FRAME_CEILING_BYTES
        && *budget_limit_bytes == limit)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_unit_the_hub_writer_budget_can_never_fit_is_refused_before_it_transfers() {
    // Below the unit's assembly peak (fragments read back plus the payload).
    const HUB_LIMIT: usize = 96 * 1024 * 1024;
    let root = tempfile::tempdir().expect("tempdir");
    let hub = start_hub_with_writer_limit(root.path(), Some(HUB_LIMIT)).await;
    let edge_path = root.path().join("edge.db");
    let identity_path = root.path().join("edge.db.fabric-identity.key");
    FabricIdentity::load_or_generate(&identity_path).expect("persist edge identity");
    let dial_spec = peer_dial_spec(&hub.ticket, &identity_path);
    let (parent_id, _) = seed_reopened_source(&edge_path, &dial_spec).await;

    let edge = Arc::new(Database::open(&edge_path).expect("reopen edge database"));
    let client = SyncClient::new(edge.clone(), &dial_spec, TenantId::from(TENANT))
        .expect("construct reopened SyncClient");
    let watermark = client.push_watermark();
    let refused = client.push().await;
    assert!(
        matches!(&refused, Err(error) if is_receive_refusal(error, HUB_LIMIT)),
        "the hub refuses by type, naming its writer budget, never as a lost reply: {refused:?}"
    );
    assert_eq!(
        client.push_watermark(),
        watermark,
        "a refused push advances nothing"
    );
    assert!(
        client
            .has_pending_push_changes()
            .expect("inspect pending work"),
        "the refused unit stays pending at the edge"
    );
    assert!(
        row(&hub.db, "parents", parent_id).is_none(),
        "the hub applied nothing"
    );
    client.shutdown().await;
    edge.close().expect("close the reopened source");
    hub.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_unit_refused_under_transient_hub_pressure_stays_pending_and_lands_on_retry() {
    const HUB_LIMIT: usize = 1024 * 1024 * 1024;
    // Leaves less free than the unit's assembly peak, while the limit itself
    // could fit it.
    const HELD: usize = HUB_LIMIT - 100 * 1024 * 1024;
    let root = tempfile::tempdir().expect("tempdir");
    let hub = start_hub_with_writer_limit(root.path(), Some(HUB_LIMIT)).await;
    let edge_path = root.path().join("edge.db");
    let identity_path = root.path().join("edge.db.fabric-identity.key");
    FabricIdentity::load_or_generate(&identity_path).expect("persist edge identity");
    let dial_spec = peer_dial_spec(&hub.ticket, &identity_path);
    let (parent_id, child_id) = seed_reopened_source(&edge_path, &dial_spec).await;

    let edge = Arc::new(Database::open(&edge_path).expect("reopen edge database"));
    let source_lsn = edge.current_lsn();
    let client = SyncClient::new(edge.clone(), &dial_spec, TenantId::from(TENANT))
        .expect("construct reopened SyncClient");
    let watermark = client.push_watermark();
    let pressure = hub
        .db
        .hold_writer_memory_for_test(HELD)
        .expect("hold most of the hub's writer budget");
    let refused = client.push().await;
    assert!(
        matches!(&refused, Err(error) if is_receive_refusal(error, HUB_LIMIT)),
        "assembly under pressure refuses by type, never as a lost reply: {refused:?}"
    );
    assert_eq!(
        client.push_watermark(),
        watermark,
        "a refused push advances nothing"
    );
    assert!(
        client
            .has_pending_push_changes()
            .expect("inspect pending work"),
        "the refused unit stays pending at the edge"
    );
    assert!(
        row(&hub.db, "parents", parent_id).is_none(),
        "the hub applied nothing"
    );

    drop(pressure);
    let retried = client.push().await;
    assert!(
        retried.is_ok(),
        "the same unit lands once the pressure is gone: {retried:?}"
    );
    assert_eq!(
        client.push_watermark(),
        source_lsn,
        "the edge recorded the confirmed push"
    );
    let hub_parent = row(&hub.db, "parents", parent_id).expect("hub committed the parent");
    assert_eq!(
        hub_parent.values.get("body"),
        Some(&Value::Text("x".repeat(IROH_FRAME_CEILING_BYTES))),
        "the hub holds the complete oversized revision"
    );
    assert!(row(&hub.db, "children", child_id).is_some());
    client.shutdown().await;
    edge.close().expect("close the reopened source");
    hub.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_edge_pulls_the_oversized_unit_and_converges() {
    let root = tempfile::tempdir().expect("tempdir");
    let hub = start_hub(root.path()).await;
    let source_path = root.path().join("edge.db");
    let source_identity = root.path().join("edge.db.fabric-identity.key");
    FabricIdentity::load_or_generate(&source_identity).expect("persist source identity");
    let source_dial = peer_dial_spec(&hub.ticket, &source_identity);
    let (parent_id, child_id) = seed_reopened_source(&source_path, &source_dial).await;
    let source = Arc::new(Database::open(&source_path).expect("reopen the source"));
    let source_client = SyncClient::new(source.clone(), &source_dial, TenantId::from(TENANT))
        .expect("construct the source client");
    source_client
        .push()
        .await
        .expect("the source delivers its oversized unit to the hub");
    source_client.shutdown().await;
    source.close().expect("close the source");

    // A second edge that never saw the unit pulls it from the hub.
    let reader_path = root.path().join("reader.db");
    let reader_identity = root.path().join("reader.db.fabric-identity.key");
    FabricIdentity::load_or_generate(&reader_identity).expect("persist reader identity");
    let reader_dial = peer_dial_spec(&hub.ticket, &reader_identity);
    let reader = Arc::new(Database::open(&reader_path).expect("open the second edge"));
    let reader_client = SyncClient::new(reader.clone(), &reader_dial, TenantId::from(TENANT))
        .expect("construct the second edge client");
    let pulled = reader_client.pull_default().await;
    assert!(
        pulled.is_ok(),
        "the hub serves the oversized unit and the second edge applies it: {pulled:?}"
    );
    let parent = row(&reader, "parents", parent_id).expect("the second edge holds the parent");
    assert_eq!(
        parent.values.get("body"),
        Some(&Value::Text("x".repeat(IROH_FRAME_CEILING_BYTES))),
        "the second edge converged on the complete oversized revision"
    );
    assert!(
        row(&reader, "children", child_id).is_some(),
        "the second edge converged on the dependent child"
    );
    reader_client.shutdown().await;
    reader.close().expect("close the second edge");
    hub.stop().await;
}
