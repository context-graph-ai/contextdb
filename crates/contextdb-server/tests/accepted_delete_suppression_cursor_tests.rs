//! An accepted delete consumes a stale ordinary pull page without reviving it.

use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_server::transport::iroh::{IrohServer, LargeRequestTestController};
use contextdb_server::{SyncClient, SyncServer};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

async fn within<F: std::future::Future>(operation: &str, future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .unwrap_or_else(|_| panic!("bounded authenticated Iroh operation timed out: {operation}"))
}

fn spec(path: &Path) -> String {
    format!("iroh:?identity={}", path.display())
}

fn create_notes(db: &Database) {
    if db.table_meta("notes").is_none() {
        db.execute(
            "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST",
            &HashMap::new(),
        )
        .expect("create notes");
    }
}

fn put(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO notes (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("insert note");
}

fn delete(db: &Database, id: Uuid) {
    db.execute(
        "DELETE FROM notes WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .expect("delete note");
}

fn body(db: &Database, id: Uuid) -> Option<String> {
    db.execute(
        "SELECT body FROM notes WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .expect("read note")
    .rows
    .first()
    .map(|row| match &row[0] {
        Value::Text(value) => value.clone(),
        other => panic!("notes.body must be text, got {other:?}"),
    })
}

struct Hub {
    db: Arc<Database>,
    ticket: String,
    node_id: String,
    controller: LargeRequestTestController,
    shutdown_operation: &'static str,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

async fn start_hub(root: &Path, tenant: &str, shutdown_operation: &'static str) -> Hub {
    std::fs::create_dir_all(root).expect("create hub directory");
    let endpoint = IrohServer::bind(&spec(&root.join("hub.fabric-identity.key")))
        .await
        .expect("bind authenticated hub");
    let controller = endpoint.large_request_test_controller();
    let ticket = endpoint.ticket();
    let node_id = endpoint.node_id();
    let db = Arc::new(Database::open(root.join("hub.redb")).expect("open hub"));
    create_notes(&db);
    let server = Arc::new(SyncServer::new(
        db.clone(),
        &endpoint,
        TenantId::from(tenant),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });
    controller.wait_until_routes_ready_for_test().await;
    Hub {
        db,
        ticket,
        node_id,
        controller,
        shutdown_operation,
        stop,
        task,
    }
}

impl Hub {
    async fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        within(self.shutdown_operation, self.task)
            .await
            .expect("hub stops");
    }
}

async fn assert_reply_receipt_progress(
    operation: &str,
    controller: &LargeRequestTestController,
    previous_successes: &mut usize,
) {
    within(operation, controller.wait_until_requests_idle_for_test()).await;
    let observed = controller.observations_for_test();
    assert!(
        observed.successful_reply_receipts > *previous_successes,
        "each completed authenticated operation must have a server-consumed reply receipt: {observed:?}"
    );
    assert_eq!(
        observed.terminal_reply_receipt_failures, 0,
        "a successful authenticated operation must not leave a terminal reply receipt failure: {observed:?}"
    );
    *previous_successes = observed.successful_reply_receipts;
}

#[tokio::test]
async fn accepted_delete_suppression_consumes_stale_cursor_across_reopen() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "accepted-delete-suppression-cursor";
    let hub = start_hub(
        &root.path().join("hub"),
        tenant,
        "authoritative hub shutdown",
    )
    .await;
    let alternate = start_hub(
        &root.path().join("alternate"),
        tenant,
        "alternate hub shutdown",
    )
    .await;
    let mut authoritative_receipts = 0;
    let mut alternate_receipts = 0;
    let edge_path = root.path().join("edge.redb");
    let edge = Arc::new(Database::open(&edge_path).expect("open edge"));
    create_notes(&edge);
    let client = SyncClient::new(edge.clone(), &hub.ticket, TenantId::from(tenant));
    let id = Uuid::new_v4();

    put(&edge, id, "original");
    within("original edge push to authoritative hub", client.push())
        .await
        .expect("original reaches hub");
    assert_reply_receipt_progress(
        "original edge push reply receipt",
        &hub.controller,
        &mut authoritative_receipts,
    )
    .await;
    let carrier_path = root.path().join("carrier.redb");
    let carrier = Arc::new(Database::open(&carrier_path).expect("open carrier"));
    create_notes(&carrier);
    let carrier_client = SyncClient::new(carrier.clone(), &hub.ticket, TenantId::from(tenant));
    within(
        "carrier pull from authoritative hub",
        carrier_client.pull_default(),
    )
    .await
    .expect("carrier retains original lineage");
    assert_reply_receipt_progress(
        "carrier pull reply receipt",
        &hub.controller,
        &mut authoritative_receipts,
    )
    .await;
    assert_eq!(body(&carrier, id).as_deref(), Some("original"));
    drop(client);

    let client = SyncClient::new(edge.clone(), &hub.ticket, TenantId::from(tenant));
    delete(&edge, id);
    within("edge delete push to authoritative hub", client.push())
        .await
        .expect("accept delete at authoritative hub");
    assert_reply_receipt_progress(
        "edge delete push reply receipt",
        &hub.controller,
        &mut authoritative_receipts,
    )
    .await;
    client
        .change_destination(&alternate.node_id)
        .expect("select still-empty alternate");
    drop(client);
    let client = SyncClient::new(edge.clone(), &alternate.ticket, TenantId::from(tenant));
    within(
        "edge baseline pull from alternate hub",
        client.pull_default(),
    )
    .await
    .expect("consume alternate schema baseline");
    assert_reply_receipt_progress(
        "edge baseline pull reply receipt",
        &alternate.controller,
        &mut alternate_receipts,
    )
    .await;
    let baseline = client.pull_watermark();
    assert!(baseline.0 > 0);

    carrier_client
        .change_destination(&alternate.node_id)
        .expect("move carrier to alternate");
    drop(carrier_client);
    let carrier_to_alternate =
        SyncClient::new(carrier.clone(), &alternate.ticket, TenantId::from(tenant));
    within(
        "carrier stale-row push to alternate hub",
        carrier_to_alternate.push(),
    )
    .await
    .expect("carrier publishes inherited stale row");
    assert_reply_receipt_progress(
        "carrier stale-row push reply receipt",
        &alternate.controller,
        &mut alternate_receipts,
    )
    .await;
    assert_eq!(body(&alternate.db, id).as_deref(), Some("original"));

    let first = within(
        "edge stale ordinary pull from alternate hub",
        client.pull_default(),
    )
    .await
    .expect("consume stale ordinary page");
    assert_reply_receipt_progress(
        "edge stale ordinary pull reply receipt",
        &alternate.controller,
        &mut alternate_receipts,
    )
    .await;
    assert_eq!(first.applied_rows, 0);
    assert_eq!(body(&edge, id), None);
    let first_watermark = client.pull_watermark();
    assert!(first_watermark > baseline);
    let persisted = edge
        .persisted_sync_pull_cursor(&TenantId::from(tenant))
        .expect("read persisted cursor")
        .expect("stale page advances durable cursor");
    assert_eq!(persisted.1, first_watermark);
    drop(client);
    drop(edge);

    let edge = Arc::new(Database::open(&edge_path).expect("reopen edge"));
    create_notes(&edge);
    let client = SyncClient::new(edge.clone(), &alternate.ticket, TenantId::from(tenant));
    assert_eq!(client.pull_watermark(), first_watermark);
    let second = within(
        "edge quiet pull from alternate hub after reopen",
        client.pull_default(),
    )
    .await
    .expect("second pull is quiet");
    assert_reply_receipt_progress(
        "edge quiet pull reply receipt",
        &alternate.controller,
        &mut alternate_receipts,
    )
    .await;
    assert_eq!(second.applied_rows, 0);
    assert_eq!(second.skipped_rows, 0);
    assert_eq!(client.pull_watermark(), first_watermark);
    assert_eq!(body(&edge, id), None);
    within("reopened edge client shutdown", client.shutdown()).await;
    within(
        "carrier alternate client shutdown",
        carrier_to_alternate.shutdown(),
    )
    .await;
    alternate.stop().await;
    hub.stop().await;
}
