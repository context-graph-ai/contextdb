use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_server::transfer_receipts::{TransferDirection, TransferPlane};
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{FabricIdentity, SyncClient, SyncServer, peer_dial_spec};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("bounded real-Iroh operation")
}

fn bind_spec(path: &Path) -> String {
    format!("iroh:?identity={}", path.display())
}

fn declare_notes(db: &Database) {
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .expect("declare notes");
}

fn insert_note(db: &Database, id: Uuid, body: &str) {
    let mut values = HashMap::new();
    values.insert("id".to_string(), Value::Uuid(id));
    values.insert("body".to_string(), Value::Text(body.to_string()));
    db.execute("INSERT INTO notes (id, body) VALUES ($id, $body)", &values)
        .expect("insert note");
}

struct Hub {
    server: Arc<SyncServer>,
    ticket: String,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

async fn start_hub(root: &Path, tenant: &str) -> Hub {
    let identity_path = root.join("hub.fabric-identity.key");
    let endpoint = IrohServer::bind(&bind_spec(&identity_path))
        .await
        .expect("bind hub");
    let identity = Arc::new(
        FabricIdentity::load_or_generate(&identity_path).expect("load hub fabric identity"),
    );
    let node_id = identity.node_id();
    let ticket = endpoint.ticket();
    let db = Arc::new(Database::open(root.join("hub.db")).expect("open hub database"));
    declare_notes(&db);
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            db,
            endpoint.transport(),
            TenantId::from(tenant),
            node_id,
            identity,
        ),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });
    Hub {
        server,
        ticket,
        stop,
        task,
    }
}

impl Hub {
    async fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        within(self.task)
            .await
            .expect("hub server stops within the bound");
    }
}

#[tokio::test]
async fn explicit_file_identity_overrides_the_adjacent_identity_across_client_reconstruction() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "explicit-file-identity";
    let hub = start_hub(root.path(), tenant).await;
    let db_path = root.path().join("edge.db");
    let adjacent_identity = root.path().join("edge.db.fabric-identity.key");
    let explicit_identity = root.path().join("operator-selected.key");
    let adjacent_node = FabricIdentity::load_or_generate(&adjacent_identity)
        .expect("persist adjacent identity")
        .node_id();
    let explicit_node = FabricIdentity::load_or_generate(&explicit_identity)
        .expect("persist explicit identity")
        .node_id();
    assert_ne!(adjacent_node, explicit_node, "the two durable keys differ");

    let edge = Arc::new(Database::open(&db_path).expect("open edge database"));
    declare_notes(&edge);
    let endpoint = peer_dial_spec(&hub.ticket, &explicit_identity);
    insert_note(&edge, Uuid::new_v4(), "first explicit identity push");
    let first = SyncClient::new(edge.clone(), &endpoint, TenantId::from(tenant));
    within(first.push())
        .await
        .expect("explicit identity sends the first push");
    first.shutdown().await;
    drop(first);

    insert_note(&edge, Uuid::new_v4(), "second explicit identity push");
    let reconstructed = SyncClient::new(edge, &endpoint, TenantId::from(tenant));
    within(reconstructed.push())
        .await
        .expect("reconstructed client reuses the explicit identity");
    reconstructed.shutdown().await;

    let receipts = hub.server.transfer_receipts();
    let explicit_receipt = receipts
        .iter()
        .find(|receipt| receipt.peer_node_id == explicit_node)
        .expect("hub records the operator-selected identity");
    assert_eq!(explicit_receipt.direction, TransferDirection::Received);
    assert_eq!(explicit_receipt.plane, TransferPlane::Sync);
    assert_eq!(
        explicit_receipt.counters.items, 2,
        "both reconstructed clients authenticate as the explicit identity"
    );
    assert!(
        receipts
            .iter()
            .all(|receipt| receipt.peer_node_id != adjacent_node),
        "the adjacent identity never reaches the hub when identity= is explicit"
    );
    hub.stop().await;
}
