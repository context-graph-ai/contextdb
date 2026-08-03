//! A pull-only local edit is never outbound and is replaced by its hub value.

use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
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
        .expect("bounded ticketed-Iroh operation")
}

fn bind_spec(path: &Path) -> String {
    format!("iroh:?identity={}", path.display())
}

fn declare_hub(db: &Database) {
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC PULL ONLY SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .expect("declare hub table");
}

fn declare_pull_only_edge(db: &Database) {
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC PULL ONLY SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .expect("declare pull-only edge table");
}

fn insert(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO notes (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("insert exact note");
}

fn body(db: &Database, id: Uuid) -> Option<String> {
    db.execute(
        "SELECT body FROM notes WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .expect("read exact note")
    .rows
    .first()
    .map(|row| match &row[0] {
        Value::Text(value) => value.clone(),
        other => panic!("notes.body must be TEXT, got {other:?}"),
    })
}

struct Hub {
    db: Arc<Database>,
    ticket: String,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

async fn start_hub(root: &Path, tenant: &str) -> Hub {
    let identity = root.join("hub.db.fabric-identity.key");
    let endpoint = IrohServer::bind(&bind_spec(&identity))
        .await
        .expect("bind hub");
    let ticket = endpoint.ticket();
    let db = Arc::new(Database::open(root.join("hub.db")).expect("open file-backed hub"));
    declare_hub(&db);
    let server = Arc::new(SyncServer::new(
        db.clone(),
        &endpoint,
        TenantId::from(tenant),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });
    Hub {
        db,
        ticket,
        stop,
        task,
    }
}

impl Hub {
    async fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        within(self.task).await.expect("hub stops cleanly");
    }
}

#[tokio::test]
async fn pull_only_local_edit_is_replaced_by_hub_value() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "pull-only-overwrite";
    let id = Uuid::from_u128(0x9ca2_6d37_2ee4_48b1_b998_1d3c_75a5_0333);
    let hub = start_hub(root.path(), tenant).await;
    insert(&hub.db, id, "authoritative-hub-value");

    let edge_path = root.path().join("edge.db");
    let edge_identity = root.path().join("edge.db.fabric-identity.key");
    FabricIdentity::load_or_generate(&edge_identity).expect("persist edge identity");
    let edge = Arc::new(Database::open(edge_path).expect("open file-backed edge"));
    declare_pull_only_edge(&edge);
    insert(&edge, id, "local-edit-that-must-not-leak");
    let client = SyncClient::new(
        edge.clone(),
        &peer_dial_spec(&hub.ticket, &edge_identity),
        TenantId::from(tenant),
    );

    let outbound = within(client.push())
        .await
        .expect("a pull-only local edit does not make an outbound row mutation");
    assert_eq!(
        (
            outbound.applied_rows,
            outbound.skipped_rows,
            outbound.conflicts.len()
        ),
        (0, 0, 0),
        "a pull-only local edit creates neither an outbound write nor a hidden KEEP FIRST refusal"
    );
    assert_eq!(
        body(&hub.db, id).as_deref(),
        Some("authoritative-hub-value"),
        "the local pull-only edit never leaks to the hub"
    );
    within(client.pull_default())
        .await
        .expect("authenticated pull receives the hub value");
    assert_eq!(
        body(&edge, id).as_deref(),
        Some("authoritative-hub-value"),
        "pull-only local edit is replaced by the exact hub value"
    );
    assert_eq!(
        body(&hub.db, id).as_deref(),
        Some("authoritative-hub-value"),
        "the overwrite never mutates the hub winner"
    );
    within(client.shutdown()).await;
    hub.stop().await;
}
