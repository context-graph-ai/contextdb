use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::Database;
use contextdb_server::SyncClient;
use contextdb_server::transport::iroh::IrohServer;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use uuid::Uuid;

pub struct SyncFixture {
    _root: TempDir,
    pub bind_spec: String,
    pub ticket: String,
}

pub async fn start_sync_fixture() -> SyncFixture {
    let root = TempDir::new().expect("sync fixture tempdir");
    let identity = root.path().join("hub-identity.key");
    let bind_spec = contextdb_server::peer_bind_spec(&identity);
    let endpoint = IrohServer::bind(&bind_spec)
        .await
        .expect("reserve benchmark sync endpoint");
    let ticket = endpoint.ticket();
    endpoint.close().await;
    SyncFixture {
        _root: root,
        bind_spec,
        ticket,
    }
}

async fn wait_for_path(path: &Path, timeout: Duration) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for path to exist: {}", path.display());
}

fn empty_params() -> HashMap<String, Value> {
    HashMap::new()
}

fn values(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

pub async fn push_change_through_server(
    ticket: &str,
    tenant: &str,
    table: &str,
    id: Uuid,
    vector: Vec<f32>,
) {
    let tmp = TempDir::new().expect("tempdir for edge push");
    let edge_path = tmp.path().join("edge-push.db");
    let db = Arc::new(Database::open(&edge_path).expect("open edge db"));
    db.execute(
        &format!(
            "CREATE TABLE {table} (id UUID PRIMARY KEY, vector_text VECTOR({})) SYNC CONFLICT KEEP FIRST",
            vector.len()
        ),
        &empty_params(),
    )
    .expect("create edge schema");

    let tx = db.begin_or_panic();
    let row_id = db
        .insert_row(tx, table, values(vec![("id", Value::Uuid(id))]))
        .expect("insert edge row");
    db.insert_vector(
        tx,
        VectorIndexRef::new(table, "vector_text"),
        row_id,
        vector,
    )
    .expect("insert edge vector");
    db.commit(tx).expect("commit edge row");

    let identity = PathBuf::from(format!("{}.fabric-identity.key", edge_path.display()));
    let dial_spec = contextdb_server::peer_dial_spec(ticket, &identity);
    let client = SyncClient::new(db, &dial_spec, contextdb_core::TenantId::from(tenant));
    client.push().await.expect("push change through server");
}

pub async fn push_many_changes_through_server(
    ticket: &str,
    tenant: &str,
    table: &str,
    ids: Vec<Uuid>,
    dim: usize,
    started: Option<tokio::sync::oneshot::Sender<()>>,
    server_barrier_path: Option<PathBuf>,
) {
    let tmp = TempDir::new().expect("tempdir for edge push");
    let edge_path = tmp.path().join("edge-push-many.db");
    let db = Arc::new(Database::open(&edge_path).expect("open edge db"));
    db.execute(
        &format!(
            "CREATE TABLE {table} (id UUID PRIMARY KEY, vector_text VECTOR({dim})) SYNC CONFLICT KEEP FIRST"
        ),
        &empty_params(),
    )
    .expect("create edge schema");

    let tx = db.begin_or_panic();
    for (offset, id) in ids.into_iter().enumerate() {
        let row_id = db
            .insert_row(tx, table, values(vec![("id", Value::Uuid(id))]))
            .expect("insert edge row");
        let mut vector = vec![0.0_f32; dim];
        vector[offset % dim] = 1.0;
        db.insert_vector(
            tx,
            VectorIndexRef::new(table, "vector_text"),
            row_id,
            vector,
        )
        .expect("insert edge vector");
    }
    db.commit(tx).expect("commit edge rows");

    let identity = PathBuf::from(format!("{}.fabric-identity.key", edge_path.display()));
    let dial_spec = contextdb_server::peer_dial_spec(ticket, &identity);
    let client = SyncClient::new(db, &dial_spec, contextdb_core::TenantId::from(tenant));
    let push_task = tokio::spawn(async move { client.push().await });

    if let Some(started) = started {
        if let Some(path) = server_barrier_path {
            wait_for_path(&path, Duration::from_secs(10)).await;
        }
        let _ = started.send(());
    }

    tokio::time::timeout(Duration::from_secs(45), push_task)
        .await
        .expect("direct push must respond after graceful drain")
        .expect("direct push task must not panic")
        .expect("direct push must succeed");
}
