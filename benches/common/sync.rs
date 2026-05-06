use contextdb_core::{Lsn, Value, VectorIndexRef};
use contextdb_engine::Database;
use contextdb_engine::sync_types::ConflictPolicies;
use contextdb_server::SyncClient;
use contextdb_server::protocol::{
    MessageType, PullRequest, PullResponse, PushRequest, PushResponse, decode, encode,
};
use contextdb_server::subjects::{pull_subject, push_subject};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use uuid::Uuid;

use crate::common::process::workspace_root;

pub struct NatsFixture {
    _container: ContainerAsync<GenericImage>,
    pub nats_url: String,
    pub ws_url: String,
}

pub async fn start_nats() -> NatsFixture {
    let conf = workspace_root()
        .join("crates/contextdb-engine/tests/nats.conf")
        .to_string_lossy()
        .into_owned();
    let image = GenericImage::new("nats", "latest")
        .with_exposed_port(4222.tcp())
        .with_exposed_port(9222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"));
    let request = image
        .with_mount(Mount::bind_mount(conf, "/etc/nats/nats.conf"))
        .with_cmd(["--js", "--config", "/etc/nats/nats.conf"]);
    let container: ContainerAsync<GenericImage> =
        request.start().await.expect("NATS container should start");
    let nats_port = container
        .get_host_port_ipv4(4222.tcp())
        .await
        .expect("NATS port should be mapped");
    let ws_port = container
        .get_host_port_ipv4(9222.tcp())
        .await
        .expect("NATS websocket port should be mapped");
    NatsFixture {
        _container: container,
        nats_url: format!("nats://127.0.0.1:{nats_port}"),
        ws_url: format!("ws://127.0.0.1:{ws_port}"),
    }
}

pub async fn wait_for_server_ready(edge_url: &str, tenant: &str, policies: &ConflictPolicies) {
    let probe_db = Arc::new(Database::open_memory());
    let probe_client = SyncClient::new(probe_db, edge_url, tenant);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match probe_client.pull(policies).await {
            Ok(_) => return,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(err) => panic!("server did not become ready for tenant {tenant}: {err}"),
        }
    }
}

pub async fn wait_for_sync_server_ready(
    nats_url: &str,
    tenant_id: &str,
    timeout: Duration,
) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        let client = match async_nats::connect(nats_url).await {
            Ok(client) => client,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        let inbox = client.new_inbox();
        let mut inbox_sub = match client.subscribe(inbox.clone()).await {
            Ok(sub) => sub,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        let payload = match encode(
            MessageType::PullRequest,
            &PullRequest {
                since_lsn: Lsn(0),
                max_entries: Some(1),
            },
        ) {
            Ok(payload) => payload,
            Err(_) => return false,
        };

        if client
            .publish_with_reply(pull_subject(tenant_id), inbox.clone(), payload.into())
            .await
            .is_err()
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        let response = tokio::time::timeout(Duration::from_millis(500), inbox_sub.next()).await;
        match response {
            Ok(Some(msg)) if msg.status == Some(async_nats::StatusCode::NO_RESPONDERS) => {}
            Ok(Some(msg)) if msg.status.is_some() => {}
            Ok(Some(msg)) => {
                if let Ok(envelope) = decode(&msg.payload)
                    && matches!(envelope.message_type, MessageType::PullResponse)
                    && rmp_serde::from_slice::<PullResponse>(&envelope.payload).is_ok()
                {
                    return true;
                }
            }
            Ok(None) | Err(_) => {}
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    false
}

async fn wait_for_path(path: &std::path::Path, timeout: Duration) {
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
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

pub async fn push_change_through_server(
    nats_url: &str,
    tenant_id: &str,
    table: &str,
    id: Uuid,
    vector: Vec<f32>,
) {
    let tmp = TempDir::new().expect("tempdir for edge push");
    let edge_path = tmp.path().join("edge-push.db");
    let db = Arc::new(Database::open(&edge_path).expect("open edge db"));
    db.execute(
        &format!(
            "CREATE TABLE {table} (id UUID PRIMARY KEY, vector_text VECTOR({}))",
            vector.len()
        ),
        &empty_params(),
    )
    .expect("create edge schema");

    let tx = db.begin();
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

    let client = SyncClient::new(db, nats_url, tenant_id);
    client.push().await.expect("push change through server");
}

pub async fn push_many_changes_through_server(
    nats_url: &str,
    tenant_id: &str,
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
        &format!("CREATE TABLE {table} (id UUID PRIMARY KEY, vector_text VECTOR({dim}))"),
        &empty_params(),
    )
    .expect("create edge schema");

    let tx = db.begin();
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

    let client = async_nats::connect(nats_url)
        .await
        .expect("connect to NATS for direct in-flight push");
    let inbox = client.new_inbox();
    let mut inbox_sub = client
        .subscribe(inbox.clone())
        .await
        .expect("subscribe direct push inbox");
    let request = PushRequest {
        changeset: db.changes_since(Lsn(0)).into(),
    };
    let encoded =
        encode(MessageType::PushRequest, &request).expect("encode direct in-flight push request");
    client
        .publish_with_reply(push_subject(tenant_id), inbox.clone(), encoded.into())
        .await
        .expect("publish direct in-flight push request");
    client
        .flush()
        .await
        .expect("flush direct in-flight push request");

    if let Some(started) = started {
        if let Some(path) = server_barrier_path {
            wait_for_path(&path, Duration::from_secs(10)).await;
        }
        let _ = started.send(());
    }

    let msg = tokio::time::timeout(Duration::from_secs(45), inbox_sub.next())
        .await
        .expect("direct push must respond after graceful drain")
        .expect("direct push inbox must stay open");
    let envelope = decode(&msg.payload).expect("decode direct push response envelope");
    let response: PushResponse =
        rmp_serde::from_slice(&envelope.payload).expect("decode direct push response");
    if let Some(err) = response.error {
        panic!("direct push response error: {err}");
    }
    let _ = response
        .result
        .expect("direct push response must include apply result");
}
