use super::helpers::*;
use contextdb_core::Value;
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use contextdb_server::{SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

struct NatsFixture {
    _container: ContainerAsync<GenericImage>,
    nats_url: String,
}

fn setup_nats_conf() -> String {
    format!("{}/tests/nats.conf", env!("CARGO_MANIFEST_DIR"))
}

async fn start_nats() -> NatsFixture {
    let image = GenericImage::new("nats", "latest")
        .with_exposed_port(4222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"));
    let request = image
        .with_mount(Mount::bind_mount(setup_nats_conf(), "/etc/nats/nats.conf"))
        .with_cmd(["--js", "--config", "/etc/nats/nats.conf"]);
    let container = request.start().await.unwrap();
    let nats_port = container.get_host_port_ipv4(4222.tcp()).await.unwrap();
    NatsFixture {
        _container: container,
        nats_url: format!("nats://127.0.0.1:{nats_port}"),
    }
}

#[test]
fn hc_f01_ontology_ops() {
    let db = setup_ontology_db();
    let id = uuid::Uuid::new_v4();
    db.execute(
        "INSERT INTO entities (id, name) VALUES ($id, $name)",
        &make_params(vec![
            ("id", Value::Uuid(id)),
            ("name", Value::Text("n".into())),
        ]),
    )
    .unwrap();
    assert_eq!(db.scan("entities", db.snapshot()).unwrap().len(), 1);
}

#[test]
fn hc_f03_ignored_hnsw_recall() {}

#[test]
#[ignore = "requires ARM64 cross compile"]
fn hc_f06_ignored_arm64() {}

#[test]
fn hc_t01_oom_does_not_partially_commit_visible_state() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("hc_t01.db");
    let db = Database::open(&path).unwrap();
    db.execute("SET MEMORY_LIMIT '1K'", &HashMap::new())
        .unwrap();
    db.execute(
        "CREATE TABLE big (id UUID PRIMARY KEY, payload TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    db.close().unwrap();

    let reopened = Database::open(&path).unwrap();
    let err = reopened
        .execute(
            "INSERT INTO big (id, payload) VALUES ($id, $payload)",
            &make_params(vec![
                ("id", Value::Uuid(uuid::Uuid::new_v4())),
                ("payload", Value::Text("x".repeat(4096))),
            ]),
        )
        .unwrap_err();
    assert!(
        matches!(err, contextdb_core::Error::MemoryBudgetExceeded { .. }),
        "reopened database must keep enforcing the configured memory limit: {err:?}"
    );
    assert_eq!(
        reopened.scan("big", reopened.snapshot()).unwrap().len(),
        0,
        "OOM failure after reopen must not leave a partially visible row"
    );
}

#[tokio::test]
async fn hc_t02_restart_preserves_committed_sync_visibility() {
    let nats = start_nats().await;
    let edge_tmp = TempDir::new().unwrap();
    let server_tmp = TempDir::new().unwrap();
    let edge_path = edge_tmp.path().join("edge.db");
    let server_path = server_tmp.path().join("server.db");

    let edge_db = Arc::new(Database::open(&edge_path).unwrap());
    let server_db = Arc::new(Database::open(&server_path).unwrap());
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let empty = HashMap::new();
    edge_db
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)", &empty)
        .unwrap();
    server_db
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)", &empty)
        .unwrap();

    let first_id = uuid::Uuid::new_v4();
    let second_id = uuid::Uuid::new_v4();
    for id in [first_id, second_id] {
        server_db
            .execute(
                "INSERT INTO t (id, v) VALUES ($id, $v)",
                &HashMap::from([
                    ("id".to_string(), Value::Uuid(id)),
                    ("v".to_string(), Value::Text("before_restart".to_string())),
                ]),
            )
            .unwrap();
    }

    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "hc_t02",
        policies.clone(),
    ));
    let handle = tokio::spawn({
        let server = server.clone();
        async move { server.run().await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "hc_t02");
    let initial_pull = client.pull(&policies).await.unwrap();
    assert_eq!(initial_pull.applied_rows, 2);
    assert_eq!(initial_pull.skipped_rows, 0);

    handle.abort();
    let _ = handle.await;
    drop(server);
    drop(client);
    drop(edge_db);
    drop(server_db);

    let reopened_edge = Arc::new(Database::open(&edge_path).unwrap());
    let reopened_server = Arc::new(Database::open(&server_path).unwrap());
    let third_id = uuid::Uuid::new_v4();
    reopened_server
        .execute(
            "INSERT INTO t (id, v) VALUES ($id, $v)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(third_id)),
                ("v".to_string(), Value::Text("after_restart".to_string())),
            ]),
        )
        .unwrap();

    let restarted_server = Arc::new(SyncServer::new(
        reopened_server.clone(),
        &nats.nats_url,
        "hc_t02",
        policies.clone(),
    ));
    let restarted_handle = tokio::spawn({
        let server = restarted_server.clone();
        async move { server.run().await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let restarted_client = SyncClient::new(reopened_edge.clone(), &nats.nats_url, "hc_t02");
    let delta_pull = restarted_client.pull(&policies).await.unwrap();
    assert_eq!(
        delta_pull.applied_rows, 1,
        "fresh client after restart should receive only the post-restart delta"
    );
    assert_eq!(
        delta_pull.skipped_rows, 0,
        "restart-safe incremental pull should not re-deliver already-applied rows"
    );
    assert_eq!(
        reopened_edge
            .scan("t", reopened_edge.snapshot())
            .unwrap()
            .len(),
        3
    );

    restarted_handle.abort();
    let _ = restarted_handle.await;
    drop(restarted_server);
}

#[test]
fn hc_t04_bfs_mvcc() {
    let db = setup_ontology_db();
    let a = uuid::Uuid::new_v4();
    let b = uuid::Uuid::new_v4();
    let tx = db.begin();
    db.insert_edge(tx, a, b, "R".to_string(), std::collections::HashMap::new())
        .unwrap();
    db.commit(tx).unwrap();
    let result = db
        .query_bfs(
            a,
            None,
            contextdb_core::Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .unwrap();
    assert_eq!(result.nodes.len(), 1);
}
