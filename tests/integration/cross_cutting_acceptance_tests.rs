use super::helpers::*;
use contextdb_core::Value;
use contextdb_engine::Database;
use contextdb_server::identity::FabricIdentity;
use contextdb_server::{InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tempfile::TempDir;

async fn wait_for_sync_server(fabric: &InProcessBroker, tenant: &str) {
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fabric.wait_for_registered_route_for_test(&contextdb_server::subjects::status_subject(
            tenant,
        )),
    )
    .await
    .expect("sync server must register its status route");
}

#[test]
#[ignore = "requires ARM64 cross compile"]
fn ignored_arm64() {}

#[test]
fn oom_does_not_partially_commit_visible_state() {
    // Keep the reopen budget fixed. The durable row workload must exceed
    // that ceiling under its real write owner, independently of cache residency.
    const LIMIT: usize = 80 * 1024;
    let payload = "x".repeat(LIMIT * 2);
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("t01.db");
    let db = Database::open(&path).unwrap();
    db.execute("SET MEMORY_LIMIT '80K'", &HashMap::new())
        .unwrap();
    db.execute(
        "CREATE TABLE big (id UUID PRIMARY KEY, payload TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    db.close().unwrap();

    let reopened = Database::open(&path).unwrap();
    let before = reopened.writer_memory_ownership_receipt_for_test();
    let original_lsn = reopened.current_lsn();
    let err = reopened
        .execute(
            "INSERT INTO big (id, payload) VALUES ($id, $payload)",
            &make_params(vec![
                ("id", Value::Uuid(uuid::Uuid::new_v4())),
                ("payload", Value::Text(payload.clone())),
            ]),
        )
        .unwrap_err();
    let attributed = |error: &contextdb_core::Error| {
        matches!(error,
        contextdb_core::Error::MemoryBudgetExceeded { subsystem, operation, requested_bytes,
            available_bytes, budget_limit_bytes, .. }
        if subsystem == "insert" && operation == "row_insert"
            && *requested_bytes == 164942
            && *available_bytes == LIMIT - before.named_owner_total
            && *available_bytes < *requested_bytes
            && *budget_limit_bytes == LIMIT)
    };
    assert!(attributed(&err), "{err:?}");
    if let contextdb_core::Error::MemoryBudgetExceeded {
        operation,
        requested_bytes,
        available_bytes,
        budget_limit_bytes,
        hint,
        ..
    } = &err
    {
        let wrong_owner = contextdb_core::Error::MemoryBudgetExceeded {
            subsystem: "vector".to_owned().into(),
            operation: operation.clone(),
            requested_bytes: *requested_bytes,
            available_bytes: *available_bytes,
            budget_limit_bytes: *budget_limit_bytes,
            hint: hint.clone(),
        };
        assert!(!attributed(&wrong_owner));
    }
    assert_eq!(reopened.current_lsn(), original_lsn);
    assert_eq!(reopened.writer_memory_ownership_receipt_for_test(), before);
    eprintln!("reopened row refusal={err:?} before={before:?}");
    assert_eq!(
        reopened.scan("big", reopened.snapshot()).unwrap().len(),
        0,
        "OOM failure after reopen must not leave a partially visible row"
    );
    reopened
        .execute(
            "INSERT INTO big VALUES ('22222222-2222-2222-2222-222222222222', 'fitting')",
            &HashMap::new(),
        )
        .unwrap();
    reopened.close().unwrap();
    let recovered = Database::open(&path).unwrap();
    assert_eq!(
        recovered
            .execute("SELECT payload FROM big", &HashMap::new())
            .unwrap()
            .rows,
        vec![vec![Value::Text("fitting".to_owned())]]
    );
    recovered.close().unwrap();
}

#[tokio::test]
async fn restart_preserves_committed_sync_visibility() {
    let fabric = InProcessBroker::new();
    let edge_tmp = TempDir::new().unwrap();
    let server_tmp = TempDir::new().unwrap();
    let edge_path = edge_tmp.path().join("edge.db");
    let server_path = server_tmp.path().join("server.db");

    let edge_db = Arc::new(Database::open(&edge_path).unwrap());
    let server_db = Arc::new(Database::open(&server_path).unwrap());
    let empty = HashMap::new();
    server_db
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT) SYNC CONFLICT KEEP FIRST",
            &empty,
        )
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

    let hub_identity = Arc::new(FabricIdentity::generate());
    let hub_node_id = hub_identity.node_id();
    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node_id = edge_identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            server_db.clone(),
            fabric.server_as(&hub_node_id),
            contextdb_core::TenantId::from("t02"),
            hub_node_id.clone(),
            hub_identity.clone(),
        )
        .expect("construct the authenticated sync endpoint"),
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let handle = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    wait_for_sync_server(&fabric, "t02").await;

    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge_db.clone(),
        fabric.client_as(&edge_node_id),
        contextdb_core::TenantId::from("t02"),
        edge_identity.clone(),
    )
    .expect("construct the authenticated sync endpoint");
    let initial_pull = client.pull_default().await.unwrap();
    assert_eq!(initial_pull.applied_rows, 2);
    assert_eq!(initial_pull.skipped_rows, 0);

    shutdown.store(true, Ordering::SeqCst);
    handle.await.unwrap();
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

    let restarted_server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            reopened_server.clone(),
            fabric.server_as(&hub_node_id),
            contextdb_core::TenantId::from("t02"),
            hub_node_id,
            hub_identity,
        )
        .expect("construct the authenticated sync endpoint"),
    );
    let restarted_handle = tokio::spawn({
        let server = restarted_server.clone();
        async move { server.run().await }
    });
    wait_for_sync_server(&fabric, "t02").await;

    let restarted_client = SyncClient::with_authenticated_transport_and_identity_for_test(
        reopened_edge.clone(),
        fabric.client_as(&edge_node_id),
        contextdb_core::TenantId::from("t02"),
        edge_identity,
    )
    .expect("construct the authenticated sync endpoint");
    let delta_pull = restarted_client.pull_default().await.unwrap();
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
fn bfs_mvcc() {
    let db = setup_ontology_db();
    let a = uuid::Uuid::new_v4();
    let b = uuid::Uuid::new_v4();
    let tx = db.begin_or_panic();
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
