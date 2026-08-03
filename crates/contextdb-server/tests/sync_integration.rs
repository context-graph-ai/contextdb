use contextdb_core::{Lsn, RowId};
use contextdb_engine::Database;
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

struct RunningServer {
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

async fn start_server(db: Arc<Database>, fabric: &InProcessBroker, tenant: &str) -> RunningServer {
    let identity = Arc::new(FabricIdentity::generate());
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
    let task = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fabric.wait_for_registered_route_for_test(&contextdb_server::subjects::status_subject(
            tenant,
        )),
    )
    .await
    .expect("sync server must register its status route");
    RunningServer { shutdown, task }
}

fn sync_client(db: Arc<Database>, fabric: &InProcessBroker, tenant: &str) -> SyncClient {
    let identity = Arc::new(FabricIdentity::generate());
    let node_id = identity.node_id();
    SyncClient::with_authenticated_transport_and_identity_for_test(
        db,
        fabric.client_as(&node_id),
        contextdb_core::TenantId::from(tenant),
        identity,
    )
}

async fn stop_server(server: RunningServer) {
    server.shutdown.store(true, Ordering::SeqCst);
    server.task.await.expect("sync server task");
}

#[tokio::test]
async fn sync_round_trip_smoke() {
    let fabric = InProcessBroker::new();
    let edge = Arc::new(Database::open_memory());
    let server_db = Arc::new(Database::open_memory());
    let server = start_server(server_db, &fabric, "test_tenant").await;

    let client = sync_client(edge, &fabric, "test_tenant");
    let _ = client.pull_default().await;
    stop_server(server).await;
}

#[tokio::test]
async fn sync_00b_push_retries_malformed_reply_before_succeeding() {
    use contextdb_core::Value;
    use contextdb_server::protocol::{
        MessageType, PushRequest, PushResponse, WireApplyResult, decode, encode,
    };
    use contextdb_server::transport::{HandlerRegistration, IncomingRequest};
    use std::sync::atomic::AtomicUsize;
    use uuid::Uuid;

    let fabric = InProcessBroker::new();
    let edge = Arc::new(Database::open_memory());
    let empty = HashMap::new();
    edge.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT) SYNC CONFLICT KEEP FIRST",
        &empty,
    )
    .unwrap();

    let attempts = Arc::new(AtomicUsize::new(0));
    let handler = Arc::new(move |request: IncomingRequest| {
        let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
        Box::pin(async move {
            let payload = if attempt == 1 {
                vec![0x91]
            } else {
                let envelope = decode(&request.bytes)
                    .expect("retry request must be a decodable push envelope");
                assert!(
                    matches!(
                        envelope.message_type,
                        MessageType::PushRequest | MessageType::DependencyCompletePushRequest
                    ),
                    "retry must remain on an ordinary or dependency-complete push route"
                );
                let request: PushRequest = rmp_serde::from_slice(&envelope.payload)
                    .expect("retry envelope must carry a PushRequest payload");
                encode(
                    MessageType::PushResponse,
                    &PushResponse {
                        result: Some(WireApplyResult {
                            // Schema migrations carry zero rows; the data
                            // request carries one. Echo the actual request
                            // so this proves retry behavior rather than a
                            // fabricated apply result.
                            applied_rows: request.changeset.rows.len(),
                            skipped_rows: 0,
                            conflicts: Vec::new(),
                            new_lsn: Lsn(2),
                        }),
                        error: None,
                        application_error: None,
                    },
                )
                .unwrap()
            };
            (request.responder)(payload).await
        }) as contextdb_server::transport::TransportFuture<'static, ()>
    });
    let shutdown = Arc::new(AtomicBool::new(false));
    let transport = fabric.server_as("malformed-reply-hub");
    let subject = contextdb_server::subjects::push_subject("malformed-reply");
    let registered_subject = subject.clone();
    let task = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            transport
                .serve(vec![HandlerRegistration { subject, handler }], shutdown)
                .await
                .expect("malformed-reply responder")
        }
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fabric.wait_for_registered_route_for_test(&registered_subject),
    )
    .await
    .expect("malformed-reply route");

    let client = sync_client(edge.clone(), &fabric, "malformed-reply");
    let id = Uuid::new_v4();
    let mut p = HashMap::new();
    p.insert("id".to_string(), Value::Uuid(id));
    p.insert("v".to_string(), Value::Text("retry".into()));
    edge.execute("INSERT INTO t (id, v) VALUES ($id, $v)", &p)
        .unwrap();

    let result = client
        .push()
        .await
        .expect("push should retry malformed reply");
    assert_eq!(result.applied_rows, 1);
    assert!(
        client.push_watermark() > Lsn(0),
        "push watermark should advance"
    );
    shutdown.store(true, Ordering::SeqCst);
    task.await.expect("malformed-reply responder task");
}

// A1: Lazy connection and reuse
#[tokio::test]
async fn a1_lazy_connection_and_reuse() {
    use contextdb_core::Value;
    use uuid::Uuid;

    let fabric = InProcessBroker::new();
    let edge_db = Arc::new(Database::open_memory());
    let server_db = Arc::new(Database::open_memory());

    // Create table on both databases
    let empty = HashMap::new();
    edge_db
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT) SYNC CONFLICT KEEP FIRST",
            &empty,
        )
        .unwrap();
    server_db
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT) SYNC CONFLICT KEEP FIRST",
            &empty,
        )
        .unwrap();

    let server = start_server(server_db.clone(), &fabric, "reuse-test").await;

    let client = sync_client(edge_db.clone(), &fabric, "reuse-test");

    // The in-process authenticated transport is connectionless: no request
    // or reply exists before the first sync operation.
    assert!(
        fabric.recorded_exchanges().is_empty(),
        "no request/reply exchange may happen before the first sync operation"
    );

    // The initial schema push must use the registered authenticated route.
    assert!(
        client.push().await.is_ok(),
        "initial push must succeed over the registered authenticated route"
    );
    let exchanges_after_schema = fabric.recorded_exchanges().len();
    assert!(
        exchanges_after_schema > 0,
        "schema push must use request/reply exchanges"
    );

    // Insert data and push again — reuses stored connection
    let id = Uuid::new_v4();
    let mut params = HashMap::new();
    params.insert("id".to_string(), Value::Uuid(id));
    params.insert("v".to_string(), Value::Text("hello".into()));
    edge_db
        .execute("INSERT INTO t (id, v) VALUES ($id, $v)", &params)
        .unwrap();

    let result = client.push().await.unwrap();
    assert!(
        result.applied_rows > 0,
        "data must be delivered through the same available route"
    );
    assert!(
        fabric.recorded_exchanges().len() > exchanges_after_schema,
        "the second push must use that available route for new data"
    );

    // Verify server has the row
    let server_row = server_db
        .point_lookup("t", "id", &Value::Uuid(id), server_db.snapshot())
        .unwrap();
    assert!(
        server_row.is_some(),
        "server must have the row pushed by edge"
    );
    stop_server(server).await;
}

// A2: Connection failure produces actionable error
#[tokio::test]
async fn a2_connection_failure_actionable_error() {
    use contextdb_core::Value;
    use uuid::Uuid;

    let db = Arc::new(Database::open_memory());
    let empty = HashMap::new();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT) SYNC CONFLICT KEEP FIRST",
        &empty,
    )
    .unwrap();

    // Insert a row so changeset is non-empty
    let id = Uuid::new_v4();
    let mut params = HashMap::new();
    params.insert("id".to_string(), Value::Uuid(id));
    params.insert("v".to_string(), Value::Text("data".into()));
    db.execute("INSERT INTO t (id, v) VALUES ($id, $v)", &params)
        .unwrap();

    let fabric = InProcessBroker::new();
    // Publish an authenticated hub identity without serving any route. This
    // separates identity binding from the real no-responder route failure.
    let _unavailable_hub = fabric.server_as("known-unavailable-hub");
    let client = sync_client(db, &fabric, "no-server-registered");
    let result = client.push().await;

    assert!(
        result.is_err(),
        "push to an endpoint with no server must fail"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("no responder") || err_msg.contains("unreachable"),
        "error must identify the unavailable sync route, got: {}",
        err_msg
    );
}

// A3: pull_default() honors the table's durable conflict declaration
#[tokio::test]
async fn a3_pull_default_honors_declared_keep_first() {
    use contextdb_core::Value;
    use uuid::Uuid;

    let fabric = InProcessBroker::new();
    let server_db = Arc::new(Database::open_memory());
    let edge_db = Arc::new(Database::open_memory());
    let empty = HashMap::new();

    // Create table on both
    server_db
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT) SYNC CONFLICT KEEP FIRST",
            &empty,
        )
        .unwrap();
    edge_db
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT) SYNC CONFLICT KEEP FIRST",
            &empty,
        )
        .unwrap();

    // Same PK, different values — conflict
    let id = Uuid::new_v4();
    let mut server_params = HashMap::new();
    server_params.insert("id".to_string(), Value::Uuid(id));
    server_params.insert("v".to_string(), Value::Text("server-value".into()));
    server_db
        .execute("INSERT INTO t (id, v) VALUES ($id, $v)", &server_params)
        .unwrap();

    let mut edge_params = HashMap::new();
    edge_params.insert("id".to_string(), Value::Uuid(id));
    edge_params.insert("v".to_string(), Value::Text("edge-value".into()));
    edge_db
        .execute("INSERT INTO t (id, v) VALUES ($id, $v)", &edge_params)
        .unwrap();

    let server = start_server(server_db.clone(), &fabric, "pull-default-test").await;

    let client = sync_client(edge_db.clone(), &fabric, "pull-default-test");

    // The edge submits the later duplicate first. The hub's durable KEEP
    // FIRST declaration must refuse that loser and retain its original value;
    // a later pull then converges the edge to the hub winner.
    let refused = client
        .push()
        .await
        .expect("KEEP FIRST loser must receive a terminal hub response");
    assert!(
        !refused.conflicts.is_empty(),
        "the edge duplicate must be refused by the hub's durable KEEP FIRST declaration"
    );
    client.pull_default().await.unwrap();

    let row = edge_db
        .point_lookup("t", "id", &Value::Uuid(id), edge_db.snapshot())
        .unwrap()
        .expect("row must exist after pull");
    let v = row.values.get("v").expect("column v must exist");
    assert_eq!(
        v,
        &Value::Text("server-value".into()),
        "declared KEEP FIRST must converge the edge to the hub-accepted first value"
    );
    stop_server(server).await;
}

// A4: a durable SYNC OFF declaration blocks data on pull
#[tokio::test]
async fn a4_sync_off_declaration_blocks_pull() {
    use contextdb_core::Value;
    use uuid::Uuid;

    let fabric = InProcessBroker::new();
    let server_db = Arc::new(Database::open_memory());
    let edge_db = Arc::new(Database::open_memory());
    let empty = HashMap::new();

    // Create two tables on server
    server_db
        .execute(
            "CREATE TABLE synced (id UUID PRIMARY KEY, v TEXT) SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
            &empty,
        )
        .unwrap();
    server_db
        .execute(
            "CREATE TABLE blocked (id UUID PRIMARY KEY, v TEXT) SYNC OFF",
            &empty,
        )
        .unwrap();

    // Insert data in both tables on server
    let synced_id = Uuid::new_v4();
    let mut p = HashMap::new();
    p.insert("id".to_string(), Value::Uuid(synced_id));
    p.insert("v".to_string(), Value::Text("synced-data".into()));
    server_db
        .execute("INSERT INTO synced (id, v) VALUES ($id, $v)", &p)
        .unwrap();

    let blocked_id = Uuid::new_v4();
    let mut p2 = HashMap::new();
    p2.insert("id".to_string(), Value::Uuid(blocked_id));
    p2.insert("v".to_string(), Value::Text("blocked-data".into()));
    server_db
        .execute("INSERT INTO blocked (id, v) VALUES ($id, $v)", &p2)
        .unwrap();

    // Create tables on edge too
    edge_db
        .execute(
            "CREATE TABLE synced (id UUID PRIMARY KEY, v TEXT) SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
            &empty,
        )
        .unwrap();
    edge_db
        .execute(
            "CREATE TABLE blocked (id UUID PRIMARY KEY, v TEXT) SYNC OFF",
            &empty,
        )
        .unwrap();

    let server = start_server(server_db.clone(), &fabric, "direction-test").await;

    let client = sync_client(edge_db.clone(), &fabric, "direction-test");

    client.pull_default().await.unwrap();

    // "synced" row should appear on edge
    let synced_row = edge_db
        .point_lookup("synced", "id", &Value::Uuid(synced_id), edge_db.snapshot())
        .unwrap();
    assert!(
        synced_row.is_some(),
        "synced table row must appear on edge (default=Both)"
    );

    // "blocked" row should NOT appear on edge
    let blocked_rows = edge_db
        .scan_filter("blocked", edge_db.snapshot(), &|_| true)
        .unwrap();
    assert_eq!(
        blocked_rows.len(),
        0,
        "the edge's durable SYNC OFF declaration must keep the blocked table local"
    );
    stop_server(server).await;
}

// A6: reconnect preserves a routable authenticated endpoint; no socket state
// is part of the client contract.
#[tokio::test]
async fn a6_reconnect_clears_and_reestablishes() {
    use contextdb_core::Value;
    use uuid::Uuid;

    let fabric = InProcessBroker::new();
    let edge_db = Arc::new(Database::open_memory());
    let server_db = Arc::new(Database::open_memory());
    let empty = HashMap::new();

    edge_db
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT) SYNC CONFLICT KEEP FIRST",
            &empty,
        )
        .unwrap();
    server_db
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT) SYNC CONFLICT KEEP FIRST",
            &empty,
        )
        .unwrap();

    let server = start_server(server_db.clone(), &fabric, "reconnect-test").await;

    // Success path: both operations use the registered route, with reconnect
    // merely refreshing the client-side sync setup.
    let client = sync_client(edge_db.clone(), &fabric, "reconnect-test");
    assert!(
        client.push().await.is_ok(),
        "initial route exchange must succeed"
    );
    let exchanges_before_reconnect = fabric.recorded_exchanges().len();
    client.reconnect().await;
    assert!(
        client.push().await.is_ok(),
        "the registered route must remain usable after reconnect"
    );
    assert!(
        fabric.recorded_exchanges().len() > exchanges_before_reconnect,
        "the post-reconnect push must perform a real route exchange"
    );

    // Failure path: authentication knows the hub identity, but no tenant
    // route is registered, so the actual push reports NoResponder.
    let bad_db = Arc::new(Database::open_memory());
    bad_db
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT) SYNC CONFLICT KEEP FIRST",
            &empty,
        )
        .unwrap();
    let mut bad_row = HashMap::new();
    bad_row.insert("id".to_string(), Value::Uuid(Uuid::new_v4()));
    bad_row.insert("v".to_string(), Value::Text("unroutable".into()));
    bad_db
        .execute("INSERT INTO t (id, v) VALUES ($id, $v)", &bad_row)
        .unwrap();
    let _unavailable_hub = fabric.server_as("known-unavailable-reconnect-hub");
    let bad_client = sync_client(bad_db, &fabric, "bad-port");
    bad_client.reconnect().await;
    let no_responder = bad_client
        .push()
        .await
        .expect_err("an unserved tenant route must fail after reconnect");
    assert!(
        no_responder
            .to_string()
            .to_ascii_lowercase()
            .contains("no responder"),
        "the unavailable route must surface NoResponder, got {no_responder}"
    );
    stop_server(server).await;
}

// A7: each table's durable conflict declaration governs hub arbitration and convergence
#[tokio::test]
async fn a7_per_table_declared_conflict_policy_governs_push_and_convergence() {
    use contextdb_core::Value;
    use uuid::Uuid;

    let fabric = InProcessBroker::new();
    let server_db = Arc::new(Database::open_memory());
    let edge_db = Arc::new(Database::open_memory());
    let empty = HashMap::new();

    // Create two tables on both
    for db in [&server_db, &edge_db] {
        db.execute(
            "CREATE TABLE observations (id UUID PRIMARY KEY, v TEXT) SYNC CONFLICT KEEP FIRST",
            &empty,
        )
        .unwrap();
        db.execute(
            "CREATE TABLE decisions (id UUID PRIMARY KEY, v TEXT) SYNC CONFLICT KEEP LATEST",
            &empty,
        )
        .unwrap();
    }

    // Same PKs, different values — conflicts on both tables
    let obs_id = Uuid::new_v4();
    let dec_id = Uuid::new_v4();

    let mut p = HashMap::new();
    p.insert("id".to_string(), Value::Uuid(obs_id));
    p.insert("v".to_string(), Value::Text("server-obs".into()));
    server_db
        .execute("INSERT INTO observations (id, v) VALUES ($id, $v)", &p)
        .unwrap();

    let mut p = HashMap::new();
    p.insert("id".to_string(), Value::Uuid(dec_id));
    p.insert("v".to_string(), Value::Text("server-dec".into()));
    server_db
        .execute("INSERT INTO decisions (id, v) VALUES ($id, $v)", &p)
        .unwrap();

    let mut p = HashMap::new();
    p.insert("id".to_string(), Value::Uuid(obs_id));
    p.insert("v".to_string(), Value::Text("edge-obs".into()));
    edge_db
        .execute("INSERT INTO observations (id, v) VALUES ($id, $v)", &p)
        .unwrap();

    let mut p = HashMap::new();
    p.insert("id".to_string(), Value::Uuid(dec_id));
    p.insert("v".to_string(), Value::Text("edge-dec".into()));
    edge_db
        .execute("INSERT INTO decisions (id, v) VALUES ($id, $v)", &p)
        .unwrap();

    let server = start_server(server_db.clone(), &fabric, "policy-override-test").await;

    let client = sync_client(edge_db.clone(), &fabric, "policy-override-test");

    let push = client.push().await.unwrap();
    assert_eq!(
        push.applied_rows, 1,
        "declared KEEP LATEST must accept the conflicting edge decision at the hub"
    );
    assert_eq!(
        push.skipped_rows, 1,
        "declared KEEP FIRST must refuse the conflicting edge observation at the hub"
    );
    assert_eq!(
        push.conflicts.len(),
        1,
        "the refused KEEP FIRST observation must be reported as one conflict"
    );

    let hub_obs = server_db
        .point_lookup(
            "observations",
            "id",
            &Value::Uuid(obs_id),
            server_db.snapshot(),
        )
        .unwrap()
        .expect("hub observation must exist after push");
    assert_eq!(
        hub_obs.values.get("v"),
        Some(&Value::Text("server-obs".into())),
        "declared KEEP FIRST must retain the hub's first observation"
    );
    let hub_dec = server_db
        .point_lookup(
            "decisions",
            "id",
            &Value::Uuid(dec_id),
            server_db.snapshot(),
        )
        .unwrap()
        .expect("hub decision must exist after push");
    assert_eq!(
        hub_dec.values.get("v"),
        Some(&Value::Text("edge-dec".into())),
        "declared KEEP LATEST must accept the edge decision at the hub"
    );

    client.pull_default().await.unwrap();

    let edge_obs = edge_db
        .point_lookup(
            "observations",
            "id",
            &Value::Uuid(obs_id),
            edge_db.snapshot(),
        )
        .unwrap()
        .expect("edge observation must exist after convergence pull");
    assert_eq!(
        edge_obs.values.get("v"),
        Some(&Value::Text("server-obs".into())),
        "pull must converge the edge to the hub-accepted KEEP FIRST observation"
    );
    let edge_dec = edge_db
        .point_lookup("decisions", "id", &Value::Uuid(dec_id), edge_db.snapshot())
        .unwrap()
        .expect("edge decision must exist after convergence pull");
    assert_eq!(
        edge_dec.values.get("v"),
        Some(&Value::Text("edge-dec".into())),
        "pull must converge the edge to the hub-accepted KEEP LATEST decision"
    );
    stop_server(server).await;
}

// A8: Pull watermark advances after successful pull
#[tokio::test]
async fn a8_pull_watermark_advances() {
    use contextdb_core::Value;
    use uuid::Uuid;

    let fabric = InProcessBroker::new();
    let server_db = Arc::new(Database::open_memory());
    let edge_db = Arc::new(Database::open_memory());
    let empty = HashMap::new();

    server_db
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT) SYNC CONFLICT KEEP FIRST",
            &empty,
        )
        .unwrap();
    edge_db
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT) SYNC CONFLICT KEEP FIRST",
            &empty,
        )
        .unwrap();

    // Insert 5 rows on server
    for i in 0..5 {
        let id = Uuid::new_v4();
        let mut p = HashMap::new();
        p.insert("id".to_string(), Value::Uuid(id));
        p.insert("v".to_string(), Value::Text(format!("row_{}", i)));
        server_db
            .execute("INSERT INTO t (id, v) VALUES ($id, $v)", &p)
            .unwrap();
    }

    let server = start_server(server_db.clone(), &fabric, "pull-wm-test").await;

    let client = sync_client(edge_db.clone(), &fabric, "pull-wm-test");

    // First pull — gets 5 rows
    let result1 = client.pull_default().await.unwrap();
    assert_eq!(result1.applied_rows, 5, "first pull must apply 5 rows");
    assert_eq!(result1.skipped_rows, 0, "first pull must skip 0 rows");
    assert!(
        client.pull_watermark() > Lsn(0),
        "pull watermark must advance after first pull"
    );
    let prev_watermark = client.pull_watermark();

    // Insert 1 more row on server
    let id = Uuid::new_v4();
    let mut p = HashMap::new();
    p.insert("id".to_string(), Value::Uuid(id));
    p.insert("v".to_string(), Value::Text("new-row".into()));
    server_db
        .execute("INSERT INTO t (id, v) VALUES ($id, $v)", &p)
        .unwrap();

    // Second pull — should only get 1 new row
    let result2 = client.pull_default().await.unwrap();
    assert_eq!(result2.applied_rows, 1, "second pull must apply 1 row");
    assert_eq!(
        result2.skipped_rows, 0,
        "second pull must skip 0 rows — if >0, since_lsn is hardcoded to 0"
    );
    assert!(
        client.pull_watermark() > prev_watermark,
        "pull watermark must advance after second pull"
    );
    stop_server(server).await;
}

// A9: RowDelete events are synced
#[tokio::test]
async fn a9_row_delete_events_synced() {
    use contextdb_core::Value;
    use uuid::Uuid;

    let fabric = InProcessBroker::new();
    let server_db = Arc::new(Database::open_memory());
    let edge_db = Arc::new(Database::open_memory());
    let empty = HashMap::new();

    edge_db
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT) SYNC CONFLICT KEEP LATEST",
            &empty,
        )
        .unwrap();
    server_db
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT) SYNC CONFLICT KEEP LATEST",
            &empty,
        )
        .unwrap();

    let server = start_server(server_db.clone(), &fabric, "rowdelete-test").await;

    let client = sync_client(edge_db.clone(), &fabric, "rowdelete-test");

    // Insert row on edge and push to server
    let id = Uuid::new_v4();
    let mut p = HashMap::new();
    p.insert("id".to_string(), Value::Uuid(id));
    p.insert("v".to_string(), Value::Text("exists".into()));
    edge_db
        .execute("INSERT INTO t (id, v) VALUES ($id, $v)", &p)
        .unwrap();
    client.push().await.unwrap();

    // Verify server has the row
    let server_row = server_db
        .point_lookup("t", "id", &Value::Uuid(id), server_db.snapshot())
        .unwrap();
    assert!(
        server_row.is_some(),
        "server must have the row after initial push"
    );

    // Delete on edge
    let mut dp = HashMap::new();
    dp.insert("id".to_string(), Value::Uuid(id));
    edge_db
        .execute("DELETE FROM t WHERE id = $id", &dp)
        .unwrap();

    // Push the delete
    client.push().await.unwrap();

    // Server must reflect the delete
    let server_row_after = server_db
        .point_lookup("t", "id", &Value::Uuid(id), server_db.snapshot())
        .unwrap();
    assert!(
        server_row_after.is_none(),
        "server must NOT have the row after delete push. If still present, RowDelete is not emitted by changes_since()"
    );
    stop_server(server).await;
}

#[tokio::test]
async fn a9_file_backed_row_delete_events_synced() {
    use contextdb_core::Value;
    use tempfile::TempDir;
    use uuid::Uuid;

    let tmp = TempDir::new().unwrap();
    let server_path = tmp.path().join("server.db");
    let edge_path = tmp.path().join("edge.db");
    let fabric = InProcessBroker::new();
    let server_db = Arc::new(Database::open(&server_path).unwrap());
    let edge_db = Arc::new(Database::open(&edge_path).unwrap());
    let empty = HashMap::new();

    edge_db
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT) SYNC CONFLICT KEEP LATEST",
            &empty,
        )
        .unwrap();
    server_db
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT) SYNC CONFLICT KEEP LATEST",
            &empty,
        )
        .unwrap();

    let server = start_server(server_db.clone(), &fabric, "rowdelete-file-test").await;

    let client = sync_client(edge_db.clone(), &fabric, "rowdelete-file-test");

    let id = Uuid::new_v4();
    let mut p = HashMap::new();
    p.insert("id".to_string(), Value::Uuid(id));
    p.insert("v".to_string(), Value::Text("exists".into()));
    edge_db
        .execute("INSERT INTO t (id, v) VALUES ($id, $v)", &p)
        .unwrap();
    client.push().await.unwrap();

    let mut dp = HashMap::new();
    dp.insert("id".to_string(), Value::Uuid(id));
    edge_db
        .execute("DELETE FROM t WHERE id = $id", &dp)
        .unwrap();
    client.push().await.unwrap();

    let server_row_after = server_db
        .point_lookup("t", "id", &Value::Uuid(id), server_db.snapshot())
        .unwrap();
    assert!(
        server_row_after.is_none(),
        "server must NOT have the row after file-backed delete push"
    );
    stop_server(server).await;
}

// Fresh pull after insert+delete history must converge to the net state without conflicts.
#[tokio::test]
async fn a9_fresh_pull_after_delete_history_converges_without_conflict() {
    use contextdb_core::Value;
    use uuid::Uuid;

    let fabric = InProcessBroker::new();
    let server_db = Arc::new(Database::open_memory());
    let edge_a_db = Arc::new(Database::open_memory());
    let edge_b_db = Arc::new(Database::open_memory());
    let empty = HashMap::new();

    for db in [&server_db, &edge_a_db, &edge_b_db] {
        db.execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT) SYNC CONFLICT KEEP LATEST",
            &empty,
        )
        .unwrap();
    }

    let server = start_server(server_db.clone(), &fabric, "fresh-delete-history").await;

    let edge_a = sync_client(edge_a_db.clone(), &fabric, "fresh-delete-history");
    let edge_b = sync_client(edge_b_db.clone(), &fabric, "fresh-delete-history");

    let keep_id = Uuid::new_v4();
    let delete_id = Uuid::new_v4();
    for (id, value) in [(keep_id, "keep"), (delete_id, "delete-me")] {
        let mut p = HashMap::new();
        p.insert("id".to_string(), Value::Uuid(id));
        p.insert("v".to_string(), Value::Text(value.into()));
        edge_a_db
            .execute("INSERT INTO t (id, v) VALUES ($id, $v)", &p)
            .unwrap();
    }
    edge_a.push().await.unwrap();

    let mut delete_params = HashMap::new();
    delete_params.insert("id".to_string(), Value::Uuid(delete_id));
    edge_a_db
        .execute("DELETE FROM t WHERE id = $id", &delete_params)
        .unwrap();
    edge_a.push().await.unwrap();

    let pull = edge_b.pull_default().await.unwrap();
    assert!(
        pull.conflicts.is_empty(),
        "fresh pull over insert+delete history must not report conflicts: {:?}",
        pull.conflicts
    );

    let rows = edge_b_db.scan("t", edge_b_db.snapshot()).unwrap();
    assert_eq!(rows.len(), 1, "fresh pull must converge to net row count");
    assert_eq!(
        rows[0].values.get("id"),
        Some(&Value::Uuid(keep_id)),
        "deleted row must not remain after fresh pull"
    );
    stop_server(server).await;
}

#[tokio::test]
async fn a9_file_backed_fresh_pull_after_delete_history_converges_without_conflict() {
    use contextdb_core::Value;
    use tempfile::TempDir;
    use uuid::Uuid;

    let tmp = TempDir::new().unwrap();
    let server_path = tmp.path().join("server.db");
    let edge_a_path = tmp.path().join("edge-a.db");
    let edge_b_path = tmp.path().join("edge-b.db");
    let fabric = InProcessBroker::new();
    let server_db = Arc::new(Database::open(&server_path).unwrap());
    let edge_a_db = Arc::new(Database::open(&edge_a_path).unwrap());
    let edge_b_db = Arc::new(Database::open(&edge_b_path).unwrap());
    let empty = HashMap::new();

    for db in [&server_db, &edge_a_db, &edge_b_db] {
        db.execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT) SYNC CONFLICT KEEP LATEST",
            &empty,
        )
        .unwrap();
    }

    let server = start_server(server_db.clone(), &fabric, "file-fresh-delete-history").await;

    let edge_a = sync_client(edge_a_db.clone(), &fabric, "file-fresh-delete-history");
    let edge_b = sync_client(edge_b_db.clone(), &fabric, "file-fresh-delete-history");

    let keep_id = Uuid::new_v4();
    let delete_id = Uuid::new_v4();
    for (id, value) in [(keep_id, "keep"), (delete_id, "delete-me")] {
        let mut p = HashMap::new();
        p.insert("id".to_string(), Value::Uuid(id));
        p.insert("v".to_string(), Value::Text(value.into()));
        edge_a_db
            .execute("INSERT INTO t (id, v) VALUES ($id, $v)", &p)
            .unwrap();
    }
    edge_a.push().await.unwrap();

    let mut delete_params = HashMap::new();
    delete_params.insert("id".to_string(), Value::Uuid(delete_id));
    edge_a_db
        .execute("DELETE FROM t WHERE id = $id", &delete_params)
        .unwrap();
    edge_a.push().await.unwrap();

    let pull = edge_b.pull_default().await.unwrap();
    assert!(
        pull.conflicts.is_empty(),
        "file-backed fresh pull over insert+delete history must not report conflicts: {:?}",
        pull.conflicts
    );

    let rows = edge_b_db.scan("t", edge_b_db.snapshot()).unwrap();
    assert_eq!(rows.len(), 1, "fresh pull must converge to net row count");
    assert_eq!(
        rows[0].values.get("id"),
        Some(&Value::Uuid(keep_id)),
        "deleted row must not remain after fresh pull"
    );
    stop_server(server).await;
}

// A10: Vector mapping survives failed row inserts (exact test code from plan)
#[tokio::test]
async fn a10_vector_mapping_survives_failed_inserts() {
    use contextdb_core::Value;
    use contextdb_engine::sync_types::*;
    use std::collections::HashMap;
    use uuid::Uuid;

    let server_db = Arc::new(Database::open_memory());

    // Create STATE MACHINE table: draft -> [active], active -> [done]
    let empty = HashMap::new();
    server_db
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, status TEXT, embedding VECTOR(3)) \
         STATE MACHINE (status: draft -> [active], active -> [done])",
            &empty,
        )
        .unwrap();

    // Pre-insert row B on server with status='active'
    let uuid_b = Uuid::new_v4();
    let mut params_b = HashMap::new();
    params_b.insert("id".to_string(), Value::Uuid(uuid_b));
    params_b.insert("status".to_string(), Value::Text("active".into()));
    server_db
        .execute(
            "INSERT INTO t (id, status) VALUES ($id, $status)",
            &params_b,
        )
        .unwrap();

    // Build ChangeSet manually: 3 rows + 3 vectors
    let uuid_a = Uuid::new_v4();
    let uuid_c = Uuid::new_v4();

    let edge_row_a: u64 = u64::MAX - 2;
    let edge_row_b: u64 = u64::MAX - 1;
    let edge_row_c: u64 = u64::MAX;

    let changeset = ChangeSet {
        rows: vec![
            RowChange {
                table: "t".to_string(),
                natural_key: NaturalKey::single("id".to_string(), Value::Uuid(uuid_a)),
                values: {
                    let mut v = HashMap::new();
                    v.insert("id".to_string(), Value::Uuid(uuid_a));
                    v.insert("status".to_string(), Value::Text("draft".into()));
                    v.insert("embedding".to_string(), Value::Vector(vec![1.0, 0.0, 0.0]));
                    v
                },
                deleted: false,
                lsn: Lsn(10),
                created_at: None,
            },
            RowChange {
                table: "t".to_string(),
                natural_key: NaturalKey::single("id".to_string(), Value::Uuid(uuid_b)),
                values: {
                    let mut v = HashMap::new();
                    v.insert("id".to_string(), Value::Uuid(uuid_b));
                    // INVALID: server has status='active', transitioning to 'draft' is not allowed
                    v.insert("status".to_string(), Value::Text("draft".into()));
                    v.insert("embedding".to_string(), Value::Vector(vec![0.0, 1.0, 0.0]));
                    v
                },
                deleted: false,
                lsn: Lsn(11),
                created_at: None,
            },
            RowChange {
                table: "t".to_string(),
                natural_key: NaturalKey::single("id".to_string(), Value::Uuid(uuid_c)),
                values: {
                    let mut v = HashMap::new();
                    v.insert("id".to_string(), Value::Uuid(uuid_c));
                    v.insert("status".to_string(), Value::Text("draft".into()));
                    v.insert("embedding".to_string(), Value::Vector(vec![0.0, 0.0, 1.0]));
                    v
                },
                deleted: false,
                lsn: Lsn(12),
                created_at: None,
            },
        ],
        edges: Vec::new(),
        vectors: vec![
            VectorChange {
                index: contextdb_core::VectorIndexRef::new("t", "embedding"),
                row_id: RowId(edge_row_a),
                vector: vec![1.0, 0.0, 0.0],
                lsn: Lsn(10),
            },
            VectorChange {
                index: contextdb_core::VectorIndexRef::new("t", "embedding"),
                row_id: RowId(edge_row_b),
                vector: vec![0.0, 1.0, 0.0],
                lsn: Lsn(11),
            },
            VectorChange {
                index: contextdb_core::VectorIndexRef::new("t", "embedding"),
                row_id: RowId(edge_row_c),
                vector: vec![0.0, 0.0, 1.0],
                lsn: Lsn(12),
            },
        ],
        ddl: Vec::new(),

        ddl_lsn: Vec::new(),
    };

    // EdgeWins forces upsert attempt on row B — which fails due to state machine
    let policies = ConflictPolicies {
        per_table: HashMap::new(),
        default: ConflictPolicy::EdgeWins,
    };
    let result = server_db.apply_changes(changeset, &policies).unwrap();

    // Row A and C applied, row B failed (state machine violation)
    assert_eq!(result.applied_rows, 2, "rows A and C should apply");
    assert_eq!(
        result.skipped_rows, 1,
        "row B should fail (invalid state transition)"
    );
    assert_eq!(result.conflicts.len(), 1, "one conflict from row B");

    // Verify row A's vector: search for [1.0, 0.0, 0.0] — must find with high similarity
    let search_a = server_db
        .query_vector(
            contextdb_core::VectorIndexRef::new("t", "embedding"),
            &[1.0, 0.0, 0.0],
            1,
            None,
            server_db.snapshot(),
        )
        .unwrap();
    assert_eq!(search_a.len(), 1, "row A's vector must be findable");
    assert!(
        search_a[0].1 > 0.99,
        "row A's vector must have near-perfect cosine similarity, got {}",
        search_a[0].1
    );

    // KEY ASSERTION: Verify row C's vector is [0.0, 0.0, 1.0], NOT [0.0, 1.0, 0.0]
    let search_c = server_db
        .query_vector(
            contextdb_core::VectorIndexRef::new("t", "embedding"),
            &[0.0, 0.0, 1.0],
            1,
            None,
            server_db.snapshot(),
        )
        .unwrap();
    assert_eq!(search_c.len(), 1, "row C's vector must be findable");
    assert!(
        search_c[0].1 > 0.99,
        "row C's vector must be [0.0, 0.0, 1.0] with near-perfect similarity, got {} \
         (if ~0.0, row C got row B's vector [0.0, 1.0, 0.0] due to vector_row_idx mismapping)",
        search_c[0].1
    );

    // Additional: verify [0.0, 1.0, 0.0] (row B's vector) is NOT attached to any row
    let search_b = server_db
        .query_vector(
            contextdb_core::VectorIndexRef::new("t", "embedding"),
            &[0.0, 1.0, 0.0],
            1,
            None,
            server_db.snapshot(),
        )
        .unwrap();
    if !search_b.is_empty() {
        assert!(
            search_b[0].1 < 0.5,
            "row B's vector [0.0, 1.0, 0.0] should NOT be attached to any row with high similarity, \
             got {} — vector mismapping bug: B's vector landed on row C",
            search_b[0].1
        );
    }
}

// A11: Tenant ID with dots or wildcards is rejected
#[tokio::test]
async fn a11_tenant_id_validation() {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    let db = Arc::new(Database::open_memory());
    let fabric = InProcessBroker::new();

    // These must panic
    let r = catch_unwind(AssertUnwindSafe(|| {
        SyncClient::with_authenticated_transport_for_test(
            db.clone(),
            fabric.client_as("tenant-validation-edge"),
            contextdb_core::TenantId::from("foo.bar"),
        )
    }));
    assert!(r.is_err(), "dot in tenant_id must panic");

    let r = catch_unwind(AssertUnwindSafe(|| {
        SyncClient::with_authenticated_transport_for_test(
            db.clone(),
            fabric.client_as("tenant-validation-edge"),
            contextdb_core::TenantId::from("foo*"),
        )
    }));
    assert!(r.is_err(), "wildcard in tenant_id must panic");

    let r = catch_unwind(AssertUnwindSafe(|| {
        SyncClient::with_authenticated_transport_for_test(
            db.clone(),
            fabric.client_as("tenant-validation-edge"),
            contextdb_core::TenantId::from("foo>"),
        )
    }));
    assert!(r.is_err(), "multi-level route wildcard must panic");

    let r = catch_unwind(AssertUnwindSafe(|| {
        SyncClient::with_authenticated_transport_for_test(
            db.clone(),
            fabric.client_as("tenant-validation-edge"),
            contextdb_core::TenantId::from(""),
        )
    }));
    assert!(r.is_err(), "empty tenant_id must panic");

    let r = catch_unwind(AssertUnwindSafe(|| {
        SyncClient::with_authenticated_transport_for_test(
            db.clone(),
            fabric.client_as("tenant-validation-edge"),
            contextdb_core::TenantId::from("foo bar"),
        )
    }));
    assert!(r.is_err(), "space in tenant_id must panic");

    // Same for SyncServer
    let r = catch_unwind(AssertUnwindSafe(|| {
        SyncServer::with_authenticated_transport_for_test(
            db.clone(),
            fabric.server_as("tenant-validation-hub"),
            contextdb_core::TenantId::from("foo.bar"),
        )
    }));
    assert!(r.is_err(), "SyncServer must also reject dots");

    // These must succeed (no panic)
    SyncClient::with_authenticated_transport_for_test(
        db.clone(),
        fabric.client_as("tenant-validation-edge"),
        contextdb_core::TenantId::from("valid-tenant"),
    );
    SyncClient::with_authenticated_transport_for_test(
        db.clone(),
        fabric.client_as("tenant-validation-edge"),
        contextdb_core::TenantId::from("tenant_123"),
    );
    SyncClient::with_authenticated_transport_for_test(
        db.clone(),
        fabric.client_as("tenant-validation-edge"),
        contextdb_core::TenantId::from("MyTenant"),
    );
}

// A13: Pull pagination fetches all pages (exact test code from plan)
#[tokio::test]
async fn a13_pull_pagination_fetches_all_pages() {
    use contextdb_core::Value;
    use std::collections::HashMap;
    use uuid::Uuid;

    let server_db = Arc::new(Database::open_memory());
    let edge_db = Arc::new(Database::open_memory());
    // Create table on server and insert 1500 rows
    let empty = HashMap::new();
    server_db
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, data TEXT) IMMUTABLE SYNC CONFLICT KEEP FIRST",
            &empty,
        )
        .unwrap();

    // Insert all 1500 rows in one user transaction. The pull pages may divide
    // their transfer, but the source history is one committed application
    // operation rather than a test-only synthetic changeset.
    let tx = server_db.begin().expect("begin bulk source transaction");
    for i in 0..1500 {
        let id = Uuid::new_v4();
        let mut values = HashMap::new();
        values.insert("id".to_string(), Value::Uuid(id));
        values.insert("data".to_string(), Value::Text(format!("row_{}", i)));
        server_db
            .execute_in_tx(tx, "INSERT INTO t (id, data) VALUES ($id, $data)", &values)
            .expect("stage bulk source row");
    }
    server_db
        .commit(tx)
        .expect("commit all bulk source rows together");

    let fabric = InProcessBroker::new();
    let server = start_server(server_db.clone(), &fabric, "pagination-test").await;

    let client = sync_client(edge_db.clone(), &fabric, "pagination-test");

    // Pull all data
    let result = client.pull_default().await.unwrap();

    // Verify the authenticated transport path was used.
    assert!(
        client.is_connected().await,
        "the authenticated client must connect before paginated pull"
    );

    // KEY ASSERTION: all 1500 rows must arrive, not just the first page
    assert_eq!(
        result.applied_rows, 1500,
        "all 1500 rows must arrive via pagination. Got {} — \
         if 500, the pagination loop is missing.",
        result.applied_rows
    );

    // Double-check: query edge_db directly
    let rows = edge_db
        .scan_filter("t", edge_db.snapshot(), &|_| true)
        .unwrap();
    assert_eq!(
        rows.len(),
        1500,
        "edge_db must have all 1500 rows after paginated pull"
    );

    assert_eq!(
        result.skipped_rows, 0,
        "no rows should be skipped on fresh edge"
    );
    stop_server(server).await;
}

// A15: Concurrent push and pull on same client
#[tokio::test]
async fn a15_concurrent_push_and_pull() {
    use contextdb_core::Value;
    use uuid::Uuid;

    let fabric = InProcessBroker::new();
    let server_db = Arc::new(Database::open_memory());
    let edge_db = Arc::new(Database::open_memory());
    let empty = HashMap::new();

    // Create table on both
    server_db
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT) SYNC CONFLICT KEEP FIRST",
            &empty,
        )
        .unwrap();
    edge_db
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT) SYNC CONFLICT KEEP FIRST",
            &empty,
        )
        .unwrap();

    // Insert data on server (for pull to fetch)
    let server_id = Uuid::new_v4();
    let mut p = HashMap::new();
    p.insert("id".to_string(), Value::Uuid(server_id));
    p.insert("v".to_string(), Value::Text("server-data".into()));
    server_db
        .execute("INSERT INTO t (id, v) VALUES ($id, $v)", &p)
        .unwrap();

    let server = start_server(server_db.clone(), &fabric, "concurrent-client-test").await;

    let client = sync_client(edge_db.clone(), &fabric, "concurrent-client-test");

    // Insert data on edge (for push to send)
    let edge_id = Uuid::new_v4();
    let mut p = HashMap::new();
    p.insert("id".to_string(), Value::Uuid(edge_id));
    p.insert("v".to_string(), Value::Text("edge-data".into()));
    edge_db
        .execute("INSERT INTO t (id, v) VALUES ($id, $v)", &p)
        .unwrap();

    // Run push and pull concurrently
    let (push_r, pull_r) = tokio::join!(client.push(), client.pull_default());

    assert!(push_r.is_ok(), "concurrent push must succeed");
    assert!(pull_r.is_ok(), "concurrent pull must succeed");
    assert!(
        client.push_watermark() > Lsn(0),
        "push watermark must be non-zero after concurrent ops"
    );
    assert!(
        client.pull_watermark() > Lsn(0),
        "pull watermark must be non-zero after concurrent ops"
    );
    stop_server(server).await;
}

// ======== T14 ========
#[test]
fn sync_apply_accepts_peer_txid_beyond_local_watermark() {
    use contextdb_core::{TxId, Value};
    use contextdb_engine::Database;
    use contextdb_engine::sync_types::NaturalKey;
    use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy, RowChange};
    use std::collections::HashMap;
    use uuid::Uuid;

    // Single in-memory edge — the receiver. A fresh `Database::open_memory()`
    // starts with committed_watermark == TxId(0) and next_tx == TxId(1). Peer
    // TxId(100) is well beyond the local initial watermark, which is the exact
    // condition this test pins: sync-apply must accept a peer TxId that exceeds
    // the receiver's local allocator and advance both counters accordingly.
    let edge_b = Database::open_memory();

    edge_b
        .execute(
            "CREATE TABLE t (pk UUID PRIMARY KEY, x TXID NOT NULL)",
            &HashMap::new(),
        )
        .expect("edge_b CREATE TABLE must succeed");

    // Construct a ChangeSet carrying Value::TxId(TxId(100)) for table `t` column `x`.
    let pk = Uuid::from_u128(0xFEED_FACE_0000_0001_0000_0000_0000_0001);
    let mut values: HashMap<String, Value> = HashMap::new();
    values.insert("pk".to_string(), Value::Uuid(pk));
    values.insert("x".to_string(), Value::TxId(TxId(100)));
    let changeset = ChangeSet {
        rows: vec![RowChange {
            table: "t".to_string(),
            natural_key: NaturalKey::single("pk".to_string(), Value::Uuid(pk)),
            values,
            deleted: false,
            lsn: contextdb_core::Lsn(1),
            created_at: None,
        }],
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: Vec::new(),

        ddl_lsn: Vec::new(),
    };

    // Apply on edge-B — apply_changes is the sync-pull entry point and internally
    // commits under CommitSource::SyncPull.
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let result = edge_b
        .apply_changes(changeset, &policies)
        .expect("sync-apply must succeed for peer TxId beyond local watermark");

    // Row-count pinned.
    assert_eq!(
        result.applied_rows, 1,
        "sync-apply must report exactly 1 applied row, got {}",
        result.applied_rows
    );

    // Allocator pinned: next_tx must have advanced past the peer max.
    let next_tx_after = edge_b.next_tx();
    assert!(
        next_tx_after.0 >= 101,
        "edge_b.next_tx must be >= TxId(101) after applying peer TxId(100); got {:?}",
        next_tx_after
    );

    // Watermark pinned: committed_watermark must have advanced past the peer value.
    let watermark_after = edge_b.committed_watermark();
    assert!(
        watermark_after.0 >= 100,
        "edge_b.committed_watermark must be >= TxId(100) after applying peer TxId(100); got {:?}",
        watermark_after
    );

    // A subsequent local transaction on edge-B must allocate a TxId >= 101 —
    // proving the allocator did not silently reuse an id. begin() returns a bare
    // TxId; rollback releases it.
    let probe_tx = edge_b.begin_or_panic();
    assert!(
        probe_tx.0 >= 101,
        "new transaction on edge_b must issue TxId >= 101 after allocator advance; got {:?}",
        probe_tx
    );
    edge_b
        .rollback(probe_tx)
        .expect("rollback of probe tx must succeed");

    // SELECT x FROM t on edge-B returns exactly one row whose cell equals Value::TxId(TxId(100)).
    let result = edge_b
        .execute("SELECT x FROM t", &HashMap::new())
        .expect("SELECT must succeed");
    assert_eq!(result.rows.len(), 1, "edge_b must have exactly 1 row in t");
    let x_idx = result
        .columns
        .iter()
        .position(|c| c == "x")
        .expect("result must have column \"x\"");
    assert_eq!(
        result.rows[0][x_idx],
        Value::TxId(TxId(100)),
        "edge_b row cell must equal Value::TxId(TxId(100))"
    );
}

// ======== T15 ========
#[test]
fn sync_apply_rejects_peer_txid_u64_max() {
    use contextdb_core::{Error, TxId, Value};
    use contextdb_engine::Database;
    use contextdb_engine::sync_types::NaturalKey;
    use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy, RowChange};
    use std::collections::HashMap;
    use uuid::Uuid;

    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (pk UUID PRIMARY KEY, x TXID NOT NULL)",
        &HashMap::new(),
    )
    .expect("CREATE TABLE must succeed");

    // A fresh `Database::open_memory()` starts with next_tx == TxId(1) and
    // committed_watermark == TxId(0). Pin those exact values as preconditions
    // so the "allocator unchanged after overflow rejection" assertion below
    // has deterministic, typed expected values rather than merely captured
    // copies.
    let next_tx_before = db.next_tx();
    let watermark_before = db.committed_watermark();
    assert_eq!(
        next_tx_before,
        TxId(1),
        "precondition: fresh open_memory next_tx must be TxId(1)"
    );
    assert_eq!(
        watermark_before,
        TxId(0),
        "precondition: fresh open_memory committed_watermark must be TxId(0)"
    );

    // Construct a ChangeSet with Value::TxId(TxId(u64::MAX)).
    let pk = Uuid::from_u128(0xDEAD_BEEF_0000_0002_0000_0000_0000_0002);
    let mut values: HashMap<String, Value> = HashMap::new();
    values.insert("pk".to_string(), Value::Uuid(pk));
    values.insert("x".to_string(), Value::TxId(TxId(u64::MAX)));
    let changeset = ChangeSet {
        rows: vec![RowChange {
            table: "t".to_string(),
            natural_key: NaturalKey::single("pk".to_string(), Value::Uuid(pk)),
            values,
            deleted: false,
            lsn: contextdb_core::Lsn(1),
            created_at: None,
        }],
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: Vec::new(),

        ddl_lsn: Vec::new(),
    };

    // Apply must return Err(Error::TxIdOverflow { table: "t", incoming: u64::MAX }).
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let err = db
        .apply_changes(changeset, &policies)
        .expect_err("sync-apply must reject Value::TxId(TxId(u64::MAX))");

    match err {
        Error::TxIdOverflow { table, incoming } => {
            assert_eq!(table, "t", "error.table must equal the target table");
            assert_eq!(incoming, u64::MAX, "error.incoming must equal u64::MAX");
        }
        other => panic!("expected Error::TxIdOverflow, got {other:?}"),
    }

    // Allocator state must be unchanged.
    assert_eq!(
        db.next_tx(),
        next_tx_before,
        "next_tx must be unchanged after overflow rejection"
    );
    assert_eq!(
        db.committed_watermark(),
        watermark_before,
        "committed_watermark must be unchanged after overflow rejection"
    );

    // No row must have been committed into table t.
    let count_rows = db
        .execute("SELECT COUNT(*) FROM t", &HashMap::new())
        .expect("SELECT COUNT(*) must succeed")
        .rows;
    assert_eq!(count_rows.len(), 1);
    assert_eq!(
        count_rows[0][0],
        Value::Int64(0),
        "no row must be committed when sync-apply overflow aborts"
    );
}

// ======== T16 ========
#[test]
fn sync_apply_row_count_preserved_across_txid_boundary() {
    use contextdb_core::{TxId, Value};
    use contextdb_engine::Database;
    use contextdb_engine::sync_types::NaturalKey;
    use contextdb_engine::sync_types::{
        ApplyResult, ChangeSet, ConflictPolicies, ConflictPolicy, RowChange,
    };
    use std::collections::HashMap;
    use uuid::Uuid;

    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (pk UUID PRIMARY KEY, x TXID NOT NULL)",
        &HashMap::new(),
    )
    .expect("CREATE TABLE must succeed");

    // Fresh `Database::open_memory()` starts at committed_watermark == TxId(0).
    // The 50 incoming rows carry Value::TxId(TxId(51..=100)), which straddles a
    // clear gap above the local watermark — this is the "txid boundary" the
    // test title references. Row-count preservation must hold regardless of
    // that gap.

    // Build 50 RowChange entries with TxId(51..=100), each with a unique primary key.
    let mut row_changes: Vec<RowChange> = Vec::with_capacity(50);
    for i in 0..50u64 {
        let pk = Uuid::from_u128(0xABCD_0000_0000_0000_0000_0000_0000_0000u128 + (i as u128));
        let mut values: HashMap<String, Value> = HashMap::new();
        values.insert("pk".to_string(), Value::Uuid(pk));
        values.insert("x".to_string(), Value::TxId(TxId(51 + i)));
        row_changes.push(RowChange {
            table: "t".to_string(),
            natural_key: NaturalKey::single("pk".to_string(), Value::Uuid(pk)),
            values,
            deleted: false,
            lsn: contextdb_core::Lsn(100 + i),
            created_at: None,
        });
    }
    let changeset = ChangeSet {
        rows: row_changes,
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: Vec::new(),

        ddl_lsn: Vec::new(),
    };

    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let result: ApplyResult = db
        .apply_changes(changeset, &policies)
        .expect("sync-apply must succeed for 50-row TxId batch");

    // Destructure all counters.
    let ApplyResult {
        applied_rows,
        skipped_rows,
        conflicts,
        new_lsn: _new_lsn,
    } = result;

    assert_eq!(
        applied_rows, 50,
        "all 50 rows must apply; got applied_rows={applied_rows}"
    );
    assert_eq!(
        skipped_rows, 0,
        "no rows must be skipped; got skipped_rows={skipped_rows}"
    );
    assert!(
        conflicts.is_empty(),
        "no rows must land in conflicts; got conflicts.len()={}",
        conflicts.len()
    );

    // Follow-up SELECT COUNT(*) must reflect 50 committed rows.
    let count_rows = db
        .execute("SELECT COUNT(*) FROM t", &HashMap::new())
        .expect("SELECT COUNT(*) must succeed")
        .rows;
    assert_eq!(count_rows.len(), 1);
    assert_eq!(
        count_rows[0][0],
        Value::Int64(50),
        "COUNT(*) must equal 50 after applying 50 sync rows"
    );

    // Identity guard: read back two specific rows by primary key and assert the
    // stored Value::TxId matches the exact value supplied on the incoming
    // RowChange. Cardinality (50 == 50) is satisfied by pass-through stubs that
    // echo row counts without preserving per-row TxId identity; this check
    // forces byte-level fidelity of Value::TxId through the sync-apply path.
    let pk_first = Uuid::from_u128(0xABCD_0000_0000_0000_0000_0000_0000_0000u128);
    let pk_last = Uuid::from_u128(0xABCD_0000_0000_0000_0000_0000_0000_0000u128 + 49u128);

    let mut params_first: HashMap<String, Value> = HashMap::new();
    params_first.insert("pk".to_string(), Value::Uuid(pk_first));
    let first_rows = db
        .execute("SELECT x FROM t WHERE pk = $pk", &params_first)
        .expect("SELECT x WHERE pk = first must succeed")
        .rows;
    assert_eq!(
        first_rows.len(),
        1,
        "exactly one row must match pk = {pk_first}"
    );
    assert_eq!(
        first_rows[0][0],
        Value::TxId(TxId(51)),
        "row i=0 must store Value::TxId(TxId(51)), the exact value supplied on the sync RowChange"
    );

    let mut params_last: HashMap<String, Value> = HashMap::new();
    params_last.insert("pk".to_string(), Value::Uuid(pk_last));
    let last_rows = db
        .execute("SELECT x FROM t WHERE pk = $pk", &params_last)
        .expect("SELECT x WHERE pk = last must succeed")
        .rows;
    assert_eq!(
        last_rows.len(),
        1,
        "exactly one row must match pk = {pk_last}"
    );
    assert_eq!(
        last_rows[0][0],
        Value::TxId(TxId(100)),
        "row i=49 must store Value::TxId(TxId(100)), the exact value supplied on the sync RowChange"
    );
}
