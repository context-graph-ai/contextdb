use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use contextdb_server::{SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

struct NatsFixture {
    _container: ContainerAsync<GenericImage>,
    nats_url: String,
    ws_url: String,
}

async fn start_nats() -> NatsFixture {
    let nats_conf = format!("{}/tests/nats.conf", env!("CARGO_MANIFEST_DIR"));

    let image = GenericImage::new("nats", "latest")
        .with_exposed_port(4222.tcp())
        .with_exposed_port(9222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"));

    let request = image
        .with_mount(Mount::bind_mount(&nats_conf, "/etc/nats/nats.conf"))
        .with_cmd(["--js", "--config", "/etc/nats/nats.conf"]);

    let container: ContainerAsync<GenericImage> = request.start().await.unwrap();

    let nats_port = container.get_host_port_ipv4(4222.tcp()).await.unwrap();
    let ws_port = container.get_host_port_ipv4(9222.tcp()).await.unwrap();

    NatsFixture {
        _container: container,
        nats_url: format!("nats://127.0.0.1:{nats_port}"),
        ws_url: format!("ws://127.0.0.1:{ws_port}"),
    }
}

#[tokio::test]
async fn sync_round_trip_smoke() {
    let nats = start_nats().await;
    let edge = Arc::new(Database::open_memory());
    let server_db = Arc::new(Database::open_memory());
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let server = Arc::new(SyncServer::new(
        server_db,
        &nats.nats_url,
        "test_tenant",
        policies.clone(),
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });

    let client = SyncClient::new(edge, &nats.nats_url, "test_tenant");
    let _ = client.pull(&policies).await;
}

// A1: Lazy connection and reuse
#[tokio::test]
async fn a1_lazy_connection_and_reuse() {
    use contextdb_core::Value;
    use uuid::Uuid;

    let nats = start_nats().await;
    let edge_db = Arc::new(Database::open_memory());
    let server_db = Arc::new(Database::open_memory());
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);

    // Create table on both databases
    let empty = HashMap::new();
    edge_db
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)", &empty)
        .unwrap();
    server_db
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)", &empty)
        .unwrap();

    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "reuse-test",
        policies.clone(),
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "reuse-test");

    // Before any call, should not be connected (lazy)
    assert!(
        !client.is_connected().await,
        "client must not be connected before first call"
    );

    // Push to trigger connection
    client.push().await.unwrap();

    // After push, should be connected (stored in Mutex)
    assert!(
        client.is_connected().await,
        "client must be connected after push (lazy connect + store)"
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
        "data must be delivered via reused connection"
    );

    // Verify server has the row
    let server_row = server_db
        .point_lookup("t", "id", &Value::Uuid(id), server_db.snapshot())
        .unwrap();
    assert!(
        server_row.is_some(),
        "server must have the row pushed by edge"
    );
}

// A2: Connection failure produces actionable error
#[tokio::test]
async fn a2_connection_failure_actionable_error() {
    use contextdb_core::Value;
    use uuid::Uuid;

    let db = Arc::new(Database::open_memory());
    let empty = HashMap::new();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)", &empty)
        .unwrap();

    // Insert a row so changeset is non-empty
    let id = Uuid::new_v4();
    let mut params = HashMap::new();
    params.insert("id".to_string(), Value::Uuid(id));
    params.insert("v".to_string(), Value::Text("data".into()));
    db.execute("INSERT INTO t (id, v) VALUES ($id, $v)", &params)
        .unwrap();

    // Client pointing to unreachable port
    let client = SyncClient::new(db, "nats://localhost:19999", "no-server-registered");
    let result = client.push().await;

    assert!(result.is_err(), "push to unreachable NATS must fail");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("19999"),
        "error must contain the NATS port '19999', got: {}",
        err_msg
    );
}

// A3: pull_default() uses runtime-configured policies
#[tokio::test]
async fn a3_pull_default_uses_configured_policies() {
    use contextdb_core::Value;
    use uuid::Uuid;

    let nats = start_nats().await;
    let server_db = Arc::new(Database::open_memory());
    let edge_db = Arc::new(Database::open_memory());
    let empty = HashMap::new();

    // Create table on both
    server_db
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)", &empty)
        .unwrap();
    edge_db
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)", &empty)
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

    let policies = ConflictPolicies::uniform(ConflictPolicy::ServerWins);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "pull-default-test",
        policies,
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "pull-default-test");

    // Configure EdgeWins — edge value should survive
    client.set_default_conflict_policy(ConflictPolicy::EdgeWins);
    client.pull_default().await.unwrap();

    // If pull_default hardcoded ServerWins, edge value would be overwritten
    let row = edge_db
        .point_lookup("t", "id", &Value::Uuid(id), edge_db.snapshot())
        .unwrap()
        .expect("row must exist after pull");
    let v = row.values.get("v").expect("column v must exist");
    assert_eq!(
        v,
        &Value::Text("edge-value".into()),
        "EdgeWins should keep edge value; if 'server-value', pull_default used hardcoded ServerWins"
    );
}

// A4: set_table_direction() blocks data on pull
#[tokio::test]
async fn a4_set_table_direction_blocks_pull() {
    use contextdb_core::Value;
    use contextdb_engine::sync_types::SyncDirection;
    use uuid::Uuid;

    let nats = start_nats().await;
    let server_db = Arc::new(Database::open_memory());
    let edge_db = Arc::new(Database::open_memory());
    let empty = HashMap::new();

    // Create two tables on server
    server_db
        .execute("CREATE TABLE synced (id UUID PRIMARY KEY, v TEXT)", &empty)
        .unwrap();
    server_db
        .execute("CREATE TABLE blocked (id UUID PRIMARY KEY, v TEXT)", &empty)
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
        .execute("CREATE TABLE synced (id UUID PRIMARY KEY, v TEXT)", &empty)
        .unwrap();
    edge_db
        .execute("CREATE TABLE blocked (id UUID PRIMARY KEY, v TEXT)", &empty)
        .unwrap();

    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "direction-test",
        policies.clone(),
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "direction-test");

    // Block the "blocked" table
    client.set_table_direction("blocked", SyncDirection::None);
    client.pull(&policies).await.unwrap();

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
        "blocked table must have 0 rows on edge (direction=None). If >0, set_table_direction is a no-op"
    );
}

// A5: WebSocket transport
#[tokio::test]
async fn a5_websocket_transport() {
    use contextdb_core::Value;
    use uuid::Uuid;

    let nats = start_nats().await;
    let server_db = Arc::new(Database::open_memory());
    let edge_db = Arc::new(Database::open_memory());
    let empty = HashMap::new();

    server_db
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)", &empty)
        .unwrap();
    edge_db
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)", &empty)
        .unwrap();

    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "ws-test",
        policies.clone(),
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Edge connects via WebSocket
    let client = SyncClient::new(edge_db.clone(), &nats.ws_url, "ws-test");

    // Edge pushes a row over WebSocket
    let push_id = Uuid::new_v4();
    let mut params = HashMap::new();
    params.insert("id".to_string(), Value::Uuid(push_id));
    params.insert("v".to_string(), Value::Text("ws-push".into()));
    edge_db
        .execute("INSERT INTO t (id, v) VALUES ($id, $v)", &params)
        .unwrap();
    client.push().await.unwrap();

    // Verify server received it
    let server_row = server_db
        .point_lookup("t", "id", &Value::Uuid(push_id), server_db.snapshot())
        .unwrap();
    assert!(
        server_row.is_some(),
        "server must receive row pushed via WebSocket"
    );

    // Server inserts a row, edge pulls over WebSocket
    let pull_id = Uuid::new_v4();
    let mut params2 = HashMap::new();
    params2.insert("id".to_string(), Value::Uuid(pull_id));
    params2.insert("v".to_string(), Value::Text("ws-pull".into()));
    server_db
        .execute("INSERT INTO t (id, v) VALUES ($id, $v)", &params2)
        .unwrap();

    client.pull(&policies).await.unwrap();

    let edge_row = edge_db
        .point_lookup("t", "id", &Value::Uuid(pull_id), edge_db.snapshot())
        .unwrap();
    assert!(
        edge_row.is_some(),
        "edge must receive row pulled via WebSocket"
    );
}

// A6: reconnect() clears stored connection and re-establishes
#[tokio::test]
async fn a6_reconnect_clears_and_reestablishes() {
    let nats = start_nats().await;
    let edge_db = Arc::new(Database::open_memory());
    let server_db = Arc::new(Database::open_memory());
    let empty = HashMap::new();

    edge_db
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)", &empty)
        .unwrap();
    server_db
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)", &empty)
        .unwrap();

    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "reconnect-test",
        policies.clone(),
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Success path
    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "reconnect-test");
    client.push().await.unwrap(); // establishes connection
    assert!(client.is_connected().await, "must be connected after push");
    client.reconnect().await; // drops and re-establishes
    assert!(
        client.is_connected().await,
        "must be connected after reconnect to valid server"
    );
    client.push().await.unwrap(); // still works

    // Failure path: bad port
    let bad_db = Arc::new(Database::open_memory());
    let bad_client = SyncClient::new(bad_db, "nats://localhost:19999", "bad-port");
    bad_client.reconnect().await;
    assert!(
        !bad_client.is_connected().await,
        "reconnect to unreachable port must leave client disconnected"
    );
}

// A7: set_conflict_policy() per-table overrides default on pull
#[tokio::test]
async fn a7_per_table_conflict_policy_override() {
    use contextdb_core::Value;
    use uuid::Uuid;

    let nats = start_nats().await;
    let server_db = Arc::new(Database::open_memory());
    let edge_db = Arc::new(Database::open_memory());
    let empty = HashMap::new();

    // Create two tables on both
    for db in [&server_db, &edge_db] {
        db.execute(
            "CREATE TABLE observations (id UUID PRIMARY KEY, v TEXT)",
            &empty,
        )
        .unwrap();
        db.execute(
            "CREATE TABLE decisions (id UUID PRIMARY KEY, v TEXT)",
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

    let policies = ConflictPolicies::uniform(ConflictPolicy::ServerWins);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "policy-override-test",
        policies,
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "policy-override-test");

    // Default = ServerWins, but observations = InsertIfNotExists (skip duplicates)
    client.set_default_conflict_policy(ConflictPolicy::ServerWins);
    client.set_conflict_policy("observations", ConflictPolicy::InsertIfNotExists);
    client.pull_default().await.unwrap();

    // Observations: InsertIfNotExists → edge value survives
    let obs_row = edge_db
        .point_lookup(
            "observations",
            "id",
            &Value::Uuid(obs_id),
            edge_db.snapshot(),
        )
        .unwrap()
        .expect("observation row must exist");
    let obs_v = obs_row.values.get("v").expect("column v must exist");
    assert_eq!(
        obs_v,
        &Value::Text("edge-obs".into()),
        "InsertIfNotExists should keep edge observation value"
    );

    // Decisions: ServerWins → server value overwrites
    let dec_row = edge_db
        .point_lookup("decisions", "id", &Value::Uuid(dec_id), edge_db.snapshot())
        .unwrap()
        .expect("decision row must exist");
    let dec_v = dec_row.values.get("v").expect("column v must exist");
    assert_eq!(
        dec_v,
        &Value::Text("server-dec".into()),
        "ServerWins should overwrite edge decision with server value"
    );
}

// A8: Pull watermark advances after successful pull
#[tokio::test]
async fn a8_pull_watermark_advances() {
    use contextdb_core::Value;
    use uuid::Uuid;

    let nats = start_nats().await;
    let server_db = Arc::new(Database::open_memory());
    let edge_db = Arc::new(Database::open_memory());
    let empty = HashMap::new();

    server_db
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)", &empty)
        .unwrap();
    edge_db
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)", &empty)
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

    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "pull-wm-test",
        policies.clone(),
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "pull-wm-test");

    // First pull — gets 5 rows
    let result1 = client.pull(&policies).await.unwrap();
    assert_eq!(result1.applied_rows, 5, "first pull must apply 5 rows");
    assert_eq!(result1.skipped_rows, 0, "first pull must skip 0 rows");
    assert!(
        client.pull_watermark() > 0,
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
    let result2 = client.pull(&policies).await.unwrap();
    assert_eq!(result2.applied_rows, 1, "second pull must apply 1 row");
    assert_eq!(
        result2.skipped_rows, 0,
        "second pull must skip 0 rows — if >0, since_lsn is hardcoded to 0"
    );
    assert!(
        client.pull_watermark() > prev_watermark,
        "pull watermark must advance after second pull"
    );
}

// A9: RowDelete events are synced
#[tokio::test]
async fn a9_row_delete_events_synced() {
    use contextdb_core::Value;
    use uuid::Uuid;

    let nats = start_nats().await;
    let server_db = Arc::new(Database::open_memory());
    let edge_db = Arc::new(Database::open_memory());
    let empty = HashMap::new();

    edge_db
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)", &empty)
        .unwrap();
    server_db
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)", &empty)
        .unwrap();

    let policies = ConflictPolicies::uniform(ConflictPolicy::EdgeWins);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "rowdelete-test",
        policies.clone(),
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "rowdelete-test");

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
                natural_key: NaturalKey {
                    column: "id".to_string(),
                    value: Value::Uuid(uuid_a),
                },
                values: {
                    let mut v = HashMap::new();
                    v.insert("id".to_string(), Value::Uuid(uuid_a));
                    v.insert("status".to_string(), Value::Text("draft".into()));
                    v.insert("embedding".to_string(), Value::Vector(vec![1.0, 0.0, 0.0]));
                    v
                },
                deleted: false,
                lsn: 10,
            },
            RowChange {
                table: "t".to_string(),
                natural_key: NaturalKey {
                    column: "id".to_string(),
                    value: Value::Uuid(uuid_b),
                },
                values: {
                    let mut v = HashMap::new();
                    v.insert("id".to_string(), Value::Uuid(uuid_b));
                    // INVALID: server has status='active', transitioning to 'draft' is not allowed
                    v.insert("status".to_string(), Value::Text("draft".into()));
                    v.insert("embedding".to_string(), Value::Vector(vec![0.0, 1.0, 0.0]));
                    v
                },
                deleted: false,
                lsn: 11,
            },
            RowChange {
                table: "t".to_string(),
                natural_key: NaturalKey {
                    column: "id".to_string(),
                    value: Value::Uuid(uuid_c),
                },
                values: {
                    let mut v = HashMap::new();
                    v.insert("id".to_string(), Value::Uuid(uuid_c));
                    v.insert("status".to_string(), Value::Text("draft".into()));
                    v.insert("embedding".to_string(), Value::Vector(vec![0.0, 0.0, 1.0]));
                    v
                },
                deleted: false,
                lsn: 12,
            },
        ],
        edges: Vec::new(),
        vectors: vec![
            VectorChange {
                row_id: edge_row_a,
                vector: vec![1.0, 0.0, 0.0],
                lsn: 10,
            },
            VectorChange {
                row_id: edge_row_b,
                vector: vec![0.0, 1.0, 0.0],
                lsn: 11,
            },
            VectorChange {
                row_id: edge_row_c,
                vector: vec![0.0, 0.0, 1.0],
                lsn: 12,
            },
        ],
        ddl: Vec::new(),
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
        .query_vector(&[1.0, 0.0, 0.0], 1, None, server_db.snapshot())
        .unwrap();
    assert_eq!(search_a.len(), 1, "row A's vector must be findable");
    assert!(
        search_a[0].1 > 0.99,
        "row A's vector must have near-perfect cosine similarity, got {}",
        search_a[0].1
    );

    // KEY ASSERTION: Verify row C's vector is [0.0, 0.0, 1.0], NOT [0.0, 1.0, 0.0]
    let search_c = server_db
        .query_vector(&[0.0, 0.0, 1.0], 1, None, server_db.snapshot())
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
        .query_vector(&[0.0, 1.0, 0.0], 1, None, server_db.snapshot())
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

    // These must panic
    let r = catch_unwind(AssertUnwindSafe(|| {
        SyncClient::new(db.clone(), "nats://x", "foo.bar")
    }));
    assert!(r.is_err(), "dot in tenant_id must panic");

    let r = catch_unwind(AssertUnwindSafe(|| {
        SyncClient::new(db.clone(), "nats://x", "foo*")
    }));
    assert!(r.is_err(), "wildcard in tenant_id must panic");

    let r = catch_unwind(AssertUnwindSafe(|| {
        SyncClient::new(db.clone(), "nats://x", "foo>")
    }));
    assert!(r.is_err(), "NATS multi-level wildcard must panic");

    let r = catch_unwind(AssertUnwindSafe(|| {
        SyncClient::new(db.clone(), "nats://x", "")
    }));
    assert!(r.is_err(), "empty tenant_id must panic");

    let r = catch_unwind(AssertUnwindSafe(|| {
        SyncClient::new(db.clone(), "nats://x", "foo bar")
    }));
    assert!(r.is_err(), "space in tenant_id must panic");

    // Same for SyncServer
    let policies = ConflictPolicies::uniform(ConflictPolicy::ServerWins);
    let r = catch_unwind(AssertUnwindSafe(|| {
        SyncServer::new(db.clone(), "nats://x", "foo.bar", policies.clone())
    }));
    assert!(r.is_err(), "SyncServer must also reject dots");

    // These must succeed (no panic)
    SyncClient::new(db.clone(), "nats://x", "valid-tenant");
    SyncClient::new(db.clone(), "nats://x", "tenant_123");
    SyncClient::new(db.clone(), "nats://x", "MyTenant");
}

// A12: NATS request timeout returns an error
#[tokio::test]
async fn a12_nats_request_timeout_returns_error() {
    use contextdb_core::Value;
    use uuid::Uuid;

    let nats = start_nats().await;
    let edge_db = Arc::new(Database::open_memory());
    let server_db = Arc::new(Database::open_memory());
    let empty = HashMap::new();

    edge_db
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)", &empty)
        .unwrap();
    server_db
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)", &empty)
        .unwrap();

    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let _server = SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "timeout-test",
        policies.clone(),
    );

    // Subscribe to push subject but never reply (simulating hung server)
    let nats_client = async_nats::connect(&nats.nats_url).await.unwrap();
    let _sub = nats_client
        .subscribe(contextdb_server::subjects::push_subject("timeout-test"))
        .await
        .unwrap();

    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "timeout-test");

    // Insert data so push has something to send
    let id = Uuid::new_v4();
    let mut p = HashMap::new();
    p.insert("id".to_string(), Value::Uuid(id));
    p.insert("v".to_string(), Value::Text("data".into()));
    edge_db
        .execute("INSERT INTO t (id, v) VALUES ($id, $v)", &p)
        .unwrap();

    // Wrap in test-level timeout to prevent hanging in red state
    let result = tokio::time::timeout(std::time::Duration::from_secs(30), client.push()).await;

    match result {
        Ok(Err(_)) => {}
        Ok(Ok(_)) => panic!("push should have failed after NATS timeout with no fallback"),
        Err(_elapsed) => panic!("push hung — SYNC_TIMEOUT not firing"),
    }
}

// A13: Pull pagination fetches all pages (exact test code from plan)
#[tokio::test]
async fn a13_pull_pagination_fetches_all_pages() {
    use contextdb_core::Value;
    use contextdb_engine::sync_types::*;
    use std::collections::HashMap;
    use uuid::Uuid;

    let server_db = Arc::new(Database::open_memory());
    let edge_db = Arc::new(Database::open_memory());
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);

    // Create table on server and insert 1500 rows
    let empty = HashMap::new();
    server_db
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, data TEXT) IMMUTABLE",
            &empty,
        )
        .unwrap();

    // Insert 1500 rows via apply_changes (NOT execute) so all rows share ONE LSN.
    let mut rows = Vec::new();
    for i in 0..1500 {
        let id = Uuid::new_v4();
        let mut values = HashMap::new();
        values.insert("id".to_string(), Value::Uuid(id));
        values.insert("data".to_string(), Value::Text(format!("row_{}", i)));
        rows.push(RowChange {
            table: "t".to_string(),
            natural_key: NaturalKey {
                column: "id".to_string(),
                value: Value::Uuid(id),
            },
            values,
            deleted: false,
            lsn: 0,
        });
    }
    let changeset = ChangeSet {
        rows,
        edges: vec![],
        vectors: vec![],
        ddl: vec![],
    };
    let insert_policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    server_db
        .apply_changes(changeset, &insert_policies)
        .unwrap();

    // Start SyncServer on NATS
    let nats = start_nats().await;
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "pagination-test",
        policies.clone(),
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Edge client connects via NATS (NOT local fallback)
    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "pagination-test");

    // Pull all data
    let result = client.pull(&policies).await.unwrap();

    // Verify NATS path was used, not local fallback
    assert!(
        client.is_connected().await,
        "NATS must be running for A13. local_pull does not paginate — \
         test is meaningless without NATS. Run: docker compose -f \
         crates/contextdb-server/tests/docker-compose.yml up -d"
    );

    // KEY ASSERTION: all 1500 rows must arrive, not just the first page
    assert_eq!(
        result.applied_rows, 1500,
        "all 1500 rows must arrive via pagination. Got {} — \
         if 500, the pagination loop is missing (stub behavior). \
         If 0, NATS connection failed (check docker-compose).",
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
}

// A15: Concurrent push and pull on same client
#[tokio::test]
async fn a15_concurrent_push_and_pull() {
    use contextdb_core::Value;
    use uuid::Uuid;

    let nats = start_nats().await;
    let server_db = Arc::new(Database::open_memory());
    let edge_db = Arc::new(Database::open_memory());
    let empty = HashMap::new();

    // Create table on both
    server_db
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)", &empty)
        .unwrap();
    edge_db
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)", &empty)
        .unwrap();

    // Insert data on server (for pull to fetch)
    let server_id = Uuid::new_v4();
    let mut p = HashMap::new();
    p.insert("id".to_string(), Value::Uuid(server_id));
    p.insert("v".to_string(), Value::Text("server-data".into()));
    server_db
        .execute("INSERT INTO t (id, v) VALUES ($id, $v)", &p)
        .unwrap();

    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "concurrent-client-test",
        policies.clone(),
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "concurrent-client-test");

    // Insert data on edge (for push to send)
    let edge_id = Uuid::new_v4();
    let mut p = HashMap::new();
    p.insert("id".to_string(), Value::Uuid(edge_id));
    p.insert("v".to_string(), Value::Text("edge-data".into()));
    edge_db
        .execute("INSERT INTO t (id, v) VALUES ($id, $v)", &p)
        .unwrap();

    // Run push and pull concurrently
    let (push_r, pull_r) = tokio::join!(client.push(), client.pull(&policies));

    assert!(push_r.is_ok(), "concurrent push must succeed");
    assert!(pull_r.is_ok(), "concurrent pull must succeed");
    assert!(
        client.push_watermark() > 0,
        "push watermark must be non-zero after concurrent ops"
    );
    assert!(
        client.pull_watermark() > 0,
        "pull watermark must be non-zero after concurrent ops"
    );
}
