use super::common::*;
use tempfile::TempDir;
use uuid::Uuid;

fn gen_sensor_inserts(count: usize) -> String {
    let mut s = String::new();
    for i in 0..count {
        s.push_str(&format!(
            "INSERT INTO sensors (id, name) VALUES ('{}', 'sensor-{}')\n",
            Uuid::new_v4(),
            i
        ));
    }
    s
}

/// I pushed different tables from two separate edges, and the server had both tables.
#[tokio::test]
async fn f17_two_edges_push_different_tables_to_same_server() {
    let tmp = TempDir::new().expect("tempdir");
    let server_path = temp_db_file(&tmp, "f17-server.db");
    let edge_a = temp_db_file(&tmp, "f17-edge-a.db");
    let edge_b = temp_db_file(&tmp, "f17-edge-b.db");
    let nats = start_nats().await;
    let mut server = spawn_server(&server_path, "f17", &nats.nats_url);

    assert!(
        run_cli_script(
            &edge_a,
            &["--tenant-id", "f17", "--nats-url", &nats.nats_url],
            "CREATE TABLE temperatures (id UUID PRIMARY KEY, value REAL)\n.sync push\n.quit\n",
        )
        .status
        .success()
    );
    assert!(
        run_cli_script(
            &edge_b,
            &["--tenant-id", "f17", "--nats-url", &nats.nats_url],
            "CREATE TABLE pressures (id UUID PRIMARY KEY, value REAL)\n.sync push\n.quit\n",
        )
        .status
        .success()
    );

    stop_child(&mut server);
    let db = contextdb_engine::Database::open(&server_path).expect("open server db");
    assert!(db.table_names().contains(&"temperatures".to_string()));
    assert!(db.table_names().contains(&"pressures".to_string()));
}

/// I pushed rows to the same table from two edges, and the server had all distinct rows from both.
#[tokio::test]
async fn f18_two_edges_push_to_same_table_different_rows() {
    let tmp = TempDir::new().expect("tempdir");
    let server_path = temp_db_file(&tmp, "f18-server.db");
    let edge_a = temp_db_file(&tmp, "f18-edge-a.db");
    let edge_b = temp_db_file(&tmp, "f18-edge-b.db");
    let nats = start_nats().await;
    let mut server = spawn_server(&server_path, "f18", &nats.nats_url);
    let mut script_a = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script_a.push_str(&gen_sensor_inserts(50));
    script_a.push_str(".sync push\n.quit\n");
    let mut script_b = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script_b.push_str(&gen_sensor_inserts(50));
    script_b.push_str(".sync push\n.quit\n");
    assert!(
        run_cli_script(
            &edge_a,
            &["--tenant-id", "f18", "--nats-url", &nats.nats_url],
            &script_a
        )
        .status
        .success()
    );
    assert!(
        run_cli_script(
            &edge_b,
            &["--tenant-id", "f18", "--nats-url", &nats.nats_url],
            &script_b
        )
        .status
        .success()
    );
    stop_child(&mut server);
    assert_eq!(count_rows_from_file(&server_path, "sensors"), 100);
}

/// I updated the same row from two edges with different values, and the second push got a "conflict" error instead of silently overwriting.
#[tokio::test]
async fn f19_two_edges_push_conflicting_updates_to_same_row() {
    let tmp = TempDir::new().expect("tempdir");
    let server_path = temp_db_file(&tmp, "f19-server.db");
    let edge_a = temp_db_file(&tmp, "f19-edge-a.db");
    let edge_b = temp_db_file(&tmp, "f19-edge-b.db");
    let nats = start_nats().await;
    let mut server = spawn_server(&server_path, "f19", &nats.nats_url);
    let common = "CREATE TABLE sensors (id UUID PRIMARY KEY, reading REAL)\nINSERT INTO sensors (id, reading) VALUES ('00000000-0000-0000-0000-000000000001', 42.0)\n.sync push\n.quit\n";
    let a = run_cli_script(
        &edge_a,
        &["--tenant-id", "f19", "--nats-url", &nats.nats_url],
        common,
    );
    let b = run_cli_script(
        &edge_b,
        &["--tenant-id", "f19", "--nats-url", &nats.nats_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, reading REAL)\nINSERT INTO sensors (id, reading) VALUES ('00000000-0000-0000-0000-000000000001', 99.0)\n.sync push\n.quit\n",
    );
    stop_child(&mut server);
    assert!(a.status.success());
    assert!(b.status.success());
    let db = contextdb_engine::Database::open(&server_path).expect("open server");
    let result = db
        .execute("SELECT reading FROM sensors", &empty_params())
        .expect("select reading");
    assert_eq!(extract_f64(&result, 0, 0), 42.0);
    assert!(output_string(&b.stdout).contains("conflict"));
}

/// I pushed from one edge, pulled from another, and the second edge had all the data.
#[tokio::test]
async fn f20_edge_pulls_after_another_edge_pushed() {
    let tmp = TempDir::new().expect("tempdir");
    let server_path = temp_db_file(&tmp, "f20-server.db");
    let edge_a = temp_db_file(&tmp, "f20-edge-a.db");
    let edge_b = temp_db_file(&tmp, "f20-edge-b.db");
    let nats = start_nats().await;
    let mut server = spawn_server(&server_path, "f20", &nats.nats_url);
    let mut script_a = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script_a.push_str(&gen_sensor_inserts(100));
    script_a.push_str(".sync push\n.quit\n");
    let _ = run_cli_script(
        &edge_a,
        &["--tenant-id", "f20", "--nats-url", &nats.nats_url],
        &script_a,
    );
    let pulled = run_cli_script(
        &edge_b,
        &["--tenant-id", "f20", "--nats-url", &nats.nats_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n.sync pull\nSELECT count(*) FROM sensors\n.quit\n",
    );
    stop_child(&mut server);
    assert!(output_string(&pulled.stdout).contains("100"));
}

/// I pushed from two edges, both pulled, and each edge ended up with all rows from both.
#[tokio::test]
async fn f21_edge_a_pushes_edge_b_pushes_both_pull() {
    let tmp = TempDir::new().expect("tempdir");
    let server_path = temp_db_file(&tmp, "f21-server.db");
    let edge_a = temp_db_file(&tmp, "f21-edge-a.db");
    let edge_b = temp_db_file(&tmp, "f21-edge-b.db");
    let nats = start_nats().await;
    let mut server = spawn_server(&server_path, "f21", &nats.nats_url);
    let mut script_a = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script_a.push_str(&gen_sensor_inserts(50));
    script_a.push_str(".sync push\n.quit\n");
    let _ = run_cli_script(
        &edge_a,
        &["--tenant-id", "f21", "--nats-url", &nats.nats_url],
        &script_a,
    );
    let mut script_b = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script_b.push_str(&gen_sensor_inserts(50));
    script_b.push_str(".sync push\n.sync pull\nSELECT count(*) FROM sensors\n.quit\n");
    let _ = run_cli_script(
        &edge_b,
        &["--tenant-id", "f21", "--nats-url", &nats.nats_url],
        &script_b,
    );
    let pulled_a = run_cli_script(
        &edge_a,
        &["--tenant-id", "f21", "--nats-url", &nats.nats_url],
        ".sync pull\nSELECT count(*) FROM sensors\n.quit\n",
    );
    stop_child(&mut server);
    assert!(output_string(&pulled_a.stdout).contains("100"));
    assert_eq!(count_rows_from_file(&server_path, "sensors"), 100);
}

/// I created an entity on one edge and linked it to another entity on a second edge, then a third edge pulled and the graph traversal worked end-to-end.
#[tokio::test]
async fn f21b_cross_edge_graph_construction_via_sync() {
    let tmp = TempDir::new().expect("tempdir");
    let server_path = temp_db_file(&tmp, "f21b-server.db");
    let edge_a = temp_db_file(&tmp, "f21b-edge-a.db");
    let edge_b = temp_db_file(&tmp, "f21b-edge-b.db");
    let edge_c = temp_db_file(&tmp, "f21b-edge-c.db");
    let nats = start_nats().await;
    let mut server = spawn_server(&server_path, "f21b", &nats.nats_url);
    let _ = run_cli_script(
        &edge_a,
        &["--tenant-id", "f21b", "--nats-url", &nats.nats_url],
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT)\nINSERT INTO entities (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'sensor-1')\n.sync push\n.quit\n",
    );
    let _ = run_cli_script(
        &edge_b,
        &["--tenant-id", "f21b", "--nats-url", &nats.nats_url],
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT)\nINSERT INTO entities (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'region-north')\nINSERT INTO GRAPH (source_id, target_id, edge_type) VALUES ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'EDGE')\n.sync push\n.quit\n",
    );
    let pulled = run_cli_script(
        &edge_c,
        &["--tenant-id", "f21b", "--nats-url", &nats.nats_url],
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT)\n.sync pull\nSELECT * FROM GRAPH_TABLE(edges MATCH (a)-[:EDGE]->{1,1}(b) WHERE a.id = '00000000-0000-0000-0000-000000000001' COLUMNS(b.id AS target_id))\n.quit\n",
    );
    stop_child(&mut server);
    assert!(output_string(&pulled.stdout).contains("00000000-0000-0000-0000-000000000002"));
}
