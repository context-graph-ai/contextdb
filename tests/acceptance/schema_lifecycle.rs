use super::common::*;
use tempfile::TempDir;

/// I created a table and pushed from one edge node, then pulled from a second edge node, and the schema and data arrived intact.
#[tokio::test]
async fn f22_table_created_on_edge_pushed_new_edge_gets_schema_via_pull() {
    let tmp = TempDir::new().expect("tempdir");
    let server_path = temp_db_file(&tmp, "f22-server.db");
    let edge_a = temp_db_file(&tmp, "f22-edge-a.db");
    let edge_b = temp_db_file(&tmp, "f22-edge-b.db");
    let nats = start_nats().await;
    let mut server = spawn_server(&server_path, "f22", &nats.nats_url);
    let _ = run_cli_script(
        &edge_a,
        &["--tenant-id", "f22", "--nats-url", &nats.nats_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\nINSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'a')\n.sync push\n.quit\n",
    );
    let pulled = run_cli_script(
        &edge_b,
        &["--tenant-id", "f22", "--nats-url", &nats.nats_url],
        ".sync pull\n.tables\nSELECT count(*) FROM sensors\n.quit\n",
    );
    stop_child(&mut server);
    let stdout = output_string(&pulled.stdout);
    assert!(stdout.contains("sensors"));
    assert!(stdout.contains("1"));
}

/// I pushed a table from one edge, then pushed a different schema for the same table from another edge, and the server flagged the mismatch.
#[tokio::test]
async fn f23_schema_mismatch_between_edge_and_server_is_detected() {
    let tmp = TempDir::new().expect("tempdir");
    let server_path = temp_db_file(&tmp, "f23-server.db");
    let edge_a = temp_db_file(&tmp, "f23-edge-a.db");
    let edge_b = temp_db_file(&tmp, "f23-edge-b.db");
    let nats = start_nats().await;
    let mut server = spawn_server(&server_path, "f23", &nats.nats_url);
    let _ = run_cli_script(
        &edge_a,
        &["--tenant-id", "f23", "--nats-url", &nats.nats_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n.sync push\n.quit\n",
    );
    let output = run_cli_script(
        &edge_b,
        &["--tenant-id", "f23", "--nats-url", &nats.nats_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT, reading REAL)\n.sync push\n.quit\n",
    );
    stop_child(&mut server);
    let stdout = output_string(&output.stdout).to_lowercase();
    assert!(stdout.contains("schema mismatch") || stdout.contains("migration"));
}

/// I dropped a table on the edge and pushed, and the server no longer had that table.
#[tokio::test]
async fn f24_drop_table_on_edge_push_server_no_longer_serves_that_data() {
    let tmp = TempDir::new().expect("tempdir");
    let server_path = temp_db_file(&tmp, "f24-server.db");
    let edge = temp_db_file(&tmp, "f24-edge.db");
    let nats = start_nats().await;
    let mut server = spawn_server(&server_path, "f24", &nats.nats_url);
    let _ = run_cli_script(
        &edge,
        &["--tenant-id", "f24", "--nats-url", &nats.nats_url],
        "CREATE TABLE temp_data (id UUID PRIMARY KEY, name TEXT)\nINSERT INTO temp_data (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'a')\n.sync push\nDROP TABLE temp_data\n.sync push\n.quit\n",
    );
    stop_child(&mut server);
    let db = contextdb_engine::Database::open(&server_path).expect("open server");
    assert!(!db.table_names().contains(&"temp_data".to_string()));
}
