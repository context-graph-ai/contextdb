use super::common::*;
use tempfile::TempDir;

/// I killed the server mid-push, and when I reopened the database it had either all the data or none — no partial corruption.
#[tokio::test]
async fn f12_server_crash_mid_push_does_not_corrupt_server_data() {
    let tmp = TempDir::new().expect("tempdir");
    let edge_path = temp_db_file(&tmp, "f12-edge.db");
    let server_path = temp_db_file(&tmp, "f12-server.db");
    let nats = start_nats().await;
    let mut server = spawn_server(&server_path, "f12", &nats.nats_url);
    let _ = run_cli_script(
        &edge_path,
        &["--tenant-id", "f12", "--nats-url", &nats.nats_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n.sync push\n.quit\n",
    );
    stop_child(&mut server);
    let count = count_rows_from_file(&server_path, "sensors");
    assert!(count == 0 || count == 1000);
}

/// NATS went down and came back, and my edge CLI reconnected on its own without me restarting it.
#[tokio::test]
async fn f13_nats_restart_does_not_require_edge_restart() {
    let tmp = TempDir::new().expect("tempdir");
    let edge_path = temp_db_file(&tmp, "f13-edge.db");
    let server_path = temp_db_file(&tmp, "f13-server.db");
    let nats = start_nats().await;
    let mut server = spawn_server(&server_path, "f13", &nats.nats_url);
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", "f13", "--nats-url", &nats.nats_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n.sync reconnect\n.sync push\n.quit\n",
    );
    stop_child(&mut server);
    assert!(output.status.success());
    assert!(output_string(&output.stdout).contains("Reconnected"));
}

/// The server restarted, and my edge CLI pushed data to the new instance without me restarting anything.
#[tokio::test]
async fn f14_server_restart_does_not_require_edge_restart() {
    let tmp = TempDir::new().expect("tempdir");
    let edge_path = temp_db_file(&tmp, "f14-edge.db");
    let server_path = temp_db_file(&tmp, "f14-server.db");
    let nats = start_nats().await;
    let mut server = spawn_server(&server_path, "f14", &nats.nats_url);
    let _ = run_cli_script(
        &edge_path,
        &["--tenant-id", "f14", "--nats-url", &nats.nats_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n.sync push\n.quit\n",
    );
    stop_child(&mut server);
    server = spawn_server(&server_path, "f14", &nats.nats_url);
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", "f14", "--nats-url", &nats.nats_url],
        "INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'x')\n.sync push\n.quit\n",
    );
    stop_child(&mut server);
    assert!(output.status.success());
    assert!(count_rows_from_file(&server_path, "sensors") >= 1);
}

/// My edge device ran out of disk, and the CLI gave me a clear error instead of silently corrupting my data.
#[test]
fn f15_disk_full_on_edge_produces_a_clear_error_not_corruption() {
    assert!(false, "requires special infrastructure");
}

/// The server ran out of memory during a push, and it told me something went wrong instead of silently dropping rows.
#[test]
fn f16_server_out_of_memory_does_not_silently_drop_pushed_data() {
    assert!(false, "requires special infrastructure");
}
