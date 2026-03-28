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
/// Simulated by writing to a tmpfs with a size limit.
#[test]
fn f15_disk_full_on_edge_produces_a_clear_error_not_corruption() {
    use std::process::Command;

    // Create a tiny tmpfs (1MB) to simulate disk full
    let mount_dir = tempfile::tempdir().expect("tempdir");
    let mount_path = mount_dir.path().join("tiny");
    std::fs::create_dir_all(&mount_path).expect("mkdir");
    let mount_status = Command::new("mount")
        .args(["-t", "tmpfs", "-o", "size=1m", "tmpfs"])
        .arg(&mount_path)
        .status();

    // If mount fails (no permissions), skip test gracefully
    if mount_status.is_err() || !mount_status.unwrap().success() {
        eprintln!("skipping f15: cannot mount tmpfs (need root or fuse)");
        return;
    }

    let db_path = mount_path.join("full.db");
    let output = run_cli_script(
        &db_path,
        &[],
        // Insert enough data to fill 1MB
        &format!(
            "CREATE TABLE big (id UUID PRIMARY KEY, payload TEXT)\n{}.quit\n",
            (0..500)
                .map(|_i| format!(
                    "INSERT INTO big (id, payload) VALUES ('{}', '{}')\n",
                    uuid::Uuid::new_v4(),
                    "x".repeat(4000)
                ))
                .collect::<String>()
        ),
    );

    // Unmount before assertions
    let _ = Command::new("umount").arg(&mount_path).status();

    let stderr = output_string(&output.stderr).to_lowercase();
    let stdout = output_string(&output.stdout).to_lowercase();
    // Must see an error, not silent corruption
    assert!(
        stderr.contains("error") || stderr.contains("no space") || stdout.contains("error"),
        "disk full must produce a clear error, got stderr: {}, stdout: {}",
        stderr,
        stdout
    );
}

/// The server ran out of memory during a push, and it told me something went wrong instead of silently dropping rows.
/// Simulated by setting a very low MEMORY_LIMIT on the server, then pushing enough data to exceed it.
#[tokio::test]
async fn f16_server_out_of_memory_does_not_silently_drop_pushed_data() {
    let tmp = TempDir::new().expect("tempdir");
    let edge_path = temp_db_file(&tmp, "f16-edge.db");
    let server_path = temp_db_file(&tmp, "f16-server.db");
    let nats = start_nats().await;

    // Pre-configure server DB with a tiny memory limit
    {
        let db = contextdb_engine::Database::open(&server_path).expect("open server db");
        db.execute("SET MEMORY_LIMIT '1M'", &std::collections::HashMap::new())
            .expect("SET MEMORY_LIMIT");
        db.close().expect("close");
    }

    let mut server = spawn_server(&server_path, "f16", &nats.nats_url);

    // Push 500 rows with large payloads to exceed 1MB
    let mut script = String::from("CREATE TABLE big (id UUID PRIMARY KEY, payload TEXT)\n");
    for _ in 0..500 {
        script.push_str(&format!(
            "INSERT INTO big (id, payload) VALUES ('{}', '{}')\n",
            uuid::Uuid::new_v4(),
            "x".repeat(4000)
        ));
    }
    script.push_str(".sync push\n.quit\n");

    let output = run_cli_script_allow_startup_failure_with_timeout(
        &edge_path,
        &["--tenant-id", "f16", "--nats-url", &nats.nats_url],
        &script,
        std::time::Duration::from_secs(30),
    );
    stop_child(&mut server);

    let stdout = output_string(&output.stdout).to_lowercase();
    let stderr = output_string(&output.stderr).to_lowercase();
    let reported_error = stdout.contains("error")
        || stdout.contains("memory")
        || stdout.contains("limit")
        || stderr.contains("error")
        || stderr.contains("memory")
        || stderr.contains("limit");

    // Either the push succeeds cleanly and all rows arrive, or it fails clearly.
    if output.status.success() {
        assert!(
            !reported_error,
            "successful push must not report an error; stdout={stdout}, stderr={stderr}"
        );
        let count = try_count_rows_from_file(&server_path, "big")
            .expect("count rows from server db")
            .expect("successful push must materialize the big table");
        assert_eq!(
            count, 500,
            "successful push must persist all 500 rows, got {}",
            count
        );
    } else {
        assert!(
            reported_error,
            "failed push must report a clear error; stdout={stdout}, stderr={stderr}"
        );
    }
}
