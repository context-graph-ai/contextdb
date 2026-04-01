use super::common::*;
use contextdb_core::Value;
use tempfile::TempDir;

fn seed_edge_big_table(edge_path: &std::path::Path, rows: usize) {
    let db = contextdb_engine::Database::open(edge_path).expect("open edge db");
    db.execute(
        "CREATE TABLE big (id UUID PRIMARY KEY, payload TEXT)",
        &std::collections::HashMap::new(),
    )
    .expect("create big table");
    for _ in 0..rows {
        db.execute(
            "INSERT INTO big (id, payload) VALUES ($id, $payload)",
            &std::collections::HashMap::from([
                ("id".to_string(), Value::Uuid(uuid::Uuid::new_v4())),
                ("payload".to_string(), Value::Text("x".repeat(4000))),
            ]),
        )
        .expect("seed edge row");
    }
    db.close().expect("close edge db");
}

#[test]
fn a_db1_disk_limit_flag_sets_startup_ceiling() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "a_db1.db");
    let output = run_cli_script(
        &db_path,
        &["--disk-limit", "4M"],
        "SHOW DISK_LIMIT\n.quit\n",
    );
    assert!(
        output.status.success(),
        "CLI must not crash with --disk-limit"
    );
    let stdout = output_string(&output.stdout);
    assert!(
        stdout.contains("4194304"),
        "SHOW DISK_LIMIT must report startup ceiling of 4M: {stdout}"
    );
}

#[test]
fn a_db2_env_var_sets_startup_ceiling() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "a_db2.db");
    ensure_release_binaries();
    let output = std::process::Command::new(cli_bin())
        .arg(&db_path)
        .env("CONTEXTDB_DISK_LIMIT", "2M")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            child
                .stdin
                .as_mut()
                .expect("stdin")
                .write_all(b"SHOW DISK_LIMIT\n.quit\n")
                .expect("write stdin");
            child.wait_with_output()
        })
        .expect("CLI with env var");
    assert!(output.status.success());
    let stdout = output_string(&output.stdout);
    assert!(
        stdout.contains("2097152"),
        "SHOW DISK_LIMIT must report startup ceiling of 2M: {stdout}"
    );
}

#[test]
fn a_db3_set_disk_limit_below_startup_ceiling_works() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "a_db3.db");
    let output = run_cli_script(
        &db_path,
        &["--disk-limit", "4M"],
        "SET DISK_LIMIT '1M'\nSHOW DISK_LIMIT\n.quit\n",
    );
    assert!(output.status.success());
    let stdout = output_string(&output.stdout);
    assert!(
        stdout.contains("1048576"),
        "SHOW DISK_LIMIT must report configured 1M limit after SET: {stdout}"
    );
    assert!(
        stdout.contains("4194304"),
        "SHOW DISK_LIMIT must retain startup ceiling of 4M: {stdout}"
    );
}

#[test]
fn a_db4_set_disk_limit_above_startup_ceiling_errors() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "a_db4.db");
    let output = run_cli_script(
        &db_path,
        &["--disk-limit", "1M"],
        "SET DISK_LIMIT '4M'\n.quit\n",
    );
    assert!(output.status.success(), "CLI must stay alive on SET error");
    let stdout = output_string(&output.stdout).to_lowercase();
    let stderr = output_string(&output.stderr).to_lowercase();
    assert!(
        stdout.contains("error")
            || stdout.contains("ceiling")
            || stdout.contains("disk")
            || stderr.contains("error")
            || stderr.contains("ceiling"),
        "setting DISK_LIMIT above startup ceiling must produce a clear error; stdout={stdout}, stderr={stderr}"
    );
}

#[test]
fn a_db5_file_backed_disk_limit_survives_restart() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "a_db5.db");

    let configured = run_cli_script(
        &db_path,
        &[],
        "SET DISK_LIMIT '1M'\nSHOW DISK_LIMIT\n.quit\n",
    );
    assert!(configured.status.success());
    let configured_stdout = output_string(&configured.stdout);
    assert!(
        configured_stdout.contains("1048576"),
        "SHOW DISK_LIMIT must report configured limit before restart: {configured_stdout}"
    );

    let reopened = run_cli_script(&db_path, &[], "SHOW DISK_LIMIT\n.quit\n");
    assert!(reopened.status.success());
    let reopened_stdout = output_string(&reopened.stdout);
    assert!(
        reopened_stdout.contains("1048576"),
        "reopened database must still report persisted DISK_LIMIT: {reopened_stdout}"
    );
}

#[tokio::test]
async fn a_db6_server_disk_limit_rejects_sync_push_clearly() {
    let tmp = TempDir::new().expect("tempdir");
    let edge_path = temp_db_file(&tmp, "a_db6-edge.db");
    let server_path = temp_db_file(&tmp, "a_db6-server.db");
    let nats = start_nats().await;
    seed_edge_big_table(&edge_path, 64);

    let configured_limit_bytes = {
        let db = contextdb_engine::Database::open(&server_path).expect("open server db");
        db.execute(
            "CREATE TABLE big (id UUID PRIMARY KEY, payload TEXT)",
            &std::collections::HashMap::new(),
        )
        .expect("create big table");
        db.execute(
            "INSERT INTO big (id, payload) VALUES ($id, $payload)",
            &std::collections::HashMap::from([
                ("id".to_string(), Value::Uuid(uuid::Uuid::new_v4())),
                ("payload".to_string(), Value::Text("prime".repeat(1024))),
            ]),
        )
        .expect("prime server row");
        let limit_kib = (std::fs::metadata(&server_path).expect("metadata").len() / 1024).max(1);
        let configured_limit_bytes = limit_kib * 1024;
        db.execute(
            &format!("SET DISK_LIMIT '{limit_kib}K'"),
            &std::collections::HashMap::new(),
        )
        .expect("set server disk limit");
        db.close().expect("close server db");
        configured_limit_bytes
    };

    let mut server = spawn_server(&server_path, "a_db6", &nats.nats_url);
    let output = run_cli_script_allow_startup_failure_with_timeout(
        &edge_path,
        &["--tenant-id", "a_db6", "--nats-url", &nats.ws_url],
        ".sync push\n.quit\n",
        std::time::Duration::from_secs(30),
    );
    stop_child(&mut server);

    let stdout = output_string(&output.stdout).to_lowercase();
    let stderr = output_string(&output.stderr).to_lowercase();
    assert!(
        stdout.contains("disk") || stdout.contains("budget") || stderr.contains("disk"),
        "failed sync push must report disk budget rejection; stdout={stdout}, stderr={stderr}"
    );
    assert_eq!(
        count_rows_from_file(&server_path, "big"),
        1,
        "failed sync push must not leave partially visible remote rows on the server"
    );

    let reopened = run_cli_script(&server_path, &[], "SHOW DISK_LIMIT\n.quit\n");
    assert!(reopened.status.success());
    let reopened_stdout = output_string(&reopened.stdout);
    assert!(
        reopened_stdout.contains(&configured_limit_bytes.to_string()),
        "server disk limit must persist across reopen after failed sync push: {reopened_stdout}"
    );
}
