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
        &["--write", "--disk-limit", "4M"],
        "SHOW DISK_LIMIT;\n.quit\n",
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

/// Run the CLI over a fresh store with the given extra arguments and
/// environment, ask it what its disk ceiling is, and hand back what it said.
fn show_disk_limit(name: &str, extra_args: &[&str], env: &[(&str, &str)]) -> String {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, name);
    ensure_release_binaries();
    let mut command = std::process::Command::new(cli_bin());
    command.arg(&db_path).arg("--write").args(extra_args);
    for (key, value) in env {
        command.env(key, value);
    }
    let output = command
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
                .write_all(b"SHOW DISK_LIMIT;\n.quit\n")
                .expect("write stdin");
            child.wait_with_output()
        })
        .expect("CLI reports its disk ceiling");
    assert!(output.status.success());
    output_string(&output.stdout)
}

/// The startup disk ceiling is something the operator states on the command
/// line. The environment is not a behavior surface: a `CONTEXTDB_*` alias for
/// the same ceiling is not read, and setting one changes nothing and says
/// nothing -- which is what keeps a ceiling auditable from the invocation
/// alone, rather than from whatever the surrounding shell happened to export.
#[test]
fn a_db2_the_command_line_sets_the_startup_disk_ceiling_and_the_environment_does_not() {
    // 2M = 2097152 bytes.
    let stated = show_disk_limit("a_db2-stated.db", &["--disk-limit", "2M"], &[]);
    assert!(
        stated.contains("2097152"),
        "SHOW DISK_LIMIT must report the ceiling the command line stated: {stated}"
    );

    let from_environment = show_disk_limit(
        "a_db2-environment.db",
        &[],
        &[("CONTEXTDB_DISK_LIMIT", "2M")],
    );
    assert!(
        !from_environment.contains("2097152"),
        "an environment alias must not set the startup ceiling: {from_environment}"
    );
    assert!(
        from_environment.contains("none"),
        "a store nobody gave a ceiling reports it has none: {from_environment}"
    );
}

#[test]
fn a_db3_set_disk_limit_below_startup_ceiling_works() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "a_db3.db");
    let output = run_cli_script(
        &db_path,
        &["--write", "--disk-limit", "4M"],
        "SET DISK_LIMIT '1M';\nSHOW DISK_LIMIT;\n.quit\n",
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
        "SET DISK_LIMIT '4M';\n.quit\n",
    );
    assert_eq!(
        output.status.code(),
        Some(1),
        "CLI must exit cleanly with the definitive-error code on SET error, not crash"
    );
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
        &["--write"],
        "SET DISK_LIMIT '1M';\nSHOW DISK_LIMIT;\n.quit\n",
    );
    assert!(configured.status.success());
    let configured_stdout = output_string(&configured.stdout);
    assert!(
        configured_stdout.contains("1048576"),
        "SHOW DISK_LIMIT must report configured limit before restart: {configured_stdout}"
    );

    let reopened = run_cli_script(&db_path, &[], "SHOW DISK_LIMIT;\n.quit\n");
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
    let sync = start_sync_fixture().await;
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
        let configured_limit_bytes = 1024;
        db.execute("SET DISK_LIMIT '1K'", &std::collections::HashMap::new())
            .expect("set server disk limit");
        db.close().expect("close server db");
        configured_limit_bytes
    };

    let mut server = spawn_server(&server_path, "a_db6", &sync.bind_spec);
    let output = run_cli_script_allow_startup_failure_with_timeout(
        &edge_path,
        &[
            "--write",
            "--tenant-id",
            "a_db6",
            "--sync-endpoint",
            &sync.ticket,
        ],
        ".sync push\n.quit\n",
        std::time::Duration::from_secs(30),
    );
    stop_child(&mut server);

    let stdout = output_string(&output.stdout).to_lowercase();
    let stderr = output_string(&output.stderr).to_lowercase();
    assert!(
        !output.status.success(),
        "disk-budget rejection must make the push exit nonzero; stdout={stdout}, stderr={stderr}"
    );
    assert!(
        stdout.contains("disk budget exceeded") || stderr.contains("disk budget exceeded"),
        "failed sync push must report disk budget rejection; stdout={stdout}, stderr={stderr}"
    );
    assert_eq!(
        count_rows_from_file(&server_path, "big"),
        1,
        "failed sync push must not leave partially visible remote rows on the server"
    );

    let reopened = run_cli_script(&server_path, &["--write"], "SHOW DISK_LIMIT;\n.quit\n");
    assert!(reopened.status.success());
    let reopened_stdout = output_string(&reopened.stdout);
    assert!(
        reopened_stdout.contains(&configured_limit_bytes.to_string()),
        "server disk limit must persist across reopen after failed sync push: {reopened_stdout}"
    );
}
