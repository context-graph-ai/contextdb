use super::common::*;
use std::fs;
use std::process::Command;
use tempfile::TempDir;

/// I copied my database file to a backup location, opened the copy, and all 1,000 rows were still there.
#[test]
fn f45_backup_and_restore_a_database() {
    let tmp = TempDir::new().expect("tempdir");
    let original = tmp.path().join("original.db");
    let backup = tmp.path().join("backup.db");
    let db = contextdb_engine::Database::open(&original).expect("open original db");
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &empty_params(),
    )
    .expect("create table");
    let tx = db.begin_or_panic();
    for _ in 0..1_000 {
        db.insert_row(
            tx,
            "t",
            params(vec![
                ("id", contextdb_core::Value::Uuid(uuid::Uuid::new_v4())),
                ("name", contextdb_core::Value::Text("backup".into())),
            ]),
        )
        .expect("insert row");
    }
    db.commit(tx).expect("commit");
    db.close().expect("close original db");
    fs::copy(&original, &backup).expect("copy backup");
    let restored = contextdb_engine::Database::open(&backup).expect("open backup");
    assert_eq!(query_count(&restored, "SELECT count(*) FROM t"), 1_000);
}

/// I set RUST_LOG=debug and ran the CLI, and it printed debug-level logs to stderr so I can troubleshoot issues.
#[test]
fn f47_cli_has_logging_debug_mode_for_troubleshooting() {
    ensure_release_binaries();
    let tmp = TempDir::new().expect("tempdir");
    let output = Command::new(cli_bin())
        .arg(tmp.path().join("f47.db"))
        .arg("--write")
        .env("RUST_LOG", "debug")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .expect("run CLI with RUST_LOG");
    assert!(output.status.success());
    assert!(
        output_string(&output.stderr)
            .to_lowercase()
            .contains("debug")
    );
}

/// I hit the server's health endpoint, and it told me whether the server is alive and functioning.
#[test]
#[ignore = "server has no health endpoint yet"]
fn f48_monitor_server_health_in_production() {}
