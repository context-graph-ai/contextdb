use super::common::*;
use std::fs;
use std::process::Command;
use tempfile::TempDir;

#[test]
fn f44_docker_image_for_the_server() {
    assert!(false, "requires special infrastructure");
}

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
    let tx = db.begin();
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

#[test]
fn f46_upgrade_from_version_n_to_n_plus_1_without_data_loss() {
    // Create a DB, write data, close, reopen. Today this uses the same binary
    // so it passes trivially. When the persistence format changes, this test
    // should fail unless a version marker and migration path exist.
    // A real version migration test requires a fixture from an older binary —
    // that will be added when a format version marker is implemented.
    let tmp = TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("versioned.db");

    let id = uuid::Uuid::from_u128(0x11111111_1111_1111_1111_111111111111);
    {
        let db = contextdb_engine::Database::open(&db_path).expect("open db");
        db.execute(
            "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT, score REAL)",
            &empty_params(),
        )
        .expect("create table");
        db.execute(
            "INSERT INTO items (id, name, score) VALUES ($id, $name, $score)",
            &params(vec![
                ("id", contextdb_core::Value::Uuid(id)),
                ("name", contextdb_core::Value::Text("before-upgrade".into())),
                ("score", contextdb_core::Value::Float64(42.5)),
            ]),
        )
        .expect("insert");
        db.close().expect("close");
    }

    let db2 = contextdb_engine::Database::open(&db_path).expect("reopen after upgrade");
    let row = db2
        .point_lookup(
            "items",
            "id",
            &contextdb_core::Value::Uuid(id),
            db2.snapshot(),
        )
        .expect("lookup");
    assert!(row.is_some(), "data must survive version upgrade");
    let row = row.unwrap();
    assert_eq!(
        row.values.get("name"),
        Some(&contextdb_core::Value::Text("before-upgrade".into())),
    );
    assert_eq!(
        row.values.get("score"),
        Some(&contextdb_core::Value::Float64(42.5)),
    );
}

#[test]
fn f47_cli_has_logging_debug_mode_for_troubleshooting() {
    ensure_release_binaries();
    let tmp = TempDir::new().expect("tempdir");
    let output = Command::new(cli_bin())
        .arg(tmp.path().join("f47.db"))
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

#[test]
fn f48_monitor_server_health_in_production() {
    assert!(false, "server has no health endpoint");
}
