use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use std::collections::HashMap;
use tempfile::TempDir;

fn empty_params() -> HashMap<String, Value> {
    HashMap::new()
}

#[test]
fn persisted_disk_limit_rejects_sync_budget_before_mutation_after_reopen() {
    let tempdir = TempDir::new().expect("create temporary database directory");
    let database_path = tempdir.path().join("disk-limit.db");

    let database = Database::open(&database_path).expect("open database");
    database
        .execute(
            "CREATE TABLE big (id UUID PRIMARY KEY, payload TEXT)",
            &empty_params(),
        )
        .expect("create table");
    database
        .execute(
            "INSERT INTO big (id, payload) VALUES ('00000000-0000-0000-0000-000000000001', $payload)",
            &HashMap::from([("payload".to_owned(), Value::Text("prime".repeat(1024)))]),
        )
        .expect("prime table");

    // The cap is deliberately below the already-primed file, so reopen must
    // preserve it and every subsequent write preflight must refuse.
    let configured_limit_bytes = 1024;
    database
        .execute("SET DISK_LIMIT '1K'", &empty_params())
        .expect("persist disk limit");
    database.close().expect("close configured database");

    let database = Database::open(&database_path).expect("reopen database");
    assert_eq!(database.disk_limit(), Some(configured_limit_bytes));
    let current_bytes = database
        .disk_file_size()
        .expect("read reopened database size");
    assert!(
        configured_limit_bytes <= current_bytes,
        "the reopened file must already be at or beyond its configured budget: configured={configured_limit_bytes}, current={current_bytes}"
    );

    match database.check_disk_budget("sync_pull") {
        Err(Error::DiskBudgetExceeded {
            operation,
            current_bytes: observed_current_bytes,
            budget_limit_bytes,
            ..
        }) => {
            assert_eq!(operation, "sync_pull");
            assert_eq!(observed_current_bytes, current_bytes);
            assert_eq!(budget_limit_bytes, configured_limit_bytes);
        }
        other => panic!("expected typed sync preflight refusal, got {other:?}"),
    }

    assert_eq!(
        database
            .scan("big", database.snapshot())
            .expect("scan primed table")
            .len(),
        1,
        "the rejected preflight must not mutate the primed table"
    );
    database.close().expect("close reopened database");
}
