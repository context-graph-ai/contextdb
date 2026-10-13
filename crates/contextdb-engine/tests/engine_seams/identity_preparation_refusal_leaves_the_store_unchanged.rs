//! "Typed refusals remain effective": when MEMORY_LIMIT refuses the growth of
//! a commit's content-identity mutations, that commit is refused inside its
//! still-open write transaction and the store is exactly as it was. A refusal
//! never drops one identity mutation and lets the rest of the commit land.
use contextdb_core::{Error, Value};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;
use std::path::Path;

fn open(path: &Path) -> Database {
    Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the task-scoped writer")
}
fn rows(db: &Database) -> Vec<Vec<Value>> {
    db.execute("SELECT id FROM items ORDER BY id", &HashMap::new())
        .unwrap()
        .rows
}

#[test]
fn identity_preparation_refusal_leaves_the_store_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("refusal.db");
    let db = open(&path);
    db.execute("CREATE TABLE items (id INT PRIMARY KEY)", &HashMap::new())
        .unwrap();
    for id in 0..3 {
        db.execute(&format!("INSERT INTO items VALUES ({id})"), &HashMap::new())
            .unwrap();
    }
    let ddl_before = db.__durable_ddl_for_test().unwrap();
    let lsn_before = db.current_lsn();

    let refused = db.__flush_received_noop_ddl_for_test(true);
    assert!(
        matches!(
            &refused,
            Err(Error::MemoryBudgetExceeded { subsystem, operation, .. })
                if subsystem == "persistence" && operation == "prepare_commit"
        ),
        "a refused identity growth refuses the flush: {refused:?}"
    );
    assert_eq!(
        db.__durable_ddl_for_test().unwrap(),
        ddl_before,
        "the refused flush wrote no DDL history"
    );
    assert_eq!(db.current_lsn(), lsn_before);
    assert_eq!(
        rows(&db),
        vec![
            vec![Value::Int64(0)],
            vec![Value::Int64(1)],
            vec![Value::Int64(2)]
        ]
    );
    db.close().unwrap();
    drop(db);

    // The reopened store is the one from before the refusal and still accepts work.
    let db = open(&path);
    assert_eq!(db.__durable_ddl_for_test().unwrap(), ddl_before);
    assert_eq!(rows(&db).len(), 3);
    db.execute("INSERT INTO items VALUES (3)", &HashMap::new())
        .unwrap();
    assert_eq!(rows(&db).len(), 4);
    let ddl_after_insert = db.__durable_ddl_for_test().unwrap().len();

    // Control: the same flush unrefused reaches the site and writes exactly
    // its one DDL history entry.
    db.__flush_received_noop_ddl_for_test(false)
        .expect("the unrefused flush commits");
    assert_eq!(
        db.__durable_ddl_for_test().unwrap().len(),
        ddl_after_insert + 1
    );
}
