//! A store whose writer refused to save an allocator
//! table (its regional summary disagreed with its own words) closes with no
//! allocator table. What the engine sees on such a store is named here: a
//! direct read is refused with a typed "needs a writer" failure, the format
//! classification repairs it and answers "not legacy", and a writable open
//! repairs it, keeps every row and keeps its database identity.

use contextdb_core::{Error, Value, read_contract::ReadFailureKind};
use contextdb_engine::persistence::RedbPersistence;
use contextdb_engine::{Database, ReadSession};
use std::collections::HashMap;
use std::path::Path;

const ROWS: i64 = 300;

fn body(id: i64) -> String {
    format!("{id:04}-{}", "x".repeat(2000))
}

/// An engine store with enough rows that region 0's order-0 allocator
/// bitmap has a summary level, then closed and reopened as a raw redb store
/// whose summary gets one planted wrong bit; dropping the raw handle runs
/// its quick-repair commit, which must refuse to save the allocator.
/// Returns the store's database identity.
fn latched_store(path: &Path) -> u64 {
    let database = Database::open(path).expect("create store");
    database
        .execute(
            "CREATE TABLE rows (id INTEGER PRIMARY KEY, body TEXT)",
            &HashMap::new(),
        )
        .expect("commit schema");
    for id in 0..ROWS {
        database
            .execute(
                &format!("INSERT INTO rows VALUES ({id}, '{}')", body(id)),
                &HashMap::new(),
            )
            .expect("commit row");
    }
    let identity = database.database_identity_for_test();
    database.close().expect("close fixture owner");

    let raw = redb::Database::open(path).expect("open physical store");
    let planted = raw.plant_allocator_summary_mismatch_for_test();
    assert!(
        planted.is_some(),
        "fixture: region 0's order-0 allocator bitmap has no summary level to plant in"
    );
    // Drop runs the quick-repair commit that saves the allocator table.
    drop(raw);
    identity
}

fn assert_direct_read_needs_a_writer(path: &Path, context: &str) {
    match ReadSession::open(path) {
        Err(Error::ReadFailure(failure))
            if failure.kind() == ReadFailureKind::DirectReadRequiresWriter => {}
        Err(other) => {
            panic!("{context}: a direct read was refused with the wrong error: {other:?}")
        }
        Ok(_) => panic!(
            "{context}: a direct read opened a store whose allocator save was refused; \
             the refused allocator table was published"
        ),
    }
}

fn assert_rows(database: &Database, context: &str) {
    let result = database
        .execute("SELECT id, body FROM rows ORDER BY id", &HashMap::new())
        .unwrap_or_else(|error| panic!("{context}: rows unreadable: {error:?}"));
    let rows = result.rows();
    let expected: Vec<Vec<Value>> = (0..ROWS)
        .map(|id| vec![Value::Int64(id), Value::Text(body(id))])
        .collect();
    assert!(
        rows == expected,
        "{context}: {} rows read back, expected {ROWS} unchanged rows",
        rows.len()
    );
}

#[test]
fn a_store_whose_allocator_save_was_refused_repairs_on_the_next_writer_open() {
    let root = tempfile::tempdir().expect("task-scoped store");

    // The format classification is allowed to repair, and answers.
    let classified = root.path().join("classified.db");
    latched_store(&classified);
    assert_direct_read_needs_a_writer(&classified, "before classification");
    assert!(
        !RedbPersistence::is_legacy_format_store(&classified)
            .expect("classification repairs and answers"),
        "a current-format store whose allocator save was refused classified as legacy"
    );
    let file = ReadSession::open(&classified)
        .expect("direct read after the classification repaired the store");
    drop(file);

    // A writable open repairs it, keeps every row and its identity.
    let reopened = root.path().join("reopened.db");
    let identity = latched_store(&reopened);
    assert_direct_read_needs_a_writer(&reopened, "before the writable open");
    let recovered = Database::open(&reopened).expect("writable open repairs the store");
    assert_eq!(
        recovered.database_identity_for_test(),
        identity,
        "the repair changed the store's database identity"
    );
    assert_rows(&recovered, "after the writable open");
    recovered.close().expect("close recovered owner");
    let file = ReadSession::open(&reopened)
        .expect("direct read after the writable open repaired the store");
    drop(file);
}
