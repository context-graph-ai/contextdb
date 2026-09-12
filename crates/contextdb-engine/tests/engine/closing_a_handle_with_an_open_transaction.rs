//! Contract: closing a handle that still has a transaction open ends the
//! session cleanly and rolls that transaction back.
//!
//! A writing session is entitled to stop -- a piped script reaches end of
//! input, a process is asked to shut down -- without having said COMMIT or
//! ROLLBACK first. The handle owes the caller two things then: it must not
//! abort, and it must not keep the work the transaction had staged. Giving
//! back what those staged rows were holding is the engine's own bookkeeping
//! and must not be attempted as a public operation on a handle that is, by
//! then, deliberately closed.

use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn store_path(directory: &tempfile::TempDir) -> std::path::PathBuf {
    directory.path().join("open-transaction.db")
}

fn seed_and_leave_a_transaction_open(path: &std::path::Path) {
    let db = Database::open(path).expect("open the store for writing");
    db.execute(
        "CREATE TABLE staged (id INTEGER PRIMARY KEY, label TEXT)",
        &HashMap::new(),
    )
    .expect("create the fixture table");
    db.execute("BEGIN", &HashMap::new())
        .expect("begin a transaction");
    for (id, label) in [(1_i64, "a"), (2, "b")] {
        db.execute(
            "INSERT INTO staged (id, label) VALUES ($id, $label)",
            &params([
                ("id", Value::Int64(id)),
                ("label", Value::Text(label.to_owned())),
            ]),
        )
        .expect("stage an uncommitted insert");
    }
    // No COMMIT and no ROLLBACK: the session simply ends here.
    db.close().expect("closing with a transaction still open");
}

#[test]
fn closing_with_an_inserting_transaction_still_open_succeeds_and_keeps_none_of_it() {
    let directory = tempfile::tempdir().expect("task-scoped store directory");
    let path = store_path(&directory);
    seed_and_leave_a_transaction_open(&path);

    let reopened = Database::open(&path).expect("reopen the store the closed session left");
    let seen = reopened
        .execute("SELECT id FROM staged ORDER BY id", &HashMap::new())
        .expect("read the table the abandoned transaction wrote to");
    assert!(
        seen.rows.is_empty(),
        "an abandoned transaction's inserts are rolled back by the close that ended it: {:?}",
        seen.rows
    );
    reopened.close().expect("close the reopened store");
}

#[test]
fn dropping_a_handle_with_an_inserting_transaction_still_open_also_ends_cleanly() {
    let directory = tempfile::tempdir().expect("task-scoped store directory");
    let path = store_path(&directory);
    {
        let db = Database::open(&path).expect("open the store for writing");
        db.execute(
            "CREATE TABLE staged (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("create the fixture table");
        db.execute("BEGIN", &HashMap::new())
            .expect("begin a transaction");
        db.execute(
            "INSERT INTO staged (id) VALUES ($id)",
            &params([("id", Value::Int64(7))]),
        )
        .expect("stage an uncommitted insert");
        // Dropped, never closed: teardown has to survive the same shape.
    }

    let reopened = Database::open(&path).expect("reopen the store the dropped handle left");
    let seen = reopened
        .execute("SELECT id FROM staged", &HashMap::new())
        .expect("read the table the abandoned transaction wrote to");
    assert!(
        seen.rows.is_empty(),
        "a dropped handle's open transaction keeps nothing either: {:?}",
        seen.rows
    );
    reopened.close().expect("close the reopened store");
}
