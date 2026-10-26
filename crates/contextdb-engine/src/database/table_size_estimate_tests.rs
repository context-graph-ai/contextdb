use super::*;
use crate::Database;
use contextdb_core::Value;
use std::collections::HashMap;

/// Bigger than the resident projection keeps, so the answer has to come
/// from the store.
const WIDE_BODY: usize = 8 * 1024;
const ROWS: usize = 4;

fn seeded(path: &std::path::Path) -> Database {
    let database = Database::open(path).expect("claim a store to seed");
    database
        .execute(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            &HashMap::new(),
        )
        .expect("declare the measured table");
    for id in 0..ROWS {
        database
            .execute(
                "INSERT INTO notes (id, body) VALUES ($id, $body)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(id as i64)),
                    ("body".to_owned(), Value::Text("n".repeat(WIDE_BODY))),
                ]),
            )
            .expect("commit one wide body");
    }
    database
}

/// The size of a table is what it occupies, and an operator sizing a disk
/// acts on it. So a run of rows is admitted before it is inspected, and a
/// stored length that cannot be read is said out loud: the answer that
/// carries a reason fails with it, and the answer that carries none says
/// nothing rather than a number that is too small.
#[test]
fn a_table_size_that_cannot_be_read_is_never_reported_as_a_smaller_table() {
    let directory = tempfile::TempDir::new().expect("task-scoped size directory");
    let path = directory.path().join("sized.db");
    let mut database = seeded(&path);

    let healthy = database
        .table_size_estimates()
        .expect("a healthy store answers for its tables");
    let notes = healthy
        .iter()
        .find(|estimate| estimate.table == "notes")
        .expect("the measured table is answered for");
    assert_eq!(notes.row_count, ROWS as u64, "every live row is counted");
    assert!(
        notes.estimated_live_bytes >= (ROWS * WIDE_BODY) as u64,
        "the answer accounts for the bodies the file is holding: {notes:?}"
    );

    crate::persistence::arm_stored_relational_row_bytes_fault_for_test();
    let failed = database
        .table_size_estimates()
        .expect_err("a stored size that cannot be read stops the answer");
    assert!(
        failed
            .to_string()
            .contains("injected stored relational row size read failure"),
        "the caller is told what failed: {failed}"
    );

    crate::persistence::arm_stored_relational_row_bytes_fault_for_test();
    let failed = database
        .table_size_estimate("notes")
        .expect_err("the fallible single-table API reports the unreadable stored size");
    assert!(
        failed
            .to_string()
            .contains("injected stored relational row size read failure"),
        "the single-table caller is told what failed: {failed}"
    );

    // A run of rows is room, and room is admitted: a budget with none left
    // refuses the answer instead of building the whole table's lists.
    let used = database.declared_read_memory.retained_bytes_for_test();
    let original_reader = std::mem::replace(
        &mut database.declared_read_memory,
        crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(
            used.saturating_add(8),
        )
        .expect("admit the declared fixture root"),
    );
    let refused = database
        .table_size_estimates()
        .expect_err("sizing a table takes room it must be admitted for");
    assert!(
        matches!(refused, Error::MemoryBudgetExceeded { .. }),
        "the refusal is the typed memory one: {refused}"
    );
    database.declared_read_memory = original_reader;

    database.close().expect("release the store");
}
