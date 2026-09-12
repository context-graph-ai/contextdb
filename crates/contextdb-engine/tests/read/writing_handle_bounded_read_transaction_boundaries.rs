//! The edges of "a writing session's own bounded read sees its own open
//! transaction": what that read is charged for it, what it does with rows the
//! transaction changed rather than added, and the one shape it refuses.
//!
//! The read-your-writes case itself is pinned elsewhere. What is pinned here
//! is that seeing a transaction costs the read exactly what seeing committed
//! rows costs, that a staged DELETE and a staged UPDATE are honoured and not
//! only a staged INSERT, that a predicate and an ordering still hold across
//! the merged answer, and that a cursor -- which outlives the call that opened
//! it, while a transaction need not -- is refused while one is open.

use contextdb_core::read_contract::{
    ReadFailureDetail, ReadFailureKind, ReadFailureLimit, ReadLimits,
};
use contextdb_core::{Error, Value};
use contextdb_engine::{Database, QueryResult};
use std::collections::HashMap;

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn seeded() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE rows_under_test (id INTEGER PRIMARY KEY, label TEXT)",
        &HashMap::new(),
    )
    .expect("create the fixture table");
    db
}

fn insert(db: &Database, id: i64, label: &str) {
    db.execute(
        "INSERT INTO rows_under_test (id, label) VALUES ($id, $label)",
        &params([
            ("id", Value::Int64(id)),
            ("label", Value::Text(label.to_owned())),
        ]),
    )
    .expect("insert a row");
}

fn ids(result: &QueryResult) -> Vec<i64> {
    result
        .rows
        .iter()
        .map(|row| match row.first() {
            Some(Value::Int64(id)) => *id,
            other => panic!("expected an id-leading row, got {other:?}"),
        })
        .collect()
}

fn labels(result: &QueryResult) -> Vec<String> {
    result
        .rows
        .iter()
        .map(|row| match row.get(1) {
            Some(Value::Text(label)) => label.clone(),
            other => panic!("expected a label in the second column, got {other:?}"),
        })
        .collect()
}

fn crossed_limit(error: Error) -> ReadFailureLimit {
    let Error::ReadFailure(failure) = error else {
        panic!("expected a typed read refusal, got {error:?}");
    };
    assert_eq!(
        failure.kind(),
        ReadFailureKind::OwnerLimitExceeded,
        "a crossed ceiling is reported as OwnerLimitExceeded, got {failure:?}"
    );
    let ReadFailureDetail::OwnerLimitExceeded(detail) = failure.detail().clone() else {
        panic!("OwnerLimitExceeded carries its typed detail, got {failure:?}");
    };
    detail.limit
}

fn roomy() -> ReadLimits {
    ReadLimits::default()
}

#[test]
fn a_staged_delete_of_a_committed_row_hides_it_from_the_same_sessions_read() {
    let db = seeded();
    insert(&db, 1, "a");
    insert(&db, 2, "b");
    db.execute("BEGIN", &HashMap::new()).expect("begin");
    db.execute(
        "DELETE FROM rows_under_test WHERE id = $id",
        &params([("id", Value::Int64(1))]),
    )
    .expect("stage a delete of a committed row");

    let reader = db.read_session(roomy()).expect("read session");
    let seen = reader
        .execute(
            "SELECT id FROM rows_under_test ORDER BY id",
            &HashMap::new(),
        )
        .expect("read inside the open transaction");
    assert_eq!(
        ids(&seen),
        vec![2],
        "a row this transaction deleted is gone from its own read before it commits: {:?}",
        ids(&seen)
    );
}

#[test]
fn a_staged_update_of_a_committed_row_is_published_once_with_its_new_values() {
    let db = seeded();
    insert(&db, 1, "before");
    insert(&db, 2, "unchanged");
    db.execute("BEGIN", &HashMap::new()).expect("begin");
    db.execute(
        "UPDATE rows_under_test SET label = $label WHERE id = $id",
        &params([
            ("label", Value::Text("after".to_owned())),
            ("id", Value::Int64(1)),
        ]),
    )
    .expect("stage an update of a committed row");

    let reader = db.read_session(roomy()).expect("read session");
    let seen = reader
        .execute(
            "SELECT id, label FROM rows_under_test ORDER BY id",
            &HashMap::new(),
        )
        .expect("read inside the open transaction");
    assert_eq!(
        ids(&seen),
        vec![1, 2],
        "the updated row appears exactly once, not twice and not never: {:?}",
        ids(&seen)
    );
    assert_eq!(
        labels(&seen),
        vec!["after".to_owned(), "unchanged".to_owned()],
        "the version this read sees is the one the transaction staged: {:?}",
        labels(&seen)
    );
}

#[test]
fn a_predicate_still_selects_across_committed_and_staged_rows_alike() {
    let db = seeded();
    insert(&db, 1, "keep");
    insert(&db, 2, "drop");
    db.execute("BEGIN", &HashMap::new()).expect("begin");
    insert(&db, 3, "keep");
    insert(&db, 4, "drop");

    let reader = db.read_session(roomy()).expect("read session");
    let seen = reader
        .execute(
            "SELECT id FROM rows_under_test WHERE label = $label ORDER BY id",
            &params([("label", Value::Text("keep".to_owned()))]),
        )
        .expect("read inside the open transaction");
    assert_eq!(
        ids(&seen),
        vec![1, 3],
        "the predicate is applied to the transaction's own rows exactly as it is to committed \
         ones: {:?}",
        ids(&seen)
    );
}

#[test]
fn an_ordering_holds_across_committed_and_staged_rows() {
    let db = seeded();
    insert(&db, 2, "b");
    insert(&db, 4, "d");
    db.execute("BEGIN", &HashMap::new()).expect("begin");
    insert(&db, 1, "a");
    insert(&db, 3, "c");

    let reader = db.read_session(roomy()).expect("read session");
    let seen = reader
        .execute(
            "SELECT id FROM rows_under_test ORDER BY id",
            &HashMap::new(),
        )
        .expect("read inside the open transaction");
    assert_eq!(
        ids(&seen),
        vec![1, 2, 3, 4],
        "staged rows are ordered with the committed ones, not appended after them: {:?}",
        ids(&seen)
    );
}

#[test]
fn a_staged_rows_own_bytes_are_charged_against_the_memory_ceiling() {
    // One mebibyte is far above what this read needs for its own machinery
    // and far below one four-mebibyte staged value, so the two assertions
    // below cannot both hold unless the staged row itself is charged. The
    // first is what makes the second meaningful: without it a refusal could
    // simply be the ceiling being too small for any read at all.
    let ceiling = ReadLimits {
        memory: 1024 * 1024,
        ..ReadLimits::default()
    };
    let db = seeded();
    db.execute("BEGIN", &HashMap::new()).expect("begin");
    insert(&db, 1, "small");

    let reader = db.read_session(ceiling).expect("read session");
    let seen = reader
        .execute(
            "SELECT id FROM rows_under_test ORDER BY id",
            &HashMap::new(),
        )
        .expect("a small staged row fits under this ceiling, so the ceiling admits the read");
    assert_eq!(ids(&seen), vec![1]);

    db.execute(
        "INSERT INTO rows_under_test (id, label) VALUES ($id, $label)",
        &params([
            ("id", Value::Int64(2)),
            ("label", Value::Text("x".repeat(4 * 1024 * 1024))),
        ]),
    )
    .expect("stage a row larger than the ceiling");

    let refused = db
        .read_session(ceiling)
        .expect("read session")
        .execute(
            "SELECT id FROM rows_under_test ORDER BY id",
            &HashMap::new(),
        )
        .expect_err("a staged row four times the ceiling cannot be read under it");
    assert_eq!(
        crossed_limit(refused),
        ReadFailureLimit::Memory,
        "the ceiling a staged row crosses is the memory ceiling, the same one a committed row \
         of that size would cross"
    );
}

#[test]
fn a_cursor_is_refused_while_the_handle_has_a_transaction_open() {
    let db = seeded();
    insert(&db, 1, "a");
    db.execute("BEGIN", &HashMap::new()).expect("begin");
    insert(&db, 2, "b");

    let reader = db.read_session(roomy()).expect("read session");
    let refused = reader
        .open_cursor(
            "SELECT id FROM rows_under_test ORDER BY id",
            &HashMap::new(),
        )
        .err()
        .expect("a cursor cannot be opened inside an open transaction");
    let Error::ReadFailure(failure) = refused else {
        panic!("expected a typed read refusal, got {refused:?}");
    };
    assert_eq!(
        failure.kind(),
        ReadFailureKind::CursorTransactionActive,
        "the refusal is the one the reading surface already has for this: {failure:?}"
    );

    // And once the transaction is over, the same session opens one.
    db.execute("COMMIT", &HashMap::new()).expect("commit");
    let opened = reader
        .open_cursor(
            "SELECT id FROM rows_under_test ORDER BY id",
            &HashMap::new(),
        )
        .expect("with no transaction open the cursor opens as it always did");
    drop(opened);
}
