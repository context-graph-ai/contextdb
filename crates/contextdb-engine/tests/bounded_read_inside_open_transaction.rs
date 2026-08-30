//! Contract: a writing session's own bounded read view (the in-process route
//! `Database::read_session` opens over a LIVE database handle) reads under
//! that handle's OPEN transaction -- read-your-writes -- while staying bounded
//! by the SAME ceilings as any other read.
//!
//! `Database::execute` itself keeps its uncapped library contract; the point
//! pinned here is that the bounded read VIEW over that same live handle sees
//! everything the handle's open transaction has done so far, refuses past its
//! own ceilings exactly as it would against committed state, and reports the
//! ceiling it crossed rather than the row count the statement actually
//! produced.
//!
//! Cursor-in-transaction is the implementer's recorded decision and is
//! deliberately NOT pinned here.

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

fn seeded_write_database() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE tx_rows (id INTEGER PRIMARY KEY, label TEXT)",
        &HashMap::new(),
    )
    .expect("create the transaction fixture table");
    db
}

fn insert_three_uncommitted_rows(db: &Database) {
    for (id, label) in [(1i64, "a"), (2, "b"), (3, "c")] {
        db.execute(
            "INSERT INTO tx_rows (id, label) VALUES ($id, $label)",
            &params([
                ("id", Value::Int64(id)),
                ("label", Value::Text(label.to_owned())),
            ]),
        )
        .expect("insert an uncommitted row on the writer handle");
    }
}

fn roomy_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 500,
        ..ReadLimits::default()
    }
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

fn expect_owner_limit(error: Error) -> contextdb_core::read_contract::OwnerLimitExceededDetail {
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
    detail
}

#[test]
fn a_writing_sessions_own_read_session_sees_its_own_uncommitted_transaction() {
    let db = seeded_write_database();
    db.execute("BEGIN", &HashMap::new())
        .expect("begin a transaction on the writer handle");
    insert_three_uncommitted_rows(&db);

    let reader = db
        .read_session(roomy_limits())
        .expect("an in-process read session over the live writer handle");
    let seen = reader
        .execute("SELECT id FROM tx_rows ORDER BY id", &HashMap::new())
        .expect(
            "the writing session's own read-session sees its own open transaction \
             (read-your-writes)",
        );
    assert_eq!(
        ids(&seen),
        vec![1, 2, 3],
        "three uncommitted inserts on the SAME live handle are visible to a read session \
         opened over that handle"
    );
}

#[test]
fn a_tight_result_rows_ceiling_still_refuses_uncommitted_rows_naming_the_ceiling_not_the_count() {
    let db = seeded_write_database();
    db.execute("BEGIN", &HashMap::new())
        .expect("begin a transaction on the writer handle");
    insert_three_uncommitted_rows(&db);

    let tight_reader = db
        .read_session(ReadLimits {
            result_rows: 2,
            ..ReadLimits::default()
        })
        .expect("an in-process read session with a two-row ceiling");
    let refused = tight_reader
        .execute("SELECT id FROM tx_rows ORDER BY id", &HashMap::new())
        .expect_err(
            "three uncommitted rows exceed a two-row ceiling exactly as three committed rows would",
        );

    let detail = expect_owner_limit(refused);
    assert_eq!(
        detail.limit,
        ReadFailureLimit::ResultRows,
        "the crossed ceiling is result_rows: {detail:?}"
    );
    assert_eq!(
        detail.value, 2,
        "the refusal names the CEILING that was crossed (2), never the row count the \
         statement actually produced (3): {detail:?}"
    );
}

#[test]
fn rollback_on_the_writer_handle_empties_what_its_own_read_session_sees() {
    let db = seeded_write_database();
    db.execute("BEGIN", &HashMap::new())
        .expect("begin a transaction on the writer handle");
    insert_three_uncommitted_rows(&db);

    let reader = db
        .read_session(roomy_limits())
        .expect("an in-process read session over the live writer handle");
    assert_eq!(
        ids(&reader
            .execute("SELECT id FROM tx_rows ORDER BY id", &HashMap::new())
            .expect("pre-rollback read sees the three uncommitted rows")),
        vec![1, 2, 3],
        "sanity: the rows are visible before rollback"
    );

    db.execute("ROLLBACK", &HashMap::new())
        .expect("roll back the transaction on the writer handle");

    let after_rollback = reader
        .execute("SELECT id FROM tx_rows ORDER BY id", &HashMap::new())
        .expect("reading after rollback succeeds and answers with nothing");
    assert!(
        ids(&after_rollback).is_empty(),
        "a rolled-back transaction's inserts are gone from the SAME read session that saw \
         them uncommitted: {:?}",
        ids(&after_rollback)
    );
}

#[test]
fn commit_makes_the_rows_visible_to_a_second_independent_read_session_on_the_same_store() {
    let db = seeded_write_database();
    db.execute("BEGIN", &HashMap::new())
        .expect("begin a fresh transaction on the writer handle");
    insert_three_uncommitted_rows(&db);
    db.execute("COMMIT", &HashMap::new())
        .expect("commit the transaction on the writer handle");

    // A SECOND, independent read session -- never the one that watched the
    // transaction while it was open -- opened fresh over the same live store.
    let second_reader = db
        .read_session(roomy_limits())
        .expect("a second, independent in-process read session over the same store");
    let seen = second_reader
        .execute("SELECT id FROM tx_rows ORDER BY id", &HashMap::new())
        .expect("a second independent read session sees the committed rows");
    assert_eq!(
        ids(&seen),
        vec![1, 2, 3],
        "once committed, the rows are visible to any reader of this store, not only the one \
         that held the transaction open"
    );
}

#[test]
fn a_tiny_work_ceiling_still_refuses_a_scan_of_uncommitted_rows() {
    let db = seeded_write_database();
    db.execute("BEGIN", &HashMap::new())
        .expect("begin a transaction on the writer handle");
    insert_three_uncommitted_rows(&db);

    let starved_reader = db
        .read_session(ReadLimits {
            work: 1,
            ..ReadLimits::default()
        })
        .expect("an in-process read session with a one-unit work ceiling");
    let refused = starved_reader
        .execute("SELECT id FROM tx_rows ORDER BY id", &HashMap::new())
        .expect_err("scanning three uncommitted rows crosses a work ceiling of one");

    let detail = expect_owner_limit(refused);
    assert_eq!(
        detail.limit,
        ReadFailureLimit::Work,
        "a writer's own uncommitted rows still cost work to scan, exactly like committed \
         rows would: {detail:?}"
    );
}

#[test]
fn a_tiny_memory_ceiling_still_refuses_an_unindexed_sort_of_uncommitted_rows() {
    let db = seeded_write_database();
    db.execute("BEGIN", &HashMap::new())
        .expect("begin a transaction on the writer handle");
    insert_three_uncommitted_rows(&db);

    let starved_reader = db
        .read_session(ReadLimits {
            memory: 1,
            ..ReadLimits::default()
        })
        .expect("an in-process read session with a one-byte memory ceiling");
    // Sorted by `label`, an unindexed column, so the read must build a real
    // temporary sort buffer rather than walk the primary-key index in order.
    let refused = starved_reader
        .execute("SELECT id FROM tx_rows ORDER BY label", &HashMap::new())
        .expect_err(
            "an unindexed sort over three uncommitted rows needs more than one byte of \
             temporary memory",
        );

    let detail = expect_owner_limit(refused);
    assert_eq!(
        detail.limit,
        ReadFailureLimit::Memory,
        "an unindexed sort over a writer's own uncommitted rows still charges the memory \
         ceiling: {detail:?}"
    );
}
