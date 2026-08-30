//! The row APIs a caller writes against answer a quantized column too.
//!
//! `upsert_row`, `scan_filter` and `point_lookup` are the doors a program
//! uses when it is not writing SQL. A quantized column lives only in its
//! vector index, so a door that hands back the relational row and nothing
//! else hands back a column that is empty. Nothing raises: the caller reads
//! `NULL`, writes it down, and every decision built on that embedding is
//! quietly wrong -- and a `scan_filter` predicate that tests the vector
//! simply never matches, so the rows disappear from the answer instead.
//!
//! The same is true of the write door. An `upsert_row` that inserts a vector
//! has to make it searchable, an update has to replace the old one rather
//! than leave both, a genuine no-op has to leave the store exactly as it was,
//! and an upsert refused part-way has to leave nothing behind.
//!
//! Expected values come from the vector crate's own quantizer.

#![cfg(feature = "test-seams")]

use contextdb_core::table_meta::VectorQuantization;
use contextdb_core::{UpsertResult, Value};
use contextdb_engine::Database;
use contextdb_vector::stored_vector_value;
use std::collections::HashMap;
use uuid::Uuid;

/// Width of every vector column below.
const DIMENSIONS: usize = 8;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn row_id(ordinal: u128) -> Uuid {
    Uuid::from_u128(0x00C4_0000_0000_0000_0000_0000_0000_0000 + ordinal)
}

fn first_vector() -> Vec<f32> {
    vec![1.00, 0.80, 0.60, 0.40, 0.20, 0.00, -0.20, -0.40]
}

fn second_vector() -> Vec<f32> {
    vec![-0.40, -0.20, 0.00, 0.20, 0.40, 0.60, 0.80, 1.00]
}

/// What the quantizer makes of a vector, computed here rather than read back
/// from the engine under test.
fn expected(vector: &[f32], quantization: VectorQuantization) -> Vec<f32> {
    stored_vector_value(vector, quantization)
}

fn declare_table(database: &Database) {
    database
        .execute(
            &format!(
                "CREATE TABLE points (\
                 id UUID PRIMARY KEY, \
                 label TEXT, \
                 plain VECTOR({DIMENSIONS}), \
                 compact VECTOR({DIMENSIONS}) WITH (quantization = 'SQ8'), \
                 tiny VECTOR({DIMENSIONS}) WITH (quantization = 'SQ4'))"
            ),
            &empty(),
        )
        .expect("declare one ordinary and two quantized vector columns");
}

/// The values one row is upserted with.
fn row_values(id: Uuid, label: &str, vector: &[f32]) -> HashMap<String, Value> {
    let mut values = HashMap::new();
    values.insert("id".to_owned(), Value::Uuid(id));
    values.insert("label".to_owned(), Value::Text(label.to_owned()));
    values.insert("plain".to_owned(), Value::Vector(vector.to_vec()));
    values.insert("compact".to_owned(), Value::Vector(vector.to_vec()));
    values.insert("tiny".to_owned(), Value::Vector(vector.to_vec()));
    values
}

fn upsert(database: &Database, id: Uuid, label: &str, vector: &[f32]) -> UpsertResult {
    let tx = database.begin().expect("begin a writing transaction");
    let outcome = database
        .upsert_row(tx, "points", "id", row_values(id, label, vector))
        .expect("the row API accepts a row carrying quantized columns");
    database.commit(tx).expect("commit the upserted row");
    outcome
}

/// What the nearest-neighbour search answers for the quantized column.
fn searched_vector(database: &Database, order_by: &str, column: &str) -> Vec<f32> {
    let answered = database
        .execute(
            &format!("SELECT {column} FROM points ORDER BY {order_by} <=> $query LIMIT 1"),
            &params(vec![("query", Value::Vector(first_vector()))]),
        )
        .expect("a vector search over the upserted rows is served");
    match answered.rows.first().and_then(|row| row.first()) {
        Some(Value::Vector(vector)) => vector.clone(),
        other => panic!(
            "a search answers {column} with a vector; it answered {other:?}, and a caller told \
             NULL keeps the row and compares against it without learning the embedding is gone"
        ),
    }
}

/// What `point_lookup` answers for the quantized column.
fn looked_up_vector(database: &Database, id: Uuid, column: &str) -> Value {
    let row = database
        .point_lookup("points", "id", &Value::Uuid(id), database.snapshot())
        .expect("point_lookup is served")
        .unwrap_or_else(|| panic!("point_lookup finds the row it was given the key of: {id}"));
    row.values.get(column).cloned().unwrap_or(Value::Null)
}

#[test]
fn an_inserting_upsert_makes_the_quantized_vector_searchable_and_readable() {
    let directory = tempfile::TempDir::new().expect("task-scoped row-api directory");
    let database = Database::open(directory.path().join("insert.db")).expect("create the store");
    declare_table(&database);

    assert_eq!(
        upsert(&database, row_id(1), "first", &first_vector()),
        UpsertResult::Inserted
    );

    assert_eq!(
        searched_vector(&database, "compact", "compact"),
        expected(&first_vector(), VectorQuantization::SQ8),
        "a row inserted through the row API is searchable by its quantized column"
    );
    assert_eq!(
        looked_up_vector(&database, row_id(1), "compact"),
        Value::Vector(expected(&first_vector(), VectorQuantization::SQ8)),
        "point_lookup answers the quantized column of a row inserted through the row API"
    );
    assert_eq!(
        looked_up_vector(&database, row_id(1), "tiny"),
        Value::Vector(expected(&first_vector(), VectorQuantization::SQ4)),
        "point_lookup answers every quantized column, not only the first"
    );
    database.close().expect("the writer closes cleanly");
}

#[test]
fn an_updating_upsert_replaces_the_vector_and_never_leaves_the_old_one() {
    let directory = tempfile::TempDir::new().expect("task-scoped row-api directory");
    let database = Database::open(directory.path().join("update.db")).expect("create the store");
    declare_table(&database);
    assert_eq!(
        upsert(&database, row_id(1), "first", &first_vector()),
        UpsertResult::Inserted
    );

    assert_eq!(
        upsert(&database, row_id(1), "first", &second_vector()),
        UpsertResult::Updated
    );

    let new_vector = expected(&second_vector(), VectorQuantization::SQ8);
    let old_vector = expected(&first_vector(), VectorQuantization::SQ8);
    assert_eq!(
        looked_up_vector(&database, row_id(1), "compact"),
        Value::Vector(new_vector.clone()),
        "an updating upsert replaces the quantized column with the vector it was given"
    );
    // Search from the OLD vector: if the old entry were still in the index it
    // would be the nearest thing to itself, so this is what says it is gone.
    let nearest_to_old = database
        .execute(
            "SELECT compact FROM points ORDER BY compact <=> $query LIMIT 1",
            &params(vec![("query", Value::Vector(first_vector()))]),
        )
        .expect("a search from the replaced vector is served");
    assert_eq!(
        nearest_to_old.rows.first().and_then(|row| row.first()),
        Some(&Value::Vector(new_vector)),
        "the replaced vector is gone from the index; a search from it finds only the new one, \
         not the {old_vector:?} it replaced"
    );
    database.close().expect("the writer closes cleanly");
}

#[test]
fn a_no_op_upsert_leaves_the_store_exactly_as_it_was() {
    let directory = tempfile::TempDir::new().expect("task-scoped row-api directory");
    let database = Database::open(directory.path().join("noop.db")).expect("create the store");
    declare_table(&database);
    assert_eq!(
        upsert(&database, row_id(1), "first", &first_vector()),
        UpsertResult::Inserted
    );
    let held_before = database.accountant().usage().used;

    // The same row, the same values: nothing to do.
    let outcome = upsert(&database, row_id(1), "first", &first_vector());

    assert_eq!(
        outcome,
        UpsertResult::NoOp,
        "an upsert of the values already stored is a no-op"
    );
    assert_eq!(
        database.accountant().usage().used,
        held_before,
        "a no-op upsert stores no second copy of the vector it was handed"
    );
    assert_eq!(
        looked_up_vector(&database, row_id(1), "compact"),
        Value::Vector(expected(&first_vector(), VectorQuantization::SQ8)),
        "and the row still answers with the vector it had"
    );
    database.close().expect("the writer closes cleanly");
}

#[test]
fn a_refused_upsert_leaves_nothing_staged_and_nothing_charged() {
    let directory = tempfile::TempDir::new().expect("task-scoped row-api directory");
    let database = Database::open(directory.path().join("refused.db")).expect("create the store");
    declare_table(&database);
    assert_eq!(
        upsert(&database, row_id(1), "first", &first_vector()),
        UpsertResult::Inserted
    );
    let held_before = database.accountant().usage().used;

    // A ceiling this upsert cannot fit under. How tight that has to be is the
    // engine's business, so it is found by narrowing rather than guessed: the
    // arm is about what a REFUSED upsert leaves behind, and it can only say
    // that once one has actually been refused.
    let mut refused = None;
    for divisor in [1_u64, 2, 4, 16, 64] {
        database
            .set_memory_limit(Some(held_before / divisor as usize))
            .expect("declare a ceiling");
        let tx = database.begin().expect("begin a writing transaction");
        let outcome = database.upsert_row(
            tx,
            "points",
            "id",
            row_values(row_id(2), "second", &second_vector()),
        );
        let _ = database.rollback(tx);
        if outcome.is_err() {
            refused = Some(outcome);
            break;
        }
    }
    database
        .set_memory_limit(None)
        .expect("lift the ceiling again");
    let refused = refused.unwrap_or_else(|| {
        panic!(
            "an upsert carrying a vector is refused under some ceiling at or below the {held_before}              bytes already held; none of the ceilings tried refused it, so either the vector it              carries is charged to nothing or the ceiling is not consulted on this path"
        )
    });

    assert!(
        refused.is_err(),
        "an upsert that cannot place its vector is refused rather than half-applied: {refused:?}"
    );
    assert!(
        database
            .point_lookup("points", "id", &Value::Uuid(row_id(2)), database.snapshot())
            .expect("point_lookup is served")
            .is_none(),
        "the refused row is not in the store"
    );
    assert_eq!(
        database.accountant().usage().used,
        held_before,
        "and nothing it charged for is still held"
    );
    assert_eq!(
        looked_up_vector(&database, row_id(1), "compact"),
        Value::Vector(expected(&first_vector(), VectorQuantization::SQ8)),
        "the row that was already there is untouched"
    );
    database.close().expect("the writer closes cleanly");
}

#[test]
fn scan_filter_sees_the_quantized_vector_in_its_predicate_and_its_rows() {
    let directory = tempfile::TempDir::new().expect("task-scoped row-api directory");
    let database = Database::open(directory.path().join("scan.db")).expect("create the store");
    declare_table(&database);
    upsert(&database, row_id(1), "first", &first_vector());
    upsert(&database, row_id(2), "second", &second_vector());

    let wanted = expected(&second_vector(), VectorQuantization::SQ8);
    let matched = database
        .scan_filter("points", database.snapshot(), &|row| {
            row.values.get("compact") == Some(&Value::Vector(wanted.clone()))
        })
        .expect("scan_filter is served");

    assert_eq!(
        matched.len(),
        1,
        "a predicate testing the quantized column matches the row that holds that vector; a \
         predicate that never sees the vector matches nothing and the rows simply vanish from \
         the answer"
    );
    assert_eq!(
        matched[0].values.get("id"),
        Some(&Value::Uuid(row_id(2))),
        "and it is the right row"
    );
    assert_eq!(
        matched[0].values.get("compact"),
        Some(&Value::Vector(wanted)),
        "the row scan_filter hands back carries the vector its predicate matched on"
    );
    database.close().expect("the writer closes cleanly");
}

#[test]
fn the_row_apis_answer_the_same_way_on_a_reopened_store() {
    let directory = tempfile::TempDir::new().expect("task-scoped row-api directory");
    let path = directory.path().join("reopened.db");
    let database = Database::open(&path).expect("create the store");
    declare_table(&database);
    upsert(&database, row_id(1), "first", &first_vector());
    database.close().expect("the writer closes cleanly");

    let reopened = Database::open(&path).expect("reopen the store");
    assert_eq!(
        looked_up_vector(&reopened, row_id(1), "compact"),
        Value::Vector(expected(&first_vector(), VectorQuantization::SQ8)),
        "point_lookup answers the quantized column of a row loaded from the file"
    );
    assert_eq!(
        searched_vector(&reopened, "compact", "compact"),
        expected(&first_vector(), VectorQuantization::SQ8),
        "and the reopened store's rows are still searchable by that column"
    );

    let wanted = expected(&first_vector(), VectorQuantization::SQ8);
    let matched = reopened
        .scan_filter("points", reopened.snapshot(), &|row| {
            row.values.get("compact") == Some(&Value::Vector(wanted.clone()))
        })
        .expect("scan_filter is served on a reopened store");
    assert_eq!(
        matched.len(),
        1,
        "a predicate testing the quantized column matches on a reopened store too"
    );
    reopened.close().expect("the reader closes cleanly");
}
