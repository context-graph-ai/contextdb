//! Deleting a row takes its vectors with it, and a guard can see them.
//!
//! A vector column lives in its own index, not in the relational row. Two
//! public row doors still act as though the row were the whole story.
//!
//! Deleting through the row API removes the row and leaves every vector
//! behind. Nothing reports an error; the row is simply gone from `SELECT`
//! and still there in every nearest-neighbour answer, so a caller who
//! deleted a record on request keeps serving it -- and the store keeps
//! paying for it.
//!
//! A row-condition guard is the "only if it still says what I read" check a
//! caller puts on an update. Compared against a row that has no vector in
//! it, a guard naming a vector column can never match, so the update it
//! protects never happens -- silently, since a guard that does not fire is
//! indistinguishable from a row that changed under you.
//!
//! Expected values come from the vector crate's own quantizer.

#![cfg(feature = "test-seams")]

use contextdb_core::table_meta::VectorQuantization;
use contextdb_core::{RowId, Value};
use contextdb_engine::Database;
use contextdb_vector::stored_vector_value;
use std::collections::HashMap;
use uuid::Uuid;

/// Width of every vector column below.
const DIMENSIONS: usize = 8;
/// The columns a row carries, and how each one is stored.
const VECTOR_COLUMNS: [(&str, Option<VectorQuantization>); 3] = [
    ("plain", None),
    ("compact", Some(VectorQuantization::SQ8)),
    ("tiny", Some(VectorQuantization::SQ4)),
];

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn id_of(ordinal: u128) -> Uuid {
    Uuid::from_u128(0x00D5_0000_0000_0000_0000_0000_0000_0000 + ordinal)
}

fn doomed_vector() -> Vec<f32> {
    vec![1.00, 0.90, 0.80, 0.70, 0.60, 0.50, 0.40, 0.30]
}

fn surviving_vector() -> Vec<f32> {
    vec![-1.00, -0.90, -0.80, -0.70, -0.60, -0.50, -0.40, -0.30]
}

/// What the store answers for a column: the quantizer's round trip where the
/// column is quantized, and the vector itself where it is not.
fn stored(vector: &[f32], quantization: Option<VectorQuantization>) -> Vec<f32> {
    match quantization {
        Some(quantization) => stored_vector_value(vector, quantization),
        None => vector.to_vec(),
    }
}

fn seeded(path: &std::path::Path) -> Database {
    let database = Database::open(path).expect("create the store");
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
    for (ordinal, vector) in [(1_u128, doomed_vector()), (2, surviving_vector())] {
        database
            .execute(
                "INSERT INTO points (id, label, plain, compact, tiny) \
                 VALUES ($id, $label, $v, $v, $v)",
                &params(vec![
                    ("id", Value::Uuid(id_of(ordinal))),
                    ("label", Value::Text(format!("row-{ordinal}"))),
                    ("v", Value::Vector(vector)),
                ]),
            )
            .expect("store a row carrying all three vector columns");
    }
    database
}

/// The `row_id` of the row with this key, which `delete_row` is addressed by.
fn row_id_of(database: &Database, id: Uuid) -> RowId {
    database
        .point_lookup("points", "id", &Value::Uuid(id), database.snapshot())
        .expect("point_lookup is served")
        .expect("the row is there before it is deleted")
        .row_id
}

/// Every id a nearest-neighbour search over `column` answers with.
fn searched_ids(database: &Database, column: &str, query: &[f32]) -> Vec<Uuid> {
    database
        .execute(
            &format!("SELECT id FROM points ORDER BY {column} <=> $query LIMIT 10"),
            &params(vec![("query", Value::Vector(query.to_vec()))]),
        )
        .expect("a vector search is served")
        .rows
        .iter()
        .map(|row| match row.first() {
            Some(Value::Uuid(id)) => *id,
            other => panic!("a search answers with an id-leading row, got {other:?}"),
        })
        .collect()
}

/// What `SHOW VECTOR_INDEXES` reports each column is holding.
fn vector_counts(database: &Database) -> HashMap<String, i64> {
    database
        .execute("SHOW VECTOR_INDEXES", &empty())
        .expect("the store describes its vector indexes")
        .rows
        .iter()
        .map(|row| {
            let column = match row.get(1) {
                Some(Value::Text(column)) => column.clone(),
                other => panic!("a vector-index row names its column, got {other:?}"),
            };
            let count = match row.get(4) {
                Some(Value::Int64(count)) => *count,
                other => panic!("a vector-index row counts its vectors, got {other:?}"),
            };
            (column, count)
        })
        .collect()
}

#[test]
fn a_row_deleted_through_the_row_api_takes_its_vectors_with_it() {
    let directory = tempfile::TempDir::new().expect("task-scoped row-door directory");
    let database = seeded(&directory.path().join("delete.db"));
    let doomed = id_of(1);
    let row_id = row_id_of(&database, doomed);
    let counts_before = vector_counts(&database);
    let held_before = database.accountant().usage().used;

    let tx = database.begin().expect("begin a writing transaction");
    database
        .delete_row(tx, "points", row_id)
        .expect("the row API deletes the row");
    database.commit(tx).expect("commit the delete");

    // The row is gone from the relational answer; the point of this pin is
    // that it is gone from the vector answers too.
    assert!(
        !database
            .execute(
                "SELECT id FROM points WHERE id = $id",
                &params(vec![("id", Value::Uuid(doomed))]),
            )
            .expect("a keyed read is served")
            .rows
            .iter()
            .any(|_| true),
        "the deleted row is gone from an ordinary read"
    );

    for (column, quantization) in VECTOR_COLUMNS {
        let answered = searched_ids(&database, column, &doomed_vector());
        assert!(
            !answered.contains(&doomed),
            "a search on {column} from the deleted row's own vector still answers with it \
             ({answered:?}); a caller who deleted a record on request keeps serving it, and the \
             store keeps paying for it"
        );
        let _ = quantization;
    }

    let counts_after = vector_counts(&database);
    for (column, _) in VECTOR_COLUMNS {
        let before = counts_before.get(column).copied().unwrap_or_default();
        let after = counts_after.get(column).copied().unwrap_or_default();
        assert_eq!(
            after,
            before - 1,
            "deleting one row leaves {column} holding one vector fewer, not {after} against \
             {before}"
        );
    }
    assert!(
        database.accountant().usage().used < held_before,
        "and the store releases what the deleted row's vectors were costing it"
    );

    // The row that was not deleted is untouched.
    assert!(
        searched_ids(&database, "compact", &surviving_vector()).contains(&id_of(2)),
        "the row nobody deleted is still there"
    );
    database.close().expect("the writer closes cleanly");
}

#[test]
fn a_row_condition_guard_sees_the_vector_the_store_holds() {
    let directory = tempfile::TempDir::new().expect("task-scoped row-door directory");
    let database = seeded(&directory.path().join("guard.db"));
    let subject = id_of(1);

    for (column, quantization) in VECTOR_COLUMNS {
        let held = stored(&doomed_vector(), quantization);

        let tx = database.begin().expect("begin a writing transaction");
        let matches = database
            .guard_row_conditions_in_tx(
                tx,
                "points",
                "id",
                &Value::Uuid(subject),
                &[(column.to_owned(), Value::Vector(held.clone()))],
            )
            .expect("a row-condition guard is served");
        let differs = database
            .guard_row_conditions_in_tx(
                tx,
                "points",
                "id",
                &Value::Uuid(subject),
                &[(column.to_owned(), Value::Vector(surviving_vector()))],
            )
            .expect("a row-condition guard is served");
        database.commit(tx).expect("close the guarding transaction");

        assert!(
            matches,
            "a guard naming {column}'s stored vector fires; one that never fires makes the \
             update it protects silently not happen, and a guard that did not fire looks exactly \
             like a row that changed under the caller"
        );
        assert!(
            !differs,
            "and a guard naming a vector the row does not hold does not fire on {column}"
        );
    }
    database.close().expect("the writer closes cleanly");
}
