//! `validate_plan_columns`'s `PhysicalPlan::Join` arm must validate the
//! join's own `condition` field — the `ON` clause itself — not merely
//! recursively validate the left and right sub-plans (the two table scans).
//! A `JOIN ... ON <unknown column>` must be a plan-time ColumnNotFound
//! error, exactly like an unknown column in a `Filter`'s WHERE clause.
//!
//! The tricky case is when NO row pair is ever evaluated (an empty side):
//! the executor's own per-pair check
//! (`query_result_row_matches(&combined, &columns, condition, params)?` in
//! the `Join` arm of `execute_plan`) propagates an error via `?` for a
//! genuinely-unknown `ON` column, but only from inside the `for right_row
//! in &right_result.rows` loop, which never runs at all when the right side
//! is empty. So plan-time validation must not be conditional on data
//! happening to exist to evaluate the condition against — the same
//! plan-time-vs-execution-time contract the `Filter` case enforces.
//! `Database::explain` on the identical query must refuse the same way,
//! never rendering a full `Join { .. }` plan for a statically-invalid query.

use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn seed_t_and_s(db: &Database) {
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .expect("CREATE TABLE t");
    db.execute("CREATE TABLE s (id UUID PRIMARY KEY, x INTEGER)", &empty())
        .expect("CREATE TABLE s");
}

fn insert_t(db: &Database, a: i64) -> Uuid {
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO t (id, a) VALUES ($id, $a)",
        &params(vec![("id", Value::Uuid(id)), ("a", Value::Int64(a))]),
    )
    .expect("INSERT t");
    id
}

fn insert_s(db: &Database, id: Uuid, x: i64) {
    db.execute(
        "INSERT INTO s (id, x) VALUES ($id, $x)",
        &params(vec![("id", Value::Uuid(id)), ("x", Value::Int64(x))]),
    )
    .expect("INSERT s");
}

// ============================================================================
// A JOIN whose `ON` clause references an unknown column, arranged so
// no row pair is ever evaluated (the right side, `s`, is empty): must error
// with ColumnNotFound at plan time, never silently return `Ok`.
//
// The empty-side arrangement is the load-bearing part of this fixture, not
// an implementation convenience: with rows on the right side, the executor's
// own per-pair evaluator already propagates a genuine error via `?` — the
// case this pins is specifically that an empty side must not skip
// evaluating the condition and make the error conditional on data happening
// to exist, which a real plan-time check must never be.
// ============================================================================
#[test]
fn join_on_unknown_column_with_empty_right_side_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t_and_s(&db);
    insert_t(&db, 0);
    insert_t(&db, 1);
    insert_t(&db, 2);
    // `s` deliberately stays empty — no row pair for the ON condition to be
    // evaluated against.

    let result = db.execute("SELECT * FROM t INNER JOIN s ON nope = 1", &empty());
    match result {
        Err(Error::ColumnNotFound { table, column }) => {
            assert_eq!(table, "t", "ColumnNotFound.table mismatch");
            assert_eq!(column, "nope", "ColumnNotFound.column mismatch");
        }
        other => panic!(
            "expected Error::ColumnNotFound {{ table: \"t\", column: \"nope\" }} from a JOIN \
             whose ON clause names an unknown column, got {other:?} — an empty right side \
             must not let the executor's per-pair ON check skip validation and silently \
             return Ok(0 rows)"
        ),
    }
    assert_eq!(
        db.__rows_examined(),
        0,
        "plan-time validation must reject the query before scanning the join's left side \
         (`t`, 3 rows) at all — {} rows were examined",
        db.__rows_examined()
    );

    // `explain` must refuse the identical query the same way, without ever
    // building the join.
    let explained = db.explain("SELECT * FROM t INNER JOIN s ON nope = 1");
    match explained {
        Err(Error::ColumnNotFound { table, column }) => {
            assert_eq!(table, "t", "explain ColumnNotFound.table mismatch");
            assert_eq!(column, "nope", "explain ColumnNotFound.column mismatch");
        }
        other => panic!(
            "expected explain() to also refuse the query with Error::ColumnNotFound, got {other:?}"
        ),
    }
}

// ============================================================================
// The same contract via LEFT JOIN with an empty right side: a null-extended
// row per left row (the documented LEFT JOIN behavior when the condition
// never matches) must never stand in for the ColumnNotFound error — an even
// more misleading failure shape than INNER JOIN's empty result would be,
// since it looks like a normal, successful outer join.
// ============================================================================
#[test]
fn left_join_on_unknown_column_with_empty_right_side_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t_and_s(&db);
    insert_t(&db, 0);
    insert_t(&db, 1);
    insert_t(&db, 2);

    let result = db.execute("SELECT * FROM t LEFT JOIN s ON nope = 1", &empty());
    match result {
        Err(Error::ColumnNotFound { table, column }) => {
            assert_eq!(table, "t", "ColumnNotFound.table mismatch");
            assert_eq!(column, "nope", "ColumnNotFound.column mismatch");
        }
        other => panic!(
            "expected Error::ColumnNotFound {{ table: \"t\", column: \"nope\" }} from a LEFT \
             JOIN whose ON clause names an unknown column, got {other:?} — it must refuse, \
             never silently return 3 null-extended rows (one per left row)"
        ),
    }
}

// ============================================================================
// Positive control: a JOIN with a genuinely valid `ON` condition, real
// rows on both sides, must keep working and return the correct joined row.
// Guards against validation that is too eager and starts refusing valid
// joins.
// ============================================================================
#[test]
fn join_on_valid_column_with_rows_still_works() {
    let db = Database::open_memory();
    seed_t_and_s(&db);
    let id1 = insert_t(&db, 1);
    let _id2 = insert_t(&db, 2);
    insert_s(&db, id1, 100);
    // `id2` deliberately has no matching `s` row — proves the join actually
    // filters on the condition, not just "any non-empty result passes".

    let result = db
        .execute(
            "SELECT t.a, s.x FROM t INNER JOIN s ON t.id = s.id",
            &empty(),
        )
        .expect("a JOIN with a valid ON condition over real rows must succeed");
    assert_eq!(
        result.rows,
        vec![vec![Value::Int64(1), Value::Int64(100)]],
        "must return exactly the one row whose id matches across both tables"
    );
}

// ============================================================================
// Regression locks (documentary; no new tests authored here) for the two
// false-refusal risks column validation on the `ON` clause must not
// introduce:
//
// - `ON TRUE` with real data on both sides must stay legal (a boolean
//   literal is a legal predicate anywhere a predicate is legal) — covered by
//   `hybrid_query_on_true_join_tests::inner_join_on_true_produces_full_cartesian_product`
//   (and its FALSE/LEFT-JOIN sibling tests in the same file).
// - A chained join whose second `ON` references the first join's own right
//   alias, resolved through a graph-traversal CTE's `Project`-derived output
//   naming (the documented Scenario 4 flagship query: `... LEFT JOIN
//   basis_entities be ON TRUE LEFT JOIN entities e ON e.id = be.b_id`) must
//   keep resolving that alias correctly — covered by
//   `hybrid_query_on_true_join_tests::scenario4_hybrid_query_returns_rows_once_traversal_has_data`.
// ============================================================================
