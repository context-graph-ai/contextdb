//! Unknown-column validation is a PLAN-time contract, not merely an
//! execution-time one. Concretely:
//!
//! - `Database::explain(sql)` must itself refuse a statically-invalid query
//!   (`explain("SELECT a FROM t WHERE nope = 1")`) rather than parsing,
//!   planning, and returning `plan.explain()` with no validation pass at
//!   all.
//! - The JOIN/CTE `PhysicalPlan::Filter` arm must not execute its ENTIRE
//!   input (scanning the join's left side in full) before the
//!   column-existence check runs — a query that will ultimately be rejected
//!   must not do real scan work first.
//!
//! The schema-aware validation pass runs after physical planning but before
//! execution, and is also called from `Database::explain`. These tests pin
//! that contract from both angles: `explain` must refuse a
//! statically-invalid query without executing anything, and a real
//! `execute` of the same shape must refuse BEFORE doing the left-side scan
//! work a naive post-execution check would still do.

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

fn seed_t(db: &Database) {
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .expect("CREATE TABLE t");
}

fn seed_t_and_s(db: &Database) {
    seed_t(db);
    db.execute("CREATE TABLE s (id UUID PRIMARY KEY, x INTEGER)", &empty())
        .expect("CREATE TABLE s");
}

fn assert_explain_column_not_found(
    result: contextdb_core::Result<String>,
    expected_table: &str,
    expected_column: &str,
) {
    let err = result.expect_err(&format!(
        "explain() over a query referencing the nonexistent column `{expected_column}` must \
         itself fail with ColumnNotFound at plan time — never return a rendered plan for a \
         statically-invalid query"
    ));
    match err {
        Error::ColumnNotFound { table, column } => {
            assert_eq!(table, expected_table, "ColumnNotFound.table mismatch");
            assert_eq!(column, expected_column, "ColumnNotFound.column mismatch");
        }
        other => panic!(
            "expected Error::ColumnNotFound {{ table: {expected_table:?}, column: {expected_column:?} }} from explain(), got {other:?}"
        ),
    }
}

// ============================================================================
// `EXPLAIN` (Database::explain) over a bare single-table scan
// referencing an unknown column must itself fail with ColumnNotFound, not
// silently render a plan for an invalid query.
// ============================================================================
#[test]
fn explain_bare_scan_unknown_column_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t(&db);

    let result = db.explain("SELECT a FROM t WHERE nope = 1");
    assert_explain_column_not_found(result, "t", "nope");
}

// ============================================================================
// `EXPLAIN` over a JOIN whose WHERE references an unknown column
// must also fail with ColumnNotFound. This is the plan-time counterpart to
// the execution-time contract for the JOIN/CTE `Filter` case (see
// `where_clause_column_validation_tests.rs`) — `explain` must refuse it too,
// without ever building/running the join.
// ============================================================================
#[test]
fn explain_join_unknown_column_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t_and_s(&db);

    let result = db.explain("SELECT * FROM t INNER JOIN s ON t.id = s.id WHERE nope = 1");
    assert_explain_column_not_found(result, "t", "nope");
}

// ============================================================================
// `EXPLAIN` over a CTE-sourced SELECT whose outer WHERE references an
// unknown column must also fail with ColumnNotFound.
// ============================================================================
#[test]
fn explain_cte_unknown_column_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t(&db);

    let result = db.explain("WITH cte AS (SELECT a FROM t) SELECT a FROM cte WHERE nope = 1");
    assert_explain_column_not_found(result, "t", "nope");
}

// ============================================================================
// The Filter-path proof that validation precedes execution, not just
// "eventually errors": a JOIN whose WHERE references an unknown column must
// refuse WITHOUT scanning the join's left side.
//
// `Database::__rows_examined()` ("count of base rows the executor touched
// during the most recent query", already used elsewhere in this suite —
// see `indexed_scan_filter_tests.rs`) is the concrete, already-available
// instrumentation for this: it resets at the start of every `execute()`
// call, so its value after a call reports exactly what THAT query touched.
//
// It is not enough for `t` (5 rows, `s` empty) INNER JOIN ... WHERE
// nope = 1` to eventually return `Err(ColumnNotFound)` — plan-time
// validation must reject BEFORE any row is touched, so
// `__rows_examined()` must read 0. `PhysicalPlan::Filter` executing its
// whole input first and validating only afterward (leaving
// `__rows_examined()` at 5) is exactly the shape this test rules out.
// ============================================================================
#[test]
fn join_filter_validation_must_precede_left_side_execution() {
    let db = Database::open_memory();
    seed_t_and_s(&db);
    for a in 0..5i64 {
        db.execute(
            "INSERT INTO t (id, a) VALUES ($id, $a)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("a", Value::Int64(a)),
            ]),
        )
        .expect("INSERT t");
    }

    let result = db.execute(
        "SELECT * FROM t INNER JOIN s ON t.id = s.id WHERE nope = 1",
        &empty(),
    );
    match result {
        Err(Error::ColumnNotFound { table, column }) => {
            assert_eq!(table, "t", "ColumnNotFound.table mismatch");
            assert_eq!(column, "nope", "ColumnNotFound.column mismatch");
        }
        other => panic!(
            "expected Error::ColumnNotFound {{ table: \"t\", column: \"nope\" }}, got {other:?}"
        ),
    }
    assert_eq!(
        db.__rows_examined(),
        0,
        "plan-time validation must reject the query before scanning the join's left side \
         (`t`, 5 rows) at all — {} rows were examined, proving the scan ran before the \
         column-existence check instead of after it",
        db.__rows_examined()
    );
}

// ============================================================================
// Positive control: `EXPLAIN` over a query with no unknown columns must
// keep succeeding. Guards against validation that is too eager and starts
// refusing valid SQL.
// ============================================================================
#[test]
fn explain_valid_query_still_succeeds() {
    let db = Database::open_memory();
    seed_t(&db);

    let result = db.explain("SELECT a FROM t WHERE a = 1");
    assert!(
        result.is_ok(),
        "explain() over a query naming only real columns must still succeed: {result:?}"
    );
}
