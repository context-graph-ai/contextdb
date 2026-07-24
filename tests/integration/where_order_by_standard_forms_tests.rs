//! Standard-SQL WHERE/ORDER BY/SELECT-list forms that the engine currently
//! refuses or misclassifies.
//!
//! `eval_bool_expr` (`crates/contextdb-engine/src/executor.rs`) has no match
//! arm for a bare `Expr::Column` or a bare `Expr::Parameter` used directly as
//! a predicate — both fall through to the catch-all
//! `_ => Err(Error::PlanError("unsupported WHERE expression: ..."))`. Combined
//! with the predicate-error-swallowing bug (see
//! `predicate_evaluation_error_propagation_tests.rs`), a bare boolean
//! predicate today silently excludes every row instead of filtering on it —
//! this file pins the CORRECT end state (the predicate actually filters),
//! which only holds once the propagation fix and this form-support fix land
//! together.
//!
//! `resolve_sort_key_index` accepts only a literal `Expr::Column` sort key
//! (`let Expr::Column(column_ref) = &key.expr else { return
//! Err(Error::OrderByExpressionNotSupported); }`) — any other expression
//! form is refused even when the identical expression is already evaluated
//! for a SELECT list (`eval_function` supports `coalesce`, reachable from a
//! SELECT-list `Expr::FunctionCall`). This file extends ORDER BY to that
//! already-supported form. Arithmetic (`__add`/`__sub`/`__mul`/`__div`, the
//! desugaring of `+`/`-`/`*`/`/`) is DELIBERATELY NOT exercised here: the
//! pinned guard `order_by_non_column_expression_is_a_typed_error_not_silently_unsorted`
//! in `order_by_column_validation_tests.rs` requires `ORDER BY a + 1` to stay
//! an error, so widening ORDER BY to arithmetic would contradict an existing
//! green test that is out of scope for this file's fix.
//!
//! `lookup_query_result_column` (used by the SELECT-list projection
//! evaluator, `eval_project_expr`) resolves an unknown column via
//! `query_result_column_error`, which always returns `Error::PlanError` —
//! never `Error::ColumnNotFound` — unlike the WHERE/JOIN/ORDER BY paths that
//! already standardized on `ColumnNotFound` for the identical failure. A
//! bare `SELECT <unknown> FROM t` (no WHERE clause) reaches this path
//! because `validate_plan_columns`'s `PhysicalPlan::Project` arm only
//! recurses into its input and never validates the projection's own
//! expression list.

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

fn insert_flag_row(db: &Database, flag: bool, tag: &str) {
    db.execute(
        "INSERT INTO t (id, flag, tag) VALUES ($id, $flag, $tag)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("flag", Value::Bool(flag)),
            ("tag", Value::Text(tag.into())),
        ]),
    )
    .expect("INSERT t");
}

fn seed_flag_table(db: &Database) {
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, flag BOOLEAN, tag TEXT)",
        &empty(),
    )
    .expect("CREATE TABLE t");
}

// ============================================================================
// Bare boolean column as a full predicate. `WHERE flag` is
// standard SQL: it means `WHERE flag = TRUE`. Today `flag` (an
// `Expr::Column`) is not one of `eval_bool_expr`'s handled shapes, so
// evaluating it errors for every row, and that error is currently swallowed
// (see the predicate-propagation file) into "exclude every row" — the
// filter must instead actually select the TRUE rows.
// ============================================================================
#[test]
fn where_bare_boolean_column_filters_true_rows_only() {
    let db = Database::open_memory();
    seed_flag_table(&db);
    insert_flag_row(&db, true, "a");
    insert_flag_row(&db, false, "b");
    insert_flag_row(&db, true, "c");

    let r = db
        .execute("SELECT tag FROM t WHERE flag", &empty())
        .expect("a bare boolean column predicate is standard SQL and must succeed");
    let mut got: Vec<String> = r
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Text(s) => s.clone(),
            other => panic!("expected Value::Text, got {other:?}"),
        })
        .collect();
    got.sort();
    assert_eq!(
        got,
        vec!["a".to_string(), "c".to_string()],
        "WHERE flag must keep only the rows where flag = TRUE, got rows {:?}",
        r.rows
    );
}

// ============================================================================
// Negated bare boolean column. `WHERE NOT flag` must keep the
// FALSE rows. `eval_bool_expr`'s `UnaryOp::Not` arm already recurses through
// `eval_bool_expr(operand, ...)`, so this exercises the same missing
// bare-Column support one level down.
// ============================================================================
#[test]
fn where_negated_bare_boolean_column_filters_false_rows_only() {
    let db = Database::open_memory();
    seed_flag_table(&db);
    insert_flag_row(&db, true, "a");
    insert_flag_row(&db, false, "b");
    insert_flag_row(&db, true, "c");

    let r = db
        .execute("SELECT tag FROM t WHERE NOT flag", &empty())
        .expect("NOT of a bare boolean column predicate is standard SQL and must succeed");
    assert_eq!(
        r.rows,
        vec![vec![Value::Text("b".to_string())]],
        "WHERE NOT flag must keep only the row where flag = FALSE, got rows {:?}",
        r.rows
    );
}

// ============================================================================
// Boolean `$param` as a full predicate. A parameter carries a
// per-STATEMENT value, not a per-row one, so `$want = true` must select
// every row and `$want = false` must select none — this is a clean,
// data-independent probe of the same missing-bare-shape gap
// (`Expr::Parameter` is equally absent from `eval_bool_expr`'s match arms).
// ============================================================================
#[test]
fn where_boolean_parameter_true_selects_all_rows() {
    let db = Database::open_memory();
    seed_flag_table(&db);
    insert_flag_row(&db, true, "a");
    insert_flag_row(&db, false, "b");

    let r = db
        .execute(
            "SELECT tag FROM t WHERE $want",
            &params(vec![("want", Value::Bool(true))]),
        )
        .expect("a boolean $param predicate is standard SQL and must succeed");
    assert_eq!(
        r.rows.len(),
        2,
        "WHERE $want with want=true must select every row, got {:?}",
        r.rows
    );
}

#[test]
fn where_boolean_parameter_false_selects_no_rows() {
    let db = Database::open_memory();
    seed_flag_table(&db);
    insert_flag_row(&db, true, "a");
    insert_flag_row(&db, false, "b");

    let r = db
        .execute(
            "SELECT tag FROM t WHERE $want",
            &params(vec![("want", Value::Bool(false))]),
        )
        .expect("a boolean $param predicate is standard SQL and must succeed");
    assert_eq!(
        r.rows.len(),
        0,
        "WHERE $want with want=false must select no rows, got {:?}",
        r.rows
    );
}

// ============================================================================
// `WHERE NULL` positive control (must stay green through this
// change). A literal NULL predicate is standard SQL and means "match
// nothing" — an empty result, never a statement error. This case already
// happens to return an empty result today (via the predicate-evaluation
// swallow bug pinned in `predicate_evaluation_error_propagation_tests.rs`),
// so it is a REGRESSION FENCE: once predicate-evaluation errors start
// propagating, `WHERE NULL` must not flip from "silently empty for the
// wrong reason" to "loudly erroring for the wrong reason" —
// `Expr::Literal(Literal::Null)` must become a recognized `eval_bool_expr`
// shape (evaluating to `None`), not merely inherit the catch-all.
// ============================================================================
#[test]
fn where_null_literal_is_empty_result_never_an_error() {
    let db = Database::open_memory();
    seed_flag_table(&db);
    insert_flag_row(&db, true, "a");
    insert_flag_row(&db, false, "b");

    let r = db
        .execute("SELECT tag FROM t WHERE NULL", &empty())
        .expect("a literal NULL predicate must be an empty result, never a statement error");
    assert_eq!(
        r.rows.len(),
        0,
        "WHERE NULL must match no rows, got {:?}",
        r.rows
    );
}

// ============================================================================
// ORDER BY an already-SELECT-list-evaluated expression form.
// `COALESCE(...)` is evaluated today for a SELECT-list `Expr::FunctionCall`
// (`eval_function`'s `"coalesce"` arm) but `resolve_sort_key_index` refuses
// any ORDER BY key that is not a literal `Expr::Column`, so this identical
// expression is unsupported in ORDER BY. Fixture: `pref` is NULL on the
// first and third rows (falls back to `fallback`), set directly on the
// second — insertion order deliberately does not match the COALESCE order,
// so a no-op "sort" would be observably wrong.
// ============================================================================
#[test]
fn order_by_coalesce_expression_sorts_by_the_evaluated_value() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, pref INTEGER, fallback INTEGER, tag TEXT)",
        &empty(),
    )
    .expect("CREATE TABLE t");
    let insert = |pref: Option<i64>, fallback: i64, tag: &str| {
        let mut row = params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("fallback", Value::Int64(fallback)),
            ("tag", Value::Text(tag.into())),
        ]);
        row.insert(
            "pref".to_string(),
            pref.map(Value::Int64).unwrap_or(Value::Null),
        );
        db.execute(
            "INSERT INTO t (id, pref, fallback, tag) VALUES ($id, $pref, $fallback, $tag)",
            &row,
        )
        .expect("INSERT t");
    };
    // COALESCE(pref, fallback): row1 -> 30, row2 -> 5, row3 -> 10.
    insert(Some(30), 999, "row1");
    insert(None, 5, "row2");
    insert(None, 10, "row3");

    let r = db
        .execute(
            "SELECT tag FROM t ORDER BY COALESCE(pref, fallback)",
            &empty(),
        )
        .expect("ORDER BY COALESCE(...) is an already-evaluated SELECT-list form and must succeed");
    let got: Vec<String> = r
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Text(s) => s.clone(),
            other => panic!("expected Value::Text, got {other:?}"),
        })
        .collect();
    assert_eq!(
        got,
        vec!["row2".to_string(), "row3".to_string(), "row1".to_string()],
        "must sort ascending by COALESCE(pref, fallback) = [5, 10, 30], got {:?}",
        r.rows
    );
}

// ============================================================================
// SELECT-list unqualified unknown column, no WHERE clause.
// `validate_plan_columns`'s `PhysicalPlan::Project` arm only recurses into
// its input and never validates the projection list itself, so a bare
// `SELECT <unknown> FROM t` (nothing else to trip plan-time validation)
// reaches `lookup_query_result_column` at execution time, which reports
// `Error::PlanError("project column not found: ...")` — a different class
// than the WHERE/JOIN/ORDER BY paths' `Error::ColumnNotFound` for the exact
// same "this name is not real" failure.
// ============================================================================
#[test]
fn select_list_unknown_column_is_column_not_found_error() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .expect("CREATE TABLE t");
    db.execute(
        "INSERT INTO t (id, a) VALUES ($id, $a)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("a", Value::Int64(1)),
        ]),
    )
    .expect("INSERT t");

    let result = db.execute("SELECT nope FROM t", &empty());
    let err = result
        .expect_err("a SELECT-list reference to a nonexistent column must be an error, not Ok");
    match err {
        Error::ColumnNotFound { table, column } => {
            assert_eq!(table, "t", "ColumnNotFound.table mismatch");
            assert_eq!(column, "nope", "ColumnNotFound.column mismatch");
        }
        other => panic!(
            "expected Error::ColumnNotFound {{ table: \"t\", column: \"nope\" }}, \
             got {other:?} — the SELECT-list path must report the same error \
             class as WHERE/JOIN/ORDER BY do for an identical unknown-column \
             failure"
        ),
    }
}

// ============================================================================
// The same contract on an EMPTY table, ruling out a validator
// that only runs while materializing a real row (mirrors the equivalent
// empty-table cases in `where_clause_column_validation_tests.rs` and
// `order_by_column_validation_tests.rs`).
// ============================================================================
#[test]
fn select_list_unknown_column_on_empty_table_is_column_not_found_error() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .expect("CREATE TABLE t");
    // Deliberately no rows inserted.

    let result = db.execute("SELECT nope FROM t", &empty());
    let err = result.expect_err(
        "a SELECT-list reference to a nonexistent column must error even on an \
         empty table, not silently return zero rows",
    );
    match err {
        Error::ColumnNotFound { table, column } => {
            assert_eq!(table, "t");
            assert_eq!(column, "nope");
        }
        other => panic!("expected Error::ColumnNotFound, got {other:?}"),
    }
}

// ============================================================================
// Positive control (must stay green): a real SELECT-list column keeps
// working with no WHERE clause at all — the exact shape that skips
// `validate_predicate_columns` entirely (no predicate to validate) and so
// is the shape most likely to regress if the SELECT-list fix is too broad.
// ============================================================================
#[test]
fn select_list_valid_column_with_no_where_clause_still_works() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .expect("CREATE TABLE t");
    db.execute(
        "INSERT INTO t (id, a) VALUES ($id, $a)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("a", Value::Int64(42)),
        ]),
    )
    .expect("INSERT t");

    let r = db
        .execute("SELECT a FROM t", &empty())
        .expect("a real column with no WHERE clause must keep working");
    assert_eq!(r.rows, vec![vec![Value::Int64(42)]]);
}
