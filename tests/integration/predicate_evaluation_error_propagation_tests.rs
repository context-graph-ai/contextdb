//! A predicate that genuinely fails to EVALUATE (as opposed to one that reads
//! a NULL value) must surface as a statement error, never as a silently
//! empty/partial result.
//!
//! `row_matches(...)` already returns `Result<bool>` correctly — the NULL
//! case is folded to `Ok(false)` INSIDE `eval_bool_expr`, which is correct
//! SQL (`WHERE nope IS NULL` on a NULL value is a non-match, not an error).
//! The defect is at the four CALL sites
//! (`crates/contextdb-engine/src/executor.rs:2370` exec_delete,
//! `:2654` exec_update, `:4658` the indexed anchor-shape residual filter,
//! `:5117` materialize_rows/SELECT), which each do
//! `row_matches(r, w, params).unwrap_or(false)` — discarding a genuine `Err`
//! from `row_matches` and treating the row as merely non-matching. A
//! statement that hits this today never fails; it silently drops the
//! offending row (SELECT: a wrong, too-small result; DELETE/UPDATE: fewer
//! rows touched than the predicate actually calls for, with no signal).
//!
//! `WHERE -tag > 0` on a TEXT column is used as the evaluation-error probe
//! throughout: `eval_expr_value`'s `UnaryOp::Neg` arm
//! (`crates/contextdb-engine/src/executor.rs`, the row-context copy near
//! line 5296) returns `Error::PlanError("cannot negate non-numeric value")`
//! for any non-numeric operand, and nothing at plan time rejects `-tag` for
//! a real, existing TEXT column — the error can only surface once a row is
//! actually evaluated.

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
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, tag TEXT)",
        &empty(),
    )
    .expect("CREATE TABLE t");
}

fn insert_row_t(db: &Database, a: i64, tag: &str) -> Uuid {
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO t (id, a, tag) VALUES ($id, $a, $tag)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("a", Value::Int64(a)),
            ("tag", Value::Text(tag.into())),
        ]),
    )
    .expect("INSERT t");
    id
}

// ============================================================================
// SELECT. `-tag` cannot be negated (tag is TEXT on every row), so
// the predicate must fail to evaluate for every row in the table. Today that
// error is swallowed per-row at the `materialize_rows` call site
// (executor.rs:5117) and the statement wrongly succeeds with an empty
// result; it must instead fail the whole statement.
// ============================================================================
#[test]
fn select_predicate_evaluation_error_propagates_as_statement_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 1, "r1");
    insert_row_t(&db, 2, "r2");

    let result = db.execute("SELECT a FROM t WHERE -tag > 0", &empty());
    assert!(
        result.is_err(),
        "a predicate that fails to evaluate (negating a TEXT column) must be \
         a statement error, not a silently empty/wrong result: got {:?}",
        result
    );
}

// ============================================================================
// SELECT, mixed predicate. The left side of the AND
// (`a = 1`) is true for exactly one row, so a per-row swallow that only
// excludes the erroring row from the RESULT (rather than failing the whole
// statement) would silently return the OTHER row wrong-but-plausible —
// still empty here since the failing branch is reached for every row, but
// this shape independently guards that the error is not masked by a
// same-row true operand.
// ============================================================================
#[test]
fn select_compound_predicate_evaluation_error_propagates_as_statement_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 1, "r1");

    let result = db.execute("SELECT a FROM t WHERE a = 1 AND -tag > 0", &empty());
    assert!(
        result.is_err(),
        "an evaluation error on one AND operand must fail the statement even \
         when the other operand is true for a real row: got {:?}",
        result
    );
}

// ============================================================================
// DELETE. Today's swallow means `matched` silently excludes
// every row whose predicate errored, so the statement returns `Ok` with
// `rows_affected: 0` and the table is left untouched with no signal that
// anything was wrong with the predicate — indistinguishable from "nothing
// matched". It must instead fail loudly, and — because it fails — must
// touch nothing.
// ============================================================================
#[test]
fn delete_predicate_evaluation_error_propagates_as_statement_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 1, "r1");
    insert_row_t(&db, 2, "r2");

    let result = db.execute("DELETE FROM t WHERE -tag > 0", &empty());
    match result {
        Err(_) => {}
        Ok(r) => {
            let remaining = db
                .execute("SELECT a FROM t", &empty())
                .expect("count remaining rows")
                .rows
                .len();
            panic!(
                "DELETE FROM t WHERE -tag > 0 must fail with a statement error \
                 (negating a TEXT column cannot evaluate), never succeed \
                 silently: rows_affected={}, {remaining} of 2 rows remain. \
                 Full result: {r:?}",
                r.rows_affected,
            );
        }
    }
    let remaining = db
        .execute("SELECT a FROM t", &empty())
        .expect("count remaining rows after the refused DELETE")
        .rows
        .len();
    assert_eq!(
        remaining, 2,
        "a refused DELETE must not have removed any row"
    );
}

// ============================================================================
// UPDATE. Same contract as DELETE: a swallowed evaluation error
// must not let the statement report success while quietly rewriting zero
// (or the wrong subset of) rows.
// ============================================================================
#[test]
fn update_predicate_evaluation_error_propagates_as_statement_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 1, "r1");
    insert_row_t(&db, 2, "r2");

    let result = db.execute("UPDATE t SET a = 999 WHERE -tag > 0", &empty());
    match result {
        Err(_) => {}
        Ok(r) => {
            let rewritten = db
                .execute("SELECT a FROM t WHERE a = 999", &empty())
                .expect("count rows rewritten to a = 999")
                .rows
                .len();
            panic!(
                "UPDATE t SET a = 999 WHERE -tag > 0 must fail with a statement \
                 error, never succeed silently: rows_affected={}, {rewritten} \
                 rows now have a=999. Full result: {r:?}",
                r.rows_affected,
            );
        }
    }
    let rewritten = db
        .execute("SELECT a FROM t WHERE a = 999", &empty())
        .expect("count rows rewritten to a = 999 after the refused UPDATE")
        .rows
        .len();
    assert_eq!(
        rewritten, 0,
        "a refused UPDATE must not have rewritten any row"
    );
}

// ============================================================================
// Indexed anchor-shape path (executor.rs:4658). A UNIQUE-backed
// equality lookup on `uniq` plus a residual predicate that fails to
// evaluate must still fail the statement rather than silently returning the
// anchor row as though the residual filter had excluded it. Best-effort:
// this pins the observable statement-level contract; whether the planner
// actually takes the anchor-shape index path for this exact shape is an
// implementation detail this test does not assert on directly.
// ============================================================================
#[test]
fn select_predicate_evaluation_error_propagates_through_unique_indexed_lookup() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE u (id UUID PRIMARY KEY, uniq TEXT UNIQUE, tag TEXT)",
        &empty(),
    )
    .expect("CREATE TABLE u");
    db.execute(
        "INSERT INTO u (id, uniq, tag) VALUES ($id, $uniq, $tag)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("uniq", Value::Text("k1".to_string())),
            ("tag", Value::Text("r1".to_string())),
        ]),
    )
    .expect("INSERT u");

    let result = db.execute("SELECT id FROM u WHERE uniq = 'k1' AND -tag > 0", &empty());
    assert!(
        result.is_err(),
        "a residual predicate evaluation error after a unique-indexed \
         equality lookup must fail the statement, not silently drop the \
         anchor row: got {:?}",
        result
    );
}

// ============================================================================
// Positive control (must stay green): NULL-as-false is CORRECT SQL and is
// unchanged by this fix. `nope_flag` does not exist so this would normally
// be a plan-time ColumnNotFound (covered by
// `where_clause_column_validation_tests.rs`); this control instead reads a
// real, EXISTING column whose value is genuinely NULL, which is the case
// `eval_bool_expr` folds to `Ok(None)` (never an `Err`) — it must keep
// excluding the row without erroring.
// ============================================================================
#[test]
fn select_where_real_null_value_excludes_row_without_error() {
    let db = Database::open_memory();
    seed_t(&db);
    // `a` is left NULL on this row (INSERT omits it).
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO t (id, tag) VALUES ($id, $tag)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("tag", Value::Text("r1".into())),
        ]),
    )
    .expect("INSERT t with a left NULL");
    insert_row_t(&db, 5, "r2");

    let r = db
        .execute("SELECT tag FROM t WHERE a = 5", &empty())
        .expect("a real NULL value compared with `=` must exclude the row, not error");
    assert_eq!(
        r.rows,
        vec![vec![Value::Text("r2".to_string())]],
        "only the row with a = 5 must match; the NULL row is correctly excluded"
    );
}

// ============================================================================
// Qualifier resolution, single-table scope. `t2` is not the
// table being queried and is not declared as an alias anywhere in this
// statement (the query names only `t`, unaliased) — `check_predicate_columns_declared`
// (executor.rs `validate_predicate_columns`) only checks `column_ref.column`
// against the table's declared columns and never checks `column_ref.table`
// at all, so a bogus qualifier on a real column name passes plan-time
// validation; the row-level evaluator (`eval_expr_value`'s `Expr::Column`
// arm) then ALSO ignores `c.table` and resolves by bare name alone. Both
// layers must instead treat an unrecognized qualifier as ColumnNotFound.
// ============================================================================
#[test]
fn where_qualifier_naming_a_table_not_in_scope_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 1, "r1");

    // `t2` is not `t` and is not declared as an alias for `t` anywhere in
    // this statement — `a` is a real column, but not through this qualifier.
    let result = db.execute("SELECT a FROM t WHERE t2.a = 1", &empty());
    let err = result.expect_err(
        "a qualifier naming a table that is not in scope must be an error, \
         even though the bare column name it qualifies is real",
    );
    match err {
        Error::ColumnNotFound { column, .. } => {
            assert_eq!(column, "a");
        }
        other => panic!("expected Error::ColumnNotFound, got {other:?}"),
    }
}

// ============================================================================
// Qualifier resolution, JOIN ON-condition scope. The join's
// `condition` column list (executor.rs `join_column_lists`) leaves any
// column name UNIQUE to the left table bare (unqualified), even though the
// left table itself has a real name (`t`) — so `filter_column_is_resolvable`'s
// "a bare match with no qualifier of its own" fallback wrongly accepts ANY
// qualifier (real or bogus) for a bare-listed name. `badalias` names no
// relation in this query at all (only `t` and `s` are joined); `a` is a
// genuine, uniquely-left column, exactly the shape that stays bare in the
// condition list.
// ============================================================================
#[test]
fn join_on_qualifier_naming_a_table_not_in_scope_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t(&db);
    db.execute("CREATE TABLE s (id UUID PRIMARY KEY, x INTEGER)", &empty())
        .expect("CREATE TABLE s");
    insert_row_t(&db, 1, "r1");
    db.execute(
        "INSERT INTO s (id, x) VALUES ($id, $x)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("x", Value::Int64(7)),
        ]),
    )
    .expect("INSERT s");

    // `badalias` names neither `t` nor `s`. `a` is real on `t` and unique
    // across the join, so it is carried bare in the ON-condition column
    // list — the shape that lets a bogus qualifier slip through today.
    let result = db.execute(
        "SELECT t.a FROM t INNER JOIN s ON badalias.a = s.x",
        &empty(),
    );
    let err = result.expect_err(
        "an ON-condition qualifier naming a table that is not in this join \
         must be an error, even though the bare column name it qualifies is \
         real and unique on one side of the join",
    );
    match err {
        Error::ColumnNotFound { column, .. } => {
            assert_eq!(column, "a");
        }
        other => panic!("expected Error::ColumnNotFound, got {other:?}"),
    }
}

// ============================================================================
// Positive control (must stay green): a real alias correctly qualifying its
// own column keeps working — the fix must not start rejecting legitimate
// qualified references.
// ============================================================================
#[test]
fn where_qualifier_naming_the_actual_alias_still_works() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 1, "r1");
    insert_row_t(&db, 2, "r2");

    let r = db
        .execute("SELECT t1.a FROM t AS t1 WHERE t1.a = 1", &empty())
        .expect("a qualifier naming the query's own alias must keep working");
    assert_eq!(r.rows, vec![vec![Value::Int64(1)]]);
}

// ============================================================================
// Positive control (must stay green): a join's ON-condition qualified by
// the REAL joined table/alias keeps working.
// ============================================================================
#[test]
fn join_on_qualifier_naming_a_real_joined_table_still_works() {
    let db = Database::open_memory();
    seed_t(&db);
    db.execute("CREATE TABLE s (id UUID PRIMARY KEY, x INTEGER)", &empty())
        .expect("CREATE TABLE s");
    let id = insert_row_t(&db, 7, "r1");
    db.execute(
        "INSERT INTO s (id, x) VALUES ($id, $x)",
        &params(vec![("id", Value::Uuid(id)), ("x", Value::Int64(7))]),
    )
    .expect("INSERT s");

    let r = db
        .execute("SELECT t.a FROM t INNER JOIN s ON t.a = s.x", &empty())
        .expect("a real joined-table qualifier must keep working");
    assert_eq!(r.rows, vec![vec![Value::Int64(7)]]);
}

// ============================================================================
// A CTE-sourced SELECT's `WHERE` plans as a `PhysicalPlan::Filter` node over
// the CTE's materialized rows, a FIFTH call site that did
// `query_result_row_matches(...).unwrap_or(false)` — the same swallow the
// four `row_matches` sites above pin, just on the result-row family instead
// of the raw-row family. A predicate that fails to evaluate over a
// CTE-sourced SELECT silently returned an empty result instead of failing
// the statement.
// ============================================================================
#[test]
fn cte_sourced_select_predicate_evaluation_error_propagates_as_statement_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 1, "r1");

    let result = db.execute(
        "WITH x AS (SELECT a FROM t) SELECT a FROM x WHERE UPPER(a) = 'Z'",
        &empty(),
    );
    assert!(
        result.is_err(),
        "a predicate that fails to evaluate (UPPER is not an implemented \
         function) over a CTE-sourced SELECT must be a statement error, not \
         a silently empty result: got {:?}",
        result
    );
}

#[test]
fn cte_sourced_select_missing_parameter_propagates_as_statement_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 1, "r1");

    let result = db.execute(
        "WITH x AS (SELECT a FROM t) SELECT a FROM x WHERE a = $missing",
        &empty(),
    );
    assert!(
        result.is_err(),
        "a predicate referencing an unbound parameter over a CTE-sourced \
         SELECT must be a statement error, not a silently empty result: got {:?}",
        result
    );
}
