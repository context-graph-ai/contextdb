//! A WHERE predicate on a nonexistent column must be a plan-time error,
//! never a silent Ok result, on SELECT *and* on the DELETE/UPDATE write
//! paths.
//!
//! Standard SQL: an unknown column reference in a predicate is an ERROR. The
//! engine already gets this right for `CREATE INDEX ... (bad_col)`
//! (`Error::ColumnNotFound`, see `indexed_scan_filter_tests.rs` DDL04/DDL07b) —
//! every WHERE path must mirror that same error class instead of silently
//! returning `Ok`. On SELECT that means a wrong empty (or, worse, wrong
//! non-empty) result; on DELETE/UPDATE it's worse still — a missing column's
//! `IS NULL` check is vacuously true for every row, so the write path
//! silently touches the ENTIRE table instead of refusing.

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
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER, tag TEXT)",
        &empty(),
    )
    .expect("CREATE TABLE t");
}

fn insert_row_t(db: &Database, a: i64, b: i64, tag: &str) -> Uuid {
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO t (id, a, b, tag) VALUES ($id, $a, $b, $tag)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("a", Value::Int64(a)),
            ("b", Value::Int64(b)),
            ("tag", Value::Text(tag.into())),
        ]),
    )
    .expect("INSERT t");
    id
}

fn assert_column_not_found(
    result: contextdb_core::Result<contextdb_engine::database::QueryResult>,
    expected_table: &str,
    expected_column: &str,
) {
    let err = result.expect_err(&format!(
        "WHERE reference to nonexistent column `{expected_column}` must be an error, not Ok"
    ));
    match err {
        Error::ColumnNotFound { table, column } => {
            assert_eq!(table, expected_table, "ColumnNotFound.table mismatch");
            assert_eq!(column, expected_column, "ColumnNotFound.column mismatch");
        }
        other => panic!(
            "expected Error::ColumnNotFound {{ table: {expected_table:?}, column: {expected_column:?} }}, got {other:?}"
        ),
    }
}

// ============================================================================
// SELECT ... WHERE <nonexistent column> = 'x' on a real table with
// rows must fail with ColumnNotFound, never return Ok(0 rows) — an empty
// result must never be a silent stand-in for an unknown column.
// ============================================================================
#[test]
fn select_where_nonexistent_column_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 1, 100, "r1");
    insert_row_t(&db, 2, 200, "r2");

    let result = db.execute("SELECT a FROM t WHERE nope = 'x'", &empty());
    assert_column_not_found(result, "t", "nope");
}

// ============================================================================
// Positive control: a real, existing column in WHERE must keep working
// normally.
// ============================================================================
#[test]
fn select_where_valid_column_still_works() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 1, 100, "r1");
    insert_row_t(&db, 2, 200, "r2");

    let r = db
        .execute("SELECT a, b, tag FROM t WHERE a = 1", &empty())
        .expect("SELECT with a valid WHERE column must succeed");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        r.rows[0],
        vec![
            Value::Int64(1),
            Value::Int64(100),
            Value::Text("r1".to_string())
        ]
    );
}

// ============================================================================
// Compound predicate, bad column on the RIGHT of AND. The left side
// (`a = 1`) is true for an existing row, so a partial/row-level-only
// validator that only checks reached branches must still catch this —
// predicate column references are validated at plan time, independent of
// which branches would short-circuit.
// ============================================================================
#[test]
fn compound_and_with_bad_right_side_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 1, 100, "r1");

    let result = db.execute("SELECT a FROM t WHERE a = 1 AND nope = 'x'", &empty());
    assert_column_not_found(result, "t", "nope");
}

// ============================================================================
// Compound predicate, bad column on the LEFT of OR. The right side
// (`a = 1`) is true for an existing row, which would make an OR predicate
// evaluate to true row-by-row while a naive short-circuit could skip the bad
// left side entirely — plan-time validation must still catch it.
// ============================================================================
#[test]
fn compound_or_with_bad_left_side_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 1, 100, "r1");

    let result = db.execute("SELECT a FROM t WHERE nope = 'x' OR a = 1", &empty());
    assert_column_not_found(result, "t", "nope");
}

// ============================================================================
// Qualified/aliased column reference: `t1.nope` where `t1` is a valid
// alias for `t` but `nope` does not exist. The error must still name the
// physical table (`t`), matching the CREATE INDEX ColumnNotFound convention
// (which always reports the real table name, never an alias).
// ============================================================================
#[test]
fn aliased_reference_to_nonexistent_column_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 1, 100, "r1");

    let result = db.execute("SELECT t1.a FROM t AS t1 WHERE t1.nope = 'x'", &empty());
    assert_column_not_found(result, "t", "nope");
}

// ============================================================================
// The same unknown-column contract on the DELETE write path. `ghost IS NULL`
// on a column that isn't declared on `t` would evaluate the missing column
// as `Value::Null`, making `IS NULL` vacuously true for EVERY row — that
// would silently delete the entire table instead of returning a wrong
// empty/partial answer. It must be a plan-time ColumnNotFound error instead,
// exactly as CREATE INDEX and SELECT already require.
// ============================================================================
#[test]
fn delete_where_nonexistent_column_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 1, 100, "r1");
    insert_row_t(&db, 2, 200, "r2");
    insert_row_t(&db, 3, 300, "r3");

    let result = db.execute("DELETE FROM t WHERE ghost IS NULL", &empty());

    match result {
        Err(Error::ColumnNotFound { table, column }) => {
            assert_eq!(table, "t", "ColumnNotFound.table mismatch");
            assert_eq!(column, "ghost", "ColumnNotFound.column mismatch");
        }
        Ok(r) => {
            let remaining = db
                .execute("SELECT a FROM t", &empty())
                .expect("count remaining rows in `t`")
                .rows
                .len();
            panic!(
                "DELETE FROM t WHERE ghost IS NULL must fail with \
                 Error::ColumnNotFound {{ table: \"t\", column: \"ghost\" }}, never succeed \
                 silently: rows_affected={}, only {remaining} of 3 rows remain in `t` — \
                 an `IS NULL` check on a column that doesn't exist must never be treated \
                 as vacuously true for every row and wipe the ENTIRE table instead of \
                 refusing. Full result: {r:?}",
                r.rows_affected,
            );
        }
        Err(other) => panic!(
            "expected Error::ColumnNotFound {{ table: \"t\", column: \"ghost\" }}, got {other:?}"
        ),
    }
}

// ============================================================================
// The same unknown-column contract on the UPDATE write path: a vacuously-true
// `ghost IS NULL` must never silently rewrite every row in the table instead
// of refusing with ColumnNotFound.
// ============================================================================
#[test]
fn update_where_nonexistent_column_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 1, 100, "r1");
    insert_row_t(&db, 2, 200, "r2");
    insert_row_t(&db, 3, 300, "r3");

    let result = db.execute("UPDATE t SET b = 999 WHERE ghost IS NULL", &empty());

    match result {
        Err(Error::ColumnNotFound { table, column }) => {
            assert_eq!(table, "t", "ColumnNotFound.table mismatch");
            assert_eq!(column, "ghost", "ColumnNotFound.column mismatch");
        }
        Ok(r) => {
            let rewritten = db
                .execute("SELECT a FROM t WHERE b = 999", &empty())
                .expect("count rows rewritten to b = 999")
                .rows
                .len();
            panic!(
                "UPDATE t SET b = 999 WHERE ghost IS NULL must fail with \
                 Error::ColumnNotFound {{ table: \"t\", column: \"ghost\" }}, never succeed \
                 silently: rows_affected={}, {rewritten} of 3 rows in `t` now have b=999 — \
                 an `IS NULL` check on a column that doesn't exist must never be treated \
                 as vacuously true for every row and rewrite EVERY row in the table \
                 instead of refusing. Full result: {r:?}",
                r.rows_affected,
            );
        }
        Err(other) => panic!(
            "expected Error::ColumnNotFound {{ table: \"t\", column: \"ghost\" }}, got {other:?}"
        ),
    }
}

// ============================================================================
// The same unknown-column reference against an EMPTY table (no rows
// inserted at all). This is the case that pins genuine PLAN-time validation:
// a validator that only checks column existence while evaluating a row (i.e.
// runs the check inside the per-row evaluator) never runs on an empty table
// — there are no rows to evaluate against — so it would wrongly return
// `Ok([])` here even while correctly erroring once a row exists.
// ============================================================================
#[test]
fn select_where_nonexistent_column_on_empty_table_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t(&db);
    // Deliberately no rows inserted.

    let result = db.execute("SELECT a FROM t WHERE nope = 'x'", &empty());
    assert_column_not_found(result, "t", "nope");
}

// ============================================================================
// Genuine AND short-circuit: the left operand (`a = 0`) is FALSE for
// every row (the one row present has `a = 1`), so a lazy left-to-right AND
// evaluator would never evaluate the bad right operand at all. Unlike the
// bad-right-of-AND case above (where the left operand is true and the bad
// operand IS reached), this proves validation cannot be a side effect of
// evaluating the bad branch — it must happen independently of whether any
// branch would be reached.
// ============================================================================
#[test]
fn and_short_circuit_left_false_never_reaches_bad_right_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 1, 100, "r1");

    let result = db.execute("SELECT a FROM t WHERE a = 0 AND nope = 'x'", &empty());
    assert_column_not_found(result, "t", "nope");
}

// ============================================================================
// Genuine OR short-circuit: the left operand (`a = 1`) is TRUE for
// the one row present, so a lazy left-to-right OR evaluator would never
// evaluate the bad right operand. Unlike the bad-left-of-OR case above (bad
// operand on the left, so it IS reached first), this is the shape that
// actually tests reached-branch-only validation would miss.
// ============================================================================
#[test]
fn or_short_circuit_left_true_never_reaches_bad_right_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 1, 100, "r1");

    let result = db.execute("SELECT a FROM t WHERE a = 1 OR nope = 'x'", &empty());
    assert_column_not_found(result, "t", "nope");
}

// ============================================================================
// A non-binary predicate shape: `NOT (...)`. A validator that
// recurses only through `Expr::BinaryOp` would miss the unknown column
// wrapped in a `UnaryOp::Not`.
// ============================================================================
#[test]
fn not_wrapped_reference_to_nonexistent_column_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 1, 100, "r1");

    let result = db.execute("SELECT a FROM t WHERE NOT (nope = 'x')", &empty());
    assert_column_not_found(result, "t", "nope");
}

// ============================================================================
// A non-binary predicate shape: `IS NULL`. This is a worse wrong-answer than
// equality if left unguarded: a missing column would read as `Value::Null`
// everywhere, so `nope IS NULL` would be vacuously TRUE for every row — a
// form that would silently return every row (a wrong non-empty answer), not
// just an empty one.
// ============================================================================
#[test]
fn is_null_reference_to_nonexistent_column_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 1, 100, "r1");

    let result = db.execute("SELECT a FROM t WHERE nope IS NULL", &empty());
    assert_column_not_found(result, "t", "nope");
}

// ============================================================================
// A non-binary predicate shape: `IN (list)`.
// ============================================================================
#[test]
fn in_list_reference_to_nonexistent_column_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 1, 100, "r1");

    let result = db.execute("SELECT a FROM t WHERE nope IN (1, 2, 3)", &empty());
    assert_column_not_found(result, "t", "nope");
}

// ============================================================================
// A non-binary predicate shape: `LIKE`.
// ============================================================================
#[test]
fn like_reference_to_nonexistent_column_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 1, 100, "r1");

    let result = db.execute("SELECT a FROM t WHERE nope LIKE '%x%'", &empty());
    assert_column_not_found(result, "t", "nope");
}

// ============================================================================
// A non-binary predicate shape: the unknown column as a FUNCTION
// ARGUMENT (`COALESCE(nope, 'z')`). A validator that only walks
// `Expr::BinaryOp`/`Expr::Column` at the top level, without recursing into
// `Expr::FunctionCall` args, would miss this.
// ============================================================================
#[test]
fn function_argument_reference_to_nonexistent_column_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 1, 100, "r1");

    let result = db.execute("SELECT a FROM t WHERE COALESCE(nope, 'z') = 'z'", &empty());
    assert_column_not_found(result, "t", "nope");
}

// ============================================================================
// A second table and a different missing-column name, to rule out
// any validator that is accidentally keyed to the specific fixture table
// `t` or the specific fixture column name `nope`.
// ============================================================================
#[test]
fn second_table_and_column_name_is_column_not_found_error() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE orders (id UUID PRIMARY KEY, qty INTEGER, sku TEXT)",
        &empty(),
    )
    .expect("CREATE TABLE orders");
    db.execute(
        "INSERT INTO orders (id, qty, sku) VALUES ($id, $qty, $sku)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("qty", Value::Int64(5)),
            ("sku", Value::Text("sku1".to_string())),
        ]),
    )
    .expect("INSERT orders");

    let result = db.execute("SELECT qty FROM orders WHERE ghost2 = 'x'", &empty());
    assert_column_not_found(result, "orders", "ghost2");
}

// ============================================================================
// A qualified column reference across a JOIN: `s.b` where `b` EXISTS
// on `t` (the OTHER table in the join) but not on `s`. This pins WHICH
// physical table a qualified missing reference names — a validator that
// checks "does ANY table in scope declare this column" while ignoring the
// qualifier would wrongly accept this, since `b` is a real column on `t`.
//
// A WHERE clause following a JOIN is planned as a `PhysicalPlan::Filter`
// over the join result, not a `PhysicalPlan::Scan { filter }`, so
// column-reference validation must cover the `Filter` branch too, not only
// `Scan` (see the unqualified case below, which proves this is a
// validation-must-not-be-skipped contract, not merely a qualifier-
// resolution nicety).
// ============================================================================
#[test]
fn qualified_reference_across_join_names_the_correct_table() {
    let db = Database::open_memory();
    seed_t(&db);
    db.execute("CREATE TABLE s (id UUID PRIMARY KEY, x INTEGER)", &empty())
        .expect("CREATE TABLE s");
    let id = insert_row_t(&db, 1, 100, "r1");
    db.execute(
        "INSERT INTO s (id, x) VALUES ($id, $x)",
        &params(vec![("id", Value::Uuid(id)), ("x", Value::Int64(7))]),
    )
    .expect("INSERT s");

    let result = db.execute(
        "SELECT * FROM t INNER JOIN s ON t.id = s.id WHERE s.b = 1",
        &empty(),
    );
    // `b` exists on `t`, not on `s` — the error must name `s`, never `t`.
    assert_column_not_found(result, "s", "b");
}

// ============================================================================
// An UNQUALIFIED, entirely-nonexistent column (`totallybogus`, which
// exists on NEITHER joined table) in a JOIN's WHERE clause. Proves the
// contract above is not merely "qualifier resolution must be correct" but
// that JOIN WHERE validation must never be skipped altogether.
// ============================================================================
#[test]
fn unqualified_reference_in_join_where_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t(&db);
    db.execute("CREATE TABLE s (id UUID PRIMARY KEY, x INTEGER)", &empty())
        .expect("CREATE TABLE s");
    let id = insert_row_t(&db, 1, 100, "r1");
    db.execute(
        "INSERT INTO s (id, x) VALUES ($id, $x)",
        &params(vec![("id", Value::Uuid(id)), ("x", Value::Int64(7))]),
    )
    .expect("INSERT s");

    let result = db.execute(
        "SELECT * FROM t INNER JOIN s ON t.id = s.id WHERE totallybogus = 1",
        &empty(),
    );
    let err = result.expect_err(
        "a JOIN's WHERE clause referencing a column that exists on NEITHER \
         joined table must fail with ColumnNotFound — the WHERE clause after \
         a JOIN is planned as a post-join Filter node, and column validation \
         must cover that node too, never silently returning Ok",
    );
    match err {
        Error::ColumnNotFound { column, .. } => {
            assert_eq!(column, "totallybogus");
        }
        other => panic!("expected ColumnNotFound, got {other:?}"),
    }
}

// ============================================================================
// A SELECT sourced from a plain SQL CTE (no JOIN at all) with a bad
// column in its OUTER WHERE. Same contract as the unqualified-join case
// above: a CTE-sourced FROM plans its outer WHERE as a
// `PhysicalPlan::Filter`, and that must be checked too, not only a direct
// single-table `Scan { filter }`.
// ============================================================================
#[test]
fn cte_sourced_select_where_nonexistent_column_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 1, 100, "r1");

    let result = db.execute(
        "WITH cte AS (SELECT a FROM t) SELECT a FROM cte WHERE nope = 'x'",
        &empty(),
    );
    let err = result.expect_err(
        "a CTE-sourced SELECT's outer WHERE referencing an unknown column \
         must fail with ColumnNotFound — the outer WHERE is planned as a \
         post-CTE Filter node, and column validation must cover it too, \
         never silently returning Ok",
    );
    match err {
        Error::ColumnNotFound { column, .. } => {
            assert_eq!(column, "nope");
        }
        other => panic!("expected ColumnNotFound, got {other:?}"),
    }
}
