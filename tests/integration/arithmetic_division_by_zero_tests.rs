//! Dividing by zero — a literal `0`, or a column/expression that evaluates
//! to `0` — is a typed statement error (standard-SQL "division by zero"
//! wording), never a process crash and never a silently wrong result.
//! Integer division, in particular, is refused before it is attempted:
//! dividing by zero is undefined behavior a CPU traps on, and an embedded
//! consumer cannot tolerate the whole process going down over a query. The
//! same refusal covers `i64::MIN / -1` — the one other integer division
//! that cannot be represented — and float division, which would otherwise
//! silently produce `inf`/`NaN` rather than erroring. Every door arithmetic
//! reaches is covered: the SELECT list, WHERE, and ORDER BY.
//!
//! NULL semantics: only a literal or evaluated zero divisor errors. A NULL
//! operand on EITHER side of any arithmetic operator (`+`, `-`, `*`, `/`)
//! propagates NULL, per standard SQL — never treated as zero, and never
//! reaching the division-by-zero check.

use contextdb_core::Value;
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
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER)",
        &empty(),
    )
    .expect("CREATE TABLE t");
}

fn insert_row_t(db: &Database, a: i64, b: Option<i64>) {
    let mut row = params(vec![
        ("id", Value::Uuid(Uuid::new_v4())),
        ("a", Value::Int64(a)),
    ]);
    row.insert("b".to_string(), b.map(Value::Int64).unwrap_or(Value::Null));
    db.execute("INSERT INTO t (id, a, b) VALUES ($id, $a, $b)", &row)
        .expect("INSERT t");
}

// ============================================================================
// SELECT-list door: `a / 0` in the projection must fail the statement, never
// panic the process and never silently return a wrong value.
// ============================================================================
#[test]
fn select_list_division_by_zero_is_a_statement_error_not_a_panic() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 10, Some(2));

    let result = db.execute("SELECT a / 0 FROM t", &empty());
    let err = result.expect_err("division by a literal zero must be a statement error");
    let message = err.to_string();
    assert!(
        message.contains("division by zero"),
        "expected a division-by-zero message, got {message:?}"
    );
}

// ============================================================================
// WHERE door: the reported live failure shape (`WHERE a / 0 = 1`) — a
// residual predicate dividing by zero must fail the statement, not panic.
// ============================================================================
#[test]
fn where_division_by_zero_is_a_statement_error_not_a_panic() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 10, Some(2));

    let result = db.execute("SELECT id FROM t WHERE a / 0 = 1", &empty());
    let err = result.expect_err("a WHERE predicate dividing by zero must be a statement error");
    assert!(
        err.to_string().contains("division by zero"),
        "expected a division-by-zero message, got {err:?}"
    );
}

// ============================================================================
// ORDER BY door: dividing by zero inside a sort key must fail the statement,
// not panic mid-sort and not silently leave the result unsorted.
// ============================================================================
#[test]
fn order_by_division_by_zero_is_a_statement_error_not_a_panic() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 10, Some(2));

    let result = db.execute("SELECT id FROM t ORDER BY a / 0", &empty());
    let err = result.expect_err("an ORDER BY key dividing by zero must be a statement error");
    assert!(
        err.to_string().contains("division by zero"),
        "expected a division-by-zero message, got {err:?}"
    );
}

// ============================================================================
// Division by a column whose real, stored value is zero (not just a
// literal) must be refused the same way — the check is on the evaluated
// divisor, not on the syntactic shape of the expression.
// ============================================================================
#[test]
fn division_by_a_column_holding_zero_is_a_statement_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 10, Some(0));

    let result = db.execute("SELECT a / b FROM t", &empty());
    let err = result.expect_err("dividing by a column whose value is 0 must be a statement error");
    assert!(
        err.to_string().contains("division by zero"),
        "expected a division-by-zero message, got {err:?}"
    );
}

// ============================================================================
// Float division by zero is refused the same way — not left to silently
// produce `inf`/`NaN`.
// ============================================================================
#[test]
fn float_division_by_zero_is_a_statement_error() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE u (id UUID PRIMARY KEY, a REAL)", &empty())
        .expect("CREATE TABLE u");
    db.execute(
        "INSERT INTO u (id, a) VALUES ($id, $a)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("a", Value::Float64(1.5)),
        ]),
    )
    .expect("INSERT u");

    let result = db.execute("SELECT a / 0.0 FROM u", &empty());
    let err = result.expect_err("float division by a zero divisor must be a statement error");
    assert!(
        err.to_string().contains("division by zero"),
        "expected a division-by-zero message, got {err:?}"
    );
}

// ============================================================================
// Positive control (must stay green): a NULL divisor propagates NULL — it is
// never coerced to zero and never a division-by-zero error. `b` is left
// NULL on this row.
// ============================================================================
#[test]
fn null_divisor_propagates_null_not_an_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 10, None);

    let r = db
        .execute("SELECT a / b FROM t", &empty())
        .expect("a NULL divisor must propagate NULL, never error");
    assert_eq!(r.rows, vec![vec![Value::Null]]);

    let r = db
        .execute("SELECT id FROM t WHERE a / b = 1", &empty())
        .expect("a NULL divisor in a WHERE predicate must exclude the row, never error");
    assert_eq!(
        r.rows.len(),
        0,
        "NULL = anything excludes the row without erroring"
    );
}

// ============================================================================
// Positive control (must stay green): ordinary division by a real, non-zero
// value keeps working across every door.
// ============================================================================
#[test]
fn division_by_a_nonzero_value_still_works() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 10, Some(2));

    let r = db
        .execute("SELECT a / b FROM t", &empty())
        .expect("ordinary division must still work");
    assert_eq!(r.rows, vec![vec![Value::Int64(5)]]);
}

// ============================================================================
// Positive control (must stay green), mirrored on a DIFFERENT operator: NULL
// propagation is not special-cased to division. `b + 1` over a NULL `b` must
// exclude the row, the same as any other NULL comparison — never a
// statement error. `b` is left NULL on this row.
// ============================================================================
#[test]
fn null_operand_on_addition_excludes_the_row_not_an_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, 10, None);

    let r = db
        .execute("SELECT b + 1 FROM t", &empty())
        .expect("a NULL operand on addition must propagate NULL, never error");
    assert_eq!(r.rows, vec![vec![Value::Null]]);

    let r = db
        .execute("SELECT id FROM t WHERE b + 1 > 0", &empty())
        .expect(
            "a NULL operand on addition in a WHERE predicate must exclude the row, never error",
        );
    assert_eq!(
        r.rows.len(),
        0,
        "NULL > 0 excludes the row without erroring"
    );
}

// ============================================================================
// `i64::MIN / -1` is the one other integer division a machine word cannot
// represent (the mathematical result overflows `i64`) — refused with a typed
// error rather than the process aborting on the overflow.
// ============================================================================
#[test]
fn minimum_integer_divided_by_negative_one_is_a_statement_error() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, i64::MIN, Some(-1));

    let result = db.execute("SELECT a / b FROM t", &empty());
    let err = result.expect_err("i64::MIN / -1 must be a statement error, not a panic");
    assert!(
        err.to_string().contains("overflow"),
        "expected an overflow message, got {err:?}"
    );
}

// ============================================================================
// Arithmetic overflow on add/sub/mul — a planner-resolved overflowing
// predicate must not silently seek a wrong key; instead it falls back to
// residual filtering (the same pattern as checked division).
// ============================================================================

#[test]
fn indexed_predicate_with_overflowing_addition_is_handled_safely() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, i64::MAX, Some(1));
    insert_row_t(&db, 100, Some(1));

    // Parameterized addition that would overflow: i64::MAX + 1.
    // Integer overflow is a typed statement error (standard-SQL), never a panic
    // and never silently wrapped rows.
    let result = db.execute(
        "SELECT id FROM t WHERE a + $delta > 0",
        &params(vec![("delta", Value::Int64(1))]),
    );

    // The query must fail with an out-of-range error, not panic or silently wrap.
    let err = result.expect_err("integer overflow must be a statement error");
    assert!(
        err.to_string().contains("out of range") || err.to_string().contains("overflow"),
        "expected an out-of-range/overflow message, got {err:?}"
    );

    // Positive control: non-overflowing addition still works.
    let r_ok = db
        .execute(
            "SELECT id FROM t WHERE a + $delta > 0",
            &params(vec![("delta", Value::Int64(-1))]),
        )
        .expect("non-overflowing addition must work");
    assert_eq!(
        r_ok.rows.len(),
        2,
        "both rows (a=i64::MAX and a=100) satisfy a - 1 > 0"
    );
}

#[test]
fn indexed_predicate_with_overflowing_subtraction_is_handled_safely() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, i64::MIN, Some(1));
    insert_row_t(&db, 100, Some(1));

    // Parameterized subtraction that would overflow: i64::MIN - 1.
    // Integer overflow is a typed statement error (standard-SQL), never a panic
    // and never silently wrapped rows.
    let result = db.execute(
        "SELECT id FROM t WHERE a - $delta > 0",
        &params(vec![("delta", Value::Int64(1))]),
    );

    // The query must fail with an out-of-range error, not panic or silently wrap.
    let err = result.expect_err("integer overflow must be a statement error");
    assert!(
        err.to_string().contains("out of range") || err.to_string().contains("overflow"),
        "expected an out-of-range/overflow message, got {err:?}"
    );

    // Positive control: non-overflowing subtraction still works.
    let r_ok = db
        .execute(
            "SELECT id FROM t WHERE a - $delta > 0",
            &params(vec![("delta", Value::Int64(-1))]),
        )
        .expect("non-overflowing subtraction must work");
    assert_eq!(r_ok.rows.len(), 1, "only a=100 satisfies a + 1 > 0");
}

#[test]
fn indexed_predicate_with_overflowing_multiplication_is_handled_safely() {
    let db = Database::open_memory();
    seed_t(&db);
    insert_row_t(&db, i64::MAX, Some(2));
    insert_row_t(&db, 100, Some(1));

    // Parameterized multiplication that would overflow: i64::MAX * 2.
    // Integer overflow is a typed statement error (standard-SQL), never a panic
    // and never silently wrapped rows.
    let result = db.execute(
        "SELECT id FROM t WHERE a * $factor > 0",
        &params(vec![("factor", Value::Int64(2))]),
    );

    // The query must fail with an out-of-range error, not panic or silently wrap.
    let err = result.expect_err("integer overflow must be a statement error");
    assert!(
        err.to_string().contains("out of range") || err.to_string().contains("overflow"),
        "expected an out-of-range/overflow message, got {err:?}"
    );

    // Positive control: non-overflowing multiplication still works.
    let r_ok = db
        .execute(
            "SELECT id FROM t WHERE a * $factor > 0",
            &params(vec![("factor", Value::Int64(1))]),
        )
        .expect("non-overflowing multiplication must work");
    assert_eq!(r_ok.rows.len(), 2, "both rows satisfy a * 1 > 0");
}
