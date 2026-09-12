//! Index-predicate arithmetic must not panic on division by zero or overflow.
//! An equality predicate with a division on the RHS (`WHERE indexed_col = $a / $b`)
//! is evaluated at planning time to determine if it can use an index scan. If the
//! division panics (overflow or zero divisor), the planning panics the process.
//! The fix: use checked_div and return None on error, causing the predicate to
//! fall back to residual filtering instead.

use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

#[test]
fn indexed_equality_predicate_with_division_by_zero_returns_typed_error() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE nums (id INTEGER PRIMARY KEY, value INTEGER)",
        &p(),
    )
    .expect("create table");
    db.execute("CREATE INDEX idx_value ON nums(value)", &p())
        .expect("create index");
    db.execute("INSERT INTO nums (id, value) VALUES (1, 10)", &p())
        .expect("insert row");

    // Equality predicate with division by zero on the RHS.
    // This should not panic the process; it should return a typed error.
    let mut params = p();
    params.insert("a".to_string(), Value::Int64(10));
    params.insert("b".to_string(), Value::Int64(0));
    let err = db
        .execute("SELECT id FROM nums WHERE value = $a / $b", &params)
        .expect_err("division by zero should return an error");
    // Verify it's a typed error (PlanError), not a panic
    let err_str = err.to_string();
    assert!(
        err_str.contains("division by zero"),
        "error message should mention division by zero: {err_str}"
    );
}

#[test]
fn indexed_equality_predicate_with_i64_min_div_minus_one_returns_typed_error() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE nums (id INTEGER PRIMARY KEY, value INTEGER)",
        &p(),
    )
    .expect("create table");
    db.execute("CREATE INDEX idx_value ON nums(value)", &p())
        .expect("create index");
    db.execute("INSERT INTO nums (id, value) VALUES (1, 10)", &p())
        .expect("insert row");

    // Equality predicate with i64::MIN / -1, which would overflow.
    // This should not panic the process; it should return a typed error.
    let mut params = p();
    params.insert("a".to_string(), Value::Int64(i64::MIN));
    params.insert("b".to_string(), Value::Int64(-1));
    let err = db
        .execute("SELECT id FROM nums WHERE value = $a / $b", &params)
        .expect_err("i64::MIN / -1 should return an error");
    // Verify it's a typed error (PlanError), not a panic
    let err_str = err.to_string();
    assert!(
        err_str.contains("overflow"),
        "error message should mention overflow: {err_str}"
    );
}

#[test]
fn indexed_equality_predicate_with_valid_division_works() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE nums (id INTEGER PRIMARY KEY, value INTEGER)",
        &p(),
    )
    .expect("create table");
    db.execute("CREATE INDEX idx_value ON nums(value)", &p())
        .expect("create index");
    db.execute("INSERT INTO nums (id, value) VALUES (1, 5)", &p())
        .expect("insert row");
    db.execute("INSERT INTO nums (id, value) VALUES (2, 10)", &p())
        .expect("insert row");

    // Valid division that should work correctly
    let mut params = p();
    params.insert("a".to_string(), Value::Int64(10));
    params.insert("b".to_string(), Value::Int64(2));
    let result = db
        .execute("SELECT id FROM nums WHERE value = $a / $b", &params)
        .expect("valid division should work");
    assert_eq!(result.rows.len(), 1, "should find one matching row");
    let id = match &result.rows[0][0] {
        Value::Int64(n) => *n,
        other => panic!("expected int64, got {other:?}"),
    };
    assert_eq!(id, 1, "should match row with id=1 (value=5)");
}
