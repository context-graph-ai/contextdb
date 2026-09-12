//! `-i64::MIN` must be an ordinary SQL error at every production evaluator.

use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;

fn params() -> HashMap<String, Value> {
    HashMap::new()
}

fn min_params() -> HashMap<String, Value> {
    HashMap::from([("min".to_string(), Value::Int64(i64::MIN))])
}

fn assert_out_of_range(result: contextdb_core::Result<contextdb_engine::QueryResult>) {
    let error = result.expect_err("-i64::MIN must return an SQL error, never panic or wrap");
    assert_eq!(error.to_string(), "plan error: integer out of range");
}

fn numbers() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE numbers (id INTEGER PRIMARY KEY, n INTEGER)",
        &params(),
    )
    .expect("create numbers");
    db.execute(
        "INSERT INTO numbers (id, n) VALUES (1, $min)",
        &min_params(),
    )
    .expect("seed i64 minimum");
    db
}

#[test]
fn projection_negation_of_i64_min_returns_the_standard_sql_error() {
    let db = numbers();
    assert_out_of_range(db.execute("SELECT -n FROM numbers", &params()));
}

#[test]
fn predicate_negation_of_i64_min_returns_the_standard_sql_error() {
    let db = numbers();
    assert_out_of_range(db.execute("SELECT id FROM numbers WHERE -n = 1", &params()));
}

#[test]
fn constant_parameter_negation_of_i64_min_returns_the_standard_sql_error() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE numbers (id INTEGER PRIMARY KEY, n INTEGER)",
        &params(),
    )
    .expect("create numbers");
    assert_out_of_range(db.execute(
        "INSERT INTO numbers (id, n) VALUES (1, -$min)",
        &min_params(),
    ));
    assert!(
        db.execute("SELECT id FROM numbers", &params())
            .expect("connection remains usable")
            .rows
            .is_empty(),
        "a rejected constant expression must not partially insert a row"
    );
}

#[test]
fn update_and_conflict_assignment_negation_preserve_the_old_value() {
    let db = numbers();
    assert_out_of_range(db.execute("UPDATE numbers SET n = -n WHERE id = 1", &params()));
    assert_out_of_range(db.execute(
        "INSERT INTO numbers (id, n) VALUES (1, 0) ON CONFLICT (id) DO UPDATE SET n = -n",
        &params(),
    ));
    assert_eq!(
        db.execute("SELECT n FROM numbers WHERE id = 1", &params())
            .expect("connection remains usable")
            .rows,
        vec![vec![Value::Int64(i64::MIN)]],
        "either rejected assignment must leave the existing row unchanged"
    );
}

#[test]
fn graph_negation_of_i64_min_returns_the_standard_sql_error() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE nodes (id UUID PRIMARY KEY, n INTEGER)",
        &params(),
    )
    .expect("create nodes");
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &params(),
    )
    .expect("create edges");
    db.execute(
        "INSERT INTO nodes (id, n) VALUES ('00000000-0000-0000-0000-000000000001', $min)",
        &min_params(),
    )
    .expect("insert graph source");
    db.execute(
        "INSERT INTO nodes (id, n) VALUES ('00000000-0000-0000-0000-000000000002', 2)",
        &params(),
    )
    .expect("insert graph target");
    db.execute(
        "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ('00000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'LINKS')",
        &params(),
    )
    .expect("insert graph edge");
    assert_out_of_range(db.execute(
        "SELECT target FROM GRAPH_TABLE (edges MATCH (a)-[:LINKS]->(b) WHERE -a.n = 1 COLUMNS (b.id AS target))",
        &params(),
    ));
}
