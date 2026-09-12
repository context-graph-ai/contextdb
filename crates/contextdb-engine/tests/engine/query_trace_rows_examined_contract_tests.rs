use contextdb_core::Value;
use contextdb_engine::{Database, QueryResult};
use std::collections::HashMap;

fn params() -> HashMap<String, Value> {
    HashMap::new()
}

fn execute(db: &Database, sql: &str) -> QueryResult {
    db.execute(sql, &params())
        .unwrap_or_else(|error| panic!("{sql} must succeed: {error}"))
}

fn create_and_seed(db: &Database, table: &str, rows: u64) {
    execute(
        db,
        &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, body TEXT)"),
    );
    for id in 0..rows {
        execute(
            db,
            &format!("INSERT INTO {table} (id, body) VALUES ({id}, 'row-{id}')"),
        );
    }
}

#[test]
fn query_trace_rows_examined_is_statement_scoped_across_ddl() {
    let db = Database::open_memory();
    create_and_seed(&db, "trace_small", 3);

    let first_select = execute(&db, "SELECT id FROM trace_small");
    assert_eq!(first_select.trace.rows_examined, 3);

    let ddl_after_select = execute(
        &db,
        "CREATE TABLE trace_ddl_after_select (id INTEGER PRIMARY KEY)",
    );
    assert_eq!(ddl_after_select.trace.rows_examined, 0);
}
