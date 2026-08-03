use super::{
    Database, arm_query_trace_rows_examined_capture_pause_for_test,
    mark_this_thread_for_query_trace_rows_examined_capture_pause_for_test,
};
use contextdb_core::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

fn params() -> HashMap<String, Value> {
    HashMap::new()
}

fn execute(db: &Database, sql: &str) -> super::QueryResult {
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
fn query_trace_rows_examined_stays_with_the_select_that_examined_the_rows() {
    let db = Arc::new(Database::open_memory());
    create_and_seed(&db, "trace_small", 3);
    create_and_seed(&db, "trace_large", 7);

    let pause = arm_query_trace_rows_examined_capture_pause_for_test();
    let small_query = {
        let db = db.clone();
        std::thread::spawn(move || {
            mark_this_thread_for_query_trace_rows_examined_capture_pause_for_test();
            execute(&db, "SELECT id FROM trace_small")
                .trace
                .rows_examined
        })
    };

    assert!(
        pause.wait_until_reached(Duration::from_secs(2)),
        "the small SELECT must reach the trace-capture boundary"
    );

    let large_query = execute(&db, "SELECT id FROM trace_large");

    pause.release();
    assert_eq!(
        small_query.join().expect("small query thread"),
        3,
        "the smaller SELECT retains its own examined-row count after the larger SELECT completes"
    );
    assert_eq!(
        large_query.trace.rows_examined, 7,
        "the larger SELECT reports its own examined-row count"
    );
}
