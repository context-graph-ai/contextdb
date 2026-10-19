//! Index candidates that bind the same columns to the same values hold the
//! same entries in their bound ranges, so comparing those ranges reads
//! entries and learns nothing. A count served from a declared MAINTAIN COUNT
//! and a top-1 served by an index that already holds the ORDER BY must cost
//! the same however many rows the context holds.
//!
//! The shape is a consumer's event table: two UNIQUE constraints and two
//! indexes, all leading with `context_id`, and a maintained count by context.

use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

fn context() -> Uuid {
    Uuid::from_u128(0xc0)
}

fn events(rows: i64) -> (tempfile::TempDir, Database) {
    let root = tempfile::tempdir().expect("fixture directory");
    let db = Database::open(root.path().join("events.db")).expect("open the store");
    db.execute(
        "CREATE TABLE ev (id UUID PRIMARY KEY, context_id UUID NOT NULL, \
         connector_id TEXT NOT NULL, source_ref TEXT NOT NULL, seq INTEGER NOT NULL, \
         UNIQUE (context_id, id), UNIQUE (context_id, connector_id, source_ref, seq)) \
         IMMUTABLE SYNC OFF MAINTAIN COUNT (context_id), (context_id, connector_id)",
        &HashMap::new(),
    )
    .expect("declare the event table");
    for ddl in [
        "CREATE INDEX ev_seq ON ev (context_id, seq)",
        "CREATE INDEX ev_connector_seq ON ev (context_id, connector_id, seq)",
    ] {
        db.execute(ddl, &HashMap::new()).expect("declare an index");
    }
    let tx = db.begin().expect("seed transaction");
    for n in 0..rows {
        db.insert_row(
            tx,
            "ev",
            HashMap::from([
                (
                    "id".to_string(),
                    Value::Uuid(Uuid::from_u128(0xe0 << 64 | n as u128)),
                ),
                ("context_id".to_string(), Value::Uuid(context())),
                ("connector_id".to_string(), Value::Text("c".into())),
                ("source_ref".to_string(), Value::Text("s".into())),
                ("seq".to_string(), Value::Int64(n)),
            ]),
        )
        .expect("seed one event");
    }
    db.commit(tx).expect("commit the seed");
    (root, db)
}

/// Entries examined at each context size, after checking the answer: the
/// statement's single value must be what `expected` says for that size.
fn examined_at_every_size(sql: &str, expected: fn(i64) -> Value) -> Vec<(i64, u64)> {
    [20, 200, 2_000]
        .into_iter()
        .map(|rows| {
            let (_root, db) = events(rows);
            let result = db
                .execute(
                    sql,
                    &HashMap::from([("context_id".to_string(), Value::Uuid(context()))]),
                )
                .expect("run the statement");
            assert_eq!(
                result.rows,
                vec![vec![expected(rows)]],
                "{sql}: the answer at {rows} rows"
            );
            (rows, result.trace.rows_examined)
        })
        .collect()
}

fn assert_flat(sql: &str, expected: fn(i64) -> Value) {
    let examined = examined_at_every_size(sql, expected);
    assert!(
        examined.windows(2).all(|pair| pair[0].1 == pair[1].1),
        "{sql}: entries examined grew with the context's rows: {examined:?}"
    );
}

#[test]
fn a_maintained_count_reads_no_tied_range() {
    assert_flat(
        "SELECT COUNT(*) AS n FROM ev WHERE context_id = $context_id",
        Value::Int64,
    );
}

#[test]
fn a_top_one_held_by_an_index_reads_no_tied_range() {
    assert_flat(
        "SELECT seq FROM ev WHERE context_id = $context_id ORDER BY seq DESC LIMIT 1",
        |rows| Value::Int64(rows - 1),
    );
}
