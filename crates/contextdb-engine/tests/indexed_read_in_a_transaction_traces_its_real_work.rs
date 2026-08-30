#![cfg(feature = "test-seams")]
//! An index read inside an open transaction reports the work it really did.
//!
//! An operator sizes a read's work ceiling from what the read says it does. A
//! read that reports it walked an index over five rows, and in fact walked
//! every row of the table, hands that operator a ceiling that holds on a small
//! table and refuses the identical query on a large one -- and a trace that
//! cannot be used to find out why.
//!
//! Having a transaction open is not a reason to stop using the index. The
//! index knows the committed rows; the transaction's own staged rows are a
//! small overlay on top of them. So a read with a transaction open reaches its
//! rows through the index it names, still answers with everything the reader
//! staged, and reports the work that took.

use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::{Database, QueryResult, QueryTrace};
use std::collections::HashMap;

/// Rows committed before any transaction opens. Large enough that walking
/// them all is unmistakable against reaching five.
const STORED_ROWS: i64 = 1_000;
/// The first bucket the range asks for.
const RANGE_LOW: i64 = 500;
/// The last bucket the range asks for.
const RANGE_HIGH: i64 = 504;
/// Rows the range names among the committed ones.
const RANGE_ROWS: usize = 5;
/// The declared index every read below reaches its rows through.
const DECLARED_INDEX: &str = "indexed_events_bucket_idx";
/// Entries a walk may examine beyond the rows it answers with and still be a
/// walk of the run rather than of the table: the run's boundary entry, plus
/// the staged rows the overlay adds.
const WALK_SLACK: u64 = 16;

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

/// Ceilings far above anything these reads need, so a ceiling can never be
/// what refuses instead of the read finishing.
fn roomy() -> ReadLimits {
    ReadLimits {
        result_rows: 1_000_000,
        result_bytes: 512 * 1024 * 1024,
        work: 1_000_000_000,
        active_ms: 600_000,
        memory: 512 * 1024 * 1024,
        cursor_page_rows: 100_000,
        cursor_page_bytes: 64 * 1024 * 1024,
        cursor_idle_ms: 600_000,
        cursor_lifetime_ms: 1_800_000,
    }
}

/// A table with one declared index over a column whose values are all
/// distinct, so the index really holds one key per row.
fn indexed_events() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE indexed_events (id INTEGER PRIMARY KEY, bucket INTEGER, payload TEXT)",
        &empty(),
    )
    .expect("create the indexed event table");
    db.execute(
        &format!("CREATE INDEX {DECLARED_INDEX} ON indexed_events(bucket)"),
        &empty(),
    )
    .expect("declare the index the reads below reach their rows through");
    for ordinal in 0..STORED_ROWS {
        db.execute(
            "INSERT INTO indexed_events (id, bucket, payload) VALUES ($id, $bucket, $payload)",
            &params(vec![
                ("id", Value::Int64(ordinal)),
                ("bucket", Value::Int64(ordinal)),
                ("payload", Value::Text(format!("event {ordinal}"))),
            ]),
        )
        .expect("store a committed event row");
    }
    db
}

/// Stage one row on the writing handle inside whatever transaction it has
/// open, so the read below has a real overlay to answer over.
fn stage_row(db: &Database, id: i64, bucket: i64) {
    db.execute(
        "INSERT INTO indexed_events (id, bucket, payload) VALUES ($id, $bucket, $payload)",
        &params(vec![
            ("id", Value::Int64(id)),
            ("bucket", Value::Int64(bucket)),
            ("payload", Value::Text(format!("staged {id}"))),
        ]),
    )
    .expect("stage an uncommitted event row");
}

/// The two shapes that reach a declared index directly: a range over the
/// indexed column, and an ordered read of it.
const RANGE_SQL: &str = "SELECT id FROM indexed_events WHERE bucket >= $low AND bucket <= $high \
                         ORDER BY bucket";
const ORDERED_SQL: &str = "SELECT id FROM indexed_events ORDER BY bucket LIMIT 5";

fn range_params() -> HashMap<String, Value> {
    params(vec![
        ("low", Value::Int64(RANGE_LOW)),
        ("high", Value::Int64(RANGE_HIGH)),
    ])
}

fn read(db: &Database, sql: &str, sql_params: &HashMap<String, Value>) -> QueryResult {
    db.read_session(roomy())
        .expect("open a bounded read view")
        .execute(sql, sql_params)
        .expect("a bounded index read must be served")
}

fn ids(result: &QueryResult) -> Vec<i64> {
    result
        .rows
        .iter()
        .map(|row| match row.first() {
            Some(Value::Int64(id)) => *id,
            other => panic!("expected an id-leading row, got {other:?}"),
        })
        .collect()
}

/// What a read reported about itself, printed so the receipt carries the
/// measurement and not only the verdict.
fn report(shape: &str, trace: &QueryTrace) {
    println!(
        "OBSERVED {shape}: plan {} index {:?} rows_examined {}",
        trace.physical_plan, trace.index_used, trace.rows_examined
    );
}

/// The read took the route this file is about, and says which index it used.
fn assert_reached_through_the_index(shape: &str, trace: &QueryTrace) {
    report(shape, trace);
    assert_eq!(
        trace.physical_plan, "IndexScan",
        "the {shape} read reaches its rows through the declared index"
    );
    assert_eq!(
        trace.index_used.as_deref(),
        Some(DECLARED_INDEX),
        "the {shape} read names the index that answered it"
    );
}

#[test]
fn a_range_over_a_declared_index_traces_its_walk_with_a_transaction_open() {
    let db = indexed_events();

    // The same read against committed state alone. It is the oracle: same
    // rows, same index, the only difference below is an open transaction.
    let committed = read(&db, RANGE_SQL, &range_params());
    assert_reached_through_the_index("committed range", &committed.trace);
    assert_eq!(ids(&committed).len(), RANGE_ROWS);

    db.execute("BEGIN", &empty())
        .expect("begin a transaction on the writing handle");
    // Staged outside the range, so the answer is unchanged and only the work
    // is under test.
    stage_row(&db, STORED_ROWS + 1, STORED_ROWS + 1);
    let staged = read(&db, RANGE_SQL, &range_params());
    db.execute("ROLLBACK", &empty())
        .expect("discard the staged row");

    assert_eq!(
        ids(&staged),
        ids(&committed),
        "a row staged outside the range does not change the range's answer"
    );
    assert_reached_through_the_index("in-transaction range", &staged.trace);
    let ceiling = committed.trace.rows_examined + 1 + WALK_SLACK;
    assert!(
        staged.trace.rows_examined <= ceiling,
        "with a transaction open the range examined {} rows against {} for the same range on \
         committed state alone, over a table of {STORED_ROWS} rows; a read that reports \
         IndexScan and walks the whole table gives an operator a work ceiling that holds on a \
         small table and refuses the identical query on a large one",
        staged.trace.rows_examined,
        committed.trace.rows_examined
    );
}

#[test]
fn an_ordered_read_of_a_declared_index_traces_its_walk_with_a_transaction_open() {
    let db = indexed_events();

    let committed = read(&db, ORDERED_SQL, &empty());
    assert_reached_through_the_index("committed ordered", &committed.trace);
    assert_eq!(ids(&committed), vec![0, 1, 2, 3, 4]);

    db.execute("BEGIN", &empty())
        .expect("begin a transaction on the writing handle");
    // Staged above every bucket the first five rows carry, so the answer is
    // unchanged and only the route and the work are under test.
    stage_row(&db, STORED_ROWS + 1, STORED_ROWS + 1);
    let staged = read(&db, ORDERED_SQL, &empty());
    db.execute("ROLLBACK", &empty())
        .expect("discard the staged row");
    report("in-transaction ordered", &staged.trace);

    assert_eq!(
        ids(&staged),
        ids(&committed),
        "a row staged above the ordered read's rows does not change its answer"
    );

    // Both halves of the same promise are judged together, so the receipt
    // shows the route the read took AND what that route cost, rather than
    // stopping at whichever is reported first.
    let mut broken = Vec::new();
    if staged.trace.physical_plan != "IndexScan" {
        broken.push(format!(
            "with a transaction open the ordered read is planned as {} over {:?} instead of \
             walking the declared index it uses on committed state",
            staged.trace.physical_plan, staged.trace.index_used
        ));
    }
    let ceiling = committed.trace.rows_examined + 1 + WALK_SLACK;
    if staged.trace.rows_examined > ceiling {
        broken.push(format!(
            "with a transaction open the ordered read examined {} rows against {} for the same \
             five-row answer on committed state alone, over a table of {STORED_ROWS} rows",
            staged.trace.rows_examined, committed.trace.rows_examined
        ));
    }
    assert!(
        broken.is_empty(),
        "an ordered read keeps reaching its rows through the index while a transaction is open; \
         an operator who sized this read's ceilings from what it does on committed state gets a \
         read that costs the whole table the moment a transaction is open:\n{}",
        broken.join("\n")
    );
}

#[test]
fn an_index_read_with_a_transaction_open_still_answers_with_the_rows_it_staged() {
    let db = indexed_events();

    db.execute("BEGIN", &empty())
        .expect("begin a transaction on the writing handle");
    // Inside the range, so reaching the rows through the index can never mean
    // answering only from what the index knows.
    stage_row(&db, STORED_ROWS + 2, RANGE_LOW + 2);
    let staged = read(&db, RANGE_SQL, &range_params());
    db.execute("ROLLBACK", &empty())
        .expect("discard the staged row");

    assert_reached_through_the_index("in-transaction range with a staged match", &staged.trace);
    assert!(
        ids(&staged).contains(&(STORED_ROWS + 2)),
        "a row the reader staged inside the range is part of the answer: {:?}",
        ids(&staged)
    );
    assert_eq!(
        ids(&staged).len(),
        RANGE_ROWS + 1,
        "the answer is the committed rows of the range plus the one staged row: {:?}",
        ids(&staged)
    );
}
