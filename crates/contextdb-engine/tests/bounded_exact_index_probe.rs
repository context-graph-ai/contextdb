#![cfg(feature = "test-seams")]
//! A bounded read must reach a primary key the same way the engine does.
//!
//! An index the engine synthesizes for a primary key or a uniqueness
//! constraint keeps its postings by key and no ordered tree — it cannot be
//! walked or ranged, but it can be asked a whole key. A bounded read that
//! declines to ask it spends the operator's work ceiling in proportion to the
//! table for a one-row answer, and hands back a trace saying `Scan` where the
//! same query through the ordinary path says `IndexScan`. Two reading routes
//! that disagree about which index answered a query are two products.
//!
//! The fallback matters as much as the seek: a RANGE over such an index names
//! no whole key, so it must keep scanning rather than probe something the
//! storage cannot answer.

use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_engine::Database;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use std::collections::HashMap;
use std::sync::Arc;

/// Deep enough that walking the table is plainly distinguishable from
/// reaching one row.
const STORED_ROWS: i64 = 400;
/// What reaching one key may cost and still be a seek.
const SEEK_ENTRY_BUDGET: u64 = 16;

#[derive(Clone, Copy)]
struct FrozenClock;

impl DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn roomy_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 512,
        result_bytes: 16 * 1024 * 1024,
        work: 1_000_000,
        active_ms: 1_000_000,
        memory: 16 * 1024 * 1024,
        cursor_page_rows: 64,
        cursor_page_bytes: 4 * 1024 * 1024,
        cursor_idle_ms: 10_000,
        cursor_lifetime_ms: 100_000,
    }
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn request(
    sql: &str,
    bound: HashMap<String, Value>,
    limits: ReadLimits,
) -> bounded::BoundedReadRequest {
    bounded::BoundedReadRequest::new(sql, bound, limits, Arc::new(FrozenClock))
}

/// A table whose only index is the one the engine synthesized for its
/// primary key — nothing here is declared by the operator.
fn keyed_ledger() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE ledger_entries (id INTEGER PRIMARY KEY, payload TEXT)",
        &HashMap::new(),
    )
    .expect("create the keyed ledger table");
    for ordinal in 0..STORED_ROWS {
        db.execute(
            "INSERT INTO ledger_entries (id, payload) VALUES ($id, $payload)",
            &params([
                ("id", Value::Int64(ordinal)),
                ("payload", Value::Text(format!("entry {ordinal}"))),
            ]),
        )
        .expect("store a keyed ledger entry");
    }
    db
}

#[test]
fn a_bounded_primary_key_read_reaches_its_row_instead_of_walking_the_table() {
    let db = keyed_ledger();
    let outcome = bounded::execute(
        &db,
        &request(
            "SELECT id, payload FROM ledger_entries WHERE id = $id",
            params([("id", Value::Int64(STORED_ROWS - 1))]),
            roomy_limits(),
        ),
    )
    .expect("a bounded primary-key read must be served");

    assert_eq!(
        outcome.result.rows.len(),
        1,
        "the equality predicate names exactly one stored row"
    );
    assert_eq!(
        outcome.result.trace.physical_plan, "IndexScan",
        "a primary-key equality is answered through the key's own index"
    );
    assert!(
        outcome
            .result
            .trace
            .index_used
            .as_deref()
            .is_some_and(|index| index.starts_with("__pk_")),
        "the trace names the index that answered, got {:?}",
        outcome.result.trace.index_used
    );
    assert!(
        outcome.result.trace.rows_examined <= SEEK_ENTRY_BUDGET,
        "reaching one key must cost about one key; the trace reports {} examined against \
         {STORED_ROWS} stored rows",
        outcome.result.trace.rows_examined
    );
}

#[test]
fn a_bounded_primary_key_read_answers_the_same_rows_as_a_full_scan() {
    let db = keyed_ledger();
    let sought = STORED_ROWS / 2;
    let seek = bounded::execute(
        &db,
        &request(
            "SELECT id, payload FROM ledger_entries WHERE id = $id",
            params([("id", Value::Int64(sought))]),
            roomy_limits(),
        ),
    )
    .expect("the seeking read must be served");
    // The same question with a shape the key's index cannot answer, so it is
    // served by the scan path: the two must agree on the answer, or the seek
    // is a faster wrong one.
    let scan = bounded::execute(
        &db,
        &request(
            "SELECT id, payload FROM ledger_entries WHERE id >= $low AND id <= $high",
            params([
                ("low", Value::Int64(sought)),
                ("high", Value::Int64(sought)),
            ]),
            roomy_limits(),
        ),
    )
    .expect("the scanning read must be served");

    assert_eq!(seek.result.columns, scan.result.columns);
    assert_eq!(
        seek.result.rows, scan.result.rows,
        "a seek and a scan must answer the same rows for the same predicate"
    );
}

#[test]
fn a_range_over_a_synthesized_index_keeps_scanning() {
    let db = keyed_ledger();
    let outcome = bounded::execute(
        &db,
        &request(
            "SELECT id FROM ledger_entries WHERE id > $low",
            params([("low", Value::Int64(STORED_ROWS - 3))]),
            roomy_limits(),
        ),
    )
    .expect("a bounded range read must be served");

    assert_eq!(
        outcome.result.rows.len(),
        2,
        "the range names the two rows above its bound"
    );
    assert_eq!(
        outcome.result.trace.physical_plan, "Scan",
        "a range names no whole key, so an exact-lookup index cannot answer it and the read \
         stays on the scan path"
    );
    assert_eq!(
        outcome.result.trace.index_used, None,
        "a read that scanned must not report an index it did not use"
    );
}
