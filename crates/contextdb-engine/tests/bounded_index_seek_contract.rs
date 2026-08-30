#![cfg(feature = "test-seams")]
//! An indexed read must reach the keys its predicate names.
//!
//! A read planned onto a declared index is planned that way because the index
//! orders its keys: the predicate names where the answer starts and where it
//! ends, so the source touches the matching entries and nothing else. A source
//! that begins at the first key of the index, applies the predicate as a
//! rejection filter, and never stops at the end of the named range is a full
//! index walk. It answers the same rows, but it spends the operator's declared
//! work ceiling in proportion to the whole index instead of the match, and the
//! trace it hands back describes an index seek that did not happen.
//!
//! Every read below is issued through the production bounded-kernel entrance.

use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_engine::Database;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// Entries stored under distinct index keys.
const STORED_ROWS: u64 = 400;
/// The entries an equality read may inspect and still be a seek: the matching
/// run, plus generous room for the boundary entry that ends it.
const SEEK_ENTRY_BUDGET: u64 = 16;

/// Reads here are decided by ceilings and stored rows, never by elapsed time.
#[derive(Clone, Copy)]
struct FrozenClock;

impl DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        // These reads are synchronous; the immediately-completing future
        // satisfies the shared transport-facing clock trait.
        Box::pin(async {})
    }
}

fn roomy_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 256,
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

fn shelf_label(ordinal: u64) -> String {
    format!("shelf-{ordinal:04}")
}

/// One declared index over distinct text keys, deep enough that walking it is
/// plainly distinguishable from reaching one key.
fn indexed_catalog() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE catalog_entries (id UUID PRIMARY KEY, shelf TEXT, payload TEXT)",
        &HashMap::new(),
    )
    .expect("create the indexed catalog table");
    db.execute(
        "CREATE INDEX catalog_entries_shelf_idx ON catalog_entries(shelf)",
        &HashMap::new(),
    )
    .expect("declare the catalog shelf index");
    for ordinal in 0..STORED_ROWS {
        db.execute(
            "INSERT INTO catalog_entries (id, shelf, payload) VALUES ($id, $shelf, $payload)",
            &params([
                (
                    "id",
                    Value::Uuid(Uuid::from_u128(0xCA71_0000u128 + ordinal as u128)),
                ),
                ("shelf", Value::Text(shelf_label(ordinal))),
                ("payload", Value::Text(format!("entry {ordinal}"))),
            ]),
        )
        .expect("store an indexed catalog entry");
    }
    db
}

fn index_entries_touched(telemetry: &bounded::TestTelemetry) -> u64 {
    telemetry
        .source_work
        .get(&bounded::TestWorkSource::IndexRange)
        .copied()
        .unwrap_or_default()
}

/// The key an equality predicate names sits at the far end of the index, so a
/// source that starts at the first key pays for the whole index to reach it.
#[test]
fn an_indexed_equality_read_reaches_its_key_instead_of_walking_the_index() {
    let db = indexed_catalog();
    let outcome = bounded::execute(
        &db,
        &request(
            "SELECT id FROM catalog_entries WHERE shelf = $shelf",
            params([("shelf", Value::Text(shelf_label(STORED_ROWS - 1)))]),
            roomy_limits(),
        ),
    )
    .expect("an indexed equality read must be served");

    assert_eq!(
        outcome.result.rows.len(),
        1,
        "the equality predicate names exactly one stored entry"
    );
    let touched = index_entries_touched(&outcome.telemetry);
    assert!(
        touched <= SEEK_ENTRY_BUDGET,
        "an indexed equality read must reach the key its predicate names; it inspected \
         {touched} of {STORED_ROWS} index entries to answer with one row, which is a full \
         index walk with a rejection filter rather than a seek"
    );
}

/// The same read under a work ceiling sized for its own match. An operator who
/// declares room for the matching entries must get the answer, not a refusal
/// sized to the whole index.
#[test]
fn an_indexed_equality_read_is_served_under_a_work_ceiling_sized_for_its_match() {
    let db = indexed_catalog();
    let mut limits = roomy_limits();
    limits.work = SEEK_ENTRY_BUDGET * 4;
    let outcome = bounded::execute(
        &db,
        &request(
            "SELECT id FROM catalog_entries WHERE shelf = $shelf",
            params([("shelf", Value::Text(shelf_label(STORED_ROWS - 1)))]),
            limits,
        ),
    );

    let served = match outcome {
        Ok(served) => served,
        Err(error) => panic!(
            "an indexed equality read matching one entry must be served under a work ceiling \
             of {} units; it was answered with {error:?}, so the declared ceiling is being \
             spent on entries the predicate excludes",
            limits.work
        ),
    };
    assert_eq!(served.result.rows.len(), 1);
}

/// A range predicate names where the answer ends. A source that keeps reading
/// past the upper bound spends the ceiling on entries it has already excluded.
#[test]
fn an_indexed_range_read_stops_at_the_end_of_the_range_it_names() {
    let db = indexed_catalog();
    let matches = 4_u64;
    let outcome = bounded::execute(
        &db,
        &request(
            "SELECT id FROM catalog_entries WHERE shelf < $shelf",
            params([("shelf", Value::Text(shelf_label(matches)))]),
            roomy_limits(),
        ),
    )
    .expect("an indexed range read must be served");

    assert_eq!(
        outcome.result.rows.len(),
        matches as usize,
        "the range predicate names exactly the leading entries"
    );
    let touched = index_entries_touched(&outcome.telemetry);
    assert!(
        touched <= matches + SEEK_ENTRY_BUDGET,
        "an indexed range read must stop at the upper bound it names; it inspected {touched} \
         of {STORED_ROWS} index entries to answer with {matches} rows"
    );
}

/// The trace is what an operator reads to understand where a read spent its
/// budget. An index seek that reports having examined the whole table is a
/// trace that describes a plan the read did not run.
#[test]
fn the_trace_of_an_indexed_equality_read_reports_the_entries_it_examined() {
    let db = indexed_catalog();
    let outcome = bounded::execute(
        &db,
        &request(
            "SELECT id FROM catalog_entries WHERE shelf = $shelf",
            params([("shelf", Value::Text(shelf_label(STORED_ROWS - 1)))]),
            roomy_limits(),
        ),
    )
    .expect("an indexed equality read must be served");

    assert_eq!(
        outcome.result.trace.physical_plan, "IndexScan",
        "the equality predicate is answered through the declared index"
    );
    assert!(
        outcome.result.trace.rows_examined <= SEEK_ENTRY_BUDGET,
        "the trace of an index seek must report the entries the seek examined; it reports \
         {} examined against {STORED_ROWS} stored entries for a one-row answer",
        outcome.result.trace.rows_examined
    );
}
