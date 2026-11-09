//! What an INDEXED row really costs when every row's value is different.
//!
//! An index over a column keeps that column's value in a second place. A
//! fixture where every row carries the SAME payload hides that entirely --
//! one distinct value means one index key, however many rows point at it --
//! so it measures an index that is doing no work. Real rows are distinct, and
//! a distinct value under an index is held twice: once in the row, once in
//! the index.
//!
//! The memory a store is allowed to hold is decided by an ESTIMATE of what it
//! holds, so the estimate has to cover that second copy. An estimate below it
//! admits more than the operator allowed; an estimate far above it refuses
//! what the operator allowed. Both sides are checked, against the allocator.
//!
//! The store keeps its rows and index pages on disk and holds them in a
//! page cache whose size is fixed and charged against the declared limit when
//! the store opens. So the comparison is of TOTALS under a declared limit: the
//! process must never hold more than the accountant says the store holds, and
//! what the rows themselves add to the bill must stay within a small multiple
//! of what the process holds.

use contextdb_core::Value;
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::sync::atomic::{AtomicIsize, Ordering};

/// Live heap bytes: everything allocated, less everything freed.
static LIVE_BYTES: AtomicIsize = AtomicIsize::new(0);

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LIVE_BYTES.fetch_add(layout.size() as isize, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        LIVE_BYTES.fetch_add(layout.size() as isize, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        LIVE_BYTES.fetch_add(
            new_size as isize - layout.size() as isize,
            Ordering::Relaxed,
        );
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn live_bytes() -> i64 {
    LIVE_BYTES.load(Ordering::Relaxed) as i64
}

const ROWS: i64 = 200;

/// How far above the truth an estimate may sit. A store that charges itself
/// more than four times what it holds cannot hold what its limit says it can.
const CHARGE_CEILING_MULTIPLE: f64 = 4.0;

/// A declared limit far above anything this file stores. It is not a bound
/// under test: a store charges its fixed page cache against the limit it was
/// opened under, so accounting only means something once one is declared.
const DECLARED_LIMIT: usize = 256 * 1024 * 1024;

/// Payload lengths far apart: a short value and a long one are both held, so
/// the estimate has to be right at both ends and not only on average.
const LENGTHS: [usize; 3] = [100, 1_000, 100_000];

struct Measured {
    /// What the accountant says the store holds the moment it is opened: the
    /// fixed page cache reserve, charged against the declared limit.
    opened: i64,
    /// What the accountant says the store holds after the rows are stored.
    charged: i64,
    /// What the process really took, from just before the open.
    live: i64,
}

/// The noise floor of the comparison: the allocator counts a little incidental
/// work that is neither payload nor cache, so a process within this fraction
/// of its charge is covered by it. A real under-charge is a whole value held
/// where none was charged, never a hundredth of the store.
const COVERAGE_TOLERANCE: f64 = 0.01;

/// A payload of `length` bytes that no other row carries, so an index over
/// this column really keeps one key per row.
fn distinct_payload(ordinal: i64, length: usize) -> String {
    let mut payload = format!("{ordinal:016x}");
    payload.push_str(&"x".repeat(length.saturating_sub(payload.len())));
    payload
}

fn text_value(ordinal: i64, length: usize) -> Value {
    Value::Text(distinct_payload(ordinal, length))
}

fn json_value(ordinal: i64, length: usize) -> Value {
    Value::Json(serde_json::Value::String(distinct_payload(ordinal, length)))
}

/// Open a durable store under a declared limit, insert `ROWS` rows whose
/// payload column holds `length` distinct bytes, and report what the
/// accountant says the whole store holds against what the process really took
/// from the moment before the open.
fn measure_rows(
    column_type: &str,
    with_index: bool,
    length: usize,
    value: &dyn Fn(i64, usize) -> Value,
) -> Measured {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let live_before = live_bytes();
    let database = contextdb_engine::database::open_with_startup_limits(
        root.path().join("measured.db"),
        std::sync::Arc::new(contextdb_engine::plugin::CorePlugin),
        Some(DECLARED_LIMIT),
        None,
    )
    .expect("open the measured store under a declared limit");
    let opened = database.accountant().usage().used as i64;
    database
        .execute(
            &format!("CREATE TABLE measured (id INTEGER PRIMARY KEY, payload {column_type})"),
            &HashMap::new(),
        )
        .expect("create the measurement table");
    if with_index {
        database
            .execute(
                "CREATE INDEX measured_payload ON measured (payload)",
                &HashMap::new(),
            )
            .expect("create the index over the payload column");
    }
    for id in 0..ROWS {
        database
            .execute(
                "INSERT INTO measured (id, payload) VALUES ($id, $payload)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    ("payload".to_owned(), value(id, length)),
                ]),
            )
            .unwrap_or_else(|error| panic!("insert row {id} of length {length}: {error}"));
    }
    let measured = Measured {
        opened,
        charged: database.accountant().usage().used as i64,
        live: live_bytes() - live_before,
    };
    database.close().expect("close the measured store");
    measured
}

/// Measure one site at every payload length and record what the two bounds
/// say: the accountant must cover what the process holds, and must not
/// exceed it by more than the ceiling multiple.
fn judge(
    site: &str,
    column_type: &str,
    with_index: bool,
    value: &dyn Fn(i64, usize) -> Value,
    failures: &mut Vec<String>,
) {
    for length in LENGTHS {
        let measured = measure_rows(column_type, with_index, length, value);
        println!(
            "MEASURED {site} len={length}: opened={} charged={} live={}",
            measured.opened, measured.charged, measured.live
        );
        if measured.live as f64 > measured.charged as f64 * (1.0 + COVERAGE_TOLERANCE) {
            failures.push(format!(
                "{site} at length {length}: the process holds {} bytes and the accountant is \
                 told {}",
                measured.live, measured.charged
            ));
        }
        // The fixed cache reserve is charged whole at open and is held only as
        // the cache fills, so the ceiling is on what the rows added to the bill.
        let retained = measured.charged - measured.opened;
        if measured.live > 0 && retained as f64 > CHARGE_CEILING_MULTIPLE * measured.live as f64 {
            failures.push(format!(
                "{site} at length {length}: the rows add {retained} bytes to the bill and the \
                 process holds {}, so a limit holds less than a {CHARGE_CEILING_MULTIPLE:.0}th of \
                 what it says",
                measured.live
            ));
        }
    }
}

#[test]
fn a_distinct_value_under_an_index_is_charged_what_holding_it_twice_costs() {
    let mut failures = Vec::new();

    judge(
        "distinct text rows, no index",
        "TEXT",
        false,
        &text_value,
        &mut failures,
    );
    judge(
        "distinct text rows under an index",
        "TEXT",
        true,
        &text_value,
        &mut failures,
    );
    // A document column cannot carry an index -- the engine refuses one with
    // `ColumnNotIndexable` -- so text is the only variable-sized value an
    // index can hold a second copy of. The unindexed document arm stays as
    // the control that says what one copy of a document costs.
    judge(
        "distinct documents, no index",
        "JSON",
        false,
        &json_value,
        &mut failures,
    );

    assert!(
        failures.is_empty(),
        "an index keeps a second copy of every distinct value, and the estimate that admits a \
         memory limit has to cover it:\n{}",
        failures.join("\n")
    );
}
