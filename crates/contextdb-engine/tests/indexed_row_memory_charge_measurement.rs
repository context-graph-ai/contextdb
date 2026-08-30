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
//! The measurement is a SLOPE per added payload byte, so every fixed cost
//! around the value -- the index's own structure, the change log, the store's
//! bookkeeping -- cancels out.

use contextdb_core::Value;
use contextdb_engine::Database;
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

/// The noise floor of the measurement. The allocator counts a little
/// incidental work that is not the payload, so two slopes closer together
/// than this are the same slope. A real mis-charge is a whole factor -- one
/// byte charged where two are held -- never a hundredth of one.
const SLOPE_TOLERANCE: f64 = 0.01;

/// Payload lengths far enough apart that the slope between them is decided by
/// the payload and not by anything fixed around it. The shortest still has
/// room for the per-row prefix that makes every payload distinct.
const LENGTHS: [usize; 4] = [32, 100, 1_000, 100_000];

struct Measured {
    charged_per_row: f64,
    live_per_row: f64,
}

fn charge(database: &Database) -> i64 {
    database.accountant().usage().used as i64
}

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

/// Insert `ROWS` rows whose payload column holds `length` distinct bytes, and
/// report what the accountant was told each row cost against what the process
/// really took for it.
fn measure_rows(
    column_type: &str,
    with_index: bool,
    length: usize,
    value: &dyn Fn(i64, usize) -> Value,
) -> Measured {
    let database = Database::open_memory();
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

    let charged_before = charge(&database);
    let live_before = live_bytes();
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
    let charged_after = charge(&database);
    let live_after = live_bytes();

    Measured {
        charged_per_row: (charged_after - charged_before) as f64 / ROWS as f64,
        live_per_row: (live_after - live_before) as f64 / ROWS as f64,
    }
}

/// Measure one site across every payload length and record what the two
/// bounds say about the slope from the shortest.
fn judge(
    site: &str,
    column_type: &str,
    with_index: bool,
    value: &dyn Fn(i64, usize) -> Value,
    failures: &mut Vec<String>,
) {
    let baseline = measure_rows(column_type, with_index, LENGTHS[0], value);
    println!(
        "MEASURED {site} len={}: charged/row={:.1} live/row={:.1}",
        LENGTHS[0], baseline.charged_per_row, baseline.live_per_row
    );
    for length in LENGTHS.into_iter().skip(1) {
        let longer = measure_rows(column_type, with_index, length, value);
        let added = (length - LENGTHS[0]) as f64;
        let charged_slope = (longer.charged_per_row - baseline.charged_per_row) / added;
        let live_slope = (longer.live_per_row - baseline.live_per_row) / added;
        println!(
            "MEASURED {site} len={length}: charged/row={:.1} live/row={:.1} \
             charged-per-added-byte={charged_slope:.3} live-per-added-byte={live_slope:.3}",
            longer.charged_per_row, longer.live_per_row
        );
        if live_slope > charged_slope + SLOPE_TOLERANCE {
            failures.push(format!(
                "{site} at length {length}: the process takes {live_slope:.3} bytes for every \
                 payload byte added and the accountant is told {charged_slope:.3}"
            ));
        }
        if live_slope > 0.0
            && charged_slope > CHARGE_CEILING_MULTIPLE * live_slope + SLOPE_TOLERANCE
        {
            failures.push(format!(
                "{site} at length {length}: the accountant is told {charged_slope:.3} bytes for \
                 every payload byte added and the process takes {live_slope:.3}, so a limit holds \
                 less than a {CHARGE_CEILING_MULTIPLE:.0}th of what it says"
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
