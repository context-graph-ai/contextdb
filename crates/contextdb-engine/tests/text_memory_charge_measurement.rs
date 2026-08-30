//! What a stored text value really costs, measured rather than asserted.
//!
//! The memory a store is allowed to hold is decided by an ESTIMATE of what it
//! is holding, and the estimate for a text value governs both a read's own
//! ceiling and the database-wide budget an operator sets. An estimate that is
//! too small does not fail loudly: it admits more than the operator allowed
//! and the process quietly holds more than it said it would. So the estimate
//! is checked against the allocator.
//!
//! The measurement is a SLOPE, not a total. A store holds far more than the
//! values it was given -- indexes, a change log, its own bookkeeping -- so
//! comparing one total against another proves nothing. What the text arm
//! claims is that a text value costs its own bytes ONCE; growing the text and
//! watching how much more the process holds per added byte is exactly that
//! claim, and it is immune to every fixed cost around it.

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

/// The noise floor of the measurement. The allocator counts a little
/// incidental work that is not the payload, so two slopes closer together
/// than this are the same slope. A real mis-charge is a whole factor -- one
/// byte charged where two are held -- never a hundredth of one.
const SLOPE_TOLERANCE: f64 = 0.01;

const ROWS: i64 = 200;
/// Two text lengths far enough apart that the slope between them is decided by
/// the text and not by anything fixed around it.
const SHORT: usize = 16;
const LENGTHS: [usize; 4] = [16, 100, 1_000, 100_000];

struct Measured {
    charged_per_row: f64,
    live_per_row: f64,
}

fn charge(database: &Database) -> i64 {
    database.accountant().usage().used as i64
}

/// Insert `ROWS` rows whose text column holds `length` bytes, and report what
/// the accountant was told each row cost against what the process actually
/// took for it.
fn measure_rows(length: usize, with_index: bool) -> Measured {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE measured (id INTEGER PRIMARY KEY, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create the measurement table");
    if with_index {
        database
            .execute(
                "CREATE INDEX measured_payload ON measured (payload)",
                &HashMap::new(),
            )
            .expect("create the text index");
    }
    let payload = "x".repeat(length);

    let charged_before = charge(&database);
    let live_before = live_bytes();
    for id in 0..ROWS {
        database
            .execute(
                "INSERT INTO measured (id, payload) VALUES ($id, $payload)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    ("payload".to_owned(), Value::Text(payload.clone())),
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

/// The same measurement for a graph edge carrying a text property. The text
/// lives in the PROPERTY rather than the edge type, because the property is
/// where a `Value::Text` is charged; an edge type is charged by its own arm,
/// which this measurement is not about.
fn measure_edges(length: usize) -> Measured {
    let database = Database::open_memory();
    let payload = "x".repeat(length);
    let source = uuid::Uuid::new_v4();

    let charged_before = charge(&database);
    let live_before = live_bytes();
    let tx = database.begin().expect("begin the edge batch");
    for _ in 0..ROWS {
        database
            .insert_edge(
                tx,
                source,
                uuid::Uuid::new_v4(),
                "carries".to_owned(),
                HashMap::from([("note".to_owned(), Value::Text(payload.clone()))]),
            )
            .expect("insert an edge carrying a text property");
    }
    database.commit(tx).expect("commit the edge batch");
    let charged_after = charge(&database);
    let live_after = live_bytes();

    Measured {
        charged_per_row: (charged_after - charged_before) as f64 / ROWS as f64,
        live_per_row: (live_after - live_before) as f64 / ROWS as f64,
    }
}

fn slope(short: &Measured, long: &Measured, long_length: usize) -> (f64, f64) {
    let added = (long_length - SHORT) as f64;
    (
        (long.charged_per_row - short.charged_per_row) / added,
        (long.live_per_row - short.live_per_row) / added,
    )
}

#[test]
fn a_stored_text_value_is_charged_at_least_what_holding_it_costs() {
    let mut failures = Vec::new();

    for (name, with_index) in [("plain rows", false), ("rows under a text index", true)] {
        let short = measure_rows(SHORT, with_index);
        println!(
            "MEASURED {name} len={SHORT}: charged/row={:.1} live/row={:.1}",
            short.charged_per_row, short.live_per_row
        );
        for length in LENGTHS.into_iter().filter(|length| *length != SHORT) {
            let long = measure_rows(length, with_index);
            let (charged_slope, live_slope) = slope(&short, &long, length);
            println!(
                "MEASURED {name} len={length}: charged/row={:.1} live/row={:.1} \
                 charged-per-added-byte={charged_slope:.3} live-per-added-byte={live_slope:.3}",
                long.charged_per_row, long.live_per_row
            );
            if live_slope > charged_slope + SLOPE_TOLERANCE {
                failures.push(format!(
                    "{name} at length {length}: the process takes {live_slope:.3} bytes for every \
                     text byte added and the accountant is told {charged_slope:.3}"
                ));
            }
        }
    }

    let short_edge = measure_edges(SHORT);
    println!(
        "MEASURED edges len={SHORT}: charged/row={:.1} live/row={:.1}",
        short_edge.charged_per_row, short_edge.live_per_row
    );
    for length in [100_usize, 1_000] {
        let long_edge = measure_edges(length);
        let (charged_slope, live_slope) = slope(&short_edge, &long_edge, length);
        println!(
            "MEASURED edges len={length}: charged/row={:.1} live/row={:.1} \
             charged-per-added-byte={charged_slope:.3} live-per-added-byte={live_slope:.3}",
            long_edge.charged_per_row, long_edge.live_per_row
        );
        if live_slope > charged_slope + SLOPE_TOLERANCE {
            failures.push(format!(
                "edges at length {length}: the process takes {live_slope:.3} bytes for every text \
                 byte added and the accountant is told {charged_slope:.3}"
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "the memory a store is allowed to hold is decided by this estimate, so an estimate \
         smaller than the cost admits more than the operator allowed:\n{}",
        failures.join("\n")
    );
}
