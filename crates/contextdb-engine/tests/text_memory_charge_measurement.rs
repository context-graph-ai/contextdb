//! What a stored text value really costs, measured rather than asserted.
//!
//! The memory a store is allowed to hold is decided by an ESTIMATE of what it
//! is holding, and the estimate for a text value governs both a read's own
//! ceiling and the database-wide budget an operator sets. An estimate that is
//! too small does not fail loudly: it admits more than the operator allowed
//! and the process quietly holds more than it said it would. So the estimate
//! is checked against the allocator.
//!
//! The store keeps its rows on disk and holds them in a page cache whose size
//! is fixed and charged against the declared limit when the store opens. So the
//! comparison is of TOTALS under a declared limit, at several text lengths: the
//! process must never hold more than the accountant says the store holds.

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

/// The noise floor of the comparison: the allocator counts a little incidental
/// work that is neither text nor cache, so a process within this fraction of
/// its charge is covered by it. A real under-charge is a whole value held
/// where none was charged, never a hundredth of the store.
const COVERAGE_TOLERANCE: f64 = 0.01;

/// A declared limit far above anything this file stores. It is not a bound
/// under test: a store charges its fixed page cache against the limit it was
/// opened under, so accounting only means something once one is declared.
const DECLARED_LIMIT: usize = 256 * 1024 * 1024;

const ROWS: i64 = 200;
const LENGTHS: [usize; 4] = [16, 100, 1_000, 100_000];

struct Measured {
    charged: i64,
    live: i64,
}

fn open_under_a_declared_limit(root: &tempfile::TempDir) -> Database {
    contextdb_engine::database::open_with_startup_limits(
        root.path().join("measured.db"),
        std::sync::Arc::new(contextdb_engine::plugin::CorePlugin),
        Some(DECLARED_LIMIT),
        None,
    )
    .expect("open the measured store under a declared limit")
}

/// Open a durable store under a declared limit, insert `ROWS` rows whose text
/// column holds `length` bytes, and report what the accountant says the store
/// holds against what the process really took from just before the open.
fn measure_rows(length: usize, with_index: bool) -> Measured {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let live_before = live_bytes();
    let database = open_under_a_declared_limit(&root);
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
    let measured = Measured {
        charged: database.accountant().usage().used as i64,
        live: live_bytes() - live_before,
    };
    database.close().expect("close the measured store");
    measured
}

/// The same measurement for a graph edge carrying a text property. The text
/// lives in the PROPERTY rather than the edge type, because the property is
/// where a `Value::Text` is charged; an edge type is charged by its own arm,
/// which this measurement is not about.
fn measure_edges(length: usize) -> Measured {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let live_before = live_bytes();
    let database = open_under_a_declared_limit(&root);
    let payload = "x".repeat(length);
    let source = uuid::Uuid::new_v4();
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
    let measured = Measured {
        charged: database.accountant().usage().used as i64,
        live: live_bytes() - live_before,
    };
    database.close().expect("close the measured store");
    measured
}

fn judge(site: &str, length: usize, measured: &Measured, failures: &mut Vec<String>) {
    println!(
        "MEASURED {site} len={length}: charged={} live={}",
        measured.charged, measured.live
    );
    if measured.live as f64 > measured.charged as f64 * (1.0 + COVERAGE_TOLERANCE) {
        failures.push(format!(
            "{site} at length {length}: the process holds {} bytes and the accountant is told {}",
            measured.live, measured.charged
        ));
    }
}

#[test]
fn a_stored_text_value_is_charged_at_least_what_holding_it_costs() {
    let mut failures = Vec::new();

    for (name, with_index) in [("plain rows", false), ("rows under a text index", true)] {
        for length in LENGTHS {
            judge(
                name,
                length,
                &measure_rows(length, with_index),
                &mut failures,
            );
        }
    }
    for length in [16_usize, 100, 1_000] {
        judge("edges", length, &measure_edges(length), &mut failures);
    }

    assert!(
        failures.is_empty(),
        "the memory a store is allowed to hold is decided by this estimate, so an estimate \
         smaller than the cost admits more than the operator allowed:\n{}",
        failures.join("\n")
    );
}
