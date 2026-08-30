//! What a stored property value really costs, measured rather than asserted,
//! for every variable-sized value a graph edge can carry.
//!
//! The memory a store is allowed to hold is decided by an ESTIMATE of what it
//! is holding. An estimate that is too small does not fail loudly: it admits
//! more than the operator allowed and the process quietly holds more than it
//! said it would. An estimate that is far too large fails the other way -- a
//! limit that refuses work the machine could easily have done is not the
//! limit the operator asked for. So the estimate is checked against the
//! allocator from both sides.
//!
//! The measurement is a SLOPE, not a total. A store holds far more than the
//! values it was given -- indexes, a change log, its own bookkeeping -- so
//! comparing one total against another proves nothing. What each arm claims
//! is a cost PER PAYLOAD BYTE; growing the payload and watching how much more
//! the process holds per added byte is exactly that claim, and it is immune
//! to every fixed cost around it.
//!
//! An edge is retained in BOTH adjacency directions and each direction keeps
//! the whole property map, so every byte of an edge property -- the value's
//! payload and the property KEY alike -- is held twice. A plain row holds its
//! value once, and is measured beside each edge arm as the control.

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

/// How far above the truth an estimate may sit. A store that charges itself
/// more than four times what it holds cannot hold what its limit says it can:
/// an operator who sets a gibibyte gets a quarter of one, with no way to see
/// why. Four is generous headroom for per-value bookkeeping and still catches
/// an estimate that is off by a factor.
const CHARGE_CEILING_MULTIPLE: f64 = 4.0;

struct Measured {
    charged_per_row: f64,
    live_per_row: f64,
}

struct Sample {
    payload_bytes: usize,
    measured: Measured,
}

fn charge(database: &Database) -> i64 {
    database.accountant().usage().used as i64
}

/// Insert `ROWS` edges each carrying the given property map, and report what
/// the accountant was told each edge cost against what the process really
/// took for it.
fn measure_edges(properties: &dyn Fn() -> HashMap<String, Value>) -> Measured {
    let database = Database::open_memory();
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
                properties(),
            )
            .expect("insert an edge carrying the measured property");
    }
    database.commit(tx).expect("commit the edge batch");
    let charged_after = charge(&database);
    let live_after = live_bytes();

    Measured {
        charged_per_row: (charged_after - charged_before) as f64 / ROWS as f64,
        live_per_row: (live_after - live_before) as f64 / ROWS as f64,
    }
}

/// The same measurement for a plain relational row, which holds its value
/// once. This is the control every edge arm is read against.
fn measure_rows(column_type: &str, value: &dyn Fn() -> Value) -> Measured {
    let database = Database::open_memory();
    database
        .execute(
            &format!("CREATE TABLE measured (id UUID PRIMARY KEY, payload {column_type})"),
            &HashMap::new(),
        )
        .expect("create the measurement table");

    let charged_before = charge(&database);
    let live_before = live_bytes();
    for _ in 0..ROWS {
        database
            .execute(
                "INSERT INTO measured (id, payload) VALUES ($id, $payload)",
                &HashMap::from([
                    ("id".to_owned(), Value::Uuid(uuid::Uuid::new_v4())),
                    ("payload".to_owned(), value()),
                ]),
            )
            .expect("insert a row carrying the measured property");
    }
    let charged_after = charge(&database);
    let live_after = live_bytes();

    Measured {
        charged_per_row: (charged_after - charged_before) as f64 / ROWS as f64,
        live_per_row: (live_after - live_before) as f64 / ROWS as f64,
    }
}

fn text_value(length: usize) -> Value {
    Value::Text("x".repeat(length))
}

fn json_value(length: usize) -> Value {
    Value::Json(serde_json::Value::String("x".repeat(length)))
}

fn json_payload_bytes(length: usize) -> usize {
    match json_value(length) {
        Value::Json(rendered) => rendered.to_string().len(),
        _ => unreachable!("the json arm builds a json value"),
    }
}

fn vector_value(dimensions: usize) -> Value {
    Value::Vector((0..dimensions).map(|index| index as f32).collect())
}

fn vector_payload_bytes(dimensions: usize) -> usize {
    dimensions * std::mem::size_of::<f32>()
}

/// A property map of `KEYS_PER_EDGE` distinct keys of the given length, each
/// holding a fixed-size value. Only the KEY bytes grow, so the slope this
/// produces is the cost of a property NAME.
const KEYS_PER_EDGE: usize = 32;

fn keyed_properties(key_length: usize) -> HashMap<String, Value> {
    (0..KEYS_PER_EDGE)
        .map(|ordinal| {
            let mut key = format!("{ordinal:03}");
            key.push_str(&"k".repeat(key_length.saturating_sub(key.len())));
            (key, Value::Int64(ordinal as i64))
        })
        .collect()
}

fn keyed_payload_bytes(key_length: usize) -> usize {
    keyed_properties(key_length)
        .keys()
        .map(|key| key.len())
        .sum()
}

/// Compare every sample against the site's baseline and record what the two
/// bounds say about the slope between them.
fn judge(site: &str, samples: &[Sample], failures: &mut Vec<String>) {
    let baseline = &samples[0];
    println!(
        "MEASURED {site} payload={}B: charged/row={:.1} live/row={:.1}",
        baseline.payload_bytes, baseline.measured.charged_per_row, baseline.measured.live_per_row
    );
    for sample in &samples[1..] {
        let added = (sample.payload_bytes - baseline.payload_bytes) as f64;
        let charged_slope =
            (sample.measured.charged_per_row - baseline.measured.charged_per_row) / added;
        let live_slope = (sample.measured.live_per_row - baseline.measured.live_per_row) / added;
        println!(
            "MEASURED {site} payload={}B: charged/row={:.1} live/row={:.1} \
             charged-per-added-byte={charged_slope:.3} live-per-added-byte={live_slope:.3}",
            sample.payload_bytes, sample.measured.charged_per_row, sample.measured.live_per_row
        );
        if live_slope > charged_slope + SLOPE_TOLERANCE {
            failures.push(format!(
                "{site} at payload {}B: the process takes {live_slope:.3} bytes for every payload \
                 byte added and the accountant is told {charged_slope:.3}",
                sample.payload_bytes
            ));
        }
        if live_slope > 0.0
            && charged_slope > CHARGE_CEILING_MULTIPLE * live_slope + SLOPE_TOLERANCE
        {
            failures.push(format!(
                "{site} at payload {}B: the accountant is told {charged_slope:.3} bytes for every \
                 payload byte added and the process takes {live_slope:.3}, so a limit holds less \
                 than a {CHARGE_CEILING_MULTIPLE:.0}th of what it says",
                sample.payload_bytes
            ));
        }
    }
}

fn edge_samples(
    sizes: &[usize],
    payload_bytes: &dyn Fn(usize) -> usize,
    properties: &dyn Fn(usize) -> HashMap<String, Value>,
) -> Vec<Sample> {
    sizes
        .iter()
        .map(|size| Sample {
            payload_bytes: payload_bytes(*size),
            measured: measure_edges(&|| properties(*size)),
        })
        .collect()
}

fn row_samples(
    sizes: &[usize],
    column_type: &dyn Fn(usize) -> String,
    payload_bytes: &dyn Fn(usize) -> usize,
    value: &dyn Fn(usize) -> Value,
) -> Vec<Sample> {
    sizes
        .iter()
        .map(|size| Sample {
            payload_bytes: payload_bytes(*size),
            measured: measure_rows(&column_type(*size), &|| value(*size)),
        })
        .collect()
}

const TEXT_LENGTHS: [usize; 4] = [16, 100, 1_000, 100_000];
const JSON_LENGTHS: [usize; 4] = [16, 100, 1_000, 100_000];
const VECTOR_DIMENSIONS: [usize; 4] = [4, 64, 1_024, 16_384];
const KEY_LENGTHS: [usize; 4] = [4, 16, 64, 256];

#[test]
fn a_stored_property_value_is_charged_what_holding_it_costs() {
    let mut failures = Vec::new();

    judge(
        "edge property, vector",
        &edge_samples(&VECTOR_DIMENSIONS, &vector_payload_bytes, &|dimensions| {
            HashMap::from([("embedding".to_owned(), vector_value(dimensions))])
        }),
        &mut failures,
    );
    judge(
        "plain row, vector",
        &row_samples(
            &VECTOR_DIMENSIONS,
            &|dimensions| format!("VECTOR({dimensions})"),
            &vector_payload_bytes,
            &vector_value,
        ),
        &mut failures,
    );

    judge(
        "edge property, json",
        &edge_samples(&JSON_LENGTHS, &json_payload_bytes, &|length| {
            HashMap::from([("document".to_owned(), json_value(length))])
        }),
        &mut failures,
    );
    judge(
        "plain row, json",
        &row_samples(
            &JSON_LENGTHS,
            &|_| "JSON".to_owned(),
            &json_payload_bytes,
            &json_value,
        ),
        &mut failures,
    );

    judge(
        "edge property, text",
        &edge_samples(&TEXT_LENGTHS, &|length| length, &|length| {
            HashMap::from([("note".to_owned(), text_value(length))])
        }),
        &mut failures,
    );

    judge(
        "edge property names",
        &edge_samples(&KEY_LENGTHS, &keyed_payload_bytes, &keyed_properties),
        &mut failures,
    );

    assert!(
        failures.is_empty(),
        "the memory a store is allowed to hold is decided by these estimates, so an estimate \
         below the cost admits more than the operator allowed and an estimate far above it \
         refuses what the operator allowed:\n{}",
        failures.join("\n")
    );
}
