//! An in-transaction count served from a declared count holds no memory that
//! grows with the transaction's staged rows. It walks the staged identities
//! it probes one at a time; collecting them first would hold eight bytes per
//! staged row before the first charge, outside any budget.
//!
//! One test in this binary, so the process-wide peak is this statement's.
#![cfg(feature = "test-seams")]

use contextdb_core::Value;
use contextdb_engine::{Database, MaintenancePolicy};
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::sync::atomic::{AtomicIsize, Ordering};
use uuid::Uuid;

static LIVE_BYTES: AtomicIsize = AtomicIsize::new(0);
static PEAK_BYTES: AtomicIsize = AtomicIsize::new(0);

struct PeakTrackingAllocator;

fn note_growth(delta: isize) {
    let live = LIVE_BYTES.fetch_add(delta, Ordering::Relaxed) + delta;
    PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for PeakTrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note_growth(layout.size() as isize);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note_growth(layout.size() as isize);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note_growth(new_size as isize - layout.size() as isize);
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: PeakTrackingAllocator = PeakTrackingAllocator;

const STAGED: u128 = 40_000;
/// Fixed statement overhead (parse, plan, overlay controls, the answer); a
/// collection of the staged identities alone would be 8 x 40,000 bytes.
const SLACK_BYTES: isize = 128 * 1024;

fn params(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).into(), value.clone()))
        .collect()
}

fn uuid(n: u128) -> Value {
    Value::Uuid(Uuid::from_u128(n))
}

#[test]
fn an_in_transaction_count_holds_no_memory_per_staged_row() {
    let directory = tempfile::tempdir().unwrap();
    let db = Database::open(directory.path().join("in-tx-count-memory.redb")).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    for statement in [
        "CREATE TABLE events (id UUID PRIMARY KEY, context_id UUID NOT NULL, body TEXT NOT NULL) SYNC OFF",
        "CREATE INDEX idx_events_context ON events (context_id, id)",
        "ALTER TABLE events SET MAINTAIN COUNT (context_id)",
    ] {
        db.execute(statement, &HashMap::new()).unwrap();
    }
    let insert = "INSERT INTO events (id, context_id, body) VALUES ($id, $context_id, 'row')";
    for n in 0..10 {
        db.execute(insert, &params(&[("id", uuid(n)), ("context_id", uuid(1))]))
            .unwrap();
    }
    let tx = db.begin().unwrap();
    for n in 0..STAGED {
        db.execute_in_tx(
            tx,
            insert,
            &params(&[("id", uuid(1_000_000 + n)), ("context_id", uuid(2))]),
        )
        .unwrap();
    }
    let before = LIVE_BYTES.load(Ordering::Relaxed);
    PEAK_BYTES.store(before, Ordering::Relaxed);
    let answer = db.execute_in_tx(
        tx,
        "SELECT COUNT(*) FROM events WHERE context_id = $context_id",
        &params(&[("context_id", uuid(1))]),
    );
    let peak_growth = PEAK_BYTES.load(Ordering::Relaxed) - before;
    println!(
        "in-tx count over {STAGED} staged rows: peak growth {peak_growth} B, answer {answer:?}"
    );
    match answer {
        Ok(answer) => assert_eq!(answer.rows, vec![vec![Value::Int64(10)]]),
        Err(error) => assert!(
            matches!(
                error,
                contextdb_core::Error::MemoryBudgetExceeded { .. }
                    | contextdb_core::Error::ReadFailure(_)
            ),
            "a refusal is a typed budget refusal: {error:?}"
        ),
    }
    assert!(
        peak_growth < SLACK_BYTES,
        "the count held {peak_growth} B while its transaction staged {STAGED} rows; \
         nothing it holds may grow with the staged rows"
    );
    db.rollback(tx).unwrap();
}
