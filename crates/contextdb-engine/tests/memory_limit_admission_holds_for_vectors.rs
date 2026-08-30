//! An operator's memory limit means what it says about vectors and documents.
//!
//! `SET MEMORY_LIMIT` is a promise: the store will not hold more than this.
//! The promise is kept by an estimate of what each stored value costs, so
//! these pin the estimate against the ALLOCATOR rather than against the
//! accountant's own word -- admission stops before the process is over the
//! limit rather than long after it, an abandoned batch gives back exactly
//! what it took, a store reopened from disk accounts for what it loads the
//! same way it accounted for writing it, and deleting what was admitted gives
//! back what admitting it took.
//!
//! A graph edge is retained in both adjacency directions, so an edge property
//! carrying a vector or a document is the sharpest test of the estimate: the
//! payload is large, and it is held more than once.

use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use serial_test::serial;
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::sync::atomic::{AtomicIsize, Ordering};
use uuid::Uuid;

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

const LIMIT_BYTES: u64 = 4 * 1024 * 1024;
/// Big enough that the payload, not the per-edge bookkeeping around it,
/// decides what the store holds.
const DIMENSIONS: usize = 1_024;
const DOCUMENT_BYTES: usize = 1_000;
/// A batch small enough to fit any limit these use, so the batch arms measure
/// give-back rather than refusal.
const BATCH: usize = 64;

/// The two variable-sized property shapes an edge can carry.
#[derive(Clone, Copy)]
enum Payload {
    Vector,
    Document,
}

impl Payload {
    fn name(self) -> &'static str {
        match self {
            Payload::Vector => "vector",
            Payload::Document => "document",
        }
    }

    fn properties(self) -> HashMap<String, Value> {
        match self {
            Payload::Vector => HashMap::from([(
                "embedding".to_owned(),
                Value::Vector((0..DIMENSIONS).map(|index| index as f32).collect()),
            )]),
            Payload::Document => HashMap::from([(
                "document".to_owned(),
                Value::Json(serde_json::Value::String("x".repeat(DOCUMENT_BYTES))),
            )]),
        }
    }
}

fn used(database: &Database) -> u64 {
    database.accountant().usage().used as u64
}

fn insert_edge(
    database: &Database,
    tx: contextdb_core::TxId,
    payload: Payload,
    target: Uuid,
) -> contextdb_core::Result<bool> {
    database.insert_edge(
        tx,
        source_node(),
        target,
        "carries".to_owned(),
        payload.properties(),
    )
}

/// One fixed source node, so every edge lands in the same forward adjacency
/// list and every target in its own reverse one.
fn source_node() -> Uuid {
    Uuid::from_u128(0x5ce7_0000_0000_0000_0000_0000_0000_0001)
}

fn is_memory_refusal(error: &Error) -> bool {
    error.to_string().to_lowercase().contains("memory")
}

fn limited() -> Database {
    let database = Database::open_memory();
    database
        .execute("SET MEMORY_LIMIT '4M'", &HashMap::new())
        .expect("set the memory limit");
    database
}

fn admission_holds(payload: Payload) -> Vec<String> {
    let mut failures = Vec::new();
    let database = limited();
    let live_before = live_bytes();

    let mut admitted = 0_u64;
    let mut worst_held = 0_i64;
    // Every edge is committed on its own, because an edge only enters both
    // adjacency directions when its batch commits -- what a store holds while
    // a batch is still open is not what it holds once the batch lands.
    let refusal = loop {
        let tx = database.begin().expect("begin an edge write");
        match insert_edge(&database, tx, payload, Uuid::new_v4()) {
            Ok(_) => {
                database.commit(tx).expect("commit an edge write");
                admitted += 1;
                worst_held = worst_held.max(live_bytes() - live_before);
                assert!(
                    admitted < 100_000,
                    "a four-mebibyte limit must stop admitting {} edges long before this",
                    payload.name()
                );
            }
            Err(error) => {
                let _ = database.rollback(tx);
                break error;
            }
        }
    };
    let charged = used(&database);
    println!(
        "MEASURED {} admission: admitted={admitted} charged={charged} held={worst_held} \
         limit={LIMIT_BYTES}",
        payload.name()
    );

    if !is_memory_refusal(&refusal) {
        failures.push(format!(
            "what stops a store admitting {} edges is the memory limit, said plainly: {refusal}",
            payload.name()
        ));
    }
    if admitted == 0 {
        failures.push(format!(
            "the limit is roomy enough to admit real {} edges before it refuses",
            payload.name()
        ));
    }
    if charged > LIMIT_BYTES {
        failures.push(format!(
            "the accountant says the store holds {charged} of a {LIMIT_BYTES} limit after {} \
             edges",
            payload.name()
        ));
    }
    if worst_held > LIMIT_BYTES as i64 {
        failures.push(format!(
            "the process really held {worst_held} bytes of {} edges under a {LIMIT_BYTES} limit, \
             so the limit admitted more than it allowed",
            payload.name()
        ));
    }
    failures
}

#[test]
#[serial]
fn admission_stops_before_the_process_is_over_the_limit() {
    let mut failures = admission_holds(Payload::Vector);
    failures.extend(admission_holds(Payload::Document));
    assert!(
        failures.is_empty(),
        "a memory limit is a promise about what the process holds, not about what the accountant \
         believes:\n{}",
        failures.join("\n")
    );
}

fn abandoned_batch_gives_back(payload: Payload) -> Option<String> {
    let database = Database::open_memory();
    let before = used(&database);

    let tx = database.begin().expect("begin the batch");
    for _ in 0..BATCH {
        insert_edge(&database, tx, payload, Uuid::new_v4()).expect("stage an edge");
    }
    let staged = used(&database);
    assert!(
        staged > before,
        "staging {} edges costs the store something: {before} then {staged}",
        payload.name()
    );

    database.rollback(tx).expect("abandon the batch");
    let after = used(&database);
    println!(
        "MEASURED {} abandoned batch: before={before} staged={staged} after={after}",
        payload.name()
    );
    (after != before).then(|| {
        format!(
            "an abandoned batch of {} edges returns the store to exactly what it held before it: \
             before {before}, after {after}",
            payload.name()
        )
    })
}

#[test]
#[serial]
fn an_abandoned_batch_gives_back_exactly_what_it_took() {
    let failures: Vec<String> = [Payload::Vector, Payload::Document]
        .into_iter()
        .filter_map(abandoned_batch_gives_back)
        .collect();
    assert!(
        failures.is_empty(),
        "what a batch took must come back when the batch is abandoned, or a limit shrinks with \
         every rolled-back write:\n{}",
        failures.join("\n")
    );
}

fn reopened_charges_what_writing_charged(payload: Payload) -> Option<String> {
    let directory = tempfile::tempdir().expect("task-scoped store directory");
    let path = directory.path().join("reopened.db");

    let written = {
        let database = Database::open(&path).expect("open the store for writing");
        let tx = database.begin().expect("begin the edge batch");
        for _ in 0..BATCH {
            insert_edge(&database, tx, payload, Uuid::new_v4()).expect("write an edge");
        }
        database.commit(tx).expect("commit the edge batch");
        let written = used(&database);
        database.close().expect("close the store");
        written
    };

    let reopened = Database::open(&path).expect("reopen the store");
    let loaded = used(&reopened);
    reopened.close().expect("close the reopened store");
    println!(
        "MEASURED {} reopen: wrote={written} loaded={loaded}",
        payload.name()
    );
    (loaded != written).then(|| {
        format!(
            "loading {} edges charges what writing them charged, so a limit means the same thing \
             across a restart: wrote {written}, loaded {loaded}",
            payload.name()
        )
    })
}

#[test]
#[serial]
fn a_store_reopened_from_disk_charges_what_writing_it_charged() {
    let failures: Vec<String> = [Payload::Vector, Payload::Document]
        .into_iter()
        .filter_map(reopened_charges_what_writing_charged)
        .collect();
    assert!(
        failures.is_empty(),
        "a limit that means one thing before a restart and another after it is not the limit the \
         operator set:\n{}",
        failures.join("\n")
    );
}

fn delete_gives_back(payload: Payload) -> Vec<String> {
    let mut failures = Vec::new();
    let database = Database::open_memory();
    let before = used(&database);
    let live_before = live_bytes();

    let targets: Vec<Uuid> = (0..BATCH).map(|_| Uuid::new_v4()).collect();
    let tx = database.begin().expect("begin the edge batch");
    for target in &targets {
        insert_edge(&database, tx, payload, *target).expect("write an edge");
    }
    database.commit(tx).expect("commit the edge batch");
    let admitted = used(&database);

    let tx = database.begin().expect("begin the removal");
    for target in &targets {
        database
            .delete_edge(tx, source_node(), *target, "carries")
            .expect("remove an edge");
    }
    database.commit(tx).expect("commit the removal");
    let after = used(&database);
    let live_after = live_bytes();
    let still_held = live_after - live_before;
    println!(
        "MEASURED {} delete: before={before} admitted={admitted} after={after} \
         still-held={still_held}",
        payload.name()
    );

    if after > admitted {
        failures.push(format!(
            "removing {} edges cannot cost the store more than admitting them did: admitted \
             {admitted}, after removal {after}",
            payload.name()
        ));
    }
    if (after as i64) < still_held {
        failures.push(format!(
            "after removing {} edges the store still holds {still_held} bytes and the accountant \
             is told {after}, so the limit no longer covers what the process holds",
            payload.name()
        ));
    }
    failures
}

#[test]
#[serial]
fn removing_what_was_admitted_leaves_the_accountant_covering_what_is_still_held() {
    let mut failures = delete_gives_back(Payload::Vector);
    failures.extend(delete_gives_back(Payload::Document));
    assert!(
        failures.is_empty(),
        "a store that never gives back what a removal freed shrinks its own limit, and one that \
         gives back more than it freed stops covering what it holds:\n{}",
        failures.join("\n")
    );
}
