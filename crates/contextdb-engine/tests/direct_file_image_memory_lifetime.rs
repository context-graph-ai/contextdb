#![cfg(feature = "test-seams")]

use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_engine::direct_file_reader::{
    DirectFileReader, DirectFileReaderError, DirectReaderConfig, test_seams,
};
use contextdb_engine::{Database, MaintenancePolicy};
use serial_test::serial;
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct MeasuredAllocator;
static MEASURE: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
#[repr(C)]
struct Header {
    measured: bool,
    bytes: usize,
}
fn allocation_layout(layout: Layout) -> (Layout, usize) {
    let alignment = layout.align().max(std::mem::align_of::<Header>());
    let offset = std::mem::size_of::<Header>().next_multiple_of(alignment);
    (
        Layout::from_size_align(layout.size() + offset, alignment).unwrap(),
        offset,
    )
}
unsafe impl GlobalAlloc for MeasuredAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let (allocation, offset) = allocation_layout(layout);
        let base = unsafe { System.alloc(allocation) };
        if base.is_null() {
            return base;
        }
        let pointer = unsafe { base.add(offset) };
        let measured = MEASURE.load(Ordering::SeqCst);
        unsafe {
            pointer
                .sub(std::mem::size_of::<Header>())
                .cast::<Header>()
                .write(Header {
                    measured,
                    bytes: allocation.size(),
                });
        }
        if measured {
            let live = LIVE.fetch_add(allocation.size(), Ordering::SeqCst) + allocation.size();
            PEAK.fetch_max(live, Ordering::SeqCst);
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        let (allocation, offset) = allocation_layout(layout);
        let header = unsafe {
            pointer
                .sub(std::mem::size_of::<Header>())
                .cast::<Header>()
                .read()
        };
        if header.measured {
            LIVE.fetch_sub(header.bytes, Ordering::SeqCst);
        }
        unsafe {
            System.dealloc(pointer.sub(offset), allocation);
        }
    }
}
#[global_allocator]
static ALLOCATOR: MeasuredAllocator = MeasuredAllocator;

struct Clock;
impl DeadlineClock for Clock {
    fn now_ms(&self) -> u64 {
        0
    }
    fn wait_until(&self, _: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}
fn config(limit: usize) -> DirectReaderConfig {
    let mut config = DirectReaderConfig::new(ReadLimits::default(), Arc::new(Clock));
    config.memory_limit = Some(limit);
    config
}

#[test]
#[serial]
fn startup_and_retained_image_allocations_stay_admitted_until_the_last_cursor_releases() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("owned.redb");
    let db = Database::open(&path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE items (id INT PRIMARY KEY, body TEXT, embedding VECTOR(128))",
        &HashMap::new(),
    )
    .unwrap();
    let body = "\0\n\"\\".repeat(8 * 1024);
    for id in 0..128 {
        db.execute(
            "INSERT INTO items VALUES ($id, $body, $vector)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("body".into(), Value::Text(body.clone())),
                ("vector".into(), Value::Vector(vec![1.0; 128])),
            ]),
        )
        .unwrap();
    }
    db.close().unwrap();
    drop(db);
    let tiny = config(1);
    PEAK.store(0, Ordering::SeqCst);
    MEASURE.store(true, Ordering::SeqCst);
    let failure = DirectFileReader::open(&path, tiny);
    MEASURE.store(false, Ordering::SeqCst);
    assert!(matches!(
        failure,
        Err(DirectFileReaderError::MemoryBudget {
            budget_limit_bytes: 1,
            ..
        })
    ));
    assert!(
        PEAK.load(Ordering::SeqCst) < 64 * 1024,
        "a tiny ceiling refuses before source hydration"
    );
    drop(failure);

    for limit in [12, 32, 64].map(|mib| mib * 1024 * 1024) {
        let trial = config(limit);
        PEAK.store(LIVE.load(Ordering::SeqCst), Ordering::SeqCst);
        MEASURE.store(true, Ordering::SeqCst);
        let opened = DirectFileReader::open(&path, trial);
        let actual_peak = PEAK.load(Ordering::SeqCst);
        MEASURE.store(false, Ordering::SeqCst);
        assert!(
            actual_peak <= limit,
            "startup allocated {actual_peak} with ceiling {limit}"
        );
        match opened {
            Ok(reader) => {
                let (usage, _, _) = test_seams::image_memory_for_test(&reader);
                assert!(usage.used >= LIVE.load(Ordering::SeqCst));
            }
            Err(DirectFileReaderError::MemoryBudget {
                budget_limit_bytes, ..
            }) => assert_eq!(budget_limit_bytes, limit),
            Err(error) => panic!("unexpected startup result: {error}"),
        }
        println!("startup_ceiling={limit} actual_peak={actual_peak}");
    }

    let limit = 256 * 1024 * 1024;
    let admitted = config(limit);
    PEAK.store(LIVE.load(Ordering::SeqCst), Ordering::SeqCst);
    MEASURE.store(true, Ordering::SeqCst);
    let reader = DirectFileReader::open(&path, admitted).unwrap();
    let actual_retained = LIVE.load(Ordering::SeqCst);
    let actual_peak = PEAK.load(Ordering::SeqCst);
    MEASURE.store(false, Ordering::SeqCst);
    let (usage, image_charge, lifetime) = test_seams::image_memory_for_test(&reader);
    assert!(
        actual_peak <= limit,
        "startup peak {actual_peak} exceeded {limit}"
    );
    assert!(
        usage.used >= actual_retained,
        "retained {actual_retained} exceeds charged {}",
        usage.used
    );
    assert!(
        image_charge >= body.len() * 128,
        "the duplicate image has a retained owner"
    );
    println!(
        "actual_peak={actual_peak} actual_retained={actual_retained} charged={} image_charge={image_charge}",
        usage.used
    );
    let opened = reader
        .open_cursor(
            "SELECT id FROM items ORDER BY id",
            &HashMap::new(),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
    let mut cursor = opened.cursor;
    assert!(
        test_seams::cursor_resource_witness_for_test(&cursor)
            .unwrap()
            .snapshot_is_retained()
    );
    drop(reader);
    assert!(
        lifetime.upgrade().is_some(),
        "an active cursor retains the image charge"
    );
    cursor.close().unwrap();
    assert!(
        lifetime.upgrade().is_none(),
        "the last cursor releases the image and its charge together"
    );
    assert!(
        LIVE.load(Ordering::SeqCst) < 64 * 1024,
        "retained source allocations were released with their owner"
    );
}

#[test]
#[serial]
fn one_oversized_durable_value_is_refused_before_its_source_buffer_is_allocated() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("oversized.redb");
    let db = Database::open(&path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE items (id INT PRIMARY KEY, body TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO items VALUES (1, $body)",
        &HashMap::from([("body".into(), Value::Text("x".repeat(16 * 1024 * 1024)))]),
    )
    .unwrap();
    db.close().unwrap();
    drop(db);
    let limit = 8 * 1024 * 1024;
    let configuration = config(limit);
    PEAK.store(LIVE.load(Ordering::SeqCst), Ordering::SeqCst);
    MEASURE.store(true, Ordering::SeqCst);
    let result = DirectFileReader::open(&path, configuration);
    MEASURE.store(false, Ordering::SeqCst);
    let peak = PEAK.load(Ordering::SeqCst);
    println!("oversized_source_peak={peak} ceiling={limit}");
    assert!(matches!(
        result,
        Err(DirectFileReaderError::MemoryBudget { .. })
    ));
    assert!(
        peak <= limit,
        "source allocation exceeded admission: {peak} > {limit}"
    );
}
