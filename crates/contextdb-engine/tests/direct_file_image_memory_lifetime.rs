use contextdb_core::Value;
use contextdb_core::read_contract::{
    DeadlineClock, DeadlineWait, ReadFailureDetail, ReadFailureKind, ReadFailureLimit, ReadLimits,
};
use contextdb_engine::direct_file_reader::{
    DirectFileReader, DirectFileReaderError, DirectReaderConfig, test_seams,
};
use contextdb_engine::{Database, MaintenancePolicy, ReadSession, ReadSessionOptions};
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
        let free =
            contextdb_engine::read_work_probe::allocator_events::before_dealloc(pointer, layout);
        unsafe {
            System.dealloc(pointer.sub(offset), allocation);
        }
        contextdb_engine::read_work_probe::allocator_events::after_dealloc(free);
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
        lifetime.is_alive(),
        "an active cursor retains the image charge"
    );
    cursor.close().unwrap();
    assert!(
        !lifetime.is_alive(),
        "the last cursor releases the image and its charge together"
    );
    assert!(
        lifetime.is_physically_freed(),
        "actual allocator free must precede lifetime completion"
    );
    assert!(
        lifetime.accountant_is_physically_freed(),
        "the original accountant also physically freed"
    );
    assert!(
        LIVE.load(Ordering::SeqCst) < 64 * 1024,
        "retained source allocations were released with their owner"
    );
}

fn read_limits_at(limit: u64) -> ReadLimits {
    let mut limits = ReadLimits::default();
    limits.memory = limit;
    limits.result_bytes = limits.result_bytes.max(limit);
    limits.cursor_page_bytes = limits.cursor_page_bytes.max(limit);
    limits
}

fn assert_typed_memory_refusal<T: std::fmt::Debug>(result: &Result<T, contextdb_core::Error>) {
    match result {
        Err(contextdb_core::Error::ReadFailure(failure)) => {
            assert_eq!(failure.kind(), ReadFailureKind::OwnerLimitExceeded);
            match failure.detail() {
                ReadFailureDetail::OwnerLimitExceeded(detail) => {
                    assert_eq!(detail.limit, ReadFailureLimit::Memory);
                }
                other => panic!("expected an owner-limit detail, got {other:?}"),
            }
        }
        other => panic!("expected a typed memory refusal, got {other:?}"),
    }
}

/// The on-demand reader opens a store without loading an unread large value.
/// Opening under the limit succeeds because the value is not read at open.
/// The refusal belongs to the request that reads the value's body, on
/// both the direct-file route and the owner route, and it still arrives
/// before that value's source buffer is allocated.
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
    let body_len: usize = 16 * 1024 * 1024;
    db.execute(
        "INSERT INTO items VALUES (1, $body)",
        &HashMap::from([("body".into(), Value::Text("x".repeat(body_len)))]),
    )
    .unwrap();
    db.close().unwrap();
    drop(db);

    let limit = 8u64 * 1024 * 1024;
    // Far below the 16 MiB value: any allocation of its body, not merely a
    // slight overshoot, trips this. Observed refusal peaks were 7,816 B on
    // the file route and 11,946 B on the owner route.
    let source_ceiling: usize = 1024 * 1024;

    // --- Direct-file route ---
    let file_options = ReadSessionOptions {
        memory_limit: Some(limit as usize),
        limits: read_limits_at(limit),
        ..Default::default()
    };

    // (1) Opening under the limit succeeds: the on-demand reader no longer
    // loads the unread value at open (observed peak about 194 KB).
    PEAK.store(LIVE.load(Ordering::SeqCst), Ordering::SeqCst);
    MEASURE.store(true, Ordering::SeqCst);
    let file_session = ReadSession::open_with_options(&path, file_options).unwrap();
    let file_open_peak = PEAK.load(Ordering::SeqCst);
    MEASURE.store(false, Ordering::SeqCst);
    println!("file_open_peak={file_open_peak} limit={limit}");
    assert!(
        file_open_peak <= limit as usize,
        "open under the limit peaked at {file_open_peak}, ceiling {limit}"
    );

    let file_floor_live = LIVE.load(Ordering::SeqCst);
    let file_floor_charged = file_session.charged_bytes().unwrap();

    // (2) + (3) the request that reads the value's body is refused with the
    // typed memory refusal, and the counting allocator shows no allocation
    // anywhere near the body's size while that request runs.
    PEAK.store(file_floor_live, Ordering::SeqCst);
    MEASURE.store(true, Ordering::SeqCst);
    let file_result = file_session.execute("SELECT body FROM items", &HashMap::new());
    let file_read_peak = PEAK.load(Ordering::SeqCst);
    MEASURE.store(false, Ordering::SeqCst);
    println!(
        "file_read_peak_over_floor={} ceiling={source_ceiling}",
        file_read_peak - file_floor_live
    );
    assert_typed_memory_refusal(&file_result);
    assert!(
        file_read_peak - file_floor_live <= source_ceiling,
        "the refused file-route read allocated near the value's body: {} > {source_ceiling}",
        file_read_peak - file_floor_live
    );
    drop(file_result);

    // (4) after the refusal, nothing new was charged, and allocation
    // returned to the post-open floor.
    let file_after_live = LIVE.load(Ordering::SeqCst);
    let file_after_charged = file_session.charged_bytes().unwrap();
    assert_eq!(
        file_after_charged, file_floor_charged,
        "a file-route refusal leaves no charge behind"
    );
    assert!(
        file_after_live <= file_floor_live + 4096,
        "file-route allocation did not return to the post-open floor: {file_after_live} vs {file_floor_live}"
    );
    drop(file_session);

    // --- Owner route: a reopened writer serves the read ---
    let writer = Database::open(&path).unwrap();
    writer.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let owner_options = ReadSessionOptions {
        limits: read_limits_at(limit),
        ..Default::default()
    };

    PEAK.store(LIVE.load(Ordering::SeqCst), Ordering::SeqCst);
    MEASURE.store(true, Ordering::SeqCst);
    let owner_session = ReadSession::open_owner_only(&path, owner_options).unwrap();
    let owner_open_peak = PEAK.load(Ordering::SeqCst);
    MEASURE.store(false, Ordering::SeqCst);
    println!("owner_open_peak={owner_open_peak}");

    let owner_floor_live = LIVE.load(Ordering::SeqCst);
    let owner_floor_charged = owner_session.charged_bytes().unwrap();

    PEAK.store(owner_floor_live, Ordering::SeqCst);
    MEASURE.store(true, Ordering::SeqCst);
    let owner_result = owner_session.execute("SELECT body FROM items", &HashMap::new());
    let owner_read_peak = PEAK.load(Ordering::SeqCst);
    MEASURE.store(false, Ordering::SeqCst);
    println!(
        "owner_read_peak_over_floor={} ceiling={source_ceiling}",
        owner_read_peak - owner_floor_live
    );
    assert_typed_memory_refusal(&owner_result);
    assert!(
        owner_read_peak - owner_floor_live <= source_ceiling,
        "the refused owner-route read allocated near the value's body: {} > {source_ceiling}",
        owner_read_peak - owner_floor_live
    );
    drop(owner_result);

    let owner_after_live = LIVE.load(Ordering::SeqCst);
    let owner_after_charged = owner_session.charged_bytes().unwrap();
    assert_eq!(
        owner_after_charged, owner_floor_charged,
        "an owner-route refusal leaves no charge behind"
    );
    assert!(
        owner_after_live <= owner_floor_live + 4096,
        "owner-route allocation did not return to the post-open floor: {owner_after_live} vs {owner_floor_live}"
    );

    drop(owner_session);
    writer.close().unwrap();
}

/// A value that fits inside
/// its session's memory limit still reads successfully, peaks at a bounded
/// multiple of its own size, and the storage page it lands in stays charged
/// for as long as anything holds it -- even after the query result itself is
/// dropped -- releasing only when the session drops. A 6 MiB value's 8 MiB
/// page charges 8,388,736 B, held and charged before and after the result
/// drops.
#[test]
#[serial]
fn a_read_within_limit_succeeds_and_keeps_its_page_charged_until_the_session_drops() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("large.redb");
    let db = Database::open(&path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE items (id INT PRIMARY KEY, body TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    let body_len: usize = 6 * 1024 * 1024;
    db.execute(
        "INSERT INTO items VALUES (1, $body)",
        &HashMap::from([("body".into(), Value::Text("x".repeat(body_len)))]),
    )
    .unwrap();
    db.close().unwrap();
    drop(db);

    let limit = 64u64 * 1024 * 1024;
    let options = ReadSessionOptions {
        memory_limit: Some(limit as usize),
        limits: read_limits_at(limit),
        ..Default::default()
    };

    let pre_open_live = LIVE.load(Ordering::SeqCst);
    let session = ReadSession::open_with_options(&path, options).unwrap();
    let floor_live = LIVE.load(Ordering::SeqCst);

    PEAK.store(floor_live, Ordering::SeqCst);
    MEASURE.store(true, Ordering::SeqCst);
    let result = session.execute("SELECT body FROM items", &HashMap::new());
    let read_peak = PEAK.load(Ordering::SeqCst);
    MEASURE.store(false, Ordering::SeqCst);
    let result = result.expect("a read within the memory limit succeeds");
    assert_eq!(result.rows().len(), 1);

    let peak_over_floor = read_peak - floor_live;
    println!(
        "large_read_peak_over_floor={peak_over_floor} body_len={body_len} ratio={:.2}",
        peak_over_floor as f64 / body_len as f64
    );
    assert!(
        peak_over_floor <= (body_len as f64 * 5.0) as usize,
        "a successful read peaked at {peak_over_floor}, more than 5.0x the {body_len}-byte value"
    );

    // The page the value lives in is still held (the direct-file physical
    // scan keeps its table, and the page it holds, open for the reader's
    // whole lifetime), so it stays charged even after this result drops.
    drop(result);
    let charged_after_drop = session.charged_bytes().unwrap();
    assert!(
        charged_after_drop >= body_len as u64,
        "the still-held page's charge {charged_after_drop} is below the value's own size {body_len}"
    );

    // Dropping the session releases the page and its charge; allocation
    // returns to baseline.
    drop(session);
    let after_session_live = LIVE.load(Ordering::SeqCst);
    assert!(
        after_session_live <= pre_open_live + 4096,
        "dropping the session did not return allocation to baseline: {after_session_live} vs {pre_open_live}"
    );
}
