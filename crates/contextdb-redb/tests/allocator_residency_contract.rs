//! Fork-local allocator residency and recovery contracts. The large values
//! force separate regions while every assertion uses identities or counters,
//! never elapsed time. Run with `--features test-seams`.

#![cfg(feature = "test-seams")]

use redb::backends::{FileBackend, InMemoryBackend};
use redb::{Durability, ReadableDatabase, ScratchBackend, StorageBackend, TableDefinition};
use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

// Independent heap observation for this synchronous backend. The measured
// scope owns every object it allocates and drops them before ending; the exact
// zero at the end catches accidental inclusion of pre-existing allocations.
// This observes actual allocation sizes, including directories and non-paged
// bitmap payload, rather than the allocator's fixed pager residency estimate.
struct ObservedHeap;
thread_local! {
    static OBSERVE_HEAP: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static OWNED_HEAP: std::cell::Cell<isize> = const { std::cell::Cell::new(0) };
}
fn note_heap(bytes: isize) {
    let _ = OBSERVE_HEAP.try_with(|active| {
        if active.get() {
            OWNED_HEAP.with(|owned| owned.set(owned.get() + bytes));
        }
    });
}
unsafe impl std::alloc::GlobalAlloc for ObservedHeap {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let pointer = unsafe { std::alloc::GlobalAlloc::alloc(&std::alloc::System, layout) };
        if !pointer.is_null() {
            note_heap(layout.size() as isize);
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: std::alloc::Layout) -> *mut u8 {
        let pointer = unsafe { std::alloc::GlobalAlloc::alloc_zeroed(&std::alloc::System, layout) };
        if !pointer.is_null() {
            note_heap(layout.size() as isize);
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: std::alloc::Layout) {
        note_heap(-(layout.size() as isize));
        unsafe {
            std::alloc::GlobalAlloc::dealloc(&std::alloc::System, pointer, layout);
        }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: std::alloc::Layout, size: usize) -> *mut u8 {
        let resized =
            unsafe { std::alloc::GlobalAlloc::realloc(&std::alloc::System, pointer, layout, size) };
        if !resized.is_null() {
            note_heap(size as isize - layout.size() as isize);
        }
        resized
    }
}
#[global_allocator]
static OBSERVED_HEAP: ObservedHeap = ObservedHeap;
struct HeapScope;
impl HeapScope {
    fn start() -> Self {
        assert!(!OBSERVE_HEAP.with(|active| active.replace(true)));
        OWNED_HEAP.with(|owned| owned.set(0));
        Self
    }
    fn retained(&self) -> usize {
        usize::try_from(OWNED_HEAP.with(std::cell::Cell::get))
            .expect("the measured scope owns its heap")
    }
    fn finish(self) {
        let remaining = OWNED_HEAP.with(std::cell::Cell::get);
        drop(self);
        assert_eq!(
            remaining, 0,
            "the complete measured store releases every observed allocation"
        );
    }
}
impl Drop for HeapScope {
    fn drop(&mut self) {
        OBSERVE_HEAP.with(|active| active.set(false));
    }
}

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("allocator_regions");
const REGION_BYTES: u64 = 1 << 20;
const VALUE_BYTES: usize = 3 << 18;
const RESIDENT_LIMIT: usize = 256 << 10;

fn builder() -> redb::Builder {
    let mut builder = redb::Builder::new();
    builder
        .set_region_size(REGION_BYTES)
        .set_cache_size(2 << 20);
    builder
}

fn value(marker: u8) -> Vec<u8> {
    vec![marker; VALUE_BYTES]
}

fn assert_row(db: &redb::Database, key: u64, marker: u8) {
    let read = db.begin_read().expect("begin read");
    let table = read.open_table(TABLE).expect("open table");
    let found = table.get(key).expect("get row").expect("row exists");
    assert_eq!(found.value().len(), VALUE_BYTES);
    assert!(found.value().iter().all(|&byte| byte == marker));
}

fn file_backend(path: &Path) -> FileBackend {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .expect("open backend file");
    FileBackend::new(file).expect("lock backend file")
}

fn create_regions(path: &Path, rows: u64) -> usize {
    let db = builder().create(path).expect("create region fixture");
    let mut write = db.begin_write().expect("begin fixture write");
    write.set_quick_repair(true);
    {
        let mut table = write.open_table(TABLE).expect("open fixture table");
        let payload = value(0x35);
        for key in 0..rows {
            table
                .insert(key, payload.as_slice())
                .expect("insert fixture row");
        }
    }
    write.commit().expect("commit fixture rows");
    let regions = db.allocator_regions_for_test();
    assert!(
        regions >= rows as usize / 2,
        "large values must grow the region fixture"
    );
    drop(db);
    regions
}

/// N and 2N durable regions require the same bounded descriptor decoding and
/// retained allocator payload on writable open.
#[test]
fn allocator_open_decodes_a_fixed_number_of_regions() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let small = dir.path().join("small.redb");
    let large = dir.path().join("large.redb");
    let small_regions = create_regions(&small, 40);
    let large_regions = create_regions(&large, 80);
    assert!(large_regions > small_regions, "fixture must actually grow");

    let small_counter = Arc::new(ScratchFault::default());
    let large_counter = Arc::new(ScratchFault::default());
    let (small_db, small_loaded) = redb::observe_allocator_region_loads_for_test(|| {
        builder().open_with_backend(fault_backend(&small, &small_counter))
    });
    let (large_db, large_loaded) = redb::observe_allocator_region_loads_for_test(|| {
        builder().open_with_backend(fault_backend(&large, &large_counter))
    });
    let small_db = small_db.expect("reopen smaller fixture");
    let large_db = large_db.expect("reopen larger fixture");
    assert_eq!(small_db.allocator_regions_for_test(), small_regions);
    assert_eq!(large_db.allocator_regions_for_test(), large_regions);
    assert_eq!(
        small_loaded, large_loaded,
        "open decoded a size-dependent region population"
    );
    assert!(
        large_loaded <= 2,
        "writable open should hydrate only a fixed prewarm set"
    );
    let small_reads = small_counter.backend_read_bytes.load(Ordering::SeqCst);
    let large_reads = large_counter.backend_read_bytes.load(Ordering::SeqCst);
    assert!(
        large_reads <= small_reads + 64 * 1024,
        "writable open read {small_reads} bytes for N and {large_reads} bytes for 2N regions"
    );
    let small_bytes = small_db.allocator_metadata_bytes_for_test();
    let large_bytes = large_db.allocator_metadata_bytes_for_test();
    assert!(
        small_bytes <= RESIDENT_LIMIT,
        "small fixture retained {small_bytes} bytes"
    );
    assert!(
        large_bytes <= RESIDENT_LIMIT,
        "large fixture retained {large_bytes} bytes"
    );
    assert!(
        large_bytes.abs_diff(small_bytes) <= 4096,
        "doubling retained allocator payload from {small_bytes} to {large_bytes} bytes"
    );
    assert_row(&small_db, 0, 0x35);
    assert_row(&large_db, 79, 0x35);
}

#[derive(Debug, Clone, Copy)]
struct BackendWork {
    scratch_reads: usize,
    scratch_writes: usize,
    scratch_lengths: usize,
    scratch_resizes: usize,
    backend_reads: usize,
    backend_read_bytes: usize,
}

impl BackendWork {
    fn sample(fault: &ScratchFault) -> Self {
        Self {
            scratch_reads: fault.reads.load(Ordering::SeqCst),
            scratch_writes: fault.writes.load(Ordering::SeqCst),
            scratch_lengths: fault.lengths.load(Ordering::SeqCst),
            scratch_resizes: fault.resizes.load(Ordering::SeqCst),
            backend_reads: fault.backend_reads.load(Ordering::SeqCst),
            backend_read_bytes: fault.backend_read_bytes.load(Ordering::SeqCst),
        }
    }

    fn since(self, earlier: Self) -> Self {
        Self {
            scratch_reads: self.scratch_reads - earlier.scratch_reads,
            scratch_writes: self.scratch_writes - earlier.scratch_writes,
            scratch_lengths: self.scratch_lengths - earlier.scratch_lengths,
            scratch_resizes: self.scratch_resizes - earlier.scratch_resizes,
            backend_reads: self.backend_reads - earlier.backend_reads,
            backend_read_bytes: self.backend_read_bytes - earlier.backend_read_bytes,
        }
    }

    fn scratch_calls(self) -> usize {
        self.scratch_reads + self.scratch_writes + self.scratch_lengths + self.scratch_resizes
    }
}

fn rejected_first_compaction_batch(path: &Path) -> BackendWork {
    let fault = Arc::new(ScratchFault::default());
    let db = builder()
        .open_with_backend(fault_backend(path, &fault))
        .expect("open saved allocator source");
    let before = BackendWork::sample(&fault);
    let mut at_admission = None;
    let mut admissions = 0;
    let mut cursor = redb::CompactionCursor::default();
    let error = db
        .compact_step(&mut cursor, 1, |_| {
            admissions += 1;
            at_admission = Some(BackendWork::sample(&fault).since(before));
            Err(io::Error::other("refused first relocation admission"))
        })
        .expect_err("admission must reject the first relocation");
    assert!(format!("{error:?}").contains("refused first relocation admission"));
    assert_eq!(
        admissions, 1,
        "the first batch attempted more than one page"
    );
    assert!(!cursor.is_active(), "a refused batch advanced its cursor");
    let work = at_admission.expect("fixture reached page admission");
    assert_eq!(
        BackendWork::sample(&fault).since(before).scratch_calls(),
        work.scratch_calls(),
        "reject cleanup did extra allocator scratch work"
    );
    work
}

/// A one-page relocation must reach admission before any whole-source
/// allocator traversal. Rejection leaves the cursor and durable rows intact.
#[test]
fn allocator_compaction_rejection_has_fixed_pre_admission_work() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let small = dir.path().join("rejected-small.redb");
    let large = dir.path().join("rejected-large.redb");
    let small_regions = create_regions(&small, 40);
    let large_regions = create_regions(&large, 80);
    assert!(large_regions > small_regions);

    let n = rejected_first_compaction_batch(&small);
    let twice_n = rejected_first_compaction_batch(&large);
    assert!(
        n.scratch_calls() <= 32 && twice_n.scratch_calls() <= 32,
        "one-page admission copied allocator state: N={n:?}, 2N={twice_n:?}"
    );
    assert!(
        twice_n.scratch_calls() <= n.scratch_calls() + 8,
        "pre-admission allocator calls scaled with regions: N={n:?}, 2N={twice_n:?}"
    );
    assert!(
        n.backend_reads <= 64 && twice_n.backend_reads <= 64,
        "one-page admission traversed too many storage pages: N={n:?}, 2N={twice_n:?}"
    );
    assert!(
        twice_n.backend_read_bytes <= n.backend_read_bytes + 128 * 1024,
        "pre-admission storage reads scaled with regions: N={n:?}, 2N={twice_n:?}"
    );
    let reopened = builder().open(&large).expect("reopen after rejected batch");
    assert_row(&reopened, 0, 0x35);
    assert_row(&reopened, 79, 0x35);
}

/// Finite one-page batches eventually finish while a reader holds an older
/// root; a later growth and abort preserve the published rows across reopen.
#[test]
fn allocator_one_page_compaction_batches_finish_and_preserve_roots() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("finite-batches.redb");
    create_regions(&path, 32);
    let db = builder().open(&path).expect("open saved allocator source");
    let retained = db.begin_read().expect("retain committed root");
    let mut cursor = redb::CompactionCursor::default();
    let mut examined = 0;
    let mut complete = false;
    for _ in 0..4096 {
        let progress = db
            .compact_step(&mut cursor, 1, |_| Ok(()))
            .expect("advance one-page relocation");
        assert!(
            progress.pages_examined <= 16,
            "one-page batch exceeded the fixture's bounded ancestor path: {progress:?}"
        );
        examined += progress.pages_examined;
        if progress.complete {
            complete = true;
            break;
        }
    }
    assert!(complete, "finite compaction did not complete its sweep");
    assert!(examined > 0, "fixture had no pages to inspect");
    assert_eq!(
        retained
            .open_table(TABLE)
            .unwrap()
            .get(31)
            .unwrap()
            .unwrap()
            .value()[0],
        0x35
    );

    let grow = db.begin_write().expect("begin growth after compaction");
    {
        let mut table = grow.open_table(TABLE).unwrap();
        let payload = value(0x78);
        for key in 32..48 {
            table.insert(key, payload.as_slice()).expect("grow rows");
        }
    }
    grow.commit().expect("commit growth after finite sweep");
    let abort = db.begin_write().expect("begin abort after compaction");
    abort
        .open_table(TABLE)
        .unwrap()
        .insert(48, value(0x99).as_slice())
        .unwrap();
    abort.abort().expect("abort unpublished row");
    assert!(
        retained
            .open_table(TABLE)
            .unwrap()
            .get(32)
            .unwrap()
            .is_none()
    );
    drop(retained);
    drop(db);

    let mut reopened = builder().open(&path).expect("reopen after finite sweep");
    assert!(reopened.check_integrity().expect("check compacted store"));
    for key in 0..32 {
        assert_row(&reopened, key, 0x35);
    }
    for key in 32..48 {
        assert_row(&reopened, key, 0x78);
    }
    assert!(
        reopened
            .begin_read()
            .unwrap()
            .open_table(TABLE)
            .unwrap()
            .get(48)
            .unwrap()
            .is_none()
    );
}

/// Quick repair serializes the v3 region values and tracker directly into
/// reserved pages at a larger region geometry, then reopens the exact state.
#[test]
fn allocator_large_geometry_serialization_survives_reopen_and_growth() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("large-geometry.redb");
    let mut wide = builder();
    wide.set_region_size(4 << 20);
    let db = wide.create(&path).expect("create wide region fixture");
    let mut seed = db.begin_write().expect("begin wide-region seed");
    seed.set_quick_repair(true);
    {
        let mut table = seed.open_table(TABLE).expect("open seed table");
        let payload = value(0x42);
        for key in 0..112 {
            table
                .insert(key, payload.as_slice())
                .expect("seed wide regions");
        }
    }
    seed.commit().expect("serialize wide allocator state");
    let regions = db.allocator_regions_for_test();
    assert!(
        regions >= 16,
        "fixture did not span enough regions: {regions}"
    );
    assert!(db.allocator_metadata_bytes_for_test() <= RESIDENT_LIMIT);
    drop(db);

    let reopened = wide.open(&path).expect("decode wide allocator state");
    assert_eq!(reopened.allocator_regions_for_test(), regions);
    for key in 0..112 {
        assert_row(&reopened, key, 0x42);
    }
    let mut grow = reopened.begin_write().expect("begin growth after decode");
    grow.set_quick_repair(true);
    {
        let mut table = grow.open_table(TABLE).expect("open growth table");
        let payload = value(0x87);
        for key in 112..160 {
            table
                .insert(key, payload.as_slice())
                .expect("grow wide regions");
        }
    }
    grow.commit().expect("serialize grown allocator tracker");
    assert!(reopened.allocator_regions_for_test() > regions);
    drop(reopened);

    let mut final_view = wide.open(&path).expect("reopen grown tracker");
    assert!(
        final_view
            .check_integrity()
            .expect("check v3 allocator roots")
    );
    for key in 0..112 {
        assert_row(&final_view, key, 0x42);
    }
    for key in 112..160 {
        assert_row(&final_view, key, 0x87);
    }
    assert!(final_view.allocator_metadata_bytes_for_test() <= RESIDENT_LIMIT);
}

/// Dirtying more regions than the fixed window set and then committing must
/// preserve every row without growing the allocator owner with file size.
#[test]
fn allocator_windows_bound_dirty_regions_in_one_transaction() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("dirty.redb");
    let scratch = Arc::new(ScratchFault::default());
    let db = builder()
        .create_with_backend(fault_backend(&path, &scratch))
        .expect("create fixture");
    let write = db.begin_write().expect("begin write");
    {
        let mut table = write.open_table(TABLE).expect("open table");
        let payload = value(0x53);
        for key in 0..96 {
            table
                .insert(key, payload.as_slice())
                .expect("insert dirty region");
            let bytes = db.allocator_metadata_bytes_for_test();
            assert!(
                bytes <= RESIDENT_LIMIT,
                "key {key} retained {bytes} allocator bytes"
            );
        }
    }
    write.commit().expect("commit across dirty regions");
    assert!(
        scratch.writes.load(Ordering::SeqCst) > 0,
        "fixture must exercise dirty allocator spill"
    );
    assert!(db.allocator_regions_for_test() >= 48);
    drop(db);

    let reopened = builder().open(&path).expect("reopen spilled fixture");
    for key in [0, 1, 31, 32, 63, 64, 95] {
        assert_row(&reopened, key, 0x53);
    }
    assert!(reopened.allocator_metadata_bytes_for_test() <= RESIDENT_LIMIT);
}

/// Aborted dirty pages never reach committed readers, while a later repaired
/// commit preserves the old reader's root and survives reopen.
#[test]
fn allocator_abort_repair_and_retained_reader_preserve_exact_rows() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("retained.redb");
    create_regions(&path, 48);
    let db = builder().open(&path).expect("open fixture");
    let old_read = db.begin_read().expect("retain committed root");

    let aborted = db.begin_write().expect("begin aborting write");
    {
        let mut table = aborted.open_table(TABLE).expect("open aborting table");
        let payload = value(0x77);
        for key in 48..96 {
            table
                .insert(key, payload.as_slice())
                .expect("insert aborted row");
        }
    }
    aborted.abort().expect("abort dirty regions");
    assert!(
        db.begin_read()
            .unwrap()
            .open_table(TABLE)
            .unwrap()
            .get(95)
            .unwrap()
            .is_none()
    );

    let mut committed = db.begin_write().expect("begin replacement write");
    committed.set_quick_repair(true);
    {
        let mut table = committed.open_table(TABLE).expect("open replacement table");
        let payload = value(0x91);
        for key in 0..48 {
            table
                .insert(key, payload.as_slice())
                .expect("replace committed row");
        }
    }
    committed.commit().expect("commit repaired allocator state");
    let old_table = old_read.open_table(TABLE).expect("old root still readable");
    assert_eq!(old_table.get(47).unwrap().unwrap().value()[0], 0x35);
    assert_row(&db, 47, 0x91);
    drop(old_table);
    drop(old_read);
    drop(db);

    let reopened = builder().open(&path).expect("reopen repaired fixture");
    assert_row(&reopened, 0, 0x91);
    assert_row(&reopened, 47, 0x91);
    assert!(
        reopened
            .begin_read()
            .unwrap()
            .open_table(TABLE)
            .unwrap()
            .get(95)
            .unwrap()
            .is_none()
    );
    assert!(reopened.allocator_metadata_bytes_for_test() <= RESIDENT_LIMIT);
}

/// A non-durable commit's allocator changes remain available to a later
/// durable commit, and that durable publication survives a fresh process view.
#[test]
fn allocator_nondurable_then_durable_commit_preserves_spilled_rows() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("nondurable.redb");
    create_regions(&path, 40);
    let db = builder().open(&path).expect("open baseline");

    let mut nondurable = db.begin_write().expect("begin non-durable write");
    nondurable
        .set_durability(Durability::None)
        .expect("select non-durable publication");
    {
        let mut table = nondurable.open_table(TABLE).expect("open table");
        let payload = value(0x62);
        for key in 40..104 {
            table
                .insert(key, payload.as_slice())
                .expect("insert non-durable row");
        }
    }
    nondurable.commit().expect("publish non-durable rows");
    assert_row(&db, 103, 0x62);

    let mut durable = db.begin_write().expect("begin durable write");
    durable.set_quick_repair(true);
    durable
        .open_table(TABLE)
        .expect("open durable table")
        .insert(200, value(0x84).as_slice())
        .expect("insert durable marker");
    durable
        .commit()
        .expect("durably publish prior rows and marker");
    drop(db);

    let reopened = builder().open(&path).expect("reopen durable view");
    for key in [40, 71, 72, 103] {
        assert_row(&reopened, key, 0x62);
    }
    assert_row(&reopened, 0, 0x35);
    assert_row(&reopened, 200, 0x84);
    assert!(reopened.allocator_metadata_bytes_for_test() <= RESIDENT_LIMIT);
}

/// An externally registered root remains physically readable while many
/// regions are dirtied, and those retained pages are still accounted for.
#[cfg(feature = "contextdb-internal")]
#[test]
fn allocator_spill_preserves_registered_external_snapshot() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("external.redb");
    create_regions(&path, 40);
    let read_db = redb::ReadOnlyDatabase::open(&path).expect("open committed snapshot");
    let old_read = read_db.begin_read().expect("capture committed root");
    let snapshot = old_read.storage_snapshot_identity();
    drop(old_read);
    drop(read_db);

    let provider: redb::ExternalSnapshotProvider = Arc::new(move || Ok(vec![snapshot]));
    let db = builder()
        .open_with_backend_and_external_snapshots(file_backend(&path), provider)
        .expect("open writer with registered root");
    let mut write = db.begin_write().expect("begin replacement write");
    write.set_quick_repair(true);
    {
        let mut table = write.open_table(TABLE).expect("open table");
        table
            .insert(0, value(0x99).as_slice())
            .expect("replace old value");
        let payload = value(0x63);
        for key in 40..104 {
            table
                .insert(key, payload.as_slice())
                .expect("grow behind retained root");
        }
    }
    write.commit().expect("commit with external snapshot held");
    for _ in 0..3 {
        db.begin_write()
            .expect("advance deferred retention")
            .commit()
            .expect("commit deferred retention");
    }
    let retained = db
        .external_snapshot_retention_stats()
        .expect("inspect physical retention");
    assert_eq!(retained.snapshots, 1);
    assert!(retained.retained_pages > 0);
    let old = db
        .begin_external_read(snapshot)
        .expect("read registered root");
    let old_table = old.open_table(TABLE).expect("open retained table");
    assert_eq!(old_table.get(0).unwrap().unwrap().value()[0], 0x35);
    assert!(old_table.get(103).unwrap().is_none());
    assert_row(&db, 0, 0x99);
    assert_row(&db, 103, 0x63);
    assert!(db.allocator_metadata_bytes_for_test() <= RESIDENT_LIMIT);
}

#[derive(Debug, Default)]
struct ScratchFault {
    fail_writes: AtomicBool,
    reads: AtomicUsize,
    writes: AtomicUsize,
    lengths: AtomicUsize,
    resizes: AtomicUsize,
    backend_reads: AtomicUsize,
    backend_read_bytes: AtomicUsize,
    /// Once set, every main-file read fails; each refusal is counted.
    fail_backend_reads: AtomicBool,
    backend_read_failures: AtomicUsize,
    armed: Mutex<Option<(ScratchOperation, usize)>>,
    injected: AtomicBool,
    child_reads_only: AtomicBool,
    root_reads_while_armed: AtomicUsize,
}

impl ScratchFault {
    fn arm(&self, operation: ScratchOperation, nth: usize) {
        assert!(nth > 0);
        self.injected.store(false, Ordering::SeqCst);
        self.child_reads_only.store(false, Ordering::SeqCst);
        self.root_reads_while_armed.store(0, Ordering::SeqCst);
        *self.armed.lock().unwrap() = Some((operation, nth));
    }

    fn arm_child_read(&self, nth: usize) {
        self.arm(ScratchOperation::Read, nth);
        self.child_reads_only.store(true, Ordering::SeqCst);
    }

    fn hit(&self) -> bool {
        self.injected.load(Ordering::SeqCst)
    }

    fn maybe_fail(&self, operation: ScratchOperation) -> io::Result<()> {
        let mut armed = self.armed.lock().unwrap();
        if let Some((wanted, remaining)) = armed.as_mut()
            && *wanted == operation
        {
            *remaining -= 1;
            if *remaining == 0 {
                *armed = None;
                self.injected.store(true, Ordering::SeqCst);
                return Err(io::Error::other(format!(
                    "injected allocator scratch {} failure",
                    operation.label()
                )));
            }
        }
        Ok(())
    }

    fn maybe_fail_read(&self, offset: u64) -> io::Result<()> {
        // Scratch record = 8-byte identity + 4096-byte bitmap window;
        // 32 windows per array and four arrays (root first) per order.
        let array = (offset / (4096 + 8)) / 32;
        if self.child_reads_only.load(Ordering::SeqCst) && array.is_multiple_of(4) {
            if self.armed.lock().unwrap().is_some() {
                self.root_reads_while_armed.fetch_add(1, Ordering::SeqCst);
            }
            return Ok(());
        }
        self.maybe_fail(ScratchOperation::Read)
    }
}

#[derive(Debug)]
struct FaultScratch {
    inner: Box<dyn ScratchBackend>,
    fault: Arc<ScratchFault>,
}

impl ScratchBackend for FaultScratch {
    fn len(&self) -> io::Result<u64> {
        self.fault.lengths.fetch_add(1, Ordering::SeqCst);
        self.fault.maybe_fail(ScratchOperation::Len)?;
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.fault.reads.fetch_add(1, Ordering::SeqCst);
        self.fault.maybe_fail_read(offset)?;
        self.inner.read(offset, out)
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.fault.writes.fetch_add(1, Ordering::SeqCst);
        if self.fault.fail_writes.load(Ordering::SeqCst) {
            return Err(io::Error::other("injected allocator scratch write failure"));
        }
        self.fault.maybe_fail(ScratchOperation::Write)?;
        self.inner.write(offset, data)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.fault.resizes.fetch_add(1, Ordering::SeqCst);
        self.fault.maybe_fail(ScratchOperation::Resize)?;
        self.inner.set_len(len)
    }
}

#[derive(Debug)]
struct FaultBackend {
    inner: FileBackend,
    fault: Arc<ScratchFault>,
}

impl StorageBackend for FaultBackend {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        if self.fault.fail_backend_reads.load(Ordering::SeqCst) {
            self.fault
                .backend_read_failures
                .fetch_add(1, Ordering::SeqCst);
            return Err(io::Error::other("injected main-file read failure"));
        }
        self.inner.read(offset, out)?;
        self.fault.backend_reads.fetch_add(1, Ordering::SeqCst);
        self.fault
            .backend_read_bytes
            .fetch_add(out.len(), Ordering::SeqCst);
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.inner.write(offset, data)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()
    }

    fn scratch_backend(&self) -> io::Result<Option<Box<dyn ScratchBackend>>> {
        self.inner.scratch_backend().map(|scratch| {
            scratch.map(|inner| {
                Box::new(FaultScratch {
                    inner,
                    fault: Arc::clone(&self.fault),
                }) as Box<dyn ScratchBackend>
            })
        })
    }

    fn close(&self) -> io::Result<()> {
        self.inner.close()
    }
}

fn fault_backend(path: &Path, fault: &Arc<ScratchFault>) -> FaultBackend {
    FaultBackend {
        inner: file_backend(path),
        fault: Arc::clone(fault),
    }
}

/// Reopens the store a failed writer left behind; reports whether the open
/// ran redb's repair instead of trusting a clean header.
fn reopen_with_repair(path: &Path, context: &str) -> (redb::Database, bool) {
    let repaired = Arc::new(AtomicBool::new(false));
    let callback = Arc::clone(&repaired);
    let mut reopen_builder = builder();
    reopen_builder.set_repair_callback(move |_| callback.store(true, Ordering::SeqCst));
    let reopened = reopen_builder.open(path).expect(context);
    (reopened, repaired.load(Ordering::SeqCst))
}

/// Inserts `marker` rows for `keys` until one insert is refused, otherwise
/// commits and requires the commit to be refused; returns the refusal.
fn first_refusal(
    write: redb::WriteTransaction,
    keys: std::ops::Range<u64>,
    marker: u8,
    context: &str,
) -> String {
    let mut insertion_error = None;
    {
        let mut table = write.open_table(TABLE).expect("open failing table");
        let payload = value(marker);
        for key in keys {
            if let Err(error) = table.insert(key, payload.as_slice()) {
                insertion_error = Some(format!("{error:?}"));
                break;
            }
        }
    }
    if let Some(error) = insertion_error {
        drop(write);
        error
    } else {
        format!("{:?}", write.commit().expect_err(context))
    }
}

/// A scratch write failure during allocation must be returned, leave the last
/// durable root intact, and leave the header repairable after Database::drop.
#[test]
fn allocator_scratch_failure_prevents_publication_and_repairs_on_reopen() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("fault.redb");
    let fault = Arc::new(ScratchFault::default());
    let db = builder()
        .create_with_backend(fault_backend(&path, &fault))
        .expect("create fault-capable fixture");
    let initial = db.begin_write().expect("begin baseline");
    initial
        .open_table(TABLE)
        .unwrap()
        .insert(0, value(0x21).as_slice())
        .unwrap();
    initial.commit().expect("durably commit baseline");

    fault.fail_writes.store(true, Ordering::SeqCst);
    let writes_before = fault.writes.load(Ordering::SeqCst);
    let write = db.begin_write().expect("begin failing write");
    let failure = first_refusal(write, 1..129, 0x44, "scratch failure must refuse commit");
    assert!(
        failure.contains("injected allocator scratch write failure"),
        "caller lost the scratch I/O cause: {failure}"
    );
    assert!(
        fault.writes.load(Ordering::SeqCst) > writes_before,
        "fixture must hit scratch write"
    );
    drop(db);
    let remaining: Vec<_> = std::fs::read_dir(dir.path())
        .expect("list scratch directory after drop")
        .map(|entry| entry.expect("directory entry").path())
        .collect();
    assert_eq!(
        remaining,
        vec![path.clone()],
        "failed writer leaked scratch file"
    );

    let (reopened, repaired) = reopen_with_repair(&path, "repair and reopen failed writer");
    assert!(repaired, "poisoned Drop must not publish a clean header");
    assert_row(&reopened, 0, 0x21);
    let read = reopened.begin_read().expect("read repaired root");
    let table = read.open_table(TABLE).expect("open repaired table");
    assert!(
        table.get(1).unwrap().is_none(),
        "partial transaction was published"
    );
    assert!(
        table.get(128).unwrap().is_none(),
        "late partial transaction was published"
    );
}

/// Opaque in-memory backends explicitly choose the additive None fallback.
#[test]
fn allocator_backend_without_scratch_keeps_its_in_memory_fallback() {
    let backend = InMemoryBackend::new();
    assert!(backend.scratch_backend().unwrap().is_none());
    let db = builder()
        .create_with_backend(backend)
        .expect("create in-memory fixture");
    let write = db.begin_write().expect("begin in-memory write");
    write
        .open_table(TABLE)
        .unwrap()
        .insert(7, value(0x67).as_slice())
        .unwrap();
    write.commit().expect("commit in-memory row");
    assert_row(&db, 7, 0x67);
}

/// A repaired durable tree may be released during compaction. Newly added
/// regions must still start with their geometry's free pages after that handoff.
#[test]
fn allocator_reopened_repair_then_compaction_grows_beyond_saved_regions() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("source-handoff.redb");
    let baseline_regions = create_regions(&path, 48);
    let db = builder()
        .open(&path)
        .expect("reopen committed allocator tree");
    let retained = db.begin_read().expect("retain old root");

    let mut repair = db.begin_write().expect("begin quick repair commit");
    repair.set_quick_repair(true);
    repair
        .open_table(TABLE)
        .unwrap()
        .insert(48, value(0x52).as_slice())
        .unwrap();
    repair.commit().expect("publish quick repair root");

    let mut cursor = redb::CompactionCursor::default();
    db.compact_step(&mut cursor, 8, |_| Ok(()))
        .expect("materialize allocator source for compaction");
    assert_eq!(
        retained
            .open_table(TABLE)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .value()[0],
        0x35
    );

    let aborted = db.begin_write().expect("begin growth to abort");
    {
        let mut table = aborted.open_table(TABLE).unwrap();
        let payload = value(0x68);
        for key in 49..65 {
            table
                .insert(key, payload.as_slice())
                .expect("grow aborted branch");
        }
    }
    aborted.abort().expect("abort growth after materialization");
    assert!(
        db.begin_read()
            .unwrap()
            .open_table(TABLE)
            .unwrap()
            .get(64)
            .unwrap()
            .is_none()
    );

    let mut committed = db.begin_write().expect("begin durable growth");
    committed.set_quick_repair(true);
    {
        let mut table = committed.open_table(TABLE).unwrap();
        let payload = value(0x73);
        for key in 49..145 {
            table
                .insert(key, payload.as_slice())
                .expect("allocate fresh region");
        }
    }
    committed.commit().expect("publish rows in fresh regions");
    assert!(db.allocator_regions_for_test() > baseline_regions);
    assert_eq!(
        retained
            .open_table(TABLE)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .value()[0],
        0x35
    );
    assert!(
        retained
            .open_table(TABLE)
            .unwrap()
            .get(144)
            .unwrap()
            .is_none()
    );
    drop(retained);
    drop(db);

    let reopened = builder()
        .open(&path)
        .expect("reopen expanded allocator tree");
    for key in 0..48 {
        assert_row(&reopened, key, 0x35);
    }
    assert_row(&reopened, 48, 0x52);
    for key in 49..145 {
        assert_row(&reopened, key, 0x73);
    }
    assert!(reopened.allocator_metadata_bytes_for_test() <= RESIDENT_LIMIT);
}

/// Region truncation must invalidate old spill identities so regrowth reads
/// fresh free geometry instead of stale state from the removed region.
#[test]
fn allocator_compaction_shrink_then_regrow_reuses_fresh_region_geometry() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("shrink-regrow.redb");
    let original_regions = create_regions(&path, 64);
    let mut db = builder().open(&path).expect("open expanded fixture");
    let delete = db.begin_write().expect("begin deletion");
    {
        let mut table = delete.open_table(TABLE).unwrap();
        for key in 8..64 {
            table.remove(key).expect("remove tail row");
        }
    }
    delete.commit().expect("commit deletion");
    db.compact().expect("reclaim and shrink deleted tail");
    let shrunk_regions = db.allocator_regions_for_test();
    assert!(
        shrunk_regions < original_regions,
        "fixture did not truncate regions: {original_regions} -> {shrunk_regions}"
    );

    let mut grow = db.begin_write().expect("begin regrowth");
    grow.set_quick_repair(true);
    {
        let mut table = grow.open_table(TABLE).unwrap();
        let payload = value(0x83);
        for key in 64..144 {
            table
                .insert(key, payload.as_slice())
                .expect("regrow region");
        }
    }
    grow.commit().expect("publish regrown regions");
    assert!(db.allocator_regions_for_test() > shrunk_regions);
    drop(db);
    let reopened = builder().open(&path).expect("reopen regrown tree");
    for key in 0..8 {
        assert_row(&reopened, key, 0x35);
    }
    for key in 8..64 {
        assert!(
            reopened
                .begin_read()
                .unwrap()
                .open_table(TABLE)
                .unwrap()
                .get(key)
                .unwrap()
                .is_none()
        );
    }
    for key in 64..144 {
        assert_row(&reopened, key, 0x83);
    }
}

/// Shrinking only the active region must hide free bits beyond its new page
/// count. Later allocations in that region must find valid pages and reopen.
#[test]
fn allocator_partial_region_shrink_then_regrow_ignores_removed_tail_bits() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("partial-region-shrink.redb");
    let mut db = builder().create(&path).expect("create one-region fixture");
    let payload = vec![0x46; 1024];
    let seed = db.begin_write().expect("begin seed");
    {
        let mut table = seed.open_table(TABLE).expect("open seed table");
        for key in 0..192 {
            table
                .insert(key, payload.as_slice())
                .expect("insert seed row");
        }
    }
    seed.commit().expect("commit seed rows");
    assert_eq!(
        db.allocator_regions_for_test(),
        1,
        "fixture crossed a region"
    );
    let grown_len = std::fs::metadata(&path).unwrap().len();

    let delete = db.begin_write().expect("begin tail deletion");
    {
        let mut table = delete.open_table(TABLE).expect("open delete table");
        for key in 1..192 {
            table.remove(key).expect("remove seed tail");
        }
    }
    delete.commit().expect("commit tail deletion");
    db.compact().expect("shrink active region");
    assert_eq!(db.allocator_regions_for_test(), 1);
    let shrunk_len = std::fs::metadata(&path).unwrap().len();
    assert!(
        shrunk_len < grown_len,
        "fixture did not shrink active region"
    );

    let regrow = db.begin_write().expect("begin regrowth");
    {
        let mut table = regrow.open_table(TABLE).expect("open regrowth table");
        for key in 192..320 {
            table
                .insert(key, payload.as_slice())
                .expect("regrow active region");
        }
    }
    regrow.commit().expect("commit regrowth");
    assert_eq!(db.allocator_regions_for_test(), 1);
    drop(db);

    let mut reopened = builder().open(&path).expect("reopen regrown region");
    assert!(reopened.check_integrity().expect("check regrown allocator"));
    let read = reopened.begin_read().expect("read regrown region");
    let table = read.open_table(TABLE).expect("open regrown table");
    assert_eq!(table.get(0).unwrap().unwrap().value(), payload);
    for key in 1..192 {
        assert!(
            table.get(key).unwrap().is_none(),
            "removed key {key} returned"
        );
    }
    for key in 192..320 {
        assert_eq!(table.get(key).unwrap().unwrap().value(), payload);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScratchOperation {
    Len,
    Read,
    Write,
    Resize,
}

impl ScratchOperation {
    fn label(self) -> &'static str {
        match self {
            Self::Len => "len",
            Self::Read => "read",
            Self::Write => "write",
            Self::Resize => "resize",
        }
    }
}

/// Each injected call is one actual scratch operation; disarming after a hit
/// leaves cleanup able to run and exposes a lost first failure.
#[test]
fn allocator_nth_scratch_faults_refuse_publication_and_repair() {
    for operation in [
        ScratchOperation::Len,
        ScratchOperation::Read,
        ScratchOperation::Write,
    ] {
        for nth in [1, 3] {
            exercise_nth_scratch_fault(operation, nth);
        }
    }
}

fn exercise_nth_scratch_fault(operation: ScratchOperation, nth: usize) {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("nth-fault.redb");
    let fault = Arc::new(ScratchFault::default());
    let db = builder()
        .create_with_backend(fault_backend(&path, &fault))
        .expect("create injected backend");
    let baseline = db.begin_write().unwrap();
    baseline
        .open_table(TABLE)
        .unwrap()
        .insert(0, value(0x24).as_slice())
        .unwrap();
    baseline.commit().expect("commit fault baseline");

    // First fill enough regions to spill dirty windows. Revisiting early keys
    // then reads their evicted child windows while root summaries are cached.
    let mut write = db.begin_write().unwrap();
    write.set_quick_repair(true);
    let mut failure = None;
    {
        let mut table = write.open_table(TABLE).unwrap();
        let payload = value(0x62);
        for key in 1..97 {
            table
                .insert(key, payload.as_slice())
                .expect("prime scratch windows");
        }
        if operation == ScratchOperation::Read {
            fault.arm_child_read(nth);
        } else {
            fault.arm(operation, nth);
        }
        for key in (0..97).cycle().take(97 * 4) {
            match table.insert(key, value(0x86).as_slice()) {
                Ok(_) => {}
                Err(error) => {
                    failure = Some(format!("{error:?}"));
                    break;
                }
            }
        }
    }
    let failure = match failure {
        Some(error) => {
            drop(write);
            error
        }
        None => format!(
            "{:?}",
            write.commit().expect_err("armed fault must refuse commit")
        ),
    };
    assert!(
        fault.hit(),
        "{} call {nth} was not reached by production allocator work",
        operation.label()
    );
    if operation == ScratchOperation::Read {
        assert_eq!(
            fault.root_reads_while_armed.load(Ordering::SeqCst),
            0,
            "the root must stay cached before the injected child miss"
        );
    }
    assert!(
        failure.contains(&format!(
            "injected allocator scratch {} failure",
            operation.label()
        )),
        "{} call {nth} lost its I/O cause: {failure}",
        operation.label()
    );
    drop(db);

    let (reopened, repaired) = reopen_with_repair(&path, "repair after injected I/O");
    assert!(repaired, "failed writer published a clean header");
    assert_row(&reopened, 0, 0x24);
    let read = reopened.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    assert!(
        table.get(1).unwrap().is_none(),
        "partial transaction was published"
    );
    assert!(
        table.get(96).unwrap().is_none(),
        "late partial row was published"
    );
}

/// Scratch resize is part of opening a paged allocator. A failed resize must
/// refuse the writable open and leave the last durable root readable.
#[test]
fn allocator_scratch_resize_failure_refuses_open_without_publication() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("resize-fault.redb");
    create_regions(&path, 40);
    let fault = Arc::new(ScratchFault::default());
    fault.arm(ScratchOperation::Resize, 1);
    let result = builder().open_with_backend(fault_backend(&path, &fault));
    let error = match result {
        Ok(_) => panic!("scratch resize failure must refuse writable open"),
        Err(error) => format!("{error:?}"),
    };
    assert!(fault.hit());
    assert!(
        error.contains("injected allocator scratch resize failure"),
        "{error}"
    );
    let reopened = builder()
        .open(&path)
        .expect("reopen after refused allocator setup");
    assert_row(&reopened, 0, 0x35);
    assert_row(&reopened, 39, 0x35);
}

/// A scratch truncation failure during active compaction must refuse the
/// shrink, preserve the committed deletion, and require repair on reopen.
#[test]
fn allocator_scratch_resize_failure_during_active_shrink_preserves_rows() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("active-shrink-fault.redb");
    create_regions(&path, 64);
    let fault = Arc::new(ScratchFault::default());
    let mut db = builder()
        .open_with_backend(fault_backend(&path, &fault))
        .expect("open fault-capable fixture");
    let delete = db.begin_write().expect("begin deletion");
    {
        let mut table = delete.open_table(TABLE).expect("open table");
        for key in 8..64 {
            table.remove(key).expect("remove tail row");
        }
    }
    delete.commit().expect("commit deletion before shrink");

    fault.arm(ScratchOperation::Resize, 1);
    let error = format!(
        "{:?}",
        db.compact()
            .expect_err("scratch truncation must refuse compaction")
    );
    assert!(
        fault.hit(),
        "fixture did not reach active scratch truncation"
    );
    assert!(
        error.contains("injected allocator scratch resize failure"),
        "caller lost scratch truncation cause: {error}"
    );
    drop(db);

    let (reopened, repaired) = reopen_with_repair(&path, "repair after failed shrink");
    assert!(repaired, "failed shrink wrote a clean header");
    for key in 0..8 {
        assert_row(&reopened, key, 0x35);
    }
    let read = reopened.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    for key in 8..64 {
        assert!(
            table.get(key).unwrap().is_none(),
            "deleted key {key} returned"
        );
    }
}

/// Finite compaction keeps the saved source lazy. Explicit whole-file
/// compaction must still propagate a materialization fault and keep the last
/// durable root readable.
#[test]
fn allocator_scratch_failure_during_source_materialization_preserves_rows() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("materialize-fault.redb");
    create_regions(&path, 48);
    let fault = Arc::new(ScratchFault::default());
    let mut db = builder()
        .open_with_backend(fault_backend(&path, &fault))
        .expect("open saved allocator source");
    fault.arm(ScratchOperation::Write, 1);
    let mut cursor = redb::CompactionCursor::default();
    let writes_before = fault.writes.load(Ordering::SeqCst);
    db.compact_step(&mut cursor, 8, |_| Ok(()))
        .expect("finite compaction leaves the saved source lazy");
    assert!(
        !fault.hit(),
        "finite compaction materialized its saved source"
    );
    assert_eq!(
        fault.writes.load(Ordering::SeqCst),
        writes_before,
        "finite compaction spilled the whole saved source"
    );
    let error = format!(
        "{:?}",
        db.compact()
            .expect_err("materialization scratch write must refuse compaction")
    );
    assert!(fault.hit(), "fixture did not spill materialized source");
    assert!(
        error.contains("injected allocator scratch write failure"),
        "caller lost materialization I/O cause: {error}"
    );
    drop(db);

    let reopened = builder()
        .open(&path)
        .expect("reopen after failed materialization");
    for key in 0..48 {
        assert_row(&reopened, key, 0x35);
    }
}

/// A second scratch fault while rebuilding a dirty allocator must refuse
/// repair without replacing the last committed root.
#[test]
fn allocator_scratch_failure_during_repair_can_retry_from_durable_root() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("repair-fault.redb");
    let first_fault = Arc::new(ScratchFault::default());
    let db = builder()
        .create_with_backend(fault_backend(&path, &first_fault))
        .expect("create fault-capable fixture");
    let baseline = db.begin_write().expect("begin baseline");
    baseline
        .open_table(TABLE)
        .unwrap()
        .insert(0, value(0x31).as_slice())
        .unwrap();
    baseline.commit().expect("commit baseline");

    first_fault.arm(ScratchOperation::Write, 1);
    let write = db.begin_write().expect("begin failing write");
    let error = first_refusal(write, 1..97, 0x72, "scratch fault must refuse write");
    assert!(first_fault.hit(), "fixture did not poison the first writer");
    assert!(error.contains("injected allocator scratch write failure"));
    drop(db);

    let repair_fault = Arc::new(ScratchFault::default());
    repair_fault.arm(ScratchOperation::Len, 1);
    let error = match builder().open_with_backend(fault_backend(&path, &repair_fault)) {
        Ok(_) => panic!("scratch fault must refuse allocator repair"),
        Err(error) => format!("{error:?}"),
    };
    assert!(
        repair_fault.hit(),
        "fixture did not read scratch during repair"
    );
    assert!(
        error.contains("injected allocator scratch len failure"),
        "repair lost scratch I/O cause: {error}"
    );

    let (reopened, repaired) = reopen_with_repair(&path, "retry allocator repair");
    assert!(repaired, "failed repair wrote a clean header");
    assert_row(&reopened, 0, 0x31);
    let read = reopened.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    assert!(table.get(1).unwrap().is_none());
    assert!(table.get(96).unwrap().is_none());
}

/// Saved tracker directories and non-paged bitmap payload must remain bounded
/// as actual regions grow, including ordinary append and reuse of freed pages.
/// Startup decoding and storage reads retain their independent work oracles.
#[test]
fn region_tracker_allocations_stay_bounded_across_reopen_append_and_recycle() {
    const SMALL_ROWS: u64 = 4096;
    const BODY_BYTES: usize = 64 << 10;
    fn tracker_builder() -> redb::Builder {
        let mut builder = redb::Builder::new();
        builder.set_region_size(32 * 4096).set_cache_size(0);
        builder
    }
    fn seed(path: &Path, rows: u64) -> usize {
        let db = tracker_builder()
            .create(path)
            .expect("create growing tracker fixture");
        let payload = vec![0x35; BODY_BYTES];
        let mut write = db.begin_write().unwrap();
        write.set_quick_repair(true);
        {
            let mut table = write.open_table(TABLE).unwrap();
            for key in 0..rows {
                table.insert(key, payload.as_slice()).unwrap();
            }
        }
        write.commit().unwrap();
        let regions = db.allocator_regions_for_test();
        assert!(
            regions >= rows as usize,
            "one large value forces a distinct small region"
        );
        regions
    }
    fn check(db: &redb::Database, key: u64, marker: u8) {
        let read = db.begin_read().unwrap();
        let table = read.open_table(TABLE).unwrap();
        let row = table.get(key).unwrap().unwrap();
        assert_eq!(row.value().len(), BODY_BYTES);
        assert!(row.value().iter().all(|&byte| byte == marker));
    }
    fn observe(path: &Path, rows: u64) -> ([usize; 3], [usize; 3], u64, usize) {
        // Caller-owned controls exist before tracking and survive the scope.
        let fault = Arc::new(ScratchFault::default());
        let appended = vec![0x59; BODY_BYTES];
        let recycled = vec![0x72; BODY_BYTES];
        let heap = HeapScope::start();
        let (db, loaded) = redb::observe_allocator_region_loads_for_test(|| {
            tracker_builder().open_with_backend(fault_backend(path, &fault))
        });
        let db = db.expect("reopen the saved tracker");
        let startup_reads = fault.backend_read_bytes.load(Ordering::SeqCst);
        let opened_regions = db.allocator_regions_for_test();
        check(&db, rows - 1, 0x35);
        let opened = heap.retained();
        // Identical finite write batches avoid pricing a dataset-sized driver.
        for first in (rows..rows + rows / 2).step_by(64) {
            let write = db.begin_write().unwrap();
            {
                let mut table = write.open_table(TABLE).unwrap();
                for key in first..first + 64 {
                    table.insert(key, appended.as_slice()).unwrap();
                }
            }
            write.commit().unwrap();
        }
        let appended_regions = db.allocator_regions_for_test();
        assert!(
            appended_regions > opened_regions,
            "append must actually grow region geometry"
        );
        check(&db, rows + rows / 2 - 1, 0x59);
        let after_append = heap.retained();
        for first in (0..rows / 2).step_by(64) {
            let write = db.begin_write().unwrap();
            {
                let mut table = write.open_table(TABLE).unwrap();
                for key in first..first + 64 {
                    table.remove(key).unwrap();
                }
            }
            write.commit().unwrap();
        }
        for first in (rows + rows / 2..2 * rows).step_by(64) {
            let write = db.begin_write().unwrap();
            {
                let mut table = write.open_table(TABLE).unwrap();
                for key in first..first + 64 {
                    table.insert(key, recycled.as_slice()).unwrap();
                }
            }
            write.commit().unwrap();
        }
        let recycled_regions = db.allocator_regions_for_test();
        assert!(
            recycled_regions <= appended_regions + 64,
            "freed regional pages must be reused"
        );
        check(&db, rows - 1, 0x35);
        check(&db, rows + rows / 2 - 1, 0x59);
        check(&db, 2 * rows - 1, 0x72);
        {
            let read = db.begin_read().unwrap();
            let table = read.open_table(TABLE).unwrap();
            assert!(
                table.get(0).unwrap().is_none(),
                "recycling preserves the row deletion"
            );
        }
        let after_recycle = heap.retained();
        drop(db);
        heap.finish();
        (
            [opened, after_append, after_recycle],
            [opened_regions, appended_regions, recycled_regions],
            loaded,
            startup_reads,
        )
    }
    let directory = tempfile::tempdir().unwrap();
    let small = directory.path().join("tracker-n.redb");
    let large = directory.path().join("tracker-2n.redb");
    let small_regions = seed(&small, SMALL_ROWS);
    let large_regions = seed(&large, 2 * SMALL_ROWS);
    assert!(large_regions > small_regions);
    let (small_bytes, small_regions, small_loaded, small_reads) = observe(&small, SMALL_ROWS);
    let (large_bytes, large_regions, large_loaded, large_reads) = observe(&large, 2 * SMALL_ROWS);
    assert_eq!(
        small_loaded, large_loaded,
        "startup region decoding stays independent of geometry"
    );
    assert!(large_loaded <= 2, "startup retains the fixed prewarm set");
    assert!(
        large_reads <= small_reads + 64 * 1024,
        "startup storage reads stay bounded"
    );
    for checkpoint in 0..3 {
        assert!(large_regions[checkpoint] > small_regions[checkpoint]);
        assert!(
            large_bytes[checkpoint].abs_diff(small_bytes[checkpoint]) <= 4096,
            "checkpoint {checkpoint}: actual directory/non-paged payload grows with regions: N={} 2N={}",
            small_bytes[checkpoint],
            large_bytes[checkpoint]
        );
    }
}

/// The maintained free totals equal an independent recount of every bitmap.
fn assert_totals_exact(db: &redb::Database, context: &str) {
    let recount = db
        .allocator_free_totals_recount_for_test()
        .expect("recount allocator bitmaps");
    assert_eq!(
        db.allocator_free_totals(),
        Some(recount),
        "{context}: maintained (free, region) pages equal the bitmap recount"
    );
}

/// Opens the store, returning how many free-total rebuilds the open ran.
fn open_counting_rebuilds(path: &Path) -> (redb::Database, u64) {
    let before = redb::Database::free_total_recounts_for_test();
    let db = builder().open(path).expect("reopen fixture");
    (db, redb::Database::free_total_recounts_for_test() - before)
}

fn insert_rows(db: &redb::Database, keys: std::ops::Range<u64>, marker: u8) {
    let write = db.begin_write().expect("begin insert");
    {
        let mut table = write.open_table(TABLE).expect("open table");
        for key in keys {
            table.insert(key, value(marker).as_slice()).expect("insert");
        }
    }
    write.commit().expect("commit insert");
}

fn remove_rows(db: &redb::Database, keys: std::ops::Range<u64>) {
    let write = db.begin_write().expect("begin remove");
    {
        let mut table = write.open_table(TABLE).expect("open table");
        for key in keys {
            table.remove(key).expect("remove");
        }
    }
    write.commit().expect("commit remove");
}

/// Every allocation, free, split and coalesce, growth across regions, abort,
/// reader-held free and trimming clean close moves the totals exactly as the
/// bitmaps move; a reopen after a trimming close adopts the saved totals
/// without rebuilding them, including when the trim removed whole regions.
#[test]
fn allocator_free_totals_track_the_bitmaps_and_survive_trimming_reopen() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("free-totals.redb");
    let db = builder().create(&path).expect("create fixture");
    assert_totals_exact(&db, "fresh store");

    insert_rows(&db, 0..8, 0x11);
    assert_totals_exact(&db, "growth across regions");

    // Mixed small values split and coalesce buddy orders.
    let small: TableDefinition<u64, &[u8]> = TableDefinition::new("small_orders");
    let write = db.begin_write().expect("begin small values");
    {
        let mut table = write.open_table(small).expect("open small table");
        for key in 0..64u64 {
            let bytes = vec![0x5a; 64 << (key % 7)];
            table.insert(key, bytes.as_slice()).expect("insert small");
        }
    }
    write.commit().expect("commit small values");
    assert_totals_exact(&db, "mixed orders");

    let aborted = db.begin_write().expect("begin aborted write");
    {
        let mut table = aborted.open_table(TABLE).expect("open table");
        for key in 100..104 {
            table.insert(key, value(0x77).as_slice()).expect("insert");
        }
    }
    aborted.abort().expect("abort");
    assert_totals_exact(&db, "rolled back allocations");

    let held = db.begin_read().expect("hold old root");
    remove_rows(&db, 2..4);
    assert_totals_exact(&db, "frees held for a live reader");
    drop(held);
    db.begin_write()
        .expect("begin release")
        .commit()
        .expect("release held frees");
    assert_totals_exact(&db, "held frees released");

    drop(db);
    let (db, rebuilds) = open_counting_rebuilds(&path);
    assert_eq!(rebuilds, 0, "a clean close saves totals the reopen adopts");
    assert_totals_exact(&db, "reopen");
    insert_rows(&db, 200..206, 0x22);
    assert_totals_exact(&db, "growth after reopen");
}

/// Region pages and region count, read from the maintained totals.
fn geometry(db: &redb::Database) -> (u64, usize) {
    let (_, pages, _) = db.allocator_free_totals().expect("trusted totals");
    (pages, db.allocator_regions_for_test())
}

const SMALL: TableDefinition<u64, &[u8]> = TableDefinition::new("small_rows");

/// A trim inside the last region, done only by the quick-repair close: the
/// deletion commit leaves its pages pending, so no earlier commit can trim.
/// The reopen keeps the region count, has fewer region pages, adopts the
/// saved totals without a rebuild and agrees with a fresh recount.
#[test]
fn allocator_free_totals_survive_an_intra_region_trim_at_close() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("intra-region-trim.redb");
    let db = builder().create(&path).expect("create fixture");
    let write = db.begin_write().expect("begin small rows");
    {
        let mut table = write.open_table(SMALL).expect("open small table");
        for key in 0..12u64 {
            table
                .insert(key, vec![0x71_u8; 64 << 10].as_slice())
                .expect("insert");
        }
    }
    write.commit().expect("commit small rows");
    drop(db);
    let (db, rebuilds) = open_counting_rebuilds(&path);
    assert_eq!(rebuilds, 0);
    assert_totals_exact(&db, "seeded");

    let write = db.begin_write().expect("begin tail removal");
    {
        let mut table = write.open_table(SMALL).expect("open small table");
        for key in 9..12u64 {
            table.remove(key).expect("remove");
        }
    }
    write.commit().expect("commit tail removal");
    assert_totals_exact(&db, "tail removal pending");
    let (pages_before, regions_before) = geometry(&db);
    drop(db);

    let (db, rebuilds) = open_counting_rebuilds(&path);
    let (pages_after, regions_after) = geometry(&db);
    println!(
        "intra-region trim: pages {pages_before} -> {pages_after}, regions {regions_before} -> {regions_after}"
    );
    assert_eq!(
        rebuilds, 0,
        "totals saved before the trim are adopted after it"
    );
    assert_eq!(
        regions_after, regions_before,
        "an intra-region trim keeps every region"
    );
    assert!(
        pages_after < pages_before,
        "the close must actually trim the last region"
    );
    assert_totals_exact(&db, "after the intra-region trim");
}

/// Trims that remove whole trailing regions, first only at the quick-repair
/// close and then in an ordinary commit before the close. Each stage removes
/// exactly the one freed region, and each reopen adopts exact totals.
#[test]
fn allocator_free_totals_survive_region_removing_trims_at_close_and_in_ordinary_commits() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("region-trim.redb");
    create_regions(&path, 8);
    let (db, rebuilds) = open_counting_rebuilds(&path);
    assert_eq!(rebuilds, 0);
    assert_totals_exact(&db, "seeded");

    // Stage 1: the deletion's pages stay pending until the close, so only the
    // quick-repair close can remove the freed region.
    remove_rows(&db, 7..8);
    let (_, regions_before_close) = geometry(&db);
    assert_totals_exact(&db, "last row removed, pages pending");
    drop(db);
    let (db, rebuilds) = open_counting_rebuilds(&path);
    let (_, regions_after_close) = geometry(&db);
    println!("close trim: regions {regions_before_close} -> {regions_after_close}");
    assert_eq!(
        rebuilds, 0,
        "a region-removing close saves adoptable totals"
    );
    assert_eq!(
        regions_after_close,
        regions_before_close - 1,
        "the close removes exactly the freed trailing region"
    );
    assert_totals_exact(&db, "after the region-removing close");

    // Stage 2: an ordinary commit releases the freed pages and trims the
    // region itself, before any quick-repair close.
    remove_rows(&db, 6..7);
    let (_, regions_before_trim) = geometry(&db);
    db.begin_write()
        .expect("begin releasing commit")
        .commit()
        .expect("ordinary commit releases and trims");
    let (_, regions_after_trim) = geometry(&db);
    println!("ordinary trim: regions {regions_before_trim} -> {regions_after_trim}");
    assert_eq!(
        regions_after_trim,
        regions_before_trim - 1,
        "the ordinary commit removes exactly the freed trailing region"
    );
    assert_totals_exact(&db, "after the ordinary trimming commit");
    drop(db);
    let (db, rebuilds) = open_counting_rebuilds(&path);
    assert_eq!(
        rebuilds, 0,
        "the close after an ordinary trim saves adoptable totals"
    );
    assert_eq!(geometry(&db).1, regions_after_trim);
    assert_totals_exact(&db, "reopen after the ordinary trim");
    assert_row(&db, 5, 0x35);
}

/// A saved value that is absent, the wrong length, the wrong format or the
/// wrong geometry is rejected: the next open rebuilds exact totals once, and
/// the close after it saves totals the following open adopts.
#[test]
fn allocator_free_totals_reject_absent_or_malformed_saved_values() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("malformed-totals.redb");
    create_regions(&path, 6);
    let (db, _) = open_counting_rebuilds(&path);
    let (free, pages, _) = db.allocator_free_totals().expect("trusted totals");
    let regions = u32::try_from(db.allocator_regions_for_test()).unwrap();
    drop(db);
    let encode = |format: u32, regions: u32, pages: u64, free: u64| {
        let mut bytes = Vec::with_capacity(24);
        bytes.extend_from_slice(&format.to_le_bytes());
        bytes.extend_from_slice(&regions.to_le_bytes());
        bytes.extend_from_slice(&pages.to_le_bytes());
        bytes.extend_from_slice(&free.to_le_bytes());
        bytes
    };
    let cases = [
        ("absent", redb::FreeTotalsSaveForTest::Omit),
        ("short", redb::FreeTotalsSaveForTest::Write(vec![1_u8; 23])),
        (
            "format",
            redb::FreeTotalsSaveForTest::Write(encode(2, regions, pages, free)),
        ),
        (
            "regions",
            redb::FreeTotalsSaveForTest::Write(encode(1, regions + 5, pages, free)),
        ),
        (
            "pages",
            redb::FreeTotalsSaveForTest::Write(encode(1, regions, pages - 1, free)),
        ),
        (
            "free",
            redb::FreeTotalsSaveForTest::Write(encode(1, regions, pages, pages + 1)),
        ),
    ];
    for (name, mode) in cases {
        let db = builder().open(&path).expect("open fixture");
        redb::Database::set_free_totals_save_for_test(Some(mode));
        drop(db);
        redb::Database::set_free_totals_save_for_test(None);
        let (db, rebuilds) = open_counting_rebuilds(&path);
        assert_eq!(
            rebuilds, 1,
            "{name}: the rejected saved value is rebuilt once"
        );
        assert_totals_exact(&db, name);
        drop(db);
        let (db, rebuilds) = open_counting_rebuilds(&path);
        assert_eq!(
            rebuilds, 0,
            "{name}: the rebuilt totals are saved and adopted"
        );
        assert_totals_exact(&db, name);
        drop(db);
    }
}

/// A clean close interrupted before the totals are encoded, or after they are
/// encoded but before publication, publishes nothing: the reopen repairs from
/// the last durable root and rebuilds exact totals once.
#[test]
fn allocator_free_totals_after_an_interrupted_save_are_rebuilt_by_repair() {
    let dir = tempfile::tempdir().expect("fixture directory");
    for (name, mode) in [
        (
            "before encode",
            redb::FreeTotalsSaveForTest::FailBeforeEncode,
        ),
        ("after encode", redb::FreeTotalsSaveForTest::FailAfterEncode),
    ] {
        let path = dir
            .path()
            .join(format!("interrupted-{}.redb", name.replace(' ', "-")));
        create_regions(&path, 4);
        let db = builder().open(&path).expect("open fixture");
        insert_rows(&db, 4..6, 0x46);
        redb::Database::set_free_totals_save_for_test(Some(mode));
        drop(db);
        redb::Database::set_free_totals_save_for_test(None);
        let before = redb::Database::free_total_recounts_for_test();
        let (db, repaired) = reopen_with_repair(&path, name);
        assert!(
            repaired,
            "{name}: the interrupted save leaves a store that needs repair"
        );
        assert_eq!(
            redb::Database::free_total_recounts_for_test() - before,
            1,
            "{name}: repair rebuilds the totals once"
        );
        assert_totals_exact(&db, name);
        assert_row(&db, 5, 0x46);
    }
}

/// A fault while repair rebuilds the totals refuses the open and completes no
/// count; a later healthy open repairs and rebuilds exact totals once.
#[test]
fn allocator_free_total_rebuild_fault_during_repair_refuses_the_open() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("repair-rebuild-fault.redb");
    create_regions(&path, 48);
    let db = builder().open(&path).expect("open fixture");
    insert_rows(&db, 48..50, 0x47);
    redb::Database::set_free_totals_save_for_test(Some(
        redb::FreeTotalsSaveForTest::FailBeforeEncode,
    ));
    drop(db);
    redb::Database::set_free_totals_save_for_test(None);

    let fault = Arc::new(ScratchFault::default());
    let armed = Arc::clone(&fault);
    redb::Database::set_free_total_recount_start_hook_for_test(Some(Box::new(move || {
        armed.arm(ScratchOperation::Read, 1);
        armed.fail_backend_reads.store(true, Ordering::SeqCst);
    })));
    let completed_before = redb::Database::free_total_recounts_for_test();
    let opened = builder().open_with_backend(fault_backend(&path, &fault));
    redb::Database::set_free_total_recount_start_hook_for_test(None);
    assert!(
        fault.fail_backend_reads.load(Ordering::SeqCst),
        "the repair must reach its rebuild"
    );
    assert!(
        fault.hit() || fault.backend_read_failures.load(Ordering::SeqCst) > 0,
        "the injected fault must fire inside the rebuild"
    );
    assert!(
        opened.is_err(),
        "a repair whose rebuild faults refuses the open"
    );
    drop(opened);
    assert_eq!(
        redb::Database::free_total_recounts_for_test(),
        completed_before,
        "the failed rebuild completed no count"
    );

    let before = redb::Database::free_total_recounts_for_test();
    let (db, repaired) = reopen_with_repair(&path, "healthy repair after a faulted rebuild");
    assert!(repaired);
    assert_eq!(redb::Database::free_total_recounts_for_test() - before, 1);
    assert_totals_exact(&db, "after the healthy repair");
    assert_row(&db, 49, 0x47);
}

/// Totals that lost trust are never saved: the reserved value stays unwritten,
/// the save is not retried, and the next open rebuilds exact totals once.
#[test]
fn allocator_untrusted_free_totals_are_not_saved_and_rebuild_once_on_reopen() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("untrusted.redb");
    let db = builder().create(&path).expect("create fixture");
    insert_rows(&db, 0..4, 0x31);
    db.distrust_allocator_free_totals_for_test();
    assert_eq!(
        db.allocator_free_totals(),
        None,
        "untrusted totals are not published"
    );
    insert_rows(&db, 4..5, 0x32);
    assert_eq!(
        db.allocator_free_totals(),
        None,
        "later deltas do not restore trust"
    );
    drop(db);
    let (db, rebuilds) = open_counting_rebuilds(&path);
    assert_eq!(
        rebuilds, 1,
        "the unwritten saved value is rejected and rebuilt once"
    );
    assert_totals_exact(&db, "rebuilt after an untrusted save");
    drop(db);
    let (db, rebuilds) = open_counting_rebuilds(&path);
    assert_eq!(
        rebuilds, 0,
        "the rebuilt totals are saved by the next clean close"
    );
    assert_totals_exact(&db, "adopted after rebuild");
}

/// A poisoned allocator publishes no totals, and the repaired reopen rebuilds
/// them exactly once.
#[test]
fn allocator_free_totals_after_poison_are_withheld_and_rebuilt_by_repair() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("poisoned-totals.redb");
    let fault = Arc::new(ScratchFault::default());
    let db = builder()
        .create_with_backend(fault_backend(&path, &fault))
        .expect("create fault-capable fixture");
    insert_rows(&db, 0..2, 0x41);
    fault.fail_writes.store(true, Ordering::SeqCst);
    let write = db.begin_write().expect("begin failing write");
    let failure = first_refusal(write, 2..130, 0x42, "scratch failure must refuse commit");
    assert!(failure.contains("injected allocator scratch write failure"));
    assert_eq!(
        db.allocator_free_totals(),
        None,
        "a poisoned allocator publishes no totals"
    );
    drop(db);

    let before = redb::Database::free_total_recounts_for_test();
    let (db, repaired) = reopen_with_repair(&path, "repair the poisoned writer's store");
    assert!(
        repaired,
        "the poisoned writer left a store that needs repair"
    );
    assert_eq!(
        redb::Database::free_total_recounts_for_test() - before,
        1,
        "repair rebuilds the totals once"
    );
    assert_totals_exact(&db, "after repair");
    assert_row(&db, 1, 0x41);
}

/// A storage fault while the totals are being rebuilt refuses the open: the
/// rebuild publishes nothing and completes no count, and the store keeps its
/// unwritten saved value, so a later healthy open rebuilds exact totals once.
#[test]
fn allocator_free_total_rebuild_fault_refuses_the_open_and_publishes_nothing() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("rebuild-fault.redb");
    create_regions(&path, 48);
    let db = builder().open(&path).expect("open seeded fixture");
    db.distrust_allocator_free_totals_for_test();
    drop(db);

    // Fail every main-file read from the moment the rebuild starts. Its
    // first bitmap window comes from the saved allocator state on disk.
    let fault = Arc::new(ScratchFault::default());
    let armed = Arc::clone(&fault);
    redb::Database::set_free_total_recount_start_hook_for_test(Some(Box::new(move || {
        armed.fail_backend_reads.store(true, Ordering::SeqCst);
    })));
    let completed_before = redb::Database::free_total_recounts_for_test();
    let opened = builder().open_with_backend(fault_backend(&path, &fault));
    redb::Database::set_free_total_recount_start_hook_for_test(None);
    assert!(
        fault.fail_backend_reads.load(Ordering::SeqCst),
        "the open must reach the rebuild"
    );
    assert!(
        fault.backend_read_failures.load(Ordering::SeqCst) > 0,
        "the injected fault must fire inside the rebuild"
    );
    assert!(
        opened.is_err(),
        "a rebuild that hit a storage fault refuses the open"
    );
    drop(opened);
    assert_eq!(
        redb::Database::free_total_recounts_for_test(),
        completed_before,
        "the failed rebuild completed no count"
    );

    let (db, rebuilds) = open_counting_rebuilds(&path);
    assert_eq!(
        rebuilds, 1,
        "nothing was saved, so the healthy open rebuilds once"
    );
    assert_totals_exact(&db, "rebuilt after the refused open");
    assert_row(&db, 0, 0x35);
}

/// Free-total geometry is layout arithmetic: opening, an ordinary trimming
/// commit, a relocation batch and a trimming clean close visit no region,
/// whether the store holds one region or many.
#[test]
fn allocator_free_total_geometry_costs_the_same_at_one_and_many_regions() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let mut visits = Vec::new();
    for rows in [1u64, 48] {
        let path = dir.path().join(format!("geometry-{rows}.redb"));
        let regions = create_regions(&path, rows);
        let before = redb::Database::free_total_geometry_region_visits_for_test();
        let db = builder().open(&path).expect("open fixture");
        remove_rows(&db, rows - 1..rows);
        db.begin_write()
            .expect("begin trimming commit")
            .commit()
            .expect("ordinary commit releases and trims");
        let mut cursor = redb::CompactionCursor::default();
        db.compact_step(&mut cursor, 8, |_| Ok(()))
            .expect("one relocation batch");
        assert_totals_exact(&db, "after trim and relocation");
        drop(db);
        visits.push((
            regions,
            redb::Database::free_total_geometry_region_visits_for_test() - before,
        ));
    }
    assert!(
        visits[1].0 > visits[0].0,
        "the fixtures differ in region count"
    );
    assert_eq!(
        (visits[0].1, visits[1].1),
        (0, 0),
        "open, trim, relocation and save visit no region for free-total geometry: {visits:?}"
    );
}

/// A backend whose file may not grow past `limit` bytes (0 = no limit),
/// refusing like a full disk quota, and which records the length it holds.
#[derive(Debug)]
struct QuotaBackend {
    inner: InMemoryBackend,
    limit: Arc<std::sync::atomic::AtomicU64>,
    length: Arc<std::sync::atomic::AtomicU64>,
}

impl StorageBackend for QuotaBackend {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read(offset, out)
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.inner.write(offset, data)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        let limit = self.limit.load(Ordering::SeqCst);
        if limit != 0 && len > limit {
            return Err(io::Error::from(io::ErrorKind::QuotaExceeded));
        }
        self.inner.set_len(len)?;
        self.length.store(len, Ordering::SeqCst);
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()
    }
}

/// Growth asks for at least twice the file. When storage refuses that for
/// quota, the allocation that needed the growth is retried at the size it
/// needs, so a file under its limit keeps admitting writes that fit instead of
/// refusing with most of the headroom unused.
#[test]
fn a_growth_refused_for_quota_retries_at_the_size_the_allocation_needs() {
    const VALUES: TableDefinition<u64, &[u8]> = TableDefinition::new("quota_growth");
    let limit = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let length = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let db = redb::Builder::new()
        .create_with_backend(QuotaBackend {
            inner: InMemoryBackend::new(),
            limit: Arc::clone(&limit),
            length: Arc::clone(&length),
        })
        .unwrap();
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(VALUES).unwrap();
        for key in 0..32u64 {
            table
                .insert(key, vec![0x11_u8; 8 * 1024].as_slice())
                .unwrap();
        }
    }
    write.commit().unwrap();
    let before = length.load(Ordering::SeqCst);
    let headroom = 128 * 1024;
    assert!(
        before > 2 * headroom,
        "fixture: doubling the file must overshoot the headroom ({before})"
    );
    limit.store(before + headroom, Ordering::SeqCst);

    let mut admitted = 0;
    for key in 1_000..1_064u64 {
        let Ok(write) = db.begin_write() else { break };
        let inserted = write.open_table(VALUES).and_then(|mut table| {
            table
                .insert(key, vec![0x22_u8; 16 * 1024].as_slice())
                .map(|_| ())
                .map_err(redb::TableError::from)
        });
        if inserted.is_err() || write.commit().is_err() {
            break;
        }
        admitted += 1;
    }
    let after = length.load(Ordering::SeqCst);
    assert!(
        after <= before + headroom,
        "growth never passes the limit ({after} > {})",
        before + headroom
    );
    assert!(
        after > before + headroom / 2,
        "a refused preferred growth retries at the size the allocation needs: the file stayed \
         at {after} bytes of {} after {admitted} admitted writes",
        before + headroom
    );
}
