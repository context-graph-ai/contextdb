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
        builder().open_with_backend(FaultBackend {
            inner: file_backend(&small),
            fault: Arc::clone(&small_counter),
        })
    });
    let (large_db, large_loaded) = redb::observe_allocator_region_loads_for_test(|| {
        builder().open_with_backend(FaultBackend {
            inner: file_backend(&large),
            fault: Arc::clone(&large_counter),
        })
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
        .open_with_backend(FaultBackend {
            inner: file_backend(path),
            fault: Arc::clone(&fault),
        })
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
    eprintln!("first compaction admission: N={n:?}, 2N={twice_n:?}");
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
        .create_with_backend(FaultBackend {
            inner: file_backend(&path),
            fault: Arc::clone(&scratch),
        })
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

/// A scratch write failure during allocation must be returned, leave the last
/// durable root intact, and leave the header repairable after Database::drop.
#[test]
fn allocator_scratch_failure_prevents_publication_and_repairs_on_reopen() {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("fault.redb");
    let fault = Arc::new(ScratchFault::default());
    let db = builder()
        .create_with_backend(FaultBackend {
            inner: file_backend(&path),
            fault: Arc::clone(&fault),
        })
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
    let mut insertion_error = None;
    {
        let mut table = write.open_table(TABLE).expect("open failing table");
        let payload = value(0x44);
        for key in 1..129 {
            if let Err(error) = table.insert(key, payload.as_slice()) {
                insertion_error = Some(format!("{error:?}"));
                break;
            }
        }
    }
    let failure = if let Some(error) = insertion_error {
        drop(write);
        error
    } else {
        format!(
            "{:?}",
            write
                .commit()
                .expect_err("scratch failure must refuse commit")
        )
    };
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

    let repaired = Arc::new(AtomicBool::new(false));
    let callback_flag = Arc::clone(&repaired);
    let mut reopen_builder = builder();
    reopen_builder.set_repair_callback(move |_| callback_flag.store(true, Ordering::SeqCst));
    let reopened = reopen_builder
        .open(&path)
        .expect("repair and reopen failed writer");
    assert!(
        repaired.load(Ordering::SeqCst),
        "poisoned Drop must not publish a clean header"
    );
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
        .create_with_backend(FaultBackend {
            inner: file_backend(&path),
            fault: Arc::clone(&fault),
        })
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

    let repaired = Arc::new(AtomicBool::new(false));
    let callback = Arc::clone(&repaired);
    let mut reopen_builder = builder();
    reopen_builder.set_repair_callback(move |_| callback.store(true, Ordering::SeqCst));
    let reopened = reopen_builder
        .open(&path)
        .expect("repair after injected I/O");
    assert!(
        repaired.load(Ordering::SeqCst),
        "failed writer published a clean header"
    );
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
    let result = builder().open_with_backend(FaultBackend {
        inner: file_backend(&path),
        fault: Arc::clone(&fault),
    });
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
        .open_with_backend(FaultBackend {
            inner: file_backend(&path),
            fault: Arc::clone(&fault),
        })
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

    let repaired = Arc::new(AtomicBool::new(false));
    let callback = Arc::clone(&repaired);
    let mut reopen_builder = builder();
    reopen_builder.set_repair_callback(move |_| callback.store(true, Ordering::SeqCst));
    let reopened = reopen_builder
        .open(&path)
        .expect("repair after failed shrink");
    assert!(
        repaired.load(Ordering::SeqCst),
        "failed shrink wrote a clean header"
    );
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
        .open_with_backend(FaultBackend {
            inner: file_backend(&path),
            fault: Arc::clone(&fault),
        })
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
        .create_with_backend(FaultBackend {
            inner: file_backend(&path),
            fault: Arc::clone(&first_fault),
        })
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
    let mut insert_error = None;
    {
        let mut table = write.open_table(TABLE).unwrap();
        let payload = value(0x72);
        for key in 1..97 {
            if let Err(error) = table.insert(key, payload.as_slice()) {
                insert_error = Some(format!("{error:?}"));
                break;
            }
        }
    }
    let error = if let Some(error) = insert_error {
        drop(write);
        error
    } else {
        format!(
            "{:?}",
            write.commit().expect_err("scratch fault must refuse write")
        )
    };
    assert!(first_fault.hit(), "fixture did not poison the first writer");
    assert!(error.contains("injected allocator scratch write failure"));
    drop(db);

    let repair_fault = Arc::new(ScratchFault::default());
    repair_fault.arm(ScratchOperation::Len, 1);
    let error = match builder().open_with_backend(FaultBackend {
        inner: file_backend(&path),
        fault: Arc::clone(&repair_fault),
    }) {
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

    let repaired = Arc::new(AtomicBool::new(false));
    let callback = Arc::clone(&repaired);
    let mut reopen_builder = builder();
    reopen_builder.set_repair_callback(move |_| callback.store(true, Ordering::SeqCst));
    let reopened = reopen_builder.open(&path).expect("retry allocator repair");
    assert!(
        repaired.load(Ordering::SeqCst),
        "failed repair wrote a clean header"
    );
    assert_row(&reopened, 0, 0x31);
    let read = reopened.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    assert!(table.get(1).unwrap().is_none());
    assert!(table.get(96).unwrap().is_none());
}
