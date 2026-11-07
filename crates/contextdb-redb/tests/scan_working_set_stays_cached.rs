//! A one-pass scan keeps the pages it revisits cached while it runs.
//!
//! Promise: a reader's working set for one operation stays in the read cache
//! while that operation runs, within the reader's declared memory budget, and
//! what the reader is still charged once the operation ends is bounded by the
//! distinct pages it read, never by how often it revisited them (a page the
//! cache keeps may stay charged to the reader that loaded it until the cache
//! drops it, when no fixed cache reserve takes it over). A search
//! that probes the same B-tree pages over and over therefore reads each page
//! from the file once, not once per visit. When the reader's declared limit is
//! genuinely exhausted, the read is refused with the caller's own typed refusal
//! and nothing is allocated past the limit. The pages stay in the shared read
//! cache after the operation, so a follow-on query over them is warm, and the
//! native read entry point keeps the pages it reads cached the same way.
//!
//! The oracle is exact and counted at the storage backend: every read the
//! database issues to the file is recorded by offset. Nothing here measures
//! time. Each fixture's working set is far larger than the scan's free cache
//! growth (16 KiB, four pages) and far smaller than both the cache and the
//! reader's declared limit, so a cache that keeps the working set reads every
//! page exactly once however many times the scan revisits it.

use redb::backends::FileBackend;
use redb::{ReadPageCharge, ReadableDatabase, StorageBackend, StorageError, TableDefinition};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("scan_working_set");
const PAGE: usize = 4096;
const VALUE_BYTES: usize = 512;
/// Room for every fixture below many times over: the cache never has to
/// evict a page to make room for another one.
const CACHE_BYTES: usize = 64 << 20;
/// The reader's declared limit for the cached-working-set cases: well above
/// any fixture's working set, so it never has to refuse.
const READER_LIMIT: usize = 8 << 20;
/// How many times one operation revisits every key.
const PASSES: usize = 8;
/// A generous ceiling on what one cached page may keep charged to the reader
/// that loaded it: the page itself plus its allocation bookkeeping.
const CHARGE_PER_KEPT_PAGE: usize = 2 * PAGE;

/// A file backend that records how many times each offset was read.
struct CountingBackend {
    inner: FileBackend,
    reads: Arc<Mutex<Observed>>,
}

/// Reads per offset, and the bytes they returned.
#[derive(Default)]
struct Observed {
    by_offset: HashMap<u64, u64>,
    bytes: u64,
}

impl std::fmt::Debug for CountingBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CountingBackend")
    }
}

impl StorageBackend for CountingBackend {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read(offset, out)?;
        let mut reads = self.reads.lock().unwrap();
        *reads.by_offset.entry(offset).or_default() += 1;
        reads.bytes += out.len() as u64;
        Ok(())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.inner.write(offset, data)
    }
}

/// What the backend saw: distinct offsets, total reads, and the most reads
/// any one offset received.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileReads {
    distinct: u64,
    total: u64,
    most_for_one_page: u64,
}

fn file_reads(reads: &Mutex<Observed>) -> FileReads {
    let reads = reads.lock().unwrap();
    FileReads {
        distinct: reads.by_offset.len() as u64,
        total: reads.by_offset.values().sum(),
        most_for_one_page: reads.by_offset.values().copied().max().unwrap_or(0),
    }
}

fn forget_reads(reads: &Mutex<Observed>) {
    *reads.lock().unwrap() = Observed::default();
}

fn write_store(path: &Path, rows: u64) {
    let database = redb::Builder::new()
        .create(path)
        .expect("create the fixture store");
    let write = database.begin_write().expect("begin the fixture write");
    {
        let mut table = write.open_table(TABLE).expect("open the fixture table");
        let value = vec![0x5a; VALUE_BYTES];
        for key in 0..rows {
            table
                .insert(key, value.as_slice())
                .expect("insert a fixture row");
        }
    }
    write.commit().expect("commit the fixture rows");
}

/// Reopen the store for reading over a counting backend, so the read cache
/// starts empty and every page the scan needs must first come from the file.
fn open_counting(path: &Path) -> (redb::ReadOnlyDatabase, Arc<Mutex<Observed>>) {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open the fixture file");
    let reads = Arc::new(Mutex::new(Observed::default()));
    let backend = CountingBackend {
        inner: FileBackend::new(file).expect("wrap the fixture file"),
        reads: Arc::clone(&reads),
    };
    let mut builder = redb::Builder::new();
    builder.set_cache_size(CACHE_BYTES);
    let database = builder
        .open_read_only_with_backend(backend)
        .expect("reopen the fixture store");
    (database, reads)
}

/// The caller's typed refusal when its declared reader limit is reached.
#[derive(Debug)]
struct ReaderLimitReached {
    requested: usize,
    admitted: usize,
    limit: usize,
}

impl std::fmt::Display for ReaderLimitReached {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "reader limit {} reached: {} admitted, {} requested",
            self.limit, self.admitted, self.requested
        )
    }
}

impl std::error::Error for ReaderLimitReached {}

fn refusal(requested: usize, admitted: usize, limit: usize) -> StorageError {
    StorageError::AdmissionRefused(Box::new(ReaderLimitReached {
        requested,
        admitted,
        limit,
    }))
}

/// Bytes a reader currently holds: each admitted page's charge is returned
/// when the page's backing is released or the cache takes over paying for it.
#[derive(Default)]
struct HeldBytes {
    now: Mutex<usize>,
    peak: AtomicUsize,
    refusals: AtomicUsize,
}

struct HeldCharge {
    bytes: usize,
    held: Arc<HeldBytes>,
}

impl Drop for HeldCharge {
    fn drop(&mut self) {
        *self.held.now.lock().unwrap() -= self.bytes;
    }
}

/// A reader that declares `limit` bytes and refuses past it.
fn reader_budget(
    limit: usize,
    held: &Arc<HeldBytes>,
) -> Arc<dyn Fn(usize) -> Result<ReadPageCharge, StorageError> + Send + Sync> {
    let held = Arc::clone(held);
    Arc::new(move |bytes| {
        let mut now = held.now.lock().unwrap();
        match now.checked_add(bytes).filter(|after| *after <= limit) {
            Some(after) => {
                *now = after;
                held.peak.fetch_max(after, Ordering::SeqCst);
                Ok(Box::new(HeldCharge {
                    bytes,
                    held: Arc::clone(&held),
                }) as ReadPageCharge)
            }
            None => {
                held.refusals.fetch_add(1, Ordering::SeqCst);
                Err(refusal(bytes, *now, limit))
            }
        }
    })
}

/// The key order a probe takes: every key once per pass, in a stride that
/// jumps between leaves the way identity lookups do.
fn probe_order(rows: u64) -> impl Iterator<Item = u64> {
    (0..rows).map(move |index| (index * 7919) % rows)
}

/// One operation that probes every key `PASSES` times under one scan scope
/// and one declared reader budget. Returns the file reads seen after each
/// pass and the reader's held bytes once the operation has ended.
struct ScanRun {
    after_each_pass: Vec<FileReads>,
    held_after_end: usize,
    peak_held: usize,
    refusals: usize,
}

fn revisiting_scan(path: &Path, rows: u64) -> ScanRun {
    let (database, reads) = open_counting(path);
    scan_operation(&database, &reads, rows, PASSES)
}

/// One read operation on an already open store: its own read transaction,
/// scan scope and reader budget, probing every key `passes` times.
fn scan_operation(
    database: &redb::ReadOnlyDatabase,
    reads: &Mutex<Observed>,
    rows: u64,
    passes: usize,
) -> ScanRun {
    let read = database.begin_read().expect("begin the reader");
    let table = read.open_table(TABLE).expect("open the table to scan");
    forget_reads(reads);
    let held = Arc::new(HeldBytes::default());
    let after_each_pass =
        redb::with_read_page_charge_storage(reader_budget(READER_LIMIT, &held), || {
            redb::with_scan_reads(|| {
                (0..passes)
                    .map(|_| {
                        for key in probe_order(rows) {
                            let value = table
                                .get(key)
                                .expect("a key within the reader's budget is readable")
                                .expect("every fixture key is present");
                            assert_eq!(value.value().len(), VALUE_BYTES);
                        }
                        file_reads(reads)
                    })
                    .collect::<Vec<_>>()
            })
        });
    drop(table);
    drop(read);
    ScanRun {
        after_each_pass,
        held_after_end: *held.now.lock().unwrap(),
        peak_held: held.peak.load(Ordering::SeqCst),
        refusals: held.refusals.load(Ordering::SeqCst),
    }
}

/// A scan that revisits the same working set within its reader's budget
/// reads each page from the file once, and what its reader is still charged
/// once the operation ends is bounded by the distinct pages it read.
#[test]
fn a_scan_reads_each_page_it_revisits_from_the_file_once() {
    let directory = tempfile::tempdir().expect("fixture directory");
    let path = directory.path().join("revisit.redb");
    let rows = 512;
    write_store(&path, rows);
    let run = revisiting_scan(&path, rows);
    let last = *run.after_each_pass.last().unwrap();
    assert!(
        last.distinct >= 64,
        "the fixture's working set ({} pages) must be far larger than the scan's 4-page free growth",
        last.distinct
    );
    assert!(
        last.distinct as usize * PAGE * 4 <= READER_LIMIT,
        "the fixture's working set ({} pages) must sit well inside the reader's declared limit",
        last.distinct
    );
    assert_eq!(
        run.refusals, 0,
        "a working set inside the limit is never refused"
    );
    assert_eq!(
        last.total, last.distinct,
        "each of the {} distinct pages must be read from the file once over {PASSES} passes; \
         observed {} file reads, one page read {} times; reads after each pass: {:?}",
        last.distinct, last.total, last.most_for_one_page, run.after_each_pass
    );
    assert!(
        run.held_after_end <= last.distinct as usize * CHARGE_PER_KEPT_PAGE,
        "after the operation the reader is still charged {} bytes, more than its {} distinct \
         pages can account for (peak held {} bytes)",
        run.held_after_end,
        last.distinct,
        run.peak_held
    );
}

/// File reads follow the number of distinct pages a scan touches, not the
/// number of times it revisits them: doubling the working set roughly doubles
/// the reads, and every pass after the first reads nothing new.
#[test]
fn a_scan_reads_grow_with_distinct_pages_not_with_revisits() {
    let directory = tempfile::tempdir().expect("fixture directory");
    let mut first_pass = Vec::new();
    for rows in [512u64, 1024] {
        let path = directory.path().join(format!("pages-{rows}.redb"));
        write_store(&path, rows);
        let run = revisiting_scan(&path, rows);
        let first = run.after_each_pass[0];
        let last = *run.after_each_pass.last().unwrap();
        assert_eq!(
            run.refusals, 0,
            "a working set inside the limit is never refused"
        );
        assert_eq!(
            last.total,
            first.total,
            "{rows} rows: revisiting the same {} pages {} more times must add no file reads; \
             reads after each pass: {:?}",
            first.distinct,
            PASSES - 1,
            run.after_each_pass
        );
        first_pass.push(first);
    }
    let (small, large) = (first_pass[0], first_pass[1]);
    assert!(
        large.distinct * 2 >= small.distinct * 3,
        "the doubled fixture must touch materially more distinct pages ({} vs {})",
        large.distinct,
        small.distinct
    );
    assert!(
        large.total * 2 >= small.total * 3 && large.total <= small.total * 3,
        "one pass's file reads must grow with the distinct pages touched: {} reads for {} \
         pages against {} reads for {} pages",
        large.total,
        large.distinct,
        small.total,
        small.distinct
    );
}

/// When a scan needs more distinct pages than its reader declared, the read
/// is refused with the reader's own typed refusal, and no page the reader
/// was refused is allocated or read: admitted bytes never pass the limit and
/// the file never returns more bytes than were admitted.
#[test]
fn a_scan_past_its_declared_reader_limit_is_refused_and_allocates_nothing_past_it() {
    let directory = tempfile::tempdir().expect("fixture directory");
    let path = directory.path().join("exhausted.redb");
    let rows = 1024;
    write_store(&path, rows);
    let (database, reads) = open_counting(&path);
    let read = database.begin_read().expect("begin the reader");
    let table = read.open_table(TABLE).expect("open the table to scan");
    forget_reads(&reads);

    // A cumulative limit of 32 pages: the scan below touches far more
    // distinct pages than that, so the limit is genuinely exhausted however
    // well the cache keeps what it has already read.
    let limit = 32 * PAGE;
    let admitted = Arc::new(Mutex::new(0usize));
    let admit = {
        let admitted = Arc::clone(&admitted);
        Arc::new(move |bytes: usize| {
            let mut now = admitted.lock().unwrap();
            match now.checked_add(bytes).filter(|after| *after <= limit) {
                Some(after) => {
                    *now = after;
                    Ok(())
                }
                None => Err(refusal(bytes, *now, limit)),
            }
        })
    };
    let outcome = redb::with_read_memory_admission_storage(admit, || {
        redb::with_scan_reads(|| {
            for key in probe_order(rows) {
                table.get(key)?.expect("every fixture key is present");
            }
            Ok::<(), StorageError>(())
        })
    });

    let error = outcome.expect_err("a scan past its declared reader limit is refused");
    let StorageError::AdmissionRefused(cause) = &error else {
        panic!("the refusal must stay the reader's typed admission refusal, got {error:?}");
    };
    let cause = cause
        .downcast_ref::<ReaderLimitReached>()
        .unwrap_or_else(|| panic!("the refusal must carry the reader's own type, got {cause}"));
    assert_eq!(cause.limit, limit);
    assert!(cause.admitted + cause.requested > limit);
    let admitted = *admitted.lock().unwrap();
    assert!(
        admitted <= limit,
        "admitted {admitted} bytes past the {limit}-byte limit"
    );
    let seen = file_reads(&reads);
    let read_bytes = reads.lock().unwrap().bytes as usize;
    assert!(
        read_bytes <= admitted,
        "the file returned {read_bytes} bytes but only {admitted} were admitted ({seen:?})"
    );
}

/// The pages one scan read stay in the shared read cache after its scope
/// ends, so the next query over the same pages, within its own reader
/// budget, reads nothing from the file.
#[test]
fn a_scan_leaves_its_working_set_cached_for_the_next_query() {
    let directory = tempfile::tempdir().expect("fixture directory");
    let path = directory.path().join("follow-on.redb");
    let rows = 512;
    write_store(&path, rows);
    let (database, reads) = open_counting(&path);
    let first = scan_operation(&database, &reads, rows, 1);
    let populated = first.after_each_pass[0];
    assert!(
        populated.distinct >= 64,
        "the fixture's working set ({} pages) must be far larger than the scan's 4-page free growth",
        populated.distinct
    );
    assert_eq!(
        first.refusals, 0,
        "a working set inside the limit is never refused"
    );
    assert!(
        first.held_after_end <= populated.distinct as usize * CHARGE_PER_KEPT_PAGE,
        "after the first query its reader is still charged {} bytes, more than its {} distinct \
         pages can account for",
        first.held_after_end,
        populated.distinct
    );
    let second = scan_operation(&database, &reads, rows, 1);
    assert_eq!(
        second.refusals, 0,
        "a working set inside the limit is never refused"
    );
    assert_eq!(
        second.after_each_pass[0],
        FileReads {
            distinct: 0,
            total: 0,
            most_for_one_page: 0
        },
        "a follow-on query over the {} pages the first query read must read none of them \
         from the file (the first query read {} times)",
        populated.distinct,
        populated.total
    );
}

/// The native read entry point, under a declared native reader budget,
/// keeps the pages it reads cached the same way the ordinary read path must.
#[cfg(feature = "contextdb-internal")]
#[expect(
    clippy::big_endian_bytes,
    reason = "Opaque byte keys use big-endian order to preserve numeric lexical ordering."
)]
mod native_reads {
    use super::{
        CHARGE_PER_KEPT_PAGE, FileReads, Mutex, Observed, PASSES, Path, READER_LIMIT, VALUE_BYTES,
        file_reads, forget_reads, open_counting, probe_order,
    };
    use redb::{NativeStorageAdmission, NativeStorageCredit, NativeStorageRefused, TableHandle};
    use std::alloc::Layout;
    use std::ptr::NonNull;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const NATIVE_TABLE: redb::TableDefinition<&[u8], &[u8]> =
        redb::TableDefinition::new("native_scan_working_set");

    fn write_native_store(path: &Path, rows: u64) {
        let database = redb::Builder::new()
            .create(path)
            .expect("create the fixture store");
        let write = database.begin_write().expect("begin the fixture write");
        {
            let mut table = write
                .open_table(NATIVE_TABLE)
                .expect("open the fixture table");
            let value = vec![0x5a; VALUE_BYTES];
            for key in 0..rows {
                table
                    .insert(key.to_be_bytes().as_slice(), value.as_slice())
                    .expect("insert a fixture row");
            }
        }
        write.commit().expect("commit the fixture rows");
    }

    /// What the declared native budget has seen: bytes held now, the peak,
    /// and refusals.
    #[derive(Default)]
    struct BudgetStats {
        held: Mutex<usize>,
        peak: AtomicUsize,
        refusals: AtomicUsize,
    }

    /// The reader's scope cell. Every credit drawn on it retains it, and the
    /// last release frees it.
    struct BudgetCell {
        strong: AtomicUsize,
        limit: usize,
        stats: Arc<BudgetStats>,
    }

    /// One admitted allocation's receipt. Its own cell is counted in the
    /// charge and freed before the charge is refunded.
    struct ChargeCell {
        strong: AtomicUsize,
        bytes: usize,
        budget: NonNull<BudgetCell>,
    }

    fn retain<T>(cell: NonNull<()>, strong: impl Fn(&T) -> &AtomicUsize) {
        let count = strong(unsafe { cell.cast::<T>().as_ref() });
        let before = count.fetch_add(1, Ordering::SeqCst);
        assert!(before < usize::MAX / 2, "receipt strong count overflow");
    }

    unsafe fn retain_budget(cell: NonNull<()>) {
        retain::<BudgetCell>(cell, |budget| &budget.strong);
    }

    unsafe fn finish_budget(cell: NonNull<()>) {
        let cell = cell.cast::<BudgetCell>();
        if unsafe { cell.as_ref() }
            .strong
            .fetch_sub(1, Ordering::SeqCst)
            == 1
        {
            drop(unsafe { Box::from_raw(cell.as_ptr()) });
        }
    }

    unsafe fn retain_charge(cell: NonNull<()>) {
        retain::<ChargeCell>(cell, |charge| &charge.strong);
    }

    unsafe fn finish_charge(cell: NonNull<()>) {
        let cell = cell.cast::<ChargeCell>();
        if unsafe { cell.as_ref() }
            .strong
            .fetch_sub(1, Ordering::SeqCst)
            == 1
        {
            let charge = *unsafe { Box::from_raw(cell.as_ptr()) };
            let stats = Arc::clone(&unsafe { charge.budget.as_ref() }.stats);
            *stats.held.lock().unwrap() -= charge.bytes;
            unsafe { finish_budget(charge.budget.cast()) };
        }
    }

    unsafe fn admit(
        cell: NonNull<()>,
        layout: Layout,
    ) -> Result<NativeStorageCredit, NativeStorageRefused> {
        let budget = cell.cast::<BudgetCell>();
        let scope = unsafe { budget.as_ref() };
        let bytes = layout.size() + Layout::new::<ChargeCell>().size();
        {
            let mut held = scope.stats.held.lock().unwrap();
            let Some(after) = held
                .checked_add(bytes)
                .filter(|after| *after <= scope.limit)
            else {
                scope.stats.refusals.fetch_add(1, Ordering::SeqCst);
                return Err(NativeStorageRefused);
            };
            *held = after;
            scope.stats.peak.fetch_max(after, Ordering::SeqCst);
        }
        unsafe { retain_budget(cell) };
        let charge = NonNull::from(Box::leak(Box::new(ChargeCell {
            strong: AtomicUsize::new(1),
            bytes,
            budget,
        })));
        Ok(unsafe {
            NativeStorageCredit::from_owned_receipt(charge.cast(), retain_charge, finish_charge)
        })
    }

    /// A native reader that declares `limit` bytes and refuses past it.
    fn native_budget(limit: usize, stats: &Arc<BudgetStats>) -> NativeStorageAdmission {
        let cell = NonNull::from(Box::leak(Box::new(BudgetCell {
            strong: AtomicUsize::new(1),
            limit,
            stats: Arc::clone(stats),
        })));
        unsafe {
            NativeStorageAdmission::from_owned_receipt(
                cell.cast(),
                retain_budget,
                finish_budget,
                admit,
            )
        }
    }

    /// A native scan that revisits the same working set within its reader's
    /// declared budget reads each page from the file once, like the ordinary
    /// read path, and what its reader is still charged once the operation ends
    /// is bounded by the distinct pages it read.
    #[test]
    fn a_native_scan_reads_each_page_it_revisits_from_the_file_once() {
        let directory = tempfile::tempdir().expect("fixture directory");
        let path = directory.path().join("native-revisit.redb");
        let rows = 512;
        write_native_store(&path, rows);
        let (database, reads): (_, Arc<Mutex<Observed>>) = open_counting(&path);
        let stats = Arc::new(BudgetStats::default());
        let after_each_pass =
            redb::with_native_storage_admission(native_budget(READER_LIMIT, &stats), || {
                let view = database
                    .begin_native_read()
                    .expect("a native reader within its budget begins");
                let allowance = redb::ScanAllowance::try_new_native()
                    .expect("the scan allowance is admitted from the reader's budget");
                let scan = allowance
                    .try_enter_native()
                    .expect("the scan scope is admitted from the reader's budget");
                forget_reads(&reads);
                let after_each_pass = (0..PASSES)
                    .map(|_| {
                        for key in probe_order(rows) {
                            let value = view
                                .get_native_byte_value(
                                    NATIVE_TABLE.name(),
                                    key.to_be_bytes().as_slice(),
                                )
                                .expect("a key within the reader's budget is readable")
                                .expect("every fixture key is present");
                            assert_eq!(value.value().len(), VALUE_BYTES);
                        }
                        file_reads(&reads)
                    })
                    .collect::<Vec<FileReads>>();
                drop(scan);
                drop(allowance);
                drop(view);
                after_each_pass
            });
        let last = *after_each_pass.last().unwrap();
        assert!(
            last.distinct >= 64,
            "the fixture's working set ({} pages) must be far larger than the scan's 4-page free growth",
            last.distinct
        );
        assert_eq!(
            stats.refusals.load(Ordering::SeqCst),
            0,
            "a working set inside the limit is never refused"
        );
        assert_eq!(
            last.total, last.distinct,
            "each of the {} distinct pages must be read from the file once over {PASSES} native \
             passes; observed {} file reads, one page read {} times; reads after each pass: {:?}",
            last.distinct, last.total, last.most_for_one_page, after_each_pass
        );
        let held_after_end = *stats.held.lock().unwrap();
        assert!(
            held_after_end <= last.distinct as usize * CHARGE_PER_KEPT_PAGE,
            "after the operation the native reader is still charged {held_after_end} bytes, more \
             than its {} distinct pages can account for (peak held {} bytes)",
            last.distinct,
            stats.peak.load(Ordering::SeqCst)
        );
    }
}
