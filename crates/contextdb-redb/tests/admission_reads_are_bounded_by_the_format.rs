//! The reader's protected admission section does a fixed, format-bounded
//! amount of work however large the store is, and it refuses a store whose
//! structure exceeds the format's own bounds before it reads anything.
//!
//! Promise (contextdb-intent, Policies 21-23, the bounded working-set contract):
//! a writer waiting for starting readers waits for work bounded by the
//! storage format, not by the size of the store. Opening a snapshot reads a
//! fixed number of pages and decodes no allocator regions; every structural
//! shortfall the writable open refuses is refused the same way, and a reader
//! never trusts a length the file claims before checking it against the
//! format.
//!
//! Test-estate reason, shared by every function here (2026-10-15, reader/writer
//! handoff): the oracle is relational and exact -- regions decoded == 0 and
//! bytes read <= the format's page budget at one and at many regions, and each
//! damaged store is refused with exactly the answer the native read-only open
//! gives. No earlier test bounds the reader's snapshot capture: today the work
//! equals the region count. Cases are rows of one helper: the schedules differ
//! only in the damage.

#![cfg(all(feature = "test-seams", feature = "contextdb-internal"))]

use redb::backends::FileBackend;
use redb::{StorageBackend, TableDefinition};
use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("admission_rows");
const PAGE: u64 = 4096;
/// The format's admission budget in base pages (header 1, system tree 4,
/// allocator-state branches 66, leaf prefix 1, key windows 34, offsets 2,
/// value 2), stated here independently of the crate's own constant.
const ADMISSION_PAGE_BUDGET: u64 = 110;
const BODY_BYTES: usize = 64 << 10;

fn builder() -> redb::Builder {
    let mut builder = redb::Builder::new();
    builder.set_region_size(32 * PAGE).set_cache_size(0);
    builder
}

/// A store whose large values force one small region each; returns the
/// region count the writer reports.
fn store_with_regions(path: &Path, rows: u64) -> usize {
    let db = builder().create(path).expect("create the region fixture");
    let payload = vec![0x35; BODY_BYTES];
    let mut write = db.begin_write().expect("begin the fixture write");
    write.set_quick_repair(true);
    {
        let mut table = write.open_table(TABLE).expect("open the fixture table");
        for key in 0..rows {
            table
                .insert(key, payload.as_slice())
                .expect("insert a fixture row");
        }
    }
    write.commit().expect("commit the fixture rows");
    let regions = db.allocator_regions_for_test();
    assert!(
        regions >= rows as usize,
        "one large value forces a distinct small region"
    );
    regions
}

/// A read-only backend that counts the bytes read through it.
struct CountingBackend {
    inner: FileBackend,
    read_bytes: Arc<AtomicU64>,
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
        self.read_bytes
            .fetch_add(out.len() as u64, Ordering::SeqCst);
        Ok(())
    }

    fn set_len(&self, _len: u64) -> io::Result<()> {
        Err(io::Error::other(
            "an admission read never resizes the store",
        ))
    }

    fn sync_data(&self) -> io::Result<()> {
        Err(io::Error::other("an admission read never syncs the store"))
    }

    fn write(&self, _offset: u64, _data: &[u8]) -> io::Result<()> {
        Err(io::Error::other("an admission read never writes the store"))
    }
}

fn counting_backend(path: &Path) -> (CountingBackend, Arc<AtomicU64>) {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open the store file");
    let read_bytes = Arc::new(AtomicU64::new(0));
    (
        CountingBackend {
            inner: FileBackend::new(file).expect("wrap the store file"),
            read_bytes: Arc::clone(&read_bytes),
        },
        read_bytes,
    )
}

/// Test-estate reason (2026-10-15): capturing the committed snapshot for registration costs the
/// same fixed bytes at one region and at 64, never more than the format's page budget, and decodes
/// no region. Today's capture (a read-only open followed by a read transaction) decodes every
/// region.
#[test]
fn capturing_the_committed_snapshot_reads_a_format_bounded_number_of_bytes() {
    let directory = tempfile::tempdir().expect("fixture directory");
    let mut observed = Vec::new();
    for rows in [1u64, 64] {
        let path = directory.path().join(format!("capture-{rows}.redb"));
        store_with_regions(&path, rows);
        let (backend, read_bytes) = counting_backend(&path);
        let (captured, loaded) = redb::observe_allocator_region_loads_for_test(|| {
            let database = builder()
                .open_read_only_external_with_backend(backend)
                .expect("the external read-only view opens");
            database
                .committed_snapshot_for_registration()
                .map(|_| ())
                .map_err(|error| error.to_string())
        });
        captured.expect("an intact store captures");
        assert_eq!(loaded, 0, "capture decodes no allocator region");
        assert_eq!(
            redb::ADMISSION_PAGE_BUDGET,
            ADMISSION_PAGE_BUDGET,
            "the crate's budget is the format's derived 110 base pages"
        );
        let receipt = redb::last_admission_read_receipt()
            .expect("a capture leaves a receipt of what it charged");
        assert!(
            receipt.charged_pages <= ADMISSION_PAGE_BUDGET,
            "capture charged {} base pages at {rows} rows",
            receipt.charged_pages
        );
        let bytes = read_bytes.load(Ordering::SeqCst);
        assert!(
            bytes <= ADMISSION_PAGE_BUDGET * PAGE,
            "capture read {bytes} bytes at {rows} rows, over the {ADMISSION_PAGE_BUDGET}-page budget"
        );
        observed.push(bytes);
    }
    assert!(
        observed[1] <= observed[0] + 32 * PAGE,
        "capture work grew with the store: {} bytes at one row, {} at 64",
        observed[0],
        observed[1]
    );
}

/// Test-estate reason (2026-10-15): the windowed allocator-state lookup returns the same value
/// bytes as the full-page lookup over stores of several sizes, so bounding the read never changes
/// the answer.
#[test]
fn the_windowed_allocator_state_lookup_equals_the_full_page_lookup() {
    let directory = tempfile::tempdir().expect("fixture directory");
    for rows in [1u64, 8, 64] {
        let path = directory.path().join(format!("lookup-{rows}.redb"));
        store_with_regions(&path, rows);
        let (windowed, full) =
            redb::allocator_state_lookups_for_test(&path).expect("both lookups answer");
        assert_eq!(
            windowed, full,
            "the windowed lookup must return the same bytes as the full-page lookup \
             at {rows} rows"
        );
    }
}

/// Damage a store the way a bounded-format check must refuse, then compare the
/// reader's capture with what the native read-only open says.
fn assert_capture_refuses_exactly_what_the_native_open_refuses(damage: redb::StoreDamage) {
    let directory = tempfile::tempdir().expect("fixture directory");
    let path = directory.path().join("damaged.redb");
    store_with_regions(&path, 4);
    redb::damage_store_for_test(&path, damage).expect("apply the damage");

    let native = builder()
        .open_read_only(&path)
        .map(|_| ())
        .map_err(|error| error.to_string());
    let (backend, read_bytes) = counting_backend(&path);
    let captured = builder()
        .open_read_only_external_with_backend(backend)
        .map_err(|error| error.to_string())
        .and_then(|database| {
            database
                .committed_snapshot_for_registration()
                .map(|_| ())
                .map_err(|error| error.to_string())
        });
    assert_eq!(
        captured.is_err(),
        native.is_err(),
        "{damage:?}: the reader refuses exactly the stores the native read-only open refuses \
         (native {native:?}, capture {captured:?})"
    );
    assert!(
        read_bytes.load(Ordering::SeqCst) <= ADMISSION_PAGE_BUDGET * PAGE,
        "{damage:?}: even a refusal reads at most the format's budget"
    );
}

/// Test-estate reason (2026-10-15): every clean-state refusal of the native read-only open also
/// refuses the reader's capture.
#[test]
fn every_native_clean_state_predicate_refuses_the_capture_the_same_way() {
    for damage in [
        redb::StoreDamage::RecoveryRequired,
        redb::StoreDamage::LayoutLengthMismatch,
        redb::StoreDamage::TwoPhaseCommitUsed,
        redb::StoreDamage::AllocatorStateTableMissing,
        redb::StoreDamage::AllocatorStateInvalid,
    ] {
        assert_capture_refuses_exactly_what_the_native_open_refuses(damage);
    }
}

/// Test-estate reason (2026-10-15): an extent pointer larger than the format allows is refused as
/// corruption before the oversized extent is read.
#[test]
fn an_oversized_extent_pointer_is_refused_before_it_is_read() {
    let directory = tempfile::tempdir().expect("fixture directory");
    let path = directory.path().join("oversized-extent.redb");
    store_with_regions(&path, 4);
    redb::damage_store_for_test(&path, redb::StoreDamage::OversizedExtentPointer)
        .expect("apply the damage");
    let (backend, read_bytes) = counting_backend(&path);
    let refused = builder()
        .open_read_only_external_with_backend(backend)
        .map_err(|error| error.to_string())
        .and_then(|database| {
            database
                .committed_snapshot_for_registration()
                .map(|_| ())
                .map_err(|error| error.to_string())
        });
    assert!(
        refused.is_err(),
        "an oversized extent must be refused as corruption"
    );
    assert!(
        read_bytes.load(Ordering::SeqCst) <= ADMISSION_PAGE_BUDGET * PAGE,
        "the refusal came before reading past the format's budget"
    );
}

/// Test-estate reason (2026-10-15): a capture that needs n base pages, given a budget of n - 1, is
/// refused as corruption before it reads the page that would exceed the budget.
#[test]
fn a_budget_one_page_short_refuses_the_capture_before_it_reads_past_the_budget() {
    let directory = tempfile::tempdir().expect("fixture directory");
    let path = directory.path().join("short-budget.redb");
    store_with_regions(&path, 4);

    let (backend, _) = counting_backend(&path);
    let database = builder()
        .open_read_only_external_with_backend(backend)
        .expect("the external read-only view opens");
    database
        .committed_snapshot_for_registration()
        .expect("an intact store captures");
    let needed = redb::last_admission_read_receipt()
        .expect("a capture leaves a receipt")
        .charged_pages;
    assert!(needed >= 2, "a capture reads at least a header and a root");
    drop(database);

    let (backend, _) = counting_backend(&path);
    let database = builder()
        .open_read_only_external_with_backend(backend)
        .expect("the external read-only view opens");
    let _short = redb::override_admission_page_budget_for_test(needed - 1);
    let refused = database
        .committed_snapshot_for_registration()
        .map(|_| ())
        .map_err(|error| error.to_string());
    assert!(
        refused.is_err(),
        "a budget one page short must refuse the capture, not read past it"
    );
    assert!(
        redb::last_admission_read_receipt()
            .expect("a refused capture leaves a receipt")
            .charged_pages
            < needed,
        "the charge never exceeds the budget it was given"
    );
}

/// Test-estate reason (2026-10-15): a store whose region payload is malformed but whose user data
/// and clean-state keys are intact is readable by the reader, which decodes no region payloads, and
/// the damage is detected by the next writable open. Today the reader refuses it.
#[test]
fn a_malformed_region_payload_is_readable_by_the_reader_and_detected_by_the_writer() {
    let directory = tempfile::tempdir().expect("fixture directory");
    let path = directory.path().join("malformed-payload.redb");
    store_with_regions(&path, 4);
    redb::damage_store_for_test(&path, redb::StoreDamage::RegionPayloadMalformed)
        .expect("apply the damage");

    let (backend, _) = counting_backend(&path);
    let database = builder()
        .open_read_only_external_with_backend(backend)
        .expect("the reader's view opens: it decodes no region payload");
    database
        .committed_snapshot_for_registration()
        .expect("the reader captures a snapshot of a store whose user data is intact");

    let repaired = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = Arc::clone(&repaired);
    let mut writer = builder();
    writer.set_repair_callback(move |_| seen.store(true, Ordering::SeqCst));
    match writer.open(&path) {
        Err(_) => {}
        Ok(_) => assert!(
            repaired.load(Ordering::SeqCst),
            "the writable open must detect the malformed payload: repair it or refuse"
        ),
    }
}
