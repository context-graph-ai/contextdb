//! A reader asked for history holds it inside its memory limit: a stored
//! history record too large for the room left is refused before it is copied
//! or decoded, the refusal gives back everything the request took, and the
//! same history is answered in full once the reader has room for it.
//!
//! The answer built from that history is held to the same limit: every copy
//! its construction keeps or builds is admitted before it is made, its
//! encoding is measured and admitted before it exists, and the finished answer
//! stays charged for as long as the caller holds it. The result-size ceiling
//! limits the answer itself, never the history it came from, and cancellation
//! or active time stops the answer's construction where it is.
//!
//! Measured with a counting global allocator, so what is compared is what the
//! request actually allocated, not what it said it would. Run one test at a
//! time in its own process: the allocator counts every thread.

#[path = "fixtures/isolated_process.rs"]
mod isolated_process;

use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_engine::Database;
use contextdb_engine::direct_file_reader::test_seams::{
    DirectCancellation, DirectFileReader, DirectFileReaderError, DirectImageMetadataKind,
    DirectImageState, DirectMetadataBody, DirectMetadataRequest, DirectMetadataResponse,
    DirectReaderConfig, charged_high_water_for_test, image_memory_for_test,
    metadata_with_cancellation_for_test, open_for_test, reset_charged_high_water_for_test,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct CountingAllocator;
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

unsafe impl GlobalAlloc for CountingAllocator {
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
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// The most the request's own allocations may exceed what it had admitted
/// when refused: the storage page the oversized record is read from, which
/// the reader's fixed page-cache reservation already covers, plus allocator
/// and bookkeeping noise.
const STORAGE_READ_SLACK_BYTES: usize = 256 * 1024;

/// Allocations made while measuring, from the first measured allocation.
fn measure<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    LIVE.store(0, Ordering::SeqCst);
    PEAK.store(0, Ordering::SeqCst);
    MEASURE.store(true, Ordering::SeqCst);
    let result = operation();
    MEASURE.store(false, Ordering::SeqCst);
    (result, PEAK.load(Ordering::SeqCst))
}

/// Bytes allocated while measuring that are still live now.
fn measured_live() -> usize {
    LIVE.load(Ordering::SeqCst)
}

struct FixedClock;

impl DeadlineClock for FixedClock {
    fn now_ms(&self) -> u64 {
        1
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn config(memory_limit: Option<usize>) -> DirectReaderConfig {
    config_with(memory_limit, 64 * 1024 * 1024)
}

fn config_with(memory_limit: Option<usize>, result_bytes: u64) -> DirectReaderConfig {
    let mut config = DirectReaderConfig::new(
        ReadLimits {
            result_rows: 1_000,
            result_bytes,
            work: 1_000_000,
            active_ms: 60_000,
            memory: 1024 * 1024 * 1024,
            cursor_page_rows: 100,
            cursor_page_bytes: 2 * 1024 * 1024,
            cursor_idle_ms: 10_000,
            cursor_lifetime_ms: 20_000,
        },
        Arc::new(FixedClock),
    );
    config.memory_limit = memory_limit;
    config
}

const WIDE_COLUMNS: usize = 4_000;

/// One ordinary table, a few commits of history, then one schema statement
/// whose stored record is large: a table declaring thousands of columns.
fn seed(path: &std::path::Path) {
    let database = Database::open(path).expect("open the history memory fixture");
    database
        .execute(
            "CREATE TABLE small_rows (id UUID PRIMARY KEY, body TEXT)",
            &HashMap::new(),
        )
        .expect("declare the ordinary table");
    for row in 0..8u128 {
        database
            .execute(
                "INSERT INTO small_rows (id, body) VALUES ($id, 'small')",
                &HashMap::from([(
                    "id".to_owned(),
                    contextdb_core::Value::Uuid(uuid::Uuid::from_u128(0x77_0000 + row)),
                )]),
            )
            .expect("insert an ordinary row");
    }
    let columns = (0..WIDE_COLUMNS)
        .map(|column| format!("wide_column_{column:05} TEXT"))
        .collect::<Vec<_>>()
        .join(", ");
    database
        .execute(
            &format!("CREATE TABLE wide_schema (id UUID PRIMARY KEY, {columns})"),
            &HashMap::new(),
        )
        .expect("declare the wide table");
    database.close().expect("close the fixture writer");
}

fn change_history(
    reader: &contextdb_engine::direct_file_reader::test_seams::DirectFileReader,
) -> Result<usize, DirectFileReaderError> {
    reader
        .metadata(DirectMetadataRequest::ImageState {
            kind: DirectImageMetadataKind::ChangeLog,
        })
        .map(|response| match response.body {
            DirectMetadataBody::ImageState {
                state: DirectImageState::ChangeLog(changes),
            } => changes.ddl.len(),
            other => panic!("change history answers as change-log state: {other:?}"),
        })
}

fn sync_response(
    reader: &DirectFileReader,
) -> Result<DirectMetadataResponse, DirectFileReaderError> {
    reader.metadata(DirectMetadataRequest::ImageState {
        kind: DirectImageMetadataKind::Sync,
    })
}

fn change_response(
    reader: &DirectFileReader,
) -> Result<DirectMetadataResponse, DirectFileReaderError> {
    reader.metadata(DirectMetadataRequest::ImageState {
        kind: DirectImageMetadataKind::ChangeLog,
    })
}

#[allow(dead_code)]
fn sync_state(
    reader: &contextdb_engine::direct_file_reader::test_seams::DirectFileReader,
) -> Result<usize, DirectFileReaderError> {
    reader
        .metadata(DirectMetadataRequest::ImageState {
            kind: DirectImageMetadataKind::Sync,
        })
        .map(|response| match response.body {
            DirectMetadataBody::ImageState {
                state: DirectImageState::Sync(state),
            } => state.tables.len(),
            other => panic!("sync provenance answers as sync state: {other:?}"),
        })
}

fn assert_result_bytes_refusal(error: &DirectFileReaderError, ceiling: u64) {
    use contextdb_core::read_contract::{ReadFailureDetail, ReadFailureKind, ReadFailureLimit};
    let DirectFileReaderError::ReadFailure(failure) = error else {
        panic!("a ceiling too small for the answer must be a typed read refusal: {error:?}");
    };
    assert_eq!(
        failure.kind(),
        ReadFailureKind::OwnerLimitExceeded,
        "a crossed byte ceiling is reported as OwnerLimitExceeded: {failure:?}"
    );
    let ReadFailureDetail::OwnerLimitExceeded(detail) = failure.detail() else {
        panic!("the refusal carries no ceiling: {failure:?}");
    };
    assert_eq!(detail.limit, ReadFailureLimit::ResultBytes);
    let required = detail
        .required
        .as_ref()
        .expect("a caller is told the number that would carry its answer");
    assert!(
        required.required_bytes > ceiling,
        "the number offered is larger than the ceiling that refused: {required:?}"
    );
}

#[test]
fn an_oversized_history_record_is_refused_before_it_is_copied_and_leaves_nothing_charged() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "an_oversized_history_record_is_refused_before_it_is_copied_and_leaves_nothing_charged",
    ) {
        return;
    }
    let root = tempfile::tempdir().expect("history memory directory");
    let path = root.path().join("history.db");
    seed(&path);

    // Room enough to answer everything: how much the complete history costs.
    let roomy = open_for_test(&path, config(None)).expect("open a reader with room");
    let (roomy_before, _, _) = image_memory_for_test(&roomy);
    let (answered, roomy_peak) = measure(|| change_history(&roomy));
    assert_eq!(answered.expect("the history answers with room"), 2);
    let (roomy_after, _, _) = image_memory_for_test(&roomy);
    let history_charge = roomy_after.used.saturating_sub(roomy_before.used);
    drop(roomy);

    // A limit that opens, with room for about a quarter of that history. The
    // open's own charge grows with the limit (the page cache is a share of
    // it), so the limit is found by asking the reader what it has left.
    let wanted = history_charge / 4;
    let mut limit = {
        let probe = open_for_test(&path, config(None)).expect("probe the open charge");
        let (usage, _, _) = image_memory_for_test(&probe);
        usage.used + wanted
    };
    let (tight, before, available) = loop {
        let tight = open_for_test(&path, config(Some(limit))).expect("open inside the tight limit");
        let (before, _, _) = image_memory_for_test(&tight);
        let available = limit.saturating_sub(before.used);
        if available <= wanted + wanted / 2 {
            break (tight, before, available);
        }
        limit = limit.saturating_sub(available - wanted);
    };
    let (refused, refused_peak) = measure(|| change_history(&tight));
    let (after, _, _) = image_memory_for_test(&tight);
    println!(
        "history memory: history_charge={history_charge} roomy_peak={roomy_peak} \
         limit={limit} open_used={} available={available} refused_peak={refused_peak} \
         after_refusal_used={}",
        before.used, after.used
    );
    match refused {
        Err(DirectFileReaderError::MemoryBudget { .. }) => {}
        other => panic!("the oversized history must be a typed memory refusal: {other:?}"),
    }
    assert_eq!(
        after.used, before.used,
        "a refused history request gives back every byte it took"
    );
    assert!(
        refused_peak <= available + STORAGE_READ_SLACK_BYTES,
        "the refused request allocated past what it could admit: peak={refused_peak} \
         available={available}"
    );
    assert!(
        roomy_peak > available + STORAGE_READ_SLACK_BYTES,
        "the fixture's history must genuinely not fit the tight limit: roomy_peak={roomy_peak} \
         available={available}"
    );
}

/// Many single-row commits into one table: distinct LSNs and distinct
/// `(lsn, row_id)` coordinates, one per commit, so `change_log_table_index`
/// and `change_log_lsn_refcounts` grow proportionally to row count rather
/// than staying a handful of entries the way `seed`'s eight rows do.
const MANY_ROWS: u64 = 12_000;

fn seed_many_rows(path: &std::path::Path) {
    seed_rows(path, MANY_ROWS);
}

/// Answers built from many row changes, but few enough to seed quickly.
const ANSWER_ROWS: u64 = 3_000;

fn seed_rows(path: &std::path::Path, rows: u64) {
    let database = Database::open(path).expect("open the many-rows history fixture");
    database
        .execute("CREATE TABLE m (id UUID PRIMARY KEY)", &HashMap::new())
        .expect("declare the many-rows table");
    for row in 0..rows {
        database
            .execute(
                "INSERT INTO m (id) VALUES ($id)",
                &HashMap::from([(
                    "id".to_owned(),
                    contextdb_core::Value::Uuid(uuid::Uuid::from_u128(row as u128)),
                )]),
            )
            .expect("insert one of many ordinary rows");
    }
    database
        .close()
        .expect("close the many-rows fixture writer");
}

/// Open a fresh reader at `limit` and report the room left after open (the
/// same technique `an_oversized_history_record_is_refused_before_it_is_copied_and_leaves_nothing_charged`
/// uses to find a limit a fixture's cost can be measured against): the
/// open's own charge is asked for directly rather than assumed, because it
/// grows with the limit itself (the page cache reservation is a share of
/// it).
fn open_at(
    path: &std::path::Path,
    limit: usize,
) -> (
    contextdb_engine::direct_file_reader::test_seams::DirectFileReader,
    usize,
) {
    let reader = open_for_test(path, config(Some(limit))).expect("open inside the tight limit");
    let (usage, _, _) = image_memory_for_test(&reader);
    let available = limit.saturating_sub(usage.used);
    (reader, available)
}

/// Converge on a `memory_limit` whose room left after open lands in
/// `[wanted, wanted + wanted / 2]`: enough to hold `wanted` bytes of request
/// work, not much more.
fn reader_with_available_near(
    path: &std::path::Path,
    wanted: usize,
) -> (
    contextdb_engine::direct_file_reader::test_seams::DirectFileReader,
    usize,
) {
    let mut limit = {
        let probe = open_for_test(path, config(None)).expect("probe the open charge");
        let (usage, _, _) = image_memory_for_test(&probe);
        usage.used + wanted
    };
    loop {
        let (reader, available) = open_at(path, limit);
        if available <= wanted + wanted / 2 && available.saturating_add(wanted / 2) >= wanted {
            return (reader, available);
        }
        if available > wanted + wanted / 2 {
            limit = limit.saturating_sub(available - wanted);
        } else {
            limit = limit.saturating_add(wanted - available);
        }
    }
}

/// A reader whose room after open is within a hundredth of `wanted`, and not
/// below it.
fn reader_with_available_within(
    path: &std::path::Path,
    wanted: usize,
) -> (
    contextdb_engine::direct_file_reader::test_seams::DirectFileReader,
    usize,
) {
    let mut limit = {
        let probe = open_for_test(path, config(None)).expect("probe the open charge");
        let (usage, _, _) = image_memory_for_test(&probe);
        usage.used + wanted
    };
    for _ in 0..64 {
        let (reader, available) = open_at(path, limit);
        if available >= wanted && available <= wanted + wanted / 100 {
            return (reader, available);
        }
        if available > wanted {
            limit -= (available - wanted) / 2 + 1;
        } else {
            limit += wanted - available;
        }
    }
    panic!("no limit leaves room within a hundredth of {wanted}");
}

/// Open a fresh reader at `limit` and try to force its history log gap the
/// same way `fallback_reader` does, WITHOUT letting a too-tight limit end
/// the test: `force_history_log_gap_for_test`'s `ensure_image_history()`
/// panics rather than returning an error when there is not enough room for
/// the load, so a limit below the threshold is caught with `catch_unwind`
/// and reported as `None` rather than as a crash.
fn try_open_at_after_history_load(
    path: &std::path::Path,
    limit: usize,
) -> Option<(
    contextdb_engine::direct_file_reader::test_seams::DirectFileReader,
    usize,
)> {
    use contextdb_engine::direct_file_reader::test_seams::force_history_log_gap_for_test;
    let reader = open_for_test(path, config(Some(limit))).expect("open inside the tight limit");
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        force_history_log_gap_for_test(&reader);
    })) {
        Ok(()) => {
            let (usage, _, _) = image_memory_for_test(&reader);
            let available = limit.saturating_sub(usage.used);
            Some((reader, available))
        }
        Err(_) => None,
    }
}

/// The smallest `limit` (within 64 bytes) between `known_too_small` and
/// `known_safe` whose forced history log gap succeeds. `known_too_small`
/// must genuinely be too small (an open-only probe's own charge, with no
/// room left over, is always too small) and `known_safe` must genuinely
/// succeed (an unlimited reader's own used bytes after its own successful
/// load, added to that same probe's charge, always does) -- the bisection
/// never has to guess at either end. A declared limit changes the resident
/// proof cache's own ceiling (a share of the declared session), so this is
/// the actual cost the load pays at each candidate limit, not a delta
/// transferred from a differently-provisioned reader.
fn history_load_threshold(
    path: &std::path::Path,
    known_too_small: usize,
    known_safe: usize,
) -> usize {
    let mut lower = known_too_small;
    let mut upper = known_safe;
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    while upper - lower > 64 {
        let mid = lower + (upper - lower) / 2;
        match try_open_at_after_history_load(path, mid) {
            Some(_) => upper = mid,
            None => lower = mid,
        }
    }
    std::panic::set_hook(previous_hook);
    upper
}

/// A ceiling far below the answer's encoded size refuses it on that size,
/// before the encoding is built, and a refused request leaves nothing of its
/// own charged.
#[test]
fn a_roomy_reader_with_a_tight_result_ceiling_refuses_before_the_answer_is_encoded() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_roomy_reader_with_a_tight_result_ceiling_refuses_before_the_answer_is_encoded",
    ) {
        return;
    }
    let root = tempfile::tempdir().expect("history memory directory");
    let path = root.path().join("history.db");
    seed(&path);

    let ceiling = 512u64;
    let reader = open_for_test(&path, config_with(None, ceiling)).expect("open with ample memory");
    let (before, _, _) = image_memory_for_test(&reader);
    let (answered, _peak) = measure(|| change_history(&reader));
    let (after, _, _) = image_memory_for_test(&reader);
    let error = answered.expect_err("a ceiling this small cannot carry the fixture's history");
    assert_result_bytes_refusal(&error, ceiling);
    // Ample memory means the history behind this answer loads and is
    // cached regardless of the result-ceiling refusal -- that cache is the
    // reader's own, not this one refused request's, and it is what a later
    // request (with room to spare, or a wider ceiling) would reuse. What a
    // refused request must not do is grow that charge further, or leave any
    // unadmitted charge of its own behind: a second refusal changes nothing.
    let (answered_again, _peak_again) = measure(|| change_history(&reader));
    let (after_again, _, _) = image_memory_for_test(&reader);
    assert_result_bytes_refusal(
        &answered_again.expect_err("the same ceiling refuses the same way a second time"),
        ceiling,
    );
    assert_eq!(
        after.used, after_again.used,
        "a repeated result-ceiling refusal against a cached history charges nothing further"
    );
    assert!(
        after.used >= before.used,
        "the cached history is never smaller than what the reader opened with"
    );
}

/// The result ceiling limits the sync answer by the answer's own size. The
/// fixture's history is large -- one schema statement declares thousands of
/// columns -- while its sync answer is a few row arrivals and two tables'
/// policy: it answers under a ceiling of exactly its own encoded size and is
/// refused, typed, one byte below it, without copying the history either time.
#[test]
fn a_small_sync_answer_over_a_large_history_is_limited_by_its_own_size() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_small_sync_answer_over_a_large_history_is_limited_by_its_own_size",
    ) {
        return;
    }
    let root = tempfile::tempdir().expect("history memory directory");
    let path = root.path().join("history.db");
    seed(&path);

    let roomy = open_for_test(&path, config(None)).expect("open a reader with room");
    let (before, _, _) = image_memory_for_test(&roomy);
    let answered = sync_response(&roomy).expect("with room, sync provenance answers");
    let (after, _, _) = image_memory_for_test(&roomy);
    let answer_bytes = answered.canonical_bytes.len();
    let expected = answered.body.clone();
    drop(answered);
    let history_charge = after.used.saturating_sub(before.used);
    assert!(
        answer_bytes.saturating_mul(16) < history_charge,
        "the fixture needs a large history and a small sync answer: answer={answer_bytes} \
         history_charge={history_charge}"
    );
    drop(roomy);

    let fitting = open_for_test(&path, config_with(None, answer_bytes as u64))
        .expect("open with a ceiling of exactly the answer");
    // The first request loads the history; the measured one reads it cached.
    sync_response(&fitting).expect("the answer fits a ceiling of its own size");
    let (fits, fitting_peak) = measure(|| sync_response(&fitting));
    assert_eq!(
        fits.expect("the answer fits a ceiling of its own size")
            .body,
        expected,
        "the same sync answer, under a ceiling of exactly its size"
    );

    let ceiling = answer_bytes as u64 - 1;
    let tight = open_for_test(&path, config_with(None, ceiling)).expect("open one byte tighter");
    assert_result_bytes_refusal(
        &sync_response(&tight).expect_err("one byte short of the answer refuses it"),
        ceiling,
    );
    let (refused, refused_peak) = measure(|| sync_response(&tight));
    assert_result_bytes_refusal(
        &refused.expect_err("one byte short of the answer refuses it again"),
        ceiling,
    );
    println!(
        "sync answer: answer_bytes={answer_bytes} history_charge={history_charge} \
         fitting_peak={fitting_peak} refused_peak={refused_peak}"
    );
    assert!(
        refused_peak <= fitting_peak,
        "a refused answer allocates no more than the one that fits: refused={refused_peak} \
         fitting={fitting_peak}"
    );
    assert!(
        refused_peak.saturating_mul(4) < history_charge,
        "neither request copies the history its answer is derived from: \
         refused={refused_peak} history_charge={history_charge}"
    );
}

/// A reader with room for the fixture's history, but not for the answer
/// projected from it too, is refused as a typed memory budget -- not
/// answered by holding the whole projected answer past its own limit until
/// `encode_metadata_body`'s result finally gets checked.
#[test]
fn a_reader_with_room_for_history_but_not_its_answer_is_refused_typed_and_clean() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_reader_with_room_for_history_but_not_its_answer_is_refused_typed_and_clean",
    ) {
        return;
    }
    let root = tempfile::tempdir().expect("history memory directory");
    let path = root.path().join("history.db");
    seed(&path);

    let roomy = open_for_test(&path, config(None)).expect("open a reader with room");
    let (roomy_before, _, _) = image_memory_for_test(&roomy);
    assert_eq!(
        change_history(&roomy).expect("the history answers with room"),
        2
    );
    let (roomy_after, _, _) = image_memory_for_test(&roomy);
    // What is retained once the answer itself has been returned and
    // dropped: the cached history (and, since the fix, its aux indexes),
    // never the transient answer -- exactly the number a later request's
    // own admission is sized off.
    let history_charge = roomy_after.used.saturating_sub(roomy_before.used);
    drop(roomy);

    let (tight, available) = reader_with_available_near(&path, history_charge);
    let (before, _, _) = image_memory_for_test(&tight);
    let (refused, refused_peak) = measure(|| change_history(&tight));
    let (after, _, _) = image_memory_for_test(&tight);
    match refused {
        Err(DirectFileReaderError::MemoryBudget { .. }) => {}
        other => panic!(
            "history fits but the answer projected from it does not: expected a typed memory \
             refusal, got {other:?} (history_charge={history_charge} available={available})"
        ),
    }
    assert_eq!(
        after.used, before.used,
        "a refused answer construction gives back every byte it took, including the history \
         load that preceded it"
    );
    assert!(
        refused_peak <= available + STORAGE_READ_SLACK_BYTES,
        "the refused request allocated past what it could admit: peak={refused_peak} \
         available={available}"
    );
}

/// A second request against an already-cached history still answers inside
/// the reader's own room, and still gives every transient byte back once it
/// returns -- the cache is not a place a second request's own admission can
/// be skipped.
#[test]
fn repeated_requests_after_history_is_cached_still_answer_and_stay_within_room() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "repeated_requests_after_history_is_cached_still_answer_and_stay_within_room",
    ) {
        return;
    }
    let root = tempfile::tempdir().expect("history memory directory");
    let path = root.path().join("history.db");
    seed(&path);

    let reader = open_for_test(&path, config(None)).expect("open a reader with room");
    assert_eq!(change_history(&reader).expect("first answer"), 2);
    let (after_first, _, _) = image_memory_for_test(&reader);
    assert_eq!(
        change_history(&reader).expect("second answer, from cache"),
        2
    );
    let (after_second, _, _) = image_memory_for_test(&reader);
    assert_eq!(
        after_first.used, after_second.used,
        "a cached history answers repeatedly without growing what the reader retains"
    );
}

/// A cached history's checkpoint is not exempted the second time it is
/// asked for: a request that arrives already cancelled is still refused as
/// cancelled, even though nothing is left to walk.
#[test]
fn a_cached_historys_second_request_still_honours_cancellation() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_cached_historys_second_request_still_honours_cancellation",
    ) {
        return;
    }
    use contextdb_engine::direct_file_reader::test_seams::{
        DirectCancellation, metadata_with_cancellation_for_test,
    };
    let root = tempfile::tempdir().expect("history memory directory");
    let path = root.path().join("history.db");
    seed(&path);

    let reader = open_for_test(&path, config(None)).expect("open a reader with room");
    assert_eq!(
        change_history(&reader).expect("first answer caches the history"),
        2
    );

    let cancellation = DirectCancellation::new();
    cancellation.cancel();
    let refused = metadata_with_cancellation_for_test(
        &reader,
        DirectMetadataRequest::ImageState {
            kind: DirectImageMetadataKind::ChangeLog,
        },
        &cancellation,
    );
    assert!(
        matches!(refused, Err(DirectFileReaderError::Cancelled)),
        "a cached history must still honour a request that arrives already cancelled: {refused:?}"
    );

    // The sync arm shares the same cached-history cache-hit path.
    let refused_sync = metadata_with_cancellation_for_test(
        &reader,
        DirectMetadataRequest::ImageState {
            kind: DirectImageMetadataKind::Sync,
        },
        &cancellation,
    );
    assert!(
        matches!(refused_sync, Err(DirectFileReaderError::Cancelled)),
        "the sync arm shares the same cached-history checkpoint: {refused_sync:?}"
    );
}

/// The aux indexes `ensure_image_history_within` derives from a loaded
/// change log (`change_log_table_index`, `change_log_lsn_refcounts`) are
/// admitted before they are built: a reader too tight to hold the decoded
/// change log and its aux indexes together is refused as a typed memory
/// budget rather than building either past what it can admit, and a second,
/// identically tight request against the same fixture charges nothing more
/// than the first left behind.
#[test]
fn many_row_changes_admit_their_aux_indexes_before_building_them_and_clean_up_on_refusal() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "many_row_changes_admit_their_aux_indexes_before_building_them_and_clean_up_on_refusal",
    ) {
        return;
    }
    let root = tempfile::tempdir().expect("history memory directory");
    let path = root.path().join("history.db");
    seed_many_rows(&path);

    let roomy = open_for_test(&path, config(None)).expect("open a reader with room");
    let (roomy_before, _, _) = image_memory_for_test(&roomy);
    assert_eq!(
        change_history(&roomy).expect("the many-rows history answers with room"),
        1,
        "one CREATE TABLE statement, then many ordinary inserts"
    );
    let (roomy_after, _, _) = image_memory_for_test(&roomy);
    let retained_charge = roomy_after.used.saturating_sub(roomy_before.used);
    drop(roomy);

    let (tight, available) = reader_with_available_near(&path, retained_charge);
    let (before, _, _) = image_memory_for_test(&tight);
    let (refused, refused_peak) = measure(|| change_history(&tight));
    let (after, _, _) = image_memory_for_test(&tight);
    match refused {
        Err(DirectFileReaderError::MemoryBudget { .. }) => {}
        other => panic!(
            "many rows' aux indexes must be a typed memory refusal when room is tight: \
             {other:?} (retained_charge={retained_charge} available={available})"
        ),
    }
    assert!(
        after.used <= before.used.saturating_add(retained_charge),
        "a refused request must never retain more than the history and aux indexes it could \
         legitimately have cached: before={} after={} retained_charge={retained_charge}",
        before.used,
        after.used
    );
    let (refused_again, _) = measure(|| change_history(&tight));
    let (after_again, _, _) = image_memory_for_test(&tight);
    assert!(
        matches!(
            refused_again,
            Err(DirectFileReaderError::MemoryBudget { .. })
        ),
        "the same tight limit refuses the same way a second time: {refused_again:?}"
    );
    assert_eq!(
        after.used, after_again.used,
        "a repeated refusal against an already-loaded (or already-refused) history charges \
         nothing further -- exact cleanup holds on every attempt, not just the first"
    );
    assert!(
        refused_peak
            <= available
                .saturating_add(retained_charge)
                .saturating_add(STORAGE_READ_SLACK_BYTES),
        "the refused request allocated past what it and its cached history could admit: \
         peak={refused_peak} available={available} retained_charge={retained_charge}"
    );
}

/// The complete first request -- history load, answer construction and
/// encoding -- never allocates past what the reader admitted for it, and the
/// answer it returns stays charged for exactly as long as the caller holds
/// it. A repeated answer from the cached history, with the reader's page
/// caches already warm, occupies no more than it is charged while held.
#[test]
fn a_history_answer_fits_its_admission_while_built_and_stays_charged_while_held() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_history_answer_fits_its_admission_while_built_and_stays_charged_while_held",
    ) {
        return;
    }
    let root = tempfile::tempdir().expect("history memory directory");
    let path = root.path().join("history.db");
    seed_rows(&path, ANSWER_ROWS);

    let reader = open_for_test(&path, config(None)).expect("open a reader with room");
    let (opened, _, _) = image_memory_for_test(&reader);
    let (first, first_peak) = measure(|| change_response(&reader));
    let first = first.expect("the history answers with room");
    let first_live = measured_live();
    let high_water = charged_high_water_for_test(&reader);
    let (holding_first, _, _) = image_memory_for_test(&reader);
    let admitted_peak = high_water.saturating_sub(opened.used);
    println!(
        "first answer: peak={first_peak} admitted_peak={admitted_peak} live={first_live} \
         held_charge={}",
        holding_first.used.saturating_sub(opened.used)
    );
    assert!(
        first_peak <= admitted_peak.saturating_add(STORAGE_READ_SLACK_BYTES),
        "the request allocated past what it admitted: peak={first_peak} \
         admitted={admitted_peak}"
    );
    drop(first);
    let (cached, _, _) = image_memory_for_test(&reader);
    assert!(
        cached.used < holding_first.used,
        "dropping the answer gives its charge back"
    );

    let (second, second_peak) = measure(|| change_response(&reader));
    let second = second.expect("the cached history answers again");
    let second_live = measured_live();
    let (holding_second, _, _) = image_memory_for_test(&reader);
    let second_charge = holding_second.used.saturating_sub(cached.used);
    println!(
        "cached answer: peak={second_peak} live={second_live} held_charge={second_charge} \
         canonical_bytes={}",
        second.canonical_bytes.len()
    );
    assert!(
        second_live <= second_charge + STORAGE_READ_SLACK_BYTES,
        "a held answer from the cached history occupies more than it is charged: \
         live={second_live} charged={second_charge}"
    );
    assert!(
        second_charge >= second.canonical_bytes.len(),
        "the held answer's encoding is part of its charge"
    );
    drop(second);
    let (released, _, _) = image_memory_for_test(&reader);
    assert_eq!(
        released.used, cached.used,
        "dropping the cached answer gives back exactly its charge"
    );
}

/// What one change answer costs a reader whose history is already loaded,
/// measured on an unlimited reader: the history's charge, the most one
/// answer's construction admits, and what the finished answer holds.
#[derive(Debug)]
struct OneAnswerBudget {
    history: usize,
    construction: usize,
    answer: usize,
}

impl OneAnswerBudget {
    /// Room for the history and one answer's construction, and half another
    /// answer more: one answer is built, but not a second beside a held
    /// first.
    fn room(&self) -> usize {
        self.history + self.construction + self.answer / 2
    }
}

fn one_answer_budget(path: &std::path::Path) -> OneAnswerBudget {
    let probe = open_for_test(path, config(None)).expect("open a reader with room");
    let (opened, _, _) = image_memory_for_test(&probe);
    sync_response(&probe).expect("a small answer loads the history");
    let (cached, _, _) = image_memory_for_test(&probe);
    reset_charged_high_water_for_test(&probe);
    let held = change_response(&probe).expect("the history answers with room");
    let (holding, _, _) = image_memory_for_test(&probe);
    let budget = OneAnswerBudget {
        history: cached.used.saturating_sub(opened.used),
        construction: charged_high_water_for_test(&probe).saturating_sub(cached.used),
        answer: holding.used.saturating_sub(cached.used),
    };
    drop(held);
    assert!(
        budget.answer <= budget.construction,
        "an answer never holds more than its construction admitted: {budget:?}"
    );
    budget
}

/// Room for the history and one answer: a second answer is refused, typed,
/// while the first is held, and answers once the first is dropped.
#[test]
fn a_second_answer_is_refused_while_the_first_is_held() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_second_answer_is_refused_while_the_first_is_held",
    ) {
        return;
    }
    let root = tempfile::tempdir().expect("history memory directory");
    let path = root.path().join("history.db");
    seed_rows(&path, ANSWER_ROWS);

    let budget = one_answer_budget(&path);

    let (reader, available) = reader_with_available_within(&path, budget.room());
    sync_response(&reader).expect("a small answer loads the history");
    let first = change_response(&reader).expect("the first answer fits");
    let refused = change_response(&reader).map(|response| response.canonical_bytes.len());
    println!("held answers: {budget:?} available={available}");
    match refused {
        Err(DirectFileReaderError::MemoryBudget { .. }) => {}
        other => panic!("a second answer beside a held one must be a typed refusal: {other:?}"),
    }
    drop(first);
    change_response(&reader).expect("once the first answer is dropped, the second fits");
}

/// Two requests racing on one reader with room for one answer: never both
/// hold one, a refusal is typed, and everything is given back afterwards.
#[test]
fn racing_requests_never_hold_two_answers_past_the_limit() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "racing_requests_never_hold_two_answers_past_the_limit",
    ) {
        return;
    }
    let root = tempfile::tempdir().expect("history memory directory");
    let path = root.path().join("history.db");
    seed_rows(&path, ANSWER_ROWS);

    let budget = one_answer_budget(&path);

    let (reader, _) = reader_with_available_within(&path, budget.room());
    sync_response(&reader).expect("a small answer loads the history");
    let (settled, _, _) = image_memory_for_test(&reader);
    let reader = Arc::new(reader);
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let outcomes = (0..2)
        .map(|_| {
            let reader = Arc::clone(&reader);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let answer = change_response(&reader);
                let outcome = match &answer {
                    Ok(_) => Ok(()),
                    Err(DirectFileReaderError::MemoryBudget { .. }) => Err(()),
                    Err(other) => panic!("a racing refusal must be typed: {other:?}"),
                };
                // Both hold whatever they got until both have finished.
                barrier.wait();
                drop(answer);
                outcome
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|thread| thread.join().expect("racing request thread"))
        .collect::<Vec<_>>();
    assert!(
        outcomes.iter().filter(|outcome| outcome.is_ok()).count() <= 1,
        "two answers were held at once past the limit: {outcomes:?}"
    );
    let (after, _, _) = image_memory_for_test(&reader);
    // Pressure may evict idle cached pages, so the reader can end below
    // where it started, never above.
    assert!(
        after.used <= settled.used,
        "racing requests give back everything they took: after={} settled={}",
        after.used,
        settled.used
    );
}

/// A clock that counts every reading taken while armed and cancels the
/// request at a chosen reading.
struct SteppingClock {
    armed: AtomicBool,
    reads: AtomicUsize,
    cancel_at: std::sync::Mutex<Option<(usize, DirectCancellation)>>,
}

impl DeadlineClock for SteppingClock {
    fn now_ms(&self) -> u64 {
        if !self.armed.load(Ordering::SeqCst) {
            return 1;
        }
        let read = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some((at, cancellation)) = self.cancel_at.lock().unwrap().as_ref()
            && read == *at
        {
            cancellation.cancel();
        }
        1 + read as u64
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

/// A reader over the many-rows fixture whose history is already cached, with
/// its clock armed from here on.
fn cached_reader_with(
    clock: Arc<SteppingClock>,
    active_ms: u64,
) -> (tempfile::TempDir, DirectFileReader) {
    let root = tempfile::tempdir().expect("history memory directory");
    let path = root.path().join("history.db");
    seed_rows(&path, ANSWER_ROWS);
    let mut config = config(None);
    config.limits.active_ms = active_ms;
    config.clock = clock.clone();
    let reader = open_for_test(&path, config).expect("open the checkpoint reader");
    change_response(&reader).expect("cache the history");
    clock.armed.store(true, Ordering::SeqCst);
    (root, reader)
}

fn cached_answer_under(
    cancel_at: Option<usize>,
    active_ms: u64,
) -> (
    Result<DirectMetadataResponse, DirectFileReaderError>,
    usize,
    usize,
) {
    let cancellation = DirectCancellation::new();
    let clock = Arc::new(SteppingClock {
        armed: false.into(),
        reads: 0.into(),
        cancel_at: std::sync::Mutex::new(cancel_at.map(|at| (at, cancellation.clone()))),
    });
    let (_root, reader) = cached_reader_with(clock.clone(), active_ms);
    let (answer, peak) = measure(|| {
        metadata_with_cancellation_for_test(
            &reader,
            DirectMetadataRequest::ImageState {
                kind: DirectImageMetadataKind::ChangeLog,
            },
            &cancellation,
        )
    });
    (answer, clock.reads.load(Ordering::SeqCst), peak)
}

/// A cached history's answer is projected record by record, and a request
/// cancelled half way through that projection stops there: cancelled, after
/// the projection had started and before it finished -- its allocation peak
/// lies strictly between an early stop and the whole answer.
#[test]
fn cancellation_stops_a_cached_answer_mid_projection() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "cancellation_stops_a_cached_answer_mid_projection",
    ) {
        return;
    }
    let (complete, steps, complete_peak) = cached_answer_under(None, u64::MAX / 4);
    complete.expect("an uncancelled request answers");
    let (cancelled, cancelled_steps, cancelled_peak) =
        cached_answer_under(Some(steps / 2), u64::MAX / 4);
    println!(
        "mid-projection cancellation: steps={steps} cancelled_steps={cancelled_steps} \
         complete_peak={complete_peak} cancelled_peak={cancelled_peak}"
    );
    assert!(
        matches!(cancelled, Err(DirectFileReaderError::Cancelled)),
        "a request cancelled mid-projection is answered as cancelled: {cancelled:?}"
    );
    assert!(
        cancelled_steps < steps,
        "the projection stopped where it was cancelled: {cancelled_steps} of {steps} steps"
    );
    assert!(
        cancelled_peak > complete_peak / 5 && cancelled_peak < complete_peak * 4 / 5,
        "the request stopped inside the projection, not before it started nor after it \
         finished: cancelled={cancelled_peak} complete={complete_peak}"
    );
}

/// Active time runs out half way through a cached answer's projection: the
/// request stops there with the active-time refusal.
#[test]
fn active_time_stops_a_cached_answer_mid_projection() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "active_time_stops_a_cached_answer_mid_projection",
    ) {
        return;
    }
    use contextdb_core::read_contract::{ReadFailureDetail, ReadFailureLimit};
    let (complete, steps, complete_peak) = cached_answer_under(None, u64::MAX / 4);
    complete.expect("a request with time to spare answers");
    let (expired, expired_steps, expired_peak) = cached_answer_under(None, (steps / 2) as u64);
    println!(
        "mid-projection expiry: steps={steps} expired_steps={expired_steps} \
         complete_peak={complete_peak} expired_peak={expired_peak}"
    );
    match expired {
        Err(DirectFileReaderError::ReadFailure(failure)) => match failure.detail() {
            ReadFailureDetail::OwnerLimitExceeded(detail) => {
                assert_eq!(detail.limit, ReadFailureLimit::ActiveMs)
            }
            other => panic!("the refusal names the active-time limit: {other:?}"),
        },
        other => panic!("a request out of active time is refused as such: {other:?}"),
    }
    assert!(
        expired_steps < steps,
        "the projection stopped when its time ran out: {expired_steps} of {steps} steps"
    );
    assert!(
        expired_peak > complete_peak / 5 && expired_peak < complete_peak * 4 / 5,
        "the request stopped inside the projection: expired={expired_peak} \
         complete={complete_peak}"
    );
}

/// Cancelled while its answer is being encoded, a request is refused rather
/// than answered.
#[test]
fn cancellation_during_encoding_refuses_the_answer() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "cancellation_during_encoding_refuses_the_answer",
    ) {
        return;
    }
    use contextdb_engine::direct_file_reader::test_seams::while_encoding_answers_for_test;
    let root = tempfile::tempdir().expect("history memory directory");
    let path = root.path().join("history.db");
    seed(&path);
    let reader = open_for_test(&path, config(None)).expect("open a reader with room");
    change_response(&reader).expect("cache the history");
    let cancellation = DirectCancellation::new();
    let cancel = cancellation.clone();
    let _hook = while_encoding_answers_for_test(move || cancel.cancel());
    let answered = metadata_with_cancellation_for_test(
        &reader,
        DirectMetadataRequest::ImageState {
            kind: DirectImageMetadataKind::ChangeLog,
        },
        &cancellation,
    );
    assert!(
        matches!(answered, Err(DirectFileReaderError::Cancelled)),
        "a request cancelled while its answer was encoded is refused: {answered:?}"
    );
}

/// The indexes a loaded history keeps beside it -- each table's list of row
/// changes and the per-commit counts -- are charged at what they occupy,
/// whatever the stored table names cost: every table's key is its name, so
/// many tables with long legal names make the index larger than any fixed
/// allowance per change. Built through the same admission a reader uses, the
/// kept structures never occupy more than they were charged.
#[test]
fn history_indexes_are_charged_for_long_table_names_and_list_growth() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "history_indexes_are_charged_for_long_table_names_and_list_growth",
    ) {
        return;
    }
    use contextdb_engine::database::ChangeLogAuxIndexFixtureForTest;
    for (tables, name_len, rows_per_table) in [(2_000, 4_096, 1), (64, 16, 1_500), (500, 1, 3)] {
        let mut fixture = ChangeLogAuxIndexFixtureForTest::new(tables, name_len, rows_per_table);
        let (charged, peak) = measure(|| fixture.build());
        let kept = measured_live();
        println!(
            "history indexes: tables={tables} name_len={name_len} rows_per_table={rows_per_table} \
             charged={charged} kept={kept} peak={peak}"
        );
        assert!(
            kept <= charged,
            "the kept indexes occupy more than they were charged: kept={kept} charged={charged} \
             (tables={tables} name_len={name_len} rows_per_table={rows_per_table})"
        );
        assert!(
            peak <= charged.saturating_add(charged / 2).saturating_add(4 * 1024),
            "building the indexes allocated past what was admitted for them: peak={peak} \
             charged={charged}"
        );
        drop(fixture);
    }
}

fn fallback_reader(
    path: &std::path::Path,
    memory_limit: Option<usize>,
) -> (DirectFileReader, usize) {
    use contextdb_engine::direct_file_reader::test_seams::force_history_log_gap_for_test;
    let reader =
        open_for_test(path, config(memory_limit)).expect("open a reader for the fallback path");
    force_history_log_gap_for_test(&reader);
    let (usage, _, _) = image_memory_for_test(&reader);
    (reader, usage.used)
}

/// A history answer built by the persisted-state fallback -- the path a
/// store takes when its change and schema logs no longer reach back to the
/// position asked for -- keeps the same promise as the ordinary path: it
/// answers every current row, it is admitted as it is built so a reader too
/// tight for it is refused, typed, before the allocation that would not fit,
/// and a returned answer stays charged while the caller holds it.
#[test]
fn a_history_answer_built_from_persisted_state_is_admitted_as_it_is_built() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_history_answer_built_from_persisted_state_is_admitted_as_it_is_built",
    ) {
        return;
    }
    use contextdb_engine::direct_file_reader::test_seams::held_answer_bytes_for_test;
    let root = tempfile::tempdir().expect("history memory directory");
    let path = root.path().join("history.db");
    seed_rows(&path, ANSWER_ROWS);

    let (roomy, cached) = fallback_reader(&path, None);
    let first = change_response(&roomy).expect("with room, the fallback history answers");
    let rows = match &first.body {
        DirectMetadataBody::ImageState {
            state: DirectImageState::ChangeLog(changes),
        } => changes.rows.len(),
        other => panic!("change history answers as change-log state: {other:?}"),
    };
    assert_eq!(
        rows, ANSWER_ROWS as usize,
        "the fallback answers every current row, not an empty history"
    );
    drop(first);
    // Warm: the reader's page caches, charged when it opened, are already
    // full, so what this request allocates is its own work and its answer.
    reset_charged_high_water_for_test(&roomy);
    let (answered, roomy_peak) = measure(|| change_response(&roomy));
    let answered = answered.expect("with room, the fallback history answers again");
    let held_live = measured_live();
    let admitted_peak = charged_high_water_for_test(&roomy).saturating_sub(cached);
    let answer_charge = held_answer_bytes_for_test(&answered);
    println!(
        "fallback answer: rows={rows} peak={roomy_peak} admitted_peak={admitted_peak} \
         live={held_live} held_charge={answer_charge} canonical_bytes={}",
        answered.canonical_bytes.len()
    );
    assert!(
        roomy_peak <= admitted_peak + STORAGE_READ_SLACK_BYTES,
        "the fallback allocated past what it admitted as it was built: peak={roomy_peak} \
         admitted={admitted_peak}"
    );
    let (before_drop, _, _) = image_memory_for_test(&roomy);
    drop(answered);
    // What this request left live once its answer is gone -- cache growth,
    // already charged to the reader -- is not the answer's; the rest is.
    let residue = measured_live();
    let answer_live = held_live.saturating_sub(residue);
    println!("fallback answer held: answer_live={answer_live} residue={residue}");
    assert!(
        answer_live <= answer_charge + 16 * 1024,
        "the held fallback answer occupies more than it is charged: \
         answer_live={answer_live} charged={answer_charge}"
    );
    let (released, _, _) = image_memory_for_test(&roomy);
    assert_eq!(
        before_drop.used - released.used,
        answer_charge,
        "dropping the answer releases exactly the answer's own admission -- no more, no less"
    );

    // What is still charged past the answer's own admission is this reader's
    // session-scoped retained proof pages (`content_proof`), warmed by the
    // row/history authentication both `first` and `answered` did and kept
    // for the whole read session rather than either individual answer -- one
    // live charge per shared page until its last holder drops it. Some of
    // what it retains rides for free on the resident cache's own fixed
    // floor; the rest is this statement's own charge. Both figures come from
    // seams, not a guessed constant, so this equality holds whatever the
    // actual byte counts are for this measurement.
    let (retained, accounted) =
        contextdb_engine::direct_file_reader::test_seams::retained_content_proof_bytes_for_test(
            &roomy,
        );
    let (_, resident_bytes, resident_charged) =
        contextdb_engine::direct_file_reader::test_seams::resident_proof_chunks_for_test(&roomy);
    let resident_floor_covered = resident_bytes - resident_charged;
    let session_retained_charge = accounted - resident_floor_covered;
    println!(
        "fallback session retained proof: retained={retained} accounted={accounted} \
         resident_bytes={resident_bytes} resident_charged={resident_charged} \
         session_retained_charge={session_retained_charge}"
    );
    assert_eq!(
        released.used - cached,
        session_retained_charge,
        "past the answer's own admission, exactly the session's retained proof-page charge is \
         left standing: retained={retained} accounted={accounted} \
         resident_bytes={resident_bytes} resident_charged={resident_charged}"
    );

    // Clearing the session's own retained proof pages -- as ending the read
    // session does -- gives that charge back too, returning the reader to
    // exactly its pre-request floor.
    contextdb_engine::direct_file_reader::test_seams::clear_statement_proofs_for_test(&roomy);
    let (after_session_clear, _, _) = image_memory_for_test(&roomy);
    assert_eq!(
        after_session_clear.used, cached,
        "once the session's retained proof pages are cleared, along with the answer's own \
         admission already released above, the reader returns to exactly its pre-request floor"
    );
    drop(roomy);

    // Room for a quarter of the answer beyond what the reader already holds,
    // plus a 64 KiB pad. A declared limit changes the resident proof cache's
    // own ceiling (a share of the declared session), so what the SAME
    // history load actually costs on a tightly-budgeted reader is not a
    // fixed offset from what it cost on the unlimited-budget `roomy` reader
    // above -- it can land on either side by a margin of a few KiB depending
    // on exactly where the bisected declaration falls. The pad is well under
    // a fortieth of the answer itself, so the room this leaves still refuses
    // the full second answer below.
    let probe = open_for_test(&path, config(None)).expect("probe the open charge");
    let (opened, _, _) = image_memory_for_test(&probe);
    drop(probe);
    const HISTORY_LOAD_MARGIN_BYTES: usize = 512 * 1024;
    let wanted = cached.saturating_sub(opened.used) + answer_charge / 4 + HISTORY_LOAD_MARGIN_BYTES;
    let (tight, _) = reader_with_available_within(&path, wanted);
    let (usage, _, _) = image_memory_for_test(&tight);
    let limit = usage.limit.expect("the tight reader has a memory limit");
    drop(tight);
    let (tight, before) = fallback_reader(&path, Some(limit));
    let (refused, refused_peak) = measure(|| change_response(&tight));
    let (after, _, _) = image_memory_for_test(&tight);
    let room = limit.saturating_sub(before);
    println!("fallback refusal: room={room} refused_peak={refused_peak} roomy_peak={roomy_peak}");
    match refused {
        Err(DirectFileReaderError::MemoryBudget { .. }) => {}
        other => panic!(
            "a fallback answer too large for the room left must be a typed memory refusal: {:?}",
            other.map(|response| response.canonical_bytes.len())
        ),
    }
    // A refused answer gives back its own admission in full; what may still
    // be standing past `before` is this reader's own session-retained proof
    // pages (`content_proof`) warmed by whatever authentication ran before
    // the refusal fired -- the same session-scoped charge item 4's success
    // path above accounts for, not the refused answer's own bytes. Both
    // figures come from seams, not a guessed constant.
    let (retained, accounted) =
        contextdb_engine::direct_file_reader::test_seams::retained_content_proof_bytes_for_test(
            &tight,
        );
    let (_, resident_bytes, resident_charged) =
        contextdb_engine::direct_file_reader::test_seams::resident_proof_chunks_for_test(&tight);
    let resident_floor_covered = resident_bytes - resident_charged;
    let session_retained_charge = accounted - resident_floor_covered;
    assert_eq!(
        after.used - before,
        session_retained_charge,
        "a refused fallback answer gives back its own admission; only this reader's own \
         session-retained proof pages are left standing: retained={retained} \
         accounted={accounted} resident_bytes={resident_bytes} \
         resident_charged={resident_charged}"
    );
    assert!(
        refused_peak <= room + STORAGE_READ_SLACK_BYTES,
        "the refused fallback allocated past what it could admit: peak={refused_peak} \
         room={room}"
    );
}

/// A store with `kept` live rows and `deleted` rows deleted one commit each,
/// whose deletions still owe their push: each is a pending deletion.
fn seed_pending_deletions(path: &std::path::Path, kept: u64, deleted: u64) {
    let database = Database::open(path).expect("open the pending-deletion fixture");
    database
        .execute(
            "CREATE TABLE p (id UUID PRIMARY KEY, body TEXT)",
            &HashMap::new(),
        )
        .expect("declare a synced table");
    for row in 0..kept + deleted {
        database
            .execute(
                "INSERT INTO p (id, body) VALUES ($id, 'body')",
                &HashMap::from([(
                    "id".to_owned(),
                    contextdb_core::Value::Uuid(uuid::Uuid::from_u128(0x5000_0000 + row as u128)),
                )]),
            )
            .expect("insert a row");
    }
    for row in kept..kept + deleted {
        database
            .execute(
                "DELETE FROM p WHERE id = $id",
                &HashMap::from([(
                    "id".to_owned(),
                    contextdb_core::Value::Uuid(uuid::Uuid::from_u128(0x5000_0000 + row as u128)),
                )]),
            )
            .expect("delete a row");
    }
    database
        .close()
        .expect("close the pending-deletion fixture");
}

/// (live rows, deleted rows, keys carried more than once) of a change answer.
fn answer_rows(response: &DirectMetadataResponse) -> (usize, usize, usize) {
    let DirectMetadataBody::ImageState {
        state: DirectImageState::ChangeLog(changes),
    } = &response.body
    else {
        panic!(
            "change history answers as change-log state: {:?}",
            response.body
        );
    };
    let mut keys = std::collections::HashMap::<String, usize>::new();
    for row in &changes.rows {
        *keys.entry(format!("{:?}", row.natural_key)).or_default() += 1;
    }
    (
        changes.rows.iter().filter(|row| !row.deleted).count(),
        changes.rows.iter().filter(|row| row.deleted).count(),
        keys.values().filter(|count| **count > 1).count(),
    )
}

/// Pending deletions complete a committed reader's history answer on both
/// paths: every deletion is carried once, a deleted key's older insert is
/// reconciled away rather than answered beside its deletion, the answer is
/// charged at what it holds while held, and dropping it gives back
/// everything except the statement's own retained proof-page charge (a
/// leaf-locator entry the resident set does not cover), which the
/// retained-proof accounting charges until the statement itself ends.
#[test]
fn pending_deletions_complete_the_history_answer_on_both_paths() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "pending_deletions_complete_the_history_answer_on_both_paths",
    ) {
        return;
    }
    use contextdb_engine::direct_file_reader::test_seams::{
        held_answer_bytes_for_test, statement_own_charge_for_test,
    };
    let root = tempfile::tempdir().expect("history memory directory");
    let path = root.path().join("history.db");
    seed_pending_deletions(&path, 5, 50);

    let primary = open_for_test(&path, config(None)).expect("open a reader");
    let answered = change_response(&primary).expect("the history answers");
    assert_eq!(
        answer_rows(&answered),
        (5, 50, 0),
        "five live rows, fifty deletions, each deleted key once (as its deletion)"
    );
    drop(answered);
    drop(primary);

    let (fallback, cached) = fallback_reader(&path, None);
    change_response(&fallback).expect("warm the fallback reader");
    let (answered, _) = measure(|| change_response(&fallback));
    let answered = answered.expect("the fallback history answers");
    let held_live = measured_live();
    let charged = held_answer_bytes_for_test(&answered);
    assert_eq!(
        answer_rows(&answered),
        (5, 50, 0),
        "the fallback carries every pending deletion, not only the live rows"
    );
    drop(answered);
    let residue = measured_live();
    assert!(
        held_live.saturating_sub(residue) <= charged + 16 * 1024,
        "the held answer occupies more than it is charged: live={} charged={charged}",
        held_live.saturating_sub(residue)
    );
    let statement_charge = statement_own_charge_for_test(&fallback);
    let (released, _, _) = image_memory_for_test(&fallback);
    assert_eq!(
        released.used,
        cached + statement_charge,
        "dropping the answer gives its charge back, except the statement's own retained \
         proof-page charge (a leaf-locator entry), read live from the seam rather than assumed \
         to be zero"
    );
}

/// Many pending deletions under a limit with room for only part of them are
/// refused, typed, before the collection that would not fit is built, and
/// the refusal gives back everything it took.
#[test]
fn many_pending_deletions_under_a_tight_limit_are_refused_before_they_are_built() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "many_pending_deletions_under_a_tight_limit_are_refused_before_they_are_built",
    ) {
        return;
    }
    use contextdb_engine::direct_file_reader::test_seams::held_answer_bytes_for_test;
    let root = tempfile::tempdir().expect("history memory directory");
    let path = root.path().join("history.db");
    seed_pending_deletions(&path, 0, 3_000);

    let (roomy, cached) = fallback_reader(&path, None);
    let answered = change_response(&roomy).expect("with room, the deletions answer");
    assert_eq!(
        answer_rows(&answered).1,
        3_000,
        "every pending deletion is answered"
    );
    let answer_charge = held_answer_bytes_for_test(&answered);
    drop(answered);
    drop(roomy);

    // At 3,000 pending deletions, the resident proof cache's ceiling being a
    // share of the declared limit (see `open_at`'s own doc comment) makes
    // the forced history load's real cost swing by much more, as the limit
    // is bisected, than a fixed pad transferred from the unlimited `roomy`
    // reader above can cover -- a pad this test tried at 32 KiB and again at
    // 512 KiB, and a larger pad made the room actually left at the failing
    // allocation SMALLER, not bigger, confirming the transfer itself, not
    // its size, was wrong. `history_load_threshold` finds the load's actual
    // minimum at each candidate limit directly (catching the panic
    // `force_history_log_gap_for_test` raises below that minimum, rather
    // than transferring a number from elsewhere), bisecting between a
    // limit definitely too small (an open-only probe's own charge, with
    // nothing left over) and one definitely safe (the unlimited reader's
    // own used bytes after its own successful load). A quarter of the
    // answer's charge is added on top of that measured minimum, which
    // still refuses the full answer below.
    let probe = open_for_test(&path, config(None)).expect("probe the open charge");
    let (opened, _, _) = image_memory_for_test(&probe);
    drop(probe);
    let threshold = history_load_threshold(&path, opened.used, opened.used + cached);
    let limit = threshold + answer_charge / 4;
    let (tight, before) = fallback_reader(&path, Some(limit));
    let (refused, refused_peak) = measure(|| change_response(&tight));
    let (after, _, _) = image_memory_for_test(&tight);
    let room = limit.saturating_sub(before);
    println!(
        "pending deletions: answer_charge={answer_charge} room={room} refused_peak={refused_peak}"
    );
    match refused {
        Err(DirectFileReaderError::MemoryBudget { .. }) => {}
        other => panic!(
            "pending deletions too large for the room left must be a typed memory refusal: {:?}",
            other.map(|response| answer_rows(&response))
        ),
    }
    assert_eq!(
        after.used, before,
        "a refused answer gives back every byte it took"
    );
    assert!(
        refused_peak <= room + STORAGE_READ_SLACK_BYTES,
        "the refused answer allocated past what it could admit: peak={refused_peak} room={room}"
    );
}

/// A request cancelled while it reads the pending deletions stops there.
#[test]
fn cancellation_stops_the_pending_deletion_read() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "cancellation_stops_the_pending_deletion_read",
    ) {
        return;
    }
    use contextdb_engine::direct_file_reader::test_seams::force_history_log_gap_for_test;
    let root = tempfile::tempdir().expect("history memory directory");
    let path = root.path().join("history.db");
    seed_pending_deletions(&path, 0, 3_000);
    let run = |cancel_at: Option<usize>| {
        let cancellation = DirectCancellation::new();
        let clock = Arc::new(SteppingClock {
            armed: false.into(),
            reads: 0.into(),
            cancel_at: std::sync::Mutex::new(cancel_at.map(|at| (at, cancellation.clone()))),
        });
        let mut config = config(None);
        config.limits.active_ms = u64::MAX / 4;
        config.clock = clock.clone();
        let reader = open_for_test(&path, config).expect("open the checkpoint reader");
        force_history_log_gap_for_test(&reader);
        clock.armed.store(true, Ordering::SeqCst);
        let (answer, peak) = measure(|| {
            metadata_with_cancellation_for_test(
                &reader,
                DirectMetadataRequest::ImageState {
                    kind: DirectImageMetadataKind::ChangeLog,
                },
                &cancellation,
            )
        });
        (
            answer.map(|response| answer_rows(&response)),
            clock.reads.load(Ordering::SeqCst),
            peak,
        )
    };
    let (complete, steps, complete_peak) = run(None);
    assert_eq!(complete.expect("an uncancelled request answers").1, 3_000);
    let (cancelled, cancelled_steps, cancelled_peak) = run(Some(steps / 3));
    println!(
        "pending-deletion cancellation: steps={steps} cancelled_steps={cancelled_steps} \
         complete_peak={complete_peak} cancelled_peak={cancelled_peak}"
    );
    assert!(
        matches!(cancelled, Err(DirectFileReaderError::Cancelled)),
        "a request cancelled while reading pending deletions is cancelled: {cancelled:?}"
    );
    assert!(
        cancelled_steps < steps && cancelled_peak < complete_peak,
        "the read stopped where it was cancelled: {cancelled_steps} of {steps} steps, \
         peak {cancelled_peak} of {complete_peak}"
    );
    assert!(
        steps > 3_000,
        "the pending-deletion read checks for cancellation as it goes: {steps} steps"
    );
}

/// A pending-deletion record the reader cannot read refuses the history as a
/// corrupt store on both paths; it is never answered without that deletion.
/// (A record damaged in the file is already refused when the reader opens,
/// which decodes every lineage record; this read failure is injected at the
/// history request.)
#[test]
fn an_unreadable_pending_deletion_record_refuses_the_history() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "an_unreadable_pending_deletion_record_refuses_the_history",
    ) {
        return;
    }
    use contextdb_engine::database::fail_next_pending_deletion_read_for_test;
    use contextdb_engine::direct_file_reader::test_seams::DirectStoreDiagnostic;
    let root = tempfile::tempdir().expect("history memory directory");
    let path = root.path().join("history.db");
    seed_pending_deletions(&path, 2, 3);
    let primary = open_for_test(&path, config(None)).expect("open a reader");
    let (fallback, _) = fallback_reader(&path, None);
    for (route, reader) in [("primary", &primary), ("fallback", &fallback)] {
        fail_next_pending_deletion_read_for_test();
        match change_response(reader) {
            Err(DirectFileReaderError::CorruptStore(DirectStoreDiagnostic::MalformedRecord, _)) => {
            }
            other => panic!(
                "{route}: an unreadable pending deletion is a corrupt store, not an answer \
                 without it: {:?}",
                other.map(|response| answer_rows(&response))
            ),
        }
        assert_eq!(
            answer_rows(&change_response(reader).expect("the next, readable request answers")),
            (2, 3, 0),
            "{route}: once the record reads, the answer is complete"
        );
    }
}

/// Wide tables the long-history fixture creates and drops again.
const ALTERATIONS: usize = 200;

/// A small current schema with a long, large schema history: one small
/// table that remains, after `ALTERATIONS` wide tables with long column
/// names were each created and dropped again. The first commit is a link,
/// so the schema history starts after the first position and a store whose
/// change log is gone still keeps it.
fn seed_long_schema_history(path: &std::path::Path) {
    let database = Database::open(path).expect("open the schema-history fixture");
    let tx = database.begin().expect("begin the first link");
    database
        .insert_edge(
            tx,
            uuid::Uuid::from_u128(0x6000_0001),
            uuid::Uuid::from_u128(0x6000_0002),
            "FIRST".to_owned(),
            HashMap::new(),
        )
        .expect("the first commit is a link");
    database.commit(tx).expect("commit the first link");
    database
        .execute("CREATE TABLE s (id UUID PRIMARY KEY)", &HashMap::new())
        .expect("declare the small table");
    for step in 0..ALTERATIONS {
        let columns = (0..4)
            .map(|column| format!("c{column}_{} TEXT", "x".repeat(240)))
            .collect::<Vec<_>>()
            .join(", ");
        database
            .execute(
                &format!("CREATE TABLE w{step:05} (id UUID PRIMARY KEY, {columns})"),
                &HashMap::new(),
            )
            .expect("declare a wide table");
        database
            .execute(&format!("DROP TABLE w{step:05}"), &HashMap::new())
            .expect("drop it again");
    }
    database
        .execute(
            "INSERT INTO s (id) VALUES ($id)",
            &HashMap::from([(
                "id".to_owned(),
                contextdb_core::Value::Uuid(uuid::Uuid::from_u128(0x6000_0003)),
            )]),
        )
        .expect("one current row");
    database.close().expect("close the schema-history fixture");
}

fn schema_history_reader(
    path: &std::path::Path,
    memory_limit: Option<usize>,
) -> (DirectFileReader, usize) {
    use contextdb_engine::direct_file_reader::test_seams::force_change_log_gap_for_test;
    let reader = open_for_test(path, config(memory_limit)).expect("open the reader");
    force_change_log_gap_for_test(&reader);
    let (usage, _, _) = image_memory_for_test(&reader);
    (reader, usage.used)
}

/// A history answer built by the fallback over a long schema history keeps
/// the history in its authored order when it fits, is refused -- typed,
/// before the reconstruction's excess allocation -- when the room is tight,
/// and stops where it is when cancelled during the reconstruction.
#[test]
fn a_long_schema_history_is_reconstructed_inside_the_limit() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_long_schema_history_is_reconstructed_inside_the_limit",
    ) {
        return;
    }
    use contextdb_engine::direct_file_reader::test_seams::held_answer_bytes_for_test;
    let root = tempfile::tempdir().expect("history memory directory");
    let path = root.path().join("history.db");
    seed_long_schema_history(&path);

    let (roomy, cached) = schema_history_reader(&path, None);
    let (answered, roomy_peak) = measure(|| change_response(&roomy));
    let answered = answered.expect("with room, the schema history answers");
    let answer_charge = held_answer_bytes_for_test(&answered);
    let DirectMetadataBody::ImageState {
        state: DirectImageState::ChangeLog(changes),
    } = &answered.body
    else {
        panic!("change history answers as change-log state");
    };
    assert_eq!(
        changes.ddl.len(),
        1 + 2 * ALTERATIONS,
        "the fallback carries the authored schema history, not the rendered current schema"
    );
    assert!(
        changes.ddl_lsn.windows(2).all(|pair| pair[0] <= pair[1]),
        "the schema history keeps its authored order"
    );
    assert_eq!(changes.rows.len(), 1, "the one current row");
    drop(answered);
    drop(roomy);

    // Room for the history already loaded and a fifth of the answer.
    let probe = open_for_test(&path, config(None)).expect("probe the open charge");
    let (opened, _, _) = image_memory_for_test(&probe);
    drop(probe);
    let wanted = cached.saturating_sub(opened.used) + answer_charge / 5;
    let (tight, _) = reader_with_available_within(&path, wanted);
    let (usage, _, _) = image_memory_for_test(&tight);
    let limit = usage.limit.expect("the tight reader has a memory limit");
    drop(tight);
    let (tight, before) = schema_history_reader(&path, Some(limit));
    let (refused, refused_peak) = measure(|| change_response(&tight));
    let (after, _, _) = image_memory_for_test(&tight);
    let room = limit.saturating_sub(before);
    println!(
        "schema history: statements={} answer_charge={answer_charge} roomy_peak={roomy_peak} \
         room={room} refused_peak={refused_peak}",
        1 + 2 * ALTERATIONS
    );
    match refused {
        Err(DirectFileReaderError::MemoryBudget { .. }) => {}
        other => panic!(
            "a schema history too large for the room left must be a typed memory refusal: {:?}",
            other.map(|response| response.canonical_bytes.len())
        ),
    }
    assert_eq!(
        after.used, before,
        "a refused answer gives back every byte it took"
    );
    assert!(
        refused_peak <= room + STORAGE_READ_SLACK_BYTES,
        "the refused reconstruction allocated past what it could admit: peak={refused_peak} \
         room={room}"
    );
}

/// A request cancelled while the fallback reconstructs a long schema history
/// stops there.
#[test]
fn cancellation_stops_a_schema_history_reconstruction() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "cancellation_stops_a_schema_history_reconstruction",
    ) {
        return;
    }
    use contextdb_engine::direct_file_reader::test_seams::force_change_log_gap_for_test;
    let root = tempfile::tempdir().expect("history memory directory");
    let path = root.path().join("history.db");
    seed_long_schema_history(&path);
    let run = |cancel_at: Option<usize>| {
        let cancellation = DirectCancellation::new();
        let clock = Arc::new(SteppingClock {
            armed: false.into(),
            reads: 0.into(),
            cancel_at: std::sync::Mutex::new(cancel_at.map(|at| (at, cancellation.clone()))),
        });
        let mut config = config(None);
        config.limits.active_ms = u64::MAX / 4;
        config.clock = clock.clone();
        let reader = open_for_test(&path, config).expect("open the checkpoint reader");
        force_change_log_gap_for_test(&reader);
        clock.armed.store(true, Ordering::SeqCst);
        let (answer, peak) = measure(|| {
            metadata_with_cancellation_for_test(
                &reader,
                DirectMetadataRequest::ImageState {
                    kind: DirectImageMetadataKind::ChangeLog,
                },
                &cancellation,
            )
        });
        (
            answer.map(|response| response.canonical_bytes.len()),
            clock.reads.load(Ordering::SeqCst),
            peak,
        )
    };
    let (complete, steps, complete_peak) = run(None);
    complete.expect("an uncancelled request answers");
    let (cancelled, cancelled_steps, cancelled_peak) = run(Some(steps / 2));
    println!(
        "schema reconstruction cancellation: steps={steps} cancelled_steps={cancelled_steps} \
         complete_peak={complete_peak} cancelled_peak={cancelled_peak}"
    );
    assert!(
        steps > 2 * ALTERATIONS,
        "the reconstruction checks for cancellation per statement: {steps} steps"
    );
    assert!(
        matches!(cancelled, Err(DirectFileReaderError::Cancelled)),
        "a request cancelled during the reconstruction is cancelled: {cancelled:?}"
    );
    assert!(
        cancelled_steps < steps && cancelled_peak < complete_peak,
        "the reconstruction stopped where it was cancelled: {cancelled_steps} of {steps} steps, \
         peak {cancelled_peak} of {complete_peak}"
    );
}
