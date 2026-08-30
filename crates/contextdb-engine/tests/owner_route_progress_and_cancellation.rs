//! Progress reporting and cancellation over the OWNER (channel) route.
//!
//! `read_progress_reporting.rs` pins the file route: an idle store, read
//! in-process, no channel between the report and the caller. This file pins
//! the harder cross-process case -- a live owner in its own writable
//! `Database`, and a reader that dials it over the authenticated local
//! channel. The same promises must survive that hop: a running read reports
//! what it has done so far before it returns, cancelling from inside the
//! observer stops the owner's work and frees the slot it held while leaving
//! the session and its cursor usable, and a report belonging to one request
//! is never mistaken for another's.

#![cfg(feature = "test-seams")]

use contextdb_core::read_contract::{
    OwnerReadCancellation, OwnerReadLimits, OwnerServiceTimeouts, OwnerServingState,
    ReadClientTimeouts, ReadLimits, ReadRoute,
};
use contextdb_core::{Error, Value};
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, ReadPhase, ReadProgress, ReadProgressObserver,
    ReadSession, ReadSessionOptions,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

const FIXTURE_ROWS: i64 = 4_000;
const INSERT_BATCH: i64 = 500;

/// The rows a filtered scan is looking for, far enough apart that reaching
/// each one is its own long walk -- the same technique
/// `read_progress_reporting.rs` uses for the file route. The first needle is
/// close to the start, so a cursor's opening page (which finds it) stays
/// comfortably under one reporting interval of work; the second is far
/// enough past it that reaching it from the first is itself well past one
/// interval.
const NEEDLES: [i64; 2] = [200, 2_500];

fn secure_runtime_root(directory: &tempfile::TempDir, name: &str) -> std::path::PathBuf {
    let root = directory.path().join(name);
    std::fs::create_dir(&root).expect("create task-scoped owner runtime root");
    #[cfg(unix)]
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure task-scoped owner runtime root");
    root
}

/// Ceilings roomy enough that the fixture is never refused, so a test's
/// outcome is decided by progress/cancellation behavior, never by a ceiling
/// it never meant to exercise.
fn roomy_owner_options(runtime_dir: std::path::PathBuf) -> DatabaseOpenOptions {
    DatabaseOpenOptions {
        owner_reads: OwnerReadConfig {
            limits: OwnerReadLimits {
                limits: ReadLimits {
                    result_rows: 10_000,
                    result_bytes: 64 * 1024 * 1024,
                    work: 10_000_000,
                    active_ms: 600_000,
                    memory: 64 * 1024 * 1024,
                    cursor_page_rows: 100,
                    cursor_page_bytes: 4 * 1024 * 1024,
                    cursor_idle_ms: 600_000,
                    cursor_lifetime_ms: 1_800_000,
                },
                concurrency: 8,
            },
            timeouts: OwnerServiceTimeouts {
                request_ms: 60_000,
                shutdown_drain_ms: 10_000,
            },
            runtime_dir: Some(runtime_dir),
            handler: None,
            ..OwnerReadConfig::default()
        },
        ..DatabaseOpenOptions::default()
    }
}

/// A live writable owner over a fresh store, already seeded, serving from the
/// given runtime root.
fn live_owner(path: &Path, runtime_root: std::path::PathBuf) -> Database {
    let owner = Database::open_with_options(path, roomy_owner_options(runtime_root))
        .expect("start the writable owner");
    assert_eq!(
        owner.owner_read_status().state,
        OwnerServingState::Serving,
        "the owner must be serving before a reader dials it"
    );
    owner
}

/// Rows enough that a full scan crosses the progress-reporting interval
/// several times over. Two rows are marked `needle` at [`NEEDLES`], far
/// enough apart to give a filtered scan its own long walk between them.
fn seed_progress_rows(database: &Database) {
    database
        .execute(
            "CREATE TABLE owner_progress_rows (id INTEGER PRIMARY KEY, marker TEXT, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create the owner-route progress fixture table");
    let mut next = 0;
    while next < FIXTURE_ROWS {
        let tx = database
            .begin()
            .expect("begin an owner-route progress fixture batch");
        let last = (next + INSERT_BATCH).min(FIXTURE_ROWS);
        while next < last {
            let marker = if NEEDLES.contains(&next) {
                "needle"
            } else {
                "hay"
            };
            database
                .execute_in_tx(
                    tx,
                    "INSERT INTO owner_progress_rows (id, marker, payload) VALUES ($id, $marker, $payload)",
                    &HashMap::from([
                        ("id".to_owned(), Value::Int64(next)),
                        ("marker".to_owned(), Value::Text(marker.to_owned())),
                        ("payload".to_owned(), Value::Text(format!("row-{next}"))),
                    ]),
                )
                .unwrap_or_else(|error| panic!("insert owner-route progress row {next}: {error}"));
            next += 1;
        }
        database
            .commit(tx)
            .expect("commit an owner-route progress fixture batch");
    }
}

/// Everything one observer was told.
#[derive(Default)]
struct Recorder {
    reports: Mutex<Vec<ReadProgress>>,
}

impl ReadProgressObserver for Recorder {
    fn progress(&self, progress: ReadProgress) {
        self.reports
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(progress);
    }
}

impl Recorder {
    fn reports(&self) -> Vec<ReadProgress> {
        self.reports
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn of_phase(&self, phase: ReadPhase) -> Vec<ReadProgress> {
        self.reports()
            .into_iter()
            .filter(|progress| progress.phase == phase)
            .collect()
    }

    fn clear(&self) {
        self.reports
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }
}

/// An observer that cancels the read reporting to it the first time it hears
/// from it -- the same pattern `read_progress_reporting.rs` uses for the file
/// route, reused here so the owner-channel case is measured against the same
/// technique.
struct CancelOnFirstReport {
    cancellation: OwnerReadCancellation,
    reports: Mutex<Vec<ReadProgress>>,
}

impl ReadProgressObserver for CancelOnFirstReport {
    fn progress(&self, progress: ReadProgress) {
        self.reports
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(progress);
        self.cancellation.cancel();
    }
}

/// An observer that only cancels once ARMED. Used where an EARLIER operation
/// on the same session (opening a cursor, which reports its own progress
/// finding the cursor's first page) must not be mistaken for the operation
/// under test: arming happens only right before the call this test means to
/// interrupt, so a report from an earlier call is recorded nowhere and
/// cancels nothing.
struct ArmableCancelOnReport {
    armed: Mutex<Option<OwnerReadCancellation>>,
    reports_while_armed: Mutex<Vec<ReadProgress>>,
}

impl ArmableCancelOnReport {
    fn new() -> Self {
        Self {
            armed: Mutex::new(None),
            reports_while_armed: Mutex::new(Vec::new()),
        }
    }

    fn arm(&self, cancellation: OwnerReadCancellation) {
        *self
            .armed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(cancellation);
    }

    fn reports_while_armed_count(&self) -> usize {
        self.reports_while_armed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }
}

impl ReadProgressObserver for ArmableCancelOnReport {
    fn progress(&self, progress: ReadProgress) {
        let armed = self
            .armed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(cancellation) = armed.as_ref() {
            self.reports_while_armed
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(progress);
            cancellation.cancel();
        }
    }
}

/// The reader's OWN requested ceilings. On the owner route the effective
/// ceiling is the stricter of the reader's request and the owner's policy
/// (cli.md, "Declared limits"), so a reader that requests
/// `ReadSessionOptions::default()` (result_rows 500) is refused at 500 rows
/// no matter how roomy the owner's own policy is. These must be at least as
/// roomy as [`roomy_owner_options`] for a test that runs a fixture-sized read
/// to completion.
fn roomy_reader_options() -> ReadSessionOptions {
    ReadSessionOptions {
        limits: ReadLimits {
            result_rows: 10_000,
            result_bytes: 64 * 1024 * 1024,
            work: 10_000_000,
            active_ms: 600_000,
            memory: 64 * 1024 * 1024,
            cursor_page_rows: 100,
            cursor_page_bytes: 4 * 1024 * 1024,
            cursor_idle_ms: 600_000,
            cursor_lifetime_ms: 1_800_000,
        },
        timeouts: ReadClientTimeouts::default(),
        ..ReadSessionOptions::default()
    }
}

/// Reader options for the cursor-cancellation pin: one row per page, so
/// reaching a later needle is its own long walk done inside a single fetch
/// (the same technique `read_progress_reporting.rs` uses for the file
/// route).
fn one_row_page_reader_options() -> ReadSessionOptions {
    ReadSessionOptions {
        limits: ReadLimits {
            cursor_page_rows: 1,
            ..roomy_reader_options().limits
        },
        timeouts: ReadClientTimeouts::default(),
        ..ReadSessionOptions::default()
    }
}

fn owner_active_readers(path: &Path, runtime_root: &Path) -> u64 {
    let report = ReadSession::with_runtime_directory_for_test(runtime_root, || {
        ReadSession::owner_report(path, ReadSessionOptions::default())
    })
    .expect("the owner answers a status report over its channel");
    report
        .serving
        .expect("a serving owner's report carries its serving section")
        .admission
        .active_readers
}

#[test]
fn a_statement_over_the_owner_channel_reports_progress_before_the_result_returns_and_never_decreases()
 {
    let directory = tempfile::TempDir::new().expect("task-scoped owner-progress directory");
    let path = directory.path().join("owner-progress.db");
    let runtime_root = secure_runtime_root(&directory, "owner-progress-runtime");

    let owner = live_owner(&path, runtime_root.clone());
    seed_progress_rows(&owner);

    let recorder = Arc::new(Recorder::default());
    let reader = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_progress(
            &path,
            roomy_reader_options(),
            Arc::clone(&recorder) as Arc<dyn ReadProgressObserver>,
        )
    })
    .expect("a live owner selects the owner route");
    assert_eq!(reader.route(), ReadRoute::Owner);
    recorder.clear();

    let answered = reader
        .execute(
            "SELECT id FROM owner_progress_rows ORDER BY id",
            &HashMap::new(),
        )
        .expect("the scan over the owner channel answers");
    assert_eq!(answered.rows.len(), FIXTURE_ROWS as usize);

    let executing = recorder.of_phase(ReadPhase::Executing);
    assert!(
        !executing.is_empty(),
        "a scan of {FIXTURE_ROWS} rows over the owner channel reports its progress at least \
         once, before the result returns"
    );
    let mut previous = ReadProgress {
        phase: ReadPhase::Executing,
        rows: 0,
        bytes: 0,
        loaded_bytes: 0,
        total_bytes: None,
        work: 0,
        active_ms: 0,
    };
    for report in &executing {
        assert!(
            report.rows >= previous.rows
                && report.bytes >= previous.bytes
                && report.work >= previous.work,
            "reported counts over the owner channel never decrease: {previous:?} then {report:?}"
        );
        previous = *report;
    }
    let last = executing.last().expect("at least one report");
    assert!(
        last.rows > 0 && last.bytes > 0 && last.work > 0,
        "the last report before the answer carries real, positive counts: {last:?}"
    );
}

#[test]
fn cancelling_a_statement_from_inside_the_owner_channel_observer_frees_the_owner_slot_and_leaves_the_session_usable()
 {
    let directory = tempfile::TempDir::new().expect("task-scoped owner-cancellation directory");
    let path = directory.path().join("owner-cancel-execute.db");
    let runtime_root = secure_runtime_root(&directory, "owner-cancel-execute-runtime");

    let owner = live_owner(&path, runtime_root.clone());
    seed_progress_rows(&owner);

    let cancellation = OwnerReadCancellation::new();
    let observer = Arc::new(CancelOnFirstReport {
        cancellation: cancellation.clone(),
        reports: Mutex::new(Vec::new()),
    });
    let reader = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_progress(
            &path,
            roomy_reader_options(),
            Arc::clone(&observer) as Arc<dyn ReadProgressObserver>,
        )
    })
    .expect("a live owner selects the owner route");
    assert_eq!(reader.route(), ReadRoute::Owner);

    let refused = reader.execute_with_cancellation(
        "SELECT id FROM owner_progress_rows ORDER BY id",
        &HashMap::new(),
        &cancellation,
    );
    assert!(
        matches!(refused, Err(Error::ReadCancelled)),
        "cancelling from inside the observer, over the owner channel, yields ReadCancelled: \
         {refused:?}"
    );
    let heard = observer
        .reports
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .len();
    assert!(
        heard >= 1,
        "the caller heard from the read over the channel before it cancelled it"
    );

    assert_eq!(
        owner_active_readers(&path, &runtime_root),
        0,
        "the owner's in-flight reader count returns to zero once the cancelled read has \
         finished unwinding"
    );

    let next = reader
        .execute(
            "SELECT id FROM owner_progress_rows WHERE id = 0",
            &HashMap::new(),
        )
        .expect(
            "the SAME reader session's next statement succeeds: cancelling one statement did \
             not lose the session, its route, or its channel connection",
        );
    assert_eq!(next.rows.len(), 1);
}

#[test]
fn cancelling_a_cursor_fetch_from_inside_the_owner_channel_observer_leaves_the_cursor_open() {
    let directory = tempfile::TempDir::new().expect("task-scoped owner-cursor-cancel directory");
    let path = directory.path().join("owner-cancel-cursor.db");
    let runtime_root = secure_runtime_root(&directory, "owner-cancel-cursor-runtime");

    let owner = live_owner(&path, runtime_root.clone());
    seed_progress_rows(&owner);

    let observer = Arc::new(ArmableCancelOnReport::new());
    let reader = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_progress(
            &path,
            one_row_page_reader_options(),
            Arc::clone(&observer) as Arc<dyn ReadProgressObserver>,
        )
    })
    .expect("a live owner selects the owner route");

    // One row per page over a needle filter, with no ordering asked for: an
    // ordered answer would be collected and sorted while the cursor is
    // OPENED, leaving the fetch nothing to walk. The observer is not armed
    // yet, so whatever progress opening the cursor reports is inert. The
    // fetch below then has to walk past over two thousand hay rows to reach
    // the second needle -- work enough to cross the reporting interval, and
    // it is armed for exactly that call, so its own report is the one that
    // cancels it.
    let mut cursor = reader
        .open_cursor(
            "SELECT id FROM owner_progress_rows WHERE marker = $marker",
            &HashMap::from([("marker".to_owned(), Value::Text("needle".to_owned()))]),
        )
        .expect("open a cursor over the owner channel");
    // An open cursor is work the owner is holding; it keeps its reader slot
    // until it is closed, and a fetch takes no second one.
    let held_by_the_open_cursor = owner_active_readers(&path, &runtime_root);

    let cancellation = OwnerReadCancellation::new();
    observer.arm(cancellation.clone());
    let refused = cursor.fetch_with_cancellation(None, &cancellation);
    assert!(
        matches!(refused, Err(Error::ReadCancelled)),
        "cancelling a cursor fetch from inside the observer, over the owner channel, yields \
         ReadCancelled: {refused:?}"
    );
    assert!(
        observer.reports_while_armed_count() >= 1,
        "the caller heard from the fetch over the channel, while armed for exactly this call, \
         before it cancelled it"
    );

    assert_eq!(
        owner_active_readers(&path, &runtime_root),
        held_by_the_open_cursor,
        "the cancelled fetch releases what it took and no more: the open cursor still holds \
         its slot"
    );

    // The cursor is still Open: a later fetch with no cancellation works.
    let page = cursor
        .fetch(None)
        .expect("the cursor survives the cancelled fetch and answers the next one");
    assert!(
        !page.rows.is_empty() || !page.has_more,
        "the cursor resumes cleanly after the cancelled fetch: {page:?}"
    );

    cursor.close().expect("close the cursor");
    assert_eq!(
        owner_active_readers(&path, &runtime_root),
        0,
        "closing the cursor returns the owner's reader count to zero"
    );
}

#[test]
fn a_report_from_a_finished_request_is_never_attributed_to_the_next_request_on_the_owner_channel() {
    let directory = tempfile::TempDir::new().expect("task-scoped owner-attribution directory");
    let path = directory.path().join("owner-attribution.db");
    let runtime_root = secure_runtime_root(&directory, "owner-attribution-runtime");

    let owner = live_owner(&path, runtime_root.clone());
    seed_progress_rows(&owner);

    let recorder = Arc::new(Recorder::default());
    let reader = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_progress(
            &path,
            roomy_reader_options(),
            Arc::clone(&recorder) as Arc<dyn ReadProgressObserver>,
        )
    })
    .expect("a live owner selects the owner route");
    recorder.clear();

    // A large first request, run to completion (never cancelled), whose
    // progress reports carry large counts.
    let big = reader
        .execute(
            "SELECT id FROM owner_progress_rows ORDER BY id",
            &HashMap::new(),
        )
        .expect("the large scan over the owner channel answers");
    assert_eq!(big.rows.len(), FIXTURE_ROWS as usize);
    let big_reporting_interval_crossings = recorder
        .of_phase(ReadPhase::Executing)
        .iter()
        .filter(|report| report.work >= 1_000)
        .count();
    assert!(
        big_reporting_interval_crossings > 0,
        "sanity: the large first request produced at least one report at or past the \
         reporting interval, so there is something for a stale report to leak"
    );

    // A tiny second request, its own progress reports isolated from the
    // first's: nothing this request's observer sees may carry the first
    // request's large counts.
    recorder.clear();
    let tiny = reader
        .execute(
            "SELECT id FROM owner_progress_rows WHERE id = 0",
            &HashMap::new(),
        )
        .expect("the tiny second scan over the owner channel answers");
    assert_eq!(tiny.rows.len(), 1);
    for report in recorder.of_phase(ReadPhase::Executing) {
        assert!(
            report.work < 1_000 && report.rows <= 1,
            "a report captured while running the tiny second request must never carry the \
             large first request's counts (a stale/late report attributed to the wrong \
             request): {report:?}"
        );
    }
}
