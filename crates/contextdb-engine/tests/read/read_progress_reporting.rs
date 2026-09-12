//! A read that takes a while says so while it is still running.
//!
//! A caller waiting on one blocking call cannot tell a read that is working
//! from a read that is wedged, and the difference matters most on exactly the
//! reads that take long enough to worry about: a large store being opened, a
//! scan walking many rows to find few. So a caller may hand a session an
//! observer and be told what the read has done so far -- store bytes loaded
//! while the store is being opened, then rows, bytes, items and milliseconds
//! while the answer is being assembled.
//!
//! What the observer is told is what had happened at the moment it was told:
//! the report is made inline, on the thread running the read, and nothing
//! arrives after the operation has returned.

use contextdb_core::read_contract::{OwnerReadCancellation, ReadLimits};
use contextdb_core::{Error, Result, Value};
use contextdb_engine::{
    Database, ReadPhase, ReadProgress, ReadProgressObserver, ReadSession, ReadSessionOptions,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;

/// Rows enough that a scan of them passes the reporting interval more than
/// once, and payload enough that loading them passes the hydration interval.
const FIXTURE_ROWS: i64 = 4_000;
const PAYLOAD_BYTES: usize = 512;
const INSERT_BATCH: i64 = 500;

/// The rows a filtered scan is looking for, far enough apart that reaching
/// each one is its own long walk. There are three because a cursor page of
/// one row reads one row ahead to know whether more follow, so two needles
/// would both be found before the first page was even published.
const NEEDLES: [i64; 3] = [1_000, 2_500, 3_900];

/// Everything one observer was told, and where it was told it.
#[derive(Default)]
struct Recorder {
    reports: Mutex<Vec<(ReadProgress, ThreadId)>>,
}

impl ReadProgressObserver for Recorder {
    fn progress(&self, progress: ReadProgress) {
        self.reports
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push((progress, std::thread::current().id()));
    }
}

impl Recorder {
    fn reports(&self) -> Vec<(ReadProgress, ThreadId)> {
        self.reports
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn of_phase(&self, phase: ReadPhase) -> Vec<ReadProgress> {
        self.reports()
            .into_iter()
            .filter(|(progress, _)| progress.phase == phase)
            .map(|(progress, _)| progress)
            .collect()
    }

    fn clear(&self) {
        self.reports
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }
}

/// An observer that interrupts the read reporting to it the first time it
/// hears from it.
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

fn payload(row: i64) -> String {
    let mut text = format!("row-{row}-");
    while text.len() < PAYLOAD_BYTES {
        text.push('p');
    }
    text
}

/// A store big enough that reading it is not instant: many rows, each of them
/// carrying real bytes, with two findable ones far apart.
fn seed_store(path: &Path) {
    let database = Database::open(path).expect("open the progress fixture writer");
    database
        .execute(
            "CREATE TABLE progress_rows (id INTEGER PRIMARY KEY, marker TEXT, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create the progress fixture table");
    let mut next = 0;
    while next < FIXTURE_ROWS {
        let tx = database.begin().expect("begin a progress fixture batch");
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
                    "INSERT INTO progress_rows (id, marker, payload) VALUES ($id, $marker, $payload)",
                    &HashMap::from([
                        ("id".to_owned(), Value::Int64(next)),
                        ("marker".to_owned(), Value::Text(marker.to_owned())),
                        ("payload".to_owned(), Value::Text(payload(next))),
                    ]),
                )
                .unwrap_or_else(|error| panic!("insert progress fixture row {next}: {error}"));
            next += 1;
        }
        database
            .commit(tx)
            .expect("commit a progress fixture batch");
    }
    database.close().expect("the fixture writer closes cleanly");
}

fn find_the_needles(session: &ReadSession) -> Result<contextdb_engine::QueryResult> {
    session.execute(
        "SELECT id FROM progress_rows WHERE marker = $marker",
        &HashMap::from([("marker".to_owned(), Value::Text("needle".to_owned()))]),
    )
}

/// Reports of one phase, in the order they were made, must each describe more
/// work than the one before it and be at least one interval apart: a report is
/// worth making when there is something new to say.
fn assert_reporting_cadence(reports: &[ReadProgress], counter: impl Fn(&ReadProgress) -> u64) {
    let mut previous = 0u64;
    for report in reports {
        let value = counter(report);
        assert!(
            value >= previous + 1_000,
            "reports are at least an interval apart: {previous} then {value}"
        );
        previous = value;
    }
}

#[test]
fn a_scan_reports_the_answer_it_has_assembled_while_it_is_still_assembling_it() {
    let directory = tempfile::TempDir::new().expect("task-scoped progress directory");
    let path = directory.path().join("assembling.db");
    seed_store(&path);

    let recorder = Arc::new(Recorder::default());
    let session = ReadSession::open_with_progress(
        &path,
        ReadSessionOptions::default(),
        Arc::clone(&recorder) as Arc<dyn ReadProgressObserver>,
    )
    .expect("open the idle store with progress reporting");
    recorder.clear();

    let answered = find_the_needles(&session).expect("the scan answers");
    assert_eq!(
        answered.rows.len(),
        NEEDLES.len(),
        "the fixture's findable rows are all found"
    );

    let executing = recorder.of_phase(ReadPhase::Executing);
    assert!(
        !executing.is_empty(),
        "a scan of {FIXTURE_ROWS} rows reports its progress at least once"
    );
    let this_thread = std::thread::current().id();
    for (progress, thread) in recorder.reports() {
        assert_eq!(
            thread, this_thread,
            "the read reports on the thread running it, not from somewhere else"
        );
        assert_eq!(progress.phase, ReadPhase::Executing);
        assert_eq!(
            progress.loaded_bytes, 0,
            "an executing read is loading no store bytes"
        );
        assert!(
            progress.work <= ReadLimits::default().work,
            "reported work is this read's progress against the ceiling that would stop it"
        );
        assert!(
            progress.rows <= NEEDLES.len() as u64,
            "a read never reports more rows than the answer holds: {progress:?}"
        );
    }
    assert_reporting_cadence(&executing, |progress| progress.work);
    let last = executing.last().expect("at least one report");
    assert!(
        last.work > 1_000,
        "the scan examined far more than it returned: {last:?}"
    );

    // Nothing arrives once the operation has returned: a later read that was
    // never given this observer tells it nothing, so what it heard is exactly
    // what the read it was watching did.
    let after_the_read = recorder.reports().len();
    let silent = ReadSession::open(&path).expect("open the same store without an observer");
    find_the_needles(&silent).expect("the unobserved scan answers");
    assert_eq!(
        recorder.reports().len(),
        after_the_read,
        "no report arrives after the operation that was being reported returned"
    );
}

#[test]
fn opening_a_large_store_reports_the_bytes_it_has_loaded() {
    let directory = tempfile::TempDir::new().expect("task-scoped progress directory");
    let path = directory.path().join("hydrating.db");
    seed_store(&path);

    let recorder = Arc::new(Recorder::default());
    let this_thread = std::thread::current().id();
    let session = ReadSession::open_with_progress(
        &path,
        ReadSessionOptions::default(),
        Arc::clone(&recorder) as Arc<dyn ReadProgressObserver>,
    )
    .expect("open the idle store with progress reporting");

    let hydrating = recorder.of_phase(ReadPhase::Hydrating);
    assert!(
        !hydrating.is_empty(),
        "opening a store of more than a megabyte reports the load at least once"
    );
    for (progress, thread) in recorder.reports() {
        assert_eq!(
            progress.phase,
            ReadPhase::Hydrating,
            "opening runs no statement"
        );
        assert_eq!(
            thread, this_thread,
            "hydration reports on the opening thread"
        );
        assert_eq!(progress.rows, 0, "a hydrating read has assembled no rows");
        assert_eq!(
            progress.bytes, 0,
            "a hydrating read has assembled no answer"
        );
        assert_eq!(
            progress.total_bytes, None,
            "a total that would have to be estimated is reported as absent, not guessed"
        );
    }
    assert_reporting_cadence(&hydrating, |progress| progress.loaded_bytes);
    let last = hydrating.last().expect("at least one report");
    assert!(
        last.loaded_bytes >= 1024 * 1024,
        "the reported load covers the store that was actually read: {last:?}"
    );

    drop(session);
}

#[test]
fn every_read_a_session_runs_is_reported_not_only_its_first() {
    let directory = tempfile::TempDir::new().expect("task-scoped progress directory");
    let path = directory.path().join("every-read.db");
    seed_store(&path);

    let recorder = Arc::new(Recorder::default());
    // One row per page, so a later needle is only reached by fetching again --
    // and that fetch does its own long walk to reach it.
    let options = ReadSessionOptions {
        limits: ReadLimits {
            cursor_page_rows: 1,
            ..ReadLimits::default()
        },
        ..ReadSessionOptions::default()
    };
    let session = ReadSession::open_with_progress(
        &path,
        options,
        Arc::clone(&recorder) as Arc<dyn ReadProgressObserver>,
    )
    .expect("open the idle store with progress reporting");

    recorder.clear();
    find_the_needles(&session).expect("the scan answers");
    assert!(
        !recorder.of_phase(ReadPhase::Executing).is_empty(),
        "a statement reports its progress"
    );

    recorder.clear();
    let mut cursor = session
        .open_cursor(
            "SELECT id FROM progress_rows WHERE marker = $marker",
            &HashMap::from([("marker".to_owned(), Value::Text("needle".to_owned()))]),
        )
        .expect("open a cursor over the same scan");
    assert!(
        !recorder.of_phase(ReadPhase::Executing).is_empty(),
        "opening a cursor reports its progress too"
    );

    recorder.clear();
    let page = cursor.fetch(None).expect("fetch the cursor's next page");
    assert!(!page.rows.is_empty(), "a later needle is on a later page");
    assert!(
        !recorder.of_phase(ReadPhase::Executing).is_empty(),
        "fetching a page reports its progress too, not only the open that preceded it"
    );

    cursor.close().expect("close the cursor");
}

#[test]
fn a_caller_told_about_a_running_read_can_still_interrupt_it() {
    let directory = tempfile::TempDir::new().expect("task-scoped progress directory");
    let path = directory.path().join("interrupted.db");
    seed_store(&path);

    // The report is made from inside the running read, so a caller that acts
    // on it -- here, by withdrawing the read -- is acting on a read that has
    // not returned yet, and the read notices.
    let cancellation = OwnerReadCancellation::new();
    let observer = Arc::new(CancelOnFirstReport {
        cancellation: cancellation.clone(),
        reports: Mutex::new(Vec::new()),
    });
    let session = ReadSession::open_with_progress(
        &path,
        ReadSessionOptions::default(),
        Arc::clone(&observer) as Arc<dyn ReadProgressObserver>,
    )
    .expect("open the idle store with progress reporting");

    let refused = session.execute_with_cancellation(
        "SELECT id FROM progress_rows WHERE marker = $marker",
        &HashMap::from([("marker".to_owned(), Value::Text("needle".to_owned()))]),
        &cancellation,
    );
    assert!(
        matches!(refused, Err(Error::ReadCancelled)),
        "the read the caller withdrew reports that it was withdrawn: {refused:?}"
    );
    let heard = observer
        .reports
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .len();
    assert!(
        heard >= 1,
        "the caller heard from the read before it interrupted it"
    );
}
