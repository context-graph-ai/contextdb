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

use contextdb_core::read_contract::{
    DatabaseIdentity, DeadlineClock, LocalUserIdentity, OwnerReadCancellation, OwnerReadLimits,
    OwnerReadStatus, OwnerServiceTimeouts, OwnerServingState, ReadClientTimeouts, ReadLimits,
    ReadRoute, WriterRunNumber,
};
use contextdb_core::{Error, Value};
#[cfg(unix)]
use contextdb_engine::local_transport::{
    ChannelPathFacts, LocalConfigurationSource, LocalHandshake, LocalRequest, LocalRequestEnvelope,
    LocalResponse, MonotonicDeadlineClock,
};
#[cfg(unix)]
use contextdb_engine::owner_read::{
    OwnerClient, OwnerReadScaffoldError, OwnerReadService, OwnerServicePublicationObserver,
    OwnerServiceSpec, ValidatedOwnerListener,
};
#[cfg(unix)]
use contextdb_engine::read_contract::decode_cursor_page;
use contextdb_engine::read_session::{
    ReadKernelCancellationEvent, ReadKernelSource, ReadKernelSourceEvent, ReadKernelTestObserver,
    ReadSessionOperation,
};
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, ReadPhase, ReadProgress, ReadProgressObserver,
    ReadSession, ReadSessionOptions,
};
use std::collections::HashMap;
#[cfg(unix)]
use std::future::Future;
#[cfg(unix)]
use std::num::NonZeroU64;
use std::path::Path;
#[cfg(unix)]
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, Condvar, Mutex};
#[cfg(unix)]
use std::task::{Context, Poll, Wake, Waker};

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

#[cfg(unix)]
fn seed_ordered_rows(database: &Database) {
    database
        .execute(
            "CREATE TABLE owner_publication_rows (id INTEGER PRIMARY KEY, marker TEXT)",
            &HashMap::new(),
        )
        .expect("create the owner publication fixture table");
    let selected = [200_i64, 2_500, 3_500];
    let mut next = 0_i64;
    while next < FIXTURE_ROWS {
        let tx = database
            .begin()
            .expect("begin an owner publication fixture batch");
        let last = (next + INSERT_BATCH).min(FIXTURE_ROWS);
        while next < last {
            let marker = if selected.contains(&next) {
                "needle"
            } else {
                "hay"
            };
            database
                .execute_in_tx(
                    tx,
                    "INSERT INTO owner_publication_rows (id, marker) VALUES ($id, $marker)",
                    &HashMap::from([
                        ("id".to_owned(), Value::Int64(next)),
                        ("marker".to_owned(), Value::Text(marker.to_owned())),
                    ]),
                )
                .unwrap_or_else(|error| panic!("insert owner publication row {next}: {error}"));
            next += 1;
        }
        database
            .commit(tx)
            .expect("commit an owner publication fixture batch");
    }
}

#[cfg(unix)]
fn one_page_id(page: &contextdb_core::read_contract::CursorPage) -> i64 {
    let [row] = page.rows.as_slice() else {
        panic!("the one-row cursor page returned {:?}", page.rows);
    };
    let [Value::Int64(id)] = row.as_slice() else {
        panic!("the cursor id projection returned {row:?}");
    };
    *id
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

#[cfg(unix)]
struct FutureSignal {
    ready: Mutex<bool>,
    changed: Condvar,
}

#[cfg(unix)]
impl Wake for FutureSignal {
    fn wake(self: Arc<Self>) {
        let mut ready = self.ready.lock().expect("future signal state");
        *ready = true;
        self.changed.notify_one();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let mut ready = self.ready.lock().expect("future signal state");
        *ready = true;
        self.changed.notify_one();
    }
}

#[cfg(unix)]
fn block_on<F: Future>(future: F) -> F::Output {
    let signal = Arc::new(FutureSignal {
        ready: Mutex::new(true),
        changed: Condvar::new(),
    });
    let waker = Waker::from(Arc::clone(&signal));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        if let Poll::Ready(output) = Pin::as_mut(&mut future).poll(&mut context) {
            return output;
        }
        let mut ready = signal.ready.lock().expect("future signal state");
        while !*ready {
            ready = signal.changed.wait(ready).expect("future signal wait");
        }
        *ready = false;
    }
}

#[cfg(unix)]
#[derive(Default)]
struct WaitableRecorder {
    reports: Mutex<Vec<ReadProgress>>,
    changed: Condvar,
}

#[cfg(unix)]
impl WaitableRecorder {
    fn wait_for_report(&self) {
        let mut reports = self.reports.lock().expect("progress report state");
        while reports.is_empty() {
            reports = self.changed.wait(reports).expect("progress report wait");
        }
    }
}

#[cfg(unix)]
impl ReadProgressObserver for WaitableRecorder {
    fn progress(&self, progress: ReadProgress) {
        self.reports
            .lock()
            .expect("progress report state")
            .push(progress);
        self.changed.notify_all();
    }
}

#[cfg(unix)]
#[derive(Default)]
struct PublicationGateState {
    before: Vec<u64>,
    cancellations: Vec<u64>,
    published: Vec<u64>,
}

/// Holds only the first completed fetch at the carrier boundary. The owner
/// reader remains free to receive the cancellation and releases this wait by
/// recording that receipt; no elapsed-time race decides the test.
#[cfg(unix)]
#[derive(Default)]
struct PublicationGate {
    state: Mutex<PublicationGateState>,
    changed: Condvar,
}

#[cfg(unix)]
impl PublicationGate {
    fn wait_for_boundary(&self) -> u64 {
        let mut state = self.state.lock().expect("publication gate state");
        while state.before.is_empty() {
            state = self.changed.wait(state).expect("publication boundary wait");
        }
        state.before[0]
    }

    fn assert_first_page_was_withdrawn(&self, request_ordinal: u64) {
        let state = self.state.lock().expect("publication gate state");
        assert!(state.cancellations.contains(&request_ordinal));
        assert!(
            !state.published.contains(&request_ordinal),
            "the cancelled cursor page must never cross the carrier publication boundary"
        );
    }
}

#[cfg(unix)]
impl OwnerServicePublicationObserver for PublicationGate {
    fn before_cursor_page_publication(&self, request_ordinal: u64) {
        let mut state = self.state.lock().expect("publication gate state");
        state.before.push(request_ordinal);
        self.changed.notify_all();
        if state.before.len() == 1 {
            while !state.cancellations.contains(&request_ordinal) {
                state = self
                    .changed
                    .wait(state)
                    .expect("publication cancellation receipt wait");
            }
        }
    }

    fn cancellation_received(&self, request_ordinal: u64) {
        self.state
            .lock()
            .expect("publication gate state")
            .cancellations
            .push(request_ordinal);
        self.changed.notify_all();
    }

    fn cursor_page_published(&self, request_ordinal: u64) {
        self.state
            .lock()
            .expect("publication gate state")
            .published
            .push(request_ordinal);
        self.changed.notify_all();
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
    fetch_gate: Arc<Barrier>,
}

impl ArmableCancelOnReport {
    fn new() -> Self {
        Self {
            armed: Mutex::new(None),
            reports_while_armed: Mutex::new(Vec::new()),
            fetch_gate: Arc::new(Barrier::new(2)),
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
            let mut reports = self
                .reports_while_armed
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if reports.is_empty() {
                self.fetch_gate.wait();
            }
            reports.push(progress);
            cancellation.cancel();
        }
    }
}

// Existing read cancellation contract: the owner remains inside the fetch when
// its caller cancels from a real progress frame, independent of thread scheduling.
struct FetchCancellationGate {
    progress: Arc<Barrier>,
    parked: AtomicBool,
    observed: AtomicBool,
}

impl ReadKernelTestObserver for FetchCancellationGate {
    fn before_source_touch(
        &self,
        event: ReadKernelSourceEvent,
        cancellation: OwnerReadCancellation,
    ) {
        if event.operation == ReadSessionOperation::CursorFetch
            && event.source == ReadKernelSource::TableRow
            && event.completed_items == 1_000
            && !self.parked.swap(true, Ordering::SeqCst)
        {
            assert_eq!(event.route, ReadRoute::Owner);
            let (sent, received) = std::sync::mpsc::channel();
            let _listener = cancellation.tell_on_cancel(move || {
                let _ = sent.send(());
            });
            self.progress.wait();
            received
                .recv()
                .expect("the observer's cancellation reaches the executing owner");
        }
    }

    fn cancellation_observed(
        &self,
        event: ReadKernelCancellationEvent,
        cancellation: OwnerReadCancellation,
    ) {
        assert_eq!(event.operation, ReadSessionOperation::CursorFetch);
        assert!(cancellation.is_cancelled());
        self.observed.store(true, Ordering::SeqCst);
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

    let observer = Arc::new(ArmableCancelOnReport::new());
    let gate = Arc::new(FetchCancellationGate {
        progress: observer.fetch_gate.clone(),
        parked: AtomicBool::new(false),
        observed: AtomicBool::new(false),
    });
    let mut options = roomy_owner_options(runtime_root.clone());
    options.test_kernel_observer = Some(gate.clone());
    let owner = Database::open_with_options(&path, options).expect("start the observed owner");
    seed_progress_rows(&owner);
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
    // fetch crosses a progress interval, then parks at the kernel seam until
    // cancellation arrives over the channel. Its observer waits for that park
    // before cancelling, so a completed page cannot win the scheduling race.
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
        gate.parked.load(Ordering::SeqCst),
        "the fetch reached the kernel gate"
    );
    assert!(
        gate.observed.load(Ordering::SeqCst),
        "the kernel observed cancellation before answering"
    );
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

#[cfg(unix)]
#[test]
fn cancellation_received_before_owner_cursor_page_publication_withdraws_and_replays_the_page() {
    let directory = tempfile::TempDir::new().expect("task-scoped publication directory");
    let path = directory.path().join("owner-publication.db");
    let runtime_root = secure_runtime_root(&directory, "owner-publication-runtime");
    let database = Arc::new(
        Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                owner_reads: OwnerReadConfig {
                    enabled: false,
                    ..OwnerReadConfig::default()
                },
                ..DatabaseOpenOptions::default()
            },
        )
        .expect("open the publication fixture without a second owner service"),
    );
    seed_ordered_rows(&database);

    let owner_user = LocalUserIdentity(nix::unistd::Uid::effective().as_raw() as u64);
    let handshake = LocalHandshake::current(
        DatabaseIdentity([0x61; 16]),
        WriterRunNumber([0x72; 16]),
        owner_user,
    );
    let listener = ValidatedOwnerListener::new(ChannelPathFacts {
        path: runtime_root.join("owner.sock"),
        runtime_directory: runtime_root.clone(),
        is_socket: true,
        owner: owner_user,
        mode: 0o700,
    });
    let clock: Arc<dyn DeadlineClock> = Arc::new(MonotonicDeadlineClock::new());
    let gate = Arc::new(PublicationGate::default());
    let gate_observer: Arc<dyn OwnerServicePublicationObserver> = gate.clone();
    let service = OwnerReadService::start(
        OwnerServiceSpec::new(
            Arc::clone(&database),
            listener,
            handshake.clone(),
            OwnerReadStatus {
                state: OwnerServingState::Serving,
                reason: None,
            },
            roomy_owner_options(runtime_root).owner_reads,
            LocalConfigurationSource::Override,
            Arc::clone(&clock),
        )
        .with_publication_observer_for_test(gate_observer),
    )
    .expect("start the observed production owner service");
    let timeouts = ReadClientTimeouts {
        connect_ms: 60_000,
        routing_retry_ms: 60_000,
        response_ms: 60_000,
    };
    let mut client = block_on(OwnerClient::connect(
        service.channel_path(),
        handshake,
        timeouts,
        Arc::clone(&clock),
    ))
    .expect("connect through the observed owner carrier");
    let limits = one_row_page_reader_options().limits;
    let baseline = service.resources();

    let opened = block_on(client.request(LocalRequestEnvelope {
        limits,
        request: LocalRequest::CursorOpen {
            statement: "SELECT id FROM owner_publication_rows WHERE marker = $marker".to_owned(),
            params: std::collections::BTreeMap::from([(
                "marker".to_owned(),
                Value::Text("needle".to_owned()),
            )]),
        },
    }))
    .expect("open the publication cursor");
    let [LocalResponse::CursorOpened { opened }] = opened.as_slice() else {
        panic!("cursor open must return one complete response: {opened:?}");
    };
    let cursor_id = opened.cursor_id;
    let first_page = decode_cursor_page(&opened.payload).expect("decode the first cursor page");
    assert_eq!(one_page_id(&first_page), 200);
    assert!(first_page.has_more);
    let after_open = service.resources();
    assert_eq!(after_open.cursor_count, 1);
    assert_eq!(after_open.active_slots, 1);
    assert_eq!(after_open.active_cancellations, 0);

    let progress = Arc::new(WaitableRecorder::default());
    let progress_observer: Arc<dyn ReadProgressObserver> = progress.clone();
    let cancellation = OwnerReadCancellation::new();
    let fetch_cancellation = cancellation.clone();
    let fetch = std::thread::spawn(move || {
        let result = block_on(client.request_watching(
            LocalRequestEnvelope {
                limits,
                request: LocalRequest::CursorFetch {
                    cursor_id,
                    rows: NonZeroU64::new(2),
                },
            },
            Some(&progress_observer),
            Some(&fetch_cancellation),
        ));
        (client, result)
    });

    let request_ordinal = gate.wait_for_boundary();
    progress.wait_for_report();
    cancellation.cancel();
    let (mut client, cancelled) = fetch.join().expect("cancelled publication fetch joins");
    assert!(
        matches!(
            cancelled,
            Err(OwnerReadScaffoldError::Database(Error::ReadCancelled))
        ),
        "owner publication cancellation keeps the route-neutral typed result: {cancelled:?}"
    );
    gate.assert_first_page_was_withdrawn(request_ordinal);

    // A status request can only run after the prior request's publication
    // guard and cancellation registration have both been released.
    block_on(client.request(LocalRequestEnvelope {
        limits,
        request: LocalRequest::OwnerStatus,
    }))
    .expect("the cancelled request releases the owner connection");
    let after_cancellation = service.resources();
    assert_eq!(after_cancellation.cursor_count, 1);
    assert_eq!(after_cancellation.active_slots, after_open.active_slots);
    assert_eq!(after_cancellation.active_cancellations, 0);
    assert!(after_cancellation.cursor_retained_bytes > 0);
    assert_eq!(
        i128::from(after_cancellation.accountant_used_bytes)
            - i128::from(after_open.accountant_used_bytes),
        i128::from(after_cancellation.cursor_retained_bytes)
            - i128::from(after_open.cursor_retained_bytes),
        "the pending page moves the database and owner cursor accounts by the same bytes"
    );

    let replayed = block_on(client.request(LocalRequestEnvelope {
        limits,
        request: LocalRequest::CursorFetch {
            cursor_id,
            rows: NonZeroU64::new(1),
        },
    }))
    .expect("the same cursor publishes its withdrawn page on the next fetch");
    let [LocalResponse::CursorPage { page: replayed }] = replayed.as_slice() else {
        panic!("cursor replay must return one complete page: {replayed:?}");
    };
    let replayed = decode_cursor_page(&replayed.payload).expect("decode the replayed page");
    assert_eq!(
        one_page_id(&replayed),
        2_500,
        "no withheld row may be skipped"
    );
    assert!(replayed.has_more);
    block_on(client.request(LocalRequestEnvelope {
        limits,
        request: LocalRequest::OwnerStatus,
    }))
    .expect("the replayed fetch releases its request registration");
    let after_partial_replay = service.resources();
    assert_eq!(after_partial_replay.cursor_count, 1);
    assert_eq!(after_partial_replay.active_slots, 1);
    assert_eq!(after_partial_replay.active_cancellations, 0);
    assert_eq!(
        after_cancellation
            .cursor_retained_bytes
            .checked_sub(after_partial_replay.cursor_retained_bytes),
        after_cancellation
            .accountant_used_bytes
            .checked_sub(after_partial_replay.accountant_used_bytes),
        "publishing part of a replay releases the same exact bytes from both accounts"
    );

    let next = block_on(client.request(LocalRequestEnvelope {
        limits,
        request: LocalRequest::CursorFetch {
            cursor_id,
            rows: None,
        },
    }))
    .expect("fetch after the replay resumes the bounded continuation");
    let [LocalResponse::CursorPage { page: next }] = next.as_slice() else {
        panic!("cursor continuation must return one complete page: {next:?}");
    };
    let next = decode_cursor_page(&next.payload).expect("decode the continued page");
    assert_eq!(
        one_page_id(&next),
        3_500,
        "the replay must not duplicate a row"
    );
    assert!(!next.has_more);
    block_on(client.request(LocalRequestEnvelope {
        limits,
        request: LocalRequest::OwnerStatus,
    }))
    .expect("the final replay fetch releases its request registration");
    assert_eq!(
        service.resources(),
        baseline,
        "publishing the final replay row returns the cursor and its remaining charge"
    );

    block_on(client.request(LocalRequestEnvelope {
        limits,
        request: LocalRequest::CursorClose { cursor_id },
    }))
    .expect("close the replayed cursor");
    assert_eq!(
        service.resources(),
        baseline,
        "closing the replayed cursor returns every slot and memory charge exactly once"
    );

    let direct_directory = tempfile::TempDir::new().expect("task-scoped direct parity directory");
    let direct_path = direct_directory.path().join("direct-publication.db");
    let direct_runtime = secure_runtime_root(&direct_directory, "direct-publication-runtime");
    let direct_database = Database::open_with_options(
        &direct_path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                enabled: false,
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the direct parity fixture");
    seed_ordered_rows(&direct_database);
    direct_database
        .close()
        .expect("release the direct parity fixture for file reading");
    let direct = ReadSession::with_runtime_directory_for_test(&direct_runtime, || {
        ReadSession::open_with_options(&direct_path, one_row_page_reader_options())
    })
    .expect("the idle parity fixture selects the direct route");
    assert_eq!(direct.route(), ReadRoute::File);
    let mut direct_cursor = direct
        .open_cursor(
            "SELECT id FROM owner_publication_rows WHERE marker = $marker",
            &HashMap::from([("marker".to_owned(), Value::Text("needle".to_owned()))]),
        )
        .expect("open the direct parity cursor");
    let direct_cancellation = OwnerReadCancellation::new();
    direct_cancellation.cancel();
    let direct_cancelled = direct_cursor.fetch_with_cancellation(None, &direct_cancellation);
    assert!(
        matches!(direct_cancelled, Err(Error::ReadCancelled)),
        "direct and owner fetch cancellation must retain the same typed result: \
         {direct_cancelled:?}"
    );
    let direct_replay = direct_cursor
        .fetch(None)
        .expect("the direct cursor also remains usable after typed cancellation");
    assert_eq!(one_page_id(&direct_replay), 2_500);
    direct_cursor
        .close()
        .expect("close the direct parity cursor");
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
