//! Shared fixture for the writer-acquisition journeys: a store with readers
//! parked at named points of their snapshot acquisition, and a writer opened
//! on its own thread whose admission is observed through the engine's own
//! event record, so a proof never sleeps to learn what the writer is doing.
#![allow(dead_code)]

use contextdb_core::Value;
use contextdb_core::read_contract::ReadRoute;
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    ReaderPausePoint, ReaderSourceStartPause, WriterAdmissionEvent, WriterAdmissionWatch,
    pause_readers_at_for_test, pause_readers_at_source_start_for_test,
    physical_admission_counts_for_test, watch_writer_admission_for_test,
};
use contextdb_engine::{Database, DatabaseOpenOptions, ReadSession};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Duration;

/// Failsafe only: a step that never happens fails as a message, not a hang.
/// No assertion here measures how long anything took.
pub const WATCHDOG: Duration = Duration::from_secs(60);

pub const SEEDED_ROWS: i64 = 3;

/// A store with committed content, closed, so the next opener claims it fresh.
pub fn seeded_store(directory: &Path, name: &str) -> PathBuf {
    let path = directory.join(name);
    let database = Database::open(&path).expect("claim a store to seed");
    database
        .execute(
            "CREATE TABLE held (id INTEGER PRIMARY KEY, body TEXT)",
            &HashMap::new(),
        )
        .expect("create the held table");
    for row in 0..SEEDED_ROWS {
        database
            .execute(
                "INSERT INTO held (id, body) VALUES ($id, $body)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(row)),
                    ("body".to_owned(), Value::Text("h".repeat(64 * 1024))),
                ]),
            )
            .expect("seed one row");
    }
    database.close().expect("release the seeded store");
    path
}

pub fn count_rows(session: &ReadSession) -> i64 {
    let answer = session
        .execute("SELECT COUNT(*) FROM held", &HashMap::new())
        .expect("a held-table count");
    match &answer.rows[0][0] {
        Value::Int64(count) => *count,
        other => panic!("a count is an integer, got {other:?}"),
    }
}

pub fn count_rows_in_writer(database: &Database) -> i64 {
    let answer = database
        .execute("SELECT COUNT(*) FROM held", &HashMap::new())
        .expect("a held-table count");
    match &answer.rows[0][0] {
        Value::Int64(count) => *count,
        other => panic!("a count is an integer, got {other:?}"),
    }
}

/// Where in its snapshot acquisition a reader is stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Inside admission, holding the data lock: the store's first read source
    /// has just started.
    SourceStart,
    /// Just before the committed image is decoded.
    BeforeDecode,
}

pub fn pause_at(path: &Path, phase: Phase) -> ReaderSourceStartPause {
    match phase {
        Phase::SourceStart => pause_readers_at_source_start_for_test(path),
        Phase::BeforeDecode => pause_readers_at_for_test(path, ReaderPausePoint::BeforeDecode),
    }
}

/// A real direct reader, stopped at `phase`, whose finished session is handed
/// back through `session` once it is let go.
pub struct ParkedReader {
    pause: std::sync::Arc<ReaderSourceStartPause>,
    ticket: u64,
    session: mpsc::Receiver<contextdb_core::Result<ReadSession>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl ParkedReader {
    pub fn let_go(&self) {
        self.pause.release(self.ticket);
    }

    /// The session this reader ends with. Blocks until the reader is let go
    /// and finishes; the watchdog only turns a hang into a message.
    pub fn session(mut self) -> ReadSession {
        self.let_go();
        let answer = self
            .session
            .recv_timeout(WATCHDOG)
            .expect("the released reader never finished its acquisition");
        if let Some(thread) = self.thread.take() {
            thread.join().expect("the reader thread finishes");
        }
        answer.expect("the reader that was in acquisition keeps its snapshot")
    }
}

impl ParkedReader {
    /// Like `session`, but hands back whatever the reader ended with,
    /// refusal included.
    pub fn outcome(mut self) -> contextdb_core::Result<ReadSession> {
        // A reader stopped at a pause point cannot answer until it is let go.
        self.let_go();
        let answer = self
            .session
            .recv_timeout(WATCHDOG)
            .expect("the released reader never finished its acquisition");
        if let Some(thread) = self.thread.take() {
            thread.join().expect("the reader thread finishes");
        }
        answer
    }
}

impl Drop for ParkedReader {
    fn drop(&mut self) {
        self.let_go();
    }
}

/// Park `count` readers of `path` at `phase`. All of them are stopped before
/// this returns, each by its own ticket.
pub fn park_readers(path: &Path, phase: Phase, count: usize) -> Vec<ParkedReader> {
    let pause = std::sync::Arc::new(pause_at(path, phase));
    let mut parked = Vec::new();
    for _ in 0..count {
        let (tell, heard) = mpsc::channel();
        let reader_path = path.to_path_buf();
        let thread = std::thread::spawn(move || {
            let _ = tell.send(ReadSession::open(&reader_path));
        });
        // One at a time: every earlier reader's ticket is already handed
        // out, so the ticket this wait returns is the reader just started.
        let ticket = pause
            .wait_for_paused_reader(WATCHDOG)
            .expect("a reader never reached the pause point");
        parked.push(ParkedReader {
            pause: std::sync::Arc::clone(&pause),
            ticket,
            session: heard,
            thread: Some(thread),
        });
    }
    parked
}

/// What a reader that arrived behind a claiming writer may end with: the
/// writer's answer as owner, or the typed transient answers of a writer that
/// has claimed the store but is not serving reads yet. Never `held_by_readers`
/// (it waited for no reader) and never a session served from the file past the
/// writer's claim.
pub fn assert_a_late_reader_was_routed_behind_the_writer(
    answer: contextdb_core::Result<ReadSession>,
    what: &str,
) {
    use contextdb_core::read_contract::ReadFailureKind;
    match answer {
        Ok(session) => assert_eq!(
            session.route(),
            ReadRoute::Owner,
            "{what}: a reader that arrived behind the writer is answered by the writer, not by the file"
        ),
        Err(contextdb_core::Error::ReadFailure(failure))
            if matches!(
                failure.kind(),
                ReadFailureKind::OwnerNotServing | ReadFailureKind::HeldByWriter
            ) => {}
        Err(other) => panic!(
            "{what}: a reader arriving behind a claiming writer is routed to the writer or \
             told the writer is not serving yet; got {other:?}"
        ),
    }
}

/// What a writable open on its own thread ended with.
pub struct WriterOpen {
    pub result: mpsc::Receiver<contextdb_core::Result<Database>>,
    thread: Option<std::thread::JoinHandle<()>>,
    thread_id: std::thread::ThreadId,
}

impl WriterOpen {
    /// The thread that runs the writable open: the engine's wait-helper
    /// events are recorded against it.
    pub fn thread_id(&self) -> std::thread::ThreadId {
        self.thread_id
    }

    pub fn finish(mut self) -> contextdb_core::Result<Database> {
        let answer = self
            .result
            .recv_timeout(WATCHDOG)
            .expect("the writable open never answered");
        if let Some(thread) = self.thread.take() {
            thread.join().expect("the writer thread finishes");
        }
        answer
    }
}

pub fn open_writer(path: &Path, options: DatabaseOpenOptions) -> WriterOpen {
    let path = path.to_path_buf();
    let (tell, heard) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        let _ = tell.send(Database::open_with_options(&path, options));
    });
    WriterOpen {
        result: heard,
        thread_id: thread.thread().id(),
        thread: Some(thread),
    }
}

pub fn next_event(watch: &WriterAdmissionWatch, what: &str) -> WriterAdmissionEvent {
    watch
        .next(WATCHDOG)
        .unwrap_or_else(|| panic!("the writable open recorded nothing {what}"))
}

pub fn watch(path: &Path) -> WriterAdmissionWatch {
    watch_writer_admission_for_test(path)
}

/// What the writer is expected to do about a reader stopped at the phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expected {
    /// The reader is inside the store's protected acquisition section: the
    /// writer waits, once, until the reader leaves it, then opens.
    WaitsForTheReader,
    /// The reader is outside the protected section: the writer opens without
    /// ever waiting.
    OpensAtOnce,
}

/// Physical admission counters as a value, so a journey reads a delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Counts {
    pub data_shared_grants: u64,
    pub gate_requests: u64,
    pub hold_shared_grants: u64,
    pub hold_range_refusals: u64,
    pub writer_hold_waits: u64,
}

pub fn counts(path: &Path) -> Counts {
    let observed = physical_admission_counts_for_test(path);
    Counts {
        data_shared_grants: observed.data_shared_grants,
        gate_requests: observed.gate_requests,
        hold_shared_grants: observed.hold_shared_grants,
        hold_range_refusals: observed.hold_range_refusals,
        writer_hold_waits: observed.writer_hold_waits,
    }
}

/// What every entrant that arrives after a writer's claim must leave
/// untouched: no gate request, no hold, no data lock, no refused range try.
pub fn assert_no_entrant_reached_the_protected_section(before: Counts, after: Counts) {
    assert_eq!(
        (
            after.gate_requests - before.gate_requests,
            after.hold_shared_grants - before.hold_shared_grants,
            after.hold_range_refusals - before.hold_range_refusals,
            after.data_shared_grants - before.data_shared_grants,
        ),
        (0, 0, 0, 0),
        "an entrant that arrives after the writer's claim stops at the claim: it makes \
         no gate request, takes no hold, and enters no data-lock section \
         (before {before:?}, after {after:?})"
    );
}

/// The shape of one writer-acquisition journey.
#[derive(Debug, Clone, Copy)]
pub struct Journey {
    pub phase: Phase,
    pub expected: Expected,
    /// A second reader arrives after the writer has claimed the store.
    pub late_reader: bool,
    /// The store has no companion file when its readers arrive.
    pub strip_companion: bool,
}

/// One writer opens while one reader is stopped at `phase`; a second reader
/// arrives after the writer has claimed the store. Afterwards the first
/// reader still holds the snapshot it began acquiring, the writer's later
/// commit is not in it, and the second reader was never refused, never took
/// the data lock past the writer, and was answered by the writer as owner.
pub fn assert_writer_opens_during(journey: Journey) {
    let Journey {
        phase,
        expected,
        late_reader,
        strip_companion,
    } = journey;
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "journey.db");
    if strip_companion {
        match std::fs::remove_file(contextdb_core::companion::store_companion_path(&path)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("remove the companion to make a companionless store: {error}"),
        }
    }
    let mut readers = park_readers(&path, phase, 1);
    let first = readers.remove(0);
    let watch = watch(&path);

    let writer = open_writer(&path, DatabaseOpenOptions::default());
    match expected {
        Expected::WaitsForTheReader => assert_eq!(
            next_event(&watch, "while a reader is inside its acquisition"),
            WriterAdmissionEvent::Waiting,
            "a writer that meets a reader still acquiring its snapshot waits for it; \
             it is not refused for the caller to retry"
        ),
        Expected::OpensAtOnce => assert_eq!(
            next_event(&watch, "while a reader is outside its protected section"),
            WriterAdmissionEvent::Acquired,
            "a reader that is not inside the protected acquisition section holds nothing \
             a writer must wait for"
        ),
    }

    // A reader that arrives now is behind the writer: it never takes the data
    // lock past it, so the count of shared grants does not move.
    let grants_before = physical_admission_counts_for_test(&path).data_shared_grants;
    let late = late_reader.then(|| {
        let late_path = path.clone();
        let (tell, heard) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let _ = tell.send(ReadSession::open(&late_path));
        });
        (heard, thread)
    });

    first.let_go();
    let first_session = first.session();
    if expected == Expected::WaitsForTheReader {
        assert_eq!(
            next_event(&watch, "after the reader let go"),
            WriterAdmissionEvent::Acquired,
            "the writer opens the moment the reader leaves its acquisition"
        );
    }
    let database = writer.finish().expect("the waiting writer opens");
    database
        .execute(
            "INSERT INTO held (id, body) VALUES (100, 'after')",
            &HashMap::new(),
        )
        .expect("the writer commits after it opened");

    assert_eq!(
        count_rows(&first_session),
        SEEDED_ROWS,
        "the reader that was acquiring when the writer arrived keeps the snapshot it began"
    );

    if let Some((late_answer, late_thread)) = late {
        let late_session = late_answer
            .recv_timeout(WATCHDOG)
            .expect("the late reader never finished");
        late_thread.join().expect("the late reader thread finishes");
        assert_a_late_reader_was_routed_behind_the_writer(late_session, "the late reader");
        assert_eq!(
            physical_admission_counts_for_test(&path).data_shared_grants,
            grants_before,
            "a reader arriving behind a waiting writer does not take the data lock before it"
        );
    }
    drop(first_session);
    database.close().expect("close the writer");
}

/// The engine's own deterministic clock, which a writer's wait can bind to
/// natively: advancing it wakes the parked wait, nothing polls.
pub use contextdb_engine::local_transport::ManualDeadlineClock as ManualClock;

/// The typed refusal a cancellable wait gets where the platform cannot give
/// the cancellation guarantee. One predicate, so the contract's spelling is
/// written in one place.
pub fn is_unsupported_cancellable_wait(error: &contextdb_core::Error) -> bool {
    matches!(error, contextdb_core::Error::ReaderWaitUnsupported)
}

/// Readers started after the writer's claim, each on its own thread.
pub struct Newcomers {
    answers: Vec<(
        mpsc::Receiver<contextdb_core::Result<ReadSession>>,
        std::thread::JoinHandle<()>,
    )>,
}

pub fn spawn_newcomers(path: &Path, count: usize) -> Newcomers {
    let answers = (0..count)
        .map(|_| {
            let reader_path = path.to_path_buf();
            let (tell, heard) = mpsc::channel();
            let thread = std::thread::spawn(move || {
                let _ = tell.send(ReadSession::open(&reader_path));
            });
            (heard, thread)
        })
        .collect();
    Newcomers { answers }
}

impl Newcomers {
    /// Every newcomer finishes, none was refused, and each was answered by
    /// the writer that claimed the store before it arrived.
    pub fn all_answered_by_the_writer(self) {
        self.answers
            .into_iter()
            .map(|(heard, thread)| {
                let answer = heard
                    .recv_timeout(WATCHDOG)
                    .expect("a newcomer never finished");
                thread.join().expect("the newcomer thread finishes");
                assert_a_late_reader_was_routed_behind_the_writer(answer, "a newcomer");
            })
            .collect()
    }
}

/// A writer queued behind one reader parked inside the protected section,
/// with the admission counters read at the moment the writer is known to be
/// waiting. Everything that happens after `before` happens after the claim.
pub struct QueuedWriter {
    pub path: PathBuf,
    pub directory: tempfile::TempDir,
    pub reader: ParkedReader,
    pub writer: WriterOpen,
    pub watch: WriterAdmissionWatch,
    pub before: Counts,
}

pub fn queue_a_writer_behind_a_reader(name: &str) -> QueuedWriter {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), name);
    let mut readers = park_readers(&path, Phase::SourceStart, 1);
    let reader = readers.remove(0);
    let watch = watch(&path);
    let writer = open_writer(&path, DatabaseOpenOptions::default());
    assert_eq!(
        next_event(&watch, "while a reader is inside its acquisition"),
        WriterAdmissionEvent::Waiting,
        "the writer must be waiting before the journey's newcomers arrive"
    );
    let before = counts(&path);
    QueuedWriter {
        path,
        directory,
        reader,
        writer,
        watch,
        before,
    }
}

/// The writer a queued journey ended with, still open and still serving.
pub struct OpenedWriter {
    pub database: Database,
    pub path: PathBuf,
    _directory: tempfile::TempDir,
}

impl OpenedWriter {
    pub fn close(self) {
        self.database.close().expect("close the writer");
    }
}

impl QueuedWriter {
    /// The parked reader leaves and the writer opens. The writer is handed
    /// back open: whoever arrived behind it is answered by it.
    pub fn release_and_open(self) -> OpenedWriter {
        let QueuedWriter {
            path,
            directory,
            reader,
            writer,
            watch,
            before: _,
        } = self;
        reader.let_go();
        drop(reader.session());
        assert_eq!(
            next_event(&watch, "after the reader left"),
            WriterAdmissionEvent::Acquired
        );
        let database = writer.finish().expect("the queued writer opens");
        OpenedWriter {
            database,
            path,
            _directory: directory,
        }
    }
}

/// The typed I/O refusal a wait gets when its helper dies without finishing.
pub fn is_typed_io_refusal(error: &contextdb_core::Error) -> bool {
    matches!(error, contextdb_core::Error::ReaderWaitHelperFailed(_))
}

/// The typed refusal for a store whose helper's death could not be confirmed.
pub fn is_quarantine_refusal(error: &contextdb_core::Error) -> bool {
    matches!(error, contextdb_core::Error::ReaderWaitQuarantined { .. })
}
