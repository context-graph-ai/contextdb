#![cfg(all(unix, feature = "test-seams"))]
//! A caller that ends a coordination wait ends the whole of it, including
//! whatever that wait started on its behalf.
//!
//! Both of the store's coordination waits sleep in the kernel on an advisory
//! byte, which is exactly right: the wake IS the event, nothing polls, and no
//! interval decides how quickly a caller learns. But a caller can also stop
//! waiting — its declared budget runs out, or it cancels — and at that moment
//! the wait owes it two things that are easy to conflate. It owes a prompt
//! answer, which it gives. It also owes that nothing it started keeps running
//! and keeps acting on the store afterwards, which is the part this file is
//! about.
//!
//! Why that second one is a caller-visible promise and not an internal tidiness
//! matter:
//!
//! A helper left blocked on the EXCLUSIVE byte is not idle. It is queued for
//! the very hold that says "a writer is taking this store", and when the real
//! reader finally lets go, that abandoned helper takes it — on behalf of a
//! caller that stopped caring long ago. Anyone asking about the store in that
//! moment sees a holder that is not there. A phantom holder is worse than a
//! slow answer, because it is indistinguishable from a real one.
//!
//! And a supervisor retries. Each expired or cancelled wait that leaves a
//! blocked helper behind against a holder that never lets go adds one more
//! permanently parked thread to the process. A long-lived writer that polls a
//! stuck store accumulates them without bound, and nothing in the answers it
//! receives ever hints at it.
//!
//! There is also a contract about HOW a wait expires. Every time value in this
//! work is declared configuration proven through the manual clock, and no
//! acceptance test asserts a real wall-clock duration. A wait that expires by
//! reading real time cannot be proven at all: a proof of it is a sleep, and a
//! sleep is the thing this whole design refuses.
//!
//! What is pinned here is observable effects and bounds, never thread
//! internals. The blocking acquisition these waits use is not itself
//! cancellable, so the implementer may keep the promise by bounding and
//! reusing helpers, or by making an abandoned one provably inert — either
//! satisfies every assertion below.
//!
//! What "nothing left running" is checked BY is worth writing down, because
//! the obvious instrument turned out to be the wrong one. Counting helper
//! threads only measures a design that has them; this wait has none, so a
//! count of them can only ever read zero and would pass whatever the code did.
//! The observable that cannot lie is the store itself: once the real holder
//! lets go, a fresh wait is answered `Released` at once. Anything this wait had
//! left queued for the exclusive hold would have taken it in that moment, and
//! the fresh wait would block on it instead.
//!

use contextdb_core::Value;
use contextdb_core::read_contract::{
    DeadlineClock, OwnerReadCancellation, OwnerReadLimits, OwnerServiceTimeouts, OwnerServingState,
    ReadClientTimeouts, ReadFailureKind, ReadLimits, ReadRoute,
};
use contextdb_core::{Error, Result};
use contextdb_engine::local_transport::ManualDeadlineClock;
use contextdb_engine::persistence::{ReaderReleaseWait, wait_for_reader_release};
use contextdb_engine::read_session::{ReadSessionEvent, ReadSessionTestObserver};
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, OwnerRequestHandler, ReadPhase, ReadProgress,
    ReadProgressObserver, ReadSession, ReadSessionOptions,
};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::Duration;

/// Failsafe only. Nothing here asserts how long anything took; this turns a
/// wait that never answers into a readable failure instead of a hang.
const WATCHDOG: Duration = Duration::from_secs(30);
/// How long an answer is observed NOT to have arrived. Not a latency claim: it
/// is how a journey establishes that a wait is genuinely still parked.
const PARKED: Duration = Duration::from_secs(2);
/// A declared budget far longer than this journey could ever really wait, so
/// that real time reaching it is not what ends the wait. Only the manual clock
/// can.
const HOUR_LONG_RETRY_MS: u64 = 60 * 60 * 1000;
/// How many times a supervisor's wait is repeated against a holder that never
/// lets go.
const REPEATED_WAITS: usize = 20;
const WIDE_ROWS: usize = 8;
const WIDE_ROW_BYTES: usize = 512 * 1024;

struct EchoHandler;

impl OwnerRequestHandler for EchoHandler {
    fn handle(
        &self,
        _namespace: &str,
        request: &[u8],
        _cancellation: &OwnerReadCancellation,
    ) -> Result<Vec<u8>> {
        Ok(request.to_vec())
    }
}

fn secure_runtime_root(directory: &Path, name: &str) -> PathBuf {
    let root = directory.join(name);
    std::fs::create_dir(&root).expect("create the task-scoped runtime root");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure the task-scoped runtime root");
    std::fs::canonicalize(&root).expect("resolve the root the way both sides will")
}

fn serving_writer_options(runtime_dir: PathBuf) -> DatabaseOpenOptions {
    DatabaseOpenOptions {
        owner_reads: OwnerReadConfig {
            limits: OwnerReadLimits::default(),
            timeouts: OwnerServiceTimeouts::default(),
            runtime_dir: Some(runtime_dir),
            handler: Some(Arc::new(EchoHandler)),
            ..OwnerReadConfig::default()
        },
        ..DatabaseOpenOptions::default()
    }
}

fn seeded_store(directory: &Path, name: &str) -> PathBuf {
    let path = directory.join(name);
    let database = Database::open(&path).expect("claim a store to seed");
    database
        .execute(
            "CREATE TABLE held (id INTEGER PRIMARY KEY, body TEXT)",
            &HashMap::new(),
        )
        .expect("create the held table");
    for row in 0..WIDE_ROWS {
        database
            .execute(
                "INSERT INTO held (id, body) VALUES ($id, $body)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(row as i64)),
                    ("body".to_owned(), Value::Text("h".repeat(WIDE_ROW_BYTES))),
                ]),
            )
            .expect("seed one wide row");
    }
    database.close().expect("release the seeded store");
    path
}

// ---------------------------------------------------------------------------
// A writer parked inside its published claim window, so a caller's claim wait
// has something real to expire against.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct WriterParkedInsideItsClaim {
    reached: (Mutex<bool>, Condvar),
    release: (Mutex<bool>, Condvar),
    parked: AtomicBool,
}

impl ReadSessionTestObserver for WriterParkedInsideItsClaim {
    fn observe_event(&self, event: ReadSessionEvent) {
        if event != ReadSessionEvent::PersistenceOpened || self.parked.swap(true, Ordering::SeqCst)
        {
            return;
        }
        {
            let (flag, signal) = &self.reached;
            *flag.lock().expect("claim state") = true;
            signal.notify_all();
        }
        let (flag, signal) = &self.release;
        let mut released = flag.lock().expect("claim state");
        while !*released {
            released = signal.wait(released).expect("claim condvar");
        }
    }
}

impl WriterParkedInsideItsClaim {
    fn wait_until_claimed(&self) {
        let (flag, signal) = &self.reached;
        let mut reached = flag.lock().expect("claim state");
        while !*reached {
            let (next, timed_out) = signal
                .wait_timeout(reached, WATCHDOG)
                .expect("claim condvar");
            reached = next;
            assert!(
                !timed_out.timed_out() || *reached,
                "the writer never reached the point where it holds the store",
            );
        }
    }

    fn let_go(&self) {
        let (flag, signal) = &self.release;
        *flag.lock().expect("claim state") = true;
        signal.notify_all();
    }
}

struct ParkedWriter {
    gate: Arc<WriterParkedInsideItsClaim>,
    thread: Option<std::thread::JoinHandle<std::result::Result<OwnerServingState, String>>>,
}

impl ParkedWriter {
    fn finish(mut self) -> OwnerServingState {
        self.gate.let_go();
        self.thread
            .take()
            .expect("the parked writer is finished exactly once")
            .join()
            .expect("join the parked writer")
            .expect("the parked writer opens the store")
    }
}

impl Drop for ParkedWriter {
    fn drop(&mut self) {
        self.gate.let_go();
    }
}

fn park_a_writer_inside_its_claim(path: &Path, runtime_root: &Path) -> ParkedWriter {
    let gate = Arc::new(WriterParkedInsideItsClaim::default());
    let observer: Arc<dyn ReadSessionTestObserver> = gate.clone();
    let mut options = serving_writer_options(runtime_root.to_path_buf());
    options.test_observer = Some(observer);
    let path = path.to_path_buf();
    let thread = std::thread::spawn(move || match Database::open_with_options(&path, options) {
        Ok(database) => {
            let state = database.owner_read_status().state;
            database.close().expect("close the parked writer");
            Ok(state)
        }
        Err(error) => Err(error.to_string()),
    });
    gate.wait_until_claimed();
    ParkedWriter {
        gate,
        thread: Some(thread),
    }
}

// ---------------------------------------------------------------------------
// A real direct reader holding the store, so a reader-release wait has
// something real to be cancelled against.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct ParkingReader {
    entered: (Mutex<bool>, Condvar),
    release: (Mutex<bool>, Condvar),
    parked: AtomicBool,
}

impl ParkingReader {
    fn wait_until_holding(&self) {
        let (flag, signal) = &self.entered;
        let mut entered = flag.lock().expect("parked reader state");
        while !*entered {
            let (next, timed_out) = signal
                .wait_timeout(entered, WATCHDOG)
                .expect("parked reader condvar");
            entered = next;
            assert!(
                !timed_out.timed_out() || *entered,
                "the reader never reported loading the store, so nothing ever held it"
            );
        }
    }

    fn let_go(&self) {
        let (flag, signal) = &self.release;
        *flag.lock().expect("parked reader state") = true;
        signal.notify_all();
    }
}

impl ReadProgressObserver for ParkingReader {
    fn progress(&self, progress: ReadProgress) {
        if progress.phase != ReadPhase::Hydrating || self.parked.swap(true, Ordering::SeqCst) {
            return;
        }
        {
            let (flag, signal) = &self.entered;
            *flag.lock().expect("parked reader state") = true;
            signal.notify_all();
        }
        let (flag, signal) = &self.release;
        let mut released = flag.lock().expect("parked reader state");
        while !*released {
            released = signal.wait(released).expect("parked reader condvar");
        }
    }
}

/// Ask the released store whether anything is still holding it, WITHOUT
/// risking the whole journey on the answer.
///
/// If something a cancelled wait left behind took the exclusive hold, this
/// wait blocks on it forever. Asking on another thread turns that into the
/// failure message the journey is about, instead of a test binary that hangs
/// and says nothing.
fn released_store_answer(path: &Path, what: &str) -> ReaderReleaseWait {
    let (tell, heard) = mpsc::channel();
    let path = path.to_path_buf();
    std::thread::spawn(move || {
        let stop = OwnerReadCancellation::new();
        let _ = tell.send(wait_for_reader_release(&path, None, &stop));
    });
    heard.recv_timeout(WATCHDOG).unwrap_or_else(|error| {
        panic!(
            "{what}: the released store never answered a fresh wait ({error}). Something an \
             earlier wait left behind is holding the exclusive hold this one is queued for.",
        )
    })
}

fn park_a_reader(path: &Path, runtime: &Path) -> (Arc<ParkingReader>, std::thread::JoinHandle<()>) {
    let parking = Arc::new(ParkingReader::default());
    let observer: Arc<dyn ReadProgressObserver> = parking.clone();
    let path = path.to_path_buf();
    let runtime = runtime.to_path_buf();
    let holding = std::thread::spawn(move || {
        let session = ReadSession::open_with_progress_in_runtime_dir(
            &path,
            ReadSessionOptions::default(),
            Some(runtime),
            observer,
        )
        .expect("an idle store is readable");
        assert_eq!(session.route(), ReadRoute::File);
    });
    parking.wait_until_holding();
    (parking, holding)
}

/// A claim window expires on the caller's declared budget, and that budget is
/// a declared time value like every other one here: it is proven by moving the
/// clock, never by waiting for real time to pass.
#[test]
fn a_claim_window_expires_on_the_manual_clock_and_not_on_real_time() {
    let directory = tempfile::TempDir::new().expect("task-scoped coordination directory");
    let runtime_root = secure_runtime_root(directory.path(), "manual-clock-runtime");
    let path = seeded_store(directory.path(), "manual-clock.db");
    let writer = park_a_writer_inside_its_claim(&path, &runtime_root);

    let clock = ManualDeadlineClock::at(0);
    let asking_clock: Arc<dyn DeadlineClock> = Arc::new(clock.clone());
    let (tell, heard) = mpsc::channel();
    let ask_path = path.clone();
    let ask_runtime = runtime_root.clone();
    let asking = std::thread::spawn(move || {
        let answer: Result<ReadSession> =
            ReadSession::with_runtime_directory_for_test(&ask_runtime, || {
                ReadSession::open_owner_only_with_clock_for_test(
                    &ask_path,
                    ReadSessionOptions {
                        limits: ReadLimits::default(),
                        timeouts: ReadClientTimeouts {
                            routing_retry_ms: HOUR_LONG_RETRY_MS,
                            ..ReadClientTimeouts::default()
                        },
                        ..ReadSessionOptions::default()
                    },
                    asking_clock,
                )
            });
        let _ = tell.send(match answer {
            Ok(session) => format!("opened on the {:?} route", session.route()),
            Err(Error::ReadFailure(failure)) => format!("{:?}", failure.kind()),
            Err(other) => format!("unexpected: {other}"),
        });
    });

    // Real time is not what this wait is spending. The declared budget is an
    // hour, and this journey is not going to wait one.
    assert!(
        heard.recv_timeout(PARKED).is_err(),
        "the caller declared an hour-long budget, so nothing should have ended its wait yet",
    );

    // Moving the caller's own clock past its own budget is what ends it.
    clock.advance_to(HOUR_LONG_RETRY_MS + 1);
    let answer = heard.recv_timeout(WATCHDOG).unwrap_or_else(|error| {
        panic!(
            "advancing the manual clock past the caller's declared budget did not end its wait \
             ({error}). A wait that expires on real time cannot be proven without sleeping for \
             it, and every declared time value in this work is proven on the manual clock."
        )
    });
    asking.join().expect("join the asking caller");

    assert_ne!(
        answer,
        format!("{:?}", ReadFailureKind::OwnerNotRunning),
        "a writer was holding this store the whole time, so the expiry answer is a not-serving \
         one and never an absent one",
    );

    assert_eq!(writer.finish(), OwnerServingState::Serving);
}

/// A cancelled wait takes what it started with it. While the reader is STILL
/// holding, nothing of that wait is left running — which is the only way to
/// know that nothing of it can take the hold later.
#[test]
fn a_cancelled_reader_wait_leaves_nothing_running_behind_it() {
    let directory = tempfile::TempDir::new().expect("task-scoped coordination directory");
    let runtime_root = secure_runtime_root(directory.path(), "cancelled-runtime");
    let path = seeded_store(directory.path(), "cancelled.db");
    let (parking, holding) = park_a_reader(&path, &runtime_root);

    let stop = OwnerReadCancellation::new();
    let (tell, heard) = mpsc::channel();
    let wait_path = path.clone();
    let waiting_stop = stop.clone();
    std::thread::spawn(move || {
        let _ = tell.send(wait_for_reader_release(&wait_path, None, &waiting_stop));
    });
    assert!(
        heard.recv_timeout(PARKED).is_err(),
        "a reader is holding the store, so the wait is parked",
    );

    stop.cancel();
    let answer = heard
        .recv_timeout(WATCHDOG)
        .expect("a cancelled wait answers its caller promptly");
    assert!(
        matches!(answer, ReaderReleaseWait::Stopped),
        "a caller that asked to stop is told it stopped",
    );

    parking.let_go();
    holding.join().expect("the reader finishes its read");

    // The moment the real holder lets go is when an abandoned acquisition
    // would take the exclusive hold, on behalf of a caller answered long ago.
    // Asking again right here is what would meet it: a fresh wait would block
    // on that phantom instead of being answered.
    let after = released_store_answer(&path, "after one cancelled wait");
    assert!(
        matches!(after, ReaderReleaseWait::Released),
        "once the real reader has let go the store is free, with no phantom holder in the way",
    );
}

/// A supervisor retries. Twenty cancelled waits against a holder that never
/// lets go must not leave twenty parked threads behind.
#[test]
fn repeated_cancelled_waits_against_a_stuck_holder_do_not_pile_up() {
    let directory = tempfile::TempDir::new().expect("task-scoped coordination directory");
    let runtime_root = secure_runtime_root(directory.path(), "repeated-runtime");
    let path = seeded_store(directory.path(), "repeated.db");
    let (parking, holding) = park_a_reader(&path, &runtime_root);

    for attempt in 0..REPEATED_WAITS {
        let stop = OwnerReadCancellation::new();
        let (tell, heard) = mpsc::channel();
        let wait_path = path.clone();
        let waiting_stop = stop.clone();
        std::thread::spawn(move || {
            let _ = tell.send(wait_for_reader_release(&wait_path, None, &waiting_stop));
        });
        stop.cancel();
        let answer = heard
            .recv_timeout(WATCHDOG)
            .unwrap_or_else(|error| panic!("wait {attempt} never answered its stop: {error}"));
        assert!(
            matches!(answer, ReaderReleaseWait::Stopped),
            "wait {attempt} was cancelled, so it is answered Stopped",
        );
    }

    parking.let_go();
    holding.join().expect("the reader finishes its read");

    // Twenty abandoned acquisitions would all be queued for the exclusive hold
    // the release just freed, and the first of them would be holding it now.
    // One fresh wait answered at once is twenty proofs that none of them are.
    let after = released_store_answer(&path, "after {REPEATED_WAITS} cancelled waits");
    assert!(
        matches!(after, ReaderReleaseWait::Released),
        "after {REPEATED_WAITS} cancelled waits the released store is free to the next caller, \
         so not one of them left anything queued for its exclusive hold",
    );
}

/// The promise this must not break: a wait that is NOT cancelled still sleeps
/// until the real release, with no timer of its own and no answer before it.
#[test]
fn an_uncancelled_wait_still_sleeps_until_the_real_release() {
    let directory = tempfile::TempDir::new().expect("task-scoped coordination directory");
    let runtime_root = secure_runtime_root(directory.path(), "uncancelled-runtime");
    let path = seeded_store(directory.path(), "uncancelled.db");
    let (parking, holding) = park_a_reader(&path, &runtime_root);

    let (tell, heard) = mpsc::channel();
    let wait_path = path.clone();
    std::thread::spawn(move || {
        let stop = OwnerReadCancellation::new();
        let _ = tell.send(wait_for_reader_release(&wait_path, None, &stop));
    });
    assert!(
        heard.recv_timeout(PARKED).is_err(),
        "a reader is holding the store, so the wait is parked and stays parked",
    );

    parking.let_go();
    holding.join().expect("the reader finishes its read");
    let answer = heard
        .recv_timeout(WATCHDOG)
        .expect("the release is what wakes the waiter");
    assert!(
        matches!(answer, ReaderReleaseWait::Released),
        "the holder letting go is what the waiter was waiting for",
    );
}
