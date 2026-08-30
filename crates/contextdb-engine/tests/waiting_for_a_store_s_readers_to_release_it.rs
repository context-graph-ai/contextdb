//! Waiting for the direct readers of a store to let go of it.
//!
//! A caller that wants to become the writer of a store readers are holding has
//! only two honest choices: fail now, or wait. Waiting must not mean polling --
//! a supervisor that checks every second holds the store hostage for up to a
//! second after it is free, and burns a wakeup a second until then. So the wait
//! is the kernel's own: a direct reader holds an exclusive advisory lock on its
//! breadcrumb for exactly as long as it is holding the store open, and blocking
//! on that lock sleeps until the holder unlocks it or dies.
//!
//! And a wait a caller cannot get out of is a hang, so an explicit stop is
//! answered as promptly as a release, with the holder still holding.
//!
//! WHICH window is being waited on is the part worth writing down, because it
//! is not the one a reader of this file would assume. A direct reading session
//! holds the store only while it is LOADING the committed image; once that
//! image has decoded the session holds nothing -- no redb handle, no lock, no
//! breadcrumb -- and answers every later statement from what it already
//! decoded. That is deliberate, and it is why several readers coexist on one
//! store. It also means the load is the ONLY window in which a would-be writer
//! can collide with a reader at all, so the load is exactly what this wait
//! waits for: a caller taking over a store waits out the readers still
//! hydrating, and the moment the last of them has decoded, the store is free.
//!
//! So these journeys park a reader inside that load, through the production
//! progress observer, on the reading thread, while the hold is real. No test
//! seam manufactures a holder here.

#![cfg(unix)]

use contextdb_core::Value;
use contextdb_core::read_contract::{OwnerReadCancellation, ReadRoute};
use contextdb_engine::local_transport::RuntimeDirectory;
use contextdb_engine::persistence::{ReaderReleaseWait, wait_for_reader_release};
use contextdb_engine::{
    Database, ReadPhase, ReadProgress, ReadProgressObserver, ReadSession, ReadSessionOptions,
};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::Duration;

/// Failsafe only. Nothing here asserts how LONG a wake took; this exists so a
/// wait that never wakes fails as a readable message instead of hanging.
const WATCHDOG: Duration = Duration::from_secs(60);
/// How long a parked wait is observed still parked. Not a latency claim: it is
/// how a journey establishes that the wait is genuinely blocked on the holder
/// rather than having already answered.
const PARKED: Duration = Duration::from_secs(2);
/// Enough committed bytes that loading the image reports its progress at least
/// once, which is where a reader can be parked while it holds the store.
const WIDE_ROWS: usize = 8;
const WIDE_ROW_BYTES: usize = 512 * 1024;

fn supplied_runtime_directory(inside: &Path) -> PathBuf {
    let root = inside.join("reader-runtime");
    std::fs::create_dir(&root).expect("create the operator-supplied runtime directory");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure the operator-supplied runtime directory");
    std::fs::canonicalize(&root).expect("resolve the supplied directory the way the store will")
}

/// An idle store with enough committed bytes that its load is observable.
fn idle_store(directory: &Path) -> PathBuf {
    let path = directory.join("held.db");
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

/// Stops the reading thread inside the load, where it is holding the store,
/// and lets the test decide when it may finish.
#[derive(Default)]
struct ParkingReader {
    entered: (Mutex<bool>, Condvar),
    release: (Mutex<bool>, Condvar),
    parked: AtomicBool,
}

impl ParkingReader {
    /// Block until the reading thread is inside the hold. An event, not a
    /// duration: the watchdog only turns a reader that never got there into a
    /// readable failure.
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

/// A real direct reader, parked inside the load with the store held. Joining
/// the returned handle after `let_go` is what releases it.
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
        assert_eq!(
            session.route(),
            ReadRoute::File,
            "a direct reader is what holds the store; an owner route holds nothing"
        );
    });
    parking.wait_until_holding();
    (parking, holding)
}

/// Run the wait on its own thread, answering through a channel so the test can
/// release or cancel while it is parked.
fn wait_in_thread(
    path: &Path,
    runtime: &Path,
    stop: OwnerReadCancellation,
) -> mpsc::Receiver<ReaderReleaseWait> {
    let (tell, heard) = mpsc::channel();
    let path = path.to_path_buf();
    let runtime = RuntimeDirectory::supplied(runtime);
    std::thread::spawn(move || {
        let _ = tell.send(wait_for_reader_release(&path, Some(&runtime), &stop));
    });
    heard
}

fn answer(heard: &mpsc::Receiver<ReaderReleaseWait>, what: &str) -> ReaderReleaseWait {
    heard
        .recv_timeout(WATCHDOG)
        .unwrap_or_else(|error| panic!("the wait never answered {what}: {error}"))
}

/// The holder letting go wakes the waiter, and only then.
#[test]
fn a_reader_letting_go_of_the_store_wakes_the_waiter() {
    let directory = tempfile::TempDir::new().expect("task-scoped reader-release directory");
    let runtime = supplied_runtime_directory(directory.path());
    let path = idle_store(directory.path());
    let (parking, holding) = park_a_reader(&path, &runtime);

    let heard = wait_in_thread(&path, &runtime, OwnerReadCancellation::new());
    assert!(
        heard.recv_timeout(PARKED).is_err(),
        "a store a reader is still holding is not released, so the wait stays parked"
    );

    parking.let_go();
    holding.join().expect("the reader finishes its read");
    assert!(
        matches!(answer(&heard, "the release"), ReaderReleaseWait::Released),
        "the holder letting go is what the waiter was waiting for"
    );
}

/// A store nobody is holding needs no wait: the answer is already Released.
#[test]
fn a_store_nobody_holds_answers_released_without_waiting() {
    let directory = tempfile::TempDir::new().expect("task-scoped reader-release directory");
    let runtime = supplied_runtime_directory(directory.path());
    let path = idle_store(directory.path());

    let heard = wait_in_thread(&path, &runtime, OwnerReadCancellation::new());
    assert!(
        matches!(
            answer(&heard, "an unheld store"),
            ReaderReleaseWait::Released
        ),
        "nobody is holding it, so there is nothing to wait for"
    );
}

/// An explicit stop is answered as promptly as a release -- and the holder is
/// STILL holding, so the answer is about the caller's decision to stop, never
/// a release that did not happen.
#[test]
fn an_explicit_stop_ends_the_wait_with_the_reader_still_holding_the_store() {
    let directory = tempfile::TempDir::new().expect("task-scoped reader-release directory");
    let runtime = supplied_runtime_directory(directory.path());
    let path = idle_store(directory.path());
    let (parking, holding) = park_a_reader(&path, &runtime);

    let stop = OwnerReadCancellation::new();
    let heard = wait_in_thread(&path, &runtime, stop.clone());
    assert!(
        heard.recv_timeout(PARKED).is_err(),
        "the wait is parked on a holder that has not let go"
    );

    stop.cancel();
    assert!(
        matches!(answer(&heard, "the stop"), ReaderReleaseWait::Stopped),
        "a caller that asked to stop is told it stopped, not that the store is free"
    );

    // The stop said nothing about the holder, which is still holding.
    let still_held = wait_in_thread(&path, &runtime, OwnerReadCancellation::new());
    assert!(
        still_held.recv_timeout(PARKED).is_err(),
        "the reader that was holding the store when the stop arrived is holding it still"
    );

    parking.let_go();
    holding.join().expect("the reader finishes its read");
    assert!(
        matches!(
            answer(&still_held, "the release"),
            ReaderReleaseWait::Released
        ),
        "and it releases normally afterwards"
    );
}

/// A stop asked for before the wait began is answered at once, so an interrupt
/// cannot fall into the gap before the listener is registered.
#[test]
fn a_stop_asked_for_before_the_wait_began_is_answered_at_once() {
    let directory = tempfile::TempDir::new().expect("task-scoped reader-release directory");
    let runtime = supplied_runtime_directory(directory.path());
    let path = idle_store(directory.path());
    let (parking, holding) = park_a_reader(&path, &runtime);

    let stop = OwnerReadCancellation::new();
    stop.cancel();
    let heard = wait_in_thread(&path, &runtime, stop);
    assert!(
        matches!(
            answer(&heard, "an already-cancelled stop"),
            ReaderReleaseWait::Stopped
        ),
        "an already-cancelled stop is answered without ever parking"
    );

    parking.let_go();
    holding.join().expect("the reader finishes its read");
}
