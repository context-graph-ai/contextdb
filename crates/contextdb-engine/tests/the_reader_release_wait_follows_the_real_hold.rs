#![cfg(all(unix, feature = "test-seams"))]
//! Waiting for a store's readers to let go is answered by the READERS, not by
//! what happens to be written down about them.
//!
//! A caller that wants to become the writer of a store readers are holding
//! waits, and then takes the store. Everything that wait says is acted on: told
//! "released", the caller opens the store writable. So the wait has to be
//! driven by the condition that actually blocks that caller -- a reader holding
//! the committed image open -- and by nothing weaker.
//!
//! Reader breadcrumbs are not that condition. They are DIAGNOSIS: a
//! best-effort ephemeral note so a refusal can name who to go and look at, and
//! the document that introduces them says so. A reader whose breadcrumb could
//! not be published is still a reader; a runtime directory that cannot be read
//! still has readers behind it. Answering "released" from an empty or unusable
//! scan tells the caller a store is free while a process is holding it, and the
//! caller then opens it and is refused by the storage layer with a fault it
//! never asked about -- or, worse, waits again on a hold that is invisible.
//!
//! These journeys therefore hold the store for real and make the DIAGNOSIS
//! fail, separately. Each hold is a production `ReadSession` parked inside its
//! load through the production progress observer -- the only window in which a
//! direct reader holds anything at all -- and each is corroborated by asking
//! Redb itself whether a writable owner could take the store, which is the
//! definitive signal today.
//!
//! The mechanism that makes the wait definitive is the implementer's to
//! choose. What is pinned here is only the behavior a caller is entitled to.

use contextdb_core::Value;
use contextdb_core::read_contract::{OwnerReadCancellation, ReadRoute};
use contextdb_engine::local_transport::RuntimeDirectory;
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    RawRedbWriterOpenObservation, try_raw_redb_writer_open_for_test,
};
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

/// Failsafe only. Nothing here asserts how long a wake took; this exists so a
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

/// The directory a reader publishes its breadcrumb below, and a writer scans.
fn diagnostic_directory(runtime_root: &Path) -> PathBuf {
    runtime_root.join("contextdb")
}

/// Make the diagnosis fail while leaving the store, the runtime root, and the
/// reader itself untouched.
///
/// The directory is present and readable; it simply is not owner-only, which is
/// exactly the condition a reader refuses to publish a breadcrumb into. Nothing
/// about the reader's hold on the store changes.
fn make_breadcrumbs_unpublishable(runtime_root: &Path) {
    let directory = diagnostic_directory(runtime_root);
    if !directory.exists() {
        std::fs::create_dir(&directory).expect("create the diagnostic directory");
    }
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o750))
        .expect("make the diagnostic directory one no reader will publish into");
}

/// Make the diagnosis unreadable: the pathname a scan expects to be a directory
/// is an ordinary file, so scanning it fails outright.
fn make_breadcrumbs_unreadable(runtime_root: &Path) {
    let directory = diagnostic_directory(runtime_root);
    if directory.exists() {
        std::fs::remove_dir_all(&directory).expect("clear the diagnostic directory");
    }
    std::fs::write(&directory, b"not a directory").expect("plant an unusable diagnostic pathname");
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

/// The definitive fact, asked of the storage engine itself: a store a direct
/// reader is holding cannot be taken by a writable owner. This is what the wait
/// is really about, and it is true whether or not anything was written down.
fn assert_the_store_is_really_held(path: &Path, context: &str) {
    assert_eq!(
        try_raw_redb_writer_open_for_test(path),
        RawRedbWriterOpenObservation::DatabaseAlreadyOpen,
        "{context}: the store must actually be held, or this journey proves nothing",
    );
}

fn assert_the_store_is_really_free(path: &Path, context: &str) {
    assert_eq!(
        try_raw_redb_writer_open_for_test(path),
        RawRedbWriterOpenObservation::Acquired,
        "{context}: the store must actually be free",
    );
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

fn describe(answer: &ReaderReleaseWait) -> String {
    match answer {
        ReaderReleaseWait::Released => "Released".to_owned(),
        ReaderReleaseWait::Stopped => "Stopped".to_owned(),
        ReaderReleaseWait::Unobservable(error) => format!("Unobservable({error})"),
    }
}

/// A reader with no breadcrumb is still a reader. The store is held, so the
/// wait is not over.
#[test]
fn a_reader_whose_breadcrumb_could_not_be_published_still_holds_the_wait() {
    let directory = tempfile::TempDir::new().expect("task-scoped reader-release directory");
    let runtime = supplied_runtime_directory(directory.path());
    let path = idle_store(directory.path());
    make_breadcrumbs_unpublishable(&runtime);

    let (parking, holding) = park_a_reader(&path, &runtime);
    assert_the_store_is_really_held(&path, "a reader parked inside its load");

    let heard = wait_in_thread(&path, &runtime, OwnerReadCancellation::new());
    if let Ok(early) = heard.recv_timeout(PARKED) {
        panic!(
            "the wait answered {} while a reader was holding the store. A caller told this opens \
             a store another process has open.",
            describe(&early)
        );
    }

    parking.let_go();
    holding.join().expect("the reader finishes its read");
    assert_the_store_is_really_free(&path, "after the only reader let go");
    assert!(
        matches!(answer(&heard, "the release"), ReaderReleaseWait::Released),
        "the holder letting go is what the waiter was waiting for"
    );
}

/// Several readers, only one of them written down. The wait ends when the LAST
/// real hold ends, not when the one the diagnosis happened to know about does.
#[test]
fn the_wait_ends_on_the_last_real_hold_not_the_last_recorded_one() {
    let directory = tempfile::TempDir::new().expect("task-scoped reader-release directory");
    let runtime = supplied_runtime_directory(directory.path());
    let path = idle_store(directory.path());

    // The first reader publishes itself normally.
    let (recorded, recorded_thread) = park_a_reader(&path, &runtime);
    // The second cannot, and holds the store exactly as hard.
    make_breadcrumbs_unpublishable(&runtime);
    let (unrecorded, unrecorded_thread) = park_a_reader(&path, &runtime);
    assert_the_store_is_really_held(&path, "two readers parked inside their loads");

    let heard = wait_in_thread(&path, &runtime, OwnerReadCancellation::new());
    assert!(
        heard.recv_timeout(PARKED).is_err(),
        "two readers are holding the store, so the wait is parked"
    );

    recorded.let_go();
    recorded_thread.join().expect("the first reader finishes");
    assert_the_store_is_really_held(&path, "one reader let go, the other did not");
    if let Ok(early) = heard.recv_timeout(PARKED) {
        panic!(
            "the wait answered {} when the reader it could NAME let go, while another reader was \
             still holding the store",
            describe(&early)
        );
    }

    unrecorded.let_go();
    unrecorded_thread
        .join()
        .expect("the second reader finishes");
    assert_the_store_is_really_free(&path, "after the last reader let go");
    assert!(
        matches!(
            answer(&heard, "the last release"),
            ReaderReleaseWait::Released
        ),
        "the last real hold ending is what ends the wait"
    );
}

/// A caller can always get out. The stop is answered as promptly as a release,
/// and it says the caller stopped -- never that the store is free.
#[test]
fn an_explicit_stop_still_ends_a_wait_on_a_reader_nothing_recorded() {
    let directory = tempfile::TempDir::new().expect("task-scoped reader-release directory");
    let runtime = supplied_runtime_directory(directory.path());
    let path = idle_store(directory.path());
    make_breadcrumbs_unpublishable(&runtime);

    let (parking, holding) = park_a_reader(&path, &runtime);
    assert_the_store_is_really_held(&path, "a reader parked inside its load");

    let stop = OwnerReadCancellation::new();
    let heard = wait_in_thread(&path, &runtime, stop.clone());
    if let Ok(early) = heard.recv_timeout(PARKED) {
        panic!(
            "the wait answered {} before the caller ever asked to stop, while a reader was \
             holding the store",
            describe(&early)
        );
    }

    stop.cancel();
    assert!(
        matches!(answer(&heard, "the stop"), ReaderReleaseWait::Stopped),
        "a caller that asked to stop is told it stopped, not that the store is free"
    );
    assert_the_store_is_really_held(&path, "the stop said nothing about the holder");

    parking.let_go();
    holding.join().expect("the reader finishes its read");
}

/// A diagnosis that cannot be read is not a store with no readers. While a
/// reader is holding it, the wait is neither over nor settled against the
/// caller: the hold is what it is waiting on, and the hold is still there.
#[test]
fn an_unreadable_diagnostic_directory_neither_releases_nor_settles_the_wait() {
    let directory = tempfile::TempDir::new().expect("task-scoped reader-release directory");
    let runtime = supplied_runtime_directory(directory.path());
    let path = idle_store(directory.path());
    make_breadcrumbs_unreadable(&runtime);

    let (parking, holding) = park_a_reader(&path, &runtime);
    assert_the_store_is_really_held(&path, "a reader parked inside its load");

    let heard = wait_in_thread(&path, &runtime, OwnerReadCancellation::new());
    if let Ok(early) = heard.recv_timeout(PARKED) {
        panic!(
            "the wait answered {} because it could not read the place readers write themselves \
             down, while a reader was holding the store. Released sends the caller at a store \
             that is taken; a settled failure makes a caller give up on a wait that would have \
             ended.",
            describe(&early)
        );
    }

    parking.let_go();
    holding.join().expect("the reader finishes its read");
    assert_the_store_is_really_free(&path, "after the only reader let go");
    assert!(
        matches!(answer(&heard, "the release"), ReaderReleaseWait::Released),
        "the hold ending ends the wait, whatever could or could not be read about it"
    );
}

/// A store that really has no readers is released at once, whether or not
/// anything about readers can be read. This is the other half of the contract:
/// making the wait definitive must not make it hang.
#[test]
fn a_store_nobody_holds_is_released_even_when_the_diagnosis_is_unusable() {
    let directory = tempfile::TempDir::new().expect("task-scoped reader-release directory");
    let runtime = supplied_runtime_directory(directory.path());
    let path = idle_store(directory.path());
    make_breadcrumbs_unreadable(&runtime);
    assert_the_store_is_really_free(&path, "nobody is holding this store");

    let heard = wait_in_thread(&path, &runtime, OwnerReadCancellation::new());
    assert!(
        matches!(
            answer(&heard, "an unheld store"),
            ReaderReleaseWait::Released
        ),
        "nobody is holding it, so there is nothing to wait for"
    );
}
