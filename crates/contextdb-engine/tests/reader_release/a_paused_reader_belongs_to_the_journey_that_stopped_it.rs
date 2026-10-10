//! Two journeys running at once each stop their own store's readers, and
//! neither can reach the other's.
//!
//! The whole estate runs in one process at default parallelism, so two proofs
//! that both stop a reader are ordinary, not exotic. If the pause had one
//! process-wide target and one ticket list, the first journey's wait would
//! hand back the second journey's reader and the first journey's release would
//! let the second journey's reader go -- one fixture silently affecting
//! another, and a failure that looks like a product defect in whichever of
//! them lost the race. A pause is therefore owned: it names one store, hands
//! back only that store's readers, and frees only the readers it stopped.
use contextdb_core::Value;
use contextdb_core::read_contract::ReadRoute;
use contextdb_engine::persistence::read_persistence_test_scaffold::pause_readers_at_source_start_for_test;
use contextdb_engine::{Database, ReadSession, ReadSessionOptions};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Failsafe only: a reader that never arrives fails as a message, not a hang.
const WATCHDOG: Duration = Duration::from_secs(60);
/// How long a journey looks for a reader it must NOT be handed, and how long a
/// wrongly addressed release is given to wrongly take effect.
const LOOKED_FOR: Duration = Duration::from_secs(2);

fn supplied_runtime_directory(inside: &Path) -> PathBuf {
    let root = inside.join("reader-runtime");
    std::fs::create_dir(&root).expect("create the operator-supplied runtime directory");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure the operator-supplied runtime directory");
    std::fs::canonicalize(&root).expect("resolve the supplied directory the way the store will")
}

fn idle_store(directory: &Path, name: &str) -> PathBuf {
    let path = directory.join(name);
    let database = Database::open(&path).expect("claim a store to seed");
    database
        .execute(
            "CREATE TABLE held (id INTEGER PRIMARY KEY, body TEXT)",
            &HashMap::new(),
        )
        .expect("create the held table");
    database
        .execute(
            "INSERT INTO held (id, body) VALUES (0, $body)",
            &HashMap::from([("body".to_owned(), Value::Text("h".repeat(256 * 1024)))]),
        )
        .expect("seed one wide row");
    database.close().expect("release the seeded store");
    path
}

struct PausedTestClock;
impl contextdb_core::read_contract::DeadlineClock for PausedTestClock {
    fn now_ms(&self) -> u64 {
        0
    }
    fn wait_until(&self, _: u64) -> contextdb_core::read_contract::DeadlineWait<'_> {
        Box::pin(std::future::pending())
    }
}

fn open_paused_reader(path: &Path, runtime: &Path) -> ReadSession {
    ReadSession::with_runtime_directory_for_test(runtime, || {
        ReadSession::open_with_clock_for_test(
            path,
            ReadSessionOptions::default(),
            std::sync::Arc::new(PausedTestClock),
        )
    })
    .expect("an idle store is readable")
}

fn reader(path: &Path, runtime: &Path) -> std::thread::JoinHandle<()> {
    let path = path.to_path_buf();
    let runtime = runtime.to_path_buf();
    std::thread::spawn(move || {
        let session =
            open_paused_reader(&path, &runtime);
        assert_eq!(session.route(), ReadRoute::File);
    })
}

/// Like `reader`, but also hands back the receiving half of a channel the
/// reader thread signals the instant it returns. A caller can then prove a
/// bounded negative -- "this reader has NOT unblocked within `within`" -- by
/// asserting `recv_timeout(within)` itself times out, instead of sleeping the
/// full window and polling `JoinHandle::is_finished` afterwards: the channel
/// wakes the caller the moment the reader actually finishes, so a genuine
/// early failure (the reader wrongly released) is reported immediately
/// rather than only after the whole window elapses.
fn reader_with_completion_signal(
    path: &Path,
    runtime: &Path,
) -> (std::thread::JoinHandle<()>, std::sync::mpsc::Receiver<()>) {
    let path = path.to_path_buf();
    let runtime = runtime.to_path_buf();
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        let session =
            open_paused_reader(&path, &runtime);
        assert_eq!(session.route(), ReadRoute::File);
        let _ = finished_tx.send(());
    });
    (handle, finished_rx)
}

#[test]
fn two_simultaneous_stores_cannot_take_each_other_s_paused_readers() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let runtime = supplied_runtime_directory(directory.path());
    let first_path = idle_store(directory.path(), "first.db");
    let second_path = idle_store(directory.path(), "second.db");

    let first = pause_readers_at_source_start_for_test(&first_path);
    let second = pause_readers_at_source_start_for_test(&second_path);

    let (first_reader, first_reader_finished) =
        reader_with_completion_signal(&first_path, &runtime);
    let (second_reader, second_reader_finished) =
        reader_with_completion_signal(&second_path, &runtime);

    let first_ticket = first
        .wait_for_paused_reader(WATCHDOG)
        .expect("the first store's reader is stopped inside its own load");
    let second_ticket = second
        .wait_for_paused_reader(WATCHDOG)
        .expect("the second store's reader is stopped inside its own load");
    assert_ne!(
        first_ticket, second_ticket,
        "two stopped readers are two different readers"
    );

    // Each journey has been handed its own store's only reader. Asking again
    // must not hand it the other journey's, which is stopped right now.
    assert_eq!(
        first.wait_for_paused_reader(LOOKED_FOR),
        None,
        "the first store's pause answers with the first store's readers and no others"
    );
    assert_eq!(
        second.wait_for_paused_reader(LOOKED_FOR),
        None,
        "the second store's pause answers with the second store's readers and no others"
    );

    // A release addressed through the wrong store's pause frees nobody: each
    // reader's completion channel must still be silent after `LOOKED_FOR`,
    // not merely "not yet observed" after a fixed sleep.
    first.release(second_ticket);
    second.release(first_ticket);
    assert_eq!(
        first_reader_finished.recv_timeout(LOOKED_FOR),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout),
        "the first store's reader is only the first store's pause to free"
    );
    assert_eq!(
        second_reader_finished.recv_timeout(LOOKED_FOR),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout),
        "the second store's reader is only the second store's pause to free"
    );

    // Each journey frees its own, and only its own.
    first.release(first_ticket);
    first_reader.join().expect("the first reader finishes");
    assert!(
        !second_reader.is_finished(),
        "freeing one store's reader leaves the other store's reader exactly where it was"
    );

    second.release(second_ticket);
    second_reader.join().expect("the second reader finishes");
}

/// A journey that came apart still frees the reader it stopped: dropping the
/// pause lets every reader of that store carry on, and stops pausing later
/// ones.
#[test]
fn dropping_a_pause_frees_the_readers_it_stopped() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let runtime = supplied_runtime_directory(directory.path());
    let path = idle_store(directory.path(), "dropped.db");

    let holding = {
        let pause = pause_readers_at_source_start_for_test(&path);
        let holding = reader(&path, &runtime);
        pause
            .wait_for_paused_reader(WATCHDOG)
            .expect("the reader is stopped inside its load");
        assert!(!holding.is_finished());
        holding
    };
    holding
        .join()
        .expect("the dropped pause let its reader carry on");
}
