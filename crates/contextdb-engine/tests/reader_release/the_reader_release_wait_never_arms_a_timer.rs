//! Waiting for a store's readers to let go costs nothing while it waits.
//!
//! A supervisor waiting to take over a store may wait for a long time, and the
//! whole reason this is a wait rather than a retry loop is what a retry loop
//! costs: a check every second holds the store hostage for up to a second after
//! it is actually free, and burns a wakeup a second until then. On a machine
//! running many stores that is a background load nobody asked for, in exchange
//! for a worse answer.
//!
//! What is pinned here is therefore the SHAPE of every block the wait performs,
//! not how long anything took. A wait that sleeps until the kernel tells it a
//! holder let go supplies no deadline of its own -- the release is what wakes
//! it. A wait that polls supplies a deadline every time, and that deadline IS
//! the poll interval. Counting blocks separates the two without a stopwatch,
//! and the count also settles the other half: a wait that blocks a bounded
//! number of times per real hold is not spinning.
//!
//! The recorder belongs to the thread calling the wait. The waiting thread
//! resets it and returns its own observations with its answer, so sibling
//! tests cannot add blocks or clear this journey's recording.

use contextdb_core::Value;
use contextdb_core::read_contract::{OwnerReadCancellation, ReadRoute};
use contextdb_engine::local_transport::RuntimeDirectory;
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    ReaderReleaseBlockForTest, ReaderSourceStartPause, pause_readers_at_source_start_for_test,
    reader_release_blocks_for_test, reset_reader_release_blocks_for_test,
};
use contextdb_engine::persistence::{ReaderReleaseWait, wait_for_reader_release};
use contextdb_engine::{Database, ReadSession, ReadSessionOptions};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::time::Duration;

/// Failsafe only. Nothing here asserts how long a wake took.
const WATCHDOG: Duration = Duration::from_secs(60);
/// How long the wait is observed still parked, so the blocks counted below are
/// counted across a window in which a poller would have polled many times.
const PARKED: Duration = Duration::from_secs(2);
/// How many readers hold the store in this journey.
const HOLDS: usize = 2;
const WIDE_ROWS: usize = 8;
const WIDE_ROW_BYTES: usize = 512 * 1024;

fn supplied_runtime_directory(inside: &Path) -> PathBuf {
    let root = inside.join("reader-runtime");
    std::fs::create_dir(&root).expect("create the operator-supplied runtime directory");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure the operator-supplied runtime directory");
    std::fs::canonicalize(&root).expect("resolve the supplied directory the way the store will")
}

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

/// A real direct reader, stopped where its read-image source starts -- the
/// first production point inside the load-time hold, before any registration
/// is published -- and let go when the journey says so.
///
/// Nothing here manufactures a holder. The reader is an ordinary one, holding
/// the store exactly as any reader does; the store simply stops it at that
/// point until this handle releases it.
struct ParkingReader {
    /// This journey's own pause over its own store: the only handle that can
    /// free the reader it stopped, and no handle at all on anyone else's.
    pause: ReaderSourceStartPause,
    ticket: u64,
}

impl ParkingReader {
    fn let_go(&self) {
        self.pause.release(self.ticket);
    }
}

/// A journey that came apart before it let its reader go still lets it go, so
/// a failed assertion leaves no reading thread stopped in the store.
impl Drop for ParkingReader {
    fn drop(&mut self) {
        self.let_go();
    }
}

fn park_a_reader(path: &Path, runtime: &Path) -> (Arc<ParkingReader>, std::thread::JoinHandle<()>) {
    let pause = pause_readers_at_source_start_for_test(path);
    let path = path.to_path_buf();
    let runtime = runtime.to_path_buf();
    let holding = std::thread::spawn(move || {
        let session =
            ReadSession::open_in_runtime_dir(&path, ReadSessionOptions::default(), Some(runtime))
                .expect("an idle store is readable");
        assert_eq!(
            session.route(),
            ReadRoute::File,
            "a direct reader is what holds the store; an owner route holds nothing"
        );
    });
    let ticket = pause
        .wait_for_paused_reader(WATCHDOG)
        .expect("the reader never started its read-image source, so nothing ever held the store");
    (Arc::new(ParkingReader { pause, ticket }), holding)
}

fn wait_in_thread(
    path: &Path,
    runtime: &Path,
) -> mpsc::Receiver<(ReaderReleaseWait, Vec<ReaderReleaseBlockForTest>)> {
    let (tell, heard) = mpsc::channel();
    let path = path.to_path_buf();
    let runtime = RuntimeDirectory::supplied(runtime);
    std::thread::spawn(move || {
        let stop = OwnerReadCancellation::new();
        reset_reader_release_blocks_for_test();
        let answer = wait_for_reader_release(&path, Some(&runtime), &stop);
        let _ = tell.send((answer, reader_release_blocks_for_test()));
    });
    heard
}

/// The wait sleeps on the holders and on nothing else.
#[test]
fn waiting_out_two_readers_arms_no_deadline_and_blocks_once_per_hold() {
    let directory = tempfile::TempDir::new().expect("task-scoped reader-release directory");
    let runtime = supplied_runtime_directory(directory.path());
    let path = idle_store(directory.path());

    let (first, first_thread) = park_a_reader(&path, &runtime);
    let (second, second_thread) = park_a_reader(&path, &runtime);

    let heard = wait_in_thread(&path, &runtime);
    assert!(
        heard.recv_timeout(PARKED).is_err(),
        "two readers are holding the store, so the wait is parked"
    );

    first.let_go();
    first_thread.join().expect("the first reader finishes");
    assert!(
        heard.recv_timeout(PARKED).is_err(),
        "one reader is still holding the store, so the wait is still parked"
    );

    second.let_go();
    second_thread.join().expect("the second reader finishes");
    let (answer, blocks) = heard
        .recv_timeout(WATCHDOG)
        .expect("the wait answers once the last reader lets go");
    assert!(
        matches!(answer, ReaderReleaseWait::Released),
        "the last hold ending ends the wait"
    );

    assert!(
        !blocks.is_empty(),
        "the wait answered without ever blocking, so it spun while two readers held the store",
    );
    let armed: Vec<_> = blocks
        .iter()
        .filter(|block| !matches!(block, ReaderReleaseBlockForTest::UntilHolderReleased))
        .collect();
    assert!(
        armed.is_empty(),
        "the wait armed a deadline of its own: {armed:?}. A deadline is a poll interval, and a \
         poll interval is how long a caller keeps a free store hostage after it is free.",
    );
    // Each real hold is worth waking for once. Anything beyond a small multiple
    // of the holds is a retry loop wearing a wait's clothing -- and this window
    // held the store still for long enough that a retry loop would show it.
    assert!(
        blocks.len() <= HOLDS.saturating_mul(2),
        "the wait blocked {} times to wait out {HOLDS} holds: {blocks:?}",
        blocks.len(),
    );
}
