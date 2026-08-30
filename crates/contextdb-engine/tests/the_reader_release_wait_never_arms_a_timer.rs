#![cfg(all(unix, feature = "test-seams"))]
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
//! This journey owns its test binary. The recorder below is process-wide,
//! because the wait does its blocking on threads it spawns itself, and a second
//! wait running beside this one would mix its blocks in.
//!
//! -------------------------------------------------------------------------
//! SEAM THIS JOURNEY NEEDS, VERBATIM
//!
//! It does not exist yet, so this file is compile-RED until it lands. In
//! `crates/contextdb-engine/src/persistence.rs`, inside
//! `pub mod read_persistence_test_scaffold`:
//!
//! ```ignore
//!     /// One blocking wait a reader-release wait actually performed.
//!     ///
//!     /// A wait that sleeps until the kernel reports a holder let go blocks
//!     /// once per hold and supplies no deadline of its own. A wait that polls
//!     /// supplies a deadline every time, and that deadline IS the poll
//!     /// interval. Recording the shape of each block separates the two
//!     /// without measuring how long anything took.
//!     #[derive(Debug, Clone, Copy, PartialEq, Eq)]
//!     pub enum ReaderReleaseBlockForTest {
//!         /// Blocked until the kernel reported that a real holder of the
//!         /// store let go or died. Nothing but that event decides when this
//!         /// wakes.
//!         UntilHolderReleased,
//!         /// Blocked with a deadline of the wait's own choosing, in
//!         /// milliseconds.
//!         UntilDeadline { after_ms: u64 },
//!     }
//!
//!     /// Start recording what reader-release waits block in, discarding
//!     /// anything recorded before. Recording is process-wide, so a proof
//!     /// that uses it owns its test binary.
//!     pub fn reset_reader_release_blocks_for_test() { .. }
//!
//!     /// Every block reader-release waits have performed since the reset, in
//!     /// order.
//!     pub fn reader_release_blocks_for_test() -> Vec<ReaderReleaseBlockForTest> { .. }
//! ```
//!
//! Production `wait_for_reader_release` records one entry for every block it
//! performs, whatever mechanism it blocks on.
//! -------------------------------------------------------------------------

use contextdb_core::Value;
use contextdb_core::read_contract::{OwnerReadCancellation, ReadRoute};
use contextdb_engine::local_transport::RuntimeDirectory;
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    ReaderReleaseBlockForTest, reader_release_blocks_for_test, reset_reader_release_blocks_for_test,
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

fn wait_in_thread(path: &Path, runtime: &Path) -> mpsc::Receiver<ReaderReleaseWait> {
    let (tell, heard) = mpsc::channel();
    let path = path.to_path_buf();
    let runtime = RuntimeDirectory::supplied(runtime);
    std::thread::spawn(move || {
        let stop = OwnerReadCancellation::new();
        let _ = tell.send(wait_for_reader_release(&path, Some(&runtime), &stop));
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

    reset_reader_release_blocks_for_test();
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
    assert!(
        matches!(
            heard
                .recv_timeout(WATCHDOG)
                .expect("the wait answers once the last reader lets go"),
            ReaderReleaseWait::Released
        ),
        "the last hold ending ends the wait"
    );

    let blocks = reader_release_blocks_for_test();
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
