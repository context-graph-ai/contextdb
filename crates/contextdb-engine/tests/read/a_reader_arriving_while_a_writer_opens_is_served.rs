//! A reader that arrives while another opener is taking the store for writing
//! is served the committed rows; it is never refused because the writer holds
//! the data descriptor.
//!
//! The writer is held at the point in its open where it already holds the
//! store's data descriptor exclusively but has published nothing a reader can
//! dial: its companion claim is taken, its pre-writer reader set is captured,
//! and no view, owner channel or serving decision exists yet. The reader opens
//! with a clock whose deadlines never arrive on their own, so the only thing
//! that can end its wait is the writer finishing its open; no budget expiry can
//! stand in for it.

use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait};
use contextdb_engine::persistence::read_persistence_test_scaffold::observe_writer_reader_snapshots_for_test;
use contextdb_engine::persistence::store_companion_path;
use contextdb_engine::read_session::{
    ReadRouteOpenFailure, ReadSessionEvent, ReadSessionTestObserver,
};
use contextdb_engine::{Database, ReadSession, ReadSessionOptions};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, mpsc};

/// A clock whose deadlines are never reached by time alone.
struct UnexpiringClock;

impl DeadlineClock for UnexpiringClock {
    fn now_ms(&self) -> u64 {
        1
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(std::future::pending())
    }
}

enum ReaderProgress {
    /// The reader has started waiting on the writer, or its selection ended.
    Arrived,
    /// The reader finished, served or refused.
    Finished(Result<Vec<Vec<Value>>, String>),
}

struct ReaderArrival(Mutex<mpsc::Sender<ReaderProgress>>);

impl ReadSessionTestObserver for ReaderArrival {
    fn observe_event(&self, event: ReadSessionEvent) {
        let arrived = match event {
            ReadSessionEvent::ClaimWindowWait { .. } => true,
            ReadSessionEvent::RouteOpenFailed { attempt, failure } => {
                attempt >= 2 || failure != ReadRouteOpenFailure::WriterBecameOwner
            }
            _ => false,
        };
        if arrived {
            let _ = self
                .0
                .lock()
                .expect("reader arrival sender")
                .send(ReaderProgress::Arrived);
        }
    }
}

fn seed(path: &Path) {
    let seed = Database::open(path).expect("create store");
    seed.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .expect("create table");
    seed.execute("INSERT INTO items VALUES (1)", &HashMap::new())
        .expect("commit row");
    seed.close().expect("close seeding writer");
}

/// Hold a writer's open at the held data descriptor, open a reader against
/// it, release the writer once the reader has arrived, and require that the
/// reader was served the committed rows.
fn read_while_a_writer_holds_the_data_descriptor(path: &Path) {
    // Dropping `resume` (including by a panic below) lets the writer go.
    let (reached, reached_rx) = mpsc::channel::<()>();
    let (resume, resume_rx) = mpsc::channel::<()>();
    let reached = Mutex::new(reached);
    let resume_rx = Mutex::new(resume_rx);
    let (finish, finish_rx) = mpsc::channel::<()>();
    let writer_path = path.to_path_buf();
    let writer = std::thread::spawn(move || {
        let database = observe_writer_reader_snapshots_for_test(
            Arc::new(move |_registry| {
                let _ = reached.lock().expect("writer reached sender").send(());
                let _ = resume_rx.lock().expect("writer resume receiver").recv();
            }),
            || Database::open(&writer_path),
        )
        .expect("the writer's own open completes once resumed");
        let _ = finish_rx.recv();
        drop(database);
    });
    reached_rx
        .recv()
        .expect("the writer reaches the held data descriptor before publishing an owner");

    let (progress, progress_rx) = mpsc::channel();
    let finished = progress.clone();
    let reader_path = path.to_path_buf();
    let reader = std::thread::spawn(move || {
        let result = ReadSession::open_with_observations_for_test(
            &reader_path,
            ReadSessionOptions::default(),
            Arc::new(UnexpiringClock),
            Some(Arc::new(ReaderArrival(Mutex::new(progress)))),
            None,
        )
        .and_then(|session| session.execute("SELECT id FROM items", &HashMap::new()))
        .map(|result| result.rows().to_vec())
        .map_err(|error| error.to_string());
        let _ = finished.send(ReaderProgress::Finished(result));
    });

    // Release the writer only once the reader has arrived against the held
    // descriptor (waiting on it, or already answered).
    let mut answer = match progress_rx.recv().expect("the reader reports its arrival") {
        ReaderProgress::Arrived => None,
        ReaderProgress::Finished(result) => Some(result),
    };
    resume.send(()).expect("resume the writer");
    let answer = match answer.take() {
        Some(result) => result,
        None => loop {
            match progress_rx.recv().expect("the reader reports its answer") {
                ReaderProgress::Arrived => continue,
                ReaderProgress::Finished(result) => break result,
            }
        },
    };
    reader.join().expect("reader thread");

    let rows = answer.unwrap_or_else(|refusal| {
        panic!(
            "a reader arriving while a writer opens the store must be served the committed \
             rows, not refused: {refusal}"
        )
    });
    assert_eq!(rows, vec![vec![Value::Int64(1)]]);
    finish.send(()).expect("let the writer close");
    writer.join().expect("writer thread");
}

/// A store without its companion (copied, restored, or never owned beside
/// this runtime) gives the reader no recorded owner, so it goes to the file
/// itself while the writer holds the data descriptor.
#[test]
fn a_reader_arriving_while_a_writer_takes_a_store_without_its_companion_reads_the_committed_rows() {
    let directory = tempfile::tempdir().expect("scratch directory");
    let path = directory.path().join("store.db");
    seed(&path);
    std::fs::remove_file(store_companion_path(&path)).expect("remove the store's companion");
    read_while_a_writer_holds_the_data_descriptor(&path);
}

/// A store whose companion still records its previous, departed writer.
#[test]
fn a_reader_arriving_while_a_writer_reopens_a_previously_owned_store_reads_the_committed_rows() {
    let directory = tempfile::tempdir().expect("scratch directory");
    let path = directory.path().join("store.db");
    seed(&path);
    read_while_a_writer_holds_the_data_descriptor(&path);
}
