//! A bounded vector read on a file-backed store reads one consistent image
//! even while a commit that may empty a vector partition holds its vector
//! removal pass: the read registers the published snapshot without waiting
//! for that commit and answers exactly the state published before it, with
//! the deleted row and its vector still present.
//!
//! The deleting commit is held at each step before it is published in
//! memory; the engine reports that its removal pass is active there.

use contextdb_core::read_contract::ReadLimits;
use contextdb_core::{SnapshotId, Value};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::local_transport::ManualDeadlineClock;
use contextdb_engine::writer_commit_test_seams::{CommitBoundary, observe as observe_commit};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, mpsc};

const SEARCH: &str = "SELECT id FROM items ORDER BY embedding <=> $probe LIMIT 2";

fn search(registrations: &Arc<Registrations>) -> bounded::BoundedReadRequest {
    let mut request = bounded::BoundedReadRequest::new(
        SEARCH,
        HashMap::from([("probe".to_owned(), Value::Vector(vec![1.0, 0.0, 0.0]))]),
        ReadLimits::default(),
        Arc::new(ManualDeadlineClock::at(0)),
    );
    request.probe = Some(Arc::clone(registrations) as Arc<dyn bounded::ExecutionProbe>);
    request
}

fn answer(database: &Database) -> Vec<Vec<Value>> {
    bounded::execute(database, &search(&Arc::default()))
        .expect("the search answers when no commit is in progress")
        .result
        .rows()
        .to_vec()
}

/// A file-backed store whose vectors are served by a maintained index.
fn indexed_store(path: &std::path::Path) -> Database {
    let database = Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the store");
    database
        .execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, bucket INTEGER NOT NULL, embedding VECTOR(3) PARTITION_KEY (bucket) MAX_PARTITIONS 4 SEARCH_MODE INDEXED AUTO_INDEX_AT 1)",
            &HashMap::new(),
        )
        .expect("create table");
    for (id, vector) in [
        (1i64, vec![1.0, 0.0, 0.0]),
        (2, vec![0.9, 0.1, 0.0]),
        (3, vec![0.6, 0.0, 0.8]),
    ] {
        database
            .execute(
                "INSERT INTO items VALUES ($id, $id, $vector)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    ("vector".to_owned(), Value::Vector(vector)),
                ]),
            )
            .expect("commit a vector row");
    }
    database
        .run_maintenance_cycle()
        .expect("publish the index generation the searches use");
    database
}

/// Records the snapshot the read registered.
#[derive(Default)]
struct Registrations(Mutex<Vec<SnapshotId>>);

impl bounded::ExecutionProbe for Registrations {
    fn registered_snapshot_for_test(&self, snapshot: SnapshotId) {
        self.0.lock().expect("registrations").push(snapshot);
    }

    fn before_work(&self, _source: bounded::TestWorkSource, _completed_work: u64) {}

    fn cancellation_observed(&self, _completed_work: u64) {}
}

/// The outcome of one read: its answer or refusal, or the text of a panic.
type ReadOutcome = Result<Result<Vec<Vec<Value>>, bounded::TestError>, String>;

enum Progress {
    WaitingForCommit,
    Finished(ReadOutcome),
}

fn read_beside_a_removal_pass_held_at(boundary: CommitBoundary) {
    let directory = tempfile::tempdir().expect("scratch directory");
    let database = indexed_store(&directory.path().join("store.db"));
    let before = answer(&database);
    assert_eq!(before, vec![vec![Value::Int64(1)], vec![Value::Int64(2)]]);
    let published_before = database.snapshot();
    let registrations = Arc::new(Registrations::default());
    let request = search(&registrations);
    let outcome = std::thread::scope(|scope| {
        // The deleting commit removes row 1 and its vector; it is held at
        // `boundary`. Dropping `resume` (including by a panic below) lets it
        // finish.
        let (reached, reached_rx) = mpsc::channel::<()>();
        let (resume, resume_rx) = mpsc::channel::<()>();
        let deleting = &database;
        let committer = scope.spawn(move || {
            let tx = deleting.begin().expect("begin a delete");
            deleting
                .execute_in_tx(tx, "DELETE FROM items WHERE id = 1", &HashMap::new())
                .expect("stage the delete");
            let reached = Mutex::new(Some(reached));
            let resume = Mutex::new(resume_rx);
            observe_commit(
                Arc::new(move |point| {
                    if point == boundary
                        && let Some(reached) = reached.lock().expect("reached sender").take()
                    {
                        let _ = reached.send(());
                        let _ = resume.lock().expect("resume receiver").recv();
                    }
                }),
                || deleting.commit(tx),
            )
        });
        reached_rx
            .recv()
            .unwrap_or_else(|_| panic!("the delete never reached {boundary:?}"));
        assert!(
            database.active_removal_pass_for_test(),
            "the delete holds its vector removal pass at {boundary:?}"
        );

        let (progress, progress_rx) = mpsc::channel();
        let waiting = Mutex::new(Some(progress.clone()));
        let reading = &database;
        scope.spawn(move || {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                contextdb_tx::commit_wait_test_seams::observe(
                    Arc::new(move || {
                        if let Some(waiting) = waiting.lock().expect("waiting sender").take() {
                            let _ = waiting.send(Progress::WaitingForCommit);
                        }
                    }),
                    || {
                        bounded::execute(reading, &request)
                            .map(|answer| answer.result.rows().to_vec())
                    },
                )
            }))
            .map_err(|_| "the read panicked".to_owned());
            let _ = progress.send(Progress::Finished(outcome));
        });
        let outcome = match progress_rx.recv().expect("the reader reports its progress") {
            Progress::WaitingForCommit => {
                panic!("a bounded search must not wait for a deleting commit held at {boundary:?}")
            }
            Progress::Finished(outcome) => outcome,
        };
        drop(resume);
        committer
            .join()
            .expect("committer thread")
            .expect("the delete completes once released");
        outcome
    });
    let rows = outcome
        .unwrap_or_else(|panic| panic!("the read could not run: {panic}"))
        .unwrap_or_else(|refusal| {
            panic!(
                "a bounded search beside a removal pass held at {boundary:?} must be answered: \
                 {refusal:?}"
            )
        });
    let registered = registrations.0.lock().expect("registrations").clone();
    assert_eq!(
        (rows.clone(), registered.clone()),
        (before, vec![published_before]),
        "beside a deleting commit held at {boundary:?}, whose removal pass is active and whose \
         delete is not yet published, the search reads exactly the published state and its \
         snapshot; it answered {rows:?} having registered {registered:?}"
    );
    assert_eq!(
        answer(&database),
        vec![vec![Value::Int64(2)], vec![Value::Int64(3)]],
        "the released delete is the one the post-commit answer describes"
    );
}

#[test]
fn a_captured_file_root_keeps_the_last_vector_of_a_retired_partition_readable() {
    read_beside_a_removal_pass_held_at(CommitBoundary::BeforeMemoryPublication);
}
