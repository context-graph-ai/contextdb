//! A bounded read never waits for a writer's commit to register its
//! snapshot: like a plain statement, a bounded vector or graph read registers
//! the snapshot already published, so commits in progress neither delay it
//! nor spend its active time, and it still reads one committed snapshot.
//!
//! A committer is held inside its commit (later committers queue behind it)
//! while a bounded vector search runs; the engine's commit-wait hook reports
//! any failed attempt by the read to take the commit boundary.

use contextdb_core::read_contract::ReadLimits;
use contextdb_core::{SnapshotId, Value};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::local_transport::ManualDeadlineClock;
use contextdb_engine::writer_commit_test_seams::{CommitBoundary, observe as observe_commit};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, mpsc};

/// The read's own active time: shorter than the wait in the first test.
const ACTIVE_MS: u64 = 200;
const SEARCH: &str = "SELECT id FROM items ORDER BY embedding <=> $probe LIMIT 2";

fn search(clock: &ManualDeadlineClock) -> bounded::BoundedReadRequest {
    bounded::BoundedReadRequest::new(
        SEARCH,
        HashMap::from([("probe".to_owned(), Value::Vector(vec![1.0, 0.0, 0.0]))]),
        ReadLimits {
            active_ms: ACTIVE_MS,
            ..ReadLimits::default()
        },
        Arc::new(clock.clone()),
    )
}

/// A store with three vector rows and an unrelated table; returns the
/// uncontended answer.
fn store(path: &std::path::Path) -> (Database, Vec<Vec<Value>>) {
    let database = Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the store");
    for statement in [
        "CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR(3))",
        "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
    ] {
        database
            .execute(statement, &HashMap::new())
            .expect("create table");
    }
    for (id, vector) in [
        (1i64, vec![1.0, 0.0, 0.0]),
        (2, vec![0.9, 0.1, 0.0]),
        (3, vec![0.0, 0.0, 1.0]),
    ] {
        database
            .execute(
                "INSERT INTO items VALUES ($id, $vector)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    ("vector".to_owned(), Value::Vector(vector)),
                ]),
            )
            .expect("commit a vector row");
    }
    let expected = bounded::execute(&database, &search(&ManualDeadlineClock::at(0)))
        .expect("the search answers when no commit is in progress")
        .result
        .rows()
        .to_vec();
    assert_eq!(expected, vec![vec![Value::Int64(1)], vec![Value::Int64(2)]]);
    (database, expected)
}

/// Commit one staged statement. With `hold`, the commit reports `reached`
/// from inside itself and waits there until the paired resume sender is
/// dropped.
fn commit_statement(
    database: &Database,
    sql: &str,
    params: HashMap<String, Value>,
    hold: Option<(mpsc::Sender<()>, mpsc::Receiver<()>)>,
) -> contextdb_core::Result<()> {
    let tx = database.begin().expect("begin a write");
    database
        .execute_in_tx(tx, sql, &params)
        .expect("stage a write");
    let Some((reached, resume)) = hold else {
        return database.commit(tx);
    };
    let reached = Mutex::new(Some(reached));
    let resume = Mutex::new(resume);
    observe_commit(
        Arc::new(move |point| {
            if point == CommitBoundary::BeforeRedb
                && let Some(reached) = reached.lock().expect("reached sender").take()
            {
                let _ = reached.send(());
                let _ = resume.lock().expect("resume receiver").recv();
            }
        }),
        || database.commit(tx),
    )
}

/// The outcome of one read: its answer or refusal, or the text of a panic.
type ReadOutcome = Result<Result<Vec<Vec<Value>>, bounded::TestError>, String>;

/// Run the read; `at_first_wait` runs once, at the read's first failed
/// attempt to take the commit boundary.
fn run_read(
    database: &Database,
    request: &bounded::BoundedReadRequest,
    at_first_wait: impl FnOnce() + Send + 'static,
) -> ReadOutcome {
    let first = Mutex::new(Some(at_first_wait));
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        contextdb_tx::commit_wait_test_seams::observe(
            Arc::new(move || {
                if let Some(at_first_wait) = first.lock().expect("first wait").take() {
                    at_first_wait();
                }
            }),
            || bounded::execute(database, request).map(|answer| answer.result.rows().to_vec()),
        )
    }))
    .map_err(|panic| {
        panic
            .downcast_ref::<&str>()
            .map(|text| (*text).to_owned())
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "a non-text panic".to_owned())
    })
}

enum Progress {
    WaitingForCommit,
    Finished(ReadOutcome),
}

fn note(id: i64) -> HashMap<String, Value> {
    HashMap::from([("id".to_owned(), Value::Int64(id))])
}

#[test]
fn a_bounded_search_waiting_for_a_commit_is_answered_without_spending_its_active_time() {
    let directory = tempfile::tempdir().expect("scratch directory");
    let (database, expected) = store(&directory.path().join("store.db"));
    let clock = ManualDeadlineClock::at(0);
    let outcome = std::thread::scope(|scope| {
        let (reached, reached_rx) = mpsc::channel();
        let (resume, resume_rx) = mpsc::channel::<()>();
        let mut resume = Some(resume);
        let committing = &database;
        let committer = scope.spawn(move || {
            commit_statement(
                committing,
                "INSERT INTO notes VALUES ($id)",
                note(1),
                Some((reached, resume_rx)),
            )
        });
        reached_rx
            .recv()
            .expect("the committer is inside its commit");

        let (progress, progress_rx) = mpsc::channel();
        let waiting = progress.clone();
        let advance = clock.clone();
        let request = search(&clock);
        let reading = &database;
        scope.spawn(move || {
            let outcome = run_read(reading, &request, move || {
                // The wait has outlasted the read's whole active time.
                advance.advance_to(ACTIVE_MS + 300);
                let _ = waiting.send(Progress::WaitingForCommit);
            });
            let _ = progress.send(Progress::Finished(outcome));
        });
        // A read that waits is released by letting the committer finish; a
        // read that never waits answers on its own.
        let outcome = loop {
            match progress_rx.recv().expect("the reader reports its progress") {
                Progress::WaitingForCommit => drop(resume.take()),
                Progress::Finished(outcome) => break outcome,
            }
        };
        drop(resume.take());
        committer
            .join()
            .expect("committer thread")
            .expect("the held commit completes once released");
        outcome
    });
    let rows = outcome
        .unwrap_or_else(|panic| panic!("the read could not run: {panic}"))
        .unwrap_or_else(|refusal| {
            panic!(
                "a bounded search must be answered, never refused for active time it spent \
                 waiting for a commit: {refusal:?}"
            )
        });
    assert_eq!(rows, expected);
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

#[test]
fn a_bounded_search_under_back_to_back_committers_never_waits_and_reads_one_committed_snapshot() {
    let directory = tempfile::tempdir().expect("scratch directory");
    let (database, expected) = store(&directory.path().join("store.db"));
    let published = database.snapshot();
    let registrations = Arc::new(Registrations::default());
    let clock = ManualDeadlineClock::at(0);
    let mut request = search(&clock);
    request.probe = Some(Arc::clone(&registrations) as Arc<dyn bounded::ExecutionProbe>);
    let outcome = std::thread::scope(|scope| {
        // Committer 1 is held inside a commit that would change the search's
        // answer; committers 2 and 3 queue behind it with further changes.
        // Dropping `resume` (including by a panic below) lets them all finish.
        let (reached, reached_rx) = mpsc::channel();
        let (resume, resume_rx) = mpsc::channel::<()>();
        let committing = &database;
        let mut committers = vec![scope.spawn(move || {
            commit_statement(
                committing,
                "UPDATE items SET embedding = $vector WHERE id = 1",
                HashMap::from([("vector".to_owned(), Value::Vector(vec![0.0, 1.0, 0.0]))]),
                Some((reached, resume_rx)),
            )
        })];
        reached_rx.recv().expect("committer 1 is inside its commit");
        committers.push(scope.spawn(move || {
            commit_statement(
                committing,
                "UPDATE items SET embedding = $vector WHERE id = 2",
                HashMap::from([("vector".to_owned(), Value::Vector(vec![0.0, 0.0, 1.0]))]),
                None,
            )
        }));
        committers.push(scope.spawn(move || {
            commit_statement(
                committing,
                "INSERT INTO items VALUES (4, $vector)",
                HashMap::from([("vector".to_owned(), Value::Vector(vec![1.0, 0.0, 0.0]))]),
                None,
            )
        }));

        let (progress, progress_rx) = mpsc::channel();
        let waiting = progress.clone();
        let reading = &database;
        scope.spawn(move || {
            let outcome = run_read(reading, &request, move || {
                let _ = waiting.send(Progress::WaitingForCommit);
            });
            let _ = progress.send(Progress::Finished(outcome));
        });
        let outcome = match progress_rx.recv().expect("the reader reports its progress") {
            Progress::WaitingForCommit => panic!(
                "a bounded search must register the published snapshot without waiting for the \
                 commit boundary; it tried to take the boundary while committers held it"
            ),
            Progress::Finished(outcome) => outcome,
        };
        drop(resume);
        for committer in committers {
            committer
                .join()
                .expect("committer thread")
                .expect("a queued commit completes once the held one is released");
        }
        outcome
    });
    let rows = outcome
        .unwrap_or_else(|panic| panic!("the read could not run: {panic}"))
        .unwrap_or_else(|refusal| {
            panic!("a bounded search under committers must be answered: {refusal:?}")
        });
    assert_eq!(
        rows, expected,
        "the search reads the snapshot published before the held commit, untouched by any \
         commit in progress"
    );
    assert_eq!(
        registrations.0.lock().expect("registrations").as_slice(),
        &[published],
        "the search registers exactly the snapshot that was published when it started"
    );
}
