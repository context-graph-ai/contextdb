//! A bounded read's active time measures the read's own work. Waiting for a
//! writer's commit to finish before the read can register its snapshot is
//! not that work: a read that waits behind a commit is answered once the
//! commit finishes, however long the wait took, and is never refused for
//! active time it spent waiting.
//!
//! A committer is held inside its commit; a bounded vector search starts and
//! waits for the commit boundary. At its first failed attempt the read's own
//! clock is moved past its whole active time, but not past the default
//! routing allowance, then the committer is let go.

use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::local_transport::ManualDeadlineClock;
use contextdb_engine::writer_commit_test_seams::{CommitBoundary, observe as observe_commit};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, mpsc};

const ACTIVE_MS: u64 = 200;
/// Inside the default routing allowance (1 000 ms), past the active time.
const WAITED_MS: u64 = 500;
const SEARCH: &str = "SELECT id FROM items ORDER BY embedding <=> $probe LIMIT 2";

fn limits() -> ReadLimits {
    ReadLimits {
        active_ms: ACTIVE_MS,
        ..ReadLimits::default()
    }
}

fn search_params() -> HashMap<String, Value> {
    HashMap::from([("probe".to_owned(), Value::Vector(vec![1.0, 0.0, 0.0]))])
}

enum ReaderProgress {
    /// The read failed to take the commit boundary and is waiting for it.
    WaitingForCommit,
    /// The read finished, answered or refused.
    Finished(Result<Vec<Vec<Value>>, String>),
}

#[test]
fn a_bounded_search_waiting_for_a_commit_is_answered_without_spending_its_active_time() {
    let directory = tempfile::tempdir().expect("scratch directory");
    let database = Database::open_with_options(
        directory.path().join("store.db"),
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
    let expected = bounded::execute(
        &database,
        &bounded::BoundedReadRequest::new(
            SEARCH,
            search_params(),
            limits(),
            Arc::new(ManualDeadlineClock::at(0)),
        ),
    )
    .expect("the search answers when no commit is in progress")
    .result
    .rows()
    .to_vec();
    assert_eq!(expected, vec![vec![Value::Int64(1)], vec![Value::Int64(2)]]);

    let clock = ManualDeadlineClock::at(0);
    let answer = std::thread::scope(|scope| {
        // Hold a committer inside its commit. Its write touches another
        // table, so the search's answer is the same before and after it.
        // Dropping `resume` (including by a panic below) lets it finish.
        let (reached, reached_rx) = mpsc::channel::<()>();
        let (resume, resume_rx) = mpsc::channel::<()>();
        let reached = Mutex::new(Some(reached));
        let resume_rx = Mutex::new(resume_rx);
        let writing = &database;
        let committer = scope.spawn(move || {
            let tx = writing.begin().expect("begin a write");
            writing
                .execute_in_tx(tx, "INSERT INTO notes VALUES (1)", &HashMap::new())
                .expect("stage a write");
            observe_commit(
                Arc::new(move |point| {
                    if point == CommitBoundary::BeforeRedb
                        && let Some(reached) = reached.lock().expect("reached sender").take()
                    {
                        let _ = reached.send(());
                        let _ = resume_rx.lock().expect("resume receiver").recv();
                    }
                }),
                || writing.commit(tx),
            )
        });
        reached_rx
            .recv()
            .expect("the committer is inside its commit");

        let (progress, progress_rx) = mpsc::channel();
        let waiting = Mutex::new(Some(progress.clone()));
        let read_clock = clock.clone();
        let advance_clock = clock.clone();
        let reading = &database;
        scope.spawn(move || {
            let answer = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                contextdb_tx::commit_wait_test_seams::observe(
                    Arc::new(move || {
                        if let Some(waiting) = waiting.lock().expect("waiting sender").take() {
                            // The wait has outlasted the read's whole active time.
                            advance_clock.advance_to(WAITED_MS);
                            let _ = waiting.send(ReaderProgress::WaitingForCommit);
                        }
                    }),
                    || {
                        bounded::execute(
                            reading,
                            &bounded::BoundedReadRequest::new(
                                SEARCH,
                                search_params(),
                                limits(),
                                Arc::new(read_clock.clone()),
                            ),
                        )
                    },
                )
                .map(|answer| answer.result.rows().to_vec())
                .map_err(|refusal| format!("{refusal:?}"))
            }))
            .unwrap_or_else(|_| Err("the read panicked".to_owned()));
            let _ = progress.send(ReaderProgress::Finished(answer));
        });

        let answer = match progress_rx.recv().expect("the reader reports its progress") {
            // A read that never had to wait is answered on its own terms.
            ReaderProgress::Finished(answer) => {
                drop(resume);
                answer
            }
            ReaderProgress::WaitingForCommit => {
                drop(resume);
                loop {
                    match progress_rx.recv().expect("the reader reports its answer") {
                        ReaderProgress::Finished(answer) => break answer,
                        ReaderProgress::WaitingForCommit => continue,
                    }
                }
            }
        };
        committer
            .join()
            .expect("committer thread")
            .expect("the held commit completes once released");
        answer
    });

    let rows = answer.unwrap_or_else(|refusal| {
        panic!(
            "a bounded search that waited for a commit must be answered, not refused for active \
             time it spent waiting: {refusal}"
        )
    });
    assert_eq!(rows, expected);
}
