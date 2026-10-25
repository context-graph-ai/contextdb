//! A bounded read's active time measures the read's own work. Waiting for a
//! writer's commit to finish before the read can register its snapshot is
//! not that work. That wait is bounded by the routing allowance the read
//! declares: inside it the read is answered once the commit finishes, past it
//! the read is refused with the commit-boundary wait as its typed cause, and
//! in neither case is it refused for active time. A reader waiting at the
//! boundary is admitted before any committer that started waiting after it.
//!
//! A committer is held inside its commit while a bounded vector search waits
//! for the boundary; the read's own clock is moved at its first failed
//! attempt, then the committer is let go.

use crate::commit_boundary_support::{
    BoundaryEvent, BoundaryParty, as_committer, execute_declaring_routing_allowance,
    is_commit_boundary_wait_exceeded, observe_commit_boundary,
};
use contextdb_core::read_contract::{ReadFailureDetail, ReadFailureLimit, ReadLimits};
use contextdb_core::{SnapshotId, Value};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::local_transport::ManualDeadlineClock;
use contextdb_engine::writer_commit_test_seams::{CommitBoundary, observe as observe_commit};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, mpsc};

/// The read's own active time: shorter than any wait below.
const ACTIVE_MS: u64 = 200;
/// The routing allowance every read here declares.
const ROUTING_MS: u64 = 1_000;
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

/// A store with three vector rows and an unrelated table for commits that
/// leave the search's answer unchanged; returns the uncontended answer.
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

/// Stage a write to the unrelated table and commit it. With `hold`, the
/// commit reports `reached` from inside itself and waits there until the
/// paired resume sender is dropped.
fn commit_note(
    database: &Database,
    id: i64,
    hold: Option<(mpsc::Sender<()>, mpsc::Receiver<()>)>,
) -> contextdb_core::Result<()> {
    let tx = database.begin().expect("begin a write");
    database
        .execute_in_tx(
            tx,
            "INSERT INTO notes VALUES ($id)",
            &HashMap::from([("id".to_owned(), Value::Int64(id))]),
        )
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

/// Run the read declaring the routing allowance; `at_first_wait` runs once,
/// at the read's first failed attempt to take the commit boundary.
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
            || {
                execute_declaring_routing_allowance(database, request, ROUTING_MS)
                    .map(|answer| answer.result.rows().to_vec())
            },
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

/// Hold a committer inside its commit, start the read, move the read's clock
/// to `waited_ms` at its first failed attempt on the boundary, and release
/// the committer either at once (`release_before_answer`) or only after the
/// read has answered. Returns the read's outcome.
fn read_behind_a_held_commit(
    database: &Database,
    request: bounded::BoundedReadRequest,
    clock: &ManualDeadlineClock,
    waited_ms: u64,
    release_before_answer: bool,
) -> ReadOutcome {
    std::thread::scope(|scope| {
        let (reached, reached_rx) = mpsc::channel();
        let (resume, resume_rx) = mpsc::channel::<()>();
        let mut resume = Some(resume);
        let committer = scope.spawn(move || commit_note(database, 1, Some((reached, resume_rx))));
        reached_rx
            .recv()
            .expect("the committer is inside its commit");

        let (progress, progress_rx) = mpsc::channel();
        let waiting = progress.clone();
        let advance = clock.clone();
        scope.spawn(move || {
            let outcome = run_read(database, &request, move || {
                advance.advance_to(waited_ms);
                let _ = waiting.send(Progress::WaitingForCommit);
            });
            let _ = progress.send(Progress::Finished(outcome));
        });
        let outcome = loop {
            match progress_rx.recv().expect("the reader reports its progress") {
                Progress::WaitingForCommit if release_before_answer => drop(resume.take()),
                Progress::WaitingForCommit => {}
                Progress::Finished(outcome) => break outcome,
            }
        };
        drop(resume.take());
        committer
            .join()
            .expect("committer thread")
            .expect("the held commit completes once released");
        outcome
    })
}

#[test]
fn a_bounded_search_waiting_for_a_commit_is_answered_without_spending_its_active_time() {
    let directory = tempfile::tempdir().expect("scratch directory");
    let (database, expected) = store(&directory.path().join("store.db"));
    let clock = ManualDeadlineClock::at(0);
    // Past the whole active time, inside the declared routing allowance.
    let outcome =
        read_behind_a_held_commit(&database, search(&clock), &clock, ROUTING_MS / 2, true);
    let rows = outcome
        .unwrap_or_else(|panic| panic!("the read could not run: {panic}"))
        .unwrap_or_else(|refusal| {
            panic!(
                "a bounded search that waited for a commit inside its routing allowance must be \
                 answered, not refused for active time it spent waiting: {refusal:?}"
            )
        });
    assert_eq!(rows, expected);
}

/// Records whether the read registered a snapshot.
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
fn a_commit_boundary_wait_past_the_routing_allowance_is_refused_as_that_wait() {
    let directory = tempfile::tempdir().expect("scratch directory");
    let (database, _expected) = store(&directory.path().join("store.db"));
    let clock = ManualDeadlineClock::at(0);
    let registrations = Arc::new(Registrations::default());
    let mut request = search(&clock);
    request.probe = Some(Arc::clone(&registrations) as Arc<dyn bounded::ExecutionProbe>);
    // Past the declared routing allowance while the committer still holds
    // the boundary; the committer is released only after the read answers.
    let outcome = read_behind_a_held_commit(&database, request, &clock, ROUTING_MS + 500, false);
    let refusal = match outcome.unwrap_or_else(|panic| panic!("the read could not run: {panic}")) {
        Err(bounded::TestError::Refused(refusal)) => refusal,
        other => panic!(
            "a commit-boundary wait past the declared routing allowance must be refused, while \
             the commit is still held; got {other:?}"
        ),
    };
    assert!(
        !matches!(
            refusal.detail(),
            ReadFailureDetail::OwnerLimitExceeded(detail) if detail.limit == ReadFailureLimit::ActiveMs
        ),
        "a commit-boundary wait is never refused as active time: {refusal:?}"
    );
    assert!(
        is_commit_boundary_wait_exceeded(&refusal),
        "the refusal names the commit-boundary wait as its cause: {refusal:?}"
    );
    assert!(
        registrations.0.lock().expect("registrations").is_empty(),
        "a refused read registers no snapshot"
    );
}

enum Seen {
    Boundary(BoundaryEvent),
    ReaderFinished(ReadOutcome),
}

#[test]
fn a_reader_waiting_at_the_commit_boundary_is_admitted_before_committers_that_wait_after_it() {
    const LATER_COMMITTERS: [usize; 2] = [2, 3];
    let directory = tempfile::tempdir().expect("scratch directory");
    let (database, expected) = store(&directory.path().join("store.db"));
    let clock = ManualDeadlineClock::at(0);
    let (seen, seen_rx) = mpsc::channel();
    let boundary_events = Mutex::new(seen.clone());
    let mut order = Vec::new();
    let answer = observe_commit_boundary(
        &database,
        Arc::new(move |event| {
            let _ = boundary_events
                .lock()
                .expect("boundary sender")
                .send(Seen::Boundary(event));
        }),
        || {
            std::thread::scope(|scope| {
                let mut await_event = |wanted: &dyn Fn(&BoundaryEvent) -> bool| loop {
                    match seen_rx.recv().expect("the boundary reports its events") {
                        Seen::Boundary(event) => {
                            order.push(event);
                            if wanted(&event) {
                                break;
                            }
                        }
                        Seen::ReaderFinished(outcome) => panic!(
                            "the reader finished before the committers it must precede were \
                             waiting: {outcome:?}"
                        ),
                    }
                };
                // Committer 1 holds the boundary inside its commit.
                let (reached, reached_rx) = mpsc::channel();
                let (resume, resume_rx) = mpsc::channel::<()>();
                let first = scope.spawn(|| {
                    as_committer(1, || commit_note(&database, 1, Some((reached, resume_rx))))
                });
                reached_rx.recv().expect("committer 1 is inside its commit");
                // The reader starts waiting for the boundary.
                let request = search(&clock);
                let finished = seen.clone();
                let reading = &database;
                scope.spawn(move || {
                    let outcome = run_read(reading, &request, || {});
                    let _ = finished.send(Seen::ReaderFinished(outcome));
                });
                await_event(&|event| *event == BoundaryEvent::ReaderWaiting);
                // Later committers start waiting after the reader.
                let later: Vec<_> = LATER_COMMITTERS
                    .iter()
                    .map(|&number| {
                        let committing = &database;
                        scope.spawn(move || {
                            as_committer(number, || commit_note(committing, number as i64, None))
                        })
                    })
                    .collect();
                for number in LATER_COMMITTERS {
                    await_event(&|event| *event == BoundaryEvent::CommitterWaiting(number));
                }
                drop(resume);
                first
                    .join()
                    .expect("committer 1 thread")
                    .expect("committer 1 completes once released");
                for committer in later {
                    committer
                        .join()
                        .expect("committer thread")
                        .expect("a later committer completes");
                }
                loop {
                    match seen_rx.recv().expect("the reader reports its answer") {
                        Seen::Boundary(event) => order.push(event),
                        Seen::ReaderFinished(outcome) => break outcome,
                    }
                }
            })
        },
    );
    let rows = answer
        .unwrap_or_else(|panic| panic!("the read could not run: {panic}"))
        .unwrap_or_else(|refusal| panic!("the waiting reader must be answered: {refusal:?}"));
    assert_eq!(rows, expected);
    while let Ok(Seen::Boundary(event)) = seen_rx.try_recv() {
        order.push(event);
    }
    let admitted = |party: BoundaryParty| {
        order
            .iter()
            .position(|event| *event == BoundaryEvent::Admitted(party))
            .unwrap_or_else(|| panic!("the boundary never admitted {party:?}: {order:?}"))
    };
    let reader = admitted(BoundaryParty::Reader);
    for number in LATER_COMMITTERS {
        assert!(
            reader < admitted(BoundaryParty::Committer(number)),
            "the reader waited before committer {number} and must be admitted before it: {order:?}"
        );
    }
}
