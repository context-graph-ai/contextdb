//! After a writer reopen, a read session selects its route once and then
//! reads like any other session: later statements do not select a route
//! again, and the wait for the new writer is bounded by the session's routing
//! allowance, never charged to a statement's active time. Every session
//! operation goes through that same selection after a reopen: asking what the
//! session has charged, or sending the owner a request, reaches the new route
//! and never the departed writer's channel.

use contextdb_core::Value;
use contextdb_core::read_contract::{
    OwnerReadCancellation, OwnerRequestHandler, ReadClientTimeouts, ReadLimits, ReadRoute,
};
use contextdb_engine::local_transport::ManualDeadlineClock;
use contextdb_engine::persistence::read_persistence_test_scaffold::observe_writer_reader_snapshots_for_test;
use contextdb_engine::read_session::{ReadSessionEvent, ReadSessionTestObserver};
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, ReadSession, ReadSessionOptions,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};

const STARTED_MS: u64 = 10_000;
const SELECT: &str = "SELECT id FROM items ORDER BY id";

fn seeded_writer(path: &Path) -> (Database, Vec<Vec<Value>>) {
    let writer = Database::open(path).expect("open the first writer");
    writer
        .execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("create table");
    for id in 0..4i64 {
        writer
            .execute(
                "INSERT INTO items (id) VALUES ($id)",
                &HashMap::from([("id".to_owned(), Value::Int64(id))]),
            )
            .expect("commit row");
    }
    (writer, (0..4i64).map(|id| vec![Value::Int64(id)]).collect())
}

/// Records every route-selection event while armed.
#[derive(Default)]
struct RouteSelections {
    armed: AtomicBool,
    seen: Mutex<Vec<ReadSessionEvent>>,
}

impl RouteSelections {
    fn count_from_now(&self) {
        self.seen.lock().expect("selection events").clear();
        self.armed.store(true, Ordering::SeqCst);
    }

    fn taken(&self) -> Vec<ReadSessionEvent> {
        self.armed.store(false, Ordering::SeqCst);
        std::mem::take(&mut *self.seen.lock().expect("selection events"))
    }
}

impl ReadSessionTestObserver for RouteSelections {
    fn observe_event(&self, event: ReadSessionEvent) {
        let selecting = matches!(
            event,
            ReadSessionEvent::OwnerResolution { .. }
                | ReadSessionEvent::LocalChannelOperation
                | ReadSessionEvent::BeforeDirectBackendOpen { .. }
                | ReadSessionEvent::DirectBackendOpen
                | ReadSessionEvent::ClaimWindowWait { .. }
                | ReadSessionEvent::RouteOpenFailed { .. }
                | ReadSessionEvent::OwnershipRaceRetry { .. }
                | ReadSessionEvent::RouteSelected(_)
        );
        if selecting && self.armed.load(Ordering::SeqCst) {
            self.seen.lock().expect("selection events").push(event);
        }
    }
}

fn session_counting_selections(path: &Path) -> (ReadSession, Arc<RouteSelections>) {
    let selections = Arc::new(RouteSelections::default());
    let session = ReadSession::open_with_observations_for_test(
        path,
        ReadSessionOptions::default(),
        Arc::new(ManualDeadlineClock::at(STARTED_MS)),
        Some(Arc::clone(&selections) as Arc<dyn ReadSessionTestObserver>),
        None,
    )
    .expect("open a session beside the first writer");
    assert_eq!(session.route(), ReadRoute::Owner);
    (session, selections)
}

fn rows(session: &ReadSession, what: &str) -> Vec<Vec<Value>> {
    session
        .execute(SELECT, &HashMap::new())
        .unwrap_or_else(|refusal| panic!("{what} must read the committed rows: {refusal}"))
        .rows()
        .to_vec()
}

/// Statements before any reopen select nothing, so the events counted here
/// are route selection and nothing else.
fn assert_steady_statements_select_nothing(
    session: &ReadSession,
    selections: &RouteSelections,
    committed: &[Vec<Value>],
    after: &str,
) {
    selections.count_from_now();
    assert_eq!(
        rows(session, &format!("the second statement {after}")),
        committed
    );
    session.charged_bytes().unwrap_or_else(|refusal| {
        panic!("the session's charge {after} must be answered: {refusal}")
    });
    assert_eq!(
        rows(session, &format!("the third statement {after}")),
        committed
    );
    let selected = selections.taken();
    assert!(
        selected.is_empty(),
        "statements {after} must read through the route the session already selected, without \
         selecting a route again; they emitted {selected:?}"
    );
}

#[test]
fn after_a_writer_reopen_later_statements_use_the_route_the_session_selected_to_the_new_writer() {
    let directory = tempfile::tempdir().expect("scratch directory");
    let path = directory.path().join("store.db");
    let (first, committed) = seeded_writer(&path);
    let (session, selections) = session_counting_selections(&path);
    assert_eq!(rows(&session, "the first statement"), committed);
    assert_steady_statements_select_nothing(
        &session,
        &selections,
        &committed,
        "beside the first writer",
    );

    first.close().expect("close the first writer");
    let replacement = Database::open(&path).expect("reopen the store for writing");
    let charged = session.charged_bytes();
    assert!(
        charged.is_ok(),
        "after the reopen, the session's charge is answered through its new route, not the \
         departed writer's channel: {charged:?}"
    );
    assert_eq!(
        rows(&session, "the first statement after the reopen"),
        committed
    );
    assert_steady_statements_select_nothing(
        &session,
        &selections,
        &committed,
        "after the session selected its route to the replacement writer",
    );
    drop(session);
    replacement.close().expect("close the replacement writer");
}

#[test]
fn after_its_writer_closes_later_statements_use_the_file_route_the_session_selected() {
    let directory = tempfile::tempdir().expect("scratch directory");
    let path = directory.path().join("store.db");
    let (first, committed) = seeded_writer(&path);
    let (session, selections) = session_counting_selections(&path);
    assert_eq!(rows(&session, "the first statement"), committed);
    assert_steady_statements_select_nothing(
        &session,
        &selections,
        &committed,
        "beside the first writer",
    );

    first.close().expect("close the only writer");
    assert_eq!(
        rows(&session, "the first statement after the writer closed"),
        committed
    );
    assert_steady_statements_select_nothing(
        &session,
        &selections,
        &committed,
        "after the session selected the file route",
    );
}

enum ReaderProgress {
    /// The re-selection reached its direct backend open and is held there.
    AtDirectOpen,
    /// The re-selecting statement is waiting for the opening writer.
    WaitingForWriter,
    /// The statement finished, served or refused.
    Finished(Result<Vec<Vec<Value>>, String>),
}

/// Holds the first direct backend open after it is armed and reports the
/// first claim-window wait that follows it.
struct HoldDirectOpenThenReportClaimWait {
    armed: AtomicBool,
    progress: Mutex<Option<mpsc::Sender<ReaderProgress>>>,
    resume: Mutex<mpsc::Receiver<()>>,
}

impl ReadSessionTestObserver for HoldDirectOpenThenReportClaimWait {
    fn observe_event(&self, event: ReadSessionEvent) {
        match event {
            ReadSessionEvent::BeforeDirectBackendOpen { .. }
                if self.armed.swap(false, Ordering::SeqCst) =>
            {
                if let Some(progress) = self.progress.lock().expect("progress sender").as_ref() {
                    let _ = progress.send(ReaderProgress::AtDirectOpen);
                }
                let _ = self.resume.lock().expect("reader resume receiver").recv();
            }
            ReadSessionEvent::ClaimWindowWait { .. } => {
                if let Some(progress) = self.progress.lock().expect("progress sender").take() {
                    let _ = progress.send(ReaderProgress::WaitingForWriter);
                }
            }
            _ => {}
        }
    }
}

#[test]
fn a_reselection_wait_inside_the_routing_allowance_never_spends_a_statements_active_time() {
    const ACTIVE_MS: u64 = 500;
    const ROUTING_MS: u64 = 1_000;
    let directory = tempfile::tempdir().expect("scratch directory");
    let path = directory.path().join("store.db");
    let (first, committed) = seeded_writer(&path);

    let options = ReadSessionOptions {
        limits: ReadLimits {
            active_ms: ACTIVE_MS,
            ..ReadLimits::default()
        },
        timeouts: ReadClientTimeouts {
            routing_retry_ms: ROUTING_MS,
            ..ReadClientTimeouts::default()
        },
        ..ReadSessionOptions::default()
    };
    let clock = ManualDeadlineClock::at(STARTED_MS);
    let (progress, progress_rx) = mpsc::channel();
    let (reader_resume, reader_resume_rx) = mpsc::channel::<()>();
    let observer = Arc::new(HoldDirectOpenThenReportClaimWait {
        armed: AtomicBool::new(false),
        progress: Mutex::new(Some(progress.clone())),
        resume: Mutex::new(reader_resume_rx),
    });
    let session = ReadSession::open_with_observations_for_test(
        &path,
        options,
        Arc::new(clock.clone()),
        Some(Arc::clone(&observer) as Arc<dyn ReadSessionTestObserver>),
        None,
    )
    .expect("open a session beside the first writer");
    assert_eq!(session.route(), ReadRoute::Owner);
    assert_eq!(rows(&session, "the first statement"), committed);
    first.close().expect("close the first writer");
    observer.armed.store(true, Ordering::SeqCst);

    let answer = std::thread::scope(|scope| {
        // Dropping either resume sender (including by a panic below) lets
        // the reader or the replacement writer go.
        let reader_resume = reader_resume;
        let finished = progress;
        let reading = &session;
        scope.spawn(move || {
            let answer = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                reading
                    .execute(SELECT, &HashMap::new())
                    .map(|result| result.rows().to_vec())
                    .map_err(|error| error.to_string())
            }))
            .unwrap_or_else(|_| Err("the statement panicked".to_owned()));
            let _ = finished.send(ReaderProgress::Finished(answer));
        });
        match progress_rx.recv().expect("the reader reports its progress") {
            ReaderProgress::AtDirectOpen => {}
            other => panic!(
                "after its writer closed, the session must re-select its route through the \
                 direct backend open; it reported {}",
                describe(&other)
            ),
        }

        // The replacement writer takes the store and is held inside its open,
        // holding the store and not yet serving.
        let (reached, reached_rx) = mpsc::channel::<()>();
        let (writer_resume, writer_resume_rx) = mpsc::channel::<()>();
        let reached = Mutex::new(reached);
        let writer_resume_rx = Mutex::new(writer_resume_rx);
        let opening = scope.spawn(|| {
            observe_writer_reader_snapshots_for_test(
                Arc::new(move |_registry| {
                    let _ = reached.lock().expect("writer reached sender").send(());
                    let _ = writer_resume_rx
                        .lock()
                        .expect("writer resume receiver")
                        .recv();
                }),
                || Database::open(&path),
            )
        });
        reached_rx
            .recv()
            .expect("the replacement writer holds the store before it serves");

        drop(reader_resume);
        match progress_rx.recv().expect("the reader reports its progress") {
            ReaderProgress::WaitingForWriter => {}
            other => panic!(
                "a session that meets an opening replacement writer must wait for it inside its \
                 routing allowance; it reported {}",
                describe(&other)
            ),
        }
        // The wait has outlasted the statement's whole active time but is
        // still inside the routing allowance.
        clock.advance_to(STARTED_MS + ACTIVE_MS + 100);
        drop(writer_resume);
        let replacement = opening
            .join()
            .expect("writer thread")
            .expect("the replacement writer opens once resumed");
        let answer = match progress_rx.recv().expect("the reader reports its answer") {
            ReaderProgress::Finished(answer) => answer,
            other => panic!("the reader reported {} after its wait", describe(&other)),
        };
        (answer, replacement)
    });
    let (answer, replacement) = answer;
    let rows = answer.unwrap_or_else(|refusal| {
        panic!(
            "a session whose routing wait stayed inside its routing allowance must be answered, \
             not refused for active time it never spent reading: {refusal}"
        )
    });
    assert_eq!(rows, committed);
    drop(session);
    replacement.close().expect("close the replacement writer");
}

fn describe(progress: &ReaderProgress) -> String {
    match progress {
        ReaderProgress::AtDirectOpen => "its direct backend open".to_owned(),
        ReaderProgress::WaitingForWriter => "a claim-window wait".to_owned(),
        ReaderProgress::Finished(answer) => format!("an answer: {answer:?}"),
    }
}

struct Echo;

impl OwnerRequestHandler for Echo {
    fn handle(
        &self,
        _namespace: &str,
        request: &[u8],
        _cancellation: &OwnerReadCancellation,
    ) -> contextdb_core::Result<Vec<u8>> {
        Ok(request.to_vec())
    }
}

#[test]
fn after_a_writer_reopen_an_owner_request_reaches_the_new_writer() {
    let directory = tempfile::tempdir().expect("scratch directory");
    let path = directory.path().join("store.db");
    let (first, committed) = seeded_writer(&path);
    let session = ReadSession::open(&path).expect("open a session beside the first writer");
    assert_eq!(session.route(), ReadRoute::Owner);
    assert_eq!(rows(&session, "the first statement"), committed);

    first.close().expect("close the first writer");
    let replacement = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                handler: Some(Arc::new(Echo)),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("reopen the store for writing with a request handler");
    let reply = session
        .request_owner("echo", b"after the reopen")
        .unwrap_or_else(|refusal| {
            panic!(
                "after the reopen, an owner request reaches the new writer, not the departed \
                 writer's channel: {refusal}"
            )
        });
    assert_eq!(reply, b"after the reopen".to_vec());
    drop(session);
    replacement.close().expect("close the replacement writer");
}
