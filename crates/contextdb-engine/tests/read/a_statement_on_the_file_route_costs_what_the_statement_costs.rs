//! A read-only consumer's statement costs what the statement costs.
//!
//! Rendering one page of a stored item issues nine bounded statements through
//! a reader that nobody owns, and the whole page has 50 ms to come back. Each
//! of those statements is answered on the caller's own thread, from a file the
//! session already holds open and an image it already loaded. If answering one
//! also starts an operating-system thread and opens a pipe, the page pays for
//! nine thread starts and nine pipes -- setup that on a busy machine costs
//! milliseconds each while the read itself costs microseconds, and that no
//! statement asked for.
//!
//! What is pinned here is therefore the COST SHAPE of a warm statement, not
//! how long one took: over a hundred statements on a session that is already
//! open and already warm, the reader starts no thread and opens no pipe. The
//! counts are per thread, so this proof is honest beside every other test
//! sharing the binary.
//!
//! The two proofs beside it are what the cost shape may not be bought with. A
//! statement that runs past its declared active time still comes back as the
//! typed active-time refusal, and it comes back without waiting on anything.
//! A read that is blocked when another thread withdraws it still stops.
use super::reader_proof_support as fixture;
use contextdb_core::read_contract::{
    DeadlineClock, OwnerReadCancellation, ReadFailureDetail, ReadFailureKind, ReadFailureLimit,
    ReadRoute,
};
use contextdb_core::{Error, Value};
use contextdb_engine::local_transport::ManualDeadlineClock;
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    read_os_setup_for_test, reset_read_os_setup_for_test,
};
use contextdb_engine::read_session::{
    ReadKernelCancellationEvent, ReadKernelSourceEvent, ReadKernelTestObserver,
};
use contextdb_engine::{Database, ReadSession, ReadSessionOptions};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, Barrier, Mutex};

/// Enough statements that one thread or pipe per statement cannot hide, and
/// few enough that the proof stays a unit test.
const STATEMENTS: usize = 100;
/// Statements run before the counting starts, so anything a route sets up once
/// is already set up.
const WARMUP: usize = 5;
/// The active-time ceiling the refusal proof declares and then exceeds.
const ACTIVE_MS: u64 = 20;

fn seed(path: &std::path::Path) {
    let database = Database::open(path).expect("create the committed show fixture");
    database
        .execute(
            "CREATE TABLE shown_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
            &HashMap::new(),
        )
        .expect("declare the shown table");
    for id in 0..16_i64 {
        database
            .execute(
                "INSERT INTO shown_rows (id, payload) VALUES ($id, 'rendered')",
                &HashMap::from([("id".to_owned(), Value::Int64(id))]),
            )
            .expect("seed one shown row");
    }
    database.close().expect("release the seeded store");
}

/// The point read a rendered page issues, nine times over.
fn point_read(session: &ReadSession) {
    let answered = session
        .execute(
            "SELECT payload FROM shown_rows WHERE id = $id",
            &HashMap::from([("id".to_owned(), Value::Int64(7))]),
        )
        .expect("a warm bounded point read on an idle store answers");
    assert_eq!(
        answered.rows(),
        vec![vec![Value::Text("rendered".to_owned())]]
    );
}

#[test]
fn a_hundred_warm_statements_on_the_file_route_start_no_thread_and_open_no_pipe() {
    let directory = tempfile::TempDir::new().expect("task-scoped show-cost directory");
    let path = directory.path().join("responsive-show.db");
    let runtime = fixture::runtime(directory.path());
    seed(&path);

    ReadSession::with_runtime_directory_for_test(&runtime, || {
        let session = ReadSession::open_with_options(&path, ReadSessionOptions::default())
            .expect("an idle store opens on the committed-file route");
        assert_eq!(
            session.route(),
            ReadRoute::File,
            "the cost under test is the one a consumer pays when nobody owns the store"
        );
        for _ in 0..WARMUP {
            point_read(&session);
        }

        reset_read_os_setup_for_test();
        for _ in 0..STATEMENTS {
            point_read(&session);
        }
        let setup = read_os_setup_for_test();

        assert_eq!(
            (setup.deadline_timer_threads, setup.coordination_wake_pipes),
            (0, 0),
            "{STATEMENTS} warm statements started {} operating-system threads and opened {} wake \
             pipes. A statement that never waits needs no thread to wake it and nothing to sleep \
             on, and a rendered page pays whatever one statement pays nine times inside a 50 ms \
             budget.",
            setup.deadline_timer_threads,
            setup.coordination_wake_pipes,
        );
        // What a publication genuinely owes, kept in view beside what it does
        // not: each statement's answer is checked once against the durable
        // erasure frontier, because a result whose tables were erased
        // underneath it is not answerable.
        let candidate_roots = 1
            + usize::from(
                contextdb_engine::persistence::read_persistence_test_scaffold::reader_breadcrumb_runtime_root_for_test()
                    .is_some(),
            );
        assert_eq!(
            (setup.erasure_publications, setup.erasure_frontier_reads),
            (STATEMENTS * candidate_roots, STATEMENTS * candidate_roots),
            "each statement reads and publishes once per known coordination directory: {setup:?}",
        );
    });
}

/// Advances the manual clock past the declared active time from inside the
/// running read, so the refusal is produced by the real kernel rather than by
/// a clock the test moved beforehand.
struct AdvanceOnce {
    clock: Arc<ManualDeadlineClock>,
    armed: AtomicBool,
}

impl ReadKernelTestObserver for AdvanceOnce {
    fn before_source_touch(&self, _: ReadKernelSourceEvent, _: OwnerReadCancellation) {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.clock.advance_to(self.clock.now_ms() + ACTIVE_MS + 1);
        }
    }
    fn cancellation_observed(&self, _: ReadKernelCancellationEvent, _: OwnerReadCancellation) {}
}

#[test]
fn a_statement_past_its_active_time_is_refused_in_the_typed_words_without_waiting_for_anything() {
    let directory = tempfile::TempDir::new().expect("task-scoped active-time directory");
    let path = directory.path().join("expiring-show.db");
    let runtime = fixture::runtime(directory.path());
    seed(&path);

    let clock = Arc::new(ManualDeadlineClock::default());
    let advance = Arc::new(AdvanceOnce {
        clock: Arc::clone(&clock),
        armed: AtomicBool::new(false),
    });
    let mut options = ReadSessionOptions::default();
    options.limits.active_ms = ACTIVE_MS;

    let session = ReadSession::with_runtime_directory_for_test(&runtime, || {
        ReadSession::open_with_observations_for_test(
            &path,
            options,
            Arc::clone(&clock) as Arc<dyn DeadlineClock>,
            None,
            Some(Arc::clone(&advance) as Arc<dyn ReadKernelTestObserver>),
        )
    })
    .expect("an idle store opens on the committed-file route");
    assert_eq!(session.route(), ReadRoute::File);
    point_read(&session);

    advance.armed.store(true, Ordering::SeqCst);
    reset_read_os_setup_for_test();
    let refused = session.execute(
        "SELECT payload FROM shown_rows WHERE id = $id",
        &HashMap::from([("id".to_owned(), Value::Int64(7))]),
    );
    let setup = read_os_setup_for_test();

    assert!(
        !advance.armed.load(Ordering::SeqCst),
        "the real kernel reached the clock advance"
    );
    let Err(Error::ReadFailure(failure)) = refused else {
        panic!("a statement past its active time keeps its typed refusal, got {refused:?}");
    };
    assert_eq!(failure.kind(), ReadFailureKind::OwnerLimitExceeded);
    let ReadFailureDetail::OwnerLimitExceeded(detail) = failure.detail() else {
        panic!("an active-time refusal carries typed active-time detail");
    };
    assert_eq!(detail.limit, ReadFailureLimit::ActiveMs);
    assert_eq!(detail.value, ACTIVE_MS);
    assert_eq!(
        (setup.deadline_timer_threads, setup.coordination_wake_pipes),
        (0, 0),
        "the refusal was produced by waiting on something ({setup:?}) rather than by reading the \
         budget, so a caller learns it is over its limit only after the wait it has already \
         exceeded",
    );

    point_read(&session);
}

/// Parks the running read on its first touch of the store and holds it there
/// until the journey says otherwise, so the withdrawal below reaches a read
/// that is genuinely blocked rather than one that has already finished.
struct ParkOnFirstTouch {
    reached: Mutex<Option<SyncSender<()>>>,
    released: Arc<Barrier>,
}

impl ReadKernelTestObserver for ParkOnFirstTouch {
    fn before_source_touch(&self, _: ReadKernelSourceEvent, _: OwnerReadCancellation) {
        let reached = self
            .reached
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(reached) = reached {
            reached.send(()).expect("the journey is waiting to be told");
            self.released.wait();
        }
    }
    fn cancellation_observed(&self, _: ReadKernelCancellationEvent, _: OwnerReadCancellation) {}
}

#[test]
fn a_blocked_read_still_stops_when_another_thread_withdraws_it() {
    let directory = tempfile::TempDir::new().expect("task-scoped withdrawal directory");
    let path = directory.path().join("withdrawn-show.db");
    let runtime = fixture::runtime(directory.path());
    seed(&path);

    let (tell, told) = sync_channel(1);
    let released = Arc::new(Barrier::new(2));
    let park = Arc::new(ParkOnFirstTouch {
        reached: Mutex::new(Some(tell)),
        released: Arc::clone(&released),
    });
    let session = ReadSession::with_runtime_directory_for_test(&runtime, || {
        ReadSession::open_with_kernel_observer_for_test(
            &path,
            ReadSessionOptions::default(),
            Arc::clone(&park) as Arc<dyn ReadKernelTestObserver>,
        )
    })
    .expect("an idle store opens on the committed-file route");
    assert_eq!(session.route(), ReadRoute::File);

    let cancellation = OwnerReadCancellation::new();
    std::thread::scope(|reads| {
        let reading = reads.spawn(|| {
            session.execute_with_cancellation(
                "SELECT payload FROM shown_rows ORDER BY id",
                &HashMap::new(),
                &cancellation,
            )
        });
        told.recv().expect("the read reaches the store and parks");
        // The read is parked inside the kernel on the other thread. The
        // withdrawal below is therefore made against a read in flight.
        cancellation.cancel();
        released.wait();
        let refused = reading.join().expect("the withdrawn read returns");
        assert!(
            matches!(refused, Err(Error::ReadCancelled)),
            "a read withdrawn from another thread while it was blocked reported {refused:?} \
             instead of stopping"
        );
    });
}
