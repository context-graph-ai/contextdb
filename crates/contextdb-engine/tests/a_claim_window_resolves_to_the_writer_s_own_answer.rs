#![cfg(all(unix, feature = "test-seams"))]
//! A caller that arrives while a writer is claiming a store gets that writer's
//! real answer, inside the caller's own declared budget.
//!
//! The companion journey next door pins what happens when the budget runs out.
//! This one pins the ordinary case, which is the one consumers actually live
//! in: a supervisor starts the writer and immediately asks about it. The writer
//! publishes a moment later, and the ask must come back with what the writer
//! decided -- serving, serving-disabled, or the startup failure it hit -- rather
//! than a stale verdict formed before the writer had decided anything.
//!
//! Two things make that provable without measuring a clock and without a sleep
//! anywhere. The writer is parked between claiming the store and publishing its
//! decision, so the window is genuinely open. And the caller announces that it
//! has entered the window, so the writer is released AT that moment, on the
//! caller's own thread -- the publication is inside the caller's wait by
//! construction, not by luck.
//!
//! -------------------------------------------------------------------------
//! SEAMS THIS JOURNEY NEEDS, VERBATIM
//!
//! Neither exists yet, so this file is compile-RED until they land. Both are
//! test-seam doors onto production route code; neither changes what a shipped
//! build does.
//!
//! 1. In `crates/contextdb-engine/src/read_session.rs`, in
//!    `mod route_observation`, add this variant to `enum ReadSessionEvent`:
//!
//! ```ignore
//!         /// Emitted by an owner-only selection that found a live claim on
//!         /// this store whose holder has not published a serving decision
//!         /// yet, immediately before the caller waits for that decision
//!         /// inside its own declared `routing_retry_ms`. `attempt` is the
//!         /// selection attempt that observed the claim. A test observer may
//!         /// block here, which is what lets a proof drive the writer's
//!         /// publication against a caller that is provably already waiting.
//!         ClaimWindowWait {
//!             attempt: u64,
//!         },
//! ```
//!
//! 2. In the same file, in `impl ReadSession`, beside
//!    `open_with_observer_for_test`:
//!
//! ```ignore
//!     /// The owner-only door, opened through the production route selector
//!     /// with deterministic route observation enabled.
//!     #[cfg(feature = "test-seams")]
//!     #[doc(hidden)]
//!     pub fn open_owner_only_with_observer_for_test(
//!         path: impl AsRef<Path>,
//!         options: ReadSessionOptions,
//!         observer: Arc<dyn ReadSessionTestObserver>,
//!     ) -> Result<Self> {
//!         Self::select(
//!             path.as_ref(),
//!             options,
//!             None,
//!             RouteRequirement::OwnerOnly,
//!             Some(observer),
//!             None,
//!             None,
//!             None,
//!         )
//!     }
//! ```
//! -------------------------------------------------------------------------

use contextdb_core::read_contract::{
    OwnerReadCancellation, OwnerReadLimits, OwnerServiceTimeouts, OwnerServingState,
    ReadClientTimeouts, ReadFailureDetail, ReadFailureKind, ReadLimits, ReadRoute,
};
use contextdb_core::{Error, Result, Value};
use contextdb_engine::read_session::{ReadSessionEvent, ReadSessionTestObserver};
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, OwnerRequestHandler, ReadSession,
    ReadSessionOptions,
};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

/// Failsafe only. Nothing here asserts how long anything took.
const WATCHDOG: Duration = Duration::from_secs(60);

/// The budget this caller declares. It is deliberately roomy: the point of
/// these journeys is that a writer publishing INSIDE the caller's budget yields
/// the writer's real answer, so the budget must not be what ends the wait.
const DECLARED_ROUTING_RETRY_MS: u64 = 30_000;

struct EchoHandler;

impl OwnerRequestHandler for EchoHandler {
    fn handle(
        &self,
        _namespace: &str,
        request: &[u8],
        _cancellation: &OwnerReadCancellation,
    ) -> Result<Vec<u8>> {
        Ok(request.to_vec())
    }
}

fn secure_runtime_root(directory: &Path, name: &str) -> PathBuf {
    let root = directory.join(name);
    std::fs::create_dir(&root).expect("create the task-scoped runtime root");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure the task-scoped runtime root");
    std::fs::canonicalize(&root).expect("resolve the root the way both sides will")
}

fn session_options() -> ReadSessionOptions {
    ReadSessionOptions {
        limits: ReadLimits::default(),
        timeouts: ReadClientTimeouts {
            routing_retry_ms: DECLARED_ROUTING_RETRY_MS,
            ..ReadClientTimeouts::default()
        },
        ..ReadSessionOptions::default()
    }
}

/// A writer that will serve inspection on the runtime root it is given.
fn serving_writer_options(runtime_dir: PathBuf) -> DatabaseOpenOptions {
    DatabaseOpenOptions {
        owner_reads: OwnerReadConfig {
            limits: OwnerReadLimits::default(),
            timeouts: OwnerServiceTimeouts::default(),
            runtime_dir: Some(runtime_dir),
            handler: Some(Arc::new(EchoHandler)),
            ..OwnerReadConfig::default()
        },
        ..DatabaseOpenOptions::default()
    }
}

/// A writer whose operator turned inspection off. It holds the store exactly as
/// any other writer does; its published decision is that it will not serve.
fn disabled_writer_options(runtime_dir: PathBuf) -> DatabaseOpenOptions {
    let mut options = serving_writer_options(runtime_dir);
    options.owner_reads.enabled = false;
    options
}

/// A writer pointed at a runtime directory that cannot be used, so its channel
/// startup really fails. The store still opens -- serving never fails a
/// writable open -- and the failure is what the writer publishes.
fn failing_writer_options(directory: &Path) -> DatabaseOpenOptions {
    let unusable = directory.join("not-a-runtime-directory");
    std::fs::write(
        &unusable,
        b"this pathname is a file, not a runtime directory",
    )
    .expect("plant an unusable runtime pathname");
    serving_writer_options(unusable)
}

fn seed(path: &Path) {
    let database = Database::open(path).expect("create the committed fixture");
    database
        .execute(
            "CREATE TABLE claim_rows (id INTEGER, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create the fixture table");
    database
        .execute(
            "INSERT INTO claim_rows (id, payload) VALUES ($id, $payload)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(1)),
                ("payload".to_owned(), Value::Text("claim".to_owned())),
            ]),
        )
        .expect("insert the fixture row");
    database.close().expect("release the seeded store");
}

/// Parks a writer between claiming the store and publishing its serving
/// decision.
///
/// The persistence-opened boundary IS the claim: the companion is held
/// exclusively and the run a reader dials has been published, while the serving
/// decision does not exist yet. This is the writer's own startup, stopped where
/// every writer really is for a moment.
#[derive(Default)]
struct WriterParkedInsideItsClaim {
    claimed: (Mutex<bool>, Condvar),
    release: (Mutex<bool>, Condvar),
    /// Holds the writer OPEN after it has published its decision, until the
    /// journey has finished using it. A writer that closes is winding down and
    /// correctly refuses ordinary requests, so a journey that let it close
    /// before reading would be testing shutdown, not the answer a waiting
    /// caller was owed.
    finish: (Mutex<bool>, Condvar),
    parked: AtomicBool,
}

impl ReadSessionTestObserver for WriterParkedInsideItsClaim {
    fn observe_event(&self, event: ReadSessionEvent) {
        if event != ReadSessionEvent::PersistenceOpened || self.parked.swap(true, Ordering::SeqCst)
        {
            return;
        }
        {
            let (flag, signal) = &self.claimed;
            *flag.lock().expect("claim-window state") = true;
            signal.notify_all();
        }
        let (flag, signal) = &self.release;
        let mut released = flag.lock().expect("claim-window state");
        while !*released {
            released = signal.wait(released).expect("claim-window condvar");
        }
    }
}

impl WriterParkedInsideItsClaim {
    fn wait_until_claimed(&self) {
        let (flag, signal) = &self.claimed;
        let mut claimed = flag.lock().expect("claim-window state");
        while !*claimed {
            let (next, timed_out) = signal
                .wait_timeout(claimed, WATCHDOG)
                .expect("claim-window condvar");
            claimed = next;
            assert!(
                !timed_out.timed_out() || *claimed,
                "the writer never reached the point where it holds the store",
            );
        }
    }

    fn let_go(&self) {
        let (flag, signal) = &self.release;
        *flag.lock().expect("claim-window state") = true;
        signal.notify_all();
    }

    /// Called on the writer's own thread, once it is open and serving.
    fn stay_open_until_finished(&self) {
        let (flag, signal) = &self.finish;
        let mut finished = flag.lock().expect("claim-window state");
        while !*finished {
            finished = signal.wait(finished).expect("claim-window condvar");
        }
    }

    fn request_finish(&self) {
        let (flag, signal) = &self.finish;
        *flag.lock().expect("claim-window state") = true;
        signal.notify_all();
    }
}

/// The parked writer, still holding the store.
///
/// It stays open until the journey asks it to finish, because a journey that
/// reached the owner has to be able to USE the owner: a writer that has closed
/// is winding down and correctly refuses every ordinary request, so ending it
/// before the caller reads would make the read prove the opposite of what this
/// file is about.
struct ParkedWriter {
    gate: Arc<WriterParkedInsideItsClaim>,
    thread: Option<std::thread::JoinHandle<std::result::Result<OwnerServingState, String>>>,
}

impl ParkedWriter {
    /// Let the writer finish and say what it published about itself.
    fn finish(mut self) -> OwnerServingState {
        self.gate.let_go();
        self.gate.request_finish();
        self.thread
            .take()
            .expect("the parked writer is finished exactly once")
            .join()
            .expect("join the parked writer")
            .expect("the parked writer opens the store")
    }
}

impl Drop for ParkedWriter {
    fn drop(&mut self) {
        // A journey that fails an assertion before finishing must not leave a
        // writer parked inside its claim, or held open, with nobody left to
        // let it go.
        self.gate.let_go();
        self.gate.request_finish();
    }
}

fn park_a_writer_inside_its_claim(path: &Path, options: DatabaseOpenOptions) -> ParkedWriter {
    let gate = Arc::new(WriterParkedInsideItsClaim::default());
    let observer: Arc<dyn ReadSessionTestObserver> = gate.clone();
    let held_open = gate.clone();
    let path = path.to_path_buf();
    let thread = std::thread::spawn(move || {
        let mut options = options;
        options.test_observer = Some(observer);
        match Database::open_with_options(&path, options) {
            Ok(database) => {
                let state = database.owner_read_status().state;
                // Open, published, and serving -- and it stays that way until
                // the journey has had its answer out of it.
                held_open.stay_open_until_finished();
                database.close().expect("close the parked writer");
                Ok(state)
            }
            Err(error) => Err(error.to_string()),
        }
    });
    gate.wait_until_claimed();
    ParkedWriter {
        gate,
        thread: Some(thread),
    }
}

/// The caller's own observation. When the selection announces that it is
/// waiting inside a claim window, the parked writer is released -- on this
/// thread, at that moment -- so the writer's publication is inside the caller's
/// wait by construction.
struct ReleaseTheWriterOnceInside {
    gate: Arc<WriterParkedInsideItsClaim>,
    waits: AtomicUsize,
}

impl ReadSessionTestObserver for ReleaseTheWriterOnceInside {
    fn observe_event(&self, event: ReadSessionEvent) {
        if let ReadSessionEvent::ClaimWindowWait { .. } = event {
            self.waits.fetch_add(1, Ordering::SeqCst);
            self.gate.let_go();
        }
    }
}

/// Every reader breadcrumb the runtime root carries. A door that never opens
/// the committed file leaves this empty.
fn reader_breadcrumbs(runtime_root: &Path) -> Vec<PathBuf> {
    fn collect(directory: &Path, found: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(directory) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect(&path, found);
            } else if path
                .extension()
                .is_some_and(|extension| extension == "reader")
            {
                found.push(path);
            }
        }
    }
    let mut found = Vec::new();
    collect(runtime_root, &mut found);
    found
}

/// Run one journey: park a writer inside its claim, ask the owner-only door
/// about the store while it is parked, and release the writer at the instant
/// the door reports that it is waiting for the writer's decision.
///
/// The writer is handed back STILL OPEN. What the caller was owed is a usable
/// answer from a live owner, so the journey gets to use it before deciding the
/// writer is done.
fn ask_inside_the_claim_window(
    path: &Path,
    runtime_root: &Path,
    writer_options: DatabaseOpenOptions,
) -> (Result<ReadSession>, usize, ParkedWriter) {
    let writer = park_a_writer_inside_its_claim(path, writer_options);
    let observer = Arc::new(ReleaseTheWriterOnceInside {
        gate: writer.gate.clone(),
        waits: AtomicUsize::new(0),
    });
    let watching: Arc<dyn ReadSessionTestObserver> = observer.clone();

    let answer = ReadSession::with_runtime_directory_for_test(runtime_root, || {
        ReadSession::open_owner_only_with_observer_for_test(path, session_options(), watching)
    });

    (answer, observer.waits.load(Ordering::SeqCst), writer)
}

/// A writer that comes up serving is reached on its own channel, by a caller
/// that was already waiting when it published.
#[test]
fn a_writer_that_publishes_serving_inside_the_budget_answers_the_caller() {
    let directory = tempfile::TempDir::new().expect("task-scoped claim-window directory");
    let runtime_root = secure_runtime_root(directory.path(), "resolves-serving-runtime");
    let path = directory.path().join("resolves-serving.db");
    seed(&path);

    let (answer, waits, writer) = ask_inside_the_claim_window(
        &path,
        &runtime_root,
        serving_writer_options(runtime_root.clone()),
    );

    assert!(
        waits > 0,
        "the door never reported waiting inside the claim window, so nothing about this journey \
         happened inside it",
    );
    // The writer is still open here, and that is the point: a caller that
    // waited out the claim window is owed a session it can actually read
    // through, not merely one that opened. A writer that has closed is winding
    // down and refuses ordinary requests, so reading here -- against the same
    // live writer the caller waited for -- is what makes the answer worth
    // having.
    let session = answer.expect("a writer that publishes serving inside the budget is reachable");
    assert_eq!(
        session.route(),
        ReadRoute::Owner,
        "the caller is owed the writer's own channel, never the committed file",
    );
    let rows = session
        .execute("SELECT id FROM claim_rows", &HashMap::new())
        .expect("the owner answers the caller that waited for it");
    assert_eq!(rows.rows.len(), 1);
    drop(session);

    assert_eq!(
        writer.finish(),
        OwnerServingState::Serving,
        "the parked writer is the one that went on to serve",
    );

    assert_eq!(
        reader_breadcrumbs(&runtime_root),
        Vec::<PathBuf>::new(),
        "waiting out a claim window never hydrates the store, so no reader breadcrumb exists",
    );
}

/// A writer whose operator turned inspection off publishes exactly that, and
/// the waiting caller is told it -- not that the store is unowned.
#[test]
fn a_writer_that_publishes_serving_disabled_inside_the_budget_says_so() {
    let directory = tempfile::TempDir::new().expect("task-scoped claim-window directory");
    let runtime_root = secure_runtime_root(directory.path(), "resolves-disabled-runtime");
    let path = directory.path().join("resolves-disabled.db");
    seed(&path);

    let (answer, waits, writer) = ask_inside_the_claim_window(
        &path,
        &runtime_root,
        disabled_writer_options(runtime_root.clone()),
    );
    let published = writer.finish();

    assert!(waits > 0, "the door never entered the claim window");
    assert_eq!(
        published,
        OwnerServingState::ServingDisabled,
        "the parked writer published that inspection is switched off",
    );
    match answer {
        Ok(session) => panic!(
            "a writer that will not serve answered on the {:?} route",
            session.route()
        ),
        Err(Error::ReadFailure(failure)) => assert_eq!(
            failure.kind(),
            ReadFailureKind::OwnerNotServing,
            "the caller is owed the writer's own decision: it holds the store and will not serve",
        ),
        Err(other) => panic!("the claim-window ask answered {other:?}"),
    }
}

/// A writer whose channel really failed to start publishes the failure with its
/// reason, and the waiting caller is handed that reason rather than being sent
/// off to look for a store nobody owns.
#[test]
fn a_writer_that_publishes_a_startup_failure_inside_the_budget_names_it() {
    let directory = tempfile::TempDir::new().expect("task-scoped claim-window directory");
    let runtime_root = secure_runtime_root(directory.path(), "resolves-failure-runtime");
    let path = directory.path().join("resolves-failure.db");
    seed(&path);

    let (answer, waits, writer) = ask_inside_the_claim_window(
        &path,
        &runtime_root,
        failing_writer_options(directory.path()),
    );
    let published = writer.finish();

    assert!(waits > 0, "the door never entered the claim window");
    assert_eq!(
        published,
        OwnerServingState::NotServing,
        "the parked writer's channel really failed to start",
    );
    match answer {
        Ok(session) => panic!(
            "a writer whose channel failed answered on the {:?} route",
            session.route()
        ),
        Err(Error::ReadFailure(failure)) => {
            assert_eq!(
                failure.kind(),
                ReadFailureKind::OwnerNotServing,
                "the caller is owed the writer's published failure, not an absent store",
            );
            assert!(
                matches!(failure.detail(), ReadFailureDetail::Reason { .. }),
                "the writer recorded WHY it cannot serve, and the caller is owed that reason",
            );
        }
        Err(other) => panic!("the claim-window ask answered {other:?}"),
    }
}
