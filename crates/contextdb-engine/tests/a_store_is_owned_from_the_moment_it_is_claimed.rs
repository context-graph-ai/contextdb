#![cfg(all(unix, feature = "test-seams"))]
//! A store is owned from the moment a writer takes it, not from the moment
//! that writer gets around to saying so.
//!
//! The ruling is about a window, and the window has to start where OWNERSHIP
//! starts. A writer takes the store by claiming the companion exclusively;
//! only afterwards does it publish the run beside the store. Everything in
//! between is a stretch in which a process genuinely owns the store and
//! nothing published says so — and a caller arriving there is told the store
//! has no owner.
//!
//! That answer is the licence for an absent-store fallback: create it, seed
//! it, take it over. A supervisor that starts a writer and immediately asks
//! about it lands in exactly this stretch, is told nobody owns the store, and
//! goes to take a store another process is already holding. It then collides
//! with the storage layer and reports a fault it never asked about, or worse,
//! decides the store needs creating.
//!
//! Two shapes, because the writer reaches its claim by two different roads:
//!
//! A store that has been written before still carries the PREVIOUS run's
//! record while the new writer holds it, so nothing about the new writer is
//! visible yet and the stale record is what a caller would be answered from.
//!
//! A store being created has no record at all yet — its companion is
//! deliberately created late, only once the store proves readable and current,
//! so that a refused or legacy root is left byte-for-byte alone. That ordering
//! is right and this file does not ask for it to change. What it asks is that
//! the promise hold anyway: while a writer owns the store, no caller is told
//! the store is unowned. Which hold anchors that stretch is the implementer's
//! design.
//!
//! -------------------------------------------------------------------------
//! SEAM THIS JOURNEY NEEDS, VERBATIM
//!
//! The only park point that exists today is `PersistenceOpened`, and it is
//! emitted after `open_loaded` returns -- after publication -- so nothing
//! observable exists inside this window at all. That is why these pins need a
//! new one rather than reusing what is already there.
//!
//! This is ONE seam shared with the vigil proofs, which need the same park
//! point for their cross-process claimed-store guard. In
//! `crates/contextdb-engine/src/read_session.rs`, in `mod route_observation`,
//! add this variant to `enum ReadSessionEvent`:
//!
//! ```ignore
//!         /// Emitted inside a writable open at the moment this writer takes
//!         /// the companion's FIRST exclusive claim on the store, before it
//!         /// has published anything a reader can dial and before any serving
//!         /// decision exists. A test observer may block here, which is what
//!         /// lets a proof hold a store claimed-but-unannounced against a
//!         /// caller in another process -- the window in which an owner-only
//!         /// ask must not be told the store is free.
//!         CompanionClaimTaken,
//! ```
//!
//! It reaches the writer's `test_observer` exactly as `PersistenceOpened`
//! does, and is emitted from the claim site inside `open_loaded`.
//!
//! WHERE IT MUST FIRE FOR THE NEW-STORE BRANCH. On a store that already has a
//! companion, the claim site and the start of ownership are the same moment,
//! so the variant above is the whole answer. On a store being created they are
//! NOT: that branch proves the root readable and current through an exclusive
//! writable open FIRST and creates the companion only afterwards, so the
//! writer already owns the store for the whole stretch before any companion
//! claim exists. A `CompanionClaimTaken` emitted only at companion creation
//! would fire after that stretch has closed and could not see it.
//!
//! What these pins need is an emission at the point the writer FIRST holds the
//! store exclusively on each branch. If the implementer anchors both branches
//! on one hold, this one variant serves both and fires at that anchor. If the
//! branches keep separate first holds, this same variant is what the
//! new-store branch emits at its own first hold. Either satisfies the pins;
//! the ordering itself is deliberate and is not what is being asked to change.
//! -------------------------------------------------------------------------

use contextdb_core::read_contract::{
    OwnerReadCancellation, OwnerReadLimits, OwnerServiceTimeouts, OwnerServingState,
    ReadClientTimeouts, ReadFailureKind, ReadLimits, ReadRoute,
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
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A budget roomy enough that it is never what ends a wait in the journeys
/// that expect the writer's real answer.
const RESOLVING_RETRY_MS: u64 = 30_000;
/// A budget short enough that the expiry journeys finish promptly. It is the
/// caller's own declared statement, and the only thing that may end its wait.
const EXPIRING_RETRY_MS: u64 = 400;

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

fn session_options(routing_retry_ms: u64) -> ReadSessionOptions {
    ReadSessionOptions {
        limits: ReadLimits::default(),
        timeouts: ReadClientTimeouts {
            routing_retry_ms,
            ..ReadClientTimeouts::default()
        },
        ..ReadSessionOptions::default()
    }
}

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

/// A store a writer has already served and closed, so its companion carries a
/// PREVIOUS run's record. This is the ordinary state of every store that has
/// ever been opened, and it is what a new writer's claim sits on top of.
fn store_with_a_previous_run(directory: &Path, runtime_root: &Path, name: &str) -> PathBuf {
    let path = directory.join(name);
    let database = Database::open_with_options(&path, serving_writer_options(runtime_root.into()))
        .expect("the previous run takes the store");
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
                ("payload".to_owned(), Value::Text("claimed".to_owned())),
            ]),
        )
        .expect("insert the fixture row");
    assert_eq!(
        database.owner_read_status().state,
        OwnerServingState::Serving,
        "the previous run really did serve, so its record is a settled one",
    );
    database
        .close()
        .expect("the previous run lets the store go");
    path
}

/// Holds a writer at the moment it takes the store, and lets the journey
/// decide when it may go on.
///
/// This is the writer's own startup, stopped where every writer really is for
/// a moment: it owns the store and has announced nothing.
#[derive(Default)]
struct WriterHoldingAnUnannouncedClaim {
    claimed: (std::sync::Mutex<bool>, std::sync::Condvar),
    release: (std::sync::Mutex<bool>, std::sync::Condvar),
    finish: (std::sync::Mutex<bool>, std::sync::Condvar),
    parked: std::sync::atomic::AtomicBool,
}

impl ReadSessionTestObserver for WriterHoldingAnUnannouncedClaim {
    fn observe_event(&self, event: ReadSessionEvent) {
        if event != ReadSessionEvent::CompanionClaimTaken
            || self.parked.swap(true, Ordering::SeqCst)
        {
            return;
        }
        {
            let (flag, signal) = &self.claimed;
            *flag.lock().expect("claim state") = true;
            signal.notify_all();
        }
        let (flag, signal) = &self.release;
        let mut released = flag.lock().expect("claim state");
        while !*released {
            released = signal.wait(released).expect("claim condvar");
        }
    }
}

impl WriterHoldingAnUnannouncedClaim {
    fn wait_until_claimed(&self) {
        let (flag, signal) = &self.claimed;
        let mut claimed = flag.lock().expect("claim state");
        while !*claimed {
            claimed = signal.wait(claimed).expect("claim condvar");
        }
    }

    fn release(&self) {
        let (flag, signal) = &self.release;
        *flag.lock().expect("claim state") = true;
        signal.notify_all();
    }

    fn stay_open_until_finished(&self) {
        let (flag, signal) = &self.finish;
        let mut done = flag.lock().expect("claim state");
        while !*done {
            done = signal.wait(done).expect("claim condvar");
        }
    }

    fn request_finish(&self) {
        let (flag, signal) = &self.finish;
        *flag.lock().expect("claim state") = true;
        signal.notify_all();
    }
}

/// A writer held at its first claim, on its own thread. It stays OPEN after it
/// publishes until the journey has finished with it, because a writer that has
/// closed is winding down and refuses ordinary requests.
struct HeldWriter {
    gate: Arc<WriterHoldingAnUnannouncedClaim>,
    thread: Option<std::thread::JoinHandle<std::result::Result<OwnerServingState, String>>>,
}

impl HeldWriter {
    fn finish(mut self) -> OwnerServingState {
        self.gate.release();
        self.gate.request_finish();
        self.thread
            .take()
            .expect("the held writer is finished exactly once")
            .join()
            .expect("join the held writer")
            .expect("the held writer opens the store")
    }
}

impl Drop for HeldWriter {
    fn drop(&mut self) {
        // A journey that fails an assertion before finishing must not strand
        // the writer at its claim or hold it open with nobody left to let go.
        self.gate.release();
        self.gate.request_finish();
    }
}

/// Take a writer to the moment it owns the store and hold it there.
fn hold_a_writer_at_its_claim(path: &Path, runtime_root: &Path) -> HeldWriter {
    let gate = Arc::new(WriterHoldingAnUnannouncedClaim::default());
    let observer: Arc<dyn ReadSessionTestObserver> = gate.clone();
    let held_open = Arc::clone(&gate);
    let mut options = serving_writer_options(runtime_root.to_path_buf());
    options.test_observer = Some(observer);
    let path = path.to_path_buf();
    let thread = std::thread::spawn(move || match Database::open_with_options(&path, options) {
        Ok(database) => {
            let state = database.owner_read_status().state;
            held_open.stay_open_until_finished();
            database.close().expect("close the held writer");
            Ok(state)
        }
        Err(error) => Err(error.to_string()),
    });
    gate.wait_until_claimed();
    HeldWriter {
        gate,
        thread: Some(thread),
    }
}

/// The caller's own observation: when the door reports that it is waiting out
/// a claim, the held writer is released AT that moment, on the caller's own
/// thread. The writer's publication is therefore inside the caller's wait by
/// construction rather than by luck.
struct ReleaseTheWriterOnceInside {
    gate: Arc<WriterHoldingAnUnannouncedClaim>,
    waits: AtomicUsize,
}

impl ReadSessionTestObserver for ReleaseTheWriterOnceInside {
    fn observe_event(&self, event: ReadSessionEvent) {
        if let ReadSessionEvent::ClaimWindowWait { .. } = event {
            self.waits.fetch_add(1, Ordering::SeqCst);
            self.gate.release();
        }
    }
}

fn refusal_kind(result: Result<ReadSession>, context: &str) -> ReadFailureKind {
    match result {
        Ok(session) => panic!(
            "{context}: the door opened a {:?}-route session where a refusal was expected",
            session.route()
        ),
        Err(Error::ReadFailure(failure)) => failure.kind(),
        Err(other) => panic!("{context}: expected a typed read refusal, got {other:?}"),
    }
}

/// An existing store, mid-handover. The previous run's record is the only
/// thing published, and it describes a writer that is gone.
#[test]
fn a_claimed_store_carrying_a_previous_run_s_record_is_never_reported_unowned() {
    let directory = tempfile::TempDir::new().expect("task-scoped claim directory");
    let runtime_root = secure_runtime_root(directory.path(), "prior-run-runtime");
    let path = store_with_a_previous_run(directory.path(), &runtime_root, "prior-run.db");

    let writer = hold_a_writer_at_its_claim(&path, &runtime_root);

    let answer = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_owner_only(&path, session_options(EXPIRING_RETRY_MS))
    });
    let kind = refusal_kind(answer, "a store a writer has claimed but not published");

    assert_ne!(
        kind,
        ReadFailureKind::OwnerNotRunning,
        "a writer owns this store right now -- it took the companion exclusively before this ask \
         began -- so reporting the store unowned licenses a consumer to create and take a store \
         another process is holding. The previous run's record is stale, not absent.",
    );

    assert_eq!(
        writer.finish(),
        OwnerServingState::Serving,
        "the held writer really did own this store and went on to serve it",
    );
}

/// The same window, resolved rather than expired: a caller that is already
/// waiting when the writer publishes gets the writer's real answer.
#[test]
fn a_caller_inside_the_claim_gets_the_writer_s_answer_once_it_publishes() {
    let directory = tempfile::TempDir::new().expect("task-scoped claim directory");
    let runtime_root = secure_runtime_root(directory.path(), "resolves-runtime");
    let path = store_with_a_previous_run(directory.path(), &runtime_root, "resolves.db");

    let writer = hold_a_writer_at_its_claim(&path, &runtime_root);
    let observer = Arc::new(ReleaseTheWriterOnceInside {
        gate: Arc::clone(&writer.gate),
        waits: AtomicUsize::new(0),
    });
    let watching: Arc<dyn ReadSessionTestObserver> = observer.clone();

    let answer = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_owner_only_with_observer_for_test(
            &path,
            session_options(RESOLVING_RETRY_MS),
            watching,
        )
    });

    assert!(
        observer.waits.load(Ordering::SeqCst) > 0,
        "the door never reported waiting inside the claim, so nothing here happened inside it",
    );
    let session = answer.expect("a writer that publishes inside the budget is reachable");
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

    assert_eq!(writer.finish(), OwnerServingState::Serving);
}

/// A store being created. Nothing about it has ever been published, and the
/// writer creating it owns it from its own first claim onwards.
#[test]
fn a_store_being_created_is_never_reported_unowned_while_its_writer_holds_it() {
    let directory = tempfile::TempDir::new().expect("task-scoped claim directory");
    let runtime_root = secure_runtime_root(directory.path(), "new-store-runtime");
    let path = directory.path().join("brand-new.db");

    let writer = hold_a_writer_at_its_claim(&path, &runtime_root);

    let answer = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_owner_only(&path, session_options(EXPIRING_RETRY_MS))
    });
    let kind = refusal_kind(answer, "a store its writer is still creating");

    assert_ne!(
        kind,
        ReadFailureKind::OwnerNotRunning,
        "a writer owns this store right now, so telling a caller nobody owns it sends that \
         caller to create the very store already being created. Nothing published yet is not the \
         same fact as nobody holding it.",
    );

    assert_eq!(
        writer.finish(),
        OwnerServingState::Serving,
        "the held writer really did create and then serve this store",
    );
}

/// The control that keeps the promise honest: a store nobody has claimed still
/// answers owner-absent immediately. Making a held claim visible must not make
/// every absent store into a wait.
#[test]
fn a_store_nobody_has_claimed_still_answers_owner_absent_at_once() {
    let directory = tempfile::TempDir::new().expect("task-scoped claim directory");
    let runtime_root = secure_runtime_root(directory.path(), "unclaimed-runtime");
    let path = store_with_a_previous_run(directory.path(), &runtime_root, "unclaimed.db");

    let kind = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        refusal_kind(
            ReadSession::open_owner_only(&path, session_options(EXPIRING_RETRY_MS)),
            "a store whose last writer closed and which nobody holds",
        )
    });
    assert_eq!(
        kind,
        ReadFailureKind::OwnerNotRunning,
        "nobody is holding this store, so owner-absent is the true answer and it is owed at once",
    );
}
