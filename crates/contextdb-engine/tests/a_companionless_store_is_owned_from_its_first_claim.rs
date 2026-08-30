#![cfg(all(unix, feature = "test-seams"))]
//! A store file that has no companion beside it is still a store somebody can
//! own, and the promise starts when they take it.
//!
//! This is the one road to ownership where the two moments are furthest apart.
//! A writer opening a store with no companion proves the root readable and
//! current through an exclusive writable open FIRST, and only then creates the
//! companion. Everything in between is a stretch where a process holds the
//! store exclusively and there is not one byte beside it that says so — the
//! longest false-absent window in the system, and the one a caller is most
//! likely to land in, because it is the stretch that does real work.
//!
//! An owner-only caller arriving there is told the store has no owner. That is
//! the licence for an absent-store fallback, and the consumer acts on it: it
//! goes to create or take a store that another process is already holding
//! exclusively, and meets a storage-layer refusal it never asked about.
//!
//! The ordering itself was never the problem, and these journeys do not ask for
//! it to change. Proving the root before adopting a companion is what keeps a
//! legacy or corrupt file byte-for-byte untouched when the open is refused.
//! What they ask is that the stretch be COVERED: a claim taken early enough to
//! span it, carrying no record, saying nothing about serving.
//!
//! Which brings the second half, and it is not a detail. A claim taken before
//! the store has been proven readable is a claim taken on files that turn out
//! to be refusable — a legacy layout, a corrupt root, a file that is not a
//! store at all. Every one of those opens must leave the directory exactly as
//! it found it. An operator who runs a writer against the wrong file and is
//! correctly refused must not be left with a companion sitting beside that
//! file, and the next caller to ask about the path must be told plainly that
//! nobody owns it. Covering the window is only half the promise; leaving
//! nothing behind when the open fails is the other half, and a claim that
//! covers the window by littering has traded one defect for another.

use contextdb_core::read_contract::{
    OwnerReadCancellation, OwnerReadLimits, OwnerServiceTimeouts, OwnerServingState,
    ReadClientTimeouts, ReadFailureKind, ReadLimits, ReadRoute,
};
use contextdb_core::{Error, Result, Value};
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    DurableStoreDamage, RawRedbReadOnlyOpenObservation, RawRedbWriterOpenObservation,
    prepare_durable_store_damage_for_test, try_raw_redb_read_only_open_for_test,
    try_raw_redb_writer_open_for_test,
};
use contextdb_engine::read_session::{ReadSessionEvent, ReadSessionTestObserver};
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, OwnerRequestHandler, ReadSession,
    ReadSessionOptions,
};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

/// Failsafe only. Nothing here asserts how long anything took; this turns a
/// park point that never fires into a readable failure instead of a hang.
const WATCHDOG: Duration = Duration::from_secs(30);
/// A budget roomy enough that it is never what ends a wait in the journey that
/// expects the writer's real answer.
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

/// Where a store's companion sits: beside the store, named for it.
fn companion_beside(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".lock");
    PathBuf::from(name)
}

/// A real store file with NO companion beside it.
///
/// This is not a contrived shape. It is what a store copied, restored from a
/// backup, or moved between machines looks like: the companion is coordination
/// state for a live writer, not part of the store, so it does not travel with
/// it.
fn store_without_a_companion(directory: &Path, name: &str) -> PathBuf {
    let path = directory.join(name);
    let database = Database::open(&path).expect("create the store");
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
    database.close().expect("let the store go");

    let companion = companion_beside(&path);
    std::fs::remove_file(&companion).expect("take the companion away, as a copy or restore does");
    assert!(
        !companion.exists(),
        "the fixture is a store file with no companion beside it",
    );
    path
}

/// Holds a writer at the moment it takes the store, and lets the journey decide
/// when it may go on.
#[derive(Default)]
struct WriterHoldingAnUnannouncedClaim {
    claimed: (std::sync::Mutex<bool>, std::sync::Condvar),
    release: (std::sync::Mutex<bool>, std::sync::Condvar),
    finish: (std::sync::Mutex<bool>, std::sync::Condvar),
    parked: AtomicBool,
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
    /// Block until the writer reports that it has claimed the store.
    ///
    /// Bounded, because "the writer never claimed anything" is one of the real
    /// answers here: on a root the writer goes on to refuse, a claim taken only
    /// after the root is proven never happens at all. That has to read as a
    /// failure, not as a hang.
    fn wait_until_claimed(&self, context: &str) {
        let (flag, signal) = &self.claimed;
        let mut claimed = flag.lock().expect("claim state");
        while !*claimed {
            let (next, timed_out) = signal
                .wait_timeout(claimed, WATCHDOG)
                .expect("claim condvar");
            claimed = next;
            assert!(
                !timed_out.timed_out() || *claimed,
                "{context}: the writer never claimed the store. A claim taken only once the root \
                 has been proven readable leaves the whole stretch before that proof uncovered, \
                 and on a root the writer refuses there is never a claim at all.",
            );
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

struct HeldWriter {
    gate: Arc<WriterHoldingAnUnannouncedClaim>,
    thread: Option<std::thread::JoinHandle<std::result::Result<OwnerServingState, String>>>,
}

impl HeldWriter {
    /// Take the writer's own outcome, whatever it was. A journey about a root
    /// the writer refuses needs the refusal, not a panic about it.
    fn thread_result(
        mut self,
    ) -> std::result::Result<std::result::Result<OwnerServingState, String>, String> {
        self.thread
            .take()
            .expect("the held writer is finished exactly once")
            .join()
            .map_err(|_| "the held writer panicked".to_owned())
    }

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
        self.gate.release();
        self.gate.request_finish();
    }
}

fn hold_a_writer_at_its_claim(path: &Path, runtime_root: &Path, context: &str) -> HeldWriter {
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
    gate.wait_until_claimed(context);
    HeldWriter {
        gate,
        thread: Some(thread),
    }
}

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

/// The window itself: a writer holds a companion-less store and nothing beside
/// it says so.
#[test]
fn a_companionless_store_its_writer_holds_is_never_reported_unowned() {
    let directory = tempfile::TempDir::new().expect("task-scoped companion-less directory");
    let runtime_root = secure_runtime_root(directory.path(), "companionless-runtime");
    let path = store_without_a_companion(directory.path(), "companionless.db");

    let writer = hold_a_writer_at_its_claim(
        &path,
        &runtime_root,
        "a writer taking a companion-less store",
    );

    // The claim has to be EARLY, and this is what says so. A claim taken only
    // after the root has been proven readable is taken with the store already
    // held exclusively -- so if the writer has redb by now, the park point is
    // downstream of the stretch this journey exists to cover, and everything
    // below would pass without ever entering it.
    assert_eq!(
        try_raw_redb_writer_open_for_test(&path),
        RawRedbWriterOpenObservation::Acquired,
        "the writer already held the store exclusively when it claimed it, so the claim comes \
         AFTER the proof rather than before it. The stretch between taking the store and \
         claiming it is exactly the false-absent window, and a park point on the far side of it \
         cannot see it.",
    );

    let kind = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        refusal_kind(
            ReadSession::open_owner_only(&path, session_options(EXPIRING_RETRY_MS)),
            "a companion-less store its writer is holding",
        )
    });

    assert_ne!(
        kind,
        ReadFailureKind::OwnerNotRunning,
        "a writer holds this store exclusively right now. Nothing beside the store says so yet, \
         but that is a fact about what has been written down, not about who owns it -- and a \
         caller told the store is unowned goes and takes a store it cannot have.",
    );

    assert_eq!(
        writer.finish(),
        OwnerServingState::Serving,
        "the held writer really did own this store and went on to serve it",
    );
}

/// The same window, resolved: a caller already waiting when the writer
/// publishes gets the writer's real answer, on the writer's own channel.
#[test]
fn a_caller_waiting_on_a_companionless_store_gets_the_writer_s_answer() {
    let directory = tempfile::TempDir::new().expect("task-scoped companion-less directory");
    let runtime_root = secure_runtime_root(directory.path(), "companionless-resolve-runtime");
    let path = store_without_a_companion(directory.path(), "companionless-resolve.db");

    let writer = hold_a_writer_at_its_claim(
        &path,
        &runtime_root,
        "a writer taking a companion-less store",
    );
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
    assert_eq!(session.route(), ReadRoute::Owner);
    let rows = session
        .execute("SELECT id FROM claim_rows", &HashMap::new())
        .expect("the owner answers the caller that waited for it");
    assert_eq!(rows.rows.len(), 1);
    drop(session);

    assert_eq!(writer.finish(), OwnerServingState::Serving);
}

/// A root the writer is going to refuse is claimed before it is judged.
///
/// This is the sharpest statement of where the claim belongs. A claim taken
/// after the root is proven readable never happens at all on a root that fails
/// the proof -- so on exactly the files an operator is most likely to point a
/// writer at by mistake, the store is held with nothing saying so for the whole
/// of the check. Reaching the claim here is what proves it precedes the
/// judgement.
#[test]
fn a_root_the_writer_will_refuse_is_still_claimed_before_it_is_judged() {
    let directory = tempfile::TempDir::new().expect("task-scoped companion-less directory");
    let runtime_root = secure_runtime_root(directory.path(), "refused-claim-runtime");
    let path = store_without_a_companion(directory.path(), "refused-claim.db");
    prepare_durable_store_damage_for_test(&path, DurableStoreDamage::LegacyLayout)
        .expect("make this a root a writer must refuse");

    // The writer will fail this open. It must still have claimed the store
    // before it started deciding that.
    let writer = hold_a_writer_at_its_claim(
        &path,
        &runtime_root,
        "a writer opening a root it will go on to refuse",
    );

    let kind = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        refusal_kind(
            ReadSession::open_owner_only(&path, session_options(EXPIRING_RETRY_MS)),
            "a root a writer is holding while it judges it",
        )
    });
    assert_ne!(
        kind,
        ReadFailureKind::OwnerNotRunning,
        "a writer is holding this root while it decides whether it can adopt it, so the store is \
         not free for anyone else to take",
    );

    // Releasing lets the refusal happen, and the refusal must leave nothing.
    writer.gate.release();
    writer.gate.request_finish();
    let outcome = writer
        .thread_result()
        .expect("the writer thread finishes")
        .expect_err("a legacy root is refused, never opened");
    assert!(
        !outcome.is_empty(),
        "the refusal names what was wrong with the root",
    );
    assert!(
        !companion_beside(&path).exists(),
        "the refused open left a companion beside a file the writer would not adopt",
    );
}

/// A refused open leaves the directory as it found it.
///
/// The file here is one a writer must refuse, and the refusal is correct. What
/// must not survive it is a companion: an operator pointed at the wrong file
/// gets an error and an unchanged directory, not an error and a new artifact
/// beside a file they never meant to touch.
#[test]
fn a_refused_open_of_a_companionless_store_leaves_no_companion_behind() {
    let directory = tempfile::TempDir::new().expect("task-scoped companion-less directory");
    let runtime_root = secure_runtime_root(directory.path(), "refused-runtime");
    let path = store_without_a_companion(directory.path(), "refused.db");
    prepare_durable_store_damage_for_test(&path, DurableStoreDamage::LegacyLayout)
        .expect("make this a root a writer must refuse");
    let companion = companion_beside(&path);
    assert!(
        !companion.exists(),
        "the fixture starts with no companion beside the store",
    );

    let refused = Database::open_with_options(&path, serving_writer_options(runtime_root.clone()));
    assert!(
        refused.is_err(),
        "a root a writer cannot adopt must be refused, not opened",
    );

    assert!(
        !companion.exists(),
        "the refused open left a companion at {}. An operator who pointed a writer at the wrong \
         file was told so and then handed an artifact beside that file anyway.",
        companion.display(),
    );

    // And the next caller to ask about the path is told the plain truth: after
    // a refused open nobody owns this store, and the answer is owed at once.
    let kind = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        refusal_kind(
            ReadSession::open_owner_only(&path, session_options(EXPIRING_RETRY_MS)),
            "a store whose only writer was refused",
        )
    });
    assert_eq!(
        kind,
        ReadFailureKind::OwnerNotRunning,
        "the refused writer holds nothing, so nobody owns this store and saying so is right",
    );
}

/// The control that keeps the promise honest: a companion-less store nobody is
/// opening still answers owner-absent at once. Covering the window must not
/// turn every unowned store into a wait.
#[test]
fn a_companionless_store_nobody_holds_answers_owner_absent_at_once() {
    let directory = tempfile::TempDir::new().expect("task-scoped companion-less directory");
    let runtime_root = secure_runtime_root(directory.path(), "companionless-idle-runtime");
    let path = store_without_a_companion(directory.path(), "companionless-idle.db");

    let kind = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        refusal_kind(
            ReadSession::open_owner_only(&path, session_options(EXPIRING_RETRY_MS)),
            "a companion-less store nobody is opening",
        )
    });
    assert_eq!(
        kind,
        ReadFailureKind::OwnerNotRunning,
        "nobody is holding this store, so owner-absent is the true answer and it is owed at once",
    );
    assert!(
        !companion_beside(&path).exists(),
        "asking about a store never creates coordination state beside it",
    );
}

const CRASH_CHILD_ROLE: &str = "CONTEXTDB_COMPANIONLESS_CRASH_ROLE";
const CRASH_CHILD_STORE: &str = "CONTEXTDB_COMPANIONLESS_CRASH_STORE";

/// A writer that dies with the store open, which is the only way to produce a
/// root Redb will not read until its own crash repair has rewritten it.
#[test]
fn companionless_crash_child() {
    if std::env::var_os(CRASH_CHILD_ROLE).is_none() {
        return;
    }
    let path = PathBuf::from(std::env::var_os(CRASH_CHILD_STORE).expect("child receives a store"));
    let _database = Database::open(&path).expect("the child takes the store");
    std::process::abort();
}

/// A real store whose last writer died with it open, and with no companion
/// beside it.
///
/// Redb refuses to READ such a root at all: it is unreadable until Redb's own
/// crash repair runs, and that repair rewrites the file. That is exactly the
/// root the claim-less road must not touch, so it is the root these journeys
/// are about.
fn crash_dirty_store_without_a_companion(directory: &Path, name: &str) -> PathBuf {
    let path = store_without_a_companion(directory, name);
    let status = Command::new(std::env::current_exe().expect("current integration-test binary"))
        .arg("--exact")
        .arg("companionless_crash_child")
        .arg("--nocapture")
        .env(CRASH_CHILD_ROLE, "1")
        .env(CRASH_CHILD_STORE, &path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("run the writer that dies with the store open");
    assert!(
        !status.success(),
        "the child is meant to die holding the store, and it exited cleanly instead",
    );
    // The child's own open created a companion. Take it away again, so the
    // road under test is the one with no companion beside the store.
    let companion = companion_beside(&path);
    let _ = std::fs::remove_file(&companion);
    assert!(!companion.exists(), "the fixture keeps no companion");
    assert_eq!(
        try_raw_redb_read_only_open_for_test(&path),
        RawRedbReadOnlyOpenObservation::RepairAborted,
        "this fixture only proves anything if Redb really refuses to read it without repairing \\
         it first",
    );
    path
}

fn file_bytes(path: &Path) -> Vec<u8> {
    std::fs::read(path).expect("read the store's bytes")
}

/// A store nobody has claimed is never repaired by a caller that was refused
/// the right to claim it.
///
/// Redb's crash repair REWRITES the root it repairs. Running it here would
/// rewrite a store this process has no claim on, and two openers meeting the
/// same crash-dirty root would rewrite it at the same time. So the claim-less
/// road judges nothing it would have to repair to read: the refusal is the
/// one the claim itself gave, and the file is left exactly as it was found.
#[test]
fn a_refused_open_never_repairs_a_store_it_could_not_claim() {
    let directory = tempfile::TempDir::new().expect("task-scoped companion-less directory");
    let enclosing = directory.path().join("read-only-directory");
    std::fs::create_dir(&enclosing).expect("create the enclosing directory");
    let path = crash_dirty_store_without_a_companion(&enclosing, "crash-dirty.db");
    let before = file_bytes(&path);
    std::fs::set_permissions(&enclosing, std::fs::Permissions::from_mode(0o500))
        .expect("make the directory one no new file can be created in");

    let refusal = Database::open(&path).err();

    std::fs::set_permissions(&enclosing, std::fs::Permissions::from_mode(0o700))
        .expect("restore the directory for cleanup");

    assert!(
        refusal.is_some(),
        "no companion could be created beside this store, so the open is refused",
    );
    assert_eq!(
        file_bytes(&path),
        before,
        "the refused open rewrote a crash-dirty store it holds no claim on. Redb's repair is \\
         what rewrites it, and running that without a claim lets two openers repair the same \\
         root at once.",
    );
    assert_eq!(
        try_raw_redb_read_only_open_for_test(&path),
        RawRedbReadOnlyOpenObservation::RepairAborted,
        "and the root is still the crash-dirty one it was, waiting for a writer that can claim it",
    );
    assert!(
        !companion_beside(&path).exists(),
        "a refused open leaves no coordination state beside the file",
    );
}

/// A directory this process cannot write is not a verdict about the FILE the
/// caller pointed at.
///
/// The claim now comes before the judgement, which means the claim can fail
/// first -- and the ordinary way it fails is a directory that will take no new
/// file. A person who has pointed a writer at something that is not a store is
/// owed that answer and the one next step that exists, not a complaint about
/// companion creation they cannot act on. Nothing is claimed in that case, so
/// classifying read-only creates nothing and changes no ownership fact.
#[test]
fn an_unwritable_directory_still_reports_what_is_wrong_with_the_file() {
    let directory = tempfile::TempDir::new().expect("task-scoped companion-less directory");
    let enclosing = directory.path().join("read-only-directory");
    std::fs::create_dir(&enclosing).expect("create the enclosing directory");
    let path = enclosing.join("not-a-store.db");
    std::fs::write(&path, b"this file was never a contextdb store").expect("plant the fixture");
    std::fs::set_permissions(&enclosing, std::fs::Permissions::from_mode(0o500))
        .expect("make the directory one no new file can be created in");

    let refusal = Database::open(&path).expect_err("a file that is not a store is refused");

    // Restore before asserting, so a failing assertion still leaves a
    // directory the harness can remove.
    std::fs::set_permissions(&enclosing, std::fs::Permissions::from_mode(0o700))
        .expect("restore the directory for cleanup");

    match refusal {
        Error::StoreCorrupted { path: named, .. } => assert_eq!(
            named,
            path.display().to_string(),
            "the refusal names the file the caller pointed at",
        ),
        Error::LegacyVectorStoreDetected { .. } => {}
        other => panic!(
            "a writer pointed at a non-store in an unwritable directory is owed an answer about \
             the file, with the next step it implies, and got {other:?}",
        ),
    }
    assert!(
        !companion_beside(&path).exists(),
        "a refused open leaves no coordination state beside the file",
    );
}
