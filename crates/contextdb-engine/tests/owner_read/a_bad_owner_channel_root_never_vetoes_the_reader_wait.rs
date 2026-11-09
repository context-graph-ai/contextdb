//! Where the owner's channel lives has no say in whether a store's readers
//! have let go of it.
//!
//! These are two unrelated facts that happen to arrive through one argument. A
//! packaged deployment states a runtime root so its writer and its readers can
//! find each other's CHANNEL. Whether a direct reader is still holding the
//! store is answered by the store's own companion, and has been since the hold
//! moved there. A caller waiting to take over a store is asking the second
//! question; the first is none of its business.
//!
//! It matters because of who calls this door. A consumer that configures a
//! runtime root passes it on every call, including this one, and a supervisor
//! turns a wait it cannot observe into one attempt and a startup failure. So a
//! root that is absent, or misconfigured to a file, or unreadable, must not
//! decide that a store's readers are unobservable: the readers are perfectly
//! observable, the caller simply asked with a bad channel setting attached.
//! Answering `Unobservable` there converts a configuration mistake about
//! CHANNELS into a refusal to wait for READERS, and the caller gives up on a
//! wait that would have ended on its own.
//!
//! The three bad roots below are the three an operator actually produces: a
//! pathname that was never created, a pathname that is a file because a config
//! value was pointed at the wrong thing, and a directory the process cannot
//! read. In each case the real hold is what the wait follows, and the release
//! is what ends it.
//!
//! Owner-channel startup failure remains its own separate concern, reported to
//! the operator as a not-serving answer. It is never a verdict about readers.

use contextdb_core::Value;
use contextdb_core::read_contract::{OwnerReadCancellation, ReadRoute};
use contextdb_engine::local_transport::{channel_socket_path, derive_channel_address};
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    RawRedbWriterOpenObservation, ReaderSourceStartPause,
    canonical_reader_breadcrumb_directory_for_test, pause_readers_at_source_start_for_test,
    reader_breadcrumb_runtime_root_for_test, try_raw_redb_writer_open_for_test,
};
use contextdb_engine::persistence::{ReaderReleaseWait, wait_for_reader_release_in_runtime_dir};
use contextdb_engine::{Database, ReadSession, ReadSessionOptions};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::time::Duration;

/// Failsafe only. Nothing here asserts how long a wake took; this turns a wait
/// that never wakes into a readable failure instead of a hang.
const WATCHDOG: Duration = Duration::from_secs(60);
/// How long a parked wait is observed still parked. Not a latency claim: it is
/// how a journey establishes that the wait is genuinely blocked on the holder
/// rather than having already answered.
const PARKED: Duration = Duration::from_secs(2);
/// A store with real committed content, so the load these journeys stop a
/// reader inside is a real one and not an empty file's.
const WIDE_ROWS: usize = 8;
const WIDE_ROW_BYTES: usize = 512 * 1024;

/// The runtime root the READER is given -- a good one, because this journey is
/// about the root the WAIT is given, and nothing else may be broken.
fn readers_runtime_directory(inside: &Path) -> PathBuf {
    let root = inside.join("reader-runtime");
    std::fs::create_dir(&root).expect("create the reader's runtime directory");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure the reader's runtime directory");
    std::fs::canonicalize(&root).expect("resolve it the way the store will")
}

/// A task-owned file occupies the exact directory where diagnostic
/// breadcrumbs would be published. The valid runtime root and authoritative
/// reader hold remain usable; directory preparation cannot chmod this file
/// into a directory when a fallback runtime is available.
struct BreadcrumbObstruction(PathBuf);

impl Drop for BreadcrumbObstruction {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn make_breadcrumbs_unpublishable(runtime_root: &Path, store: &Path) -> BreadcrumbObstruction {
    let path = canonical_reader_breadcrumb_directory_for_test(store, runtime_root)
        .expect("resolve the engine's diagnostic publication directory");
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .expect("occupy only this store's diagnostic directory with a regular file");
    BreadcrumbObstruction(path)
}

/// A runtime root an operator never created.
fn absent_root(inside: &Path) -> PathBuf {
    inside.join("owner-channel-root-that-was-never-created")
}

/// A runtime root pointed at a file, which is what a misdirected configuration
/// value produces.
fn malformed_root(inside: &Path) -> PathBuf {
    let path = inside.join("owner-channel-root-that-is-a-file");
    std::fs::write(&path, b"a configuration value pointed at the wrong thing")
        .expect("plant a runtime root that is a file");
    path
}

/// A runtime root this process cannot read.
fn unreadable_root(inside: &Path) -> PathBuf {
    let path = inside.join("owner-channel-root-that-cannot-be-read");
    std::fs::create_dir(&path).expect("create the unreadable runtime root");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000))
        .expect("make the runtime root unreadable");
    path
}

fn idle_store(directory: &Path, name: &str) -> PathBuf {
    let path = directory.join(name);
    let database = Database::open(&path).expect("claim a store to seed");
    database
        .execute(
            "CREATE TABLE held (id INTEGER PRIMARY KEY, body TEXT)",
            &HashMap::new(),
        )
        .expect("create the held table");
    for row in 0..WIDE_ROWS {
        database
            .execute(
                "INSERT INTO held (id, body) VALUES ($id, $body)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(row as i64)),
                    ("body".to_owned(), Value::Text("h".repeat(WIDE_ROW_BYTES))),
                ]),
            )
            .expect("seed one wide row");
    }
    database.close().expect("release the seeded store");
    path
}

/// A real direct reader, stopped where its read-image source starts while
/// the store hold is live, and let go when the journey says so. Diagnostic
/// breadcrumb publication is obstructed separately by the fixture.
///
/// Nothing here manufactures a holder. The reader is an ordinary one, holding
/// the store exactly as any reader does; the store simply stops it at that
/// point until this handle releases it.
struct ParkingReader {
    /// This journey's own pause over its own store: the only handle that can
    /// free the reader it stopped, and no handle at all on anyone else's.
    pause: ReaderSourceStartPause,
    ticket: u64,
}

impl ParkingReader {
    fn let_go(&self) {
        self.pause.release(self.ticket);
    }
}

/// A journey that came apart before it let its reader go still lets it go, so
/// a failed assertion leaves no reading thread stopped in the store.
impl Drop for ParkingReader {
    fn drop(&mut self) {
        self.let_go();
    }
}

fn park_a_reader(path: &Path, runtime: &Path) -> (Arc<ParkingReader>, std::thread::JoinHandle<()>) {
    let pause = pause_readers_at_source_start_for_test(path);
    let path = path.to_path_buf();
    let runtime = runtime.to_path_buf();
    let holding = std::thread::spawn(move || {
        let session =
            ReadSession::open_in_runtime_dir(&path, ReadSessionOptions::default(), Some(runtime))
                .expect("an idle store is readable");
        assert_eq!(
            session.route(),
            ReadRoute::File,
            "a direct reader is what holds the store; an owner route holds nothing"
        );
    });
    let ticket = pause
        .wait_for_paused_reader(WATCHDOG)
        .expect("the reader never started its read-image source, so nothing ever held the store");
    (Arc::new(ParkingReader { pause, ticket }), holding)
}

/// The definitive fact, asked of the storage engine itself.
fn assert_really_held(path: &Path, context: &str) {
    assert_eq!(
        try_raw_redb_writer_open_for_test(path),
        RawRedbWriterOpenObservation::DatabaseAlreadyOpen,
        "{context}: the store must actually be held, or this journey proves nothing",
    );
}

/// Run the root-taking door on its own thread, so the journey can watch it
/// stay parked and then release the holder.
fn wait_in_thread(path: &Path, root: PathBuf) -> mpsc::Receiver<ReaderReleaseWait> {
    let (tell, heard) = mpsc::channel();
    let path = path.to_path_buf();
    std::thread::spawn(move || {
        let stop = OwnerReadCancellation::new();
        let _ = tell.send(wait_for_reader_release_in_runtime_dir(
            &path,
            Some(&root),
            &stop,
        ));
    });
    heard
}

fn describe(answer: &ReaderReleaseWait) -> String {
    match answer {
        ReaderReleaseWait::Released => "Released".to_owned(),
        ReaderReleaseWait::Stopped => "Stopped".to_owned(),
        ReaderReleaseWait::Unobservable(error) => format!("Unobservable({error})"),
    }
}

/// One journey, run against each shape of bad owner-channel root: the wait
/// follows the real hold and ends on the real release, whatever the root says.
fn the_wait_follows_the_hold_despite(root_kind: &str, make_root: fn(&Path) -> PathBuf) {
    let directory = tempfile::TempDir::new().expect("task-scoped bad-root directory");
    let readers_runtime = readers_runtime_directory(directory.path());
    let path = idle_store(directory.path(), "bad-root.db");
    let mut registration_roots = vec![readers_runtime.clone()];
    if let Some(default) = reader_breadcrumb_runtime_root_for_test()
        && !registration_roots.contains(&default)
    {
        registration_roots.push(default);
    }
    let obstructions = registration_roots
        .iter()
        .map(|runtime| make_breadcrumbs_unpublishable(runtime, &path))
        .collect::<Vec<_>>();
    let root = make_root(directory.path());

    let (parking, holding) = park_a_reader(&path, &readers_runtime);
    assert_really_held(&path, "a reader parked inside its load");
    for obstruction in &obstructions {
        assert!(
            std::fs::symlink_metadata(&obstruction.0)
                .expect("inspect the deliberate registration obstruction")
                .file_type()
                .is_file(),
            "the parked reader cannot publish a breadcrumb below a regular file"
        );
        assert_eq!(
            std::fs::read_dir(&obstruction.0).unwrap_err().kind(),
            std::io::ErrorKind::NotADirectory,
            "no diagnostic reader registration can exist at this publication path"
        );
    }

    let heard = wait_in_thread(&path, root.clone());
    if let Ok(early) = heard.recv_timeout(PARKED) {
        panic!(
            "with a {root_kind} owner-channel root ({}), the wait answered {} while a reader was \
             holding the store. Where the CHANNEL lives says nothing about whether the READERS \
             have let go, and a caller told this gives up on a wait that would have ended.",
            root.display(),
            describe(&early),
        );
    }

    parking.let_go();
    holding.join().expect("the reader finishes its read");
    let answer = heard
        .recv_timeout(WATCHDOG)
        .unwrap_or_else(|error| panic!("the wait never answered the release: {error}"));
    make_removable(&root);
    assert!(
        matches!(answer, ReaderReleaseWait::Released),
        "with a {root_kind} owner-channel root the real release still ends the wait, but it \
         answered {}",
        describe(&answer),
    );
}

/// Hand a deliberately unreadable fixture back before the task-scoped
/// directory is torn down.
///
/// A directory with no permissions cannot be removed by the cleanup that owns
/// it, so leaving one behind outlives the journey that made it: it accumulates
/// under the task-scoped root and blocks anything that later walks the tree.
fn make_removable(root: &Path) {
    if root.is_dir() {
        let _ = std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700));
    }
}

/// A root nobody created.
#[test]
fn an_absent_owner_channel_root_does_not_decide_whether_readers_hold_the_store() {
    the_wait_follows_the_hold_despite("absent", absent_root);
}

/// A root a configuration value pointed at a file.
#[test]
fn a_malformed_owner_channel_root_does_not_decide_whether_readers_hold_the_store() {
    the_wait_follows_the_hold_despite("malformed", malformed_root);
}

/// A root this process cannot read.
#[test]
fn an_unreadable_owner_channel_root_does_not_decide_whether_readers_hold_the_store() {
    the_wait_follows_the_hold_despite("unreadable", unreadable_root);
}

/// And the other half: a bad root must not make an unheld store look held
/// either. A caller waiting on a store nobody is reading is answered at once,
/// whatever its channel setting says.
#[test]
fn a_bad_owner_channel_root_does_not_make_an_unheld_store_look_held() {
    let directory = tempfile::TempDir::new().expect("task-scoped bad-root directory");
    let path = idle_store(directory.path(), "bad-root-unheld.db");
    let root = malformed_root(directory.path());

    let heard = wait_in_thread(&path, root);
    let answer = heard
        .recv_timeout(WATCHDOG)
        .unwrap_or_else(|error| panic!("the wait never answered an unheld store: {error}"));
    assert!(
        matches!(answer, ReaderReleaseWait::Released),
        "nobody is holding this store, so there is nothing to wait for, but the wait answered {}",
        describe(&answer),
    );
}

/// The root itself is usable, while this store's channel pathname is a regular
/// file. This must not decide a reader-release wait. The file is planted only
/// after the reader has taken its real source hold, so no owner can bind there.
/// The owner-channel path does not set the reader-registration root.
#[test]
fn a_malformed_channel_path_inside_a_usable_root_does_not_veto_the_reader_wait() {
    let directory = tempfile::TempDir::new().expect("task-scoped malformed-channel directory");
    let root = readers_runtime_directory(directory.path());
    let path = idle_store(directory.path(), "malformed-channel.db");
    let (parking, holding) = park_a_reader(&path, &root);
    assert_really_held(&path, "reader before malformed channel publication");

    let channel = channel_socket_path(&root, derive_channel_address(&path).unwrap()).unwrap();
    std::fs::write(&channel, b"not a socket").expect("plant malformed channel pathname");
    let heard = wait_in_thread(&path, root);
    assert!(
        heard.recv_timeout(PARKED).is_err(),
        "a malformed channel path ended the wait before its reader released"
    );
    parking.let_go();
    holding.join().expect("the direct reader completes");
    assert!(matches!(
        heard.recv_timeout(WATCHDOG).expect("wait answers release"),
        ReaderReleaseWait::Released
    ));
}
