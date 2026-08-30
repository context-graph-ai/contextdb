#![cfg(all(unix, feature = "test-seams"))]
//! A writer that has claimed a store but has not yet published its serving
//! decision is never reported absent.
//!
//! The window is real and unavoidable: a writer takes the store, publishes the
//! run a reader needs in order to dial it, and only afterwards learns whether
//! its own inspection channel came up. Between those two moments there IS an
//! owner -- it holds the store's companion exclusively -- and it has said
//! nothing about serving yet.
//!
//! What a consumer does with `owner_not_running` is why this matters. That
//! answer means "nobody owns this store", and it is the licence for an
//! absent-store fallback: create it, seed it, take it over. Handing that answer
//! out while a writer is holding the store points the consumer at a store that
//! is not free, and the consumer has no way to tell it was misled.
//!
//! So when a caller's own declared budget runs out before the writer publishes,
//! the answer it gets has to be BOTH of these things: not owner-absent, and not
//! the answer a caller gets when it dials a store no writer has claimed and
//! nothing answers. The second is what keeps the two situations apart at the
//! point where a consumer decides whether the store is there at all -- a
//! transport timeout against an unclaimed store is a store nobody holds, and
//! this is not.
//!
//! Both fixtures below are real. The claim is a real writer parked between
//! claiming the store and publishing its decision, through the startup
//! observation the open surface already carries. The unclaimed control is a
//! real channel pathname with a real socket behind it that never answers, and a
//! companion no writer holds.

use contextdb_core::read_contract::{
    OwnerReadCancellation, OwnerReadLimits, OwnerServiceTimeouts, ReadClientTimeouts,
    ReadFailureKind, ReadLimits, ReadRoute,
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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::Duration;

/// Failsafe only. Nothing here asserts how long anything took; this turns a
/// fixture that never reaches its state into a readable failure rather than a
/// hang.
const WATCHDOG: Duration = Duration::from_secs(60);

/// The budget the caller in these journeys declares for reaching a route. It is
/// the caller's own statement of how long it is willing to wait, and it is the
/// only thing that may end its wait.
const DECLARED_ROUTING_RETRY_MS: u64 = 400;
const DECLARED_CONNECT_MS: u64 = 400;

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

fn declared_timeouts() -> ReadClientTimeouts {
    ReadClientTimeouts {
        connect_ms: DECLARED_CONNECT_MS,
        routing_retry_ms: DECLARED_ROUTING_RETRY_MS,
        ..ReadClientTimeouts::default()
    }
}

fn session_options() -> ReadSessionOptions {
    ReadSessionOptions {
        limits: ReadLimits::default(),
        timeouts: declared_timeouts(),
        ..ReadSessionOptions::default()
    }
}

fn owner_options(runtime_dir: PathBuf) -> DatabaseOpenOptions {
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

fn seed(path: &Path) {
    let database = Database::open(path).expect("create the committed fixture");
    database
        .execute(
            "CREATE TABLE claimed_rows (id INTEGER, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create the fixture table");
    database
        .execute(
            "INSERT INTO claimed_rows (id, payload) VALUES ($id, $payload)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(1)),
                ("payload".to_owned(), Value::Text("claimed".to_owned())),
            ]),
        )
        .expect("insert the fixture row");
    database.close().expect("release the seeded store");
}

/// Parks a writer between claiming the store and publishing its serving
/// decision, and lets the journey decide when it may go on.
///
/// The persistence-opened boundary IS the claim: the companion has been taken
/// exclusively and the run a reader would dial has been published, while the
/// serving decision does not exist yet. Nothing is manufactured here -- this is
/// the writer's own startup, stopped where every writer really is for a moment.
#[derive(Default)]
struct WriterParkedInsideItsClaim {
    claimed: (Mutex<bool>, Condvar),
    release: (Mutex<bool>, Condvar),
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
                "the writer never reached the point where it holds the store, so nothing was \
                 claimed",
            );
        }
    }

    fn let_go(&self) {
        let (flag, signal) = &self.release;
        *flag.lock().expect("claim-window state") = true;
        signal.notify_all();
    }
}

/// A real writer, holding the store with its serving decision unpublished.
fn park_a_writer_inside_its_claim(
    path: &Path,
    runtime_root: &Path,
) -> (
    Arc<WriterParkedInsideItsClaim>,
    std::thread::JoinHandle<String>,
) {
    let parked = Arc::new(WriterParkedInsideItsClaim::default());
    let observer: Arc<dyn ReadSessionTestObserver> = parked.clone();
    let path = path.to_path_buf();
    let runtime_root = runtime_root.to_path_buf();
    let writer = std::thread::spawn(move || {
        let mut options = owner_options(runtime_root);
        options.test_observer = Some(observer);
        match Database::open_with_options(&path, options) {
            Ok(database) => {
                let state = format!("{:?}", database.owner_read_status().state);
                database.close().expect("close the parked writer");
                state
            }
            Err(error) => format!("refused: {error}"),
        }
    });
    parked.wait_until_claimed();
    (parked, writer)
}

/// A store no writer has claimed, whose channel pathname has a real socket
/// behind it that never answers a handshake.
///
/// This is the control the claim-window answer must stay distinguishable from,
/// and it is built the way it really happens: a writer served here and went
/// away, and something is listening at the pathname it published without ever
/// replying. The companion is not held, so nobody owns this store.
struct UnclaimedStoreThatNeverAnswers {
    path: PathBuf,
    _listener: std::os::unix::net::UnixListener,
}

fn unclaimed_store_that_never_answers(
    directory: &Path,
    runtime_root: &Path,
) -> UnclaimedStoreThatNeverAnswers {
    let path = directory.join("unclaimed.db");
    seed(&path);
    let database = Database::open_with_options(&path, owner_options(runtime_root.to_path_buf()))
        .expect("a writer serves here once so a channel pathname is published");
    let channel = channels_under(runtime_root)
        .into_iter()
        .next()
        .expect("a serving writer holds a channel");
    database
        .close()
        .expect("the writer goes away, leaving the store unclaimed");

    // A listener may bind only at a short pathname, so it is bound elsewhere
    // and its directory entry moved to the pathname a reader dials. The socket
    // itself is unchanged and still listening -- it simply never accepts, which
    // is exactly what a caller's connect deadline exists for.
    let short = std::env::temp_dir().join(format!("cdb-silent-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&short);
    let listener = std::os::unix::net::UnixListener::bind(&short)
        .expect("stand a silent channel up where a listener can bind");
    std::fs::rename(&short, &channel).expect("move the silent channel to the dialled pathname");
    assert!(
        channel.exists(),
        "the silent channel is what a reader finds"
    );

    UnclaimedStoreThatNeverAnswers {
        path,
        _listener: listener,
    }
}

fn channels_under(runtime_root: &Path) -> Vec<PathBuf> {
    fn collect(directory: &Path, found: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(directory) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(metadata) = std::fs::symlink_metadata(&path) else {
                continue;
            };
            if metadata.file_type().is_dir() {
                collect(&path, found);
            } else if std::os::unix::fs::FileTypeExt::is_socket(&metadata.file_type()) {
                found.push(path);
            }
        }
    }
    let mut found = Vec::new();
    collect(runtime_root, &mut found);
    found
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

/// A caller whose declared budget expires while a writer still holds the store
/// is told something other than "nobody owns this store" -- and something other
/// than what an unclaimed store that never answers says.
#[test]
fn a_budget_that_expires_inside_the_claim_window_never_licenses_an_absent_store_fallback() {
    let directory = tempfile::TempDir::new().expect("task-scoped claim-window directory");
    let runtime_root = secure_runtime_root(directory.path(), "claim-window-runtime");

    // The control is measured first, so its answer is something this journey
    // observed rather than a value written down here and hoped for.
    let unclaimed = unclaimed_store_that_never_answers(directory.path(), &runtime_root);
    let unclaimed_answer = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        refusal_kind(
            ReadSession::open_owner_only(&unclaimed.path, session_options()),
            "a store no writer has claimed, dialled through a channel that never answers",
        )
    });

    // The control is only a control if it really is a TRANSPORT timeout: a
    // store nobody has claimed whose channel is dialled and never replies. If
    // it collapsed into owner-absent there would be nothing here to stay
    // distinguishable from.
    assert_ne!(
        unclaimed_answer,
        ReadFailureKind::OwnerNotRunning,
        "the unclaimed control must be a store whose channel was dialled and timed out, not one \
         whose channel was never reached",
    );

    let path = directory.path().join("claim-window.db");
    seed(&path);
    let (parked, writer) = park_a_writer_inside_its_claim(&path, &runtime_root);

    let claim_window_answer = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        refusal_kind(
            ReadSession::open_owner_only(&path, session_options()),
            "a store a writer holds with its serving decision unpublished",
        )
    });

    parked.let_go();
    let writer_state = writer.join().expect("join the parked writer");
    assert_eq!(
        writer_state, "Serving",
        "the parked writer really did hold this store and go on to serve it",
    );

    assert_ne!(
        claim_window_answer,
        ReadFailureKind::OwnerNotRunning,
        "a writer was holding this store the whole time, so owner-absent is a settled verdict \
         that licenses a consumer to treat an owned store as one it may create and take",
    );
    assert_ne!(
        claim_window_answer, unclaimed_answer,
        "a caller cannot tell a held claim from a store nobody has claimed if both answer \
         {unclaimed_answer:?}, and the difference is exactly what decides whether an \
         absent-store fallback is allowed",
    );
}

/// The same window, asked the plainest question a consumer asks: is this store
/// owned? A held claim is never answered with owner-absent, whichever door the
/// question comes through.
#[test]
fn an_owner_status_ask_inside_the_claim_window_is_not_answered_owner_absent() {
    let directory = tempfile::TempDir::new().expect("task-scoped claim-window directory");
    let runtime_root = secure_runtime_root(directory.path(), "claim-window-status-runtime");
    let path = directory.path().join("claim-window-status.db");
    seed(&path);
    let (parked, writer) = park_a_writer_inside_its_claim(&path, &runtime_root);

    let answer = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::owner_status(&path, session_options())
    });

    parked.let_go();
    let writer_state = writer.join().expect("join the parked writer");
    assert_eq!(
        writer_state, "Serving",
        "the parked writer really did hold this store and go on to serve it",
    );

    match answer {
        Ok(status) => {
            // A published decision is a fine answer; the window is private
            // coordination state and has no public state of its own.
            assert_ne!(
                format!("{:?}", status.state),
                "NotApplicable",
                "a store held by a live writer is not a store owner inspection does not apply to",
            );
        }
        Err(Error::ReadFailure(failure)) => assert_ne!(
            failure.kind(),
            ReadFailureKind::OwnerNotRunning,
            "a writer is holding this store, so it is not a store nobody owns",
        ),
        Err(other) => panic!("the owner-status ask answered {other:?}"),
    }
}

/// The window costs the writer nothing. A caller re-observing through its own
/// deadlines never touches the store file and never delays the startup it is
/// waiting on, so the writer it was asking about still ends up serving.
#[test]
fn asking_inside_the_claim_window_leaves_the_writer_s_startup_untouched() {
    let directory = tempfile::TempDir::new().expect("task-scoped claim-window directory");
    let runtime_root = secure_runtime_root(directory.path(), "claim-window-cost-runtime");
    let path = directory.path().join("claim-window-cost.db");
    seed(&path);
    let (parked, writer) = park_a_writer_inside_its_claim(&path, &runtime_root);

    let (asked_tx, asked_rx) = mpsc::channel::<ReadFailureKind>();
    let ask_path = path.clone();
    let ask_runtime = runtime_root.clone();
    let asking = std::thread::spawn(move || {
        let kind = ReadSession::with_runtime_directory_for_test(&ask_runtime, || {
            refusal_kind(
                ReadSession::open_owner_only(&ask_path, session_options()),
                "a caller asking while the writer holds its claim",
            )
        });
        let _ = asked_tx.send(kind);
    });

    let asked = asked_rx
        .recv_timeout(WATCHDOG)
        .expect("the ask answers inside the caller's own declared budget");
    asking.join().expect("join the asking caller");

    parked.let_go();
    let writer_state = writer.join().expect("join the parked writer");
    assert_eq!(
        writer_state, "Serving",
        "asking about a starting writer must not cost it the store it was claiming",
    );
    assert_ne!(
        asked,
        ReadFailureKind::OwnerNotRunning,
        "the ask that cost the writer nothing still must not report the store unowned",
    );
    assert_eq!(
        reader_breadcrumbs(&runtime_root),
        Vec::<PathBuf>::new(),
        "re-observing a claim never hydrates the store, so no reader breadcrumb exists",
    );
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

/// The route a caller is owed once the writer's decision exists is the owner's
/// own, so the file is never what answers here.
#[test]
fn a_writer_that_publishes_is_reached_through_its_own_channel() {
    let directory = tempfile::TempDir::new().expect("task-scoped claim-window directory");
    let runtime_root = secure_runtime_root(directory.path(), "claim-window-route-runtime");
    let path = directory.path().join("claim-window-route.db");
    seed(&path);
    let database = Database::open_with_options(&path, owner_options(runtime_root.clone()))
        .expect("a writer that has published its decision");

    let route = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_owner_only(&path, session_options())
            .expect("a published, serving writer answers the owner-only door")
            .route()
    });
    assert_eq!(route, ReadRoute::Owner);

    database.close().expect("close the fixture writer");
}
