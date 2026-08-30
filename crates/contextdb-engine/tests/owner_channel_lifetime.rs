//! A channel exists for exactly as long as somebody is listening on it.
//!
//! The pathname a reader dials is the only evidence it has that a store has an
//! owner, so a socket that outlives its listener is a lie the next reader
//! believes: it dials, gets nothing, and reports a disconnection from an owner
//! that was never there — while the store sitting beside it is a perfectly
//! readable idle file. A writer therefore takes its channel down when it stops
//! serving, and a reader that finds nobody listening concludes there is no
//! owner and reads the file.

#![cfg(all(unix, feature = "test-seams"))]

use contextdb_core::read_contract::{
    OwnerReadCancellation, OwnerReadLimits, OwnerRequestHandler, OwnerServingState,
    ReadClientTimeouts, ReadFailureKind, ReadLimits, ReadRoute,
};
use contextdb_core::{Error, Result, Value};
use contextdb_engine::local_transport::{
    LocalHandshake, ManualDeadlineClock, channel_filesystem_identity, encode_message,
    encode_payload_frame,
};
use contextdb_engine::persistence::read_persistence_test_scaffold;
use contextdb_engine::read_session::{ReadSessionEvent, ReadSessionTestObserver};
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, ReadSession, ReadSessionOptions,
};
use std::collections::HashMap;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

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
    std::fs::create_dir(&root).expect("create the task-scoped owner runtime root");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure the task-scoped owner runtime root");
    root
}

fn owner_options(runtime_dir: PathBuf) -> DatabaseOpenOptions {
    DatabaseOpenOptions {
        owner_reads: OwnerReadConfig {
            runtime_dir: Some(runtime_dir),
            handler: Some(Arc::new(EchoHandler)),
            ..OwnerReadConfig::default()
        },
        ..DatabaseOpenOptions::default()
    }
}

fn short_route_options() -> ReadSessionOptions {
    ReadSessionOptions {
        timeouts: ReadClientTimeouts {
            connect_ms: 100,
            routing_retry_ms: 100,
            response_ms: 200,
        },
        ..ReadSessionOptions::default()
    }
}

fn open_against_silent_channel(path: &Path, runtime_root: &Path) -> Result<ReadSession> {
    open_against_silent_channel_after_first_waiter(path, runtime_root, || {})
}

fn open_against_silent_channel_after_first_waiter(
    path: &Path,
    runtime_root: &Path,
    after_first_waiter: impl FnOnce(),
) -> Result<ReadSession> {
    let clock = ManualDeadlineClock::at(1_000);
    let opening_clock = clock.clone();
    let opening_path = path.to_path_buf();
    let opening_runtime_root = runtime_root.to_path_buf();
    let opening = std::thread::spawn(move || {
        ReadSession::with_runtime_directory_for_test(&opening_runtime_root, || {
            ReadSession::open_with_clock_for_test(
                &opening_path,
                short_route_options(),
                Arc::new(opening_clock),
            )
        })
    });

    let mut deadlines_driven = 0u64;
    let mut after_first_waiter = Some(after_first_waiter);
    while deadlines_driven < 4 && !opening.is_finished() {
        for _ in 0..100_000 {
            if clock.registered_waiter_count() == 1 || opening.is_finished() {
                break;
            }
            std::thread::yield_now();
        }
        if opening.is_finished() {
            break;
        }
        if clock.registered_waiter_count() != 1 {
            clock.advance_to(u64::MAX / 2);
            opening.thread().unpark();
            let _ = opening.join();
            panic!("the silent owner candidate did not register its response deadline");
        }
        if let Some(after_first_waiter) = after_first_waiter.take() {
            after_first_waiter();
        }
        deadlines_driven = deadlines_driven.saturating_add(1);
        clock.advance_to(10_000u64.saturating_mul(deadlines_driven));
    }
    assert!(
        deadlines_driven > 0,
        "the silent owner candidate answered without reaching its response deadline"
    );
    assert!(
        opening.is_finished(),
        "route selection consumed more than the two sanctioned attempts"
    );
    opening.join().expect("join the route-selection proof")
}

fn seed(database: &Database) {
    database
        .execute(
            "CREATE TABLE channel_rows (id INTEGER PRIMARY KEY, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create the channel-lifetime fixture table");
    database
        .execute(
            "INSERT INTO channel_rows (id, payload) VALUES ($id, $payload)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(1)),
                ("payload".to_owned(), Value::Text("kept".to_owned())),
            ]),
        )
        .expect("insert the channel-lifetime fixture row");
}

/// Every socket a runtime root currently holds.
fn channels_under(runtime_root: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(runtime_root) else {
        return Vec::new();
    };
    entries
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|extension| extension.to_str()) == Some("sock"))
        .collect()
}

#[test]
fn a_writer_that_stops_serving_takes_its_channel_down() {
    let directory = tempfile::TempDir::new().expect("task-scoped channel-lifetime directory");
    let runtime_root = secure_runtime_root(directory.path(), "clean-close-runtime");
    let path = directory.path().join("clean-close.db");
    let database = Database::open_with_options(&path, owner_options(runtime_root.clone()))
        .expect("open a writer that serves owner reads");
    seed(&database);
    assert_eq!(
        channels_under(&runtime_root).len(),
        1,
        "a serving writer holds exactly one channel"
    );

    database.close().expect("the writer closes cleanly");
    assert!(
        channels_under(&runtime_root).is_empty(),
        "a writer that stopped serving leaves no channel behind: {:?}",
        channels_under(&runtime_root)
    );
}

#[test]
fn writer_startup_never_unlinks_a_live_channel_merely_because_its_name_exists() {
    let directory = tempfile::TempDir::new().expect("task-scoped channel-lifetime directory");
    let runtime_root = secure_runtime_root(directory.path(), "startup-live-channel-runtime");
    let path = directory.path().join("startup-live-channel.db");
    let first = Database::open_with_options(&path, owner_options(runtime_root.clone()))
        .expect("open the first writer");
    seed(&first);
    let channel = channels_under(&runtime_root)
        .into_iter()
        .next()
        .expect("the first writer publishes one channel");
    let old_record = read_persistence_test_scaffold::inspect_companion_record_for_test(&path)
        .expect("inspect the first writer's authenticated identity");
    let old_handshake = LocalHandshake::current(
        old_record.fields.database_identity,
        old_record.fields.writer_run_number,
        old_record.fields.owner_user,
    );
    first.close().expect("the first writer releases the store");

    let short = directory.path().join("s");
    let _ = std::fs::remove_file(&short);
    let listener = std::os::unix::net::UnixListener::bind(&short)
        .expect("stand a live prior-run responder at a bindable pathname");
    std::fs::rename(&short, &channel).expect("move the live responder to the published pathname");
    let live_identity = channel_filesystem_identity(&channel)
        .expect("record the live responder's filesystem identity");
    listener
        .set_nonblocking(true)
        .expect("make the proof responder nonblocking");
    let bytes = encode_payload_frame(
        &encode_message(&old_handshake).expect("encode the prior-run handshake"),
    )
    .expect("frame the prior-run handshake");
    let stop = Arc::new(AtomicBool::new(false));
    let responder_stop = Arc::clone(&stop);
    let responder = std::thread::spawn(move || {
        let mut answered = 0usize;
        while !responder_stop.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    stream
                        .write_all(&bytes)
                        .expect("answer the bounded startup probe");
                    answered = answered.saturating_add(1);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::yield_now();
                }
                Err(error) => panic!("accept the bounded startup probe: {error}"),
            }
        }
        answered
    });

    let second = Database::open_with_options(&path, owner_options(runtime_root.clone()))
        .expect("a channel-startup failure does not fail the primary database open");
    assert_eq!(
        channel_filesystem_identity(&channel)
            .expect("the live responder remains at the published pathname"),
        live_identity,
        "a live or wrong responder is preserved rather than unlinked by pathname"
    );
    assert_eq!(
        second.owner_read_status().state,
        OwnerServingState::NotServing,
        "the writer remains usable but reports that its own inspection channel could not start"
    );

    let read =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path));
    match read {
        Err(Error::ReadFailure(failure)) => assert_eq!(
            failure.kind(),
            ReadFailureKind::OwnerNotServing,
            "the unrelated responder must not hide the actual holder's recorded startup failure"
        ),
        Err(other) => panic!("the recorded startup failure returned {other:?}"),
        Ok(session) => panic!(
            "a writer whose owner channel failed incorrectly selected the {:?} route",
            session.route()
        ),
    }
    let report = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::owner_report(&path, short_route_options())
    })
    .expect("owner status reports the actual holder's recorded startup failure");
    let recorded = read_persistence_test_scaffold::inspect_companion_record_for_test(&path)
        .expect("inspect the bounded startup failure published for readers");
    assert_eq!(report.status, recorded.fields.owner_read_status);
    assert_eq!(report.status.state, OwnerServingState::NotServing);
    assert!(report.serving.is_none());

    stop.store(true, Ordering::SeqCst);
    let answered = responder.join().expect("join the proof responder");
    assert!(
        answered >= 3,
        "startup, ordinary read, and owner status must each encounter the preserved responder; \
         observed {answered} asks"
    );

    second.close().expect("close the second writer");
    let _ = std::fs::remove_file(&channel);
}

#[test]
fn a_live_owner_is_still_the_route_a_reader_selects() {
    let directory = tempfile::TempDir::new().expect("task-scoped channel-lifetime directory");
    let runtime_root = secure_runtime_root(directory.path(), "live-owner-runtime");
    let path = directory.path().join("live-owner.db");
    let database = Database::open_with_options(&path, owner_options(runtime_root.clone()))
        .expect("open a writer that serves owner reads");
    seed(&database);

    let session =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path))
            .expect("a live owner is reachable");
    assert_eq!(session.route(), ReadRoute::Owner);
    let answered = session
        .execute("SELECT payload FROM channel_rows", &HashMap::new())
        .expect("the live owner answers");
    assert_eq!(answered.rows.len(), 1);

    drop(session);
    database.close().expect("the writer closes cleanly");
}

#[test]
fn a_live_writer_whose_published_channel_is_missing_is_never_reported_absent_or_read_around() {
    let directory = tempfile::TempDir::new().expect("task-scoped channel-lifetime directory");
    let runtime_root = secure_runtime_root(directory.path(), "missing-live-channel-runtime");
    let path = directory.path().join("missing-live-channel.db");
    let database = Database::open_with_options(&path, owner_options(runtime_root.clone()))
        .expect("open a writer that serves owner reads");
    seed(&database);
    let channel = channels_under(&runtime_root)
        .into_iter()
        .next()
        .expect("a serving writer publishes one channel");
    std::fs::remove_file(&channel)
        .expect("make the published listener pathname unavailable while its writer stays live");

    let ordinary =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path));
    let owner_only = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_owner_only(&path, short_route_options())
    });
    let status = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::owner_report(&path, short_route_options())
    });

    for (journey, answer) in [("ordinary read", ordinary), ("owner-only read", owner_only)] {
        match answer {
            Err(Error::ReadFailure(failure)) => assert_eq!(
                failure.kind(),
                ReadFailureKind::OwnerNotServing,
                "{journey}: the companion still proves a live holder, so the answer is not an \
                 absent owner and the file is never opened behind it"
            ),
            Err(other) => panic!("{journey}: expected a typed live-holder refusal, got {other:?}"),
            Ok(session) => panic!(
                "{journey}: a missing published channel incorrectly selected the {:?} route",
                session.route()
            ),
        }
    }
    match status {
        Err(Error::ReadFailure(failure)) => {
            assert_ne!(
                failure.kind(),
                ReadFailureKind::OwnerNotRunning,
                "owner status must not say nobody owns a store whose writer still holds it"
            );
            assert_eq!(failure.kind(), ReadFailureKind::OwnerNotServing);
        }
        Err(other) => panic!("owner status returned an untyped answer: {other:?}"),
        Ok(report) => panic!(
            "an unreachable serving owner was reported as {:?} instead of unavailable",
            report.status
        ),
    }

    database
        .close()
        .expect("close the writer whose pathname was deliberately removed");
}

struct DistrustCompanionAfterOwnerDiscovery {
    companion: PathBuf,
    changed: AtomicBool,
}

impl ReadSessionTestObserver for DistrustCompanionAfterOwnerDiscovery {
    fn observe_event(&self, event: ReadSessionEvent) {
        if !matches!(event, ReadSessionEvent::OwnerResolution { .. })
            || self.changed.swap(true, Ordering::SeqCst)
        {
            return;
        }
        std::fs::set_permissions(&self.companion, std::fs::Permissions::from_mode(0o640))
            .expect("make the companion indeterminate after its owner was discovered");
    }
}

#[test]
fn a_published_owner_whose_channel_and_companion_become_unavailable_is_never_read_around() {
    let directory = tempfile::TempDir::new().expect("task-scoped channel-lifetime directory");
    let runtime_root = secure_runtime_root(directory.path(), "missing-indeterminate-runtime");
    let path = directory.path().join("missing-indeterminate.db");
    let database = Database::open_with_options(&path, owner_options(runtime_root.clone()))
        .expect("open a writer that serves owner reads");
    seed(&database);
    let channel = channels_under(&runtime_root)
        .into_iter()
        .next()
        .expect("a serving writer publishes one channel");
    std::fs::remove_file(&channel)
        .expect("remove the listener after the writer published its channel identity");

    let companion = path.with_extension("db.lock");
    let observer = Arc::new(DistrustCompanionAfterOwnerDiscovery {
        companion: companion.clone(),
        changed: AtomicBool::new(false),
    });
    let watching: Arc<dyn ReadSessionTestObserver> = observer.clone();
    let result = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_observer_for_test(&path, short_route_options(), watching)
    });

    assert!(
        observer.changed.load(Ordering::SeqCst),
        "the proof must invalidate the companion only after discovering its published owner"
    );
    match result {
        Err(Error::ReadFailure(failure)) => assert_eq!(
            failure.kind(),
            ReadFailureKind::OwnerDisconnected,
            "an indeterminate published owner is terminal and never licenses the file route"
        ),
        Err(other) => panic!("the indeterminate published owner returned {other:?}"),
        Ok(session) => panic!(
            "an indeterminate published owner incorrectly selected the {:?} route",
            session.route()
        ),
    }

    std::fs::set_permissions(&companion, std::fs::Permissions::from_mode(0o600))
        .expect("restore the fixture companion");
    database
        .close()
        .expect("close the writer whose coordination path was deliberately disturbed");
}

#[test]
fn an_owner_that_leaves_mid_session_is_answered_by_that_owner_not_by_the_file() {
    let directory = tempfile::TempDir::new().expect("task-scoped channel-lifetime directory");
    let runtime_root = secure_runtime_root(directory.path(), "mid-session-runtime");
    let path = directory.path().join("mid-session.db");
    let database = Database::open_with_options(&path, owner_options(runtime_root.clone()))
        .expect("open a writer that serves owner reads");
    seed(&database);

    let session =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path))
            .expect("a live owner is reachable");
    assert_eq!(session.route(), ReadRoute::Owner);

    // The route was selected against an owner that really was there. Losing it
    // now is that owner's answer, and this session reports it rather than
    // quietly finding another way to the same bytes: a reader is never
    // silently moved to a different view of the store mid-session.
    database.close().expect("the owner goes away mid-session");
    let refused = session
        .execute("SELECT payload FROM channel_rows", &HashMap::new())
        .expect_err("a session whose owner left does not answer from somewhere else");
    let Error::ReadFailure(failure) = &refused else {
        panic!("the session reported {refused:?}");
    };
    assert!(
        matches!(
            failure.kind(),
            ReadFailureKind::OwnerNotServing | ReadFailureKind::OwnerDisconnected
        ),
        "an owner that stopped is reported as such, not routed around: {refused:?}"
    );
    assert_eq!(session.route(), ReadRoute::Owner, "the route never changed");
}

#[test]
fn a_reader_finding_nobody_listening_reads_the_idle_file() {
    let directory = tempfile::TempDir::new().expect("task-scoped channel-lifetime directory");
    let runtime_root = secure_runtime_root(directory.path(), "stale-channel-runtime");
    let path = directory.path().join("stale-channel.db");
    let database = Database::open_with_options(&path, owner_options(runtime_root.clone()))
        .expect("open a writer that serves owner reads");
    seed(&database);
    let channel = channels_under(&runtime_root)
        .into_iter()
        .next()
        .expect("a serving writer holds a channel");
    database.close().expect("the writer closes cleanly");

    // Exactly the state every writer that ran before channels were taken down
    // on close left behind: a socket at the pathname a reader dials, and
    // nobody on the other end of it. Binding and dropping a listener leaves
    // the directory entry, which is what a process that exits without
    // unlinking leaves.
    // A socket is bound at a short pathname (the address a listener may bind
    // is far shorter than a pathname a reader may dial) and the entry is then
    // moved to where the reader will look for it. What is left is a socket
    // file with nobody behind it.
    let short = std::env::temp_dir().join(format!("cdb-stale-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&short);
    let stale = std::os::unix::net::UnixListener::bind(&short)
        .expect("stand a channel up somewhere a listener can bind");
    drop(stale);
    std::fs::rename(&short, &channel)
        .expect("move the abandoned channel to the pathname a reader will dial");
    assert!(
        channel.exists(),
        "the leftover channel is what the next reader will find"
    );

    let session =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path))
            .expect("a store whose owner is gone is still readable");
    assert_eq!(
        session.route(),
        ReadRoute::File,
        "nobody is listening, so there is no owner to ask"
    );
    let answered = session
        .execute("SELECT payload FROM channel_rows", &HashMap::new())
        .expect("the idle file answers");
    assert_eq!(answered.rows.len(), 1);

    assert!(
        !channel.exists(),
        "the reader reclaimed the entry nobody was listening on"
    );
}

#[test]
fn a_reader_finding_no_authenticated_owner_reads_the_idle_file() {
    let directory = tempfile::TempDir::new().expect("task-scoped channel-lifetime directory");
    let runtime_root = secure_runtime_root(directory.path(), "silent-channel-runtime");
    let path = directory.path().join("silent-channel.db");
    let database = Database::open_with_options(&path, owner_options(runtime_root.clone()))
        .expect("open a writer that serves owner reads");
    seed(&database);
    let channel = channels_under(&runtime_root)
        .into_iter()
        .next()
        .expect("a serving writer holds a channel");
    database.close().expect("the writer closes cleanly");
    let store_before = std::fs::read(&path).expect("snapshot the committed store");
    let companion = path.with_extension("db.lock");
    let companion_before = std::fs::read(&companion).expect("snapshot the durable companion");

    // A socket accepting connections is not an authenticated owner. This
    // listener deliberately never accepts or answers, while the companion
    // lock proves no writer owns the store.
    let short = std::env::temp_dir().join(format!("cdb-silent-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&short);
    let _silent = std::os::unix::net::UnixListener::bind(&short)
        .expect("stand a silent channel up where a listener can bind");
    std::fs::rename(&short, &channel)
        .expect("move the silent channel to the pathname the reader dials");

    let session = open_against_silent_channel(&path, &runtime_root)
        .expect("an idle store remains readable when no channel authenticates an owner");
    assert_eq!(
        session.route(),
        ReadRoute::File,
        "only a live authenticated owner may select the owner route"
    );
    let answered = session
        .execute("SELECT payload FROM channel_rows", &HashMap::new())
        .expect("the idle committed file answers");
    assert_eq!(answered.rows.len(), 1);
    drop(session);

    assert_eq!(
        std::fs::read(&path).expect("re-read the committed store"),
        store_before,
        "route selection and direct inspection never mutate the store"
    );
    assert_eq!(
        std::fs::read(&companion).expect("re-read the durable companion"),
        companion_before,
        "route selection and direct inspection never mutate the durable companion"
    );
}

#[test]
fn a_silent_channel_cannot_route_around_a_live_writer_through_an_alias() {
    let directory = tempfile::TempDir::new().expect("task-scoped channel-lifetime directory");
    let runtime_root = secure_runtime_root(directory.path(), "held-silent-channel-runtime");
    let path = directory.path().join("held-silent-channel.db");
    let database = Database::open_with_options(&path, owner_options(runtime_root.clone()))
        .expect("open a writer that serves owner reads");
    seed(&database);
    let alias = directory.path().join("held-silent-channel-alias.db");
    std::os::unix::fs::symlink(&path, &alias).expect("name the same live store through an alias");
    let channel = channels_under(&runtime_root)
        .into_iter()
        .next()
        .expect("a serving writer holds a channel");

    // Keep the writer's real companion lock held while replacing only its
    // reachable pathname with a listener that never authenticates. An
    // ordinary reader must fail on that owner candidate, never open the file
    // behind a writer that still owns it.
    std::fs::remove_file(&channel).expect("unlink the owner's reachable pathname");
    let short = std::env::temp_dir().join(format!("cdb-hs-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&short);
    let silent = std::os::unix::net::UnixListener::bind(&short)
        .expect("stand a silent replacement channel up");
    std::fs::rename(&short, &channel).expect("move the silent channel to the published pathname");

    let result = open_against_silent_channel(&alias, &runtime_root);
    match result {
        Err(Error::ReadFailure(failure)) => assert_eq!(
            failure.kind(),
            ReadFailureKind::OwnerTimeout,
            "the live owner candidate answers in the channel vocabulary"
        ),
        Err(other) => panic!("the live owner candidate returned {other:?}"),
        Ok(_) => panic!("a silent channel licensed direct access behind a live writer"),
    }

    drop(silent);
    std::fs::remove_file(&channel).expect("remove the silent replacement channel");
    database.close().expect("close the still-owning writer");
}

#[test]
fn a_dangling_symlink_never_routes_through_its_alias_companion() {
    let directory = tempfile::TempDir::new().expect("task-scoped dangling-alias directory");
    let runtime_root = secure_runtime_root(directory.path(), "dangling-alias-runtime");
    let alias = directory.path().join("dangling-alias.db");
    let database = Database::open_with_options(&alias, owner_options(runtime_root.clone()))
        .expect("open the store whose old alias identity stays live");
    seed(&database);

    // The pathname no longer names the live store. It is now a symlink to a
    // missing target beneath a missing parent, while the unlinked old store's
    // companion and authenticated owner remain reachable under the alias's
    // former identity. Route selection must follow what the pathname names
    // now, not ask the old alias owner.
    std::fs::remove_file(&alias).expect("unlink the old store pathname");
    let missing_target = directory
        .path()
        .join("parent-that-does-not-exist")
        .join("missing-target.db");
    std::os::unix::fs::symlink(&missing_target, &alias)
        .expect("replace the old store pathname with a dangling symlink");

    let result =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&alias));
    match result {
        Err(Error::ReadFailure(failure)) => assert_eq!(
            failure.kind(),
            ReadFailureKind::StoreNotFound,
            "the missing symlink target keeps the stable missing-store answer"
        ),
        Err(other) => panic!("the dangling target returned {other:?}"),
        Ok(session) => panic!(
            "the dangling target incorrectly selected the old alias's {:?} route",
            session.route()
        ),
    }
    assert!(
        !missing_target
            .parent()
            .expect("missing target has a parent")
            .exists(),
        "reading through the dangling alias must not create its target parent"
    );

    database
        .close()
        .expect("close the unlinked old store and its owner service");
}

#[test]
fn an_unlinked_store_name_never_routes_to_the_old_live_owner() {
    let directory = tempfile::TempDir::new().expect("task-scoped unlinked-store directory");
    let runtime_root = secure_runtime_root(directory.path(), "unlinked-store-runtime");
    let path = directory.path().join("unlinked-store.db");
    let database = Database::open_with_options(&path, owner_options(runtime_root.clone()))
        .expect("open the store whose old owner stays live");
    seed(&database);

    std::fs::remove_file(&path).expect("unlink the pathname while the old owner retains its inode");
    let result =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path));
    match result {
        Err(Error::ReadFailure(failure)) => assert_eq!(
            failure.kind(),
            ReadFailureKind::StoreNotFound,
            "the pathname no longer names the old owner's store"
        ),
        Err(other) => panic!("the unlinked store name returned {other:?}"),
        Ok(session) => panic!(
            "the unlinked store name incorrectly selected the old owner's {:?} route",
            session.route()
        ),
    }

    database
        .close()
        .expect("close the old owner of the now-unlinked store");
}

#[test]
fn an_untrusted_idle_companion_does_not_hide_an_existing_idle_store() {
    let directory = tempfile::TempDir::new().expect("task-scoped idle-companion directory");
    let runtime_root = secure_runtime_root(directory.path(), "idle-companion-runtime");
    let path = directory.path().join("idle-companion.db");
    let database = Database::open_with_options(&path, owner_options(runtime_root.clone()))
        .expect("create an ordinary file-backed store");
    seed(&database);
    database.close().expect("leave the store idle");

    // A stale or damaged coordination artifact is not ownership. When there
    // is no channel candidate to fall back from, redb's read-only open remains
    // the authority for the existing idle file, just as it is for a copied
    // store whose companion is absent entirely.
    let companion = path.with_extension("db.lock");
    std::fs::set_permissions(&companion, std::fs::Permissions::from_mode(0o640))
        .expect("make the idle companion fail the owner-record trust check");
    let session =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path))
            .expect("an untrusted idle companion must not hide the readable idle store");
    assert_eq!(session.route(), ReadRoute::File);
    let answered = session
        .execute("SELECT payload FROM channel_rows", &HashMap::new())
        .expect("the existing idle file answers directly");
    assert_eq!(answered.rows.len(), 1);
    drop(session);

    std::fs::set_permissions(&companion, std::fs::Permissions::from_mode(0o600))
        .expect("restore the fixture companion");
}

#[test]
fn an_untrusted_companion_never_licenses_a_file_fallback() {
    let directory = tempfile::TempDir::new().expect("task-scoped channel-lifetime directory");
    let runtime_root = secure_runtime_root(directory.path(), "untrusted-companion-runtime");
    let path = directory.path().join("untrusted-companion.db");
    let database = Database::open_with_options(&path, owner_options(runtime_root.clone()))
        .expect("open a writer that serves owner reads");
    seed(&database);
    let channel = channels_under(&runtime_root)
        .into_iter()
        .next()
        .expect("a serving writer holds a channel");
    database.close().expect("the writer closes cleanly");

    let short = std::env::temp_dir().join(format!("cdb-uc-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&short);
    let silent = std::os::unix::net::UnixListener::bind(&short)
        .expect("stand a silent replacement channel up");
    std::fs::rename(&short, &channel).expect("move it to the published pathname");

    let companion = path.with_extension("db.lock");
    let result = open_against_silent_channel_after_first_waiter(&path, &runtime_root, || {
        std::fs::set_permissions(&companion, std::fs::Permissions::from_mode(0o640))
            .expect("make the companion fail its trust boundary after channel discovery");
    });
    match result {
        Err(Error::ReadFailure(failure)) => assert_eq!(
            failure.kind(),
            ReadFailureKind::OwnerTimeout,
            "an indeterminate owner stays a channel refusal"
        ),
        Err(other) => panic!("the indeterminate owner returned {other:?}"),
        Ok(_) => panic!("an untrusted companion licensed a direct-file fallback"),
    }

    drop(silent);
    std::fs::remove_file(&channel).expect("remove the silent replacement channel");
    std::fs::set_permissions(&companion, std::fs::Permissions::from_mode(0o600))
        .expect("restore the fixture companion");
}

#[test]
fn selecting_a_route_against_a_draining_owner_takes_none_of_its_capacity() {
    let directory = tempfile::TempDir::new().expect("task-scoped channel-lifetime directory");
    let runtime_root = secure_runtime_root(directory.path(), "draining-capacity-runtime");
    let path = directory.path().join("draining-capacity.db");
    let mut options = owner_options(runtime_root.clone());
    // One slot, so anything a reader leaves behind while looking for a route
    // is the difference between an owner that can answer and one that cannot.
    options.owner_reads.limits = OwnerReadLimits {
        limits: ReadLimits::default(),
        concurrency: 1,
    };
    let database = Database::open_with_options(&path, options)
        .expect("open a one-slot writer that serves owner reads");
    seed(&database);

    // A reader that finds the owner draining is told so. Asking the question
    // must not cost the owner its only slot: a route that was never selected
    // leaves nothing behind on the channel it looked at.
    let refused =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path));
    match refused {
        Ok(session) => {
            assert_eq!(session.route(), ReadRoute::Owner);
            drop(session);
        }
        Err(Error::ReadFailure(failure)) => {
            assert_ne!(
                failure.kind(),
                ReadFailureKind::OwnerAtCapacity,
                "looking for a route must not consume the capacity it is asking about"
            );
        }
        Err(other) => panic!("route selection reported {other:?}"),
    }

    // The owner still has its slot: a reader can still be served.
    let session =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path))
            .expect("the one-slot owner is still reachable");
    assert_eq!(session.route(), ReadRoute::Owner);
    let answered = session
        .execute("SELECT payload FROM channel_rows", &HashMap::new())
        .expect("the one-slot owner answers after the earlier route selection");
    assert_eq!(answered.rows.len(), 1);

    drop(session);
    database.close().expect("the writer closes cleanly");
}
