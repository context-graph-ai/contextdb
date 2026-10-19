//! A read that has finished leaves nothing of its own in a runtime directory:
//! no reader registry, no registry lock, no erasure fence, and no `contextdb`
//! child of the platform directory that it created. An operator-supplied
//! directory itself always stays.
//!
//! Nothing is removed while a writer that can write or purge holds the store,
//! and a fence that still carries crash-recovery state is never removed: when
//! either is true the files stay until whoever comes last can remove them.
//!
//! Every test runs again in a child process whose platform runtime directory
//! is private to it, so the assertions see only what these reads published.

use contextdb_core::Value;
use contextdb_core::read_contract::ReadFailureKind;
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    ReaderFenceDepartureForTest, arm_purge_park_before_durable_commit_for_test,
    arm_reader_fence_departure_for_test, arm_reader_leave_park_for_test,
    install_reader_join_wait_hook_for_test, reader_fence_departure_armed_for_test,
};
use contextdb_engine::{Database, ReadSession, ReadSessionOptions};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

const PLATFORM_CHILD: &str = "CONTEXTDB_RUNTIME_CLEANUP_CHILD";
const CRASH_PATH: &str = "CONTEXTDB_RUNTIME_CLEANUP_CRASH_PATH";
const ABORT_READER_PATH: &str = "CONTEXTDB_RUNTIME_CLEANUP_ABORT_READER_PATH";

/// Run `test` again in a child whose `XDG_RUNTIME_DIR` is a fresh private
/// directory. Returns true in the parent, which then has nothing more to do.
fn run_with_platform_runtime(test: &str) -> bool {
    if std::env::var_os(PLATFORM_CHILD).is_some() {
        return false;
    }
    let directory = tempfile::tempdir().unwrap();
    let platform = directory.path().join("platform-runtime");
    std::fs::create_dir(&platform).unwrap();
    std::fs::set_permissions(&platform, std::fs::Permissions::from_mode(0o700)).unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(crate::child_test_filter(module_path!(), test))
        .arg("--nocapture")
        .env("XDG_RUNTIME_DIR", &platform)
        .env(PLATFORM_CHILD, "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "runtime-cleanup child failed: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    true
}

fn platform() -> PathBuf {
    PathBuf::from(std::env::var_os("XDG_RUNTIME_DIR").expect("child runs with a platform dir"))
}

fn supplied_root(directory: &Path) -> PathBuf {
    let root = directory.join("supplied-runtime");
    std::fs::create_dir(&root).unwrap();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    root
}

/// Every reader-registry artifact standing under `root`: registry directories,
/// registry locks and fences.
fn registry_residue(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("storage-") || name.ends_with(".fence") {
                found.push(path.clone());
            }
            if path.is_dir() {
                pending.push(path);
            }
        }
    }
    found.sort();
    found
}

/// Everything standing under `root`, for a failure message.
fn everything_under(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory)
            .into_iter()
            .flatten()
            .flatten()
        {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path.clone());
            }
            found.push(path);
        }
    }
    found.sort();
    found
}

fn assert_no_reader_files(label: &str) {
    assert!(
        !platform().join("contextdb").exists(),
        "{label}: a finished read leaves nothing in the platform runtime directory, \
         not even the `contextdb` directory it created; left: {:?}",
        everything_under(&platform())
    );
}

fn seed(path: &Path) {
    let db = Database::open(path).expect("create the runtime-cleanup fixture");
    db.execute(
        "CREATE TABLE erased_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL) HISTORY ALL SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .unwrap();
    for id in 1..=3_i64 {
        db.execute(
            "INSERT INTO erased_rows (id, payload) VALUES ($id, 'secret')",
            &HashMap::from([("id".to_owned(), Value::Int64(id))]),
        )
        .unwrap();
    }
    db.close().unwrap();
}

fn read_all(session: &ReadSession) -> usize {
    session
        .execute("SELECT id FROM erased_rows", &HashMap::new())
        .unwrap()
        .rows
        .len()
}

#[test]
fn a_finished_read_leaves_nothing_in_either_runtime_root() {
    if run_with_platform_runtime("a_finished_read_leaves_nothing_in_either_runtime_root") {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("finished.db");
    let supplied = supplied_root(directory.path());
    seed(&path);
    let session = ReadSession::open_in_runtime_dir(
        &path,
        ReadSessionOptions::default(),
        Some(supplied.clone()),
    )
    .unwrap();
    assert_eq!(read_all(&session), 3);
    assert!(
        !registry_residue(&platform()).is_empty(),
        "a live read is registered in the platform directory"
    );
    drop(session);
    assert_no_reader_files("sole reader, two roots");
    assert!(
        registry_residue(&supplied).is_empty(),
        "nothing of the read is left in the supplied root: {:?}",
        registry_residue(&supplied)
    );
    assert!(
        supplied.is_dir(),
        "the supplied root itself is the operator's"
    );
    assert!(platform().is_dir(), "the platform directory itself stays");
}

#[test]
fn a_store_without_a_trusted_companion_leaves_nothing() {
    if run_with_platform_runtime("a_store_without_a_trusted_companion_leaves_nothing") {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("no-companion.db");
    seed(&path);
    std::fs::remove_file(contextdb_core::store_companion_path(&path)).unwrap();
    let session = ReadSession::open(&path).unwrap();
    assert_eq!(read_all(&session), 3);
    drop(session);
    assert_no_reader_files("no trusted companion, so no fence");
}

#[test]
fn a_reader_leaving_under_a_live_writer_leaves_its_registry_for_the_writers_close() {
    if run_with_platform_runtime(
        "a_reader_leaving_under_a_live_writer_leaves_its_registry_for_the_writers_close",
    ) {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live-writer.db");
    seed(&path);
    let session = ReadSession::open(&path).unwrap();
    assert_eq!(read_all(&session), 3);
    let writer = Database::open(&path).expect("a writer opens once the reader has hydrated");
    drop(session);
    assert!(
        !registry_residue(&platform()).is_empty(),
        "nothing is removed while a writer that can purge holds the store"
    );
    writer.close().unwrap();
    assert_no_reader_files("the writer's close removes what the reader left");
}

#[test]
fn a_writer_close_leaves_a_contended_registry_for_the_next_reader() {
    use contextdb_engine::{OwnerReadTestHooks, QuiescentStopPhase};

    if run_with_platform_runtime("a_writer_close_leaves_a_contended_registry_for_the_next_reader") {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("contended.db");
    seed(&path);
    let session = ReadSession::open(&path).unwrap();
    // The registry is held only from the moment the close has drained -- after
    // its last write, whose registry refresh would otherwise wait on it -- to
    // just past the close's own cleanup attempt.
    let held = std::sync::Arc::new(std::sync::Mutex::new(None::<std::fs::File>));
    let holder = std::sync::Arc::clone(&held);
    let stop_observer = std::sync::Arc::new(move |phase: QuiescentStopPhase| {
        if phase != QuiescentStopPhase::AcceptedDrained {
            return;
        }
        let lock_path = registry_residue(&platform())
            .into_iter()
            .find(|path| path.to_string_lossy().ends_with(".snapshot-registry-lock"))
            .expect("the departed reader's registry lock is still there");
        let file = std::fs::File::open(lock_path).unwrap();
        fs2::FileExt::lock_exclusive(&file).unwrap();
        *holder.lock().unwrap() = Some(file);
    });
    let writer = Database::open_with_options(
        &path,
        contextdb_engine::DatabaseOpenOptions {
            owner_reads: contextdb_engine::OwnerReadConfig {
                test_hooks: Some(OwnerReadTestHooks {
                    clock: std::sync::Arc::new(
                        contextdb_engine::local_transport::ManualDeadlineClock::default(),
                    ),
                    drain_started: std::sync::mpsc::channel().0,
                    active_request_registered: None,
                    accept_thread_exited: None,
                    accept_thread_joined: None,
                    service_dropped: None,
                    quiescent_stop: Some(stop_observer),
                }),
                ..contextdb_engine::OwnerReadConfig::default()
            },
            ..contextdb_engine::DatabaseOpenOptions::default()
        },
    )
    .unwrap();
    drop(session);
    writer.close().unwrap();
    assert!(
        held.lock().unwrap().is_some(),
        "the close reached its drained phase holding the registry"
    );
    assert!(
        !registry_residue(&platform()).is_empty(),
        "a close that finds the registry held leaves it alone rather than waiting"
    );
    drop(held.lock().unwrap().take());
    let next = ReadSession::open(&path).unwrap();
    assert_eq!(read_all(&next), 3);
    drop(next);
    assert_no_reader_files("the next reader to leave removes it");
}

#[test]
fn only_the_last_of_two_readers_removes_the_registry() {
    if run_with_platform_runtime("only_the_last_of_two_readers_removes_the_registry") {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("two-readers.db");
    seed(&path);
    let first = ReadSession::open(&path).unwrap();
    let second = ReadSession::open(&path).unwrap();
    assert_eq!(read_all(&first), 3);
    drop(first);
    assert!(
        !registry_residue(&platform()).is_empty(),
        "a reader that is not the last leaves the registry to the one still reading"
    );
    assert_eq!(read_all(&second), 3);
    drop(second);
    assert_no_reader_files("the last of two readers");
}

fn assert_erasure(error: contextdb_core::Error) {
    assert!(matches!(
        error,
        contextdb_core::Error::ReadFailure(failure)
            if failure.kind() == ReadFailureKind::AuthoritativeErasure
    ));
}

/// A registration whose fence departed after the reader opened it is
/// refused, and the session's one ownership-race retry registers again
/// against a live fence: a purge after that reaches the reader.
#[test]
fn a_registration_whose_fence_departed_registers_again_and_sees_a_purge() {
    if run_with_platform_runtime(
        "a_registration_whose_fence_departed_registers_again_and_sees_a_purge",
    ) {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("fence-departed.db");
    seed(&path);
    arm_reader_fence_departure_for_test(ReaderFenceDepartureForTest::Fence);
    let session = ReadSession::open(&path).expect("the retried registration registers");
    assert!(
        !reader_fence_departure_armed_for_test(),
        "the first registration met a departed fence"
    );
    assert_eq!(read_all(&session), 3);
    let writer = Database::open(&path).unwrap();
    writer
        .execute(
            "PURGE FROM erased_rows WHERE payload = 'secret'",
            &HashMap::new(),
        )
        .expect("durable authoritative purge");
    assert_erasure(
        session
            .execute("SELECT payload FROM erased_rows", &HashMap::new())
            .unwrap_err(),
    );
    writer.close().unwrap();
    drop(session);
    assert_no_reader_files("after a departed fence");
}

/// A join and a leave of one runtime root never interleave. A leave parked
/// holding the root's serializer -- with its own record already being taken
/// away -- makes an arriving join wait; released, the leave finishes, the join
/// registers into whatever the leave left, and once it too leaves nothing is
/// left at all.
#[test]
fn a_join_waits_for_a_leave_of_the_same_runtime_root() {
    if run_with_platform_runtime("a_join_waits_for_a_leave_of_the_same_runtime_root") {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("join-leave.db");
    seed(&path);
    let leaving = ReadSession::open(&path).unwrap();
    assert_eq!(read_all(&leaving), 3);
    let park = arm_reader_leave_park_for_test();
    let leave = std::thread::spawn(move || drop(leaving));
    park.parked
        .recv()
        .expect("the leave parks holding the serializer");

    let joining_path = path.clone();
    let (waiting_sender, waiting) = std::sync::mpsc::sync_channel(1);
    let join = std::thread::spawn(move || {
        install_reader_join_wait_hook_for_test(Box::new(move || {
            let _ = waiting_sender.send(());
        }));
        let session = ReadSession::open(&joining_path)?;
        let rows = read_all(&session);
        Ok::<(ReadSession, usize), contextdb_core::Error>((session, rows))
    });
    waiting
        .recv()
        .expect("the join waits on the serializer the leave holds");
    park.resume.send(()).unwrap();
    leave.join().unwrap();
    let (joined, rows) = join
        .join()
        .unwrap()
        .expect("the join registers after the leave");
    assert_eq!(rows, 3);
    assert!(
        !registry_residue(&platform()).is_empty(),
        "the join's registration is in place"
    );
    drop(joined);
    assert_no_reader_files("after an interleaved leave and join");
}

#[test]
fn crashing_writer_child() {
    let Ok(path) = std::env::var(CRASH_PATH) else {
        return;
    };
    let writer = Database::open(path).unwrap();
    let _ = writer.execute(
        "PURGE FROM erased_rows WHERE payload = 'secret'",
        &HashMap::new(),
    );
    panic!("purge writer did not stop at its armed crash boundary");
}

#[test]
fn a_pending_fence_outlives_its_last_reader_until_writer_recovery() {
    if run_with_platform_runtime("a_pending_fence_outlives_its_last_reader_until_writer_recovery") {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("pending.db");
    seed(&path);
    let session = ReadSession::open(&path).unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(crate::child_test_filter(
            module_path!(),
            "crashing_writer_child",
        ))
        .arg("--nocapture")
        .env(CRASH_PATH, &path)
        .env(
            "CONTEXTDB_PURGE_PUBLICATION_CRASH_AT",
            "before-durable-commit",
        )
        .status()
        .unwrap();
    assert_eq!(
        status.code(),
        Some(86),
        "writer stopped outside the crash seam"
    );
    drop(session);
    assert!(
        registry_residue(&platform())
            .iter()
            .any(|path| path.to_string_lossy().ends_with(".fence")),
        "a fence awaiting crash recovery is never removed by a departing reader"
    );
    let recovered = Database::open(&path).expect("writer recovery reconciles the fence");
    recovered.close().unwrap();
    assert_no_reader_files("after recovery cleared the fence");
}

/// A writer parked holding its purge fences, and a client asking for an owner
/// cursor meanwhile: the cursor takes the fence by try, so it is refused at
/// once, retryably, and never waits on the writer -- whose commit finishes.
/// Asked again after the purge, the cursor registers and reads nothing purged.
#[test]
fn an_owner_cursor_never_waits_on_a_purging_writers_fence() {
    if run_with_platform_runtime("an_owner_cursor_never_waits_on_a_purging_writers_fence") {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("handoff-cycle.db");
    seed(&path);
    let writer = std::sync::Arc::new(Database::open(&path).unwrap());
    let park = arm_purge_park_before_durable_commit_for_test();
    let purging = std::sync::Arc::clone(&writer);
    let purge = std::thread::spawn(move || {
        purging.execute(
            "PURGE FROM erased_rows WHERE payload = 'secret'",
            &HashMap::new(),
        )
    });
    park.parked
        .recv()
        .expect("the purge parks holding its fences");

    let client = ReadSession::open(&path).expect("open the live owner's route");
    assert!(
        client
            .open_cursor("SELECT id FROM erased_rows", &HashMap::new())
            .is_err(),
        "a cursor meeting a purging writer's fence is refused, not made to wait"
    );
    park.resume.send(()).unwrap();
    purge
        .join()
        .unwrap()
        .expect("the purge commits while the cursor was refused");
    match client.open_cursor("SELECT id FROM erased_rows", &HashMap::new()) {
        Ok(cursor) => assert_eq!(
            cursor.first_page().rows.len(),
            0,
            "the cursor registered after the purge reads nothing purged"
        ),
        Err(contextdb_core::Error::ReadFailure(failure))
            if failure.kind() == ReadFailureKind::AuthoritativeErasure => {}
        Err(error) => panic!("unexpected cursor outcome: {error:?}"),
    }
    drop(client);
    std::sync::Arc::try_unwrap(writer)
        .unwrap_or_else(|writer| {
            panic!(
                "the writer still has {} owners",
                std::sync::Arc::strong_count(&writer)
            )
        })
        .close()
        .unwrap();
}

/// An owner cursor registers in every root its owner publishes to. When one
/// root refuses -- here because its fence departed -- the cursor's held
/// registries roll back and the owner retries as busy; the retry registers
/// everywhere, and nothing of the refused attempt is left.
#[test]
fn an_owner_cursor_refused_in_one_root_rolls_back_every_root() {
    if run_with_platform_runtime("an_owner_cursor_refused_in_one_root_rolls_back_every_root") {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("owner-rollback.db");
    let supplied = supplied_root(directory.path());
    seed(&path);
    let writer = Database::open_with_options(
        &path,
        contextdb_engine::DatabaseOpenOptions {
            owner_reads: contextdb_engine::OwnerReadConfig {
                runtime_dir: Some(supplied.clone()),
                ..contextdb_engine::OwnerReadConfig::default()
            },
            ..contextdb_engine::DatabaseOpenOptions::default()
        },
    )
    .unwrap();
    let client = ReadSession::open_in_runtime_dir(
        &path,
        ReadSessionOptions::default(),
        Some(supplied.clone()),
    )
    .unwrap();
    arm_reader_fence_departure_for_test(ReaderFenceDepartureForTest::Fence);
    // The owner cursor meets the departed fence in one root and is refused,
    // retryably; every root it had joined is rolled back.
    assert!(
        client
            .open_cursor("SELECT id FROM erased_rows", &HashMap::new())
            .is_err(),
        "an owner cursor whose fence departed is refused"
    );
    assert!(!reader_fence_departure_armed_for_test());
    for root in [platform(), supplied.clone()] {
        assert!(
            registry_residue(&root).iter().all(|path| path
                .extension()
                .is_none_or(|extension| extension != "snapshot")),
            "the refused cursor left no registration in {}: {:?}",
            root.display(),
            registry_residue(&root)
        );
    }
    let cursor = client
        .open_cursor("SELECT id FROM erased_rows", &HashMap::new())
        .expect("asked again, the owner cursor registers in every root");
    assert_eq!(cursor.first_page().rows.len(), 3);
    drop(cursor);
    drop(client);
    writer.close().unwrap();
    assert_no_reader_files("owner cursor after a refused attempt");
    assert!(
        registry_residue(&supplied).is_empty(),
        "nothing of the refused attempt is left in the supplied root: {:?}",
        registry_residue(&supplied)
    );
}

#[test]
fn aborting_reader_child() {
    let Ok(path) = std::env::var(ABORT_READER_PATH) else {
        return;
    };
    let session = ReadSession::open(path).unwrap();
    let _ = session.execute("SELECT id FROM erased_rows", &HashMap::new());
    std::process::abort();
}

/// A reader process that died left its registration behind. It is not
/// reclaimed by a departing reader, so the registry stays; the registration
/// of a dead process is reclaimed only where it always was.
#[test]
fn a_dead_readers_registration_keeps_the_registry_until_it_is_reclaimed() {
    if run_with_platform_runtime(
        "a_dead_readers_registration_keeps_the_registry_until_it_is_reclaimed",
    ) {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dead-reader.db");
    seed(&path);
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(crate::child_test_filter(
            module_path!(),
            "aborting_reader_child",
        ))
        .arg("--nocapture")
        .env(ABORT_READER_PATH, &path)
        .status()
        .unwrap();
    assert!(
        !status.success(),
        "the reader child aborts while registered"
    );
    let session = ReadSession::open(&path).unwrap();
    assert_eq!(read_all(&session), 3);
    drop(session);
    assert!(
        !registry_residue(&platform()).is_empty(),
        "a registration of another process -- even a dead one -- keeps the registry"
    );
    let writer = Database::open(&path).expect("a writer opens past a dead reader");
    writer.close().unwrap();
    println!(
        "DEAD_READER_RESIDUE_AFTER_WRITER_CLOSE={:?}",
        registry_residue(&platform())
    );
}
