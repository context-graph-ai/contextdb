//! Writer/reader handoff at the registration and failure edges of a direct
//! reader's snapshot acquisition.
//!
//! Promise (contextdb-intent, Policies 21-23, and the shipped read contract):
//! a registered reader keeps its snapshot, a writer is never blocked by a
//! reader's bookkeeping, and when bookkeeping cannot be completed the store
//! stays safe by RETAINING (nothing is reclaimed, PURGE is refused) rather
//! than by refusing the writer's commit.
//!
//! Test-estate reasons (2026-10-15, reader/writer handoff), one per
//! function below, each stating the behaviour it pins and the bug no existing
//! test catches.

use super::*;
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::sync::mpsc;

const WATCHDOG: std::time::Duration = std::time::Duration::from_secs(60);

fn fixture() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let directory = tempfile::tempdir().expect("task-scoped store directory");
    let path = directory.path().join("handoff.db");
    let seed = crate::Database::open(&path).expect("create a durable store");
    seed.execute(
        "CREATE TABLE erased_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL) HISTORY ALL",
        &HashMap::new(),
    )
    .expect("create the table");
    for id in 1..=3_i64 {
        seed.execute(
            "INSERT INTO erased_rows (id, payload) VALUES ($id, 'secret')",
            &HashMap::from([("id".to_owned(), Value::Int64(id))]),
        )
        .expect("seed a row");
    }
    seed.close().expect("release the seed writer");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("secure the runtime directory");
    (directory, path, runtime)
}

fn identity(path: &Path) -> (u64, u64) {
    let metadata = std::fs::metadata(path).expect("stat the store");
    (metadata.dev(), metadata.ino())
}

fn writer_options(runtime: &Path) -> crate::DatabaseOpenOptions {
    crate::DatabaseOpenOptions {
        owner_reads: crate::OwnerReadConfig {
            runtime_dir: Some(runtime.to_path_buf()),
            ..crate::OwnerReadConfig::default()
        },
        ..crate::DatabaseOpenOptions::default()
    }
}

fn assert_erasure_refusal(result: Result<crate::QueryResult>) {
    assert!(
        matches!(
            &result,
            Err(Error::ReadFailure(failure))
                if failure.kind()
                    == contextdb_core::read_contract::ReadFailureKind::AuthoritativeErasure
        ) || matches!(
            &result,
            Err(Error::ReaderCoordinationUnavailable { operation, .. })
                if operation == "authoritative purge"
        ),
        "PURGE must be refused (authoritative erasure, or a purge coordination refusal) while \
         reader inventory is uncertain: {result:?}"
    );
}

const PURGE: &str = "PURGE FROM erased_rows WHERE id = 1";

/// Test-estate reason (2026-10-15): a reader registry held by another party never refuses the
/// writer's commit: the commit succeeds, retains everything, and PURGE is refused until the
/// inventory is certain. Today the commit waits on the held registry.
#[test]
fn a_commit_that_meets_a_busy_registry_succeeds_and_retains_and_purge_is_refused() {
    let (_directory, path, runtime) = fixture();
    let writer = std::sync::Arc::new(
        crate::Database::open_with_options(&path, writer_options(&runtime))
            .expect("the writer opens with a task-scoped registry"),
    );
    let mut registry = ReaderSnapshotRegistryLock::create(&runtime, identity(&path))
        .expect("create registry lock");
    registry
        .try_lock_exclusive()
        .expect("nobody holds the registry until this proof takes it");

    // The commit runs on its own thread so that a commit which waits on the
    // held registry fails as a message at the failsafe bound instead of
    // hanging the proof.
    let (tell, heard) = mpsc::channel();
    let committing = std::sync::Arc::clone(&writer);
    let commit = std::thread::spawn(move || {
        let _ = tell.send(committing.execute(
            "INSERT INTO erased_rows (id, payload) VALUES (100, 'later')",
            &HashMap::new(),
        ));
    });
    heard
        .recv_timeout(WATCHDOG)
        .expect("the commit never answered: it waited on the registry another party holds")
        .expect("a busy registry retains; it never refuses the commit");
    commit.join().expect("the commit thread finishes");
    assert_erasure_refusal(writer.execute(PURGE, &HashMap::new()));

    drop(registry);
    writer
        .execute(PURGE, &HashMap::new())
        .expect("with the registry free and no reader, the same PURGE is accepted");
    std::sync::Arc::try_unwrap(writer)
        .unwrap_or_else(|_| panic!("the commit thread released its handle"))
        .close()
        .expect("close the writer");
}

/// A provisional registration protects every root and is visible only after
/// physical reader admission. It detaches when the admitted reader closes.
#[test]
fn a_provisional_registration_is_visible_after_data_admission_and_detaches_on_reader_close() {
    if crate::test_process::rerun_alone(
        module_path!(),
        "a_provisional_registration_is_visible_after_data_admission_and_detaches_on_reader_close",
        &[],
    ) {
        return;
    }
    use read_persistence_test_scaffold::{ReaderPausePoint, pause_readers_at_for_test};

    let (_directory, path, runtime) = fixture();
    let identity = identity(&path);
    let pause = pause_readers_at_for_test(&path, ReaderPausePoint::AfterProvisional);
    let (tell, heard) = mpsc::channel();
    let reader_path = path.clone();
    let reader_runtime = runtime.clone();
    let reader = std::thread::spawn(move || {
        let _ = tell.send(crate::ReadSession::open_in_runtime_dir(
            &reader_path,
            crate::ReadSessionOptions::default(),
            Some(reader_runtime),
        ));
    });
    let ticket = pause
        .wait_for_paused_reader(WATCHDOG)
        .expect("the reader never published its provisional registration");

    match capture_reader_snapshot_registry(identity, Some(&runtime)) {
        ReaderSnapshotRegistry::Complete(snapshots) => {
            assert_eq!(
                snapshots.len(),
                1,
                "one reader, one provisional registration"
            );
            assert_eq!(
                snapshots[0].storage_snapshot.transaction_id, 0,
                "a provisional registration protects everything: tx 0, no root"
            );
            assert!(snapshots[0].storage_snapshot.user_root.is_none());
        }
        other => panic!("the scan in the gap must be complete: {other:?}"),
    }

    // Publication follows physical admission, so an independent writer data
    // lock cannot win while the provisional reader remains paused.
    let probe = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    assert!(matches!(
        physical_admission::try_data(&probe, physical_admission::ByteLock::Exclusive).unwrap(),
        physical_admission::Granted::HeldByAnother
    ));
    drop(probe);
    pause.release(ticket);
    let session = heard
        .recv_timeout(WATCHDOG)
        .expect("the reader never finished")
        .expect("the physically admitted reader finishes snapshot capture");
    reader.join().expect("the reader thread finishes");
    assert_eq!(
        session.route(),
        contextdb_core::read_contract::ReadRoute::File,
        "the reader admitted before publication captures its file snapshot"
    );
    let writer = crate::Database::open_with_options(&path, writer_options(&runtime))
        .expect("the writer opens after the reader finishes snapshot capture");
    drop(session);
    assert!(
        snapshot_paths_for_test(&runtime, identity).is_empty(),
        "the detached provisional registration leaves nothing behind"
    );
    writer.close().expect("close the writer");
}

fn snapshot_paths_for_test(runtime: &Path, identity: (u64, u64)) -> Vec<PathBuf> {
    let directory = reader_snapshot_directory(runtime, identity);
    match std::fs::read_dir(directory) {
        Ok(entries) => entries
            .map(|entry| entry.expect("registration entry").path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "snapshot")
            })
            .collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => panic!("cannot enumerate test registrations: {error}"),
    }
}

/// Test-estate reason (2026-10-15): a failure under the data lock detaches without blocking the
/// writer: the reader releases the data lock and the hold first and only then drops its
/// registration. Held after the lock release and before the drop, the writer must open at once;
/// when the failure came after the record write began the record is marked failed, retention
/// ignores it and a PURGE is accepted; when it came before the record write the record is still
/// provisional with its reader alive and a PURGE is refused, naming the reader.
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn assert_a_failure_under_the_data_lock_detaches_without_blocking(
    fault: read_persistence_test_scaffold::RegistrationFault,
    purge_in_the_gap_is_accepted: bool,
) {
    use read_persistence_test_scaffold::{
        ReaderPausePoint, inject_registration_fault_for_test,
        observe_writer_reader_snapshots_for_test, pause_readers_at_for_test,
    };

    let (_directory, path, runtime) = fixture();
    let identity = identity(&path);
    let _fault = inject_registration_fault_for_test(&path, fault);
    let pause = pause_readers_at_for_test(&path, ReaderPausePoint::AfterDRelease);
    let (tell, heard) = mpsc::channel();
    let reader_path = path.clone();
    let reader_runtime = runtime.clone();
    let reader = std::thread::spawn(move || {
        let _ = tell.send(
            crate::ReadSession::open_in_runtime_dir(
                &reader_path,
                crate::ReadSessionOptions::default(),
                Some(reader_runtime),
            )
            .map(|_| ()),
        );
    });
    let ticket = pause
        .wait_for_paused_reader(WATCHDOG)
        .expect("the failing reader never released the data lock");

    // The writer opens while the failing reader still owns its registration
    // and has not yet dropped it.
    let observed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = std::sync::Arc::clone(&observed);
    let writer = observe_writer_reader_snapshots_for_test(
        std::sync::Arc::new(move |snapshot| sink.lock().expect("capture").push(snapshot)),
        || crate::Database::open_with_options(&path, writer_options(&runtime)),
    )
    .expect("the writer opens: a reader that failed under the data lock holds nothing it needs");
    let scanned = observed.lock().expect("captured scan").clone();
    assert_eq!(scanned.len(), 1, "one writer open scans once");
    let retained_as_tx_zero = match &scanned[0] {
        read_persistence_test_scaffold::WriterReaderSnapshotRegistryObservation::Complete(
            snapshots,
        ) => snapshots
            .iter()
            .any(|snapshot| snapshot.storage_transaction_id == 0),
        other => panic!("{fault:?}: the scan in the gap must be complete: {other:?}"),
    };
    let purge = writer.execute(PURGE, &HashMap::new());
    if purge_in_the_gap_is_accepted {
        assert!(
            !retained_as_tx_zero,
            "{fault:?}: the failed reader's record carries the failed marker and retention ignores it"
        );
        purge.unwrap_or_else(|error| {
            panic!(
                "{fault:?}: a PURGE is accepted once the failed reader is proven to serve nothing: {error:?}"
            )
        });
    } else {
        assert!(
            retained_as_tx_zero,
            "{fault:?}: the still-provisional record of a live reader retains everything (tx 0)"
        );
        match purge {
            Err(Error::ReaderCoordinationUnavailable {
                operation, reason, ..
            }) if operation == "authoritative purge" => {
                assert!(
                    reason.contains("is still acquiring its snapshot"),
                    "{fault:?}: the refusal says the reader is still acquiring: {reason}"
                );
                assert!(
                    reason.contains(&std::process::id().to_string()),
                    "{fault:?}: the refusal names the reader's process: {reason}"
                );
            }
            other => panic!("{fault:?}: PURGE must be refused, naming the reader: {other:?}"),
        }
    }

    pause.release(ticket);
    let failure = heard
        .recv_timeout(WATCHDOG)
        .expect("the failing reader never finished");
    reader.join().expect("the reader thread finishes");
    assert!(
        failure.is_err(),
        "{fault:?}: the reader reports the typed failure that detached it"
    );
    assert!(
        snapshot_paths_for_test(&runtime, identity).is_empty(),
        "{fault:?}: the detached registration leaves nothing behind"
    );
    if !purge_in_the_gap_is_accepted {
        writer
            .execute(PURGE, &HashMap::new())
            .expect("once the failed reader is gone, the same PURGE is accepted");
    }
    writer.close().expect("close the writer");
}

#[test]
fn a_second_root_left_provisional_detaches_without_blocking_the_writer() {
    assert_a_failure_under_the_data_lock_detaches_without_blocking(
        read_persistence_test_scaffold::RegistrationFault::SecondRootStaysProvisional,
        true,
    );
}

#[test]
fn a_torn_record_on_a_root_detaches_without_blocking_the_writer() {
    assert_a_failure_under_the_data_lock_detaches_without_blocking(
        read_persistence_test_scaffold::RegistrationFault::TornRecord,
        true,
    );
}

#[test]
fn a_failed_fdatasync_of_the_record_detaches_without_blocking_the_writer() {
    assert_a_failure_under_the_data_lock_detaches_without_blocking(
        read_persistence_test_scaffold::RegistrationFault::FdatasyncFails,
        true,
    );
}

#[test]
fn a_failed_fence_try_detaches_without_blocking_the_writer() {
    assert_a_failure_under_the_data_lock_detaches_without_blocking(
        read_persistence_test_scaffold::RegistrationFault::FenceTryFails,
        false,
    );
}

#[test]
fn a_refused_capture_detaches_without_blocking_the_writer() {
    assert_a_failure_under_the_data_lock_detaches_without_blocking(
        read_persistence_test_scaffold::RegistrationFault::CaptureRefuses,
        false,
    );
}

/// Test-estate reason (2026-10-15): a reader's registration takes one open-or-create of the
/// registry per publication root and one take of it, with no departed-descriptor retry, even when
/// the last other reader leaves during the join. The counters are cumulative per physical store, so
/// the joiner is measured as a delta. Whether the runtime folder is left empty afterwards belongs
/// to the root-directory serializer's own tests.
#[test]
fn a_join_during_the_last_leave_opens_once_takes_once_and_retries_never() {
    use read_persistence_test_scaffold::{
        ReaderPausePoint, pause_readers_at_for_test, registration_attempt_counts_for_test,
    };

    let (_directory, path, runtime) = fixture();
    let earlier = crate::ReadSession::open_in_runtime_dir(
        &path,
        crate::ReadSessionOptions::default(),
        Some(runtime.clone()),
    )
    .expect("the reader that will be the last to leave");

    // The counters are cumulative per physical store, so the joiner is measured
    // as a delta from here.
    let before = registration_attempt_counts_for_test(&path);
    let pause = pause_readers_at_for_test(&path, ReaderPausePoint::BeforeRegistration);
    let (tell, heard) = mpsc::channel();
    let joiner_path = path.clone();
    let joiner_runtime = runtime.clone();
    let joiner = std::thread::spawn(move || {
        let _ = tell.send(crate::ReadSession::open_in_runtime_dir(
            &joiner_path,
            crate::ReadSessionOptions::default(),
            Some(joiner_runtime),
        ));
    });
    let ticket = pause
        .wait_for_paused_reader(WATCHDOG)
        .expect("the joining reader never reached its registration");

    // The last other reader leaves now: its teardown removes the registry the
    // joiner is about to open.
    drop(earlier);
    pause.release(ticket);

    let session = heard
        .recv_timeout(WATCHDOG)
        .expect("the joining reader never finished")
        .expect("a join that races the last leave still registers");
    joiner.join().expect("the joiner thread finishes");
    let after = registration_attempt_counts_for_test(&path);
    let opens = after.opens - before.opens;
    let l0_takes = after.l0_takes - before.l0_takes;
    let retries = after.retries - before.retries;
    assert_eq!(
        opens, l0_takes,
        "every open-or-create of the registry is followed by exactly one take of it"
    );
    assert!(
        (1..=2).contains(&opens),
        "one open-or-create per publication root the joiner publishes in (one or two), got {opens}"
    );
    assert_eq!(retries, 0, "no departed-descriptor retry");
    drop(session);
}

#[cfg(all(target_os = "linux", feature = "test-seams"))]
#[test]
fn a_join_holding_the_runtime_lifetime_lock_never_registers_into_a_removed_root() {
    use read_persistence_test_scaffold::{
        ReaderPausePoint, install_reader_join_wait_hook_for_test, pause_readers_at_for_test,
        registration_attempt_counts_for_test,
    };
    if crate::test_process::rerun_alone(
        module_path!(),
        "a_join_holding_the_runtime_lifetime_lock_never_registers_into_a_removed_root",
        &[],
    ) {
        return;
    }
    let private_base =
        platform_reader_runtime_base().expect("the isolated child has an XDG runtime root");
    std::fs::set_permissions(&private_base, std::fs::Permissions::from_mode(0o700))
        .expect("secure only this child-owned runtime root");
    let (_directory_a, path_a, channel_runtime) = fixture();
    let (_directory_b, path_b, _) = fixture();
    assert_ne!(identity(&path_a), identity(&path_b));
    let earlier = open_with_frozen_request_clock(
        &path_a,
        crate::ReadSessionOptions::default(),
        Some(channel_runtime.clone()),
    )
    .expect("A is the last registered reader");
    let publication_roots = reader_publication_directories(Some(&channel_runtime)).len() as u64;
    let runtime = default_reader_runtime_directory().unwrap();
    let root = runtime.path().to_path_buf();
    let root_before = identity(&root);
    let counts_before = registration_attempt_counts_for_test(&path_b);
    let pause = pause_readers_at_for_test(&path_b, ReaderPausePoint::AfterRuntimeRootLifetimeHold);
    let (answer_tx, answer_rx) = mpsc::channel();
    let joining_path = path_b.clone();
    let joiner = std::thread::spawn(move || {
        let result = open_with_frozen_request_clock(
            &joining_path,
            crate::ReadSessionOptions::default(),
            Some(channel_runtime),
        );
        let _ = answer_tx.send(result);
    });
    let ticket = pause
        .wait_for_paused_reader(WATCHDOG)
        .expect("B holds the lifetime lock after creating the root but before registration");

    // The old exclusive serializer may block A here; the shared lifetime
    // implementation may finish A's leave. Both edges are explicit events,
    // so the test never infers a blocked operation from elapsed time.
    #[derive(Debug)]
    enum LeaveEdge {
        WaitingForRoot,
        Finished,
    }
    let (edge_tx, edge_rx) = mpsc::channel();
    let leaver = std::thread::spawn(move || {
        let waiting = edge_tx.clone();
        install_reader_join_wait_hook_for_test(Box::new(move || {
            let _ = waiting.send(LeaveEdge::WaitingForRoot);
        }));
        drop(earlier);
        let _ = edge_tx.send(LeaveEdge::Finished);
    });
    let edge = edge_rx
        .recv_timeout(WATCHDOG)
        .expect("A reaches a driven leave edge");
    let root_after_a = std::fs::metadata(&root)
        .ok()
        .map(|metadata| (metadata.dev(), metadata.ino()));
    pause.release(ticket);
    drop(pause);
    let joined = answer_rx
        .recv_timeout(WATCHDOG)
        .expect("B's finite join completes");
    joiner.join().expect("B's join thread finishes");
    // Retain B's result while A finishes, so a successful B registration
    // remains present if the old serializer had to wait for B's join.
    leaver
        .join()
        .expect("A's leave finishes after B's lifetime hold is released");
    let counts_after = registration_attempt_counts_for_test(&path_b);
    let opens = counts_after.opens - counts_before.opens;
    let takes = counts_after.l0_takes - counts_before.l0_takes;
    let retries = counts_after.retries - counts_before.retries;
    let diagnostic = joined.as_ref().err().map(|error| format!("{error:?}"));
    eprintln!(
        "join_root_edge={edge:?} root_before={root_before:?} root_after_A={root_after_a:?} opens={opens} takes={takes} retries={retries} result={diagnostic:?}"
    );
    let session = joined.unwrap_or_else(|error| panic!(
        "B joins a live root or retries once into a fresh root, never refuses because A left: {error:?}"
    ));
    assert!(
        (publication_roots..=publication_roots + 1).contains(&opens),
        "one join per publication root plus at most one fresh-root retry: {opens}"
    );
    assert!((publication_roots..=publication_roots + 1).contains(&takes) && takes <= opens);
    assert!(retries <= 1, "no unbounded departed-root retry: {retries}");
    let registered = session
        .file_storage_registration_for_test()
        .expect("B has a real file registration");
    assert!(registered.descriptor_open);
    let registration = registered
        .registration_path
        .expect("B publishes its exact registered root");
    assert!(registration.starts_with(&root));
    let metadata = std::fs::metadata(&registration)
        .expect("B's registration remains linked under the live root");
    assert_eq!(
        metadata.nlink(),
        1,
        "B never retains an unlinked registration"
    );
    assert!(root.is_dir(), "B's published root is live");
    if identity(&root) == root_before {
        assert_eq!(
            retries, 0,
            "a root that stayed live requires no departure retry"
        );
        assert_eq!(opens, takes, "each registry open has one successful take");
    } else {
        assert_eq!(
            opens,
            publication_roots + 1,
            "a removed root is joined again exactly once"
        );
        assert_eq!(retries, 1, "one fresh-root retry, never a loop");
    }
    let answer = session
        .execute(
            "SELECT id, payload FROM erased_rows ORDER BY id",
            &HashMap::new(),
        )
        .expect("the successful join reads B's complete original content");
    assert_eq!(
        answer.rows(),
        &[
            vec![Value::Int64(1), Value::Text("secret".into())],
            vec![Value::Int64(2), Value::Text("secret".into())],
            vec![Value::Int64(3), Value::Text("secret".into())],
        ]
    );
    drop(session);
}

#[cfg(all(target_os = "linux", feature = "test-seams"))]
fn reader_runtime_artifacts(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(directory) else {
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

#[cfg(all(target_os = "linux", feature = "test-seams"))]
#[test]
fn an_entering_reader_keeps_a_skipped_registry_and_cleans_every_root_when_it_leaves_last() {
    use read_persistence_test_scaffold::{
        ReaderPausePoint, install_reader_join_wait_hook_for_test, pause_readers_at_for_test,
        registration_attempt_counts_for_test,
    };
    if crate::test_process::rerun_alone(
        module_path!(),
        "an_entering_reader_keeps_a_skipped_registry_and_cleans_every_root_when_it_leaves_last",
        &[],
    ) {
        return;
    }
    let private_base = platform_reader_runtime_base().unwrap();
    std::fs::set_permissions(&private_base, std::fs::Permissions::from_mode(0o700)).unwrap();
    let (_directory, path, supplied) = fixture();
    let earlier = open_with_frozen_request_clock(
        &path,
        crate::ReadSessionOptions::default(),
        Some(supplied.clone()),
    )
    .unwrap();
    let platform = default_reader_runtime_directory()
        .unwrap()
        .path()
        .to_path_buf();
    let registry = reader_snapshot_registry_lock_path(&platform, identity(&path));
    let registry_before = identity(&registry);
    let roots = reader_publication_directories(Some(&supplied)).len() as u64;
    let before = registration_attempt_counts_for_test(&path);
    let pause = pause_readers_at_for_test(&path, ReaderPausePoint::LeavingWithRegistryExclusion);
    let leaving = std::thread::spawn(move || drop(earlier));
    let ticket = pause
        .wait_for_paused_reader(WATCHDOG)
        .expect("the real leave holds this store's registry exclusion");
    let (waiting_tx, waiting_rx) = mpsc::sync_channel(1);
    let (resume_tx, resume_rx) = mpsc::sync_channel(1);
    let (session_tx, session_rx) = mpsc::channel();
    let joining_path = path.clone();
    let joining_runtime = supplied.clone();
    let joining = std::thread::spawn(move || {
        install_reader_join_wait_hook_for_test(Box::new(move || {
            waiting_tx.send(()).expect("report the real registry wait");
            resume_rx
                .recv_timeout(WATCHDOG)
                .expect("the guard releases the waiting join");
        }));
        let result = open_with_frozen_request_clock(
            &joining_path,
            crate::ReadSessionOptions::default(),
            Some(joining_runtime),
        );
        let _ = session_tx.send(result);
    });
    waiting_rx
        .recv_timeout(WATCHDOG)
        .expect("the entering reader reaches its real registry wait");
    // The waiting reader's physical hold is the real reason cleanup skips.
    let cleanup_gate = with_last_reader_gate(&path, identity(&path), |held| held);
    pause.release(ticket);
    drop(pause);
    leaving
        .join()
        .expect("the earlier reader leaves while the entrant is still waiting");
    let registry_after_leave = std::fs::metadata(&registry)
        .ok()
        .map(|metadata| (metadata.dev(), metadata.ino()));
    // Release the join before asserting, including on a failed skip witness.
    let _ = resume_tx.send(());
    let result = session_rx
        .recv_timeout(WATCHDOG)
        .expect("the entering reader completes");
    joining.join().expect("the entering reader thread finishes");
    let after = registration_attempt_counts_for_test(&path);
    let opens = after.opens - before.opens;
    let takes = after.l0_takes - before.l0_takes;
    let retries = after.retries - before.retries;
    assert!(
        !cleanup_gate,
        "the entering reader's real physical hold denies last-leave removal"
    );
    assert_eq!(
        registry_after_leave,
        Some(registry_before),
        "the skipped registry is still the same linked inode"
    );
    let session = result.unwrap_or_else(|error| {
        panic!("the waiting entrant reads after cleanup skipped: {error:?}")
    });
    assert_eq!(
        (opens, takes, retries),
        (roots, roots, 0),
        "the preserved registry needs no departure retry"
    );
    let rows = session
        .execute(
            "SELECT id, payload FROM erased_rows ORDER BY id",
            &HashMap::new(),
        )
        .unwrap();
    assert_eq!(
        rows.rows(),
        &[
            vec![Value::Int64(1), Value::Text("secret".into())],
            vec![Value::Int64(2), Value::Text("secret".into())],
            vec![Value::Int64(3), Value::Text("secret".into())],
        ]
    );
    drop(rows);
    let registration = session
        .file_storage_registration_for_test()
        .unwrap()
        .registration_path
        .unwrap();
    assert!(
        registration.is_file(),
        "the entering reader has a live registration"
    );
    drop(session);
    let platform_left = reader_runtime_artifacts(&private_base);
    let supplied_left = reader_runtime_artifacts(&supplied);
    eprintln!(
        "skip_registry_before={registry_before:?} after_A={registry_after_leave:?} opens={opens} takes={takes} retries={retries} platform_left={platform_left:?} supplied_left={supplied_left:?}"
    );
    assert!(
        !platform.exists(),
        "the last entering reader removes its empty platform contextdb root; left={platform_left:?}"
    );
    assert!(
        platform_left.is_empty(),
        "no reader registry, lock or fence remains in the platform root"
    );
    assert!(
        supplied_left.is_empty(),
        "no reader registry, lock or fence remains in the supplied root: {supplied_left:?}"
    );
    assert!(
        supplied.is_dir() && private_base.is_dir(),
        "both operator-owned root directories remain"
    );
}

// Run B while a real unrelated A leave owns its store exclusion and root lifetime hold.
// The wait hook reports a blocking root acquisition directly; no timeout is an oracle.
#[cfg(target_os = "linux")]
pub(super) fn during_unrelated_reader_leave<R: Send>(
    operation: impl FnOnce() -> R + Send,
) -> (R, bool) {
    use read_persistence_test_scaffold::{
        ReaderPausePoint, install_reader_join_wait_hook_for_test, pause_readers_at_for_test,
    };
    let private_base = platform_reader_runtime_base().unwrap();
    std::fs::set_permissions(&private_base, std::fs::Permissions::from_mode(0o700)).unwrap();
    let (_directory, path, supplied) = fixture();
    let earlier =
        open_with_frozen_request_clock(&path, crate::ReadSessionOptions::default(), Some(supplied))
            .unwrap();
    let pause = pause_readers_at_for_test(&path, ReaderPausePoint::LeavingWithRegistryExclusion);
    std::thread::scope(|scope| {
        let leaving = scope.spawn(move || drop(earlier));
        let ticket = pause
            .wait_for_paused_reader(WATCHDOG)
            .expect("A's real leave holds its registry exclusion");
        let (edge_tx, edge_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        let working = scope.spawn(move || {
            let waiting = edge_tx.clone();
            install_reader_join_wait_hook_for_test(Box::new(move || {
                let _ = waiting.send(true);
            }));
            let result = operation();
            let _ = result_tx.send(result);
            let _ = edge_tx.send(false);
        });
        let blocked = edge_rx
            .recv_timeout(WATCHDOG)
            .expect("B either finishes or reports its actual root wait");
        pause.release(ticket);
        drop(pause);
        leaving.join().unwrap();
        let result = result_rx
            .recv_timeout(WATCHDOG)
            .expect("B completes after A's finite leave");
        working.join().unwrap();
        (result, blocked)
    })
}

#[cfg(all(target_os = "linux", feature = "test-seams"))]
fn isolated_root_case(name: &str) -> bool {
    if crate::test_process::rerun_alone(module_path!(), name, &[]) {
        return true;
    }
    let private_base = platform_reader_runtime_base().unwrap();
    std::fs::set_permissions(&private_base, std::fs::Permissions::from_mode(0o700)).unwrap();
    false
}

#[cfg(all(target_os = "linux", feature = "test-seams"))]
#[test]
fn unrelated_leave_does_not_block_breadcrumb_create_in() {
    if isolated_root_case("unrelated_leave_does_not_block_breadcrumb_create_in") {
        return;
    }
    let (_directory, path, _) = fixture();
    let runtime = default_reader_runtime_directory().unwrap();
    let (result, blocked) = during_unrelated_reader_leave(move || {
        ReaderBreadcrumbGuard::create_in(
            &path,
            &runtime,
            Some(&current_reader_breadcrumb().unwrap()),
        )
    });
    let guard = result.expect("B publishes its real breadcrumb");
    assert!(
        !blocked,
        "B's diagnostic publication never waits for unrelated A's leave"
    );
    assert!(guard.path().is_file());
}

#[cfg(all(target_os = "linux", feature = "test-seams"))]
#[test]
fn unrelated_leave_does_not_drop_create_for_open_breadcrumb() {
    if isolated_root_case("unrelated_leave_does_not_drop_create_for_open_breadcrumb") {
        return;
    }
    let (_directory, path, _) = fixture();
    let runtime = default_reader_runtime_directory().unwrap();
    let (result, blocked) = during_unrelated_reader_leave(move || {
        let limits = crate::ReadSessionOptions::default().limits;
        let declared =
            crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(limits.memory)
                .unwrap();
        let context = crate::executor::bounded::ReadOpenContext::new(
            declared.child().unwrap(),
            limits,
            crate::read_session::read_clock(),
            OwnerReadCancellation::new(),
        )
        .unwrap();
        ReaderBreadcrumbGuard::create_for_open(
            &path,
            &runtime,
            Some(&current_reader_breadcrumb().unwrap()),
            &context,
        )
    });
    assert!(!blocked);
    let guard = result
        .expect("B's open diagnostic has no refusal")
        .expect("unrelated A never silently drops B's breadcrumb");
    assert!(guard.path().is_file());
}

#[cfg(all(target_os = "linux", feature = "test-seams"))]
#[test]
fn unrelated_leave_keeps_other_store_inventory_complete() {
    if isolated_root_case("unrelated_leave_keeps_other_store_inventory_complete") {
        return;
    }
    let (_directory, path, supplied) = fixture();
    let reader = open_with_frozen_request_clock(
        &path,
        crate::ReadSessionOptions::default(),
        Some(supplied.clone()),
    )
    .unwrap();
    let registered = reader.file_storage_registration_for_test().unwrap();
    let physical = identity(&path);
    let (inventory, blocked) = during_unrelated_reader_leave(move || {
        capture_reader_snapshot_registry(physical, Some(&supplied))
    });
    assert!(!blocked);
    match inventory {
        ReaderSnapshotRegistry::Complete(snapshots) => assert!(snapshots.iter().any(|snapshot| {
            snapshot.handle_identity == registered.handle_identity
                && snapshot.storage_snapshot.transaction_id == registered.storage_transaction_id
                && snapshot.storage_snapshot.user_root.map(|root| root.page)
                    == registered.storage_root_page
                && snapshot
                    .storage_snapshot
                    .user_root
                    .map(|root| root.checksum)
                    == registered.storage_root_checksum
                && snapshot.storage_snapshot.user_root.map(|root| root.length)
                    == registered.storage_root_length
        })),
        other => panic!("B's real registered reader stays observable during A's leave: {other:?}"),
    }
}

#[cfg(all(target_os = "linux", feature = "test-seams"))]
#[test]
fn unrelated_leave_does_not_skip_other_writer_idle_registry_cleanup() {
    if isolated_root_case("unrelated_leave_does_not_skip_other_writer_idle_registry_cleanup") {
        return;
    }
    let (_directory, path, supplied) = fixture();
    let persistence =
        RedbPersistence::open_with_cache_in_runtime_dir(&path, REDB_CACHE_BYTES, Some(&supplied))
            .unwrap();
    let _fixture_decode_scope = crate::read_image_memory::ImageMemory::enter_writer_operation(
        crate::memory_accounting::AccountantHandle::standalone(None, true).unwrap(),
    )
    .unwrap();
    let pin = persistence.owner_cursor_pin().unwrap();
    drop(pin);
    let roots = reader_publication_directories(Some(&supplied));
    let registry_paths: Vec<_> = roots
        .iter()
        .flat_map(|root| {
            [
                reader_snapshot_directory(root.path(), identity(&path)),
                reader_snapshot_registry_lock_path(root.path(), identity(&path)),
            ]
        })
        .collect();
    assert!(
        registry_paths.iter().any(|path| path.is_file()),
        "B has a real idle registry before writer cleanup"
    );
    persistence.make_writer_quiescent().unwrap();
    let (remaining, blocked) = during_unrelated_reader_leave(|| {
        persistence.remove_idle_reader_registries();
        registry_paths
            .iter()
            .filter(|path| path.exists())
            .cloned()
            .collect::<Vec<_>>()
    });
    assert!(!blocked);
    assert!(
        remaining.is_empty(),
        "B removes its own idle registries while unrelated A leaves: {remaining:?}"
    );
}

fn open_with_frozen_request_clock(
    path: impl AsRef<std::path::Path>,
    options: crate::ReadSessionOptions,
    runtime_dir: Option<std::path::PathBuf>,
) -> Result<crate::ReadSession> {
    crate::ReadSession::open_in_runtime_dir_with_clock_for_test(
        path,
        options,
        runtime_dir,
        std::sync::Arc::new(crate::local_transport::ManualDeadlineClock::at(0)),
    )
}
