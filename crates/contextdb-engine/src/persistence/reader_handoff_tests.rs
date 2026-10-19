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

/// Test-estate reason (2026-10-15): a reader protects its snapshot from the moment it registers (a
/// provisional record: transaction 0, no root), so a scan sees it; a reader that then loses the
/// data-lock race to a writer detaches and routes to the writer.
#[test]
fn a_provisional_registration_is_visible_to_a_scan_then_detached_when_the_data_lock_race_is_lost() {
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

    // The writer opens in the gap: the reader holds neither the hold nor the
    // data lock yet.
    let writer = crate::Database::open_with_options(&path, writer_options(&runtime))
        .expect("the writer opens while the reader is only provisional");
    pause.release(ticket);
    let session = heard
        .recv_timeout(WATCHDOG)
        .expect("the reader never finished")
        .expect("a reader that lost the race is routed, not refused");
    reader.join().expect("the reader thread finishes");
    assert_eq!(
        session.route(),
        contextdb_core::read_contract::ReadRoute::Owner,
        "the reader that lost the data-lock race is answered by the writer that won it"
    );
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
