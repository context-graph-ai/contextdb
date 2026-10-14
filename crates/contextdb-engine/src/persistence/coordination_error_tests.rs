use super::*;
use std::os::unix::fs::PermissionsExt as _;

fn writer() -> (tempfile::TempDir, RedbPersistence) {
    let directory = tempfile::tempdir().expect("task-scoped store directory");
    let path = directory.path().join("coordination.db");
    let database = crate::Database::open(&path).expect("create a durable store");
    database.close().expect("close the seed writer");
    let persistence = RedbPersistence::open(&path).expect("open the test writer");
    (directory, persistence)
}

fn assert_coordination_error(error: Error, operation: &str, path: &Path, cause: &str) {
    assert!(
        matches!(&error, Error::ReaderCoordinationUnavailable {
            operation: actual_operation,
            path: actual_path,
            reason,
        } if actual_operation == operation
            && actual_path == &path.display().to_string()
            && reason.contains(cause)),
        "expected typed {operation} refusal at {} caused by {cause}: {error:?}",
        path.display(),
    );
}

#[test]
fn closed_writer_ownership_refuses_recovery_compaction_and_cursor_pin_with_typed_paths() {
    let (_directory, persistence) = writer();
    let owner = persistence.writer_file.write().unwrap().take().unwrap();
    let path = persistence.path();

    assert_coordination_error(
        persistence
            .recover_authoritative_purge_publication()
            .unwrap_err(),
        "authoritative purge recovery",
        path,
        "writer file ownership is closed",
    );
    assert_coordination_error(
        persistence
            .compact_incremental(
                Arc::new(crate::memory_accounting::MemoryAccountant::no_limit()).into(),
                Some(0.0),
            )
            .expect_err("closed ownership refuses compaction"),
        "storage reclamation",
        path,
        "writer file ownership is closed",
    );
    assert_coordination_error(
        persistence
            .owner_cursor_pin()
            .err()
            .expect("closed ownership refuses a cursor pin"),
        "owner cursor pin",
        path,
        "database physical ownership is closed",
    );
    *persistence.writer_file.write().unwrap() = Some(owner);
}

#[test]
fn poisoned_writer_state_refuses_recovery_compaction_and_cursor_pin_with_typed_paths() {
    let (_directory, persistence) = writer();
    std::thread::scope(|scope| {
        let poisoned = scope.spawn(|| {
            let _held = persistence.writer_file.write().unwrap();
            panic!("poison the writer ownership slot");
        });
        assert!(poisoned.join().is_err());
    });
    let path = persistence.path();

    assert_coordination_error(
        persistence
            .recover_authoritative_purge_publication()
            .unwrap_err(),
        "authoritative purge recovery",
        path,
        "writer file state is unavailable",
    );
    assert_coordination_error(
        persistence
            .compact_incremental(
                Arc::new(crate::memory_accounting::MemoryAccountant::no_limit()).into(),
                Some(0.0),
            )
            .expect_err("poisoned ownership refuses compaction"),
        "storage reclamation",
        path,
        "writer file state is unavailable",
    );
    assert_coordination_error(
        persistence
            .owner_cursor_pin()
            .err()
            .expect("poisoned ownership refuses a cursor pin"),
        "owner cursor pin",
        path,
        "writer file state is unavailable",
    );
}

#[test]
fn missing_companion_refuses_recovery_and_cursor_pin_with_typed_database_path() {
    let (directory, persistence) = writer();
    let companion = contextdb_core::store_companion_path(persistence.path());
    let parked = directory.path().join("parked-companion");
    std::fs::rename(&companion, &parked).expect("hide the published companion pathname");

    assert_coordination_error(
        persistence
            .recover_authoritative_purge_publication()
            .unwrap_err(),
        "authoritative purge recovery",
        persistence.path(),
        "cannot inspect companion publication incarnation",
    );
    assert_coordination_error(
        persistence
            .owner_cursor_pin()
            .err()
            .expect("missing companion refuses a cursor pin"),
        "owner cursor pin",
        persistence.path(),
        "cannot inspect published owner cursor companion identity: open companion",
    );
    std::fs::rename(&parked, &companion).expect("restore the companion pathname");
}

#[test]
fn unusable_publication_fence_refuses_recovery_and_cursor_registration_with_typed_paths() {
    let (directory, mut persistence) = writer();
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).unwrap();
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    persistence.runtime_dir = Some(runtime.clone());
    let owner = persistence.writer_file.read().unwrap();
    let physical_identity = owner.as_ref().unwrap().identity();
    drop(owner);
    let (database_identity, _, _, _) = published_writer_identity(persistence.path()).unwrap();
    let snapshot_dir = reader_snapshot_directory(&runtime, physical_identity);
    let fence = snapshot_dir.join(format!(
        "purge-publication-{}.fence",
        uuid::Uuid::from_bytes(database_identity.0).simple(),
    ));
    std::fs::create_dir_all(&fence).expect("a directory cannot serve as the publication file");
    for path in [reader_breadcrumb_root(&runtime), snapshot_dir] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }

    assert_coordination_error(
        persistence
            .recover_authoritative_purge_publication()
            .unwrap_err(),
        "authoritative purge recovery",
        &runtime,
        "publication fence cannot be opened",
    );
    assert_coordination_error(
        persistence
            .owner_cursor_pin()
            .err()
            .expect("unusable fence refuses cursor registration"),
        "owner cursor pin",
        &runtime,
        "reader snapshot registration failed",
    );

    std::fs::remove_dir(&fence).expect("replace the unopenable fence");
    std::fs::write(&fence, b"invalid fence state").expect("write a malformed fence");
    std::fs::set_permissions(&fence, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert_coordination_error(
        persistence
            .recover_authoritative_purge_publication()
            .unwrap_err(),
        "authoritative purge recovery",
        &runtime,
        "publication fence state cannot be inspected",
    );

    std::fs::remove_file(&fence).expect("replace the malformed fence");
    drop(
        purge_publication::open_for_reader(&runtime, physical_identity, database_identity)
            .expect("publish a healthy erasure fence"),
    );
    let registry_lock = reader_snapshot_registry_lock_path(&runtime, physical_identity);
    std::fs::remove_file(&registry_lock).expect("replace the existing registry lock file");
    std::fs::create_dir(&registry_lock).expect("a directory cannot serve as the registry lock");
    assert_coordination_error(
        persistence
            .owner_cursor_pin()
            .err()
            .expect("unusable registry lock refuses cursor registration"),
        "owner cursor pin",
        &runtime,
        "publish the reader snapshot registration",
    );
}

#[test]
fn provider_inventory_refusal_preserves_owner_cursor_operation_runtime_and_cause() {
    const CHILD: &str = "CONTEXTDB_OWNER_PIN_PROVIDER_COORDINATION_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let directory = tempfile::tempdir().expect("task-scoped platform runtime");
        let platform = directory.path().join("platform");
        std::fs::create_dir(&platform).unwrap();
        std::fs::set_permissions(&platform, std::fs::Permissions::from_mode(0o700)).unwrap();
        let outcome = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "persistence::coordination_error_tests::provider_inventory_refusal_preserves_owner_cursor_operation_runtime_and_cause",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("XDG_RUNTIME_DIR", &platform)
            .status()
            .expect("run the proof with its own platform runtime");
        assert!(outcome.success(), "the provider refusal proof must pass");
        return;
    }

    let (directory, persistence) = writer();
    let path = persistence.path().to_path_buf();
    drop(persistence);
    let runtime = directory.path().join("candidate-runtime");
    std::fs::create_dir(&runtime).unwrap();
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    let persistence =
        RedbPersistence::open_with_cache_in_runtime_dir(&path, REDB_CACHE_BYTES, Some(&runtime))
            .expect("open while both runtime roots are private");
    std::fs::write(runtime.join("nonempty"), b"candidate remains discoverable").unwrap();
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o755)).unwrap();

    let refusal = persistence
        .owner_cursor_pin()
        .err()
        .expect("the provider refuses the unsafe candidate inventory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert_coordination_error(
        refusal,
        "owner cursor pin",
        &runtime,
        "authenticated reader snapshot registry is indeterminate",
    );
    drop(
        persistence
            .owner_cursor_pin()
            .expect("restored coordination admits the pin"),
    );
}

#[test]
fn compaction_checkpoint_control_reservation_remains_non_io_at_the_hook() {
    let accountant = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(31));
    accountant.try_allocate(7).unwrap();
    let refusal = match RedbPersistence::reserve_compaction_checkpoint_controls(
        Arc::clone(&accountant).into(),
        Some("items"),
        &[2, 3],
    ) {
        Err(error) => error,
        Ok(_) => panic!("checkpoint controls unexpectedly admitted"),
    };
    let failure = parking_lot::Mutex::new(None);
    let storage = RedbPersistence::compaction_checkpoint_storage_error(refusal, &failure);
    let redb::StorageError::AdmissionRefused(payload) = storage else {
        panic!("checkpoint reservation crossed storage as {storage:?}");
    };
    assert!(
        failure.lock().is_none(),
        "memory refusal needs no I/O side channel"
    );
    let error = payload.downcast::<Error>().unwrap();
    assert!(matches!(*error, Error::MemoryBudgetExceeded {
        subsystem, operation, requested_bytes: 1024, available_bytes: 24,
        budget_limit_bytes: 31, ..
    } if subsystem == "storage_compaction" && operation == "checkpoint_controls"));
    assert_eq!(accountant.usage().used, 7);
    accountant.release(7);
}

#[test]
fn compaction_hook_preserves_recovered_page_refusal_and_keeps_device_failures_as_io() {
    let accountant = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(19));
    let refusal = accountant
        .try_allocate_for(
            4096,
            "storage_compaction",
            "relocate_pages",
            "checkpoint page read",
        )
        .unwrap_err();
    let recovered =
        RedbPersistence::storage_error(redb::StorageError::AdmissionRefused(Box::new(refusal)));
    let failure = parking_lot::Mutex::new(None);
    let storage = RedbPersistence::compaction_checkpoint_storage_error(recovered, &failure);
    assert!(matches!(&storage, redb::StorageError::AdmissionRefused(_)));
    assert!(failure.lock().is_none());
    let recovered = RedbPersistence::storage_error(storage);
    assert!(matches!(recovered, Error::MemoryBudgetExceeded {
        subsystem, operation, requested_bytes: 4096, available_bytes: 19,
        budget_limit_bytes: 19, ..
    } if subsystem == "storage_compaction" && operation == "relocate_pages"));
    let device_error = RedbPersistence::storage_error(redb::StorageError::Io(std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        "checkpoint device failed",
    )));
    let storage = RedbPersistence::compaction_checkpoint_storage_error(device_error, &failure);
    assert!(matches!(storage, redb::StorageError::Io(_)));
    assert!(matches!(failure.lock().take(), Some(Error::Other(message))
        if message.contains("checkpoint device failed")));
}
