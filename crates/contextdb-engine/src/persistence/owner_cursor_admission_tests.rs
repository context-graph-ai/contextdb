use super::*;
use std::cell::RefCell;
use std::os::unix::fs::PermissionsExt as _;
use std::rc::Rc;

type ReaderDescriptorValidationHook = ([u8; 16], Box<dyn FnOnce()>);

thread_local! {
    static BEFORE_PHYSICAL_PRESENCE_PROBE: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    static BEFORE_PROVIDER_ADMISSION: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    static BEFORE_READER_DESCRIPTOR_VALIDATION: RefCell<Option<ReaderDescriptorValidationHook>> = const { RefCell::new(None) };
}

pub(super) fn before_reader_descriptor_validation_for_test(snapshot: &RegisteredReaderSnapshot) {
    let hook = BEFORE_READER_DESCRIPTOR_VALIDATION.with(|hook| {
        let mut hook = hook.borrow_mut();
        if hook
            .as_ref()
            .is_some_and(|(identity, _)| *identity == snapshot.handle_identity)
        {
            hook.take().map(|(_, operation)| operation)
        } else {
            None
        }
    });
    if let Some(hook) = hook {
        hook();
    }
}

pub(super) fn before_provider_admission_for_test() {
    BEFORE_PROVIDER_ADMISSION.with(|hook| {
        if let Some(hook) = hook.borrow_mut().take() {
            hook();
        }
    });
}

fn writer() -> (tempfile::TempDir, RedbPersistence, PathBuf) {
    let directory = tempfile::tempdir().expect("task-scoped store directory");
    let path = directory.path().join("owner-admission.db");
    let database = crate::Database::open(&path).expect("create a durable store");
    database.close().expect("close the seed writer");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).unwrap();
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    let persistence =
        RedbPersistence::open_with_cache_in_runtime_dir(&path, REDB_CACHE_BYTES, Some(&runtime))
            .expect("open the writer with a task-scoped registry");
    (directory, persistence, runtime)
}

fn assert_coordination_refusal(error: Error, path: &Path, cause: &str) {
    assert!(
        matches!(&error, Error::ReaderCoordinationUnavailable {
            operation,
            path: actual_path,
            reason,
        } if operation == "owner cursor pin"
            && actual_path == &path.display().to_string()
            && reason.contains(cause)),
        "expected owner cursor coordination refusal at {} caused by {cause}: {error:?}",
        path.display(),
    );
}

fn snapshot_paths(runtime: &Path, identity: (u64, u64)) -> Vec<PathBuf> {
    let directory = reader_snapshot_directory(runtime, identity);
    match std::fs::read_dir(directory) {
        Ok(entries) => entries
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "snapshot")
            })
            .collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => panic!("cannot enumerate test registrations: {error}"),
    }
}

#[test]
// Exclusive guards intentionally force contention with the admission read locks.
#[allow(clippy::readonly_write_lock)]
fn owner_cursor_persistence_guard_contention_refuses_before_lock_release() {
    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();
    {
        let held = persistence.lock_file.write().unwrap();
        assert_coordination_refusal(
            persistence
                .owner_cursor_pin()
                .err()
                .expect("busy companion refuses"),
            persistence.path(),
            "companion ownership state is unavailable",
        );
        assert!(held.is_some());
    }
    {
        let held = persistence.db.write().unwrap();
        assert_coordination_refusal(
            persistence
                .owner_cursor_pin()
                .err()
                .expect("busy database refuses"),
            persistence.path(),
            "Redb handle state is unavailable",
        );
        assert!(held.is_some());
    }
    {
        let held = persistence.writer_file.write().unwrap();
        assert_coordination_refusal(
            persistence
                .owner_cursor_pin()
                .err()
                .expect("busy writer file refuses"),
            persistence.path(),
            "writer file state is unavailable",
        );
        assert!(held.is_some());
    }
    assert!(snapshot_paths(&runtime, identity).is_empty());
    assert!(!OWNER_CURSOR_COORDINATION_NONBLOCKING.with(|mode| mode.get()));
    drop(
        persistence
            .owner_cursor_pin()
            .expect("retry after guard release"),
    );
}

#[test]
fn owner_cursor_refresh_contention_refuses_before_refresh_release() {
    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();
    persistence
        .with_db(|db| {
            db.with_external_snapshot_refresh_lock_for_test(|| {
                // Ordinary root capture does not acquire the provider refresh lock.
                drop(
                    db.begin_read()
                        .expect("local root capture remains available"),
                );
                assert_coordination_refusal(
                    persistence
                        .owner_cursor_pin()
                        .err()
                        .expect("busy refresh refuses"),
                    persistence.path(),
                    "external snapshot refresh is held by another operation",
                );
                // The refresh stays held through these assertions; no concurrent
                // release or timer can make a blocking admission pass this proof.
                assert!(snapshot_paths(&runtime, identity).is_empty());
                let owner = persistence
                    .writer_file
                    .read()
                    .unwrap()
                    .as_ref()
                    .cloned()
                    .unwrap();
                assert!(
                    !owner
                        .unknown_reader_presence(&ReaderSnapshotRegistry::Complete(Vec::new()))
                        .unwrap()
                );
                assert!(!OWNER_CURSOR_COORDINATION_NONBLOCKING.with(|mode| mode.get()));
                Ok(())
            })
        })
        .unwrap();
    drop(
        persistence
            .owner_cursor_pin()
            .expect("retry after refresh release"),
    );
    #[cfg(target_os = "linux")]
    maintenance_contention_row(MaintenanceContention::PendingHandoff, false);
}

#[test]
fn owner_cursor_registry_contention_is_retryable_before_lock_release() {
    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();
    let mut held = ReaderSnapshotRegistryLock::create(&runtime, identity).unwrap();
    held.lock_exclusive().unwrap();

    // The held description stays locked across the refusal assertion. There is
    // no timer or concurrent unlock that could make a blocking implementation pass.
    let error = persistence
        .owner_cursor_pin_for_admission()
        .err()
        .expect("occupied registry yields admission");
    assert!(matches!(&error, OwnerCursorPinAdmissionError::Busy(_)));
    assert_coordination_refusal(
        error.into_error(),
        &runtime,
        "reader snapshot registry lock is held by another operation",
    );
    assert!(held.locked);
    assert!(snapshot_paths(&runtime, identity).is_empty());
    held.unlock().unwrap();
    drop(
        persistence
            .owner_cursor_pin()
            .expect("retry succeeds after registry release"),
    );
}

#[test]
fn owner_cursor_fence_contention_refuses_before_either_lock_release() {
    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();
    let (database_identity, _, _, _) = published_writer_identity(persistence.path()).unwrap();

    for admission in [true, false] {
        let held = purge_publication::hold_reader_lock_for_test(
            &runtime,
            identity,
            database_identity,
            admission,
        );
        assert_coordination_refusal(
            persistence
                .owner_cursor_pin()
                .err()
                .expect("occupied fence refuses admission"),
            &runtime,
            "authoritative erasure publication fence is held by another operation",
        );
        assert!(snapshot_paths(&runtime, identity).is_empty());
        // Keep the exclusive fence description open until after the refusal.
        assert!(held.metadata().is_ok());
        drop(held);
        drop(
            persistence
                .owner_cursor_pin()
                .expect("retry succeeds after fence release"),
        );
    }
}

#[test]
fn owner_cursor_provider_contention_retains_and_admits() {
    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();
    let held = Rc::new(RefCell::new(None));
    let hook_held = Rc::clone(&held);
    let hook_runtime = runtime.clone();
    BEFORE_PROVIDER_ADMISSION.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            assert_eq!(snapshot_paths(&hook_runtime, identity).len(), 1);
            let mut registry = ReaderSnapshotRegistryLock::open(&hook_runtime, identity).unwrap();
            registry.lock_exclusive().unwrap();
            *hook_held.borrow_mut() = Some(registry);
        }));
    });

    // Ruling R4: a provider scan that meets an occupied registry does not
    // refuse. It answers the exact roots plus tx 0 (retain everything), and
    // the admission SUCCEEDS.
    let pin = match persistence.owner_cursor_pin_for_admission() {
        Ok(pin) => pin,
        Err(error) => panic!(
            "a busy registry retains and admits; it never refuses the cursor: {:?}",
            error.into_error()
        ),
    };
    assert!(held.borrow().as_ref().unwrap().locked);
    assert_eq!(
        snapshot_paths(&runtime, identity).len(),
        1,
        "while the registry is held elsewhere the pin's own registration is present"
    );
    drop(pin);
    drop(held.borrow_mut().take());
    assert!(matches!(
        capture_reader_snapshot_registry(identity, Some(&runtime)),
        ReaderSnapshotRegistry::Complete(snapshots) if snapshots.is_empty()
    ));
    assert!(
        snapshot_paths(&runtime, identity).is_empty(),
        "after the pin and the other holder are gone the registry is consistent and empty"
    );
    assert!(!OWNER_CURSOR_COORDINATION_NONBLOCKING.with(|mode| mode.get()));
    drop(
        persistence
            .owner_cursor_pin()
            .expect("a later pin succeeds once the registry is free"),
    );
    let rows = [true, false].map(|native_floor| {
        catch_unwind(AssertUnwindSafe(|| {
            newer_registered_root_is_protected_by_an_older_admitted_reader(native_floor);
        }))
        .is_ok()
    });
    assert_eq!(
        rows,
        [true, true],
        "native and external floor rows both admit"
    );
}

// A housekeeping commit advances the physical transaction without changing
// the user root. A later external reader can publish that exact transaction
// after its native capture ends: the older admitted reader still fences every
// page the later snapshot can address. Exercise the real registration and
// owner-admission doors without requiring a probabilistic reader overlap.
fn newer_registered_root_is_protected_by_an_older_admitted_reader(native_floor: bool) {
    let (_directory, persistence, runtime) = writer();
    let OwnerCursorStoragePin {
        read,
        _registration,
    } = persistence.owner_cursor_pin().unwrap();
    let old = read.storage_snapshot_identity();
    let (old_read, registration) = if native_floor {
        // Drop the external registration: only the real native read remains.
        drop(_registration);
        (read, None)
    } else {
        // External reads carry no native transaction pin. The registered
        // earlier root alone must retain the intervening transaction range.
        let external = persistence
            .with_db(|db| Ok(Arc::new(db.begin_external_read(old).unwrap())))
            .unwrap();
        drop(read);
        (external, Some(_registration))
    };
    let identity = persistence.physical_identity().unwrap();
    let (database_identity, run, _, _) = published_writer_identity(persistence.path()).unwrap();
    let later = persistence
        .with_db(|db| {
            db.begin_write().unwrap().commit().unwrap();
            let native = db.begin_read().unwrap();
            let snapshot = native.storage_snapshot_identity();
            assert!(snapshot.transaction_id > old.transaction_id);
            assert_eq!(snapshot.user_root, old.user_root);
            let registration = ReadSnapshotRegistrationGuard::from_owner(
                persistence.path(),
                database_identity,
                run,
                identity,
                snapshot,
                Some(&runtime),
            )
            .unwrap();
            drop(native);
            Ok(registration)
        })
        .unwrap();
    let admitted = match persistence.owner_cursor_pin_from_read(Some(Arc::clone(&old_read))) {
        Ok(pin) => pin,
        Err(error) => panic!(
            "the earlier reader protects the newer external registration: {:?}",
            error.into_error()
        ),
    };
    assert_eq!(admitted.read.storage_snapshot_identity(), old);
    assert!(later.snapshot.storage_snapshot.transaction_id > old.transaction_id);
    drop(admitted);
    drop(later);
    drop(registration);
    drop(old_read);
}

#[test]
fn untrusted_provider_scan_registry_is_a_terminal_typed_refusal() {
    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();
    let hook_runtime = runtime.clone();
    BEFORE_PROVIDER_ADMISSION.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            let registry = ReaderSnapshotRegistryLock::open(&hook_runtime, identity).unwrap();
            std::fs::set_permissions(&registry.path, std::fs::Permissions::from_mode(0o644))
                .unwrap();
        }));
    });
    let error = persistence.owner_cursor_pin_for_admission().err().unwrap();
    assert!(matches!(&error, OwnerCursorPinAdmissionError::Refused(_)));
    assert_coordination_refusal(error.into_error(), &runtime, "registry is indeterminate");
    let path = reader_snapshot_registry_lock_path(&runtime, identity);
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    drop(persistence.owner_cursor_pin().unwrap());
}

#[test]
fn reader_departing_during_registry_scan_releases_its_stale_registration() {
    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();
    let earlier = persistence.owner_cursor_pin().unwrap();
    let handle = earlier._registration.snapshot.handle_identity;
    let departed = Rc::new(RefCell::new(Some(earlier)));
    let hook_departed = Rc::clone(&departed);
    BEFORE_READER_DESCRIPTOR_VALIDATION.with(|hook| {
        *hook.borrow_mut() = Some((
            handle,
            Box::new(move || {
                // The scan holds the registry and has just observed this reader's
                // locked registration. Nonblocking teardown releases the snapshot
                // lifetime lock and data descriptor before validation resumes.
                drop(hook_departed.borrow_mut().take());
            }),
        ));
    });
    let admitted = persistence
        .owner_cursor_pin()
        .expect("a reader departing during the scan is authenticated stale state");
    assert!(
        departed.borrow().is_none(),
        "the real teardown ran inside the scan"
    );
    drop(admitted);
    assert!(matches!(
        capture_reader_snapshot_registry(identity, Some(&runtime)),
        ReaderSnapshotRegistry::Complete(snapshots) if snapshots.is_empty()
    ));
}

#[test]
fn locked_registry_snapshot_without_its_data_descriptor_refuses_terminally() {
    let (_directory, persistence, _runtime) = writer();
    let earlier = persistence.owner_cursor_pin().unwrap();
    let registration = earlier._registration.registration.as_ref().unwrap();
    let mut invalid = earlier._registration.snapshot.clone();
    invalid.descriptor_number = u32::MAX;
    std::fs::write(
        &registration.path,
        encode_reader_snapshot(&invalid).unwrap(),
    )
    .unwrap();
    let error = persistence.owner_cursor_pin_for_admission().err().unwrap();
    assert!(matches!(&error, OwnerCursorPinAdmissionError::Refused(_)));
    assert_coordination_refusal(
        error.into_error(),
        registration.path.parent().unwrap().parent().unwrap(),
        "registry is indeterminate",
    );
    std::fs::write(
        &registration.path,
        encode_reader_snapshot(&earlier._registration.snapshot).unwrap(),
    )
    .unwrap();
    drop(earlier);
    drop(persistence.owner_cursor_pin().unwrap());
}

#[cfg(feature = "test-seams")]
#[test]
fn replaced_provider_scan_registry_is_a_terminal_typed_refusal() {
    use read_persistence_test_scaffold::pause_reader_snapshot_registry_scan_for_test;

    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();
    let persistence = Arc::new(persistence);
    let pause = pause_reader_snapshot_registry_scan_for_test(persistence.path()).unwrap();
    let arriving = Arc::clone(&persistence);
    let reader = std::thread::spawn(move || {
        let error = arriving.owner_cursor_pin_for_admission().err().unwrap();
        let terminal = matches!(&error, OwnerCursorPinAdmissionError::Refused(_));
        (terminal, error.into_error())
    });
    let observed = pause.wait_until_inspected_for_test();
    let parked = observed.registry_directory.with_extension("parked");
    std::fs::rename(&observed.registry_directory, &parked).unwrap();
    std::fs::create_dir(&observed.registry_directory).unwrap();
    std::fs::set_permissions(
        &observed.registry_directory,
        std::fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    pause.resume_for_test();
    let (terminal, error) = reader.join().unwrap();
    assert!(terminal, "replaced scan state never reaches Busy");
    assert_coordination_refusal(
        error,
        observed.registry_directory.parent().unwrap(),
        "registry is indeterminate",
    );
    // Refused admission may already remove the empty replacement while
    // releasing its registration. Restore the pinned original in either case.
    if let Err(error) = std::fs::remove_dir(&observed.registry_directory) {
        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    }
    std::fs::rename(&parked, &observed.registry_directory).unwrap();
    pause.finish_for_test();
    assert!(matches!(
        capture_reader_snapshot_registry(identity, Some(&runtime)),
        ReaderSnapshotRegistry::Complete(snapshots) if snapshots.is_empty()
    ));
    drop(persistence.owner_cursor_pin().unwrap());
}

#[test]
fn poisoned_owner_cursor_refresh_is_a_terminal_typed_coordination_refusal() {
    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();
    let poisoned = catch_unwind(AssertUnwindSafe(|| {
        persistence
            .with_db(|db| {
                db.with_external_snapshot_refresh_lock_for_test(|| {
                    panic!("poison only the provider refresh mutex");
                });
                Ok(())
            })
            .unwrap();
    }));
    assert!(poisoned.is_err());
    let error = persistence
        .owner_cursor_pin_for_admission()
        .err()
        .expect("poisoned provider refresh refuses admission");
    assert!(matches!(&error, OwnerCursorPinAdmissionError::Refused(_)));
    assert_coordination_refusal(
        error.into_error(),
        persistence.path(),
        "Poisoned internal lock",
    );
    assert!(snapshot_paths(&runtime, identity).is_empty());
}

#[test]
fn untrusted_owner_cursor_registry_is_a_terminal_typed_refusal() {
    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();
    let registry = ReaderSnapshotRegistryLock::create(&runtime, identity).unwrap();
    std::fs::set_permissions(&registry.path, std::fs::Permissions::from_mode(0o644)).unwrap();
    let error = persistence
        .owner_cursor_pin_for_admission()
        .err()
        .expect("an untrusted registry refuses admission");
    assert!(matches!(&error, OwnerCursorPinAdmissionError::Refused(_)));
    assert_coordination_refusal(
        error.into_error(),
        &runtime,
        "not a trusted private regular file",
    );
    assert!(snapshot_paths(&runtime, identity).is_empty());
    std::fs::set_permissions(&registry.path, std::fs::Permissions::from_mode(0o600)).unwrap();
    drop(
        persistence
            .owner_cursor_pin()
            .expect("restored trust admits the cursor"),
    );
}

#[test]
fn a_still_linked_replaced_registry_lock_is_not_temporary_contention() {
    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();
    let mut registry = ReaderSnapshotRegistryLock::create(&runtime, identity).unwrap();
    let parked = registry.path.with_extension("parked");
    std::fs::rename(&registry.path, &parked).unwrap();
    let replacement = ReaderSnapshotRegistryLock::create(&runtime, identity).unwrap();
    let error = registry
        .try_lock_exclusive()
        .expect_err("a linked replaced descriptor is unsafe");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(!registry.locked);
    drop(replacement);
    std::fs::remove_file(&parked).unwrap();
}

#[cfg(feature = "test-seams")]
#[test]
fn a_registry_replaced_before_validation_is_a_terminal_typed_admission_refusal() {
    use read_persistence_test_scaffold::{
        ReaderRegistrationLockPhase, pause_reader_registration_lock_for_test,
    };

    let (_directory, persistence, _runtime) = writer();
    let persistence = Arc::new(persistence);
    let paths = persistence.owner_cursor_registry_paths_for_test();
    let pause = pause_reader_registration_lock_for_test(
        persistence.path(),
        ReaderRegistrationLockPhase::DescriptorOpened,
    )
    .unwrap();
    let arriving = Arc::clone(&persistence);
    let reader = std::thread::spawn(move || {
        let error = arriving.owner_cursor_pin_for_admission().err().unwrap();
        let terminal = matches!(&error, OwnerCursorPinAdmissionError::Refused(_));
        (terminal, error.into_error())
    });
    assert!(pause.wait_for_attempt_for_test(1, std::time::Duration::from_secs(30)));
    let parked = paths[0].with_extension("parked");
    std::fs::rename(&paths[0], &parked).unwrap();
    let replacement = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&paths[0])
        .unwrap();
    replacement
        .set_permissions(std::fs::Permissions::from_mode(0o600))
        .unwrap();
    pause.stop_pausing_for_test();
    let (terminal, error) = reader.join().unwrap();
    std::fs::remove_file(&paths[0]).unwrap();
    std::fs::rename(&parked, &paths[0]).unwrap();
    assert!(terminal, "unsafe replacement must not become Busy");
    assert_coordination_refusal(
        error,
        paths[0].parent().unwrap(),
        "not a trusted private regular file",
    );
    assert_eq!(pause.attempts_for_test(), 1);
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
enum MaintenanceContention {
    PendingHandoff,
    Registry,
    PhysicalHold,
}

// Original-view continuity and optional-maintenance deferral.
// These extend the owner-admission schedules; the real caller
// must distinguish temporary contention from permission to reclaim pages.
#[cfg(target_os = "linux")]
fn maintenance_contention_row(contention: MaintenanceContention, retention: bool) {
    use super::read_persistence_test_scaffold::{
        ReaderRegistrationLockPhase, pause_reader_registration_lock_for_test,
    };
    use crate::{Database, DatabaseOpenOptions, MaintenancePolicy};
    use contextdb_core::{Value, Wallclock};
    use std::collections::HashMap;
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("maintenance.db");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).unwrap();
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    let open = || {
        Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                owner_reads: crate::OwnerReadConfig {
                    runtime_dir: Some(runtime.clone()),
                    ..Default::default()
                },
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap()
    };
    let db = open();
    if retention {
        db.execute("CREATE TABLE retained (id INT PRIMARY KEY, expires TIMESTAMP EXPIRES) RETAIN 1 HOURS SYNC OFF", &HashMap::new()).unwrap();
        db.execute(
            "INSERT INTO retained VALUES (1, $expires)",
            &HashMap::from([("expires".into(), Value::Timestamp(2_000_000))]),
        )
        .unwrap();
    }
    db.close().unwrap();
    drop(db);
    // Use the compaction fixture's physical layout with only 64
    // half-empty leaves: deleting alternate leaves creates interior free pages.
    const PAYLOAD: redb::TableDefinition<u64, &[u8]> = redb::TableDefinition::new("payload");
    let raw = redb::Database::create(&path).unwrap();
    let tx = raw.begin_write().unwrap();
    {
        let mut table = tx.open_table(PAYLOAD).unwrap();
        let body = vec![0x5c; 4100];
        for key in 0..64 {
            table.insert(key, body.as_slice()).unwrap();
        }
    }
    tx.commit().unwrap();
    drop(raw);
    let db = open();
    db.compact_now().unwrap();
    db.close().unwrap();
    drop(db);
    let raw = redb::Database::create(&path).unwrap();
    let tx = raw.begin_write().unwrap();
    {
        let mut table = tx.open_table(PAYLOAD).unwrap();
        for key in (0..63).step_by(2) {
            table.remove(key).unwrap();
        }
    }
    tx.commit().unwrap();
    raw.begin_write().unwrap().commit().unwrap();
    drop(raw);
    let db = open();
    if retention {
        db.execute(
            "SET STORAGE_COMPACTION START_RATIO 1 MIN_INTERVAL 1 SECONDS",
            &HashMap::new(),
        )
        .unwrap();
        let checked = db.run_maintenance_cycle().unwrap();
        assert_eq!(checked.pruning.pruned_rows, 0);
        assert!(!checked.compaction.ran);
    }
    db.execute(
        "SET STORAGE_COMPACTION START_RATIO 0.000001 MIN_INTERVAL 1 SECONDS",
        &HashMap::new(),
    )
    .unwrap();
    let _due_clock = Wallclock::test_clock_guard(|| 3_000_000);
    let (persistence, _, _) = db.maintenance_publication_state_for_test();
    assert!(
        persistence.allocator_free_page_ratio().unwrap().unwrap()
            >= db.storage_compaction_policy().start_ratio()
    );
    let identity = persistence.physical_identity().unwrap();
    let state = || {
        let (_, interval, interrupted) = db.maintenance_publication_state_for_test();
        let durable_checkpoint = persistence
            .with_db(|db| {
                let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
                let table = match read.open_table(redb::TableDefinition::<&str, &[u8]>::new(
                    "writer_storage_compaction",
                )) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                    Err(error) => return Err(RedbPersistence::storage_error(error)),
                };
                Ok(table
                    .get("cursor")
                    .map_err(RedbPersistence::storage_error)?
                    .map(|bytes| bytes.value().to_vec()))
            })
            .unwrap();
        let state = persistence.storage_compaction.lock().unwrap();
        let cursor = state.0.checkpoint();
        (
            (
                cursor.0,
                cursor.1.map(str::to_owned),
                cursor.2,
                cursor.3.to_vec(),
                cursor.4,
            ),
            (state.1.pages_examined, state.1.pages_relocated),
            state.2,
            state.3.as_ref().map(PersistenceBytes::bytes),
            interval,
            interrupted,
            durable_checkpoint,
        )
    };
    let old_reader = (!retention).then(|| persistence.owner_cursor_pin().unwrap());
    let read_payload = |pin: &OwnerCursorStoragePin| {
        use redb::ReadableTable as _;
        pin.read
            .open_table(PAYLOAD)
            .unwrap()
            .iter()
            .unwrap()
            .map(|row| {
                let (key, value) = row.unwrap();
                (key.value(), value.value().to_vec())
            })
            .collect::<Vec<_>>()
    };
    let original = old_reader.as_ref().map(&read_payload);
    let before = state();
    let mut held_registry = None;
    let mut held_physical = None;
    let mut handoff = None;
    let mut pause = None;
    match contention {
        MaintenanceContention::Registry => {
            let mut held = ReaderSnapshotRegistryLock::create(&runtime, identity).unwrap();
            held.lock_exclusive().unwrap();
            held_registry = Some(held);
        }
        MaintenanceContention::PhysicalHold => {
            let held = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            assert!(matches!(
                physical_admission::try_byte(
                    &held,
                    physical_admission::READER_HOLD_OFFSET,
                    physical_admission::ByteLock::Shared
                )
                .unwrap(),
                physical_admission::Granted::Yes
            ));
            held_physical = Some(held);
        }
        MaintenanceContention::PendingHandoff => {
            let (database_identity, run, _, _) = published_writer_identity(&path).unwrap();
            let parked = pause_reader_registration_lock_for_test(
                &path,
                ReaderRegistrationLockPhase::DescriptorOpened,
            )
            .unwrap();
            let reader_path = path.clone();
            let reader_runtime = runtime.clone();
            handoff = Some(std::thread::spawn(move || {
                ReadSnapshotRegistrationGuard::owner_handoff_start(
                    &reader_path,
                    database_identity,
                    run,
                    Some(&reader_runtime),
                )
            }));
            assert!(parked.wait_for_attempt_for_test(1, std::time::Duration::from_secs(30)));
            pause = Some(parked);
        }
    }
    let observed = if retention {
        db.run_pruning_cycle_checked()
            .map(|report| (report.compacted, report.pruned_rows))
    } else {
        db.run_maintenance_cycle()
            .map(|report| (report.compaction.ran, report.pruning.pruned_rows))
    };
    let after = state();
    // Sample optional work before explicit controls can alter its fixture.
    let explicit = db.compact_now();
    let purge_refused = matches!(persistence.acquire_authoritative_purge_publication(), Err(Error::ReaderCoordinationUnavailable { ref operation, .. }) if operation == "authoritative purge");
    // Always release the real locks and join before an assertion can panic.
    drop(held_registry);
    drop(held_physical);
    if let Some(pause) = pause {
        pause.stop_pausing_for_test();
    }
    if let Some(handoff) = handoff {
        drop(handoff.join().unwrap().unwrap());
    }
    let (ran, pruned) = observed
        .expect("temporary contention must defer optional maintenance through its actual caller");
    assert!(
        !ran,
        "contention cannot authorize a successful relocation batch"
    );
    assert!(
        matches!(explicit, Err(Error::ReaderCoordinationUnavailable { ref operation, .. }) if operation == "storage reclamation"),
        "explicit compaction must refuse: {explicit:?}"
    );
    assert!(purge_refused, "purge must refuse temporary contention");
    assert_eq!(
        (&after.0, after.1, after.2, after.3, after.5),
        (&before.0, before.1, before.2, before.3, before.5),
        "deferred maintenance cannot advance cursor, checkpoint, counters or interrupted state"
    );
    assert_eq!(
        after.6, before.6,
        "deferred maintenance leaves its durable checkpoint byte-identical"
    );
    if retention {
        assert!(before.4.is_some(), "fixture has a prior completed interval");
        assert_eq!(
            after.4, None,
            "deferred retention invalidates the completed interval"
        );
    } else {
        assert_eq!(
            after.4, before.4,
            "deferred automatic work keeps its interval"
        );
    }
    assert_eq!(
        old_reader.as_ref().map(&read_payload),
        original,
        "old reader keeps its exact payload through deferred maintenance"
    );
    assert_eq!(
        pruned,
        u64::from(retention),
        "committed prune count survives deferral"
    );
    let resumed = db.run_maintenance_cycle().unwrap();
    assert_eq!(
        resumed.pruning.pruned_rows, 0,
        "no new expiration is needed to retry pending relocation"
    );
    assert!(
        resumed.compaction.ran,
        "pending relocation advances after contention releases"
    );
    assert_ne!(
        state(),
        before,
        "resumption changes actual compaction state"
    );
    assert_eq!(
        old_reader.as_ref().map(&read_payload),
        original,
        "old reader keeps its exact payload through resumed maintenance"
    );
    drop(old_reader);
    db.close().unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn automatic_compaction_defers_registry_contention() {
    maintenance_contention_row(MaintenanceContention::Registry, false);
}

#[cfg(target_os = "linux")]
#[test]
fn automatic_compaction_defers_physical_publication_hold() {
    maintenance_contention_row(MaintenanceContention::PhysicalHold, false);
}

#[cfg(target_os = "linux")]
#[test]
fn retention_compaction_defers_registry_contention() {
    maintenance_contention_row(MaintenanceContention::Registry, true);
}

#[cfg(target_os = "linux")]
#[test]
fn retention_compaction_defers_physical_publication_hold() {
    maintenance_contention_row(MaintenanceContention::PhysicalHold, true);
}

pub(super) fn before_physical_presence_probe() {
    let hook = BEFORE_PHYSICAL_PRESENCE_PROBE.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(target_os = "linux")]
#[test]
fn inventory_excludes_publication_until_its_physical_probe_finishes() {
    let (_directory, persistence, runtime) = writer();
    let persistence = Arc::new(persistence);
    let identity = persistence.physical_identity().unwrap();
    let publisher = Arc::clone(&persistence);
    let observed_busy = Rc::new(RefCell::new(false));
    let observed = Rc::clone(&observed_busy);
    BEFORE_PHYSICAL_PRESENCE_PROBE.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move || {
            // Real sanctioned publisher, driven exactly in the scan/probe gap.
            // Admission must yield before publishing even its first registration.
            *observed.borrow_mut() = matches!(
                publisher.owner_cursor_pin_for_admission(),
                Err(OwnerCursorPinAdmissionError::Busy(_))
            );
        }))
    });
    let owner = persistence
        .writer_file
        .read()
        .unwrap()
        .as_ref()
        .cloned()
        .unwrap();
    let audit = persistence.refuse_unknown_reader_presence(&owner, "storage reclamation");
    assert!(
        *observed_busy.borrow(),
        "the audit must own physical exclusion across registry capture and presence probe"
    );
    audit.unwrap();
    assert!(snapshot_paths(&runtime, identity).is_empty());
    let pin = persistence.owner_cursor_pin().unwrap();
    let handle = pin._registration.snapshot.handle_identity;
    let snapshot = pin.storage_identity();
    assert!(
        matches!(capture_reader_snapshot_registry(identity, Some(&runtime)), ReaderSnapshotRegistry::Complete(snapshots) if snapshots.iter().any(|registered| registered.handle_identity == handle && registered.storage_snapshot == snapshot))
    );
}

#[cfg(target_os = "linux")]
#[test]
fn purge_without_registered_readers_keeps_physical_custody() {
    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();
    assert!(
        matches!(capture_reader_snapshot_registry(identity, Some(&runtime)), ReaderSnapshotRegistry::Complete(snapshots) if snapshots.is_empty())
    );
    let publication = persistence
        .acquire_authoritative_purge_publication()
        .unwrap();
    // The empty-fence path still owns exclusion. A new real description is
    // used so an aliased OFD cannot manufacture a successful safety proof.
    let arriving = OpenOptions::new()
        .read(true)
        .open(persistence.path())
        .unwrap();
    let while_prepared = physical_admission::try_byte(
        &arriving,
        physical_admission::READER_HOLD_OFFSET,
        physical_admission::ByteLock::Shared,
    )
    .unwrap();
    drop(arriving);
    drop(publication);
    assert!(
        matches!(while_prepared, physical_admission::Granted::HeldByAnother),
        "purge must retain H even with zero registered readers/fences"
    );
    let arriving = OpenOptions::new()
        .read(true)
        .open(persistence.path())
        .unwrap();
    assert!(matches!(
        physical_admission::try_byte(
            &arriving,
            physical_admission::READER_HOLD_OFFSET,
            physical_admission::ByteLock::Shared
        )
        .unwrap(),
        physical_admission::Granted::Yes
    ));
}

/// Control 1: Audited reader snapshots under hold succeeds with complete own-reader inventory under unrelated root contention.
///
/// Contract: Exporting under concurrent unrelated-store reader registration succeeds with
/// authenticated complete own-reader inventory; active own readers are never inferred absent.
///
/// Baseline oracle (expected RED): In baseline code, LockedWriterFile::audit_reader_snapshots_under_hold
/// delegates directly to capture_reader_snapshot_registry without physical admission hold.
/// When an unrelated store holds the root serializer (e.g. during reader join/leave),
/// the scan meets root contention (WouldBlock) and returns ReaderSnapshotRegistry::Contended.
/// audit_reader_snapshots_under_hold translates Contended to ReaderAuditOutcome::Busy(runtime),
/// causing compaction and export to fail closed with Error::ReaderCoordinationUnavailable.
///
/// Repaired contract (expected GREEN): On Linux, LockedWriterFile::audit_reader_snapshots_under_hold
/// acquires exclusive READER_HOLD_OFFSET before scanning. Because the physical hold freezes
/// this store's admission and publication, the writer proves its own readers are frozen and
/// authenticates complete own-reader inventory, yielding ReaderAuditOutcome::Proven without
/// being blocked by unrelated root contention.
#[cfg(target_os = "linux")]
#[test]
fn audited_reader_snapshots_under_hold_succeeds_under_unrelated_root_contention() {
    let (_directory, persistence, runtime) = writer();
    let owner = persistence
        .writer_file
        .read()
        .unwrap()
        .as_ref()
        .cloned()
        .unwrap();

    let pin = persistence.owner_cursor_pin().unwrap();
    let handle = pin._registration.snapshot.handle_identity;
    let snapshot = pin.storage_identity();

    // Hold the task-scoped runtime root serializer on a separate real descriptor,
    // simulating an unrelated store in the same runtime root currently joining or leaving.
    let root_serializer =
        serialize_runtime_root(&runtime, false).expect("acquire runtime root serializer");

    let outcome = owner.audit_reader_snapshots_under_hold();

    // Release serializer before any blocking cleanup
    drop(root_serializer);

    match outcome {
        writer_file::ReaderAuditOutcome::Proven(audit) => {
            assert!(
                !audit.unknown,
                "registered own reader must not be marked unknown"
            );
            match &audit.registry {
                ReaderSnapshotRegistry::Complete(snapshots) => {
                    assert!(
                        snapshots.iter().any(|registered| {
                            registered.handle_identity == handle
                                && registered.storage_snapshot == snapshot
                        }),
                        "own registered reader must be authenticated in complete audit inventory"
                    );
                }
                other => panic!("expected Complete registry in Proven audit, got {other:?}"),
            }
        }
        writer_file::ReaderAuditOutcome::Busy(ref path) => {
            panic!(
                "baseline refusal (RED): audit returned Busy({path:?}) due to unrelated root contention; repaired contract requires Proven"
            );
        }
        writer_file::ReaderAuditOutcome::Refused(ref path, ref err) => {
            panic!("unexpected audit Refused({path:?}, {err})");
        }
    }

    drop(pin);
}

/// Control 2: Generic reader registry scanner under root contention strictly remains Contended.
///
/// Contract: Existing general capture_reader_snapshot_registry without exclusive physical
/// admission MUST fail closed on a contended root, returning Contended. Once the root serializer
/// is released, it returns Complete with own registered reader present.
#[test]
fn generic_reader_registry_scanner_fails_closed_under_root_serializer_contention() {
    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();

    let pin = persistence.owner_cursor_pin().unwrap();
    let handle = pin._registration.snapshot.handle_identity;
    let snapshot = pin.storage_identity();

    // Hold the task-scoped runtime root serializer on a separate real descriptor
    let root_serializer =
        serialize_runtime_root(&runtime, false).expect("acquire runtime root serializer");

    let registry = capture_reader_snapshot_registry(identity, Some(&runtime));
    assert!(
        matches!(&registry, ReaderSnapshotRegistry::Contended(path) if path == &runtime),
        "generic scanner without physical hold must return Contended on contended root: {registry:?}"
    );

    // Release serializer
    drop(root_serializer);

    let registry = capture_reader_snapshot_registry(identity, Some(&runtime));
    assert!(
        matches!(
            &registry,
            ReaderSnapshotRegistry::Complete(snapshots)
                if snapshots.iter().any(|registered| {
                    registered.handle_identity == handle
                        && registered.storage_snapshot == snapshot
                })
        ),
        "generic scanner succeeds after serializer release with authenticated own reader: {registry:?}"
    );

    drop(pin);
}
