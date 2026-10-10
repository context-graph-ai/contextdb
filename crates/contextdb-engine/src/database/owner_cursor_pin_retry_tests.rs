use super::*;
use crate::local_transport::ManualDeadlineClock;
use std::cell::RefCell;

thread_local! {
    static AFTER_BUSY_ATTEMPT: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
}

pub(super) fn after_busy_attempt_for_test() {
    AFTER_BUSY_ATTEMPT.with(|hook| {
        if let Some(hook) = hook.borrow_mut().take() {
            hook();
        }
    });
}

#[test]
fn busy_cursor_pin_retries_outside_commit_and_recaptures_the_complete_root() {
    let directory = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(directory.path().join("retry.db")).unwrap());
    db.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute("INSERT INTO items VALUES (1, 'original')", &HashMap::new())
        .unwrap();
    let before = db.try_snapshot_for_read().unwrap();
    let resume = Arc::new(std::sync::Barrier::new(2));
    let (busy, busy_seen) = std::sync::mpsc::sync_channel(1);
    let reader_db = Arc::clone(&db);
    let reader_resume = Arc::clone(&resume);
    let reader = db
        .persistence
        .as_ref()
        .unwrap()
        .with_owner_cursor_handle_locked_for_test(|| {
            let reader = std::thread::spawn(move || {
                AFTER_BUSY_ATTEMPT.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(move || {
                        busy.send(()).unwrap();
                        reader_resume.wait();
                    }));
                });
                let clock: Arc<dyn DeadlineClock> = Arc::new(ManualDeadlineClock::at(0));
                let budget = ReadRegistrationBudget::new(Arc::clone(&clock), 0, 10_000);
                let fixture_operation_reader_1 = reader_db
                    .declared_reader_memory()
                    .expect("admit the actual fixture reader");

                reader_db
                    .bounded_cursor_snapshot_registration(
                        clock,
                        10_000,
                        &budget,
                        &OwnerReadCancellation::new(),
                        fixture_operation_reader_1.clone(),
                    )
                    .unwrap()
                    .unwrap()
            });
            busy_seen.recv().unwrap();
            assert!(
                db.tx_mgr
                    .try_with_commit_lock_for(Duration::ZERO, || ())
                    .is_some(),
                "a busy pin releases the commit boundary before its retry wait"
            );
            assert!(db.snapshot_registry.state.lock().active.is_empty());
            reader
        });

    // A complete intervening commit must replace the root selected by the
    // failed attempt. No failed native transaction or version hold survives.
    db.execute(
        "UPDATE items SET body = 'replacement' WHERE id = 1",
        &HashMap::new(),
    )
    .unwrap();
    let expected = db.try_snapshot_for_read().unwrap();
    assert_ne!(before, expected);
    resume.wait();
    let (snapshot, _, _, storage, registration) = reader.join().unwrap();
    assert_eq!(snapshot, expected);
    assert!(storage.is_some());
    assert_eq!(db.snapshot_registry.state.lock().active.len(), 1);
    assert_eq!(
        db.execute_at_snapshot("SELECT body FROM items", &HashMap::new(), snapshot)
            .unwrap()
            .rows,
        vec![vec![Value::Text("replacement".to_owned())]],
    );
    drop((storage, registration));
    assert!(db.snapshot_registry.state.lock().active.is_empty());
}

#[test]
fn busy_cursor_pin_cancellation_and_deadline_finish_before_handle_release() {
    for cancelled in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let db = Arc::new(Database::open(directory.path().join("withdrawn.db")).unwrap());
        let clock = Arc::new(ManualDeadlineClock::at(0));
        let withdrawn = OwnerReadCancellation::new();
        let resume = Arc::new(std::sync::Barrier::new(2));
        let (busy, busy_seen) = std::sync::mpsc::sync_channel(1);
        let reader_db = Arc::clone(&db);
        let reader_clock = Arc::clone(&clock);
        let reader_withdrawn = withdrawn.clone();
        let reader_resume = Arc::clone(&resume);
        db.persistence
            .as_ref()
            .unwrap()
            .with_owner_cursor_handle_locked_for_test(|| {
                let reader = std::thread::spawn(move || {
                    AFTER_BUSY_ATTEMPT.with(|hook| {
                        *hook.borrow_mut() = Some(Box::new(move || {
                            busy.send(()).unwrap();
                            reader_resume.wait();
                        }));
                    });
                    let clock: Arc<dyn DeadlineClock> = reader_clock;
                    let budget = ReadRegistrationBudget::new(Arc::clone(&clock), 0, 10_000);
                    let fixture_operation_reader_2 = reader_db
                        .declared_reader_memory()
                        .expect("admit the actual fixture reader");

                    reader_db
                        .bounded_cursor_snapshot_registration(
                            clock,
                            10_000,
                            &budget,
                            &reader_withdrawn,
                            fixture_operation_reader_2.clone(),
                        )
                        .map(|pin| pin.is_some())
                });
                busy_seen.recv().unwrap();
                assert!(
                    db.tx_mgr
                        .try_with_commit_lock_for(Duration::ZERO, || ())
                        .is_some()
                );
                if cancelled {
                    withdrawn.cancel();
                } else {
                    clock.advance_to(10_001);
                }
                resume.wait();
                let result = reader.join().unwrap();
                if cancelled {
                    assert!(!result.unwrap());
                } else {
                    assert!(matches!(result, Err(Error::ReadFailure(failure))
                        if failure.kind() == ReadFailureKind::OwnerLimitExceeded));
                }
                assert!(db.snapshot_registry.state.lock().active.is_empty());
            });
    }
}

#[cfg(all(unix, feature = "test-seams"))]
#[derive(Clone, Copy)]
enum RegistryWaitOutcome {
    Released,
    Deadline,
    Cancelled,
}

#[cfg(all(unix, feature = "test-seams"))]
#[derive(Clone, Copy)]
enum RegistryWaitPhase {
    Registration,
    ProviderScan,
}

#[cfg(all(unix, feature = "test-seams"))]
fn owner_channel_registry_wait(outcome: RegistryWaitOutcome, phase: RegistryWaitPhase) {
    use crate::local_transport::{
        LocalHandshake, LocalRequest, LocalRequestEnvelope, LocalResponse,
    };
    use crate::owner_read::{OwnerClient, OwnerReadScaffoldError};
    use contextdb_core::read_contract::{ReadFailureDetail, ReadFailureLimit};
    use std::collections::BTreeMap;
    use std::os::unix::fs::PermissionsExt as _;

    let directory = tempfile::tempdir().unwrap();
    let runtime_dir = directory.path().join("runtime");
    std::fs::create_dir(&runtime_dir).unwrap();
    std::fs::set_permissions(&runtime_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    let path = directory.path().join("registry-wait.db");
    let clock = Arc::new(ManualDeadlineClock::at(0));
    let (drain_started, _drain_seen) = std::sync::mpsc::channel();
    let limits = ReadLimits {
        active_ms: 60_000,
        cursor_page_rows: 1,
        ..ReadLimits::default()
    };
    let db = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime_dir),
                limits: OwnerReadLimits {
                    limits,
                    ..OwnerReadLimits::default()
                },
                timeouts: OwnerServiceTimeouts {
                    request_ms: 600_000,
                    ..OwnerServiceTimeouts::default()
                },
                test_hooks: Some(crate::OwnerReadTestHooks {
                    clock: clock.clone(),
                    drain_started,
                    active_request_registered: None,
                    accept_thread_exited: None,
                    accept_thread_joined: None,
                    service_dropped: None,
                    quiescent_stop: None,
                }),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .unwrap();
    db.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute("INSERT INTO items VALUES (1), (2)", &HashMap::new())
        .unwrap();
    let service = db
        .owner_read_service
        .as_ref()
        .expect("real owner channel serves");
    let (identity, run, user, _) = crate::persistence::published_writer_identity(&path).unwrap();
    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut client = executor
        .block_on(OwnerClient::connect(
            service.channel_path(),
            LocalHandshake::current(identity, run, user),
            ReadClientTimeouts {
                response_ms: 600_000,
                ..ReadClientTimeouts::default()
            },
            clock.clone(),
        ))
        .unwrap();
    let cancelled = OwnerReadCancellation::new();
    let reader_cancelled = cancelled.clone();
    let pause = ApplyPhasePauseGuard::arm_for_test(
        db.snapshot_registry.owner_cursor_busy_test_pause.clone(),
    );
    let (finished, completed) = std::sync::mpsc::sync_channel(1);
    let scan_pause = matches!(phase, RegistryWaitPhase::ProviderScan).then(|| {
        crate::persistence::read_persistence_test_scaffold::pause_reader_snapshot_registry_scan_for_test(
            &path,
        )
        .unwrap()
    });
    let start_reader = || {
        std::thread::spawn(move || {
            let result = executor.block_on(client.request_watching(
                LocalRequestEnvelope {
                    limits,
                    request: LocalRequest::CursorOpen {
                        statement: "SELECT id FROM items ORDER BY id".to_owned(),
                        params: BTreeMap::new(),
                    },
                },
                None,
                Some(&reader_cancelled),
            ));
            finished.send(result).unwrap();
        })
    };
    let mut start_reader = Some(start_reader);
    let scan_reader = scan_pause.as_ref().map(|scan| {
        let reader = start_reader.take().unwrap()();
        scan.wait_until_inspected_for_test();
        reader
    });
    let reader = db
        .persistence
        .as_ref()
        .unwrap()
        .with_owner_cursor_registry_locked_for_test(|| {
            let reader = match scan_reader {
                Some(reader) => {
                    scan_pause.as_ref().unwrap().resume_for_test();
                    reader
                }
                None => start_reader.take().unwrap()(),
            };
            assert!(
                pause.wait_until_reached(Duration::from_secs(30)),
                "the actual owner request meets the held registry"
            );
            assert!(
                db.tx_mgr
                    .try_with_commit_lock_for(Duration::ZERO, || ())
                    .is_some(),
                "registry retries release the commit boundary"
            );
            assert!(db.snapshot_registry.state.lock().active.is_empty());
            match outcome {
                RegistryWaitOutcome::Released => (),
                RegistryWaitOutcome::Deadline => clock.advance_to(limits.active_ms + 1),
                RegistryWaitOutcome::Cancelled => cancelled.cancel(),
            }
            if !matches!(outcome, RegistryWaitOutcome::Released) {
                pause.release();
                let result = completed
                    .recv_timeout(Duration::from_secs(30))
                    .expect("withdrawal answers while the registry is still held");
                match outcome {
                    RegistryWaitOutcome::Deadline => assert!(
                        matches!(&result,
                    Err(OwnerReadScaffoldError::Database(Error::ReadFailure(failure)))
                    | Err(OwnerReadScaffoldError::Refused(failure))
                        if failure.kind() == ReadFailureKind::OwnerLimitExceeded
                        && matches!(failure.detail(), ReadFailureDetail::OwnerLimitExceeded(detail)
                            if detail.limit == ReadFailureLimit::ActiveMs)),
                        "typed active-time refusal: {result:?}"
                    ),
                    RegistryWaitOutcome::Cancelled => assert!(
                        matches!(
                            &result,
                            Err(OwnerReadScaffoldError::Database(Error::ReadCancelled))
                        ),
                        "typed cancellation refusal: {result:?}"
                    ),
                    RegistryWaitOutcome::Released => unreachable!(),
                }
                assert!(db.snapshot_registry.state.lock().active.is_empty());
            }
            reader
        });
    if matches!(outcome, RegistryWaitOutcome::Released) {
        let expected_first = if matches!(phase, RegistryWaitPhase::ProviderScan) {
            // The failed scan releases every pin and the commit boundary.
            // Retry must capture the whole root after this intervening commit.
            db.execute("UPDATE items SET id = 3 WHERE id = 1", &HashMap::new())
                .unwrap();
            Value::Int64(2)
        } else {
            Value::Int64(1)
        };
        pause.release();
        let responses = completed
            .recv_timeout(Duration::from_secs(30))
            .unwrap()
            .expect("healthy cursor succeeds after registry release");
        let [LocalResponse::CursorOpened { opened }] = responses.as_slice() else {
            panic!("expected owner cursor page: {responses:?}");
        };
        let page = crate::read_contract::decode_observed_cursor_page(
                &opened.payload,
                ReadLimits::SHIPPED_MEMORY,
            )
            .unwrap()
            .page;
        assert_eq!(page.rows, vec![vec![expected_first]]);
        assert!(page.has_more);
    }
    reader.join().unwrap();
    if let Some(scan) = scan_pause {
        scan.finish_for_test();
    }
    db.close().unwrap();
}

#[cfg(all(unix, feature = "test-seams"))]
#[test]
fn owner_channel_cursor_retries_registry_contention_until_release() {
    owner_channel_registry_wait(
        RegistryWaitOutcome::Released,
        RegistryWaitPhase::Registration,
    );
}

#[cfg(all(unix, feature = "test-seams"))]
#[test]
fn owner_channel_cursor_registry_contention_obeys_active_deadline() {
    owner_channel_registry_wait(
        RegistryWaitOutcome::Deadline,
        RegistryWaitPhase::Registration,
    );
}

#[cfg(all(unix, feature = "test-seams"))]
#[test]
fn owner_channel_cursor_registry_contention_obeys_cancellation() {
    owner_channel_registry_wait(
        RegistryWaitOutcome::Cancelled,
        RegistryWaitPhase::Registration,
    );
}

#[cfg(all(unix, feature = "test-seams"))]
#[test]
fn owner_channel_cursor_retries_registry_scan_contention_until_release() {
    owner_channel_registry_wait(
        RegistryWaitOutcome::Released,
        RegistryWaitPhase::ProviderScan,
    );
}

#[cfg(all(unix, feature = "test-seams"))]
#[test]
fn owner_channel_cursor_registry_scan_contention_obeys_active_deadline() {
    owner_channel_registry_wait(
        RegistryWaitOutcome::Deadline,
        RegistryWaitPhase::ProviderScan,
    );
}

#[cfg(all(unix, feature = "test-seams"))]
#[test]
fn owner_channel_cursor_registry_scan_contention_obeys_cancellation() {
    owner_channel_registry_wait(
        RegistryWaitOutcome::Cancelled,
        RegistryWaitPhase::ProviderScan,
    );
}

#[test]
fn healthy_registry_scan_contention_never_refuses_concurrent_cursors() {
    let directory = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(directory.path().join("cursor.db")).unwrap());
    db.execute(
        "CREATE TABLE facts (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute("INSERT INTO facts VALUES (1)", &HashMap::new())
        .unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(10));
    let threads: Vec<_> = (0..10)
        .map(|worker| {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let session = db.read_session(ReadLimits::default()).unwrap();
                barrier.wait();
                let mut failures = Vec::new();
                for round in 0..100 {
                    match session.open_cursor("SELECT id FROM facts", &HashMap::new()) {
                        Ok(cursor) => {
                            assert_eq!(cursor.first_page().rows, vec![vec![Value::Int64(1)]]);
                            cursor.close().unwrap();
                        }
                        Err(error) => failures.push((worker, round, error)),
                    }
                }
                failures
            })
        })
        .collect();
    let failures: Vec<_> = threads
        .into_iter()
        .flat_map(|thread| thread.join().unwrap())
        .collect();
    assert!(
        failures.is_empty(),
        "1,000 healthy cursor opens: {failures:?}"
    );
    db.close().unwrap();
}

#[cfg(all(unix, feature = "test-seams"))]
#[derive(Clone, Copy)]
enum RegistryTeardownOutcome {
    Released,
    Cancelled,
    Replaced,
}

#[cfg(all(unix, feature = "test-seams"))]
fn owner_channel_registry_teardown(outcome: RegistryTeardownOutcome) {
    use crate::local_transport::{
        LocalHandshake, LocalRequest, LocalRequestEnvelope, LocalResponse,
    };
    use crate::owner_read::{OwnerClient, OwnerReadScaffoldError};
    use crate::persistence::read_persistence_test_scaffold::{
        ReaderRegistrationLockPhase, pause_reader_registration_lock_for_test,
    };
    use std::collections::BTreeMap;
    use std::os::unix::fs::PermissionsExt as _;

    let directory = tempfile::tempdir().unwrap();
    let runtime_dir = directory.path().join("runtime");
    std::fs::create_dir(&runtime_dir).unwrap();
    std::fs::set_permissions(&runtime_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    let path = directory.path().join("registry-wait.db");
    let clock = Arc::new(ManualDeadlineClock::at(0));
    let (drain_started, _drain_seen) = std::sync::mpsc::channel();
    let limits = ReadLimits {
        active_ms: 60_000,
        cursor_page_rows: 1,
        ..ReadLimits::default()
    };
    // A reader of a copied store publishes no writer-incarnation fence.
    // It remains on its fixed snapshot while a writer takes ownership, so
    // its final close can legitimately remove the registry during admission.
    let seed = Database::open(&path).unwrap();
    seed.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    seed.execute("INSERT INTO items VALUES (1), (2)", &HashMap::new())
        .unwrap();
    seed.close().unwrap();
    drop(seed);
    std::fs::remove_file(crate::persistence::store_companion_path(&path)).unwrap();
    let mut last_reader = Some(crate::ReadSession::open(&path).unwrap());
    let db = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime_dir),
                limits: OwnerReadLimits {
                    limits,
                    ..OwnerReadLimits::default()
                },
                timeouts: OwnerServiceTimeouts {
                    request_ms: 600_000,
                    ..OwnerServiceTimeouts::default()
                },
                test_hooks: Some(crate::OwnerReadTestHooks {
                    clock: clock.clone(),
                    drain_started,
                    active_request_registered: None,
                    accept_thread_exited: None,
                    accept_thread_joined: None,
                    service_dropped: None,
                    quiescent_stop: None,
                }),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .unwrap();
    let service = db
        .owner_read_service
        .as_ref()
        .expect("real owner channel serves");
    let (identity, run, user, _) = crate::persistence::published_writer_identity(&path).unwrap();
    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut client = executor
        .block_on(OwnerClient::connect(
            service.channel_path(),
            LocalHandshake::current(identity, run, user),
            ReadClientTimeouts {
                response_ms: 600_000,
                ..ReadClientTimeouts::default()
            },
            clock.clone(),
        ))
        .unwrap();
    let cancelled = OwnerReadCancellation::new();
    let reader_cancelled = cancelled.clone();
    let persistence = db.persistence.as_ref().unwrap();
    let registry_paths = persistence.owner_cursor_registry_paths_for_test();
    assert!(registry_paths[0].exists());
    let descriptor_pause = pause_reader_registration_lock_for_test(
        &path,
        ReaderRegistrationLockPhase::DescriptorOpened,
    )
    .unwrap();
    let busy_pause = ApplyPhasePauseGuard::arm_for_test(
        db.snapshot_registry.owner_cursor_busy_test_pause.clone(),
    );
    let (finished, completed) = std::sync::mpsc::sync_channel(1);
    let reader = std::thread::spawn(move || {
        let result = executor.block_on(client.request_watching(
            LocalRequestEnvelope {
                limits,
                request: LocalRequest::CursorOpen {
                    statement: "SELECT id FROM items ORDER BY id".to_owned(),
                    params: BTreeMap::new(),
                },
            },
            None,
            Some(&reader_cancelled),
        ));
        finished.send(result).unwrap();
    });
    assert!(
        descriptor_pause.wait_for_attempt_for_test(1, Duration::from_secs(30)),
        "real owner admission opens the registry descriptor before validation"
    );
    let parked = registry_paths[0].with_extension("parked");
    if matches!(outcome, RegistryTeardownOutcome::Replaced) {
        // Keep the original descriptor linked: replacement must never look like
        // a trusted last-reader departure, even when the new file is private.
        std::fs::rename(&registry_paths[0], &parked).unwrap();
        let replacement = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&registry_paths[0])
            .unwrap();
        replacement
            .set_permissions(std::fs::Permissions::from_mode(0o600))
            .unwrap();
    } else {
        drop(last_reader.take());
        assert!(
            registry_paths.iter().all(|path| !path.exists()),
            "the last real registration unlinks its registry lock"
        );
    }
    descriptor_pause.stop_pausing_for_test();
    if !matches!(outcome, RegistryTeardownOutcome::Replaced) {
        assert!(
            busy_pause.wait_until_reached(Duration::from_secs(30)),
            "trusted departure yields to the outer bounded retry"
        );
        assert!(
            db.tx_mgr
                .try_with_commit_lock_for(Duration::ZERO, || ())
                .is_some(),
            "teardown retries release the commit boundary"
        );
        assert!(db.snapshot_registry.state.lock().active.is_empty());
        if matches!(outcome, RegistryTeardownOutcome::Cancelled) {
            cancelled.cancel();
        }
    }
    busy_pause.release();
    let result = completed.recv_timeout(Duration::from_secs(30)).unwrap();
    match outcome {
        RegistryTeardownOutcome::Released => {
            let responses = result.expect("new cursor survives the last reader's teardown");
            let [LocalResponse::CursorOpened { opened }] = responses.as_slice() else {
                panic!("expected owner cursor page: {responses:?}");
            };
            let page = crate::read_contract::decode_observed_cursor_page(
                &opened.payload,
                ReadLimits::SHIPPED_MEMORY,
            )
            .unwrap()
            .page;
            assert_eq!(page.rows, vec![vec![Value::Int64(1)]]);
            assert!(page.has_more);
            assert!(
                descriptor_pause.attempts_for_test() >= 2,
                "successful admission reopens rather than using the departed descriptor"
            );
        }
        RegistryTeardownOutcome::Cancelled => {
            assert!(
                matches!(
                    &result,
                    Err(OwnerReadScaffoldError::Database(Error::ReadCancelled))
                ),
                "typed cancellation during the teardown retry: {result:?}"
            );
            assert!(db.snapshot_registry.state.lock().active.is_empty());
        }
        RegistryTeardownOutcome::Replaced => {
            assert!(
                matches!(&result,
                Err(OwnerReadScaffoldError::Database(Error::Other(message)))
                    if message.starts_with("ReaderCoordinationUnavailable: owner cursor pin")
                    && message.contains(&registry_paths[0].parent().unwrap().display().to_string())
                    && message.contains("not a trusted private regular file")),
                "replaced registry retains its terminal channel refusal: {result:?}"
            );
            assert_eq!(
                descriptor_pause.attempts_for_test(),
                1,
                "unsafe replacement never enters the retry loop"
            );
            std::fs::remove_file(&registry_paths[0]).unwrap();
            std::fs::rename(&parked, &registry_paths[0]).unwrap();
        }
    }
    reader.join().unwrap();
    drop(last_reader);
    db.close().unwrap();
}

#[cfg(all(unix, feature = "test-seams"))]
#[test]
fn owner_channel_cursor_survives_registry_teardown_before_validation() {
    run_owner_channel_registry_teardown_child("released");
}

#[cfg(all(unix, feature = "test-seams"))]
#[test]
fn owner_channel_cursor_registry_teardown_retry_obeys_cancellation() {
    run_owner_channel_registry_teardown_child("cancelled");
}

#[cfg(all(unix, feature = "test-seams"))]
#[test]
fn owner_channel_cursor_replaced_registry_before_validation_refuses_terminally() {
    run_owner_channel_registry_teardown_child("replaced");
}

#[cfg(all(unix, feature = "test-seams"))]
fn run_owner_channel_registry_teardown_child(outcome: &str) {
    use std::os::unix::fs::PermissionsExt as _;
    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let filter = format!("{}::owner_channel_registry_teardown_child", module_path!())
        .split_once("::")
        .unwrap()
        .1
        .to_owned();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &filter, "--nocapture"])
        .env("CONTEXTDB_OWNER_REGISTRY_TEARDOWN_CHILD", outcome)
        .env("XDG_RUNTIME_DIR", directory.path())
        .status()
        .unwrap();
    assert!(
        status.success(),
        "registry teardown child {outcome} failed: {status}"
    );
}

#[cfg(all(unix, feature = "test-seams"))]
#[test]
fn owner_channel_registry_teardown_child() {
    let Ok(outcome) = std::env::var("CONTEXTDB_OWNER_REGISTRY_TEARDOWN_CHILD") else {
        return;
    };
    owner_channel_registry_teardown(match outcome.as_str() {
        "released" => RegistryTeardownOutcome::Released,
        "cancelled" => RegistryTeardownOutcome::Cancelled,
        "replaced" => RegistryTeardownOutcome::Replaced,
        other => panic!("unknown registry teardown outcome {other}"),
    });
}
