//! A durable bounded read keeps its captured physical root across publication
//! and cleanup, including the pause before logical registration.
use super::*;
use crate::local_transport::ManualDeadlineClock;

fn captured_read_survives_compaction(cursor: bool) {
    let dir = tempfile::tempdir().expect("temporary durable store");
    let db = Arc::new(Database::open(dir.path().join("atomic-read.db")).expect("open store"));
    db.__set_currency_maintenance_interval(Duration::from_secs(3600));
    db.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, body TEXT NOT NULL) HISTORY CURRENT ONLY SYNC OFF",
        &HashMap::new(),
    )
    .expect("create history-limited table");
    for (id, body) in [(1, "original"), (2, "stable")] {
        db.execute(
            "INSERT INTO items (id, body) VALUES ($id, $body)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(id)),
                ("body".to_owned(), Value::Text(body.to_owned())),
            ]),
        )
        .expect("seed row");
    }

    let pause = ApplyPhasePauseGuard::arm_for_test(
        db.snapshot_registry.durable_admission_test_pause.clone(),
    );
    let reader_db = Arc::clone(&db);
    let reader = std::thread::spawn(move || {
        let clock: Arc<dyn DeadlineClock> = Arc::new(ManualDeadlineClock::at(0));
        let budget = ReadRegistrationBudget::new(Arc::clone(&clock), 0, 10_000);
        let withdrawn = OwnerReadCancellation::new();
        if cursor {
            let fixture_operation_reader_1 = reader_db
                .declared_reader_memory()
                .expect("admit the actual fixture reader");

            let (snapshot, _, _, storage, registration, root) = reader_db
                .bounded_cursor_snapshot_registration_with_root(
                    false,
                    clock,
                    10_000,
                    &budget,
                    &withdrawn,
                    fixture_operation_reader_1.clone(),
                )
                .expect("cursor admission succeeds")
                .expect("cursor remains active");
            assert!(storage.is_some(), "cursor captured a durable root");
            (
                snapshot,
                Box::new((storage, registration)) as Box<dyn Send + Sync>,
                root.expect("cursor retains its captured physical root"),
            )
        } else {
            let fixture_operation_reader_2 = reader_db
                .declared_reader_memory()
                .expect("admit the actual fixture reader");

            let (snapshot, _, registration, root) = reader_db
                .bounded_read_snapshot_registration_with_root(
                    false,
                    &budget,
                    &withdrawn,
                    fixture_operation_reader_2.clone(),
                )
                .expect("read admission succeeds")
                .expect("read remains active");
            (
                snapshot,
                registration,
                root.expect("pull retains its captured physical root"),
            )
        }
    });

    assert!(
        pause.wait_until_reached_blocking(),
        "capture reached registration seam"
    );
    assert!(
        db.tx_mgr
            .try_with_commit_lock_for(Duration::ZERO, || ())
            .is_some(),
        "a captured physical root permits foreground commit progress"
    );
    let writer_db = Arc::clone(&db);
    let (attempted, attempt_seen) = std::sync::mpsc::sync_channel(1);
    let writer = std::thread::spawn(move || {
        attempted.send(()).expect("report writer attempt");
        writer_db
            .execute(
                "UPDATE items SET body = 'replacement' WHERE id = 1",
                &HashMap::new(),
            )
            .expect("replace row");
        writer_db
            .compact_currency_versions()
            .expect("drive cleanup")
    });
    attempt_seen
        .recv()
        .expect("writer starts while reader is paused");
    // Join before releasing capture: writer and latest-root cleanup must finish
    // while the reader is still paused, protected solely by its physical root.
    let report = writer
        .join()
        .expect("writer completes while capture is paused");
    pause.release();
    let (snapshot, pin, root) = reader.join().expect("reader thread completes");
    assert_eq!(
        report.pruned_versions, 1,
        "latest-root cleanup reclaims the superseded version"
    );
    assert_eq!(db.__physical_version_count_for_test("items"), 2);
    let identity = root.storage_identity();
    assert_ne!(
        identity,
        db.writer_read_root().unwrap().unwrap().storage_identity(),
        "the registered reader retains the old physical identity"
    );
    assert!(
        db.__native_read_transaction_count_for_test(identity.transaction_id)
            .unwrap()
            > 0
    );
    let fixture_operation_reader_1 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let scope = db.enter_writer_read(Some(root.clone()), fixture_operation_reader_1.clone());
    let result = db
        .execute_at_snapshot(
            "SELECT id, body FROM items ORDER BY id",
            &HashMap::new(),
            snapshot,
        )
        .expect("read the selected complete snapshot");
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int64(1), Value::Text("original".to_owned())],
            vec![Value::Int64(2), Value::Text("stable".to_owned())],
        ]
    );
    drop(scope);
    drop(pin);
    drop(root);
    assert!(db.snapshot_registry.state.lock().active.is_empty());
    assert_eq!(
        db.__native_read_transaction_count_for_test(identity.transaction_id)
            .unwrap(),
        0,
        "final unpin releases the retained physical transaction"
    );
    db.compact_currency_versions()
        .expect("cleanup after reader release");
    assert_eq!(
        db.execute("SELECT id, body FROM items ORDER BY id", &HashMap::new())
            .unwrap()
            .rows,
        vec![
            vec![Value::Int64(1), Value::Text("replacement".to_owned())],
            vec![Value::Int64(2), Value::Text("stable".to_owned())]
        ]
    );
}

#[test]
fn durable_pull_registers_before_releasing_its_captured_root() {
    captured_read_survives_compaction(false);
}

#[test]
fn durable_cursor_registers_before_releasing_its_captured_root() {
    captured_read_survives_compaction(true);
}

#[test]
fn durable_admission_rechecks_cancellation_and_deadline_before_registering() {
    for cursor in [false, true] {
        for cancelled in [false, true] {
            let dir = tempfile::tempdir().expect("temporary durable store");
            let db = Arc::new(Database::open(dir.path().join("withdrawn.db")).expect("open store"));
            let physical = db.writer_read_root().unwrap().unwrap().storage_identity();
            let native_before = db
                .__native_read_transaction_count_for_test(physical.transaction_id)
                .unwrap();
            let pause = ApplyPhasePauseGuard::arm_for_test(
                db.snapshot_registry.durable_admission_test_pause.clone(),
            );
            let clock = Arc::new(ManualDeadlineClock::at(0));
            let withdrawn = OwnerReadCancellation::new();
            let reader_db = Arc::clone(&db);
            let reader_clock = Arc::clone(&clock);
            let reader_withdrawn = withdrawn.clone();
            let reader = std::thread::spawn(move || {
                let clock: Arc<dyn DeadlineClock> = reader_clock;
                let budget = ReadRegistrationBudget::new(Arc::clone(&clock), 0, 5);
                if cursor {
                    let fixture_operation_reader_3 = reader_db
                        .declared_reader_memory()
                        .expect("admit the actual fixture reader");

                    reader_db
                        .bounded_cursor_snapshot_registration(
                            clock,
                            5,
                            &budget,
                            &reader_withdrawn,
                            fixture_operation_reader_3.clone(),
                        )
                        .map(|admitted| admitted.is_some())
                } else {
                    let fixture_operation_reader_4 = reader_db
                        .declared_reader_memory()
                        .expect("admit the actual fixture reader");

                    reader_db
                        .bounded_read_snapshot_registration(
                            &budget,
                            &reader_withdrawn,
                            fixture_operation_reader_4.clone(),
                        )
                        .map(|admitted| admitted.is_some())
                }
            });
            assert!(pause.wait_until_reached_blocking());
            if cancelled {
                withdrawn.cancel();
            } else {
                clock.advance_to(6);
            }
            pause.release();
            let admitted = reader.join().expect("reader exits");
            if cancelled {
                assert!(!admitted.expect("cancellation is not an error"));
            } else {
                assert!(matches!(
                    admitted,
                    Err(Error::ReadFailure(failure))
                        if failure.kind()
                            == contextdb_core::read_contract::ReadFailureKind::OwnerLimitExceeded
                ));
            }
            assert!(db.snapshot_registry.state.lock().active.is_empty());
            assert_eq!(
                db.__native_read_transaction_count_for_test(physical.transaction_id)
                    .unwrap(),
                native_before,
                "cancelled or expired registration retains no extra physical root"
            );
        }
    }
}
