use super::*;
use std::sync::mpsc;

fn configured_database() -> Database {
    configure(Database::open_memory())
}

fn configure(database: Database) -> Database {
    for statement in [
        "CREATE TABLE events (id INTEGER PRIMARY KEY)",
        "CREATE EVENT TYPE inserted WHEN INSERT ON events",
        "CREATE SINK callbacks TYPE callback",
        "CREATE ROUTE inserted_to_callbacks EVENT inserted TO callbacks",
    ] {
        database.execute(statement, &HashMap::new()).unwrap();
    }
    database
}

#[test]
fn an_empty_queue_does_not_complete_a_prefix_before_its_metrics_acknowledgment() {
    let database = configured_database();
    let drive = database.sink_dispatch_drive_for_test().unwrap();
    database
        .register_sink("callbacks", None, |_| Ok(()))
        .unwrap();
    database
        .execute("INSERT INTO events VALUES (1)", &HashMap::new())
        .unwrap();
    let prefix = drive.committed_prefix_for_test();
    assert_eq!(prefix.entries_for_test().len(), 1);
    let pause = Arc::new(ApplyPhasePause::new());
    let held_acknowledgment = ApplyPhasePauseGuard::arm_for_test(Arc::clone(&pause));
    *database
        .event_bus
        .dispatch_drive_for_test
        .acknowledgment_pause
        .lock() = Some(pause);
    std::thread::scope(|scope| {
        let held_acknowledgment = held_acknowledgment;
        let (finished, receipt) = mpsc::channel();
        let driving = &drive;
        scope.spawn(move || {
            finished
                .send(driving.drive_through_for_test(prefix))
                .unwrap();
        });
        assert!(held_acknowledgment.wait_until_reached_blocking());
        assert_eq!(database.sink_metrics("callbacks").queued, 0);
        assert_eq!(database.sink_metrics("callbacks").delivered, 0);
        assert!(matches!(receipt.try_recv(), Err(mpsc::TryRecvError::Empty)));
        held_acknowledgment.release();
        let receipt = receipt.recv().unwrap().unwrap();
        assert!(receipt.is_complete_for_test());
        assert_eq!(receipt.processed_lsn, receipt.committed_lsn);
        assert_eq!(receipt.processed_positions, receipt.committed_positions);
        let metrics = database.sink_metrics("callbacks");
        assert_eq!(metrics.delivered, 1);
        assert_eq!(metrics.examined, 1);
        assert_eq!(database.event_bus.metrics.lock()["callbacks"].in_flight, 0);
    });
}

#[test]
fn sink_retries_are_one_explicit_attempt_with_the_same_action_identity() {
    let database = configured_database();
    let drive = database.sink_dispatch_drive_for_test().unwrap();
    let identities = Arc::new(Mutex::new(Vec::new()));
    let delivered_identities = Arc::clone(&identities);
    database
        .register_sink("callbacks", None, move |event| {
            let mut identities = delivered_identities.lock();
            identities.push(event.action_id);
            if identities.len() == 1 {
                Err(SinkError::Transient("retry".to_owned()))
            } else {
                Ok(())
            }
        })
        .unwrap();
    database
        .execute("INSERT INTO events VALUES (1)", &HashMap::new())
        .unwrap();
    let now = Arc::new(AtomicU64::new(700));
    let clock = Arc::clone(&now);
    let _clock = Wallclock::test_clock_guard(move || clock.load(Ordering::SeqCst));
    let receipt = drive
        .drive_through_for_test(drive.committed_prefix_for_test())
        .unwrap();
    assert!(!receipt.is_complete_for_test());
    assert_eq!(receipt.pending_retries.len(), 1);
    let retry = receipt.pending_retries[0].clone();
    assert!(
        !drive
            .drive_retry_for_test(retry.identity.clone())
            .unwrap()
            .is_complete_for_test()
    );
    assert_eq!(identities.lock().len(), 1);
    now.store(retry.retry_at.0, Ordering::SeqCst);
    let receipt = drive.drive_retry_for_test(retry.identity).unwrap();
    assert!(receipt.is_complete_for_test());
    let ids = identities.lock();
    assert_eq!(ids.len(), 2);
    assert_eq!(ids[0], ids[1]);
    let metrics = database.sink_metrics("callbacks");
    assert_eq!(metrics.delivered, 1);
    assert_eq!(metrics.retried, 1);
    assert_eq!(metrics.examined, 2);
}

#[test]
fn a_durable_prefix_includes_cold_actions_and_drains_their_real_acknowledgments() {
    let directory = tempfile::tempdir().unwrap();
    let database = configure(Database::open(directory.path().join("events.db")).unwrap());
    let drive = database.sink_dispatch_drive_for_test().unwrap();
    database
        .register_sink("callbacks", None, |_| Ok(()))
        .unwrap();
    for id in 1..=3 {
        database
            .execute(
                &format!("INSERT INTO events VALUES ({id})"),
                &HashMap::new(),
            )
            .unwrap();
    }
    let prefix = drive.committed_prefix_for_test();
    assert_eq!(prefix.entries_for_test().len(), 3);
    let receipt = drive.drive_through_for_test(prefix).unwrap();
    assert!(receipt.is_complete_for_test(), "{receipt:?}");
    assert_eq!(receipt.processed_positions.len(), 3);
    let metrics = database.sink_metrics("callbacks");
    assert_eq!(metrics.queued, 0);
    assert_eq!(metrics.delivered, 3);
}

#[test]
fn a_prefix_without_a_registered_callback_reports_the_actual_blocked_identity() {
    let database = configured_database();
    let drive = database.sink_dispatch_drive_for_test().unwrap();
    database
        .execute("INSERT INTO events VALUES (1)", &HashMap::new())
        .unwrap();
    let prefix = drive.committed_prefix_for_test();
    let expected = prefix.entries_for_test().to_vec();
    let receipt = drive.drive_through_for_test(prefix).unwrap();
    assert!(!receipt.is_complete_for_test());
    assert_eq!(receipt.blocked_positions, expected);
    assert!(receipt.processed_positions.is_empty());
    assert_eq!(database.sink_metrics("callbacks").delivered, 0);
}

#[test]
fn a_captured_prefix_leaves_later_commits_queued_until_the_drive_is_dropped() {
    let database = configured_database();
    let drive = database.sink_dispatch_drive_for_test().unwrap();
    let (delivered, received) = mpsc::channel();
    database
        .register_sink("callbacks", None, move |event| {
            delivered.send(event.action_id).unwrap();
            Ok(())
        })
        .unwrap();
    database
        .execute("INSERT INTO events VALUES (1)", &HashMap::new())
        .unwrap();
    let prefix = drive.committed_prefix_for_test();
    let first = prefix.entries_for_test()[0].action_id;
    database
        .execute("INSERT INTO events VALUES (2)", &HashMap::new())
        .unwrap();
    let receipt = drive.drive_through_for_test(prefix).unwrap();
    assert!(receipt.is_complete_for_test());
    assert_eq!(receipt.processed_positions.len(), 1);
    assert_eq!(received.recv().unwrap(), first);
    assert_eq!(database.sink_metrics("callbacks").queued, 1);
    assert_eq!(database.sink_metrics("callbacks").delivered, 1);
    let later = drive.committed_prefix_for_test();
    assert_eq!(later.entries_for_test().len(), 1);
    let second = later.entries_for_test()[0].action_id;
    assert_ne!(first, second);
    drop(drive);
    assert_eq!(received.recv().unwrap(), second);
}
