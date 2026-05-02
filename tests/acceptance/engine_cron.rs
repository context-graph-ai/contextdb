use crate::common::run_cli_script;
use contextdb_core::{ContextId, TxId, Value, VersionedRow};
use contextdb_engine::{CronAuditKind, Database};
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use uuid::Uuid;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn txid_value(row: &VersionedRow, column: &str) -> TxId {
    match row.values.get(column) {
        Some(Value::TxId(tx)) => *tx,
        other => panic!("expected {column} to be Value::TxId, got {other:?}"),
    }
}

/// RED — t27_01
#[test]
fn t27_01_schedule_fires_inside_tx_and_commits_callback_writes() {
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE gc_log (id UUID PRIMARY KEY, note TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE SCHEDULE gc_evidence EVERY '200 MILLISECONDS' TX (gc_evidence_cb)",
        &empty(),
    )
    .unwrap();

    db.register_cron_callback("gc_evidence_cb", move |db_handle| {
        // The callback must write through the tx-bound cron handle supplied by the engine.
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
        p.insert("note".into(), Value::Text("fire".into()));
        db_handle.execute("INSERT INTO gc_log (id, note) VALUES ($id, $note)", &p)?;
        Ok(())
    })
    .unwrap();

    for _ in 0..30 {
        if db.scan("gc_log", db.snapshot()).unwrap().len() >= 2 {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let rows = db.scan("gc_log", db.snapshot()).unwrap();
    assert!(
        rows.len() >= 2,
        "background tickler must fire the same schedule periodically, not as a one-shot; rows={rows:?}"
    );
}

/// RED — t27_02
#[test]
fn t27_02_only_due_schedules_dispatch() {
    let db = Arc::new(Database::open_memory());
    db.execute("CREATE TABLE soon_log (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute("CREATE TABLE later_log (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE SCHEDULE soon EVERY '200 MILLISECONDS' TX (soon_cb)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE SCHEDULE later EVERY '60 SECONDS' TX (later_cb)",
        &empty(),
    )
    .unwrap();

    db.register_cron_callback("soon_cb", move |db_handle| {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
        db_handle.execute("INSERT INTO soon_log (id) VALUES ($id)", &p)?;
        Ok(())
    })
    .unwrap();
    db.register_cron_callback("later_cb", move |db_handle| {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
        db_handle.execute("INSERT INTO later_log (id) VALUES ($id)", &p)?;
        Ok(())
    })
    .unwrap();

    for _ in 0..20 {
        if !db.scan("soon_log", db.snapshot()).unwrap().is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    let soon_count = db.scan("soon_log", db.snapshot()).unwrap().len();
    let later_count = db.scan("later_log", db.snapshot()).unwrap().len();
    assert!(
        soon_count >= 1,
        "background tickler should fire the due schedule"
    );
    assert_eq!(later_count, 0, "later schedule must not fire yet");
}

/// RED — t27_03
#[test]
fn t27_03_skip_and_audit_records_missed_ticks() {
    use tempfile::TempDir;
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("skip.redb");
    let db = Arc::new(Database::open(&path).unwrap());
    db.execute("CREATE TABLE log (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE SCHEDULE s EVERY '500 MILLISECONDS' TX (cb) MISSED_TICK_POLICY 'skip-and-audit'",
        &empty(),
    )
    .unwrap();
    db.register_cron_callback("cb", move |db_handle| {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
        db_handle.execute("INSERT INTO log (id) VALUES ($id)", &p)?;
        Ok(())
    })
    .unwrap();

    {
        // Sleep long enough for ~3 missed ticks (1.6s).
        let _pause = db.pause_cron_tickler_for_test();
        std::thread::sleep(Duration::from_millis(1_600));
        let fires = db.cron_run_due_now_for_test().unwrap();
        assert_eq!(
            fires, 1,
            "skip-and-audit must dispatch exactly one fire after a missed window"
        );
        let log_rows = db.scan("log", db.snapshot()).unwrap();
        assert_eq!(
            log_rows.len(),
            1,
            "skip-and-audit must execute only the current fire, not catch up missed callbacks"
        );

        let audit = db.cron_audit_log_for_test();
        let missed = audit
            .iter()
            .filter(|e| matches!(e.kind, CronAuditKind::MissedSkipped))
            .count();
        assert!(
            missed >= 2,
            "expected >=2 MissedSkipped entries; got {missed} in {audit:?}"
        );
    }
    db.close().unwrap();
    drop(db);

    let reopened = Database::open(&path).unwrap();
    let persisted = reopened.cron_audit_log_for_test();
    let persisted_missed = persisted
        .iter()
        .filter(|e| matches!(e.kind, CronAuditKind::MissedSkipped))
        .count();
    assert!(
        persisted_missed >= 2,
        "MissedSkipped audit entries must persist across reopen; got {persisted:?}"
    );
}

/// RED — t27_04
#[test]
fn t27_04_catch_up_within_window_dispatches_all_missed() {
    let db = Arc::new(Database::open_memory());
    db.execute("CREATE TABLE log (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE SCHEDULE s EVERY '500 MILLISECONDS' TX (cb) MISSED_TICK_POLICY 'catch-up' WITHIN 60 SECONDS",
        &empty(),
    ).unwrap();
    db.register_cron_callback("cb", move |db_handle| {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
        db_handle.execute("INSERT INTO log (id) VALUES ($id)", &p)?;
        Ok(())
    })
    .unwrap();

    let _pause = db.pause_cron_tickler_for_test();
    std::thread::sleep(Duration::from_millis(1_600));
    let fires = db.cron_run_due_now_for_test().unwrap();
    assert!(
        fires >= 3,
        "catch-up must dispatch all missed fires; got {fires}"
    );
    let count = db.scan("log", db.snapshot()).unwrap().len();
    assert!(
        count >= 3,
        "callback should have committed >=3 rows; got {count}"
    );

    // Audit shape: exactly one MissedCaughtUp entry whose ticks match.
    let audit = db.cron_audit_log_for_test();
    let caught_up: Vec<u32> = audit
        .iter()
        .filter_map(|e| match &e.kind {
            CronAuditKind::MissedCaughtUp { ticks } => Some(*ticks),
            _ => None,
        })
        .collect();
    assert!(
        !caught_up.is_empty(),
        "expected at least one MissedCaughtUp audit entry; got {audit:?}"
    );
    assert!(
        caught_up.iter().any(|t| *t >= 3),
        "expected MissedCaughtUp with ticks >= 3; got entries {caught_up:?}"
    );

    let capped = Arc::new(Database::open_memory());
    capped
        .execute("CREATE TABLE capped_log (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    capped.execute(
        "CREATE SCHEDULE capped EVERY '500 MILLISECONDS' TX (capped_cb) MISSED_TICK_POLICY 'catch-up' WITHIN 1 SECONDS",
        &empty(),
    ).unwrap();
    capped
        .register_cron_callback("capped_cb", move |db_handle| {
            let mut p = HashMap::new();
            p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
            db_handle.execute("INSERT INTO capped_log (id) VALUES ($id)", &p)?;
            Ok(())
        })
        .unwrap();

    let _pause = capped.pause_cron_tickler_for_test();
    std::thread::sleep(Duration::from_millis(2_600));
    let capped_fires = capped.cron_run_due_now_for_test().unwrap();
    assert!(
        (1..=2).contains(&capped_fires),
        "catch-up WITHIN 1 SECONDS must bound replay to recent missed ticks, not replay the full backlog; got {capped_fires}"
    );
    let capped_count = capped.scan("capped_log", capped.snapshot()).unwrap().len();
    assert!(
        (1..=2).contains(&capped_count),
        "bounded catch-up callback count must stay within the policy window; got {capped_count}"
    );
}

/// REGRESSION GUARD — t27_05
#[test]
fn t27_05_drop_schedule_stops_dispatch() {
    let db = Arc::new(Database::open_memory());
    db.execute("CREATE TABLE log (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE SCHEDULE s EVERY '500 MILLISECONDS' TX (cb)",
        &empty(),
    )
    .unwrap();
    db.register_cron_callback("cb", move |db_handle| {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
        db_handle.execute("INSERT INTO log (id) VALUES ($id)", &p)?;
        Ok(())
    })
    .unwrap();

    db.execute("DROP SCHEDULE s", &empty()).unwrap();
    std::thread::sleep(Duration::from_millis(700));
    let _ = db.cron_run_due_now_for_test().unwrap();
    let rows = db.scan("log", db.snapshot()).unwrap();
    assert_eq!(rows.len(), 0, "dropped schedule must not fire");
}

/// RED — t27_06
#[test]
fn t27_06_callback_runs_inside_tx_with_single_lsn() {
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE a (id UUID PRIMARY KEY, fire_tx TXID NOT NULL)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE b (id UUID PRIMARY KEY, fire_tx TXID NOT NULL)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE SCHEDULE s EVERY '500 MILLISECONDS' TX (cb)",
        &empty(),
    )
    .unwrap();

    let observed = Arc::new(Mutex::new(Vec::<contextdb_core::Lsn>::new()));
    let observed_clone = observed.clone();
    db.register_cron_callback("cb", move |db_handle| {
        let mut p = HashMap::new();
        p.insert("aid".into(), Value::Uuid(Uuid::new_v4()));
        p.insert("bid".into(), Value::Uuid(Uuid::new_v4()));
        // fire_tx is omitted on purpose. Cron callbacks must run inside the
        // active engine tx, and TXID NOT NULL auto-stamp must use that tx.
        db_handle.execute("INSERT INTO a (id) VALUES ($aid)", &p)?;
        db_handle.execute("INSERT INTO b (id) VALUES ($bid)", &p)?;
        observed_clone.lock().unwrap().push(db_handle.current_lsn());
        Ok(())
    })
    .unwrap();

    let rx = db.subscribe();
    let event = rx.recv_timeout(Duration::from_secs(2));
    assert!(event.is_ok(), "commit event from cron fire: {event:?}");
    let event = event.unwrap();
    assert!(event.tables_changed.contains(&"a".to_string()));
    assert!(
        event.tables_changed.contains(&"b".to_string()),
        "both writes must land in the same commit (single LSN)"
    );
    // Strict single-event assertion.
    assert!(
        rx.try_recv().is_err(),
        "exactly one CommitEvent expected for a single cron fire"
    );
    // Strict single-LSN: the LSN captured INSIDE the callback must equal the event LSN.
    let captured = observed.lock().unwrap();
    assert_eq!(captured.len(), 1, "callback ran exactly once");
    assert_eq!(
        captured[0], event.lsn,
        "callback's captured LSN must equal the event's LSN"
    );

    let a_rows = db.scan("a", db.snapshot()).unwrap();
    let b_rows = db.scan("b", db.snapshot()).unwrap();
    assert_eq!(a_rows.len(), 1, "cron should insert exactly one a row");
    assert_eq!(b_rows.len(), 1, "cron should insert exactly one b row");
    let a_fire_tx = txid_value(&a_rows[0], "fire_tx");
    let b_fire_tx = txid_value(&b_rows[0], "fire_tx");
    assert_eq!(
        a_rows[0].created_tx, b_rows[0].created_tx,
        "both callback writes must share the same engine tx"
    );
    assert_eq!(
        a_fire_tx, a_rows[0].created_tx,
        "a.fire_tx must be auto-stamped with the active cron tx"
    );
    assert_eq!(
        b_fire_tx, b_rows[0].created_tx,
        "b.fire_tx must be auto-stamped with the active cron tx"
    );
    assert_eq!(
        a_fire_tx, b_fire_tx,
        "TXID auto-stamp must be identical across tables in one cron fire"
    );
}

/// RED — t27_07: schedule + audit log persist across engine restart.
#[test]
fn t27_07_schedule_persists_across_engine_restart() {
    use tempfile::TempDir;
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("cron.redb");
    let script = "\
CREATE TABLE log (id UUID PRIMARY KEY)
CREATE SCHEDULE persistent EVERY '500 MILLISECONDS' TX (cb) MISSED_TICK_POLICY 'catch-up' WITHIN 60 SECONDS
";
    let out = run_cli_script(&path, &[], script);
    assert!(
        out.status.success(),
        "child process should persist schedule; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Reopen.
    let db = Arc::new(Database::open(&path).unwrap());
    db.register_cron_callback("cb", move |db_handle| {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
        db_handle.execute("INSERT INTO log (id) VALUES ($id)", &p)?;
        Ok(())
    })
    .unwrap();

    for _ in 0..20 {
        if !db.scan("log", db.snapshot()).unwrap().is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    let rows = db.scan("log", db.snapshot()).unwrap();
    assert!(
        !rows.is_empty(),
        "background tickler must fire the persisted schedule after reopen"
    );

    let audit = db.cron_audit_log_for_test();
    assert!(
        audit.iter().any(|e| {
            e.schedule_name == "persistent"
                && matches!(
                    e.kind,
                    CronAuditKind::Fired | CronAuditKind::MissedCaughtUp { .. }
                )
        }),
        "schedule fire must be recorded before audit persistence check; got {audit:?}"
    );
    db.close().unwrap();
    drop(db);

    let reopened = Database::open(&path).unwrap();
    let persisted_audit = reopened.cron_audit_log_for_test();
    assert!(
        persisted_audit.iter().any(|e| {
            e.schedule_name == "persistent"
                && matches!(
                    e.kind,
                    CronAuditKind::Fired | CronAuditKind::MissedCaughtUp { .. }
                )
        }),
        "cron audit entries must persist across reopen; got {persisted_audit:?}"
    );
}

/// RED — t27_08: fail-loud policy returns typed error on missed window.
#[test]
fn t27_08_fail_loud_missed_tick_returns_error() {
    use contextdb_core::Error;
    use tempfile::TempDir;
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("fail_loud.redb");
    let db = Arc::new(Database::open(&path).unwrap());
    db.execute("CREATE TABLE log (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE SCHEDULE s EVERY '500 MILLISECONDS' TX (cb) MISSED_TICK_POLICY 'fail-loud'",
        &empty(),
    )
    .unwrap();
    db.register_cron_callback("cb", move |db_handle| {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
        db_handle.execute("INSERT INTO log (id) VALUES ($id)", &p)?;
        Ok(())
    })
    .unwrap();

    {
        let _pause = db.pause_cron_tickler_for_test();
        std::thread::sleep(Duration::from_millis(1_400));
        let result = db.cron_run_due_now_for_test();
        let err = result.err();
        assert!(err.is_some(), "fail-loud must return Err on missed ticks");
        let err = err.unwrap();
        match err {
            Error::MissedTicksExceeded { ticks, ref policy } => {
                assert!(ticks >= 2, "expected ticks >= 2; got {ticks}");
                assert_eq!(policy, "fail-loud");
            }
            other => panic!("expected MissedTicksExceeded, got: {other:?}"),
        }
        let audit = db.cron_audit_log_for_test();
        assert!(
            audit
                .iter()
                .any(|e| { e.schedule_name == "s" && matches!(e.kind, CronAuditKind::Failed(_)) }),
            "fail-loud miss must be recorded in audit before persistence check; got {audit:?}"
        );
        let log_rows = db.scan("log", db.snapshot()).unwrap();
        assert_eq!(
            log_rows.len(),
            0,
            "fail-loud missed ticks must not fire the callback before returning the typed error"
        );
    }
    db.close().unwrap();
    drop(db);

    let reopened = Database::open(&path).unwrap();
    let persisted = reopened.cron_audit_log_for_test();
    assert!(
        persisted
            .iter()
            .any(|e| { e.schedule_name == "s" && matches!(e.kind, CronAuditKind::Failed(_)) }),
        "fail-loud failure audit entries must persist across reopen; got {persisted:?}"
    );
}

/// RED — t27_09: tickler shuts down on Database drop.
#[test]
fn t27_09_tickler_shuts_down_on_drop() {
    use std::sync::atomic::AtomicU64;
    let counter = Arc::new(AtomicU64::new(0));
    {
        let db = Arc::new(Database::open_memory());
        db.execute(
            "CREATE SCHEDULE s EVERY '200 MILLISECONDS' TX (cb)",
            &empty(),
        )
        .unwrap();
        let counter_cb = counter.clone();
        db.register_cron_callback("cb", move |_| {
            counter_cb.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
        for _ in 0..20 {
            if counter.load(std::sync::atomic::Ordering::SeqCst) > 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        assert!(
            counter.load(std::sync::atomic::Ordering::SeqCst) > 0,
            "background tickler must fire before the drop-side shutdown assertion"
        );
        // db drops here.
    }
    let snap_before = counter.load(std::sync::atomic::Ordering::SeqCst);
    std::thread::sleep(Duration::from_millis(1_000));
    let snap_after = counter.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        snap_before, snap_after,
        "tickler must stop after Database is dropped"
    );
}

/// REGRESSION GUARD — t27_10: DROP SCHEDULE persists across engine restart.
#[test]
fn t27_10_drop_schedule_persists_across_reopen() {
    use tempfile::TempDir;
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("dropx.redb");
    let script = "\
CREATE TABLE log (id UUID PRIMARY KEY)
CREATE SCHEDULE doomed EVERY '500 MILLISECONDS' TX (cb) MISSED_TICK_POLICY 'skip-and-audit'
DROP SCHEDULE doomed
";
    let out = run_cli_script(&path, &[], script);
    assert!(
        out.status.success(),
        "child process should persist schedule drop; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Reopen and re-register the callback under the same name.
    let db = Arc::new(Database::open(&path).unwrap());
    db.register_cron_callback("cb", move |db_handle| {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
        db_handle.execute("INSERT INTO log (id) VALUES ($id)", &p)?;
        Ok(())
    })
    .unwrap();

    std::thread::sleep(Duration::from_millis(800));

    let rows = db.scan("log", db.snapshot()).unwrap();
    assert_eq!(
        rows.len(),
        0,
        "no rows must be inserted by the dropped schedule"
    );

    let audit = db.cron_audit_log_for_test();
    let from_doomed: Vec<_> = audit
        .iter()
        .filter(|e| e.schedule_name == "doomed")
        .filter(|e| {
            matches!(
                e.kind,
                CronAuditKind::MissedSkipped
                    | CronAuditKind::MissedCaughtUp { .. }
                    | CronAuditKind::Fired
            )
        })
        .collect();
    assert!(
        from_doomed.is_empty(),
        "no audit entries for the dropped schedule must appear after reopen; got {from_doomed:?}"
    );
}

/// RED — t27_11: a persisted schedule whose callback is not registered audits the miss.
#[test]
fn t27_11_missing_registered_callback_records_failed_audit_without_panic() {
    use tempfile::TempDir;
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("missing_cb.redb");
    let db = Arc::new(Database::open(&path).unwrap());
    db.execute(
        "CREATE SCHEDULE orphan EVERY '500 MILLISECONDS' TX (missing_cb) MISSED_TICK_POLICY 'skip-and-audit'",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE TABLE healthy (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE SCHEDULE healthy EVERY '500 MILLISECONDS' TX (healthy_cb) MISSED_TICK_POLICY 'skip-and-audit'",
        &empty(),
    )
    .unwrap();
    db.register_cron_callback("healthy_cb", move |db_handle| {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
        db_handle.execute("INSERT INTO healthy (id) VALUES ($id)", &p)?;
        Ok(())
    })
    .unwrap();

    {
        let _pause = db.pause_cron_tickler_for_test();
        std::thread::sleep(Duration::from_millis(700));
        let fires = db
            .cron_run_due_now_for_test()
            .expect("missing callback should be audited without panicking or poisoning cron");
        assert_eq!(
            fires, 1,
            "healthy schedule must still fire after missing callback is audited"
        );
        let healthy_rows = db.scan("healthy", db.snapshot()).unwrap();
        assert_eq!(
            healthy_rows.len(),
            1,
            "missing callback must not poison the same cron run"
        );
        let audit = db.cron_audit_log_for_test();
        assert!(
            audit.iter().any(|e| {
                e.schedule_name == "orphan"
                    && matches!(
                        &e.kind,
                        CronAuditKind::Failed(msg)
                            if msg.contains("missing_cb") && msg.contains("callback")
                    )
            }),
            "missing callback must record a durable Failed audit entry; got {audit:?}"
        );
    }
    db.close().unwrap();
    drop(db);

    let reopened = Database::open(&path).unwrap();
    let persisted = reopened.cron_audit_log_for_test();
    assert!(
        persisted.iter().any(|e| {
            e.schedule_name == "orphan"
                && matches!(
                    &e.kind,
                    CronAuditKind::Failed(msg)
                        if msg.contains("missing_cb") && msg.contains("callback")
                )
        }),
        "missing-callback Failed audit entry must persist across reopen; got {persisted:?}"
    );
}

/// RED — t27_12: a failing registered callback rolls back partial writes and audits.
#[test]
fn t27_12_registered_callback_error_rolls_back_partial_writes_and_audits() {
    use tempfile::TempDir;
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("callback_failure.redb");
    let db = Arc::new(Database::open(&path).unwrap());
    db.execute("CREATE TABLE log (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE SCHEDULE flaky EVERY '500 MILLISECONDS' TX (flaky_cb) MISSED_TICK_POLICY 'skip-and-audit'",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE TABLE healthy (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE SCHEDULE healthy EVERY '500 MILLISECONDS' TX (healthy_cb) MISSED_TICK_POLICY 'skip-and-audit'",
        &empty(),
    )
    .unwrap();
    db.register_cron_callback("flaky_cb", move |db_handle| {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
        db_handle.execute("INSERT INTO log (id) VALUES ($id)", &p)?;
        Err(contextdb_core::Error::Other("fail_after_write".into()))
    })
    .unwrap();
    db.register_cron_callback("healthy_cb", move |db_handle| {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
        db_handle.execute("INSERT INTO healthy (id) VALUES ($id)", &p)?;
        Ok(())
    })
    .unwrap();

    {
        let _pause = db.pause_cron_tickler_for_test();
        std::thread::sleep(Duration::from_millis(700));
        let fires = db
            .cron_run_due_now_for_test()
            .expect("registered callback error should be recorded in audit without poisoning cron");
        assert_eq!(
            fires, 1,
            "healthy schedule must still fire after failed callback is rolled back and audited"
        );
        let rows = db.scan("log", db.snapshot()).unwrap();
        assert!(
            rows.is_empty(),
            "callback's partial write must roll back on callback error; rows={rows:?}"
        );
        let healthy_rows = db.scan("healthy", db.snapshot()).unwrap();
        assert_eq!(
            healthy_rows.len(),
            1,
            "callback failure must not poison the same cron run"
        );
        let audit = db.cron_audit_log_for_test();
        assert!(
            audit.iter().any(|e| {
                e.schedule_name == "flaky"
                    && matches!(
                        &e.kind,
                        CronAuditKind::Failed(msg) if msg.contains("fail_after_write")
                    )
            }),
            "callback failure must record a durable Failed audit entry; got {audit:?}"
        );
    }
    db.close().unwrap();
    drop(db);

    let reopened = Database::open(&path).unwrap();
    let rows = reopened.scan("log", reopened.snapshot()).unwrap();
    assert!(
        rows.is_empty(),
        "rolled-back callback write must not reappear after reopen; rows={rows:?}"
    );
    let persisted = reopened.cron_audit_log_for_test();
    assert!(
        persisted.iter().any(|e| {
            e.schedule_name == "flaky"
                && matches!(
                    &e.kind,
                    CronAuditKind::Failed(msg) if msg.contains("fail_after_write")
                )
        }),
        "callback failure audit entry must persist across reopen; got {persisted:?}"
    );
}

/// REGRESSION GUARD — schedule DDL is engine-admin only, not scoped-handle mutable.
#[test]
fn t27_13_scoped_handle_cannot_create_or_drop_schedule() {
    use tempfile::TempDir;
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("cron_scoped_ddl.redb");
    {
        let admin = Database::open(&path).unwrap();
        admin
            .execute("CREATE SCHEDULE s EVERY '1 SECOND' TX (cb)", &empty())
            .unwrap();
        admin.close().unwrap();
    }

    let scoped =
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(Uuid::from_u128(1))]))
            .unwrap();
    let create = scoped.execute("CREATE SCHEDULE scoped EVERY '1 SECOND' TX (cb)", &empty());
    assert!(
        create.is_err(),
        "context-scoped handle must not create engine-wide schedules"
    );
    let drop_schedule = scoped.execute("DROP SCHEDULE s", &empty());
    assert!(
        drop_schedule.is_err(),
        "context-scoped handle must not drop engine-wide schedules"
    );
    let register = scoped.register_cron_callback("cb", |_| Ok(()));
    assert!(
        register.is_err(),
        "context-scoped handle must not bind engine-wide callbacks"
    );
    scoped.close().unwrap();
}

/// REGRESSION GUARD — callback transaction cannot re-enter DDL or tx control.
#[test]
fn t27_14_callback_rejects_nested_ddl_and_transaction_control() {
    let db = Arc::new(Database::open_memory());
    let captured_db = Arc::new(Database::open_memory());
    captured_db
        .execute("CREATE TABLE captured (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    captured_db.execute("BEGIN", &empty()).unwrap();
    let mut cached_params = HashMap::new();
    cached_params.insert("id".into(), Value::Uuid(Uuid::from_u128(0xCAFE)));
    captured_db
        .execute("INSERT INTO captured (id) VALUES ($id)", &cached_params)
        .unwrap();
    db.execute(
        "CREATE SCHEDULE s EVERY '500 MILLISECONDS' TX (cb)",
        &empty(),
    )
    .unwrap();

    let captured_outer = captured_db.clone();
    db.register_cron_callback("cb", move |db_handle| {
        let ddl = db_handle.execute("CREATE TABLE illegal (id UUID PRIMARY KEY)", &empty());
        assert!(
            ddl.is_err(),
            "cron callback must reject nested DDL instead of deadlocking"
        );
        let commit = db_handle.execute("COMMIT", &empty());
        assert!(
            commit.is_err(),
            "cron callback must reject nested transaction control instead of deadlocking"
        );
        let explicit_tx = db_handle.execute_in_tx(
            TxId(999),
            "CREATE TABLE also_illegal (id UUID PRIMARY KEY)",
            &empty(),
        );
        assert!(
            explicit_tx.is_err(),
            "cron callback must reject execute_in_tx instead of re-entering commit locks"
        );
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
        let captured_autocommit =
            captured_outer.execute("INSERT INTO captured (id) VALUES ($id)", &p);
        assert!(
            captured_autocommit.is_err(),
            "cron callback must reject autocommit writes through captured handles"
        );
        let mut direct_values = HashMap::new();
        direct_values.insert("id".into(), Value::Uuid(Uuid::new_v4()));
        let captured_direct = captured_outer.insert_row(TxId(1), "captured", direct_values);
        assert!(
            captured_direct.is_err(),
            "cron callback must reject direct tx-bound writes through captured handles"
        );
        Ok(())
    })
    .unwrap();

    let _pause = db.pause_cron_tickler_for_test();
    std::thread::sleep(Duration::from_millis(700));
    let fires = db
        .cron_run_due_now_for_test()
        .expect("nested DDL/tx-control rejection must not poison cron");
    assert_eq!(fires, 1);
}

/// REGRESSION GUARD — scoped opens load cron state but do not run engine-wide schedules.
#[test]
fn t27_15_scoped_open_does_not_start_persisted_tickler() {
    use tempfile::TempDir;
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("scoped_open_tickler.redb");
    {
        let admin = Database::open(&path).unwrap();
        admin
            .execute(
                "CREATE SCHEDULE orphan EVERY '200 MILLISECONDS' TX (missing_cb)",
                &empty(),
            )
            .unwrap();
        admin.close().unwrap();
    }

    {
        let scoped = Database::open_with_contexts(
            &path,
            BTreeSet::from([ContextId::new(Uuid::from_u128(1))]),
        )
        .unwrap();
        std::thread::sleep(Duration::from_millis(500));
        scoped.close().unwrap();
    }

    let admin = Database::open(&path).unwrap();
    let audit = admin.cron_audit_log_for_test();
    assert!(
        audit.is_empty(),
        "scoped open must not run persisted schedules or write missing-callback audit entries; got {audit:?}"
    );
    admin.close().unwrap();
}

/// REGRESSION GUARD — cron tx binding is confined to the callback owner thread.
#[test]
fn t27_16_callback_rejects_cross_thread_handle_use() {
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE cron_items (id UUID PRIMARY KEY, note TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE SCHEDULE s EVERY '100 MILLISECONDS' TX (cb)",
        &empty(),
    )
    .unwrap();
    let _pause = db.pause_cron_tickler_for_test();
    let read_tx = db.begin();

    let captured_db = db.clone();
    db.register_cron_callback("cb", move |db_handle| {
        std::thread::scope(|scope| {
            let supplied_join = scope.spawn(|| {
                let mut p = HashMap::new();
                p.insert("id".into(), Value::Uuid(Uuid::from_u128(0xA)));
                p.insert("note".into(), Value::Text("supplied-thread".into()));
                db_handle.execute("INSERT INTO cron_items (id, note) VALUES ($id, $note)", &p)
            });

            let captured = captured_db.clone();
            let captured_join = scope.spawn(move || {
                let mut p = HashMap::new();
                p.insert("id".into(), Value::Uuid(Uuid::from_u128(0xB)));
                p.insert("note".into(), Value::Text("captured-thread".into()));
                captured.execute("INSERT INTO cron_items (id, note) VALUES ($id, $note)", &p)
            });

            let reader = captured_db.clone();
            let read_join = scope
                .spawn(move || reader.execute_in_tx(read_tx, "SELECT * FROM cron_items", &empty()));

            let supplied = supplied_join.join().expect("supplied thread panicked");
            let captured = captured_join.join().expect("captured thread panicked");
            let read = read_join.join().expect("read thread panicked");
            assert!(
                supplied.is_err(),
                "cron callback must not expose its tx-bound handle to other threads"
            );
            assert!(
                captured.is_err(),
                "cron callback must not let same-engine captured handles write from other threads"
            );
            assert!(
                read.is_ok(),
                "cross-thread tx-bound reads must remain allowed while cron writes are isolated"
            );
        });

        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::from_u128(0xC)));
        p.insert("note".into(), Value::Text("owner-thread".into()));
        db_handle.execute("INSERT INTO cron_items (id, note) VALUES ($id, $note)", &p)?;
        Ok(())
    })
    .unwrap();

    std::thread::sleep(Duration::from_millis(150));
    let fires = db
        .cron_run_due_now_for_test()
        .expect("cross-thread rejection must not poison cron");
    assert_eq!(fires, 1);

    let rows = db.scan("cron_items", db.snapshot()).unwrap();
    assert_eq!(
        rows.len(),
        1,
        "only the owner-thread callback write should commit"
    );
    assert_eq!(
        rows[0].values.get("note"),
        Some(&Value::Text("owner-thread".into()))
    );
    db.rollback(read_tx).unwrap();
}
