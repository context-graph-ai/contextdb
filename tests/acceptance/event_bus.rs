use crate::common::run_cli_script;
use contextdb_core::Value;
use contextdb_engine::{Database, SinkError, SinkEvent};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use uuid::Uuid;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn declare_inv_schema(db: &Database) {
    db.execute(
        "CREATE TABLE invalidations (id UUID PRIMARY KEY, severity TEXT, reason TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE EVENT TYPE inv_match WHEN INSERT ON invalidations",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE SINK slack TYPE callback", &empty())
        .unwrap();
    db.execute(
        "CREATE ROUTE inv_to_slack EVENT inv_match TO slack",
        &empty(),
    )
    .unwrap();
}

fn insert_inv(db: &Database, severity: &str, reason: &str) {
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
    p.insert("sev".into(), Value::Text(severity.into()));
    p.insert("rsn".into(), Value::Text(reason.into()));
    db.execute(
        "INSERT INTO invalidations (id, severity, reason) VALUES ($id, $sev, $rsn)",
        &p,
    )
    .unwrap();
}

/// RED — t5_01
#[test]
fn t5_01_route_invokes_sink_with_row_values() {
    let db = Database::open_memory();
    declare_inv_schema(&db);
    db.execute(
        "CREATE TABLE unrelated_invalidations (id UUID PRIMARY KEY, severity TEXT, reason TEXT)",
        &empty(),
    )
    .unwrap();
    let sink_log: Arc<Mutex<Vec<SinkEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let sink_log_cb = sink_log.clone();
    db.register_sink("slack", None, move |evt| {
        sink_log_cb.lock().unwrap().push(evt.clone());
        Ok(())
    })
    .unwrap();

    let mut other = HashMap::new();
    other.insert("id".into(), Value::Uuid(Uuid::new_v4()));
    other.insert("sev".into(), Value::Text("warning".into()));
    other.insert("rsn".into(), Value::Text("wrong-table".into()));
    db.execute(
        "INSERT INTO unrelated_invalidations (id, severity, reason) VALUES ($id, $sev, $rsn)",
        &other,
    )
    .unwrap();
    insert_inv(&db, "warning", "stale-basis");

    // Allow background dispatch to complete.
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && sink_log.lock().unwrap().is_empty() {
        std::thread::sleep(Duration::from_millis(20));
    }
    std::thread::sleep(Duration::from_millis(100));
    let log = sink_log.lock().unwrap();
    assert_eq!(
        log.len(),
        1,
        "sink must be invoked exactly once for the declared table, not unrelated INSERTs"
    );
    assert_eq!(log[0].event_type, "inv_match");
    assert_eq!(log[0].table, "invalidations");
    assert_eq!(log[0].severity, "warning");
    assert_eq!(
        log[0].row_values.get("severity"),
        Some(&Value::Text("warning".into()))
    );
    assert_eq!(
        log[0].row_values.get("reason"),
        Some(&Value::Text("stale-basis".into()))
    );
}

/// RED — t5_02
#[test]
fn t5_02_severity_filter_respected() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE invalidations (id UUID PRIMARY KEY, severity TEXT, reason TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE EVENT TYPE inv_match WHEN INSERT ON invalidations",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE SINK slack TYPE callback", &empty())
        .unwrap();
    db.execute(
        "CREATE ROUTE inv_to_slack EVENT inv_match TO slack WHERE severity IN ('warning', 'critical', 'fatal')",
        &empty(),
    ).unwrap();

    let sink_log: Arc<Mutex<Vec<SinkEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let sink_log_cb = sink_log.clone();
    db.register_sink("slack", None, move |e| {
        sink_log_cb.lock().unwrap().push(e.clone());
        Ok(())
    })
    .unwrap();

    insert_inv(&db, "info", "low");
    insert_inv(&db, "warning", "high");

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && sink_log.lock().unwrap().is_empty() {
        std::thread::sleep(Duration::from_millis(20));
    }
    std::thread::sleep(Duration::from_millis(100));
    let log = sink_log.lock().unwrap();
    assert_eq!(
        log.len(),
        1,
        "only warning row should match severity filter"
    );
    assert_eq!(
        log[0].row_values.get("severity"),
        Some(&Value::Text("warning".into()))
    );
}

/// RED — t5_03
#[test]
fn t5_03_sink_isolation_slow_a_does_not_block_b() {
    // Synchronization-based discriminator (no wall-clock budget). Sink A
    // blocks on a Mutex held by the test thread, returning ONLY after a
    // rendezvous Barrier(2) trips. Sink B also waits on the same Barrier.
    // If dispatch is parallel, both sinks arrive at the Barrier and B's
    // sentinel channel resolves. If dispatch is serial (A first), B is
    // queued behind A and the Barrier never trips. A control sentinel
    // (`b_rx.recv_timeout(Duration::from_secs(5))`) converts the otherwise-
    // hanging deadlock into an assertion-equivalent panic with a clear
    // message — satisfying the verification-gate "tests fail on assertion,
    // not deadlock-killed-by-harness" condition.
    use std::sync::Barrier;
    use std::sync::mpsc;

    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE invalidations (id UUID PRIMARY KEY, severity TEXT, reason TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE EVENT TYPE inv_match WHEN INSERT ON invalidations",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE SINK slow_a TYPE callback", &empty())
        .unwrap();
    db.execute("CREATE SINK fast_b TYPE callback", &empty())
        .unwrap();
    db.execute("CREATE ROUTE r_a EVENT inv_match TO slow_a", &empty())
        .unwrap();
    db.execute("CREATE ROUTE r_b EVENT inv_match TO fast_b", &empty())
        .unwrap();

    let rendezvous = Arc::new(Barrier::new(2));
    let hold_mutex = Arc::new(Mutex::new(()));
    let (b_tx, b_rx) = mpsc::sync_channel::<()>(1);

    let r_a = rendezvous.clone();
    let h_a = hold_mutex.clone();
    db.register_sink("slow_a", None, move |_| {
        r_a.wait();
        let _g = h_a.lock().unwrap();
        Ok(())
    })
    .unwrap();

    let r_b = rendezvous.clone();
    db.register_sink("fast_b", None, move |_| {
        r_b.wait();
        b_tx.send(()).unwrap();
        Ok(())
    })
    .unwrap();

    let guard = hold_mutex.lock().unwrap();
    insert_inv(&db, "warning", "x");

    // Bounded sentinel: recv_timeout converts a serial-dispatch deadlock into
    // a clear assertion failure, not a harness-killed test.
    assert!(
        b_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
        "sink B did not complete within 5s — dispatcher serialized B behind A \
         (parallel-dispatch isolation broken)"
    );

    drop(guard);
}

/// RED — t5_04
#[test]
fn t5_04_sink_failure_does_not_block_commit() {
    // Synchronization-based discriminator (no wall-clock budget). The sink
    // holds a Mutex controlled by the test and returns Permanent on every
    // call. The test acquires the mutex BEFORE inserting; if commit is
    // blocked on sink dispatch, INSERT hangs.
    //
    // Sentinel: a separate `commit_done_tx` channel is signaled inside the
    // test thread immediately after `insert_inv` returns. The main thread
    // reads this channel via `recv_timeout(Duration::from_secs(5))` — a
    // timeout indicates the engine is blocked on the sink, and we panic
    // with a clear assertion message rather than letting the harness kill
    // the test.
    use std::sync::mpsc;

    let db = Arc::new(Database::open_memory());
    declare_inv_schema(&db);

    let sink_gate = Arc::new(Mutex::new(()));
    let sink_gate_cb = sink_gate.clone();
    db.register_sink("slack", None, move |_| {
        let _g = sink_gate_cb.lock().unwrap();
        Err(SinkError::Permanent("nope".into()))
    })
    .unwrap();

    let gate_guard = sink_gate.lock().unwrap();

    let (commit_done_tx, commit_done_rx) = mpsc::sync_channel::<()>(1);
    let writer_db = db.clone();
    let writer = std::thread::spawn(move || {
        insert_inv(&writer_db, "warning", "x");
        commit_done_tx.send(()).unwrap();
    });

    // Engine commit must return well before the gate is released.
    assert!(
        commit_done_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
        "engine commit did not return within 5s while sink was blocked — \
         commit path is waiting on sink delivery (engine-blocked-on-sink bug)"
    );

    drop(gate_guard);
    writer.join().unwrap();

    // After releasing the gate, dispatch settles and metrics record the failure.
    let metrics_deadline = Instant::now() + Duration::from_secs(5);
    let mut saw_permanent_failure = false;
    while Instant::now() <= metrics_deadline {
        let m = db.sink_metrics_for_test("slack");
        if m.permanent_failures >= 1 {
            assert_eq!(
                m.permanent_failures, 1,
                "exactly one permanent failure recorded"
            );
            saw_permanent_failure = true;
            break;
        }
        std::thread::yield_now();
    }
    assert!(
        saw_permanent_failure,
        "sink metrics did not record permanent_failures within 5s"
    );
}

/// RED — t5_05
#[test]
fn t5_05_transient_failure_retries_and_succeeds() {
    let db = Database::open_memory();
    declare_inv_schema(&db);
    let attempts = Arc::new(AtomicU64::new(0));
    let attempts_cb = attempts.clone();
    db.register_sink("slack", None, move |_| {
        let n = attempts_cb.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            Err(SinkError::Transient("first try".into()))
        } else {
            Ok(())
        }
    })
    .unwrap();

    insert_inv(&db, "warning", "retry-me");

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && attempts.load(Ordering::SeqCst) < 2 {
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "retry path must invoke the sink exactly twice (transient then success); got {}",
        attempts.load(Ordering::SeqCst)
    );
    let metrics = db.sink_metrics_for_test("slack");
    assert_eq!(metrics.retried, 1, "exactly one retry");
    assert_eq!(metrics.delivered, 1);

    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("transient-restart.redb");
    let db = Database::open(&path).unwrap();
    declare_inv_schema(&db);
    let durable_attempts = Arc::new(AtomicU64::new(0));
    let durable_attempts_cb = durable_attempts.clone();
    db.register_sink("slack", None, move |_| {
        durable_attempts_cb.fetch_add(1, Ordering::SeqCst);
        Err(SinkError::Transient("sink still offline".into()))
    })
    .unwrap();

    let durable_id = Uuid::from_u128(55);
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(durable_id));
    p.insert("sev".into(), Value::Text("warning".into()));
    p.insert("rsn".into(), Value::Text("retry-after-reopen".into()));
    db.execute(
        "INSERT INTO invalidations (id, severity, reason) VALUES ($id, $sev, $rsn)",
        &p,
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && durable_attempts.load(Ordering::SeqCst) == 0 {
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(
        durable_attempts.load(Ordering::SeqCst) >= 1,
        "transient sink must attempt delivery before simulated restart"
    );
    db.execute(
        "DELETE FROM invalidations WHERE id = $id",
        &HashMap::from([("id".into(), Value::Uuid(durable_id))]),
    )
    .unwrap();
    db.close().unwrap();
    drop(db);

    let reopened = Database::open(&path).unwrap();
    assert_eq!(
        reopened
            .scan("invalidations", reopened.snapshot())
            .unwrap()
            .len(),
        0,
        "transient retry replay must come from the durable queue, not source-table scan"
    );
    let replayed: Arc<Mutex<Vec<SinkEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let replayed_cb = replayed.clone();
    reopened
        .register_sink("slack", None, move |event| {
            replayed_cb.lock().unwrap().push(event.clone());
            Ok(())
        })
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && replayed.lock().unwrap().is_empty() {
        std::thread::sleep(Duration::from_millis(50));
    }
    let replayed_events = replayed.lock().unwrap();
    assert_eq!(
        replayed_events.len(),
        1,
        "transient-failed queue entry must survive restart and replay exactly once"
    );
    assert_eq!(
        replayed_events[0].row_values.get("reason"),
        Some(&Value::Text("retry-after-reopen".into()))
    );
}

/// RED — t5_06
#[test]
fn t5_06_durable_queue_survives_restart() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("bus.redb");
    let script = "\
CREATE TABLE invalidations (id UUID PRIMARY KEY, severity TEXT, reason TEXT)
CREATE EVENT TYPE inv_match WHEN INSERT ON invalidations
CREATE SINK slack TYPE callback
CREATE SINK audit TYPE callback
CREATE ROUTE inv_to_slack EVENT inv_match TO slack
CREATE ROUTE inv_to_audit EVENT inv_match TO audit
INSERT INTO invalidations (id, severity, reason) VALUES ('00000000-0000-0000-0000-000000000001', 'warning', 'queue-me')
DELETE FROM invalidations WHERE id = '00000000-0000-0000-0000-000000000001'
";
    let out = run_cli_script(&path, &[], script);
    assert!(
        out.status.success(),
        "child process should seed queued event; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Reopen and register a succeeding sink.
    let db = Database::open(&path).unwrap();
    assert_eq!(
        db.scan("invalidations", db.snapshot()).unwrap().len(),
        0,
        "source row is deleted before reopen; delivery must come from durable sink queue, not table replay"
    );
    let received: Arc<Mutex<Vec<SinkEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let received_cb = received.clone();
    db.register_sink("slack", None, move |event| {
        received_cb.lock().unwrap().push(event.clone());
        Ok(())
    })
    .unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && received.lock().unwrap().is_empty() {
        std::thread::sleep(Duration::from_millis(50));
    }
    let slack_events = received.lock().unwrap();
    assert_eq!(
        slack_events.len(),
        1,
        "previously queued event must be delivered exactly once after restart + new sink registration"
    );
    assert_eq!(slack_events[0].event_type, "inv_match");
    assert_eq!(slack_events[0].table, "invalidations");
    assert_eq!(
        slack_events[0].row_values.get("severity"),
        Some(&Value::Text("warning".into()))
    );
    assert_eq!(
        slack_events[0].row_values.get("reason"),
        Some(&Value::Text("queue-me".into()))
    );
    drop(slack_events);
    db.close().unwrap();
    drop(db);

    let reopened = Database::open(&path).unwrap();
    let audit_events: Arc<Mutex<Vec<SinkEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let audit_cb = audit_events.clone();
    reopened
        .register_sink("audit", None, move |event| {
            audit_cb.lock().unwrap().push(event.clone());
            Ok(())
        })
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && audit_events.lock().unwrap().is_empty() {
        std::thread::sleep(Duration::from_millis(50));
    }
    let audit = audit_events.lock().unwrap();
    assert_eq!(
        audit.len(),
        1,
        "acking slack must not drain audit's independent per-sink durable queue"
    );
    assert_eq!(audit[0].event_type, "inv_match");
    assert_eq!(
        audit[0].row_values.get("reason"),
        Some(&Value::Text("queue-me".into())),
        "durable queue must preserve row payload, not just a replay token"
    );
    drop(audit);
    reopened.close().unwrap();
    drop(reopened);

    let reopened = Database::open(&path).unwrap();
    let replay_count = Arc::new(AtomicU64::new(0));
    let replay_cb = replay_count.clone();
    reopened
        .register_sink("slack", None, move |_| {
            replay_cb.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        replay_count.load(Ordering::SeqCst),
        0,
        "delivered durable queue entry must be acked/drained, not replayed on later sink registration"
    );
}

/// RED — t5_07
#[test]
fn t5_07_drop_route_stops_dispatch() {
    let db = Database::open_memory();
    declare_inv_schema(&db);
    let count = Arc::new(AtomicU64::new(0));
    let count_cb = count.clone();
    db.register_sink("slack", None, move |_| {
        count_cb.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .unwrap();

    insert_inv(&db, "warning", "before");
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && count.load(Ordering::SeqCst) == 0 {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(count.load(Ordering::SeqCst), 1);

    db.execute("DROP ROUTE inv_to_slack", &empty()).unwrap();
    insert_inv(&db, "warning", "after");
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "DROP ROUTE must stop further dispatches"
    );
}

/// REGRESSION GUARD — t5_08
#[test]
fn t5_08_subscribe_primitive_still_works() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    let rx = db.subscribe();
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(1)));
    db.execute("INSERT INTO t (id) VALUES ($id)", &p).unwrap();
    let event = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("CommitEvent must arrive");
    assert!(event.tables_changed.contains(&"t".to_string()));
}

/// RED — t5_09: route fires on declared UPDATE event type.
#[test]
fn t5_09_route_fires_on_update_event_type() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE memos (id UUID PRIMARY KEY, body TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE EVENT TYPE memo_changed WHEN UPDATE ON memos",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE SINK slack TYPE callback", &empty())
        .unwrap();
    db.execute("CREATE ROUTE r EVENT memo_changed TO slack", &empty())
        .unwrap();

    let log: Arc<Mutex<Vec<SinkEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let log_cb = log.clone();
    db.register_sink("slack", None, move |e| {
        log_cb.lock().unwrap().push(e.clone());
        Ok(())
    })
    .unwrap();

    // INSERT does NOT match (event type is UPDATE).
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(1)));
    p.insert("body".into(), Value::Text("v1".into()));
    db.execute("INSERT INTO memos (id, body) VALUES ($id, $body)", &p)
        .unwrap();
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        log.lock().unwrap().len(),
        0,
        "INSERT must not fire UPDATE-event-type route"
    );

    // UPDATE matches.
    p.insert("body".into(), Value::Text("v2".into()));
    db.execute("UPDATE memos SET body = $body WHERE id = $id", &p)
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && log.lock().unwrap().is_empty() {
        std::thread::sleep(Duration::from_millis(20));
    }
    let captured = log.lock().unwrap();
    assert_eq!(captured.len(), 1, "UPDATE must fire exactly once");
    assert_eq!(captured[0].event_type, "memo_changed");
}

/// RED — t5_10: route fires on declared DELETE event type.
#[test]
fn t5_10_route_fires_on_delete_event_type() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE memos (id UUID PRIMARY KEY, body TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE EVENT TYPE memo_gone WHEN DELETE ON memos", &empty())
        .unwrap();
    db.execute("CREATE SINK slack TYPE callback", &empty())
        .unwrap();
    db.execute("CREATE ROUTE r EVENT memo_gone TO slack", &empty())
        .unwrap();

    let log: Arc<Mutex<Vec<SinkEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let log_cb = log.clone();
    db.register_sink("slack", None, move |e| {
        log_cb.lock().unwrap().push(e.clone());
        Ok(())
    })
    .unwrap();

    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(1)));
    p.insert("body".into(), Value::Text("v1".into()));
    db.execute("INSERT INTO memos (id, body) VALUES ($id, $body)", &p)
        .unwrap();
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(log.lock().unwrap().len(), 0);

    db.execute("DELETE FROM memos WHERE id = $id", &p).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && log.lock().unwrap().is_empty() {
        std::thread::sleep(Duration::from_millis(20));
    }
    let captured = log.lock().unwrap();
    assert_eq!(captured.len(), 1, "DELETE must fire exactly once");
    assert_eq!(captured[0].event_type, "memo_gone");
}

/// RED — t5_11: subscribe-scoped-to-context-and-scope-label (the §5 + §4 + §6 seam).
#[test]
fn t5_11_route_scoped_to_context_excludes_other_contexts() {
    use contextdb_core::types::{ContextId, ScopeLabel};
    use std::collections::BTreeSet;
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("scoped.redb");
    let ctx_a = Uuid::from_u128(0xA);
    let ctx_b = Uuid::from_u128(0xB);
    let admin = Database::open(&path).unwrap();
    admin
        .execute(
            "CREATE TABLE invs (
                id UUID PRIMARY KEY,
                severity TEXT,
                context_id UUID CONTEXT_ID,
                scope_label TEXT SCOPE_LABEL_READ ('edge', 'server') WRITE ('edge', 'server')
            )",
            &empty(),
        )
        .unwrap();
    admin
        .execute("CREATE EVENT TYPE inv_match WHEN INSERT ON invs", &empty())
        .unwrap();
    admin
        .execute("CREATE SINK slack TYPE callback", &empty())
        .unwrap();
    admin
        .execute("CREATE ROUTE r EVENT inv_match TO slack", &empty())
        .unwrap();
    admin
        .execute("CREATE SINK audit TYPE callback", &empty())
        .unwrap();
    admin
        .execute("CREATE ROUTE audit_r EVENT inv_match TO audit", &empty())
        .unwrap();
    for (id, ctx, scope) in [
        (1u128, ctx_a, "edge"),
        (2u128, ctx_b, "edge"),
        (3u128, ctx_a, "server"),
        (4u128, ctx_b, "server"),
    ] {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::from_u128(id)));
        p.insert("sev".into(), Value::Text("warning".into()));
        p.insert("ctx".into(), Value::Uuid(ctx));
        p.insert("scope".into(), Value::Text(scope.into()));
        admin
            .execute(
                "INSERT INTO invs (id, severity, context_id, scope_label) VALUES ($id, $sev, $ctx, $scope)",
                &p,
            )
            .unwrap();
    }
    admin.close().unwrap();

    let scoped = Database::open_with_constraints(
        &path,
        Some(BTreeSet::from([ContextId::new(ctx_a)])),
        Some(BTreeSet::from([ScopeLabel::new("edge")])),
        None,
    )
    .unwrap();
    let log: Arc<Mutex<Vec<SinkEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let log_cb = log.clone();
    scoped
        .register_sink("slack", None, move |e| {
            log_cb.lock().unwrap().push(e.clone());
            Ok(())
        })
        .unwrap();

    // Registering a scoped sink should replay only already-queued rows whose
    // context and scope label are visible to the registering handle.
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && log.lock().unwrap().is_empty() {
        std::thread::sleep(Duration::from_millis(20));
    }
    std::thread::sleep(Duration::from_millis(100));

    let captured = log.lock().unwrap();
    assert_eq!(
        captured.len(),
        1,
        "scoped sink must replay exactly the row passing both context and scope gates"
    );
    assert_eq!(
        captured[0].row_values.get("id"),
        Some(&Value::Uuid(Uuid::from_u128(1)))
    );
    assert_eq!(
        captured[0].row_values.get("scope_label"),
        Some(&Value::Text("edge".into()))
    );
    assert!(
        !captured.iter().any(|e| matches!(
            e.row_values.get("context_id"), Some(Value::Uuid(u)) if *u == ctx_b
        )),
        "ctx_b INSERT must NOT fire on ctx_a-scoped sink"
    );
    assert!(
        !captured.iter().any(|e| matches!(
            e.row_values.get("scope_label"), Some(Value::Text(label)) if label == "server"
        )),
        "server-scoped INSERT must NOT fire on edge-scoped sink"
    );
    drop(captured);

    let future_ctx_a = Uuid::from_u128(5);
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(future_ctx_a));
    p.insert("sev".into(), Value::Text("warning".into()));
    p.insert("ctx".into(), Value::Uuid(ctx_a));
    p.insert("scope".into(), Value::Text("edge".into()));
    scoped
        .execute(
            "INSERT INTO invs (id, severity, context_id, scope_label) VALUES ($id, $sev, $ctx, $scope)",
            &p,
        )
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline
        && !log.lock().unwrap().iter().any(|e| {
            matches!(
                e.row_values.get("id"), Some(Value::Uuid(u)) if *u == future_ctx_a
            )
        })
    {
        std::thread::sleep(Duration::from_millis(20));
    }
    let captured = log.lock().unwrap();
    assert!(
        captured.iter().any(|e| matches!(
            e.row_values.get("id"), Some(Value::Uuid(u)) if *u == future_ctx_a
        )),
        "future live ctx_a control event must fire while sink is registered"
    );
    drop(captured);

    let denied_ctx_b = Uuid::from_u128(6);
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(denied_ctx_b));
    p.insert("sev".into(), Value::Text("warning".into()));
    p.insert("ctx".into(), Value::Uuid(ctx_b));
    p.insert("scope".into(), Value::Text("edge".into()));
    assert!(
        matches!(
            scoped.execute(
                "INSERT INTO invs (id, severity, context_id, scope_label) VALUES ($id, $sev, $ctx, $scope)",
                &p,
            ),
            Err(contextdb_core::Error::ContextScopeViolation { .. })
        ),
        "ctx-a scoped handle must reject live ctx-b writes before they can enqueue"
    );

    let denied_server_scope = Uuid::from_u128(7);
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(denied_server_scope));
    p.insert("sev".into(), Value::Text("warning".into()));
    p.insert("ctx".into(), Value::Uuid(ctx_a));
    p.insert("scope".into(), Value::Text("server".into()));
    assert!(
        matches!(
            scoped.execute(
                "INSERT INTO invs (id, severity, context_id, scope_label) VALUES ($id, $sev, $ctx, $scope)",
                &p,
            ),
            Err(contextdb_core::Error::ScopeLabelViolation { .. })
        ),
        "edge-scoped handle must reject live server-scope writes before they can enqueue"
    );
    std::thread::sleep(Duration::from_millis(100));
    let captured = log.lock().unwrap();
    assert!(
        !captured.iter().any(|e| matches!(
            e.row_values.get("id"), Some(Value::Uuid(u)) if *u == denied_ctx_b || *u == denied_server_scope
        )),
        "rejected live writes must not be delivered to the scoped sink"
    );
    drop(captured);
    scoped.close().unwrap();

    let admin = Database::open(&path).unwrap();
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(future_ctx_a));
    let result = admin
        .execute(
            "SELECT id, context_id, scope_label FROM invs WHERE id = $id",
            &p,
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            Value::Uuid(future_ctx_a),
            Value::Uuid(ctx_a),
            Value::Text("edge".into())
        ]],
        "future allowed scoped write must persist"
    );
    for id in [denied_ctx_b, denied_server_scope] {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(id));
        let result = admin
            .execute("SELECT id FROM invs WHERE id = $id", &p)
            .unwrap();
        assert_eq!(result.rows.len(), 0, "denied scoped write must not persist");
    }

    let live_admin_log: Arc<Mutex<Vec<SinkEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let live_admin_cb = live_admin_log.clone();
    admin
        .__debug_register_sink_with_constraints_for_test(
            "slack",
            Some(BTreeSet::from([ContextId::new(ctx_a)])),
            Some(BTreeSet::from([ScopeLabel::new("edge")])),
            None,
            move |e| {
                live_admin_cb.lock().unwrap().push(e.clone());
                Ok(())
            },
        )
        .unwrap();
    std::thread::sleep(Duration::from_millis(100));
    live_admin_log.lock().unwrap().clear();

    let live_admin_allowed = Uuid::from_u128(8);
    let live_admin_ctx_b = Uuid::from_u128(9);
    let live_admin_server = Uuid::from_u128(10);
    for (id, ctx, scope) in [
        (live_admin_allowed, ctx_a, "edge"),
        (live_admin_ctx_b, ctx_b, "edge"),
        (live_admin_server, ctx_a, "server"),
    ] {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(id));
        p.insert("sev".into(), Value::Text("warning".into()));
        p.insert("ctx".into(), Value::Uuid(ctx));
        p.insert("scope".into(), Value::Text(scope.into()));
        admin
            .execute(
                "INSERT INTO invs (id, severity, context_id, scope_label) VALUES ($id, $sev, $ctx, $scope)",
                &p,
            )
            .unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline
        && !live_admin_log.lock().unwrap().iter().any(|e| {
            matches!(
                e.row_values.get("id"), Some(Value::Uuid(u)) if *u == live_admin_allowed
            )
        })
    {
        std::thread::sleep(Duration::from_millis(20));
    }
    std::thread::sleep(Duration::from_millis(100));
    let captured = live_admin_log.lock().unwrap();
    assert!(
        captured.iter().any(|e| matches!(
            e.row_values.get("id"), Some(Value::Uuid(u)) if *u == live_admin_allowed
        )),
        "scoped sink must receive live admin-source events that pass context and scope filters"
    );
    assert!(
        !captured.iter().any(|e| matches!(
            e.row_values.get("id"), Some(Value::Uuid(u)) if *u == live_admin_ctx_b || *u == live_admin_server
        )),
        "scoped sink must filter live admin-source events by context/scope, not rely only on scoped write rejection"
    );
    drop(captured);

    for id in [Uuid::from_u128(2), Uuid::from_u128(3), Uuid::from_u128(4)] {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(id));
        admin
            .execute("DELETE FROM invs WHERE id = $id", &p)
            .unwrap();
        assert_eq!(
            admin
                .execute("SELECT id FROM invs WHERE id = $id", &p)
                .unwrap()
                .rows
                .len(),
            0,
            "disallowed source rows are deleted before unscoped replay; audit delivery must come from queue"
        );
    }

    let audit_log: Arc<Mutex<Vec<SinkEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let audit_cb = audit_log.clone();
    admin
        .register_sink("audit", None, move |e| {
            audit_cb.lock().unwrap().push(e.clone());
            Ok(())
        })
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let captured = audit_log.lock().unwrap();
        let has_ctx_b = captured.iter().any(|e| {
            matches!(
                e.row_values.get("id"),
                Some(Value::Uuid(u)) if *u == Uuid::from_u128(2)
            )
        });
        let has_server_scope = captured.iter().any(|e| {
            matches!(
                e.row_values.get("id"),
                Some(Value::Uuid(u)) if *u == Uuid::from_u128(3)
            )
        });
        if has_ctx_b && has_server_scope {
            break;
        }
        drop(captured);
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        audit_log.lock().unwrap().iter().any(|e| matches!(
            (
                e.row_values.get("id"),
                e.row_values.get("context_id"),
                e.row_values.get("scope_label"),
                e.row_values.get("severity"),
            ),
            (
                Some(Value::Uuid(id)),
                Some(Value::Uuid(ctx)),
                Some(Value::Text(scope)),
                Some(Value::Text(severity)),
            ) if *id == Uuid::from_u128(2) && *ctx == ctx_b && scope == "edge" && severity == "warning"
        )),
        "unscoped audit sink must receive the queued ctx_b event with payload"
    );
    assert!(
        audit_log.lock().unwrap().iter().any(|e| matches!(
            (
                e.row_values.get("id"),
                e.row_values.get("context_id"),
                e.row_values.get("scope_label"),
                e.row_values.get("severity"),
            ),
            (
                Some(Value::Uuid(id)),
                Some(Value::Uuid(ctx)),
                Some(Value::Text(scope)),
                Some(Value::Text(severity)),
            ) if *id == Uuid::from_u128(3) && *ctx == ctx_a && scope == "server" && severity == "warning"
        )),
        "unscoped audit sink must receive the queued server-scope event with payload"
    );
}

/// RED — t5_12: panic in one sink does not poison the dispatcher.
#[test]
fn t5_12_subscribe_callback_exception_isolated() {
    let db = Database::open_memory();
    declare_inv_schema(&db);
    db.execute("CREATE SINK other TYPE callback", &empty())
        .unwrap();
    db.execute("CREATE ROUTE r2 EVENT inv_match TO other", &empty())
        .unwrap();

    db.register_sink("slack", None, |_| panic!("sink panic"))
        .unwrap();
    let other_count = Arc::new(AtomicU64::new(0));
    let other_cb = other_count.clone();
    db.register_sink("other", None, move |_| {
        other_cb.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .unwrap();

    insert_inv(&db, "warning", "boom");
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && other_count.load(Ordering::SeqCst) == 0 {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        other_count.load(Ordering::SeqCst),
        1,
        "second sink must still fire after first sink panics"
    );

    // Engine itself must remain healthy.
    insert_inv(&db, "warning", "after-panic");
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        other_count.load(Ordering::SeqCst),
        2,
        "engine must keep accepting writes and routing events after sink panic"
    );
}

/// RED — t5_13: principal-bound sink does not receive ACL-denied rows (§5+§9 seam).
#[test]
fn t5_13_sink_excludes_acl_denied_rows() {
    use contextdb_core::types::Principal;
    let acl_a = Uuid::from_u128(0xA);
    let acl_b = Uuid::from_u128(0xB);
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("bus-acl.redb");
    let script = "\
CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID)
CREATE TABLE secrets (id UUID PRIMARY KEY, body TEXT, acl_id UUID ACL REFERENCES acl_grants(acl_id))
CREATE EVENT TYPE secret_event WHEN INSERT ON secrets
CREATE SINK alice_sink TYPE callback
CREATE ROUTE r EVENT secret_event TO alice_sink
INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id) VALUES ('00000000-0000-0000-0000-000000000100', 'Agent', 'alice', '00000000-0000-0000-0000-00000000000a')
INSERT INTO secrets (id, body, acl_id) VALUES ('00000000-0000-0000-0000-000000000001', 'replay-granted', '00000000-0000-0000-0000-00000000000a')
INSERT INTO secrets (id, body, acl_id) VALUES ('00000000-0000-0000-0000-000000000002', 'replay-denied', '00000000-0000-0000-0000-00000000000b')
DELETE FROM secrets WHERE id = '00000000-0000-0000-0000-000000000001'
DELETE FROM secrets WHERE id = '00000000-0000-0000-0000-000000000002'
";
    let out = run_cli_script(&path, &[], script);
    assert!(
        out.status.success(),
        "child process should seed durable ACL events; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    let db = Database::open(&path).unwrap();
    assert_eq!(
        db.scan("secrets", db.snapshot()).unwrap().len(),
        0,
        "replay must come from the durable sink queue, not a current-table scan"
    );

    let log: Arc<Mutex<Vec<SinkEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let log_cb = log.clone();
    db.register_sink(
        "alice_sink",
        Some(Principal::Agent("alice".into())),
        move |e| {
            log_cb.lock().unwrap().push(e.clone());
            Ok(())
        },
    )
    .unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && log.lock().unwrap().is_empty() {
        std::thread::sleep(Duration::from_millis(50));
    }
    std::thread::sleep(Duration::from_millis(200));

    {
        let captured = log.lock().unwrap();
        assert_eq!(
            captured.len(),
            1,
            "alice's sink must replay exactly the durable granted row, not the denied queued row"
        );
        assert_eq!(
            captured[0].row_values.get("acl_id"),
            Some(&Value::Uuid(acl_a))
        );
        assert_eq!(
            captured[0].row_values.get("body"),
            Some(&Value::Text("replay-granted".into()))
        );
    }

    // With the sink live, insert one row in acl_a (granted to alice) and one in
    // acl_b (not granted). This keeps the live-delivery seam covered too.
    for (i, aid, body) in [
        (3u128, acl_a, "live-granted"),
        (4u128, acl_b, "live-denied"),
    ] {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::from_u128(i)));
        p.insert("body".into(), Value::Text(body.into()));
        p.insert("aid".into(), Value::Uuid(aid));
        db.execute(
            "INSERT INTO secrets (id, body, acl_id) VALUES ($id, $body, $aid)",
            &p,
        )
        .unwrap();
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && log.lock().unwrap().len() < 2 {
        std::thread::sleep(Duration::from_millis(50));
    }
    std::thread::sleep(Duration::from_millis(200));
    let captured = log.lock().unwrap();
    assert_eq!(
        captured.len(),
        2,
        "alice's sink must receive the replayed granted row and live granted row only"
    );
    let bodies = captured
        .iter()
        .map(|event| event.row_values.get("body").cloned())
        .collect::<Vec<_>>();
    assert!(bodies.contains(&Some(Value::Text("replay-granted".into()))));
    assert!(bodies.contains(&Some(Value::Text("live-granted".into()))));
    assert!(!bodies.contains(&Some(Value::Text("replay-denied".into()))));
    assert!(!bodies.contains(&Some(Value::Text("live-denied".into()))));
    assert!(
        captured
            .iter()
            .all(|event| event.row_values.get("acl_id") == Some(&Value::Uuid(acl_a)))
    );
}

/// RED — t5_14: SinkMetrics field accuracy across permanent_failures and queued.
#[test]
fn t5_14_sink_metrics_field_accuracy() {
    let db = Database::open_memory();
    declare_inv_schema(&db);
    db.execute("CREATE SINK held TYPE callback", &empty())
        .unwrap();
    db.execute("CREATE ROUTE r_held EVENT inv_match TO held", &empty())
        .unwrap();

    // Sink "slack" returns Permanent on every call — exercise permanent_failures.
    db.register_sink("slack", None, |_| Err(SinkError::Permanent("nope".into())))
        .unwrap();

    // Sink "held" blocks on a Mutex — exercise queued (events held mid-delivery).
    let hold_gate = Arc::new(Mutex::new(()));
    let hold_cb = hold_gate.clone();
    db.register_sink("held", None, move |_| {
        let _g = hold_cb.lock().unwrap();
        Ok(())
    })
    .unwrap();

    // Acquire the gate BEFORE inserting so "held" sink blocks on first dispatch.
    let gate_guard = hold_gate.lock().unwrap();

    for _ in 0..3 {
        insert_inv(&db, "warning", "x");
    }

    // Drain permanent_failures for "slack". No gate held for slack, so it
    // resolves all 3 failures.
    let deadline_p = Instant::now() + Duration::from_secs(5);
    let mut saw_slack_failures = false;
    while Instant::now() <= deadline_p {
        let m = db.sink_metrics_for_test("slack");
        if m.permanent_failures >= 3 {
            assert_eq!(
                m.permanent_failures, 3,
                "exactly 3 permanent failures recorded"
            );
            assert_eq!(
                m.delivered, 0,
                "permanent failures must not count as delivered"
            );
            assert_eq!(m.retried, 0, "permanent failures must not retry");
            assert_eq!(
                m.queued, 0,
                "queued counts events held mid-delivery, not failed events"
            );
            saw_slack_failures = true;
            break;
        }
        std::thread::yield_now();
    }
    assert!(
        saw_slack_failures,
        "slack metrics never reached permanent_failures=3"
    );

    // While "held" sink is blocked on the gate, queued must be > 0.
    let deadline_q = Instant::now() + Duration::from_secs(5);
    let mut saw_held_queue = false;
    while Instant::now() <= deadline_q {
        let m = db.sink_metrics_for_test("held");
        if m.queued > 0 {
            assert!(
                m.queued >= 1,
                "at least 1 event queued mid-delivery; got {}",
                m.queued
            );
            assert_eq!(m.delivered, 0, "delivered must be 0 while held");
            saw_held_queue = true;
            break;
        }
        std::thread::yield_now();
    }
    assert!(
        saw_held_queue,
        "held metrics never showed queued > 0 while gate was held"
    );

    // Release the gate; queued must drain to 0 and delivered must increase.
    drop(gate_guard);

    let deadline_d = Instant::now() + Duration::from_secs(5);
    let mut settled = false;
    let mut last = db.sink_metrics_for_test("held");
    while Instant::now() <= deadline_d {
        let m = db.sink_metrics_for_test("held");
        last = m.clone();
        if m.queued == 0 && m.delivered >= 3 {
            assert_eq!(m.queued, 0, "queued returns to 0 after gate release");
            assert_eq!(m.delivered, 3, "all 3 events delivered after gate release");
            settled = true;
            break;
        }
        std::thread::yield_now();
    }
    assert!(
        settled,
        "held metrics never settled (queued={}, delivered={})",
        last.queued, last.delivered
    );
}
