//! Opening a store holds the same bounded window of a sink's queued events
//! however long the queue is; the rest stays in the store and the dispatcher
//! reads it window by window, delivering every queued event once, in order.
use contextdb_core::Value;
use contextdb_core::types::ContextId;
use contextdb_engine::memory_accounting::{
    take_budget_ledger_for_test, watch_budget_ledger_for_test,
};
use contextdb_engine::vector_read_memory::{
    take_reader_budget_ledger_for_test, watch_reader_budget_ledger_for_test,
};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

const SINK: &str = "ev_sink";
/// The resident bounds a bounded sink window keeps.
const WINDOW_ENTRIES: usize = 1024;
const WINDOW_BYTES: usize = 1024 * 1024;

fn open(path: &std::path::Path) -> Database {
    let mut options = DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        memory_limit: Some(512 * 1024 * 1024),
        ..DatabaseOpenOptions::default()
    };
    options.owner_reads.limits.limits.memory = 1 << 30;
    Database::open_with_options(path, options).unwrap()
}

/// A store with `count` events queued for a sink no program has registered.
fn queued(path: &std::path::Path, count: usize) {
    let db = open(path);
    for sql in [
        "CREATE TABLE ev (id INTEGER PRIMARY KEY, body TEXT)",
        "CREATE EVENT TYPE ev_insert WHEN INSERT ON ev",
        "CREATE SINK ev_sink TYPE callback",
        "CREATE ROUTE ev_route EVENT ev_insert TO ev_sink",
    ] {
        db.execute(sql, &HashMap::new()).unwrap();
    }
    for id in 0..count {
        db.execute(
            "INSERT INTO ev VALUES ($id, 'an event payload of modest size')",
            &HashMap::from([("id".to_string(), Value::Int64(id as i64))]),
        )
        .unwrap();
    }
    assert_eq!(db.sink_metrics(SINK).queued, count as u64);
    db.close().unwrap();
}

/// Wait for dispatcher state, failing (never sleeping) if it does not arrive.
fn wait_for(label: &str, mut ready: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    while !ready() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {label}"
        );
        std::thread::yield_now();
    }
}

fn peak(ledger: &[contextdb_engine::memory_accounting::BudgetLedgerEntry]) -> usize {
    ledger
        .iter()
        .map(|entry| entry.used_after)
        .max()
        .unwrap_or(0)
}

#[test]
fn opening_a_store_holds_the_same_window_however_many_events_are_queued() {
    let root = tempfile::tempdir().unwrap();
    let mut peaks = Vec::new();
    for count in [2048usize, 8192] {
        let path = root.path().join(format!("events-{count}.db"));
        queued(&path, count);
        watch_reader_budget_ledger_for_test();
        watch_budget_ledger_for_test();
        let db = open(&path);
        let reader = peak(&take_reader_budget_ledger_for_test());
        let writer = peak(&take_budget_ledger_for_test());
        let window_charge = db
            .writer_memory_ownership_receipt_for_test()
            .sink_window_charge;
        let (resident, resident_bytes, depth) = db.sink_queue_residency_for_test(SINK);
        eprintln!(
            "{count} queued: open reader peak {reader} B, writer peak {writer} B, window charge {window_charge} B, resident {resident} entries / {resident_bytes} B, depth {depth}"
        );
        assert_eq!(depth, count as u64, "every queued event is still queued");
        assert_eq!(db.sink_metrics(SINK).queued, count as u64);
        assert!(resident <= WINDOW_ENTRIES, "{resident}");
        assert!(resident_bytes <= WINDOW_BYTES, "{resident_bytes}");
        db.close().unwrap();
        assert!(
            window_charge >= resident_bytes,
            "the window owner holds its entries' decoded bytes"
        );
        peaks.push((reader, window_charge, resident, resident_bytes));
    }
    assert_eq!(
        peaks[0], peaks[1],
        "a 4x longer queue must not change what open holds"
    );
}

#[test]
fn every_queued_event_is_delivered_once_in_order_across_windows() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("drain.db");
    let count = 3 * WINDOW_ENTRIES + 17;
    queued(&path, count);
    let db = open(&path);
    let expected = db.pending_sink_action_ids_for_test(SINK);
    assert_eq!(expected.len(), count);
    let delivered = Arc::new(Mutex::new(Vec::new()));
    let sink = delivered.clone();
    db.register_sink(SINK, None, move |event| {
        sink.lock().unwrap().push(event.action_id);
        Ok(())
    })
    .unwrap();
    wait_for("every queued event delivered and acknowledged", || {
        db.sink_metrics(SINK).queued == 0
    });
    let delivered = delivered.lock().unwrap().clone();
    assert_eq!(delivered, expected, "each queued event once, in id order");
    let metrics = db.sink_metrics(SINK);
    assert_eq!(metrics.queued, 0);
    let (resident, _, depth) = db.sink_queue_residency_for_test(SINK);
    assert_eq!((resident, depth), (0, 0));
    db.close().unwrap();
    let db = open(&path);
    assert_eq!(
        db.sink_metrics(SINK).queued,
        0,
        "acknowledged events stay acknowledged"
    );
    db.close().unwrap();
}

fn context(n: u128) -> uuid::Uuid {
    uuid::Uuid::from_u128(0xC0_0000 + n)
}

fn open_in(path: &std::path::Path, contexts: &[u128]) -> Database {
    let mut options = DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        memory_limit: Some(512 * 1024 * 1024),
        contexts: Some(
            contexts
                .iter()
                .map(|n| ContextId::new(context(*n)))
                .collect::<BTreeSet<_>>(),
        ),
        ..DatabaseOpenOptions::default()
    };
    options.owner_reads.limits.limits.memory = 1 << 30;
    Database::open_with_options(path, options).unwrap()
}

/// Register a collecting callback and wait until `count` deliveries.
fn drain(db: &Database, count: u64) -> Vec<u64> {
    let target = db.sink_metrics(SINK).queued.saturating_sub(count);
    let delivered = Arc::new(Mutex::new(Vec::new()));
    let sink = delivered.clone();
    db.register_sink(SINK, None, move |event| {
        sink.lock().unwrap().push(event.action_id);
        Ok(())
    })
    .unwrap();
    wait_for("the expected deliveries acknowledged", || {
        let metrics = db.sink_metrics(SINK);
        metrics.delivered >= count && metrics.queued == target
    });
    delivered.lock().unwrap().clone()
}

#[test]
fn events_a_registration_cannot_see_never_hide_the_ones_it_can() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("denied.db");
    const DENIED: usize = 2 * WINDOW_ENTRIES + 52;
    const VISIBLE: usize = 50;
    let ids = {
        let db = open(&path);
        for sql in [
            "CREATE TABLE ev (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID NOT NULL, body TEXT)",
            "CREATE EVENT TYPE ev_insert WHEN INSERT ON ev",
            "CREATE SINK ev_sink TYPE callback",
            "CREATE ROUTE ev_route EVENT ev_insert TO ev_sink",
        ] {
            db.execute(sql, &HashMap::new()).unwrap();
        }
        for id in 0..DENIED + VISIBLE {
            let owner = if id < DENIED { 2 } else { 1 };
            db.execute(
                "INSERT INTO ev VALUES ($id, $context, 'payload')",
                &HashMap::from([
                    ("id".to_string(), Value::Int64(id as i64)),
                    ("context".to_string(), Value::Uuid(context(owner))),
                ]),
            )
            .unwrap();
        }
        let ids = db.pending_sink_action_ids_for_test(SINK);
        db.close().unwrap();
        ids
    };
    assert_eq!(ids.len(), DENIED + VISIBLE);
    // Two whole windows hold only events this registration cannot see.
    let db = open_in(&path, &[1]);
    let delivered = drain(&db, VISIBLE as u64);
    assert_eq!(
        delivered,
        ids[DENIED..].to_vec(),
        "every visible event, once, in order"
    );
    assert_eq!(
        db.sink_metrics(SINK).queued,
        DENIED as u64,
        "the others stay queued"
    );
    db.close().unwrap();
    // Access broadens: the retained events are delivered, once, in order.
    let db = open_in(&path, &[1, 2]);
    let delivered = drain(&db, DENIED as u64);
    assert_eq!(delivered, ids[..DENIED].to_vec());
    assert_eq!(db.sink_metrics(SINK).queued, 0);
    db.close().unwrap();
}

#[test]
fn purging_a_row_removes_its_queued_event_beyond_the_window() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("purge.db");
    let count = 2 * WINDOW_ENTRIES + 9;
    queued(&path, count);
    let db = open(&path);
    let ids = db.pending_sink_action_ids_for_test(SINK);
    let (resident, _, depth) = db.sink_queue_residency_for_test(SINK);
    assert!(resident < count, "part of the queue is only in the store");
    assert_eq!(depth, count as u64);
    // Row `count - 5` was queued long after the resident window.
    let purged = (count - 5) as i64;
    db.execute(
        "PURGE FROM ev WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Int64(purged))]),
    )
    .unwrap();
    assert_eq!(db.sink_metrics(SINK).queued, count as u64 - 1);
    let mut expected = ids.clone();
    expected.remove(count - 5);
    assert_eq!(db.pending_sink_action_ids_for_test(SINK), expected);
    let delivered = drain(&db, count as u64 - 1);
    assert_eq!(delivered, expected, "the purged event is never delivered");
    db.close().unwrap();
}

#[test]
fn a_window_the_writer_budget_cannot_hold_is_refused_observably_and_resumes_when_it_can() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("refused.db");
    let count = WINDOW_ENTRIES + 40;
    queued(&path, count);
    let db = open(&path);
    let expected = db.pending_sink_action_ids_for_test(SINK);
    // Leave the writer room for nothing beyond what it already holds: the
    // dispatcher cannot fund a copy of the resident window.
    let used = db
        .writer_memory_ownership_receipt_for_test()
        .vector
        .accountant_total;
    db.set_memory_limit(Some(used + 1024)).unwrap();
    let delivered = Arc::new(Mutex::new(Vec::new()));
    let sink = delivered.clone();
    db.register_sink(SINK, None, move |event| {
        sink.lock().unwrap().push(event.action_id);
        Ok(())
    })
    .unwrap();
    wait_for("the refused window recorded on the sink", || {
        db.sink_metrics(SINK).window_refused
    });
    let metrics = db.sink_metrics(SINK);
    assert!(metrics.window_refusals >= 1, "{metrics:?}");
    assert_eq!(
        (metrics.queued, metrics.delivered),
        (count as u64, 0),
        "nothing dropped or skipped"
    );
    assert!(delivered.lock().unwrap().is_empty());
    // The budget returns: delivery resumes, every event once, in order.
    db.set_memory_limit(Some(512 * 1024 * 1024)).unwrap();
    wait_for("every event delivered after the budget returned", || {
        db.sink_metrics(SINK).queued == 0
    });
    assert_eq!(delivered.lock().unwrap().clone(), expected);
    assert!(!db.sink_metrics(SINK).window_refused);
    db.close().unwrap();
}

#[test]
fn a_purge_beside_a_large_unrelated_backlog_holds_no_more_than_a_small_one() {
    // The purge decodes every queued entry to find the selected row's; each
    // entry's decode credit must be returned before the next, so the writer
    // peak does not follow the unrelated backlog.
    let root = tempfile::tempdir().unwrap();
    let mut peaks = Vec::new();
    for count in [2048usize, 8192] {
        let path = root.path().join(format!("purge-peak-{count}.db"));
        queued(&path, count);
        let db = open(&path);
        // The purged row lives in its own one-row table routed to the same
        // sink, so only the queue scan sees the backlog.
        for sql in [
            "CREATE TABLE pv (id INTEGER PRIMARY KEY, body TEXT)",
            "CREATE EVENT TYPE pv_insert WHEN INSERT ON pv",
            "CREATE ROUTE pv_route EVENT pv_insert TO ev_sink",
            "INSERT INTO pv VALUES (1, 'the purged row')",
        ] {
            db.execute(sql, &HashMap::new()).unwrap();
        }
        assert_eq!(db.sink_metrics(SINK).queued, count as u64 + 1);
        watch_budget_ledger_for_test();
        db.execute("PURGE FROM pv WHERE id = 1", &HashMap::new())
            .unwrap();
        let ledger = take_budget_ledger_for_test();
        let decode = ledger
            .iter()
            .filter(|entry| entry.what == "writer_source/decode_stored_value")
            .map(|entry| entry.bytes)
            .max()
            .unwrap_or(0);
        let used_before = ledger.first().map_or(0, |entry| entry.used_after);
        let rise = peak(&ledger).saturating_sub(used_before);
        eprintln!(
            "purge beside {count} queued: writer rise {rise} B, largest decode charge {decode} B"
        );
        assert_eq!(db.sink_metrics(SINK).queued, count as u64);
        db.close().unwrap();
        peaks.push(rise);
    }
    assert!(
        peaks[1] <= peaks[0] + 64 * 1024,
        "a 4x larger unrelated backlog must not raise the purge's peak: {peaks:?}"
    );
}

#[test]
fn a_queue_the_registration_cannot_see_is_read_one_paced_window_at_a_time() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("paced.db");
    {
        let db = open(&path);
        for sql in [
            "CREATE TABLE ev (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID NOT NULL, body TEXT)",
            "CREATE EVENT TYPE ev_insert WHEN INSERT ON ev",
            "CREATE SINK ev_sink TYPE callback",
            "CREATE ROUTE ev_route EVENT ev_insert TO ev_sink",
        ] {
            db.execute(sql, &HashMap::new()).unwrap();
        }
        for id in 0..3 * WINDOW_ENTRIES {
            db.execute(
                "INSERT INTO ev VALUES ($id, $context, 'payload')",
                &HashMap::from([
                    ("id".to_string(), Value::Int64(id as i64)),
                    ("context".to_string(), Value::Uuid(context(2))),
                ]),
            )
            .unwrap();
        }
        db.close().unwrap();
    }
    // Every queued event belongs to a context this handle cannot read.
    let db = open_in(&path, &[1]);
    db.register_sink(SINK, None, |_| Ok(())).unwrap();
    wait_for("two full sweeps of window reads", || {
        db.sink_window_pacing_for_test(SINK).0 >= 8
    });
    let (reads, pauses) = db.sink_window_pacing_for_test(SINK);
    eprintln!("hidden queue: {reads} window reads, {pauses} idle pauses");
    assert!(
        pauses + 1 >= reads,
        "every read after the first waited the idle pause: {reads} reads, {pauses} pauses"
    );
    assert_eq!(db.sink_metrics(SINK).delivered, 0);
    assert_eq!(db.sink_metrics(SINK).queued, 3 * WINDOW_ENTRIES as u64);
    db.close().unwrap();
}
