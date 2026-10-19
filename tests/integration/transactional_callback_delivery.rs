//! Transactional preparation and durable external delivery must share one commit boundary.

use contextdb_core::{Error, Value, Wallclock};
use contextdb_engine::plugin::{CommitSource, DatabasePlugin};
use contextdb_engine::{
    Database, MetadataBody, MetadataRequest, ReadRoute, ReadSession, SinkError, SinkEvent,
};
use contextdb_tx::WriteSet;
use std::collections::{HashMap, HashSet};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const CRASH_CHILD_ROLE: &str = "CONTEXTDB_CALLBACK_DELIVERY_CRASH_ROLE";
const CRASH_CHILD_STORE: &str = "CONTEXTDB_CALLBACK_DELIVERY_CRASH_STORE";

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn prepare_schema(db: &Database) {
    for ddl in [
        "CREATE TABLE parents (id INTEGER PRIMARY KEY)",
        "CREATE TABLE observations (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES parents(id))",
        "CREATE TABLE incidents (id INTEGER PRIMARY KEY, observation_id INTEGER NOT NULL REFERENCES observations(id))",
        "CREATE TABLE notifications (id INTEGER PRIMARY KEY, incident_id INTEGER NOT NULL REFERENCES incidents(id))",
        "CREATE TRIGGER prepare_observation ON observations WHEN INSERT",
        "CREATE EVENT TYPE notification_ready WHEN INSERT ON notifications",
        "CREATE SINK outgoing TYPE callback",
        "CREATE ROUTE send_notification EVENT notification_ready TO outgoing",
    ] {
        db.execute(ddl, &empty()).unwrap();
    }
}

fn register_preparation(db: &Database, calls: Arc<AtomicUsize>) {
    db.register_transactional_trigger_callback("prepare_observation", move |db, ctx| {
        calls.fetch_add(1, Ordering::SeqCst);
        let id = ctx.row_values["id"].as_i64().expect("observation id");
        let parent_id = ctx.row_values["parent_id"].as_i64().expect("parent id");
        for (table, values) in [
            ("parents", vec![("id", Value::Int64(parent_id))]),
            (
                "incidents",
                vec![
                    ("id", Value::Int64(id)),
                    ("observation_id", Value::Int64(id)),
                ],
            ),
            (
                "notifications",
                vec![("id", Value::Int64(id)), ("incident_id", Value::Int64(id))],
            ),
        ] {
            db.insert_row(
                ctx.tx,
                table,
                values
                    .into_iter()
                    .map(|(key, value)| (key.to_owned(), value))
                    .collect(),
            )?;
        }
        Ok(())
    })
    .unwrap();
    db.complete_initialization().unwrap();
}

fn insert_observation(db: &Database, id: i64) -> contextdb_core::Result<()> {
    db.execute(
        "INSERT INTO observations (id, parent_id) VALUES ($id, $parent)",
        &HashMap::from([
            ("id".to_owned(), Value::Int64(id)),
            ("parent".to_owned(), Value::Int64(id + 100)),
        ]),
    )?;
    Ok(())
}

fn assert_records(db: &Database, ids: &[i64]) {
    for (table, column) in [
        ("observations", "id"),
        ("parents", "id"),
        ("incidents", "id"),
        ("notifications", "id"),
    ] {
        let expected: Vec<Vec<Value>> = ids
            .iter()
            .map(|id| {
                vec![Value::Int64(if table == "parents" {
                    id + 100
                } else {
                    *id
                })]
            })
            .collect();
        assert_eq!(
            db.execute(
                &format!("SELECT {column} FROM {table} ORDER BY {column}"),
                &empty()
            )
            .unwrap()
            .rows,
            expected,
            "durable records in {table}"
        );
    }
}

fn assert_delivery(db: &Database, pending: u64, visible: &[SinkEvent]) {
    assert_eq!(
        db.sink_metrics("outgoing").queued,
        pending,
        "pending delivery"
    );
    assert_eq!(visible.len(), 0, "user-visible events before delivery");
}

fn register_observed_sink(db: &Database, visible: Arc<Mutex<Vec<SinkEvent>>>) {
    db.register_sink("outgoing", None, move |event| {
        visible.lock().unwrap().push(event.clone());
        Ok(())
    })
    .unwrap();
}

fn wait_for_durable_ack(db: &Database) {
    loop {
        let metrics = db.sink_metrics("outgoing");
        if metrics.queued == 0 && metrics.delivered >= 1 {
            return;
        }
        std::thread::yield_now();
    }
}

fn read_only_uncertainty(path: &std::path::Path, route: ReadRoute) -> (u64, u64) {
    let session = ReadSession::open(path).expect("open bounded read-only status");
    assert_eq!(session.route(), route);
    let answer = session
        .metadata(MetadataRequest::EventsStatus, None)
        .expect("read event status");
    let MetadataBody::EventsStatus { status, .. } = answer.body else {
        panic!("expected event status");
    };
    let sink = status
        .sinks
        .iter()
        .find(|sink| sink.name == "outgoing")
        .unwrap();
    (sink.queued, sink.uncertain)
}

struct RefuseLateCommit {
    armed: AtomicBool,
}

struct ExitBeforeDurableCommit {
    armed: AtomicBool,
    marker: std::path::PathBuf,
}

impl DatabasePlugin for ExitBeforeDurableCommit {
    fn pre_commit(&self, writes: &WriteSet, _source: CommitSource) -> contextdb_core::Result<()> {
        if self.armed.load(Ordering::SeqCst) {
            assert!(
                !writes.relational_inserts.is_empty(),
                "callback records were staged"
            );
            std::fs::write(&self.marker, b"callback prepared").unwrap();
            std::process::exit(0);
        }
        Ok(())
    }
}

impl DatabasePlugin for RefuseLateCommit {
    fn pre_commit(&self, _writes: &WriteSet, _source: CommitSource) -> contextdb_core::Result<()> {
        if self.armed.load(Ordering::SeqCst) {
            Err(Error::Other("late plugin refusal".to_owned().into()))
        } else {
            Ok(())
        }
    }
}

#[test]
fn callback_created_dependency_commits_with_observation_and_pending_delivery() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let db = Database::open_memory();
    prepare_schema(&db);
    let calls = Arc::new(AtomicUsize::new(0));
    register_preparation(&db, calls.clone());
    let visible = Vec::new();

    insert_observation(&db, 1).expect("callback completes the missing parent before validation");

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_records(&db, &[1]);
    assert_delivery(&db, 1, &visible);
    let delivered = Arc::new(Mutex::new(Vec::new()));
    register_observed_sink(&db, delivered.clone());
    wait_for_durable_ack(&db);
    let delivered = delivered.lock().unwrap();
    assert_eq!(delivered.len(), 1, "one user-visible event after commit");
    assert!(
        delivered[0].action_id > 0,
        "delivery exposes the action identity"
    );
}

#[test]
fn late_plugin_refusal_after_preparation_publishes_no_records_delivery_or_event() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let plugin = Arc::new(RefuseLateCommit {
        armed: AtomicBool::new(false),
    });
    let db = Database::open_memory_with_plugin(plugin.clone()).unwrap();
    prepare_schema(&db);
    let calls = Arc::new(AtomicUsize::new(0));
    register_preparation(&db, calls.clone());
    let visible = Arc::new(Mutex::new(Vec::new()));
    register_observed_sink(&db, visible.clone());
    plugin.armed.store(true, Ordering::SeqCst);

    assert!(insert_observation(&db, 2).is_err());

    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "preparation ran before late refusal"
    );
    assert_records(&db, &[]);
    assert_delivery(&db, 0, &visible.lock().unwrap());
}

#[test]
fn local_durable_commit_failure_after_preparation_publishes_nothing_even_after_reopen() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("failed-commit.db");
    let db = Database::open(&path).unwrap();
    prepare_schema(&db);
    let calls = Arc::new(AtomicUsize::new(0));
    register_preparation(&db, calls.clone());
    let visible = Arc::new(Mutex::new(Vec::<SinkEvent>::new()));
    register_observed_sink(&db, visible.clone());

    db.fail_next_local_durable_commit_for_test();
    let error = insert_observation(&db, 3).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("injected local durable commit failure"),
        "failure reached the file-backed durable apply: {error}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "transactional callback ran before durable commit failed"
    );
    assert_records(&db, &[]);
    assert_delivery(&db, 0, &visible.lock().unwrap());

    db.close().unwrap();
    let reopened = Database::open(&path).unwrap();
    assert_records(&reopened, &[]);
    assert_delivery(&reopened, 0, &visible.lock().unwrap());
    register_observed_sink(&reopened, visible.clone());
    assert_eq!(
        visible.lock().unwrap().len(),
        0,
        "reopening did not publish an event from the failed commit"
    );
}

#[test]
fn final_commit_validation_failure_discards_callback_records_delivery_and_event() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let db = Database::open_memory();
    prepare_schema(&db);
    let prepared = Arc::new(AtomicBool::new(false));
    db.register_transactional_trigger_callback("prepare_observation", {
        let prepared = prepared.clone();
        move |db, ctx| {
            let id = ctx.row_values["id"].as_i64().unwrap();
            db.insert_row(
                ctx.tx,
                "incidents",
                HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    ("observation_id".to_owned(), Value::Int64(id)),
                ]),
            )?;
            db.insert_row(
                ctx.tx,
                "notifications",
                HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    ("incident_id".to_owned(), Value::Int64(id)),
                ]),
            )?;
            prepared.store(true, Ordering::SeqCst);
            Ok(())
        }
    })
    .unwrap();
    db.complete_initialization().unwrap();
    let visible = Arc::new(Mutex::new(Vec::new()));
    register_observed_sink(&db, visible.clone());

    assert!(
        insert_observation(&db, 3).is_err(),
        "missing parent rejects the complete transaction"
    );

    assert!(
        prepared.load(Ordering::SeqCst),
        "callback completed before final validation"
    );
    assert_records(&db, &[]);
    assert_delivery(&db, 0, &visible.lock().unwrap());
}

#[test]
fn restart_crash_child() {
    let Some(role) = std::env::var_os(CRASH_CHILD_ROLE) else {
        return;
    };
    let path = std::path::PathBuf::from(
        std::env::var_os(CRASH_CHILD_STORE).expect("child receives a task-scoped store path"),
    );
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let exit_plugin = Arc::new(ExitBeforeDurableCommit {
        armed: AtomicBool::new(false),
        marker: path.with_extension("callback-prepared"),
    });
    let db = if role == "before_commit" {
        Database::open_with_plugin(&path, exit_plugin.clone()).unwrap()
    } else {
        Database::open(&path).unwrap()
    };
    prepare_schema(&db);
    register_preparation(&db, Arc::new(AtomicUsize::new(0)));
    match role.to_str().expect("ASCII crash role") {
        "before_commit" => {
            exit_plugin.armed.store(true, Ordering::SeqCst);
            insert_observation(&db, 4).unwrap();
            unreachable!("plugin exits after callback preparation before durable commit");
        }
        "after_commit" => {
            insert_observation(&db, 5).unwrap();
            assert_records(&db, &[5]);
            assert_delivery(&db, 1, &[]);
            let pending = db.pending_sink_action_ids_for_test("outgoing");
            assert_eq!(pending.len(), 1);
            std::fs::write(
                path.with_extension("pending-action-id"),
                pending[0].to_string(),
            )
            .unwrap();
        }
        "during_delivery" => {
            insert_observation(&db, 9).unwrap();
            let marker = path.with_extension("delivery-attempted");
            db.register_sink("outgoing", None, move |event| {
                std::fs::write(&marker, event.action_id.to_string()).unwrap();
                std::process::exit(0);
            })
            .unwrap();
            std::thread::park();
            unreachable!("the sink callback exits the process");
        }
        other => panic!("unknown crash role: {other}"),
    }
    std::fs::write(path.with_extension("crash-reached"), b"ready").unwrap();
    // Skips Database::close and Drop, including their orderly shutdown paths.
    std::process::exit(0);
}

fn run_crash_child(path: &std::path::Path, role: &str) {
    let status = Command::new(std::env::current_exe().expect("integration test executable"))
        .arg("--exact")
        .arg("transactional_callback_delivery::restart_crash_child")
        .arg("--nocapture")
        .env(CRASH_CHILD_ROLE, role)
        .env(CRASH_CHILD_STORE, path)
        .status()
        .expect("run crash child");
    assert!(
        status.success(),
        "crash child reached its process exit: {status}"
    );
    match role {
        "before_commit" => assert_eq!(
            std::fs::read(path.with_extension("callback-prepared")).unwrap(),
            b"callback prepared"
        ),
        "during_delivery" => assert!(
            !std::fs::read(path.with_extension("delivery-attempted"))
                .unwrap()
                .is_empty()
        ),
        _ => assert_eq!(
            std::fs::read(path.with_extension("crash-reached")).unwrap(),
            b"ready"
        ),
    }
}

#[test]
fn restart_before_commit_recovers_no_observation_records_delivery_or_event() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("before-commit.db");
    let visible = Arc::new(Mutex::new(Vec::new()));
    run_crash_child(&path, "before_commit");
    let db = Database::open(&path).unwrap();

    assert_records(&db, &[]);
    assert_delivery(&db, 0, &visible.lock().unwrap());
    register_observed_sink(&db, visible.clone());
    assert_eq!(
        visible.lock().unwrap().len(),
        0,
        "restart published no event"
    );
}

#[test]
fn restart_after_commit_recovers_pending_action_and_delivers_committed_records() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("after-commit.db");
    run_crash_child(&path, "after_commit");
    let db = Database::open(&path).unwrap();
    let visible = Arc::new(Mutex::new(Vec::<SinkEvent>::new()));
    assert_records(&db, &[5]);
    assert_delivery(&db, 1, &visible.lock().unwrap());
    let (delivered_tx, delivered_rx) = std::sync::mpsc::channel();
    db.register_sink("outgoing", None, {
        let visible = visible.clone();
        move |event| {
            visible.lock().unwrap().push(event.clone());
            delivered_tx.send(()).unwrap();
            Ok(())
        }
    })
    .unwrap();
    delivered_rx
        .recv()
        .expect("recovered pending action was delivered");
    wait_for_durable_ack(&db);
    db.close().unwrap();
    let delivered = visible.lock().unwrap();
    assert_eq!(delivered.len(), 1, "one user-visible event");
    let committed_id: u64 = std::fs::read_to_string(path.with_extension("pending-action-id"))
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(delivered[0].action_id, committed_id);
    let reopened = Database::open(&path).unwrap();
    assert_records(&reopened, &[5]);
    assert_eq!(
        reopened.sink_metrics("outgoing").queued,
        0,
        "delivery acknowledged durably"
    );
}

#[test]
fn lost_acknowledgement_retries_same_identity_and_deduplicates_visible_event() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("lost-ack.db");
    let db = Database::open(&path).unwrap();
    prepare_schema(&db);
    register_preparation(&db, Arc::new(AtomicUsize::new(0)));
    insert_observation(&db, 6).unwrap();
    assert_records(&db, &[6]);
    assert_delivery(&db, 1, &[]);

    let attempts = Arc::new(Mutex::new(Vec::<u64>::new()));
    let visible = Arc::new(Mutex::new(Vec::<SinkEvent>::new()));
    let seen = Arc::new(Mutex::new(HashSet::<u64>::new()));
    let (retried_tx, retried_rx) = std::sync::mpsc::channel();
    db.register_sink("outgoing", None, {
        let attempts = attempts.clone();
        let visible = visible.clone();
        let seen = seen.clone();
        move |event| {
            let mut attempts = attempts.lock().unwrap();
            attempts.push(event.action_id);
            if seen.lock().unwrap().insert(event.action_id) {
                visible.lock().unwrap().push(event.clone());
            }
            if attempts.len() == 1 {
                return Err(SinkError::OutcomeUncertain(
                    "receiver applied action; acknowledgement was lost".to_owned(),
                ));
            }
            retried_tx.send(()).unwrap();
            Ok(())
        }
    })
    .unwrap();
    retried_rx
        .recv()
        .expect("uncertain action retried with the same identity");
    wait_for_durable_ack(&db);
    let attempts = attempts.lock().unwrap();
    assert_eq!(attempts.len(), 2, "uncertain delivery is retried once");
    assert_eq!(attempts[0], attempts[1], "retry preserves action identity");
    assert_eq!(
        visible.lock().unwrap().len(),
        1,
        "deduplicating receiver shows one event"
    );
    assert_records(&db, &[6]);
    assert_eq!(
        db.sink_metrics("outgoing").uncertain,
        1,
        "lost acknowledgement is surfaced"
    );
    assert_eq!(read_only_uncertainty(&path, ReadRoute::Owner), (0, 1));
    db.close().unwrap();
    assert_eq!(read_only_uncertainty(&path, ReadRoute::File), (0, 1));
    let reopened = Database::open(&path).unwrap();
    assert_records(&reopened, &[6]);
    assert_eq!(
        reopened.sink_metrics("outgoing").queued,
        0,
        "acknowledged retry clears pending delivery"
    );
    assert_eq!(reopened.sink_metrics("outgoing").uncertain, 1);
}

#[test]
fn action_identity_does_not_reuse_drained_queue_id_after_restart() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("drained-ids.db");
    let db = Database::open(&path).unwrap();
    prepare_schema(&db);
    register_preparation(&db, Arc::new(AtomicUsize::new(0)));
    insert_observation(&db, 7).unwrap();
    let (first_tx, first_rx) = std::sync::mpsc::channel();
    db.register_sink("outgoing", None, move |event| {
        first_tx.send(event.action_id).unwrap();
        Ok(())
    })
    .unwrap();
    let first_id = first_rx.recv().unwrap();
    wait_for_durable_ack(&db);
    db.close().unwrap();

    let reopened = Database::open(&path).unwrap();
    register_preparation(&reopened, Arc::new(AtomicUsize::new(0)));
    insert_observation(&reopened, 8).unwrap();
    let (second_tx, second_rx) = std::sync::mpsc::channel();
    reopened
        .register_sink("outgoing", None, move |event| {
            second_tx.send(event.action_id).unwrap();
            Ok(())
        })
        .unwrap();
    let second_id = second_rx.recv().unwrap();
    assert!(
        second_id > first_id,
        "new action cannot reuse an acknowledged identity"
    );
    wait_for_durable_ack(&reopened);
}

#[test]
fn crash_during_external_delivery_recovers_uncertainty_before_retry() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("during-delivery.db");
    run_crash_child(&path, "during_delivery");
    let attempted_id: u64 = std::fs::read_to_string(path.with_extension("delivery-attempted"))
        .unwrap()
        .parse()
        .unwrap();
    let direct_error = match ReadSession::open(&path) {
        Ok(_) => panic!("unclean redb must require writable recovery"),
        Err(error) => error,
    };
    assert!(
        direct_error
            .to_string()
            .contains("direct read requires a writer")
            && direct_error.to_string().contains("writable recovery"),
        "{direct_error}"
    );
    let db = Database::open(&path).unwrap();
    assert_eq!(read_only_uncertainty(&path, ReadRoute::Owner), (1, 1));
    assert_records(&db, &[9]);
    assert_eq!(db.sink_metrics("outgoing").queued, 1);
    assert_eq!(db.sink_metrics("outgoing").uncertain, 1);
    db.close().unwrap();
    assert_eq!(read_only_uncertainty(&path, ReadRoute::File), (1, 1));
    let db = Database::open(&path).unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    db.register_sink("outgoing", None, move |event| {
        tx.send(event.action_id).unwrap();
        Ok(())
    })
    .unwrap();
    assert_eq!(rx.recv().unwrap(), attempted_id);
    wait_for_durable_ack(&db);
    db.close().unwrap();
    let reopened = Database::open(&path).unwrap();
    assert_records(&reopened, &[9]);
    assert_eq!(reopened.sink_metrics("outgoing").queued, 0);
    assert_eq!(reopened.sink_metrics("outgoing").uncertain, 1);
    assert_eq!(read_only_uncertainty(&path, ReadRoute::Owner), (0, 1));
    reopened.close().unwrap();
    assert_eq!(read_only_uncertainty(&path, ReadRoute::File), (0, 1));
}

#[test]
fn scheduled_action_identity_survives_drained_queue_and_restart() {
    let now = Arc::new(AtomicU64::new(1_700_000_000_000));
    let clock_now = now.clone();
    let _clock = Wallclock::test_clock_guard(move || clock_now.load(Ordering::SeqCst));
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("scheduled-ids.db");
    let db = Database::open(&path).unwrap();
    let _pause = db.pause_cron_tickler_for_test();
    for ddl in [
        "CREATE TABLE scheduled_actions (id INTEGER PRIMARY KEY)",
        "CREATE EVENT TYPE scheduled_ready WHEN INSERT ON scheduled_actions",
        "CREATE SINK outgoing TYPE callback",
        "CREATE ROUTE send_scheduled EVENT scheduled_ready TO outgoing",
        "CREATE SCHEDULE first EVERY '1 MILLISECONDS' TX (first_cb)",
    ] {
        db.execute(ddl, &empty()).unwrap();
    }
    db.register_cron_callback("first_cb", |db| {
        db.execute("INSERT INTO scheduled_actions (id) VALUES (1)", &empty())?;
        Ok(())
    })
    .unwrap();
    now.store(1_700_000_000_010, Ordering::SeqCst);
    assert_eq!(
        {
            let seam_read_memory = db
                .declared_reader_memory()
                .expect("admit the fixture operation reader");
            let mut seam_copy_owner =
                contextdb_engine::vector_read_memory::admission(seam_read_memory.clone());
            db.cron_run_due_now_for_test(&mut seam_copy_owner, seam_read_memory)
        }
        .unwrap(),
        1
    );
    assert_eq!(db.sink_metrics("outgoing").queued, 1);
    let (first_tx, first_rx) = std::sync::mpsc::channel();
    db.register_sink("outgoing", None, move |event| {
        first_tx.send(event.action_id).unwrap();
        Ok(())
    })
    .unwrap();
    let first_id = first_rx.recv().unwrap();
    wait_for_durable_ack(&db);
    db.execute("DROP SCHEDULE first", &empty()).unwrap();
    db.close().unwrap();

    let reopened = Database::open(&path).unwrap();
    let _pause_reopened = reopened.pause_cron_tickler_for_test();
    reopened
        .execute(
            "CREATE SCHEDULE second EVERY '1 MILLISECONDS' TX (second_cb)",
            &empty(),
        )
        .unwrap();
    reopened
        .register_cron_callback("second_cb", |db| {
            db.execute("INSERT INTO scheduled_actions (id) VALUES (2)", &empty())?;
            Ok(())
        })
        .unwrap();
    now.store(1_700_000_000_020, Ordering::SeqCst);
    assert_eq!(
        {
            let seam_read_memory = reopened
                .declared_reader_memory()
                .expect("admit the fixture operation reader");
            let mut seam_copy_owner =
                contextdb_engine::vector_read_memory::admission(seam_read_memory.clone());
            reopened.cron_run_due_now_for_test(&mut seam_copy_owner, seam_read_memory)
        }
        .unwrap(),
        1
    );
    let pending = reopened.pending_sink_action_ids_for_test("outgoing");
    assert_eq!(
        pending.len(),
        1,
        "second scheduled row has pending delivery"
    );
    assert!(
        pending[0] > first_id,
        "scheduled action identity cannot be reused"
    );
    assert_eq!(
        reopened
            .execute("SELECT id FROM scheduled_actions ORDER BY id", &empty())
            .unwrap()
            .rows,
        vec![vec![Value::Int64(1)], vec![Value::Int64(2)]]
    );
}

#[test]
fn delivery_panic_retains_uncertain_action_for_restart_and_deduplicated_retry() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("panicked-delivery.db");
    let db = Database::open(&path).unwrap();
    prepare_schema(&db);
    register_preparation(&db, Arc::new(AtomicUsize::new(0)));
    insert_observation(&db, 10).unwrap();
    assert_records(&db, &[10]);
    assert_delivery(&db, 1, &[]);

    let seen = Arc::new(Mutex::new(HashSet::new()));
    let visible = Arc::new(Mutex::new(Vec::<SinkEvent>::new()));
    let first_id = Arc::new(AtomicU64::new(0));
    db.register_sink("outgoing", None, {
        let seen = seen.clone();
        let visible = visible.clone();
        let first_id = first_id.clone();
        move |event| {
            first_id.store(event.action_id, Ordering::SeqCst);
            if seen.lock().unwrap().insert(event.action_id) {
                visible.lock().unwrap().push(event.clone());
            }
            panic!("effect happened, acknowledgement processing panicked");
        }
    })
    .unwrap();
    loop {
        let metrics = db.sink_metrics("outgoing");
        if metrics.uncertain > 0 || metrics.permanent_failures > 0 {
            assert!(metrics.uncertain > 0, "panic outcome must be uncertain");
            assert_eq!(metrics.queued, 1, "action remains pending");
            break;
        }
        std::thread::yield_now();
    }
    db.close().unwrap();

    let reopened = Database::open(&path).unwrap();
    assert_records(&reopened, &[10]);
    assert_eq!(reopened.sink_metrics("outgoing").queued, 1);
    assert!(reopened.sink_metrics("outgoing").uncertain > 0);
    let (retried_tx, retried_rx) = std::sync::mpsc::channel();
    reopened
        .register_sink("outgoing", None, {
            let seen = seen.clone();
            let visible = visible.clone();
            move |event| {
                if seen.lock().unwrap().insert(event.action_id) {
                    visible.lock().unwrap().push(event.clone());
                }
                retried_tx.send(event.action_id).unwrap();
                Ok(())
            }
        })
        .unwrap();
    assert_eq!(retried_rx.recv().unwrap(), first_id.load(Ordering::SeqCst));
    wait_for_durable_ack(&reopened);
    assert_eq!(visible.lock().unwrap().len(), 1, "one visible effect");
}

#[test]
fn full_sink_queue_refuses_scheduled_row_and_delivery_together() {
    let now = Arc::new(AtomicU64::new(1_700_000_000_000));
    let clock_now = now.clone();
    let _clock = Wallclock::test_clock_guard(move || clock_now.load(Ordering::SeqCst));
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("scheduled-capacity.db");
    let db = Database::open(&path).unwrap();
    let _pause = db.pause_cron_tickler_for_test();
    for ddl in [
        "CREATE TABLE scheduled_actions (id INTEGER PRIMARY KEY)",
        "CREATE EVENT TYPE scheduled_ready WHEN INSERT ON scheduled_actions",
        "CREATE SINK outgoing TYPE callback",
        "CREATE ROUTE send_scheduled EVENT scheduled_ready TO outgoing",
    ] {
        db.execute(ddl, &empty()).unwrap();
    }
    db.execute("INSERT INTO scheduled_actions (id) VALUES (1)", &empty())
        .unwrap();
    assert_eq!(db.sink_metrics("outgoing").queued, 1);
    db.set_sink_queue_capacity_for_test(1);
    db.execute(
        "CREATE SCHEDULE second EVERY '1 MILLISECONDS' TX (second_cb)",
        &empty(),
    )
    .unwrap();
    db.register_cron_callback("second_cb", |db| {
        db.execute("INSERT INTO scheduled_actions (id) VALUES (2)", &empty())?;
        Ok(())
    })
    .unwrap();
    now.store(1_700_000_000_010, Ordering::SeqCst);
    assert_eq!(
        {
            let seam_read_memory = db
                .declared_reader_memory()
                .expect("admit the fixture operation reader");
            let mut seam_copy_owner =
                contextdb_engine::vector_read_memory::admission(seam_read_memory.clone());
            db.cron_run_due_now_for_test(&mut seam_copy_owner, seam_read_memory)
        }
        .unwrap(),
        0
    );
    assert_eq!(
        db.execute("SELECT id FROM scheduled_actions ORDER BY id", &empty())
            .unwrap()
            .rows,
        vec![vec![Value::Int64(1)]],
        "full queue refuses the scheduled row commit"
    );
    assert_eq!(db.sink_metrics("outgoing").queued, 1);
    db.execute("DROP SCHEDULE second", &empty()).unwrap();
    db.close().unwrap();
    let reopened = Database::open(&path).unwrap();
    assert_eq!(
        reopened
            .execute("SELECT id FROM scheduled_actions ORDER BY id", &empty())
            .unwrap()
            .rows,
        vec![vec![Value::Int64(1)]]
    );
    assert_eq!(reopened.sink_metrics("outgoing").queued, 1);
}
