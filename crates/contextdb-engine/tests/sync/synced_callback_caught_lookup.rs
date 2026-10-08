//! A caught lookup miss must not shorten a received transaction.

use contextdb_core::{Error, Lsn, Value, Wallclock};
use contextdb_engine::database::ReceivedSchemaApplyObserverForTest;
use contextdb_engine::sync_types::{
    ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange, NaturalKey, RowChange,
};
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

#[derive(Default)]
struct Calls {
    caught_existing: AtomicUsize,
    prepared: AtomicUsize,
    missing: AtomicUsize,
}

fn register_callback(db: &Database, parent_id: i64, calls: Arc<Calls>) {
    db.register_transactional_trigger_callback("prepare_received", move |handle, context| {
        // A genuinely absent table remains an ordinary error that the callback
        // can catch; it must not cause endless discovery retries.
        match handle.execute_in_tx(context.tx, "INSERT INTO absent_table VALUES (1)", &empty()) {
            Err(Error::TableNotFound(table)) => {
                assert_eq!(table, "absent_table");
                calls.missing.fetch_add(1, Ordering::SeqCst);
            }
            other => panic!("a genuinely absent table keeps its lookup error: {other:?}"),
        }
        let result = handle.execute_in_tx(
            context.tx,
            "INSERT INTO received_derived (id, parent_id) VALUES ($id, $parent)",
            &HashMap::from([
                ("id".to_owned(), context.row_values["id"].clone()),
                ("parent".to_owned(), Value::Int64(parent_id)),
            ]),
        );
        match result {
            Err(Error::TableNotFound(table)) => {
                assert_eq!(table, "received_derived");
                calls.caught_existing.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            Ok(_) => {
                calls.prepared.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            Err(error) => Err(error),
        }
    })
    .unwrap();
    db.complete_initialization().unwrap();
}

fn fixture(path: &std::path::Path, parent_id: i64, calls: Arc<Calls>) -> Database {
    let db = Database::open(path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    for sql in [
        "CREATE TABLE received_source (id INTEGER PRIMARY KEY) SYNC TWO WAY",
        "CREATE TABLE received_parent (id INTEGER PRIMARY KEY) SYNC TWO WAY",
        "CREATE TABLE received_derived (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES received_parent(id)) SYNC TWO WAY",
        "CREATE TABLE unrelated (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY",
        "INSERT INTO received_parent VALUES (7)",
        "INSERT INTO unrelated VALUES (99, 'preserved')",
        "CREATE TRIGGER prepare_received ON received_source WHEN INSERT INCLUDING SYNC",
    ] {
        db.execute(sql, &empty()).unwrap();
    }
    register_callback(&db, parent_id, calls);
    db
}

fn unit() -> ChangeSet {
    ChangeSet {
        ddl: vec![DdlChange::AlterTable {
            name: "received_source".to_owned(),
            columns: vec![
                ("id".to_owned(), "INTEGER PRIMARY KEY".to_owned()),
                ("note".to_owned(), "TEXT".to_owned()),
            ],
            constraints: Vec::new(),
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: Vec::new(),
        }],
        ddl_lsn: vec![Lsn(9001)],
        rows: vec![RowChange {
            table: "received_source".to_owned(),
            natural_key: NaturalKey::single("id".to_owned(), Value::Int64(1)),
            values: HashMap::from([("id".to_owned(), Value::Int64(1))]),
            deleted: false,
            lsn: Lsn(9001),
            created_at: None,
        }],
        ..ChangeSet::default()
    }
}

fn apply(db: &Database) -> contextdb_core::Result<contextdb_engine::sync_types::ApplyResult> {
    db.apply_changes(
        unit(),
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
}

fn assert_control(db: &Database) {
    assert_eq!(
        db.execute("SELECT id, body FROM unrelated", &empty())
            .unwrap()
            .rows,
        vec![vec![Value::Int64(99), Value::Text("preserved".to_owned())]]
    );
    assert_eq!(
        db.execute("SELECT id FROM received_parent", &empty())
            .unwrap()
            .rows,
        vec![vec![Value::Int64(7)]]
    );
}

fn assert_absent(db: &Database, before: Lsn) {
    assert_eq!(
        db.current_lsn(),
        before,
        "a refused unit spends no commit LSN"
    );
    assert!(
        db.execute("SELECT id FROM received_source", &empty())
            .unwrap()
            .rows
            .is_empty()
    );
    assert!(
        db.execute("SELECT id FROM received_derived", &empty())
            .unwrap()
            .rows
            .is_empty()
    );
    assert!(
        db.table_meta("received_source")
            .unwrap()
            .columns
            .iter()
            .all(|column| column.name != "note")
    );
    let log = db
        .changes_since(before)
        .expect("read or construct the declared fixture result");
    assert!(log.rows.is_empty());
    assert!(log.ddl.is_empty());
    assert_control(db);
}

fn assert_complete(db: &Database, before: Lsn) {
    assert_eq!(
        db.execute("SELECT id, note FROM received_source", &empty())
            .unwrap()
            .rows,
        vec![vec![Value::Int64(1), Value::Null]]
    );
    assert_eq!(
        db.execute("SELECT id, parent_id FROM received_derived", &empty())
            .unwrap()
            .rows,
        vec![vec![Value::Int64(1), Value::Int64(7)]]
    );
    let log = db
        .changes_since(before)
        .expect("read or construct the declared fixture result");
    assert_eq!(
        log.rows.len(),
        2,
        "the source and derived write each commit exactly once"
    );
    assert_eq!(log.ddl.len(), 1);
    let lsn = log.rows[0].lsn;
    assert!(log.rows.iter().all(|row| row.lsn == lsn));
    assert_eq!(
        log.ddl_lsn,
        vec![lsn],
        "schema, source and callback share one commit"
    );
    assert_eq!(
        log.rows
            .iter()
            .map(|row| row.table.clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["received_source".to_owned(), "received_derived".to_owned()])
    );
    assert_control(db);
}

#[test]
fn a_caught_existing_table_lookup_commits_the_callback_write_once_with_its_source() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("receiver.db");
    let calls = Arc::new(Calls::default());
    let db = fixture(&path, 7, calls.clone());
    let before = db.current_lsn();
    let trace = ReceivedSchemaApplyObserverForTest::new();
    let scope = db.observe_received_schema_applies_for_test(&trace);
    apply(&db).expect("catching a private staging miss does not omit a callback write");
    drop(scope);
    assert!(
        calls.caught_existing.load(Ordering::SeqCst) >= 1,
        "the callback actually swallowed the existing-table miss"
    );
    assert!(calls.prepared.load(Ordering::SeqCst) >= 1);
    assert!(calls.missing.load(Ordering::SeqCst) >= 1);
    let observed = trace.observation().unwrap();
    let affected = BTreeSet::from([
        "received_source".to_owned(),
        "received_derived".to_owned(),
        "received_parent".to_owned(),
    ]);
    assert_eq!(
        observed.staged_table_names, affected,
        "discovery stages the callback target and its FK dependency only"
    );
    assert!(observed.durable_table_names.is_subset(&affected));
    assert_complete(&db, before);
    db.close().unwrap();
    let reopened = Database::open(&path).unwrap();
    assert_complete(&reopened, before);
    reopened.close().unwrap();
}

#[test]
fn interruption_before_durable_commit_hides_the_discovered_write_and_retry_commits_whole() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("receiver.db");
    let calls = Arc::new(Calls::default());
    let db = fixture(&path, 7, calls.clone());
    let before = db.current_lsn();
    db.fail_next_received_schema_durable_commit_for_test();
    let error = apply(&db).expect_err("interrupt the fully prepared Redb transaction");
    assert!(
        error
            .to_string()
            .contains("injected received-schema Redb pre-commit failure"),
        "{error:?}"
    );
    assert!(calls.caught_existing.load(Ordering::SeqCst) >= 1);
    assert!(
        calls.prepared.load(Ordering::SeqCst) >= 1,
        "the interruption happened after the discovered write was prepared"
    );
    assert_absent(&db, before);
    db.close().unwrap();
    let reopened = Database::open(&path).unwrap();
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_absent(&reopened, before);
    register_callback(&reopened, 7, calls);
    apply(&reopened).expect("retry of the identical interrupted unit commits whole");
    assert_complete(&reopened, before);
    reopened.close().unwrap();
}

#[test]
fn complete_validation_refusal_hides_a_callback_write_discovered_after_a_caught_lookup() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("receiver.db");
    let calls = Arc::new(Calls::default());
    let db = fixture(&path, 999, calls.clone());
    let before = db.current_lsn();
    let error =
        apply(&db).expect_err("the callback's missing FK parent refuses the complete transaction");
    assert!(
        matches!(error, Error::ForeignKeyViolation { .. }),
        "{error:?}"
    );
    assert!(calls.caught_existing.load(Ordering::SeqCst) >= 1);
    assert!(
        calls.prepared.load(Ordering::SeqCst) >= 1,
        "the callback prepared its discovered write before complete validation refused it"
    );
    assert_absent(&db, before);
    db.close().unwrap();
    let reopened = Database::open(&path).unwrap();
    assert_absent(&reopened, before);
    reopened.close().unwrap();
}
