use contextdb_core::{Error, Lsn, Result as DbResult, RowId, TxId, Value};
use contextdb_engine::database::Database;
use contextdb_engine::plugin::{CommitSource, DatabasePlugin, QueryOutcome};
use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange};
use contextdb_tx::WriteSet;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};

#[derive(Debug, Clone, PartialEq)]
struct RowSnapshot {
    table: String,
    row_id: RowId,
    values: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
struct RowDeleteSnapshot {
    table: String,
    row_id: RowId,
}

#[derive(Debug, Clone, PartialEq)]
struct VectorSnapshot {
    table: String,
    column: String,
    row_id: RowId,
    vector: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq)]
struct WriteSetSnapshot {
    relational_inserts: Vec<RowSnapshot>,
    relational_deletes: Vec<RowDeleteSnapshot>,
    adj_insert_count: usize,
    adj_delete_count: usize,
    vector_inserts: Vec<VectorSnapshot>,
    vector_delete_count: usize,
    vector_move_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ErrorVariantTag {
    UniqueViolation {
        table: String,
        column: String,
    },
    ForeignKeyViolation {
        child_table: String,
        child_columns: Vec<String>,
        parent_table: String,
        parent_columns: Vec<String>,
    },
    ConditionalUpdateConflict {
        count: u64,
    },
    PluginRejected {
        hook: String,
    },
    Other,
}

#[derive(Debug, Clone, PartialEq)]
struct FailedCommitObservation {
    source: CommitSource,
    error_variant: ErrorVariantTag,
    write_set: WriteSetSnapshot,
}

struct RecordingObserverPlugin {
    events: Arc<Mutex<Vec<FailedCommitObservation>>>,
}

impl RecordingObserverPlugin {
    fn new() -> (Self, Arc<Mutex<Vec<FailedCommitObservation>>>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                events: Arc::clone(&events),
            },
            events,
        )
    }
}

impl DatabasePlugin for RecordingObserverPlugin {
    fn commit_failed(&self, ws: &WriteSet, source: CommitSource, error: &Error) {
        self.events.lock().unwrap().push(FailedCommitObservation {
            source,
            error_variant: error_variant_tag(error),
            write_set: snapshot_write_set(ws),
        });
    }
}

#[derive(Clone)]
struct LifecycleRecords {
    pre_commit: Arc<Mutex<Vec<WriteSetSnapshot>>>,
    post_commit: Arc<Mutex<Vec<WriteSetSnapshot>>>,
    commit_failed: Arc<Mutex<Vec<WriteSetSnapshot>>>,
    commit_failed_count: Arc<AtomicU64>,
}

struct LifecycleObserverPlugin {
    records: LifecycleRecords,
}

impl LifecycleObserverPlugin {
    fn new() -> (Self, LifecycleRecords) {
        let records = LifecycleRecords {
            pre_commit: Arc::new(Mutex::new(Vec::new())),
            post_commit: Arc::new(Mutex::new(Vec::new())),
            commit_failed: Arc::new(Mutex::new(Vec::new())),
            commit_failed_count: Arc::new(AtomicU64::new(0)),
        };
        (
            Self {
                records: records.clone(),
            },
            records,
        )
    }
}

impl DatabasePlugin for LifecycleObserverPlugin {
    fn pre_commit(&self, ws: &WriteSet, _source: CommitSource) -> DbResult<()> {
        self.records
            .pre_commit
            .lock()
            .unwrap()
            .push(snapshot_write_set(ws));
        Ok(())
    }

    fn post_commit(&self, ws: &WriteSet, _source: CommitSource) {
        self.records
            .post_commit
            .lock()
            .unwrap()
            .push(snapshot_write_set(ws));
    }

    fn commit_failed(&self, ws: &WriteSet, _source: CommitSource, _error: &Error) {
        self.records
            .commit_failed_count
            .fetch_add(1, Ordering::SeqCst);
        self.records
            .commit_failed
            .lock()
            .unwrap()
            .push(snapshot_write_set(ws));
    }
}

struct PreCommitRejector;

impl DatabasePlugin for PreCommitRejector {
    fn pre_commit(&self, ws: &WriteSet, _source: CommitSource) -> DbResult<()> {
        if ws.is_empty() {
            Ok(())
        } else {
            Err(Error::PluginRejected {
                hook: "pre_commit".to_string(),
                reason: "test reject".to_string(),
            })
        }
    }
}

struct SideEffectObserverPlugin {
    counter: Arc<AtomicU64>,
}

impl SideEffectObserverPlugin {
    fn new(counter: Arc<AtomicU64>) -> Self {
        Self { counter }
    }
}

impl DatabasePlugin for SideEffectObserverPlugin {
    fn commit_failed(&self, _ws: &WriteSet, _source: CommitSource, _error: &Error) {
        self.counter.fetch_add(1, Ordering::SeqCst);
    }
}

struct CompositeObserverPlugin {
    plugins: Vec<Box<dyn DatabasePlugin>>,
}

impl CompositeObserverPlugin {
    fn new(plugins: Vec<Box<dyn DatabasePlugin>>) -> Self {
        Self { plugins }
    }
}

impl DatabasePlugin for CompositeObserverPlugin {
    fn pre_commit(&self, ws: &WriteSet, source: CommitSource) -> DbResult<()> {
        for plugin in &self.plugins {
            plugin.pre_commit(ws, source)?;
        }
        Ok(())
    }

    fn post_commit(&self, ws: &WriteSet, source: CommitSource) {
        for plugin in &self.plugins {
            plugin.post_commit(ws, source);
        }
    }

    fn commit_failed(&self, ws: &WriteSet, source: CommitSource, error: &Error) {
        for plugin in &self.plugins {
            plugin.commit_failed(ws, source, error);
        }
    }

    fn on_open(&self) -> DbResult<()> {
        for plugin in &self.plugins {
            plugin.on_open()?;
        }
        Ok(())
    }

    fn on_close(&self) -> DbResult<()> {
        for plugin in &self.plugins {
            plugin.on_close()?;
        }
        Ok(())
    }

    fn on_ddl(&self, change: &DdlChange) -> DbResult<()> {
        for plugin in &self.plugins {
            plugin.on_ddl(change)?;
        }
        Ok(())
    }

    fn on_query(&self, sql: &str) -> DbResult<()> {
        for plugin in &self.plugins {
            plugin.on_query(sql)?;
        }
        Ok(())
    }

    fn post_query(&self, sql: &str, duration: std::time::Duration, outcome: &QueryOutcome) {
        for plugin in &self.plugins {
            plugin.post_query(sql, duration, outcome);
        }
    }

    fn on_sync_push(&self, changeset: &mut ChangeSet) -> DbResult<()> {
        for plugin in &self.plugins {
            plugin.on_sync_push(changeset)?;
        }
        Ok(())
    }

    fn on_sync_pull(&self, changeset: &mut ChangeSet) -> DbResult<()> {
        for plugin in &self.plugins {
            plugin.on_sync_pull(changeset)?;
        }
        Ok(())
    }
}

fn snapshot_write_set(ws: &WriteSet) -> WriteSetSnapshot {
    WriteSetSnapshot {
        relational_inserts: ws
            .relational_inserts
            .iter()
            .map(|(table, row)| RowSnapshot {
                table: table.clone(),
                row_id: row.row_id,
                values: row.values.clone(),
            })
            .collect(),
        relational_deletes: ws
            .relational_deletes
            .iter()
            .map(|(table, row_id, _)| RowDeleteSnapshot {
                table: table.clone(),
                row_id: *row_id,
            })
            .collect(),
        adj_insert_count: ws.adj_inserts.len(),
        adj_delete_count: ws.adj_deletes.len(),
        vector_inserts: ws
            .vector_inserts
            .iter()
            .map(|entry| VectorSnapshot {
                table: entry.index.table.clone(),
                column: entry.index.column.clone(),
                row_id: entry.row_id,
                vector: entry.vector.clone(),
            })
            .collect(),
        vector_delete_count: ws.vector_deletes.len(),
        vector_move_count: ws.vector_moves.len(),
    }
}

fn error_variant_tag(error: &Error) -> ErrorVariantTag {
    match error {
        Error::UniqueViolation { table, column } => ErrorVariantTag::UniqueViolation {
            table: table.clone(),
            column: column.clone(),
        },
        Error::ForeignKeyViolation {
            child_table,
            child_columns,
            parent_table,
            parent_columns,
        } => ErrorVariantTag::ForeignKeyViolation {
            child_table: child_table.clone(),
            child_columns: child_columns.clone(),
            parent_table: parent_table.clone(),
            parent_columns: parent_columns.clone(),
        },
        Error::ConditionalUpdateConflict { count } => {
            ErrorVariantTag::ConditionalUpdateConflict { count: *count }
        }
        Error::PluginRejected { hook, .. } => {
            ErrorVariantTag::PluginRejected { hook: hook.clone() }
        }
        _ => ErrorVariantTag::Other,
    }
}

fn params(values: Vec<(&str, Value)>) -> HashMap<String, Value> {
    values
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

fn empty_params() -> HashMap<String, Value> {
    HashMap::new()
}

fn text(value: &str) -> Value {
    Value::Text(value.to_string())
}

fn int(value: i64) -> Value {
    Value::Int64(value)
}

fn exec_empty(db: &Database, sql: &str) {
    db.execute(sql, &empty_params()).unwrap();
}

fn create_handles_table(db: &Database) {
    exec_empty(
        db,
        "CREATE TABLE handles (id TEXT PRIMARY KEY, evidence_id TEXT NOT NULL)",
    );
}

fn open_recording_db() -> (Database, Arc<Mutex<Vec<FailedCommitObservation>>>) {
    let (plugin, events) = RecordingObserverPlugin::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    (db, events)
}

fn recorded(events: &Arc<Mutex<Vec<FailedCommitObservation>>>) -> Vec<FailedCommitObservation> {
    events.lock().unwrap().clone()
}

fn assert_count(db: &Database, table: &str, expected: i64) {
    let rows = db
        .execute(&format!("SELECT COUNT(*) FROM {table}"), &empty_params())
        .unwrap()
        .rows;
    assert_eq!(rows[0][0], Value::Int64(expected));
}

fn single_text_value(db: &Database, sql: &str) -> String {
    let rows = db.execute(sql, &empty_params()).unwrap().rows;
    assert_eq!(rows.len(), 1, "expected one row for {sql}, got {rows:?}");
    match &rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected text value for {sql}, got {other:?}"),
    }
}

fn assert_unique_violation(result: DbResult<()>, table: &str, column: &str) {
    assert!(
        matches!(
            result,
            Err(Error::UniqueViolation {
                table: ref actual_table,
                column: ref actual_column,
            }) if actual_table == table && actual_column == column
        ),
        "expected UniqueViolation on {table}.{column}, got {result:?}"
    );
}

fn assert_plugin_rejected(result: DbResult<impl std::fmt::Debug>, hook: &str) {
    assert!(
        matches!(
            result,
            Err(Error::PluginRejected {
                hook: ref actual_hook,
                ..
            }) if actual_hook == hook
        ),
        "expected PluginRejected at {hook}, got {result:?}"
    );
}

fn assert_fk_violation(
    result: DbResult<()>,
    child_table: &str,
    child_columns: &[&str],
    parent_table: &str,
    parent_columns: &[&str],
) {
    let child_columns = strings(child_columns);
    let parent_columns = strings(parent_columns);
    assert!(
        matches!(
            result,
            Err(Error::ForeignKeyViolation {
                child_table: ref actual_child_table,
                child_columns: ref actual_child_columns,
                parent_table: ref actual_parent_table,
                parent_columns: ref actual_parent_columns,
            }) if actual_child_table == child_table
                && actual_child_columns == &child_columns
                && actual_parent_table == parent_table
                && actual_parent_columns == &parent_columns
        ),
        "expected FK violation {child_table}({}) -> {parent_table}({}), got {result:?}",
        child_columns.join(", "),
        parent_columns.join(", ")
    );
}

fn assert_observation_error(obs: &FailedCommitObservation, expected: ErrorVariantTag) {
    assert_eq!(obs.error_variant, expected);
}

fn assert_no_graph_or_vector_tombstone_writes(ws: &WriteSetSnapshot) {
    assert_eq!(
        ws.adj_insert_count, 0,
        "failed commit WriteSet must not contain graph inserts"
    );
    assert_eq!(
        ws.adj_delete_count, 0,
        "failed commit WriteSet must not contain graph deletes"
    );
    assert_eq!(
        ws.vector_delete_count, 0,
        "failed commit WriteSet must not contain vector deletes"
    );
    assert_eq!(
        ws.vector_move_count, 0,
        "failed commit WriteSet must not contain vector moves"
    );
}

fn assert_exact_row(row: &RowSnapshot, table: &str, expected: &[(&str, Value)]) {
    assert_eq!(row.table, table);
    assert_eq!(
        row.values.len(),
        expected.len(),
        "expected exact column set for {table}, got {:?}",
        row.values
    );
    for (column, value) in expected {
        assert_eq!(
            row.values.get(*column),
            Some(value),
            "unexpected value for {table}.{column}"
        );
    }
}

fn assert_only_relational_insert_write_set(
    ws: &WriteSetSnapshot,
    table: &str,
    expected: &[(&str, Value)],
) -> RowId {
    assert_eq!(
        ws.relational_inserts.len(),
        1,
        "expected exactly one relational insert, got {:?}",
        ws.relational_inserts
    );
    assert!(
        ws.relational_deletes.is_empty(),
        "failed insert WriteSet must not contain relational deletes: {:?}",
        ws.relational_deletes
    );
    assert!(
        ws.vector_inserts.is_empty(),
        "failed insert WriteSet must not contain vector inserts: {:?}",
        ws.vector_inserts
    );
    assert_no_graph_or_vector_tombstone_writes(ws);
    let row = &ws.relational_inserts[0];
    assert_exact_row(row, table, expected);
    row.row_id
}

fn assert_only_relational_and_vector_insert_write_set(
    ws: &WriteSetSnapshot,
    table: &str,
    expected_row: &[(&str, Value)],
    vector_column: &str,
    expected_vector: &[f32],
) -> RowId {
    assert_eq!(
        ws.relational_inserts.len(),
        1,
        "expected exactly one relational insert, got {:?}",
        ws.relational_inserts
    );
    assert!(
        ws.relational_deletes.is_empty(),
        "failed insert WriteSet must not contain relational deletes: {:?}",
        ws.relational_deletes
    );
    assert_eq!(
        ws.vector_inserts.len(),
        1,
        "expected exactly one vector insert, got {:?}",
        ws.vector_inserts
    );
    assert_no_graph_or_vector_tombstone_writes(ws);

    let row = &ws.relational_inserts[0];
    assert_exact_row(row, table, expected_row);
    let vector = &ws.vector_inserts[0];
    assert_eq!(vector.table, table);
    assert_eq!(vector.column, vector_column);
    assert_eq!(vector.row_id, row.row_id);
    assert_eq!(vector.vector.as_slice(), expected_vector);
    row.row_id
}

fn assert_only_relational_update_write_set(
    ws: &WriteSetSnapshot,
    table: &str,
    expected_replacement: &[(&str, Value)],
) -> RowId {
    assert_eq!(
        ws.relational_inserts.len(),
        1,
        "expected exactly one replacement row, got {:?}",
        ws.relational_inserts
    );
    assert_eq!(
        ws.relational_deletes.len(),
        1,
        "expected exactly one old-row tombstone, got {:?}",
        ws.relational_deletes
    );
    assert!(
        ws.vector_inserts.is_empty(),
        "failed update WriteSet must not contain vector inserts: {:?}",
        ws.vector_inserts
    );
    assert_no_graph_or_vector_tombstone_writes(ws);

    let row = &ws.relational_inserts[0];
    assert_exact_row(row, table, expected_replacement);
    let tombstone = &ws.relational_deletes[0];
    assert_eq!(tombstone.table, table);
    assert_eq!(tombstone.row_id, row.row_id);
    row.row_id
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| value.to_string()).collect()
}

fn unique_tag(table: &str, column: &str) -> ErrorVariantTag {
    ErrorVariantTag::UniqueViolation {
        table: table.to_string(),
        column: column.to_string(),
    }
}

fn plugin_rejected_tag(hook: &str) -> ErrorVariantTag {
    ErrorVariantTag::PluginRejected {
        hook: hook.to_string(),
    }
}

fn fk_tag(
    child_table: &str,
    child_columns: &[&str],
    parent_table: &str,
    parent_columns: &[&str],
) -> ErrorVariantTag {
    ErrorVariantTag::ForeignKeyViolation {
        child_table: child_table.to_string(),
        child_columns: strings(child_columns),
        parent_table: parent_table.to_string(),
        parent_columns: strings(parent_columns),
    }
}

#[test]
fn commit_failed_observer_receives_loser_write_set_on_unique_violation() {
    let (db, events) = open_recording_db();
    create_handles_table(&db);

    let tx_a = db.begin_or_panic();
    let tx_b = db.begin_or_panic();
    db.execute_in_tx(
        tx_a,
        "INSERT INTO handles (id, evidence_id) VALUES ('H', 'E_A')",
        &empty_params(),
    )
    .unwrap();
    db.execute_in_tx(
        tx_b,
        "INSERT INTO handles (id, evidence_id) VALUES ('H', 'E_B')",
        &empty_params(),
    )
    .unwrap();

    db.commit(tx_a).expect("winner commits");
    let result = db.commit(tx_b);
    assert_unique_violation(result, "handles", "id");

    let events = recorded(&events);
    assert_eq!(events.len(), 1, "failed commit observer must fire once");
    let obs = &events[0];
    assert_eq!(obs.source, CommitSource::User);
    assert_observation_error(obs, unique_tag("handles", "id"));
    assert_only_relational_insert_write_set(
        &obs.write_set,
        "handles",
        &[("id", text("H")), ("evidence_id", text("E_B"))],
    );

    assert_count(&db, "handles", 1);
    assert_eq!(
        single_text_value(&db, "SELECT evidence_id FROM handles"),
        "E_A"
    );
}

#[test]
fn commit_failed_observer_receives_vector_and_relational_write_set_with_byte_equal_loser_vector() {
    let (db, events) = open_recording_db();
    exec_empty(
        &db,
        "CREATE TABLE evidence (id TEXT PRIMARY KEY, note TEXT UNIQUE NOT NULL, embedding VECTOR(4))",
    );
    let winner_vector = vec![1.0, 0.0, 0.0, 0.0];
    let loser_vector = vec![0.0, 1.0, 0.0, 0.0];

    let tx_a = db.begin_or_panic();
    let tx_b = db.begin_or_panic();
    db.execute_in_tx(
        tx_a,
        "INSERT INTO evidence (id, note, embedding) VALUES ('winner', 'conflict', $embedding)",
        &params(vec![("embedding", Value::Vector(winner_vector.clone()))]),
    )
    .unwrap();
    db.execute_in_tx(
        tx_b,
        "INSERT INTO evidence (id, note, embedding) VALUES ('loser', 'conflict', $embedding)",
        &params(vec![("embedding", Value::Vector(loser_vector.clone()))]),
    )
    .unwrap();

    db.commit(tx_a).expect("winner commits");
    let result = db.commit(tx_b);
    assert_unique_violation(result, "evidence", "note");

    let events = recorded(&events);
    assert_eq!(events.len(), 1, "failed commit observer must fire once");
    let obs = &events[0];
    assert_eq!(obs.source, CommitSource::User);
    assert_observation_error(obs, unique_tag("evidence", "note"));
    assert_only_relational_and_vector_insert_write_set(
        &obs.write_set,
        "evidence",
        &[
            ("id", text("loser")),
            ("note", text("conflict")),
            ("embedding", Value::Vector(loser_vector.clone())),
        ],
        "embedding",
        &loser_vector,
    );

    let rows = db
        .execute(
            "SELECT id, embedding FROM evidence ORDER BY embedding <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(loser_vector.clone()))]),
        )
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], text("winner"));
    assert_eq!(rows[0][1], Value::Vector(winner_vector));
    assert_ne!(rows[0][0], text("loser"));
}

#[test]
fn commit_failed_observer_does_not_change_caller_error_or_outcome() {
    let (recording, events) = RecordingObserverPlugin::new();
    let side_effect_counter = Arc::new(AtomicU64::new(0));
    let plugin = CompositeObserverPlugin::new(vec![
        Box::new(recording),
        Box::new(SideEffectObserverPlugin::new(Arc::clone(
            &side_effect_counter,
        ))),
    ]);
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    create_handles_table(&db);

    let tx_a = db.begin_or_panic();
    let tx_b = db.begin_or_panic();
    db.execute_in_tx(
        tx_a,
        "INSERT INTO handles (id, evidence_id) VALUES ('H', 'E_A')",
        &empty_params(),
    )
    .unwrap();
    db.execute_in_tx(
        tx_b,
        "INSERT INTO handles (id, evidence_id) VALUES ('H', 'E_B')",
        &empty_params(),
    )
    .unwrap();
    db.commit(tx_a).expect("winner commits");

    let commit_result_b = db.commit(tx_b);
    assert_unique_violation(commit_result_b, "handles", "id");
    assert_eq!(
        side_effect_counter.load(Ordering::SeqCst),
        1,
        "observer side effect must run exactly once"
    );
    let events = recorded(&events);
    assert_eq!(events.len(), 1, "failed commit observer must fire once");
    assert_only_relational_insert_write_set(
        &events[0].write_set,
        "handles",
        &[("id", text("H")), ("evidence_id", text("E_B"))],
    );
    assert_count(&db, "handles", 1);
}

#[test]
fn pre_commit_rejection_on_non_empty_write_set_invokes_commit_failed_observer_exactly_once() {
    let (recording, events) = RecordingObserverPlugin::new();
    let plugin =
        CompositeObserverPlugin::new(vec![Box::new(recording), Box::new(PreCommitRejector)]);
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    exec_empty(&db, "CREATE TABLE items (id TEXT PRIMARY KEY)");

    let tx = db.begin_or_panic();
    db.execute_in_tx(tx, "INSERT INTO items (id) VALUES ('A')", &empty_params())
        .unwrap();
    let result = db.commit(tx);
    assert_plugin_rejected(result, "pre_commit");

    let events = recorded(&events);
    assert_eq!(events.len(), 1, "failed commit observer must fire once");
    let obs = &events[0];
    assert_eq!(obs.source, CommitSource::User);
    assert_observation_error(obs, plugin_rejected_tag("pre_commit"));
    assert_only_relational_insert_write_set(&obs.write_set, "items", &[("id", text("A"))]);
    assert_count(&db, "items", 0);
}

#[test]
fn commit_failed_observer_fires_on_auto_commit_path() {
    let (recording, events) = RecordingObserverPlugin::new();
    let plugin =
        CompositeObserverPlugin::new(vec![Box::new(recording), Box::new(PreCommitRejector)]);
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    create_handles_table(&db);

    let result = db.execute(
        "INSERT INTO handles (id, evidence_id) VALUES ('H', 'E_AUTO')",
        &empty_params(),
    );
    assert_plugin_rejected(result, "pre_commit");

    let events = recorded(&events);
    assert_eq!(events.len(), 1, "failed commit observer must fire once");
    let obs = &events[0];
    assert_eq!(obs.source, CommitSource::AutoCommit);
    assert_observation_error(obs, plugin_rejected_tag("pre_commit"));
    assert_only_relational_insert_write_set(
        &obs.write_set,
        "handles",
        &[("id", text("H")), ("evidence_id", text("E_AUTO"))],
    );
    assert_count(&db, "handles", 0);
}

#[test]
fn commit_failed_observer_fires_on_commit_time_foreign_key_violation() {
    let (db, events) = open_recording_db();
    exec_empty(&db, "CREATE TABLE parent (id TEXT PRIMARY KEY)");
    exec_empty(
        &db,
        "CREATE TABLE child (id TEXT PRIMARY KEY, parent_id TEXT NOT NULL REFERENCES parent(id))",
    );
    exec_empty(&db, "INSERT INTO parent (id) VALUES ('P1')");

    let tx_delete_parent = db.begin_or_panic();
    let tx_insert_child = db.begin_or_panic();
    db.execute_in_tx(
        tx_delete_parent,
        "DELETE FROM parent WHERE id = 'P1'",
        &empty_params(),
    )
    .unwrap();
    db.execute_in_tx(
        tx_insert_child,
        "INSERT INTO child (id, parent_id) VALUES ('C1', 'P1')",
        &empty_params(),
    )
    .unwrap();

    db.commit(tx_delete_parent).expect("parent delete commits");
    let result = db.commit(tx_insert_child);
    assert_fk_violation(result, "child", &["parent_id"], "parent", &["id"]);

    let events = recorded(&events);
    assert_eq!(events.len(), 1, "failed commit observer must fire once");
    let obs = &events[0];
    assert_eq!(obs.source, CommitSource::User);
    assert_observation_error(obs, fk_tag("child", &["parent_id"], "parent", &["id"]));
    assert_only_relational_insert_write_set(
        &obs.write_set,
        "child",
        &[("id", text("C1")), ("parent_id", text("P1"))],
    );
}

#[test]
fn commit_failed_observer_does_not_fire_on_successful_commit_and_pre_post_hooks_fire_once() {
    let (plugin, records) = LifecycleObserverPlugin::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    exec_empty(&db, "CREATE TABLE items (id TEXT PRIMARY KEY)");

    db.execute("INSERT INTO items (id) VALUES ('A')", &empty_params())
        .unwrap();
    let tx = db.begin_or_panic();
    db.execute_in_tx(tx, "INSERT INTO items (id) VALUES ('B')", &empty_params())
        .unwrap();
    db.commit(tx).expect("explicit insert commits");

    assert_eq!(records.commit_failed.lock().unwrap().len(), 0);
    assert!(records.commit_failed.lock().unwrap().is_empty());
    assert_eq!(records.commit_failed_count.load(Ordering::SeqCst), 0);

    let pre = records.pre_commit.lock().unwrap().clone();
    let post = records.post_commit.lock().unwrap().clone();
    assert_eq!(pre.len(), 2, "pre_commit must fire once per success");
    assert_eq!(post.len(), 2, "post_commit must fire once per success");
    assert_only_relational_insert_write_set(&pre[0], "items", &[("id", text("A"))]);
    assert_only_relational_insert_write_set(&post[0], "items", &[("id", text("A"))]);
    assert_only_relational_insert_write_set(&pre[1], "items", &[("id", text("B"))]);
    assert_only_relational_insert_write_set(&post[1], "items", &[("id", text("B"))]);
}

#[test]
fn commit_failed_observer_does_not_fire_on_tx_not_found_failure() {
    let (plugin, records) = LifecycleObserverPlugin::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    exec_empty(&db, "CREATE TABLE items (id TEXT PRIMARY KEY)");

    let result = db.commit(TxId(999_999));
    assert!(result.is_err(), "invalid tx commit must fail");
    assert!(records.commit_failed.lock().unwrap().is_empty());
    assert_eq!(records.commit_failed_count.load(Ordering::SeqCst), 0);
}

#[test]
fn commit_failed_observer_fires_on_pre_commit_rejection_during_sync_pull_apply() {
    let db_a = Database::open_memory();
    exec_empty(&db_a, "CREATE TABLE items (id TEXT PRIMARY KEY)");
    exec_empty(&db_a, "INSERT INTO items (id) VALUES ('A')");
    let mut changes = db_a.changes_since(Lsn(0));
    changes.ddl.clear();
    changes.ddl_lsn.clear();
    assert_eq!(changes.rows.len(), 1, "sender changeset must carry one row");
    assert!(changes.edges.is_empty());
    assert!(changes.vectors.is_empty());

    let (recording, events) = RecordingObserverPlugin::new();
    let plugin =
        CompositeObserverPlugin::new(vec![Box::new(recording), Box::new(PreCommitRejector)]);
    let db_b = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    exec_empty(&db_b, "CREATE TABLE items (id TEXT PRIMARY KEY)");

    let result = db_b.apply_changes(
        changes,
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    );
    assert_plugin_rejected(result, "pre_commit");

    let events = recorded(&events);
    assert_eq!(events.len(), 1, "failed commit observer must fire once");
    let obs = &events[0];
    assert_eq!(obs.source, CommitSource::SyncPull);
    assert_observation_error(obs, plugin_rejected_tag("pre_commit"));
    assert_only_relational_insert_write_set(&obs.write_set, "items", &[("id", text("A"))]);
    assert_count(&db_b, "items", 0);
}

#[test]
fn commit_failed_observer_fires_on_composite_foreign_key_violation() {
    let (db, events) = open_recording_db();
    exec_empty(
        &db,
        "CREATE TABLE parent (id_a TEXT, id_b TEXT, payload TEXT, UNIQUE(id_a, id_b))",
    );
    exec_empty(
        &db,
        "CREATE TABLE child (id TEXT PRIMARY KEY, parent_a TEXT NOT NULL, parent_b TEXT NOT NULL, FOREIGN KEY (parent_a, parent_b) REFERENCES parent(id_a, id_b))",
    );
    exec_empty(
        &db,
        "INSERT INTO parent (id_a, id_b, payload) VALUES ('A', 'B', 'p')",
    );

    let tx_delete_parent = db.begin_or_panic();
    let tx_insert_child = db.begin_or_panic();
    db.execute_in_tx(
        tx_delete_parent,
        "DELETE FROM parent WHERE id_a = 'A' AND id_b = 'B'",
        &empty_params(),
    )
    .unwrap();
    db.execute_in_tx(
        tx_insert_child,
        "INSERT INTO child (id, parent_a, parent_b) VALUES ('C1', 'A', 'B')",
        &empty_params(),
    )
    .unwrap();

    db.commit(tx_delete_parent).expect("parent delete commits");
    let result = db.commit(tx_insert_child);
    assert_fk_violation(
        result,
        "child",
        &["parent_a", "parent_b"],
        "parent",
        &["id_a", "id_b"],
    );

    let events = recorded(&events);
    assert_eq!(events.len(), 1, "failed commit observer must fire once");
    let obs = &events[0];
    assert_eq!(obs.source, CommitSource::User);
    assert_observation_error(
        obs,
        fk_tag(
            "child",
            &["parent_a", "parent_b"],
            "parent",
            &["id_a", "id_b"],
        ),
    );
    assert_only_relational_insert_write_set(
        &obs.write_set,
        "child",
        &[
            ("id", text("C1")),
            ("parent_a", text("A")),
            ("parent_b", text("B")),
        ],
    );
}

#[test]
fn commit_failed_observer_fires_on_conditional_update_revalidation_conflict() {
    let (db, events) = open_recording_db();
    exec_empty(
        &db,
        "CREATE TABLE counters (id TEXT PRIMARY KEY, value INTEGER NOT NULL)",
    );
    exec_empty(&db, "INSERT INTO counters (id, value) VALUES ('K', 0)");

    let guarded_tx = db.begin_or_panic();
    let guarded = db
        .guard_row_conditions_in_tx(
            guarded_tx,
            "counters",
            "id",
            &text("K"),
            &[("value".to_string(), int(0))],
        )
        .unwrap();
    assert!(guarded, "row guard must be registered");
    db.execute_in_tx(
        guarded_tx,
        "UPDATE counters SET value = 2 WHERE id = 'K'",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "UPDATE counters SET value = 1 WHERE id = 'K'",
        &empty_params(),
    )
    .unwrap();

    let result = db.commit(guarded_tx);
    assert!(
        matches!(result, Err(Error::ConditionalUpdateConflict { count: 1 })),
        "expected ConditionalUpdateConflict count=1, got {result:?}"
    );

    let events = recorded(&events);
    assert_eq!(events.len(), 1, "failed commit observer must fire once");
    let obs = &events[0];
    assert_eq!(obs.source, CommitSource::User);
    assert_observation_error(obs, ErrorVariantTag::ConditionalUpdateConflict { count: 1 });
    assert_only_relational_update_write_set(
        &obs.write_set,
        "counters",
        &[("id", text("K")), ("value", int(2))],
    );
}

#[test]
fn commit_failed_observer_handles_concurrent_failed_commits_with_per_writer_identity() {
    let (db, events) = open_recording_db();
    create_handles_table(&db);
    let barrier = Arc::new(Barrier::new(10));

    let results = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for writer_id in 0..10 {
            let barrier = Arc::clone(&barrier);
            let db = &db;
            handles.push(scope.spawn(move || {
                let evidence_id = format!("E_{writer_id}");
                let tx = db.begin_or_panic();
                db.execute_in_tx(
                    tx,
                    "INSERT INTO handles (id, evidence_id) VALUES ('H', $evidence_id)",
                    &params(vec![("evidence_id", text(&evidence_id))]),
                )
                .unwrap();
                barrier.wait();
                (writer_id, evidence_id, db.commit(tx))
            }));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });

    let mut winner = None;
    let mut loser_ids = HashSet::new();
    for (writer_id, evidence_id, result) in results {
        match result {
            Ok(()) => {
                assert!(
                    winner.replace((writer_id, evidence_id.clone())).is_none(),
                    "only one writer may win"
                );
            }
            Err(Error::UniqueViolation { table, column })
                if table == "handles" && column == "id" =>
            {
                loser_ids.insert(evidence_id);
            }
            other => panic!("unexpected commit result from writer {writer_id}: {other:?}"),
        }
    }
    let (_, winner_evidence_id) = winner.expect("one writer must win");
    assert_eq!(loser_ids.len(), 9, "nine writers must lose");

    let events = recorded(&events);
    assert_eq!(
        events.len(),
        9,
        "failed commit observer must fire once per losing writer"
    );
    let mut observed_loser_ids = HashSet::new();
    for obs in &events {
        assert_eq!(obs.source, CommitSource::User);
        assert_observation_error(obs, unique_tag("handles", "id"));
        assert_eq!(
            obs.write_set.relational_inserts.len(),
            1,
            "each failed concurrent WriteSet must carry exactly one row"
        );
        assert!(obs.write_set.relational_deletes.is_empty());
        assert!(obs.write_set.vector_inserts.is_empty());
        assert_no_graph_or_vector_tombstone_writes(&obs.write_set);
        let row = &obs.write_set.relational_inserts[0];
        assert_eq!(row.table, "handles");
        assert_eq!(row.values.len(), 2);
        assert_eq!(row.values.get("id"), Some(&text("H")));
        let Some(Value::Text(evidence_id)) = row.values.get("evidence_id") else {
            panic!("handles insert must carry evidence_id, got {row:?}");
        };
        observed_loser_ids.insert(evidence_id.clone());
    }
    assert_eq!(observed_loser_ids, loser_ids);
    assert!(!observed_loser_ids.contains(&winner_evidence_id));
    assert_count(&db, "handles", 1);
    assert_eq!(
        single_text_value(&db, "SELECT evidence_id FROM handles"),
        winner_evidence_id
    );
}
