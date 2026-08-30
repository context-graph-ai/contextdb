use contextdb_core::{Error, NodeId, Result as DbResult, RowId, Value, VectorIndexRef};
use contextdb_engine::cli_render::{render_explain, render_table_meta};
use contextdb_engine::plugin::{CommitSource, DatabasePlugin};
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use contextdb_engine::{Database, QueryResult, QueryTrace};
use contextdb_tx::WriteSet;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
struct RowSnapshot {
    table: String,
    row_id: RowId,
    values: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
struct VectorSnapshot {
    table: String,
    column: String,
    row_id: RowId,
    vector: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq)]
struct GraphSnapshot {
    source: NodeId,
    target: NodeId,
    edge_type: String,
    properties: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Default)]
struct WriteSetSnapshot {
    relational_inserts: Vec<RowSnapshot>,
    relational_deletes: Vec<(String, RowId)>,
    adj_inserts: Vec<GraphSnapshot>,
    adj_delete_count: usize,
    vector_inserts: Vec<VectorSnapshot>,
    vector_delete_count: usize,
    vector_move_count: usize,
}

#[derive(Clone, Default)]
struct LifecycleRecords {
    pre_commit: Arc<Mutex<Vec<WriteSetSnapshot>>>,
    post_commit: Arc<Mutex<Vec<WriteSetSnapshot>>>,
    commit_failed: Arc<Mutex<Vec<(WriteSetSnapshot, String)>>>,
    order: Arc<Mutex<Vec<&'static str>>>,
}

struct LifecycleObserverPlugin {
    records: LifecycleRecords,
}

impl LifecycleObserverPlugin {
    fn new() -> (Self, LifecycleRecords) {
        let records = LifecycleRecords::default();
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
        self.records.order.lock().unwrap().push("pre");
        self.records
            .pre_commit
            .lock()
            .unwrap()
            .push(snapshot_write_set(ws));
        Ok(())
    }

    fn post_commit(&self, ws: &WriteSet, _source: CommitSource) {
        self.records.order.lock().unwrap().push("post");
        self.records
            .post_commit
            .lock()
            .unwrap()
            .push(snapshot_write_set(ws));
    }

    fn commit_failed(&self, ws: &WriteSet, _source: CommitSource, error: &Error) {
        self.records.order.lock().unwrap().push("failed");
        self.records
            .commit_failed
            .lock()
            .unwrap()
            .push((snapshot_write_set(ws), error_tag(error)));
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
            .map(|(table, row_id, _)| (table.clone(), *row_id))
            .collect(),
        adj_inserts: ws
            .adj_inserts
            .iter()
            .map(|entry| GraphSnapshot {
                source: entry.source,
                target: entry.target,
                edge_type: entry.edge_type.clone(),
                properties: entry.properties.clone(),
            })
            .collect(),
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

fn error_tag(error: &Error) -> String {
    match error {
        Error::UniqueViolation { table, column } => {
            format!("unique:{table}.{column}")
        }
        Error::ForeignKeyViolation {
            child_table,
            child_columns,
            parent_table,
            parent_columns,
        } => format!(
            "fk:{}({})->{}({})",
            child_table,
            child_columns.join(","),
            parent_table,
            parent_columns.join(",")
        ),
        other => format!("{other:?}"),
    }
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn uuid(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

fn exec(db: &Database, sql: &str) -> QueryResult {
    db.execute(sql, &empty())
        .unwrap_or_else(|err| panic!("{sql}: {err:?}"))
}

fn insert_uuid_row(db: &Database, table: &str, id: Uuid, col: i64) -> QueryResult {
    db.execute(
        &format!("INSERT INTO {table} (id, col) VALUES ($id, $col)"),
        &params(vec![("id", Value::Uuid(id)), ("col", Value::Int64(col))]),
    )
    .unwrap()
}

fn count_rows(db: &Database, table: &str) -> i64 {
    let rows = db
        .execute(&format!("SELECT COUNT(*) FROM {table}"), &empty())
        .unwrap()
        .rows;
    match rows.first().and_then(|row| row.first()) {
        Some(Value::Int64(n)) => *n,
        other => panic!("expected COUNT(*) Int64, got {other:?}"),
    }
}

fn pushed(result: &QueryResult) -> Vec<&str> {
    result
        .trace
        .predicates_pushed
        .iter()
        .map(|column| column.as_ref())
        .collect()
}

fn considered(trace: &QueryTrace) -> Vec<(&str, &str)> {
    trace
        .indexes_considered
        .iter()
        .map(|candidate| (candidate.name.as_str(), candidate.rejected_reason.as_ref()))
        .collect()
}

fn assert_fk_violation(result: DbResult<QueryResult>, child: &str, parent: &str) {
    assert!(
        matches!(
            result,
            Err(Error::ForeignKeyViolation {
                child_table: ref actual_child_table,
                parent_table: ref actual_parent_table,
                ..
            }) if actual_child_table == child && actual_parent_table == parent
        ),
        "expected FK violation {child}->{parent}, got {result:?}"
    );
}

fn assert_unique_commit(result: DbResult<()>, table: &str, column: &str) {
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

fn commit_duplicate_unique_pair(db: &Database, first_id: u128, second_id: u128, col: i64) {
    let tx1 = db.begin().unwrap();
    db.insert_row(
        tx1,
        "t",
        HashMap::from([
            ("id".to_string(), Value::Uuid(uuid(first_id))),
            ("col".to_string(), Value::Int64(col)),
        ]),
    )
    .unwrap();
    let tx2 = db.begin().unwrap();
    db.insert_row(
        tx2,
        "t",
        HashMap::from([
            ("id".to_string(), Value::Uuid(uuid(second_id))),
            ("col".to_string(), Value::Int64(col)),
        ]),
    )
    .unwrap();
    db.commit(tx1).unwrap();
    assert_unique_commit(db.commit(tx2), "t", "col");
}

fn commit_duplicate_composite_pair(
    db: &Database,
    first_id: u128,
    second_id: u128,
    col1: i64,
    col2: i64,
) {
    let tx1 = db.begin().unwrap();
    db.insert_row(
        tx1,
        "t",
        HashMap::from([
            ("id".to_string(), Value::Uuid(uuid(first_id))),
            ("col1".to_string(), Value::Int64(col1)),
            ("col2".to_string(), Value::Int64(col2)),
        ]),
    )
    .unwrap();
    let tx2 = db.begin().unwrap();
    db.insert_row(
        tx2,
        "t",
        HashMap::from([
            ("id".to_string(), Value::Uuid(uuid(second_id))),
            ("col1".to_string(), Value::Int64(col1)),
            ("col2".to_string(), Value::Int64(col2)),
        ]),
    )
    .unwrap();
    db.commit(tx1).unwrap();
    assert_unique_commit(db.commit(tx2), "t", "col1");
}

fn assert_exact_row(row: &RowSnapshot, table: &str, expected: &[(&str, Value)]) {
    assert_eq!(row.table, table);
    assert_eq!(
        row.values.len(),
        expected.len(),
        "row values: {:?}",
        row.values
    );
    for (column, value) in expected {
        assert_eq!(
            row.values.get(*column),
            Some(value),
            "wrong value for {table}.{column}"
        );
    }
}

fn setup_pk_col_table(db: &Database, table: &str, extra: &str) {
    exec(
        db,
        &format!("CREATE TABLE {table} (id UUID PRIMARY KEY, col INTEGER{extra})"),
    );
}

fn setup_sibling_with_indexes(db: &Database) {
    exec(
        db,
        "CREATE TABLE sib (id UUID PRIMARY KEY, x INTEGER, y INTEGER)",
    );
    exec(db, "CREATE INDEX sib_x ON sib (x)");
    exec(db, "CREATE INDEX sib_y ON sib (y)");
    exec(db, "CREATE INDEX sib_xy ON sib (x, y)");
}

fn setup_file_db_for_open(
    path_name: &str,
    sibling_extra_indexes: bool,
    sibling_rows: usize,
) -> TempDir {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join(path_name);
    {
        let db = Database::open(&path).unwrap();
        exec(&db, "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER)");
        exec(&db, "CREATE INDEX idx_a ON t (col)");
        exec(
            &db,
            "CREATE TABLE sib (id UUID PRIMARY KEY, x INTEGER, y INTEGER)",
        );
        if sibling_extra_indexes {
            exec(&db, "CREATE INDEX sib_x ON sib (x)");
            exec(&db, "CREATE INDEX sib_y ON sib (y)");
            exec(&db, "CREATE INDEX sib_xy ON sib (x, y)");
        }
        for i in 0..3 {
            insert_uuid_row(&db, "t", uuid(0x1000 + i), i as i64);
        }
        for i in 0..sibling_rows {
            db.execute(
                "INSERT INTO sib (id, x, y) VALUES ($id, $x, $y)",
                &params(vec![
                    ("id", Value::Uuid(uuid(0x2000 + i as u128))),
                    ("x", Value::Int64(i as i64)),
                    ("y", Value::Int64(i as i64)),
                ]),
            )
            .unwrap();
        }
        db.close().unwrap();
    }
    tmp
}

fn seed_t_for_identity(db: &Database) -> Vec<Uuid> {
    exec(
        db,
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER, col2 INTEGER)",
    );
    exec(db, "CREATE INDEX idx_a ON t (col)");
    let ids = (1..=6).map(uuid).collect::<Vec<_>>();
    for (i, id) in ids.iter().copied().enumerate() {
        db.execute(
            "INSERT INTO t (id, col, col2) VALUES ($id, $col, $col2)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("col", Value::Int64((i + 1) as i64)),
                ("col2", Value::Int64(((i + 1) * 10) as i64)),
            ]),
        )
        .unwrap();
    }
    ids
}

#[test]
fn ssh_c3_maint_isolated() {
    let db = Database::open_memory();
    exec(
        &db,
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER, col2 INTEGER)",
    );
    exec(&db, "CREATE INDEX idx_a ON t (col)");
    exec(&db, "CREATE INDEX idx_b ON t (col2)");
    setup_sibling_with_indexes(&db);

    db.__reset_index_maintenance_visits();
    db.execute(
        "INSERT INTO t (id, col, col2) VALUES ($id, 1, 2)",
        &params(vec![("id", Value::Uuid(uuid(1)))]),
    )
    .unwrap();
    assert_eq!(db.__index_maintenance_visits(), 3);
}

#[test]
fn ssh_c3_maint_sibling_scaling() {
    let db = Database::open_memory();
    setup_pk_col_table(&db, "t", "");
    exec(&db, "CREATE INDEX idx_a ON t (col)");
    exec(
        &db,
        "CREATE TABLE sib (id UUID PRIMARY KEY, x INTEGER, y INTEGER)",
    );

    db.__reset_index_maintenance_visits();
    insert_uuid_row(&db, "t", uuid(10), 1);
    let v1 = db.__index_maintenance_visits();

    exec(&db, "CREATE INDEX sib_a ON sib (x)");
    exec(&db, "CREATE INDEX sib_b ON sib (y)");
    exec(&db, "CREATE INDEX sib_c ON sib (x, y)");
    db.__reset_index_maintenance_visits();
    insert_uuid_row(&db, "t", uuid(11), 2);
    let v2 = db.__index_maintenance_visits();

    assert_eq!(v1, 2);
    assert_eq!(v2, 2);
    assert_eq!(v1, v2);
}

#[test]
fn ssh_c3_maint_delete_isolated() {
    let db = Database::open_memory();
    setup_pk_col_table(&db, "t", "");
    exec(&db, "CREATE INDEX idx_a ON t (col)");
    setup_sibling_with_indexes(&db);
    insert_uuid_row(&db, "t", uuid(20), 1);

    db.__reset_index_maintenance_visits();
    exec(&db, "DELETE FROM t WHERE col = 1");
    assert_eq!(db.__index_maintenance_visits(), 2);
}

#[test]
fn ssh_c3_maint_multirow_linear() {
    let db = Database::open_memory();
    setup_pk_col_table(&db, "t", "");
    exec(&db, "CREATE INDEX idx_a ON t (col)");
    setup_sibling_with_indexes(&db);

    db.__reset_index_maintenance_visits();
    db.execute(
        "INSERT INTO t (id, col) VALUES ($a, 1), ($b, 2), ($c, 3)",
        &params(vec![
            ("a", Value::Uuid(uuid(30))),
            ("b", Value::Uuid(uuid(31))),
            ("c", Value::Uuid(uuid(32))),
        ]),
    )
    .unwrap();
    assert_eq!(db.__index_maintenance_visits(), 6);
}

#[test]
fn ssh_c3_maint_counter_resets() {
    let db = Database::open_memory();
    setup_pk_col_table(&db, "t", "");
    exec(&db, "CREATE INDEX idx_a ON t (col)");

    db.__reset_index_maintenance_visits();
    assert_eq!(db.__index_maintenance_visits(), 0);
    insert_uuid_row(&db, "t", uuid(40), 1);
    assert_eq!(db.__index_maintenance_visits(), 2);

    db.__reset_index_maintenance_visits();
    assert_eq!(db.__index_maintenance_visits(), 0);
    db.execute(
        "INSERT INTO t (id, col) VALUES ($a, 2), ($b, 3)",
        &params(vec![
            ("a", Value::Uuid(uuid(41))),
            ("b", Value::Uuid(uuid(42))),
        ]),
    )
    .unwrap();
    assert_eq!(db.__index_maintenance_visits(), 4);
}

#[test]
fn ssh_c3_maint_no_index_table() {
    let db = Database::open_memory();
    setup_pk_col_table(&db, "t", "");
    setup_sibling_with_indexes(&db);

    db.__reset_index_maintenance_visits();
    insert_uuid_row(&db, "t", uuid(50), 1);
    assert_eq!(db.__index_maintenance_visits(), 1);
}

#[test]
fn ssh_c3_maint_counter_not_hardcoded() {
    let db = Database::open_memory();
    setup_pk_col_table(&db, "ta", "");
    exec(
        &db,
        "CREATE TABLE tb (id UUID PRIMARY KEY, col INTEGER, col2 INTEGER)",
    );
    exec(&db, "CREATE INDEX b1 ON tb (col)");
    exec(&db, "CREATE INDEX b2 ON tb (col2)");

    db.__reset_index_maintenance_visits();
    insert_uuid_row(&db, "ta", uuid(60), 1);
    assert_eq!(db.__index_maintenance_visits(), 1);

    db.__reset_index_maintenance_visits();
    db.execute(
        "INSERT INTO tb (id, col, col2) VALUES ($id, 1, 2)",
        &params(vec![("id", Value::Uuid(uuid(61)))]),
    )
    .unwrap();
    assert_eq!(db.__index_maintenance_visits(), 3);
}

#[test]
fn ssh_c3_upsert_counter_honest() {
    let db = Database::open_memory();
    exec(
        &db,
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER UNIQUE, col2 INTEGER)",
    );
    setup_sibling_with_indexes(&db);
    db.execute(
        "INSERT INTO t (id, col, col2) VALUES ($id, 1, 0)",
        &params(vec![("id", Value::Uuid(uuid(70)))]),
    )
    .unwrap();

    db.__reset_index_maintenance_visits();
    db.execute(
        "INSERT INTO t (id, col, col2) VALUES ($id, 1, 9) ON CONFLICT (col) DO UPDATE SET col2 = 9",
        &params(vec![("id", Value::Uuid(uuid(71)))]),
    )
    .unwrap();
    assert_eq!(db.__index_maintenance_visits(), 0);
}

#[test]
fn ssh_c3_state_machine_pk_insert_miss_does_not_scan_existing_rows() {
    let db = Database::open_memory();
    exec(
        &db,
        "CREATE TABLE decisions (id UUID PRIMARY KEY, status TEXT NOT NULL DEFAULT 'active') STATE MACHINE (status: active -> [archived])",
    );
    // Seed size shrunk 1000 -> 100: the `scan_rows_touched == 0` contract is a
    // planner-path check (index probe vs full scan). Index selection is purely
    // schema-based — `indexed_committed_point_lookup` (contextdb-relational
    // mem.rs) hash-probes the PK/unique index at any table size, and the scan
    // counter only bumps on a full `scan_with_tx`. There is no cardinality
    // threshold, so 100 rows catches a scan-fallback regression identically to
    // 1000 while keeping the per-commit run cheap.
    for id in 0..100 {
        db.execute(
            "INSERT INTO decisions (id, status) VALUES ($id, 'active')",
            &params(vec![("id", Value::Uuid(uuid(5_000 + id)))]),
        )
        .unwrap();
    }

    db.__reset_relational_scan_rows_touched();
    for id in 0..100 {
        db.execute(
            "INSERT INTO decisions (id, status) VALUES ($id, 'active')",
            &params(vec![("id", Value::Uuid(uuid(6_000 + id)))]),
        )
        .unwrap();
    }
    assert_eq!(
        db.__relational_scan_rows_touched(),
        0,
        "state-machine inserts of fresh primary keys must treat indexed misses as terminal"
    );
}

#[test]
fn ssh_c3_composite_on_conflict_uses_tuple_index_without_scan() {
    let db = Database::open_memory();
    exec(
        &db,
        "CREATE TABLE memories (id UUID PRIMARY KEY, source_id TEXT NOT NULL, content_hash TEXT NOT NULL, label TEXT, UNIQUE (source_id, content_hash))",
    );
    // Seed size shrunk 1000 -> 100 (same planner-path rationale as
    // ssh_c3_state_machine_pk_insert_miss_does_not_scan_existing_rows above).
    // The ON CONFLICT hit-probe below targets a key within the seeded range.
    for id in 0..100 {
        db.execute(
            "INSERT INTO memories (id, source_id, content_hash, label) VALUES ($id, $source_id, $content_hash, 'seed')",
            &params(vec![
                ("id", Value::Uuid(uuid(7_000 + id))),
                ("source_id", Value::Text(format!("source-{id}"))),
                ("content_hash", Value::Text(format!("hash-{id}"))),
            ]),
        )
        .unwrap();
    }

    db.__reset_relational_scan_rows_touched();
    db.execute(
        "INSERT INTO memories (id, source_id, content_hash, label) VALUES ($id, 'source-77', 'hash-77', 'updated') ON CONFLICT (source_id, content_hash) DO UPDATE SET label = $label",
        &params(vec![
            ("id", Value::Uuid(uuid(8_000))),
            ("label", Value::Text("updated".to_string())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO memories (id, source_id, content_hash, label) VALUES ($id, 'source-new', 'hash-new', 'inserted') ON CONFLICT (source_id, content_hash) DO UPDATE SET label = $label",
        &params(vec![
            ("id", Value::Uuid(uuid(8_001))),
            ("label", Value::Text("inserted".to_string())),
        ]),
    )
    .unwrap();
    assert_eq!(
        db.__relational_scan_rows_touched(),
        0,
        "composite ON CONFLICT must probe the unique tuple index for hits and misses"
    );
}

#[test]
fn ssh_c3_maint_live_schema() {
    let db = Database::open_memory();
    setup_pk_col_table(&db, "t", ", col2 INTEGER");
    exec(&db, "CREATE TABLE sib (id UUID PRIMARY KEY, x INTEGER)");

    db.__reset_index_maintenance_visits();
    insert_uuid_row(&db, "t", uuid(80), 1);
    let v1 = db.__index_maintenance_visits();

    exec(&db, "CREATE INDEX idx_t ON t (col)");
    exec(&db, "CREATE INDEX sib_a ON sib (x)");
    db.__reset_index_maintenance_visits();
    insert_uuid_row(&db, "t", uuid(81), 2);
    let v2 = db.__index_maintenance_visits();

    assert_eq!(v1, 1);
    assert_eq!(v2, 2);
    assert_eq!(v2 - v1, 1);
}

#[test]
fn ssh_c3_maint_vector_table() {
    let db = Database::open_memory();
    exec(
        &db,
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER, embedding VECTOR(4))",
    );
    exec(&db, "CREATE INDEX idx_a ON t (col)");
    setup_sibling_with_indexes(&db);

    db.__reset_index_maintenance_visits();
    db.execute(
        "INSERT INTO t (id, col, embedding) VALUES ($id, 1, $embedding)",
        &params(vec![
            ("id", Value::Uuid(uuid(90))),
            ("embedding", Value::Vector(vec![1.0, 0.0, 0.0, 0.0])),
        ]),
    )
    .unwrap();
    assert_eq!(db.__index_maintenance_visits(), 2);
}

#[test]
fn ssh_c3_maint_edge_indegree() {
    let db = Database::open_memory();
    exec(
        &db,
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT, UNIQUE(source_id, target_id, edge_type)) DAG('edges')",
    );
    setup_sibling_with_indexes(&db);

    db.__reset_index_maintenance_visits();
    db.execute(
        "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($id, $s, $t, 'x')",
        &params(vec![
            ("id", Value::Uuid(uuid(100))),
            ("s", Value::Uuid(uuid(101))),
            ("t", Value::Uuid(uuid(102))),
        ]),
    )
    .unwrap();
    let baseline = db.__index_maintenance_visits();
    assert_eq!(
        baseline, 2,
        "E = 1 (__pk_id) + 1 shared exact storage for logical unique and graph-edge auto indexes"
    );

    let target = uuid(102);
    for i in 0..5 {
        db.execute(
            "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($id, $s, $t, 'x')",
            &params(vec![
                ("id", Value::Uuid(uuid(110 + i))),
                ("s", Value::Uuid(uuid(120 + i))),
                ("t", Value::Uuid(target)),
            ]),
        )
        .unwrap();
    }
    db.__reset_index_maintenance_visits();
    db.execute(
        "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($id, $s, $t, 'x')",
        &params(vec![
            ("id", Value::Uuid(uuid(130))),
            ("s", Value::Uuid(uuid(131))),
            ("t", Value::Uuid(target)),
        ]),
    )
    .unwrap();
    assert_eq!(db.__index_maintenance_visits(), 2);
}

#[test]
fn ssh_c3_open_isolated() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("open_isolated.redb");
    {
        let db = Database::open(&path).unwrap();
        exec(&db, "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER)");
        exec(&db, "CREATE INDEX idx_a ON t (col)");
        exec(&db, "CREATE TABLE sib (id UUID PRIMARY KEY, x INTEGER)");
        for i in 0..5 {
            insert_uuid_row(&db, "t", uuid(200 + i), i as i64);
        }
        for i in 0..2 {
            db.execute(
                "INSERT INTO sib (id, x) VALUES ($id, $x)",
                &params(vec![
                    ("id", Value::Uuid(uuid(300 + i))),
                    ("x", Value::Int64(i as i64)),
                ]),
            )
            .unwrap();
        }
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(db.__open_index_maintenance_visits(), 12);
}

#[test]
fn ssh_c3_open_sibling_scaling() {
    let tmp1 = setup_file_db_for_open("open_sibling_1.redb", false, 1);
    let db1 = Database::open(tmp1.path().join("open_sibling_1.redb")).unwrap();
    let o1 = db1.__open_index_maintenance_visits();

    let tmp2 = setup_file_db_for_open("open_sibling_2.redb", true, 1);
    let db2 = Database::open(tmp2.path().join("open_sibling_2.redb")).unwrap();
    let o2 = db2.__open_index_maintenance_visits();

    assert_eq!(o1, 7);
    assert_eq!(o2, 10);
    assert_eq!(o1 - 1, o2 - 4);
}

#[test]
fn ssh_c3_open_empty_sibling() {
    let tmp = setup_file_db_for_open("open_empty_sibling.redb", true, 0);
    let db = Database::open(tmp.path().join("open_empty_sibling.redb")).unwrap();
    assert_eq!(db.__open_index_maintenance_visits(), 6);
}

#[test]
fn ssh_c1_rows_identical_int64_pk() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("int64.redb");
    let before_rows;
    let before_trace;
    let before_update;
    {
        let db = Database::open(&path).unwrap();
        exec(
            &db,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, col INTEGER, col2 INTEGER)",
        );
        exec(&db, "CREATE INDEX idx_a ON t (col)");
        for id in 1..=6 {
            db.execute(
                "INSERT INTO t (id, col, col2) VALUES ($id, $col, $col2)",
                &params(vec![
                    ("id", Value::Int64(id)),
                    ("col", Value::Int64(id)),
                    ("col2", Value::Int64(id * 10)),
                ]),
            )
            .unwrap();
        }
        before_update = exec(&db, "UPDATE t SET col2 = 99 WHERE col = 2").rows_affected;
        let selected = db
            .execute(
                "SELECT id, col, col2 FROM t WHERE col IN (2, 4) ORDER BY id ASC",
                &empty(),
            )
            .unwrap();
        before_trace = selected.trace.clone();
        before_rows = selected.rows;
        db.close().unwrap();
    }
    let reopened = Database::open(&path).unwrap();
    let selected = reopened
        .execute(
            "SELECT id, col, col2 FROM t WHERE col IN (2, 4) ORDER BY id ASC",
            &empty(),
        )
        .unwrap();
    let after_update = exec(&reopened, "UPDATE t SET col2 = 99 WHERE col = 2").rows_affected;
    assert_eq!(
        before_rows,
        vec![
            vec![Value::Int64(2), Value::Int64(2), Value::Int64(99)],
            vec![Value::Int64(4), Value::Int64(4), Value::Int64(40)],
        ]
    );
    assert_eq!(selected.rows, before_rows);
    assert_eq!(selected.trace.physical_plan, before_trace.physical_plan);
    assert_eq!(selected.trace.index_used, before_trace.index_used);
    assert_eq!(
        selected.trace.predicates_pushed,
        before_trace.predicates_pushed
    );
    assert_eq!(before_update, after_update);
    assert_eq!(selected.trace.index_used.as_deref(), Some("idx_a"));
}

#[test]
fn ssh_c1_rows_identical_inmem() {
    let db = Database::open_memory();
    let ids = seed_t_for_identity(&db);
    let selected = db
        .execute(
            "SELECT id, col, col2 FROM t WHERE col IN (2, 4) ORDER BY col ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(
        selected.rows,
        vec![
            vec![Value::Uuid(ids[1]), Value::Int64(2), Value::Int64(20)],
            vec![Value::Uuid(ids[3]), Value::Int64(4), Value::Int64(40)],
        ]
    );
    assert_eq!(
        exec(&db, "UPDATE t SET col2 = 99 WHERE col = 2").rows_affected,
        1
    );
    assert_eq!(exec(&db, "DELETE FROM t WHERE col = 4").rows_affected, 1);
    assert_eq!(count_rows(&db, "t"), 5);
}

#[test]
fn ssh_c1_trace_identical() {
    let db = Database::open_memory();
    exec(
        &db,
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER, col2 INTEGER)",
    );
    exec(&db, "CREATE INDEX idx_c ON t (col, col2)");
    db.execute(
        "INSERT INTO t (id, col, col2) VALUES ($id, 1, 2)",
        &params(vec![("id", Value::Uuid(uuid(400)))]),
    )
    .unwrap();
    let r = db
        .execute("SELECT col FROM t WHERE col = 1 AND col2 = 2", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_c"));
    assert_eq!(pushed(&r), vec!["col", "col2"]);
    assert!(!r.trace.sort_elided);
    assert_eq!(
        considered(&r.trace),
        vec![("__pk_id", "first column not in WHERE")]
    );
}

#[test]
fn ssh_c1_explain_identical() {
    let db = Database::open_memory();
    setup_pk_col_table(&db, "t", "");
    exec(&db, "CREATE INDEX idx_a ON t (col)");
    insert_uuid_row(&db, "t", uuid(410), 1);
    let explain = render_explain(&db, "SELECT col FROM t WHERE col = 1", &empty()).unwrap();
    assert_eq!(
        explain,
        "IndexScan { index: idx_a }\n  predicates_pushed: [col]\n  indexes_considered: [__pk_id: first column not in WHERE]\n"
    );
}

#[test]
fn ssh_c1_error_variants_identical() {
    let db = Database::open_memory();
    exec(
        &db,
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER UNIQUE)",
    );
    commit_duplicate_unique_pair(&db, 420, 421, 1);

    exec(&db, "CREATE TABLE parent (id UUID PRIMARY KEY)");
    exec(
        &db,
        "CREATE TABLE child (id UUID PRIMARY KEY, parent_id UUID REFERENCES parent(id))",
    );
    assert_fk_violation(
        db.execute(
            "INSERT INTO child (id, parent_id) VALUES ($id, $parent)",
            &params(vec![
                ("id", Value::Uuid(uuid(422))),
                ("parent", Value::Uuid(uuid(423))),
            ]),
        ),
        "child",
        "parent",
    );
    let missing = db.execute(
        "INSERT INTO nope (id) VALUES ($id)",
        &params(vec![("id", Value::Uuid(uuid(424)))]),
    );
    assert!(matches!(missing, Err(Error::TableNotFound(ref table)) if table == "nope"));
}

#[test]
fn ssh_c1_explicit_tx_parity() {
    let db = Database::open_memory();
    exec(
        &db,
        "CREATE TABLE t_auto (id UUID PRIMARY KEY, col INTEGER)",
    );
    exec(&db, "CREATE TABLE t_tx (id UUID PRIMARY KEY, col INTEGER)");
    insert_uuid_row(&db, "t_auto", uuid(430), 1);
    let tx = db.begin().unwrap();
    db.insert_row(
        tx,
        "t_tx",
        HashMap::from([
            ("id".to_string(), Value::Uuid(uuid(430))),
            ("col".to_string(), Value::Int64(1)),
        ]),
    )
    .unwrap();
    db.commit(tx).unwrap();
    let auto = db
        .execute("SELECT id, col FROM t_auto ORDER BY col ASC", &empty())
        .unwrap()
        .rows;
    let in_tx = db
        .execute("SELECT id, col FROM t_tx ORDER BY col ASC", &empty())
        .unwrap()
        .rows;
    assert_eq!(auto, in_tx);
}

#[test]
fn ssh_c2_lifecycle_order() {
    let (plugin, records) = LifecycleObserverPlugin::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    exec(&db, "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER)");
    let pre_before = records.pre_commit.lock().unwrap().len();
    let post_before = records.post_commit.lock().unwrap().len();
    insert_uuid_row(&db, "t", uuid(440), 1);
    let pre = records.pre_commit.lock().unwrap();
    let post = records.post_commit.lock().unwrap();
    assert_eq!(pre.len(), pre_before + 1);
    assert_eq!(post.len(), post_before + 1);
    assert_eq!(pre.last(), post.last());
    assert!(records.commit_failed.lock().unwrap().is_empty());
    assert_eq!(records.order.lock().unwrap().as_slice(), ["pre", "post"]);
}

#[test]
fn ssh_c2_subscription_after_commit() {
    let db = Database::open_memory();
    exec(&db, "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER)");
    let rx = db.subscribe();
    insert_uuid_row(&db, "t", uuid(450), 1);
    let e1 = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    insert_uuid_row(&db, "t", uuid(451), 2);
    let e2 = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(e1.source, CommitSource::AutoCommit);
    assert_eq!(e1.tables_changed, vec!["t".to_string()]);
    assert_eq!(e1.row_count, 1);
    assert!(e2.lsn > e1.lsn);
    assert_eq!(e2.tables_changed, vec!["t".to_string()]);
    assert_eq!(e2.row_count, 1);
    assert!(rx.try_recv().is_err());
}

#[test]
fn ssh_c2_no_commit_event_when_no_subscribers() {
    let db = Database::open_memory();
    exec(&db, "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER)");
    let rx = db.subscribe();
    exec(&db, "UPDATE t SET col = col WHERE col = -999");
    assert!(rx.try_recv().is_err());
    insert_uuid_row(&db, "t", uuid(460), 1);
    let event = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(event.row_count, 1);
    assert!(rx.try_recv().is_err());
}

#[test]
fn ssh_c2_commit_failed_content_equal() {
    let (plugin, records) = LifecycleObserverPlugin::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    exec(
        &db,
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER UNIQUE)",
    );
    let loser_id = uuid(471);
    let tx1 = db.begin().unwrap();
    db.insert_row(
        tx1,
        "t",
        HashMap::from([
            ("id".to_string(), Value::Uuid(uuid(470))),
            ("col".to_string(), Value::Int64(1)),
        ]),
    )
    .unwrap();
    let tx2 = db.begin().unwrap();
    db.insert_row(
        tx2,
        "t",
        HashMap::from([
            ("id".to_string(), Value::Uuid(loser_id)),
            ("col".to_string(), Value::Int64(1)),
        ]),
    )
    .unwrap();
    db.commit(tx1).unwrap();
    assert!(matches!(db.commit(tx2), Err(Error::UniqueViolation { .. })));
    let failed = records.commit_failed.lock().unwrap();
    let (ws, tag) = failed.last().unwrap();
    assert_eq!(tag, "unique:t.col");
    assert_eq!(ws.relational_inserts.len(), 1);
    assert_exact_row(
        &ws.relational_inserts[0],
        "t",
        &[("id", Value::Uuid(loser_id)), ("col", Value::Int64(1))],
    );
}

#[test]
fn ssh_c2_failed_commit_releases_and_usable() {
    let db = Database::open_memory();
    exec(
        &db,
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER UNIQUE)",
    );
    let tx1 = db.begin().unwrap();
    db.insert_row(
        tx1,
        "t",
        HashMap::from([
            ("id".to_string(), Value::Uuid(uuid(480))),
            ("col".to_string(), Value::Int64(1)),
        ]),
    )
    .unwrap();
    let tx2 = db.begin().unwrap();
    db.insert_row(
        tx2,
        "t",
        HashMap::from([
            ("id".to_string(), Value::Uuid(uuid(481))),
            ("col".to_string(), Value::Int64(1)),
        ]),
    )
    .unwrap();
    db.commit(tx1).unwrap();
    assert!(db.commit(tx2).is_err());
    insert_uuid_row(&db, "t", uuid(482), 2);
    assert_eq!(count_rows(&db, "t"), 2);
    let loser = db
        .execute(
            "SELECT id FROM t WHERE id = $id",
            &params(vec![("id", Value::Uuid(uuid(481)))]),
        )
        .unwrap();
    assert!(loser.rows.is_empty());
}

#[test]
fn ssh_c2_post_commit_not_called_on_failure() {
    let (plugin, records) = LifecycleObserverPlugin::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    exec(
        &db,
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER UNIQUE)",
    );
    let tx1 = db.begin().unwrap();
    db.insert_row(
        tx1,
        "t",
        HashMap::from([
            ("id".to_string(), Value::Uuid(uuid(490))),
            ("col".to_string(), Value::Int64(1)),
        ]),
    )
    .unwrap();
    let tx2 = db.begin().unwrap();
    db.insert_row(
        tx2,
        "t",
        HashMap::from([
            ("id".to_string(), Value::Uuid(uuid(491))),
            ("col".to_string(), Value::Int64(1)),
        ]),
    )
    .unwrap();
    db.commit(tx1).unwrap();
    let post_before = records.post_commit.lock().unwrap().len();
    assert_unique_commit(db.commit(tx2), "t", "col");
    assert_eq!(records.post_commit.lock().unwrap().len(), post_before);
    assert_eq!(records.commit_failed.lock().unwrap().len(), 1);
}

#[test]
fn ssh_c2_sync_applied_commit_contract() {
    let source = Database::open_memory();
    exec(&source, "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER)");
    let base = source.current_lsn();
    let (plugin, records) = LifecycleObserverPlugin::new();
    let peer = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    exec(&peer, "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER)");
    let pre_before = records.pre_commit.lock().unwrap().len();
    let post_before = records.post_commit.lock().unwrap().len();
    let rx = peer.subscribe();

    insert_uuid_row(&source, "t", uuid(500), 1);
    peer.apply_changes(
        source.changes_since(base),
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    )
    .unwrap();
    assert_eq!(records.pre_commit.lock().unwrap().len(), pre_before + 1);
    assert_eq!(records.post_commit.lock().unwrap().len(), post_before + 1);
    let event = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(event.source, CommitSource::SyncPull);
    assert_eq!(event.tables_changed, vec!["t".to_string()]);
}

#[test]
fn ssh_c2_reentrant_trigger_commit() {
    let (plugin, records) = LifecycleObserverPlugin::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    exec(&db, "CREATE TABLE t (id UUID PRIMARY KEY)");
    exec(
        &db,
        "CREATE TABLE audit (id UUID PRIMARY KEY, host_id UUID)",
    );
    exec(&db, "CREATE TRIGGER tr ON t WHEN INSERT");
    db.register_trigger_callback("tr", |db_handle, ctx| {
        db_handle.execute_in_tx(
            ctx.tx,
            "INSERT INTO audit (id, host_id) VALUES ($id, $host)",
            &params(vec![
                ("id", Value::Uuid(uuid(5100))),
                (
                    "host",
                    ctx.row_values.get("id").cloned().unwrap_or(Value::Null),
                ),
            ]),
        )?;
        Ok(())
    })
    .unwrap();
    db.complete_initialization().unwrap();
    let pre_before = records.pre_commit.lock().unwrap().len();
    let post_before = records.post_commit.lock().unwrap().len();
    db.execute(
        "INSERT INTO t (id) VALUES ($id)",
        &params(vec![("id", Value::Uuid(uuid(510)))]),
    )
    .unwrap();
    assert_eq!(records.pre_commit.lock().unwrap().len(), pre_before + 1);
    assert_eq!(records.post_commit.lock().unwrap().len(), post_before + 1);
    assert_eq!(count_rows(&db, "t"), 1);
    assert_eq!(count_rows(&db, "audit"), 1);
}

#[test]
fn ssh_c4_row_order_stable() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("order.redb");
    let before;
    {
        let db = Database::open(&path).unwrap();
        seed_t_for_identity(&db);
        before = db
            .execute(
                "SELECT id FROM t WHERE col IN (2, 3, 4) ORDER BY id ASC",
                &empty(),
            )
            .unwrap()
            .rows;
        db.close().unwrap();
    }
    let reopened = Database::open(&path).unwrap();
    let after = reopened
        .execute(
            "SELECT id FROM t WHERE col IN (2, 3, 4) ORDER BY id ASC",
            &empty(),
        )
        .unwrap()
        .rows;
    assert_eq!(after, before);
}

#[test]
fn ssh_c4_tombstone_invisible() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("tombstone.redb");
    {
        let db = Database::open(&path).unwrap();
        seed_t_for_identity(&db);
        exec(&db, "DELETE FROM t WHERE col = 2");
        db.close().unwrap();
    }
    let reopened = Database::open(&path).unwrap();
    let deleted = reopened
        .execute("SELECT id FROM t WHERE col = 2", &empty())
        .unwrap();
    let deleted_scan = reopened
        .execute("SELECT id FROM t WHERE col2 = 20", &empty())
        .unwrap();
    let survivor = reopened
        .execute("SELECT id FROM t WHERE col = 3", &empty())
        .unwrap();
    let survivor_scan = reopened
        .execute("SELECT id FROM t WHERE col2 = 30", &empty())
        .unwrap();
    assert!(deleted.rows.is_empty());
    assert!(deleted_scan.rows.is_empty());
    assert_eq!(survivor.rows.len(), 1);
    assert_eq!(survivor.rows, survivor_scan.rows);
    assert_eq!(deleted.trace.index_used.as_deref(), Some("idx_a"));
    assert_eq!(deleted_scan.trace.physical_plan, "Scan");
}

#[test]
fn ssh_c4_next_row_id_no_reuse() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("next_row.redb");
    {
        let db = Database::open(&path).unwrap();
        seed_t_for_identity(&db);
        exec(&db, "DELETE FROM t WHERE col = 6");
        db.close().unwrap();
    }
    let reopened = Database::open(&path).unwrap();
    insert_uuid_row(&reopened, "t", uuid(600), 100);
    assert!(
        reopened
            .execute("SELECT id FROM t WHERE col = 6", &empty())
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(
        reopened
            .execute("SELECT id FROM t WHERE col = 100", &empty())
            .unwrap()
            .rows
            .len(),
        1
    );
    assert_eq!(count_rows(&reopened, "t"), 6);
}

#[test]
fn ssh_c4_schema_and_indexes_persist() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("schema.redb");
    {
        let db = Database::open(&path).unwrap();
        exec(
            &db,
            "CREATE TABLE t (id UUID PRIMARY KEY, status TEXT, col INTEGER UNIQUE) STATE MACHINE (status: active -> [archived])",
        );
        exec(&db, "CREATE INDEX idx_status ON t (status)");
        db.execute(
            "INSERT INTO t (id, status, col) VALUES ($id, 'active', 1)",
            &params(vec![("id", Value::Uuid(uuid(610)))]),
        )
        .unwrap();
        db.close().unwrap();
    }
    let reopened = Database::open(&path).unwrap();
    let tx1 = reopened.begin().unwrap();
    reopened
        .execute_in_tx(
            tx1,
            "INSERT INTO t (id, status, col) VALUES ($id, 'active', 2)",
            &params(vec![("id", Value::Uuid(uuid(611)))]),
        )
        .unwrap();
    let tx2 = reopened.begin().unwrap();
    reopened
        .execute_in_tx(
            tx2,
            "INSERT INTO t (id, status, col) VALUES ($id, 'active', 2)",
            &params(vec![("id", Value::Uuid(uuid(612)))]),
        )
        .unwrap();
    reopened.commit(tx1).unwrap();
    assert_unique_commit(reopened.commit(tx2), "t", "col");
    let illegal = reopened.execute("UPDATE t SET status = 'missing' WHERE col = 1", &empty());
    assert!(illegal.is_err());
    let routed = reopened
        .execute("SELECT id FROM t WHERE status = 'active'", &empty())
        .unwrap();
    assert_eq!(routed.trace.index_used.as_deref(), Some("idx_status"));
}

#[test]
fn ssh_c4_winner_stable() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("winner.redb");
    let before;
    {
        let db = Database::open(&path).unwrap();
        exec(
            &db,
            "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER UNIQUE, col2 INTEGER, col3 INTEGER)",
        );
        exec(&db, "CREATE INDEX idx_c ON t (col, col2, col3)");
        for col in [2, 5] {
            db.execute(
                "INSERT INTO t (id, col, col2, col3) VALUES ($id, $col, 20, 200)",
                &params(vec![
                    ("id", Value::Uuid(uuid(620 + col as u128))),
                    ("col", Value::Int64(col)),
                ]),
            )
            .unwrap();
        }
        before = db
            .execute(
                "SELECT col FROM t WHERE col IN (2, 5) AND col2 = 20 AND col3 = 200",
                &empty(),
            )
            .unwrap()
            .trace;
        db.close().unwrap();
    }
    let reopened = Database::open(&path).unwrap();
    let after = reopened
        .execute(
            "SELECT col FROM t WHERE col IN (2, 5) AND col2 = 20 AND col3 = 200",
            &empty(),
        )
        .unwrap()
        .trace;
    assert_eq!(before.index_used, after.index_used);
    assert_eq!(after.index_used.as_deref(), Some("idx_c"));
    assert_eq!(before.predicates_pushed, after.predicates_pushed);
    assert_eq!(before.physical_plan, after.physical_plan);
    assert_eq!(before.sort_elided, after.sort_elided);
    assert_eq!(considered(&before), considered(&after));
}

#[test]
fn ssh_c4_graph_and_vector_intact() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("graph_vector.redb");
    let target = uuid(640);
    {
        let db = Database::open(&path).unwrap();
        exec(
            &db,
            "CREATE TABLE docs (id UUID PRIMARY KEY, embedding VECTOR(4))",
        );
        exec(
            &db,
            "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) DAG('LINK')",
        );
        db.execute(
            "INSERT INTO docs (id, embedding) VALUES ($id, $embedding)",
            &params(vec![
                ("id", Value::Uuid(target)),
                ("embedding", Value::Vector(vec![1.0, 0.0, 0.0, 0.0])),
            ]),
        )
        .unwrap();
        db.execute(
            "INSERT INTO GRAPH (source_id, target_id, edge_type) VALUES ($s, $t, 'LINK')",
            &params(vec![
                ("s", Value::Uuid(uuid(641))),
                ("t", Value::Uuid(target)),
            ]),
        )
        .unwrap();
        db.close().unwrap();
    }
    let reopened = Database::open(&path).unwrap();
    let graph = reopened
        .execute(
            "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINK]->(b) WHERE a.id = $s COLUMNS(b.id AS t))",
            &params(vec![("s", Value::Uuid(uuid(641)))]),
        )
        .unwrap();
    assert_eq!(graph.rows, vec![vec![Value::Uuid(target)]]);
    let row = reopened
        .point_lookup("docs", "id", &Value::Uuid(target), reopened.snapshot())
        .unwrap()
        .unwrap();
    let ann = reopened
        .query_vector(
            VectorIndexRef::new("docs", "embedding"),
            &[1.0, 0.0, 0.0, 0.0],
            1,
            None,
            reopened.snapshot(),
        )
        .unwrap();
    assert_eq!(ann[0].0, row.row_id);
}

#[test]
fn ssh_c4_crash_shaped_reopen() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("crash.redb");
    {
        let db = Database::open(&path).unwrap();
        seed_t_for_identity(&db);
    }
    let reopened = Database::open(&path).unwrap();
    assert_eq!(count_rows(&reopened, "t"), 6);
    assert_eq!(
        reopened
            .execute("SELECT id FROM t WHERE col = 3", &empty())
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn ssh_c4_reopen_after_delete_final_write() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("delete_final.redb");
    {
        let db = Database::open(&path).unwrap();
        seed_t_for_identity(&db);
        exec(&db, "DELETE FROM t");
        db.close().unwrap();
    }
    let reopened = Database::open(&path).unwrap();
    assert_eq!(count_rows(&reopened, "t"), 0);
    let routed_empty = reopened
        .execute("SELECT id FROM t WHERE col = 1", &empty())
        .unwrap();
    let scan_empty = reopened
        .execute("SELECT id FROM t WHERE col2 = 10", &empty())
        .unwrap();
    assert!(routed_empty.rows.is_empty());
    assert!(scan_empty.rows.is_empty());
    assert_eq!(routed_empty.trace.index_used.as_deref(), Some("idx_a"));
    assert_eq!(scan_empty.trace.physical_plan, "Scan");
    reopened
        .execute(
            "INSERT INTO t (id, col, col2) VALUES ($id, 1, 10)",
            &params(vec![("id", Value::Uuid(uuid(650)))]),
        )
        .unwrap();
    let routed_new = reopened
        .execute("SELECT id FROM t WHERE col = 1", &empty())
        .unwrap();
    let scan_new = reopened
        .execute("SELECT id FROM t WHERE col2 = 10", &empty())
        .unwrap();
    assert_eq!(count_rows(&reopened, "t"), 1);
    assert_eq!(routed_new.rows, scan_new.rows);
}

#[test]
fn ssh_c2_subscribe_after_reopen() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("subscribe.redb");
    {
        let db = Database::open(&path).unwrap();
        seed_t_for_identity(&db);
        db.close().unwrap();
    }
    let reopened = Database::open(&path).unwrap();
    let rx = reopened.subscribe();
    insert_uuid_row(&reopened, "t", uuid(660), 99);
    let event = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(event.source, CommitSource::AutoCommit);
    assert_eq!(event.row_count, 1);
    assert_eq!(event.tables_changed, vec!["t".to_string()]);
    assert!(rx.try_recv().is_err());
    let routed = reopened
        .execute("SELECT id FROM t WHERE col = 3", &empty())
        .unwrap();
    assert_eq!(routed.trace.index_used.as_deref(), Some("idx_a"));
}

#[test]
fn ssh_c2_sync_applied_index_ddl_lifecycle() {
    let source = Database::open_memory();
    let peer = Database::open_memory();
    for db in [&source, &peer] {
        exec(
            db,
            "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER, col2 INTEGER)",
        );
        exec(db, "CREATE TABLE sib (id UUID PRIMARY KEY, x INTEGER)");
        exec(db, "CREATE INDEX sib_x ON sib (x)");
        db.execute(
            "INSERT INTO t (id, col, col2) VALUES ($id, 1, 2)",
            &params(vec![("id", Value::Uuid(uuid(670)))]),
        )
        .unwrap();
        db.execute(
            "INSERT INTO sib (id, x) VALUES ($id, 9)",
            &params(vec![("id", Value::Uuid(uuid(671)))]),
        )
        .unwrap();
    }
    let base = source.current_lsn();
    exec(&source, "CREATE INDEX idx_c ON t (col, col2)");
    peer.apply_changes(
        source.changes_since(base),
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    )
    .unwrap();
    let routed = peer
        .execute("SELECT id FROM t WHERE col = 1 AND col2 = 2", &empty())
        .unwrap();
    assert_eq!(routed.trace.index_used.as_deref(), Some("idx_c"));
    let sib = peer
        .execute("SELECT id FROM sib WHERE x = 9", &empty())
        .unwrap();
    assert_eq!(sib.trace.index_used.as_deref(), Some("sib_x"));

    let drop_base = source.current_lsn();
    exec(&source, "DROP INDEX idx_c ON t");
    peer.apply_changes(
        source.changes_since(drop_base),
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    )
    .unwrap();
    let rerouted = peer
        .execute("SELECT id FROM t WHERE col = 1 AND col2 = 2", &empty())
        .unwrap();
    assert_ne!(rerouted.trace.index_used.as_deref(), Some("idx_c"));
    assert!(
        !considered(&rerouted.trace)
            .iter()
            .any(|(name, _)| *name == "idx_c")
    );
}

#[test]
fn ssh_c2_composite_unique_probe_locality() {
    let db = Database::open_memory();
    exec(
        &db,
        "CREATE TABLE t (id UUID PRIMARY KEY, col1 INTEGER, col2 INTEGER, UNIQUE(col1, col2))",
    );
    setup_sibling_with_indexes(&db);
    commit_duplicate_composite_pair(&db, 680, 681, 1, 2);
    exec(&db, "CREATE INDEX sib_more ON sib (y, x)");
    commit_duplicate_composite_pair(&db, 682, 683, 3, 4);
    db.__reset_index_maintenance_visits();
    db.execute(
        "INSERT INTO t (id, col1, col2) VALUES ($id, 9, 9)",
        &params(vec![("id", Value::Uuid(uuid(684)))]),
    )
    .unwrap();
    assert_eq!(db.__index_maintenance_visits(), 2);
}

#[test]
fn ssh_c2_mixed_paradigm_failed_commit_atomic() {
    let (plugin, records) = LifecycleObserverPlugin::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    exec(
        &db,
        "CREATE TABLE t (id UUID PRIMARY KEY, note TEXT UNIQUE, embedding VECTOR(4))",
    );
    exec(
        &db,
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) DAG('mixed')",
    );
    let loser_id = uuid(691);
    let source_id = uuid(692);
    let target_id = uuid(693);
    let loser_tx = db.begin().unwrap();
    db.execute_in_tx(
        loser_tx,
        "INSERT INTO t (id, note, embedding) VALUES ($id, 'dup', $embedding)",
        &params(vec![
            ("id", Value::Uuid(loser_id)),
            ("embedding", Value::Vector(vec![0.0, 1.0, 0.0, 0.0])),
        ]),
    )
    .unwrap();
    db.execute_in_tx(
        loser_tx,
        "INSERT INTO GRAPH (source_id, target_id, edge_type) VALUES ($s, $t, 'mixed')",
        &params(vec![
            ("s", Value::Uuid(source_id)),
            ("t", Value::Uuid(target_id)),
        ]),
    )
    .unwrap();
    let winner_tx = db.begin().unwrap();
    db.execute_in_tx(
        winner_tx,
        "INSERT INTO t (id, note, embedding) VALUES ($id, 'dup', $embedding)",
        &params(vec![
            ("id", Value::Uuid(uuid(690))),
            ("embedding", Value::Vector(vec![1.0, 0.0, 0.0, 0.0])),
        ]),
    )
    .unwrap();
    db.commit(winner_tx).unwrap();
    assert!(matches!(
        db.commit(loser_tx),
        Err(Error::UniqueViolation { .. })
    ));

    assert_eq!(count_rows(&db, "t"), 1);
    let graph = db
        .execute(
            "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:mixed]->(b) WHERE a.id = $s COLUMNS(b.id AS t))",
            &params(vec![("s", Value::Uuid(source_id))]),
        )
        .unwrap();
    assert!(graph.rows.is_empty());
    let failed = records.commit_failed.lock().unwrap();
    let (ws, tag) = failed.last().unwrap();
    assert_eq!(tag, "unique:t.note");
    assert_eq!(ws.relational_inserts.len(), 1);
    assert_exact_row(
        &ws.relational_inserts[0],
        "t",
        &[
            ("id", Value::Uuid(loser_id)),
            ("note", Value::Text("dup".to_string())),
            ("embedding", Value::Vector(vec![0.0, 1.0, 0.0, 0.0])),
        ],
    );
    assert_eq!(ws.vector_inserts.len(), 1);
    assert_eq!(ws.vector_inserts[0].table, "t");
    assert_eq!(ws.vector_inserts[0].column, "embedding");
    assert_eq!(ws.vector_inserts[0].vector, vec![0.0, 1.0, 0.0, 0.0]);
    let loser_vector_row_id = ws.vector_inserts[0].row_id;
    let loser_vector_hits = db
        .query_vector(
            VectorIndexRef::new("t", "embedding"),
            &[0.0, 1.0, 0.0, 0.0],
            5,
            None,
            db.snapshot(),
        )
        .unwrap();
    assert!(
        !loser_vector_hits
            .iter()
            .any(|(row_id, _)| *row_id == loser_vector_row_id),
        "loser vector leaked into ANN results: {loser_vector_hits:?}"
    );
    assert_eq!(ws.adj_inserts.len(), 1);
    assert_eq!(ws.adj_inserts[0].source, source_id);
    assert_eq!(ws.adj_inserts[0].target, target_id);
    assert_eq!(ws.adj_inserts[0].edge_type, "mixed");
    assert!(ws.adj_inserts[0].properties.is_empty());
    db.execute(
        "INSERT INTO t (id, note, embedding) VALUES ($id, 'fresh', $embedding)",
        &params(vec![
            ("id", Value::Uuid(uuid(694))),
            ("embedding", Value::Vector(vec![0.0, 0.0, 1.0, 0.0])),
        ]),
    )
    .unwrap();
}

#[test]
fn ssh_c1_alter_drop_column_cascade_cleans_storage() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("drop_column.redb");
    {
        let db = Database::open(&path).unwrap();
        seed_t_for_identity(&db);
        exec(&db, "CREATE INDEX idx_ab ON t (col, col2)");
        let before = db.__introspect_indexes_total_entries();
        exec(&db, "ALTER TABLE t DROP COLUMN col CASCADE");
        let after = db.__introspect_indexes_total_entries();
        let rendered = render_table_meta("t", &db.table_meta("t").unwrap());
        assert!(after < before);
        assert!(!rendered.contains("idx_a"));
        assert!(!rendered.contains("idx_ab"));
        assert!(!rendered.contains(" col "));
        db.close().unwrap();
    }
    let reopened = Database::open(&path).unwrap();
    let rendered = render_table_meta("t", &reopened.table_meta("t").unwrap());
    assert!(!rendered.contains("idx_a"));
    assert!(!rendered.contains("idx_ab"));
    assert!(!rendered.contains(" col "));
}
