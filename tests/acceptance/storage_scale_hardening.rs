use super::common::*;
use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use contextdb_engine::cli_render::{render_explain, render_table_meta};
use contextdb_engine::plugin::{CommitSource, DatabasePlugin};
use contextdb_tx::WriteSet;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
struct FailedRecord {
    source: CommitSource,
    error_tag: String,
    relational_insert_count: usize,
    adj_insert_count: usize,
    vector_insert_count: usize,
    row_values: HashMap<String, Value>,
}

#[derive(Clone, Default)]
struct Records {
    pre: Arc<Mutex<usize>>,
    post: Arc<Mutex<usize>>,
    failed: Arc<Mutex<Vec<FailedRecord>>>,
}

struct Observer {
    records: Records,
}

impl Observer {
    fn new() -> (Self, Records) {
        let records = Records::default();
        (
            Self {
                records: records.clone(),
            },
            records,
        )
    }
}

impl DatabasePlugin for Observer {
    fn pre_commit(&self, _ws: &WriteSet, _source: CommitSource) -> contextdb_core::Result<()> {
        *self.records.pre.lock().unwrap() += 1;
        Ok(())
    }

    fn post_commit(&self, _ws: &WriteSet, _source: CommitSource) {
        *self.records.post.lock().unwrap() += 1;
    }

    fn commit_failed(&self, ws: &WriteSet, source: CommitSource, error: &Error) {
        let row = ws
            .relational_inserts
            .first()
            .map(|(_, row)| row.values.clone())
            .unwrap_or_default();
        let error_tag = match error {
            Error::UniqueViolation { table, column } => format!("unique:{table}.{column}"),
            other => format!("{other:?}"),
        };
        self.records.failed.lock().unwrap().push(FailedRecord {
            source,
            error_tag,
            relational_insert_count: ws.relational_inserts.len(),
            adj_insert_count: ws.adj_inserts.len(),
            vector_insert_count: ws.vector_inserts.len(),
            row_values: row,
        });
    }
}

fn uuid(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

fn exec(db: &Database, sql: &str) {
    db.execute(sql, &empty_params()).unwrap();
}

fn insert_t(db: &Database, id: Uuid, col: i64) {
    db.execute(
        "INSERT INTO t (id, col) VALUES ($id, $col)",
        &params(vec![("id", Value::Uuid(id)), ("col", Value::Int64(col))]),
    )
    .unwrap();
}

fn count_rows(db: &Database, table: &str) -> i64 {
    match &db
        .execute(&format!("SELECT COUNT(*) FROM {table}"), &empty_params())
        .unwrap()
        .rows[0][0]
    {
        Value::Int64(n) => *n,
        other => panic!("expected count Int64, got {other:?}"),
    }
}

#[test]
fn ssa_seed_fixture_journey() {
    let db = Database::open_memory();
    exec(&db, "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER)");
    exec(&db, "CREATE INDEX idx_col ON t (col)");
    exec(
        &db,
        "CREATE TABLE sib (id UUID PRIMARY KEY, x INTEGER, y INTEGER)",
    );
    exec(
        &db,
        "CREATE TABLE other (id UUID PRIMARY KEY, x INTEGER, y INTEGER)",
    );

    db.__reset_index_maintenance_visits();
    insert_t(&db, uuid(1), 1);
    let before = db.__index_maintenance_visits();
    exec(&db, "CREATE INDEX sib_x ON sib (x)");
    exec(&db, "CREATE INDEX sib_y ON sib (y)");
    exec(&db, "CREATE INDEX other_x ON other (x)");
    exec(&db, "CREATE INDEX other_y ON other (y)");
    db.__reset_index_maintenance_visits();
    insert_t(&db, uuid(2), 2);
    let after = db.__index_maintenance_visits();

    assert_eq!(before, 2);
    assert_eq!(after, 2);
}

/// Writable open restores identity bounds without replaying any history family.
fn assert_open_reads_only_bounds(
    work: &contextdb_engine::writer_query_test_seams::WriterHistoryWork,
) {
    assert_eq!(
        (
            work.row_versions,
            work.index_rebuild_inputs,
            work.graph_versions,
            work.change_log_entries,
            work.commit_index_entries,
            work.vector_memberships
        ),
        (0, 0, 0, 0, 0, 0),
        "open must not replay this table or its siblings: {work:?}"
    );
    assert_eq!(
        (
            work.ddl_occurrences,
            work.sidecar_entries,
            work.sidecar_decodes,
            work.selection_entries
        ),
        (0, [0; 8], [0; 8], 0),
        "open must not enumerate durable declarations or sidecars: {work:?}"
    );
    assert!(
        work.commit_bound_keys > 0,
        "a populated open observes real commit bounds: {work:?}"
    );
    #[cfg(feature = "test-seams")]
    assert!(
        work.allocator_regions_loaded > 0,
        "open observes real allocator metadata: {work:?}"
    );
}

#[test]
fn ssa_open_locality_journey() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("open_locality.redb");
    {
        let db = Database::open(&path).unwrap();
        exec(&db, "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER)");
        exec(&db, "CREATE INDEX idx_col ON t (col)");
        exec(&db, "CREATE TABLE sib (id UUID PRIMARY KEY, x INTEGER)");
        exec(&db, "CREATE INDEX sib_x ON sib (x)");
        exec(
            &db,
            "CREATE TABLE other (id UUID PRIMARY KEY, x INTEGER, y INTEGER)",
        );
        exec(&db, "CREATE INDEX other_x ON other (x)");
        exec(&db, "CREATE INDEX other_y ON other (y)");
        for i in 0..3 {
            insert_t(&db, uuid(10 + i), i as i64);
        }
        for i in 0..2 {
            db.execute(
                "INSERT INTO sib (id, x) VALUES ($id, $x)",
                &params(vec![
                    ("id", Value::Uuid(uuid(20 + i))),
                    ("x", Value::Int64(i as i64)),
                ]),
            )
            .unwrap();
        }
        db.execute(
            "INSERT INTO other (id, x, y) VALUES ($id, 1, 2)",
            &params(vec![("id", Value::Uuid(uuid(30)))]),
        )
        .unwrap();
        db.close().unwrap();
    }
    let (opened, work) =
        contextdb_engine::writer_query_test_seams::observe(|| Database::open(&path));
    let reopened = opened.unwrap();
    assert_open_reads_only_bounds(&work.history);
    for (sql, expected) in [
        (
            "SELECT id FROM t WHERE col = 1",
            vec![vec![Value::Uuid(uuid(11))]],
        ),
        (
            "SELECT id FROM sib WHERE x = 1",
            vec![vec![Value::Uuid(uuid(21))]],
        ),
        (
            "SELECT id FROM other WHERE x = 1 AND y = 2",
            vec![vec![Value::Uuid(uuid(30))]],
        ),
    ] {
        assert_eq!(
            reopened.execute(sql, &HashMap::new()).unwrap().rows,
            expected
        );
    }
    assert_eq!(
        reopened
            .execute("SELECT id, col FROM t ORDER BY col", &HashMap::new())
            .unwrap()
            .rows,
        (0..3)
            .map(|i| vec![Value::Uuid(uuid(10 + i)), Value::Int64(i as i64)])
            .collect::<Vec<_>>()
    );
    assert_eq!(
        reopened
            .execute("SELECT id, x FROM sib ORDER BY x", &HashMap::new())
            .unwrap()
            .rows,
        (0..2)
            .map(|i| vec![Value::Uuid(uuid(20 + i)), Value::Int64(i as i64)])
            .collect::<Vec<_>>()
    );
    assert_eq!(
        reopened
            .change_log_since(contextdb_core::Lsn(0))
            .unwrap()
            .len(),
        6,
        "positive durable population excludes an empty-store zero-work pass"
    );
    let prior = reopened.snapshot();
    let prior_lsn = reopened.current_lsn();
    insert_t(&reopened, uuid(13), 3);
    assert!(reopened.snapshot().0 > prior.0);
    assert!(reopened.current_lsn().0 > prior_lsn.0);
    assert_eq!(
        reopened
            .execute("SELECT id FROM t WHERE col = 3", &HashMap::new())
            .unwrap()
            .rows,
        vec![vec![Value::Uuid(uuid(13))]]
    );
}

#[test]
fn ssa_commit_contract_journey() {
    let (plugin, records) = Observer::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    exec(
        &db,
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER UNIQUE)",
    );
    let rx = db.subscribe();
    let pre0 = *records.pre.lock().unwrap();
    let post0 = *records.post.lock().unwrap();

    insert_t(&db, uuid(50), 1);
    assert_eq!(*records.pre.lock().unwrap(), pre0 + 1);
    assert_eq!(*records.post.lock().unwrap(), post0 + 1);
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(2)).unwrap().row_count,
        1
    );

    let tx1 = db.begin().unwrap();
    db.insert_row(
        tx1,
        "t",
        HashMap::from([
            ("id".to_string(), Value::Uuid(uuid(51))),
            ("col".to_string(), Value::Int64(2)),
        ]),
    )
    .unwrap();
    let tx2 = db.begin().unwrap();
    db.insert_row(
        tx2,
        "t",
        HashMap::from([
            ("id".to_string(), Value::Uuid(uuid(53))),
            ("col".to_string(), Value::Int64(2)),
        ]),
    )
    .unwrap();
    db.commit(tx1).unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(2)).unwrap().row_count,
        1
    );
    let post_before_fail = *records.post.lock().unwrap();
    assert!(matches!(db.commit(tx2), Err(Error::UniqueViolation { .. })));
    let failed = records.failed.lock().unwrap();
    assert_eq!(failed.len(), 1);
    let failed = &failed[0];
    assert_eq!(failed.source, CommitSource::User);
    assert_eq!(failed.error_tag, "unique:t.col");
    assert_eq!(failed.relational_insert_count, 1);
    assert_eq!(failed.adj_insert_count, 0);
    assert_eq!(failed.vector_insert_count, 0);
    assert_eq!(failed.row_values.len(), 2);
    assert_eq!(failed.row_values.get("id"), Some(&Value::Uuid(uuid(53))));
    assert_eq!(failed.row_values.get("col"), Some(&Value::Int64(2)));
    assert_eq!(*records.post.lock().unwrap(), post_before_fail);
    assert!(rx.try_recv().is_err());

    insert_t(&db, uuid(52), 3);
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(2)).unwrap().row_count,
        1
    );
    assert_eq!(count_rows(&db, "t"), 3);
}

#[test]
fn ssa_explain_schema_parity_journey() {
    let db = Database::open_memory();
    exec(
        &db,
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER, tag TEXT)",
    );
    exec(&db, "CREATE INDEX idx_col ON t (col)");
    insert_t(&db, uuid(60), 1);
    let explain = render_explain(&db, "SELECT id FROM t WHERE col = 1", &empty_params()).unwrap();
    assert_eq!(
        explain,
        "IndexScan { index: idx_col }\n  predicates_pushed: [col]\n  indexes_considered: [__pk_id: first column not in WHERE]\n"
    );
    let schema = render_table_meta("t", &db.table_meta("t").unwrap());
    assert_eq!(
        schema,
        "CREATE TABLE t (\n  id UUID PRIMARY KEY,\n  col INTEGER,\n  tag TEXT\n);\nCREATE INDEX idx_col ON t (col ASC);\n"
    );
}

#[test]
fn ssa_crash_recovery_journey() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("crash.redb");
    {
        let db = Database::open(&path).unwrap();
        exec(&db, "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER)");
        exec(&db, "CREATE INDEX idx_col ON t (col)");
        exec(
            &db,
            "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) DAG('LINK')",
        );
        insert_t(&db, uuid(80), 1);
        db.execute(
            "INSERT INTO GRAPH (source_id, target_id, edge_type) VALUES ($s, $t, 'LINK')",
            &params(vec![
                ("s", Value::Uuid(uuid(80))),
                ("t", Value::Uuid(uuid(81))),
            ]),
        )
        .unwrap();
    }
    let reopened = Database::open(&path).unwrap();
    assert_eq!(count_rows(&reopened, "t"), 1);
    assert_eq!(
        reopened
            .execute("SELECT id FROM t WHERE col = 1", &empty_params())
            .unwrap()
            .trace
            .index_used
            .as_deref(),
        Some("idx_col")
    );
    assert_eq!(
        reopened
            .execute(
                "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINK]->(b) WHERE a.id = $s COLUMNS(b.id AS t))",
                &params(vec![("s", Value::Uuid(uuid(80)))]),
            )
            .unwrap()
            .rows,
        vec![vec![Value::Uuid(uuid(81))]]
    );
}
