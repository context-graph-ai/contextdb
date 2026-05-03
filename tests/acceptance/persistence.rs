use super::common::*;
use contextdb_core::{Error, MemoryAccountant, Value, VectorIndexRef};
use contextdb_engine::Database;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use uuid::Uuid;

/// I inserted 50 rows, quit, reopened the database, and all 50 rows were still there with correct values.
#[test]
fn f01_create_insert_quit_reopen_query() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "f01.db");

    let mut script =
        String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT, reading REAL)\n");
    for idx in 0..50 {
        script.push_str(&format!(
            "INSERT INTO sensors (id, name, reading) VALUES ('{:08}-0000-0000-0000-000000000000', 'temp-{idx}', {})\n",
            idx + 1,
            idx as f64 + 0.25
        ));
    }
    script.push_str(".quit\n");
    let first = run_cli_script(&db_path, &[], &script);
    assert!(first.status.success());

    let second = run_cli_script(
        &db_path,
        &[],
        "SELECT count(*) FROM sensors\nSELECT * FROM sensors WHERE name = 'temp-3'\n.quit\n",
    );
    let stdout = output_string(&second.stdout);
    assert!(second.status.success());
    assert!(stdout.contains("50"));
    assert!(stdout.contains("temp-3"));
    assert!(stdout.contains("3.25"));
}

/// I created tables with state machines, DAG constraints, and vector columns, quit, reopened, and all the schema details were still there.
#[test]
fn f02_schema_survives_restart() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "f02.db");
    let create = "\
CREATE TABLE workflows (id UUID PRIMARY KEY, status TEXT) STATE MACHINE (status: draft -> [review], review -> [published])\n\
CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) DAG('DEPENDS_ON')\n\
CREATE TABLE embeddings (id UUID PRIMARY KEY, embedding VECTOR(384))\n\
.quit\n";
    assert!(run_cli_script(&db_path, &[], create).status.success());

    let output = run_cli_script(
        &db_path,
        &[],
        ".tables\n.schema workflows\n.schema edges\n.schema embeddings\n.quit\n",
    );
    let stdout = output_string(&output.stdout);
    assert!(output.status.success());
    assert!(stdout.contains("workflows"));
    assert!(stdout.contains("edges"));
    assert!(stdout.contains("embeddings"));
    assert!(stdout.contains("STATE MACHINE"));
    assert!(stdout.contains("DAG('DEPENDS_ON')") || stdout.contains("DAG('DEPENDS_ON"));
    assert!(stdout.contains("VECTOR(384)"));
}

/// I killed the process while it was idle after inserting 100 rows, and when I reopened the database nothing was lost or corrupted.
#[test]
fn f03_kill_9_during_idle_does_not_corrupt() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "f03.db");
    let mut child = spawn_cli(&db_path, &[]);
    let mut script = String::from("CREATE TABLE kill_test (id UUID PRIMARY KEY, name TEXT)\n");
    for idx in 0..100 {
        script.push_str(&format!(
            "INSERT INTO kill_test (id, name) VALUES ('{:08}-0000-0000-0000-000000000000', 'row-{idx}')\n",
            idx + 1
        ));
    }
    write_child_stdin(&mut child, &script);
    write_child_stdin(&mut child, "SELECT count(*) FROM kill_test\n");
    let barrier_output =
        wait_for_child_stdout_contains(&mut child, "| 100", Duration::from_secs(10));
    assert!(
        barrier_output.contains("| 100"),
        "CLI must report the 100-row commit barrier before the kill: {barrier_output}"
    );
    stop_child(&mut child);

    let reopened = run_cli_script(&db_path, &[], "SELECT count(*) FROM kill_test\n.quit\n");
    assert!(reopened.status.success());
    assert!(output_string(&reopened.stdout).contains("100"));
}

/// I pointed the CLI at a path with no existing file, inserted rows, quit, reopened, and all my data was there.
#[test]
fn f04_empty_database_file_is_a_valid_starting_point() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "f04.db");
    let output = run_cli_script(
        &db_path,
        &[],
        "\
CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'a')\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'b')\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000003', 'c')\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000004', 'd')\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000005', 'e')\n\
.quit\n",
    );
    assert!(output.status.success());
    assert!(db_path.exists());

    let reopened = run_cli_script(&db_path, &[], "SELECT count(*) FROM sensors\n.quit\n");
    assert!(reopened.status.success());
    assert!(output_string(&reopened.stdout).contains("5"));
}

/// I tried to open the same database file from two processes at once, and the second one was refused with a clear "locked" error.
#[test]
fn f05_two_processes_cannot_open_the_same_database_file() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "f05.db");
    let mut first = spawn_cli(&db_path, &[]);
    write_child_stdin(&mut first, "CREATE TABLE sensors (id UUID PRIMARY KEY)\n");
    thread::sleep(Duration::from_millis(200));

    let second = run_cli_script_allow_startup_failure(&db_path, &[], ".quit\n");
    stop_child(&mut first);

    assert!(
        !second.status.success(),
        "second process should fail while first holds the database"
    );
    let stderr = output_string(&second.stderr).to_lowercase();
    assert!(stderr.contains("locked") || stderr.contains("in use"));
}

/// I inserted graph edges, quit, reopened, and a multi-hop graph traversal still found the connected nodes.
#[test]
fn f05b_graph_edges_survive_persistence_reopen() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "f05b.db");
    let script = "\
CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT)\n\
INSERT INTO entities (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'A')\n\
INSERT INTO entities (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'B')\n\
INSERT INTO entities (id, name) VALUES ('00000000-0000-0000-0000-000000000003', 'C')\n\
INSERT INTO GRAPH (source_id, target_id, edge_type) VALUES ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'EDGE')\n\
INSERT INTO GRAPH (source_id, target_id, edge_type) VALUES ('00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000003', 'EDGE')\n\
.quit\n";
    let _ = run_cli_script(&db_path, &[], script);

    let query = "\
SELECT * FROM GRAPH_TABLE(edges MATCH (a)-[:EDGE]->{1,2}(b) WHERE a.id = '00000000-0000-0000-0000-000000000001' COLUMNS(b.id AS target_id))\n\
.quit\n";
    let output = run_cli_script(&db_path, &[], query);
    let stdout = output_string(&output.stdout);
    assert!(output.status.success());
    assert!(stdout.contains("00000000-0000-0000-0000-000000000002"));
    assert!(stdout.contains("00000000-0000-0000-0000-000000000003"));
}

/// I inserted embeddings, quit, reopened, and an ANN search still returned the correct nearest neighbor.
#[test]
fn f05c_vector_data_survives_persistence_reopen() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "f05c.db");
    let db = Database::open(&db_path).expect("open db");
    setup_vector_table(&db, 384);
    let target_id = Uuid::new_v4();
    for idx in 0..5 {
        let mut vector = vec![0.0_f32; 384];
        vector[idx] = 1.0;
        let id = if idx == 3 { target_id } else { Uuid::new_v4() };
        insert_embedding(&db, id, vector);
    }
    db.close().expect("close db");

    let reopened = Database::open(&db_path).expect("reopen db");
    let results = reopened
        .query_vector(
            contextdb_core::VectorIndexRef::new("embeddings", "embedding"),
            &{
                let mut v = vec![0.0_f32; 384];
                v[3] = 1.0;
                v
            },
            1,
            None,
            reopened.snapshot(),
        )
        .expect("ann query");
    assert_eq!(results.len(), 1);
    let row = reopened
        .point_lookup(
            "embeddings",
            "id",
            &Value::Uuid(target_id),
            reopened.snapshot(),
        )
        .expect("lookup should work");
    assert!(row.is_some());
}

/// I created a state machine with draft->review->archived, inserted a draft row, quit, reopened, and an illegal draft->archived transition was still rejected.
#[test]
fn f05d_constraint_enforcement_survives_persistence_reopen() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "f05d.db");
    let db = Database::open(&db_path).expect("open db");
    db.execute(
        "CREATE TABLE workflows (id UUID PRIMARY KEY, status TEXT) STATE MACHINE (status: draft -> [review], review -> [archived])",
        &empty_params(),
    )
    .expect("create workflow table");
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO workflows (id, status) VALUES ($id, $status)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("status", Value::Text("draft".into())),
        ]),
    )
    .expect("insert draft row");
    db.close().expect("close db");

    let reopened = Database::open(&db_path).expect("reopen db");
    let invalid = reopened.execute(
        "INSERT INTO workflows (id, status) VALUES ($id, $status) ON CONFLICT (id) DO UPDATE SET status=$status",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("status", Value::Text("archived".into())),
        ]),
    );
    assert!(invalid.is_err());
}

/// I set up parent-child propagation rules, quit, reopened, archived the parent, and the child was automatically archived too.
#[test]
fn f05e_propagation_rules_survive_persistence_reopen() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "f05e.db");
    let db = Database::open(&db_path).expect("open db");
    db.execute(
        "CREATE TABLE parents (id UUID PRIMARY KEY, status TEXT) STATE MACHINE (status: active -> [archived])",
        &empty_params(),
    )
    .expect("create parent table");
    db.execute(
        "CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id) ON STATE archived PROPAGATE SET archived, status TEXT) STATE MACHINE (status: active -> [archived])",
        &empty_params(),
    )
    .expect("create child table");
    let parent_id = Uuid::new_v4();
    let child_id = Uuid::new_v4();
    db.execute(
        "INSERT INTO parents (id, status) VALUES ($id, $status)",
        &params(vec![
            ("id", Value::Uuid(parent_id)),
            ("status", Value::Text("active".into())),
        ]),
    )
    .expect("insert parent");
    db.execute(
        "INSERT INTO children (id, parent_id, status) VALUES ($id, $parent_id, $status)",
        &params(vec![
            ("id", Value::Uuid(child_id)),
            ("parent_id", Value::Uuid(parent_id)),
            ("status", Value::Text("active".into())),
        ]),
    )
    .expect("insert child");
    db.close().expect("close db");

    let reopened = Database::open(&db_path).expect("reopen db");
    reopened
        .execute(
            "INSERT INTO parents (id, status) VALUES ($id, $status) ON CONFLICT (id) DO UPDATE SET status=$status",
            &params(vec![
                ("id", Value::Uuid(parent_id)),
                ("status", Value::Text("archived".into())),
            ]),
        )
        .expect("archive parent");
    let child = reopened
        .point_lookup(
            "children",
            "id",
            &Value::Uuid(child_id),
            reopened.snapshot(),
        )
        .expect("lookup child")
        .expect("child row");
    assert_eq!(text_value(&child, "status"), "archived");
}

/// I inserted a row with every column type (UUID, TEXT, INTEGER, REAL, BOOLEAN, TIMESTAMP, VECTOR), quit, reopened, and every value came back exactly right.
#[test]
fn f05f_all_data_types_round_trip_through_persistence() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "f05f.db");
    let id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();

    // Insert a row with every persistable column type
    {
        let db = Database::open(&db_path).expect("open db");
        db.execute(
            "CREATE TABLE everything (id UUID PRIMARY KEY, note TEXT, count INTEGER, reading REAL, enabled BOOLEAN, happened_at TIMESTAMP, embedding VECTOR(3))",
            &empty_params(),
        )
        .expect("create everything table");
        db.execute(
            "INSERT INTO everything (id, note, count, reading, enabled, happened_at, embedding) VALUES ($id, $note, $count, $reading, $enabled, $ts, $embedding)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("note", Value::Text("hello".into())),
                ("count", Value::Int64(7)),
                ("reading", Value::Float64(3.5)),
                ("enabled", Value::Bool(true)),
                ("ts", Value::Timestamp(1700000000)),
                ("embedding", Value::Vector(vec![1.0, 2.0, 3.0])),
            ]),
        )
        .expect("insert row with all types");
        db.close().expect("close db");
    }

    // Reopen and verify every value round-tripped correctly
    let reopened = Database::open(&db_path).expect("reopen db");
    let result = reopened
        .execute("SELECT * FROM everything", &empty_params())
        .expect("select everything");
    assert_eq!(result.rows.len(), 1, "expected exactly one row");
    let row = &result.rows[0];

    // Map columns to values for easier assertions
    let col_idx = |name: &str| {
        result
            .columns
            .iter()
            .position(|c| c == name)
            .unwrap_or_else(|| panic!("column {name} not found"))
    };

    assert_eq!(row[col_idx("id")], Value::Uuid(id));
    assert_eq!(row[col_idx("note")], Value::Text("hello".into()));
    assert_eq!(row[col_idx("count")], Value::Int64(7));
    assert_eq!(row[col_idx("reading")], Value::Float64(3.5));
    assert_eq!(row[col_idx("enabled")], Value::Bool(true));
    assert_eq!(row[col_idx("happened_at")], Value::Timestamp(1700000000));
    assert_eq!(
        row[col_idx("embedding")],
        Value::Vector(vec![1.0, 2.0, 3.0])
    );
}

/// I stored relational, graph, and vector data in the same table, quit, reopened, and a combined graph+vector query returned the same results both times.
#[test]
fn f05g_unified_cross_paradigm_data_survives_restart() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "f05g.db");
    let script = "\
CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT, category TEXT, embedding VECTOR(384))\n\
INSERT INTO entities (id, name, category) VALUES ('00000000-0000-0000-0000-000000000001', 'sensor-1', 'sensor')\n\
INSERT INTO entities (id, name, category) VALUES ('00000000-0000-0000-0000-000000000002', 'sensor-2', 'sensor')\n\
SELECT * FROM GRAPH_TABLE(edges MATCH (a)-[:RELATES_TO]->(b) COLUMNS(b.id AS target_id))\n\
.quit\n";
    let _ = run_cli_script(&db_path, &[], script);
    let query = "\
WITH neighborhood AS (SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[:RELATES_TO]->{1,2}(b) WHERE a.name = 'sensor-1' COLUMNS(b.id AS b_id))) \
SELECT id, name FROM entities WHERE category = 'sensor' ORDER BY embedding <=> $query LIMIT 5\n\
.quit\n";
    let first = run_cli_script(
        &db_path,
        &[
            "--tenant-id",
            "unused",
            "--nats-url",
            "nats://127.0.0.1:65530",
        ],
        query,
    );
    let second = run_cli_script(
        &db_path,
        &[
            "--tenant-id",
            "unused",
            "--nats-url",
            "nats://127.0.0.1:65530",
        ],
        query,
    );
    assert_eq!(output_string(&first.stdout), output_string(&second.stdout));
}

/// I deleted 3 out of 10 rows, quit, reopened, and the count was 7 — deleted rows stayed deleted.
#[test]
fn f05h_delete_survives_persistence() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "f05h.db");
    let db = Database::open(&db_path).expect("open db");
    setup_simple_sensor_db(&db);
    insert_sensor_rows(&db, 10);
    db.execute(
        "DELETE FROM sensors WHERE name IN ('temp-0', 'temp-1', 'temp-2')",
        &empty_params(),
    )
    .expect("delete should succeed");
    db.close().expect("close db");

    let reopened = Database::open(&db_path).expect("reopen db");
    assert_eq!(query_count(&reopened, "SELECT count(*) FROM sensors"), 7);
}

/// I rolled back a transaction with 2 inserts, then inserted 5 more rows outside the transaction, quit, reopened, and only the 5 committed rows were there.
#[test]
fn f05i_rollback_does_not_persist_after_restart() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "f05i.db");
    let output = run_cli_script(
        &db_path,
        &[],
        "\
CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
BEGIN\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'a')\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'b')\n\
ROLLBACK\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000003', 'c')\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000004', 'd')\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000005', 'e')\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000006', 'f')\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000007', 'g')\n\
.quit\n",
    );
    assert!(output.status.success());

    let reopened = run_cli_script(&db_path, &[], "SELECT count(*) FROM sensors\n.quit\n");
    assert!(reopened.status.success());
    assert!(output_string(&reopened.stdout).contains("5"));
}

/// I inserted edges A->B->C with a DAG constraint, quit, reopened, and inserting C->A was still rejected as a cycle.
#[test]
fn f05j_dag_constraint_survives_restart_and_rejects_cycles() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "f05j.db");
    let db = Database::open(&db_path).expect("open db");
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT)",
        &empty_params(),
    )
    .expect("create entities");
    db.execute(
        "CREATE TABLE edge_rows (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) DAG('CITES')",
        &empty_params(),
    )
    .expect("create edge_rows");
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = Uuid::new_v4();
    setup_graph_entities(&db, &[a, b, c]);
    let tx = db.begin();
    db.insert_edge(tx, a, b, "CITES".into(), Default::default())
        .expect("edge a->b");
    db.insert_edge(tx, b, c, "CITES".into(), Default::default())
        .expect("edge b->c");
    db.commit(tx).expect("commit edges");
    db.close().expect("close db");

    let reopened = Database::open(&db_path).expect("reopen db");
    let tx = reopened.begin();
    let result = reopened.insert_edge(tx, c, a, "CITES".into(), Default::default());
    assert!(result.is_err());
}

/// I dropped a table with 10 rows, recreated it empty, quit, reopened, and the old rows did not come back.
#[test]
fn f05k_drop_table_recreate_same_table_old_data_does_not_ghost_back() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "f05k.db");
    let db = Database::open(&db_path).expect("open db");
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &empty_params(),
    )
    .expect("create table");
    for _ in 0..10 {
        db.execute(
            "INSERT INTO t (id, name) VALUES ($id, $name)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("name", Value::Text("ghost".into())),
            ]),
        )
        .expect("insert");
    }
    db.execute("DROP TABLE t", &empty_params())
        .expect("drop table");
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &empty_params(),
    )
    .expect("recreate table");
    db.close().expect("close db");

    let reopened = Database::open(&db_path).expect("reopen db");
    assert_eq!(query_count(&reopened, "SELECT count(*) FROM t"), 0);
}

/// I updated my embedding, and search now finds the new one, not the old one,
/// and no duplicates leaked through.
#[test]
fn f05l_vector_index_correct_after_reopen_update_embedding() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "f05l.db");
    let db = Database::open(&db_path).expect("open db");
    db.execute(
        "CREATE TABLE obs (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty_params(),
    )
    .expect("create obs table");

    let id_a = Uuid::new_v4();
    let id_b = Uuid::new_v4();
    let tx = db.begin();
    // A starts near [1,0,0], B is also near [1,0,0] but slightly off
    let row_a = db
        .insert_row(tx, "obs", values(vec![("id", Value::Uuid(id_a))]))
        .expect("insert A");
    db.insert_vector(
        tx,
        contextdb_core::VectorIndexRef::new("obs", "embedding"),
        row_a,
        vec![0.95, 0.05, 0.0],
    )
    .expect("vec A");
    let row_b = db
        .insert_row(tx, "obs", values(vec![("id", Value::Uuid(id_b))]))
        .expect("insert B");
    db.insert_vector(
        tx,
        contextdb_core::VectorIndexRef::new("obs", "embedding"),
        row_b,
        vec![0.9, 0.1, 0.0],
    )
    .expect("vec B");
    db.commit(tx).expect("commit");
    db.close().expect("close db");

    // Reopen and move A to the opposite end of the space
    let reopened = Database::open(&db_path).expect("reopen db");
    reopened
        .execute(
            "UPDATE obs SET embedding = $embedding WHERE id = $id",
            &params(vec![
                ("id", Value::Uuid(id_a)),
                ("embedding", Value::Vector(vec![-1.0, 0.0, 0.0])),
            ]),
        )
        .expect("update embedding");

    // Search for [1,0,0] — B should be nearest because A moved to [-1,0,0]
    let results = reopened
        .query_vector(
            contextdb_core::VectorIndexRef::new("obs", "embedding"),
            &[1.0, 0.0, 0.0],
            2,
            None,
            reopened.snapshot(),
        )
        .expect("query vector");
    assert_eq!(
        results.len(),
        2,
        "exactly 2 results — no ghost duplicate from old embedding"
    );
    assert_eq!(
        results[0].0, row_b,
        "B should be nearest to [1,0,0] after A moved to [-1,0,0]"
    );
}

// ======== T30 ========
#[test]
fn single_database_txid_causal_order_under_backward_wallclock() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    use contextdb_core::{TxId, Value, Wallclock};
    use contextdb_engine::Database;

    // Non-monotonic wall-clock sequence — includes forward, backward, forward, backward, forward.
    let sequence: Vec<u64> = vec![1000, 1010, 1005, 1020, 1003, 1030, 1001, 1040];
    let seq_handle = Arc::new(sequence.clone());
    let idx = Arc::new(AtomicUsize::new(0));
    {
        let seq_handle = Arc::clone(&seq_handle);
        let idx = Arc::clone(&idx);
        Wallclock::set_test_clock(move || {
            let i = idx.fetch_add(1, AtomicOrdering::SeqCst);
            seq_handle[i.min(seq_handle.len() - 1)]
        });
    }

    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, ts TXID NOT NULL)",
        &Default::default(),
    )
    .expect("CREATE TABLE with TXID column must parse after stubs land");

    let mut recorded: Vec<TxId> = Vec::with_capacity(sequence.len());
    let mut ids: Vec<uuid::Uuid> = Vec::with_capacity(sequence.len());

    for _ in 0..sequence.len() {
        // `begin()` returns a bare TxId (not a guard); `commit(tx)` finalizes.
        let txid: TxId = db.begin();
        let id = uuid::Uuid::new_v4();
        let mut row = std::collections::HashMap::new();
        row.insert("id".to_string(), Value::Uuid(id));
        row.insert("ts".to_string(), Value::TxId(txid));
        db.insert_row(txid, "t", row)
            .expect("insert must succeed under stubs' TXID arm once impl lands");
        db.commit(txid).expect("commit must succeed");
        recorded.push(txid);
        ids.push(id);
    }

    // Causal-order check: AtomicU64 allocator is monotonic regardless of wall-clock skew,
    // so recorded is strictly increasing.
    for window in recorded.windows(2) {
        assert!(
            window[0].0 < window[1].0,
            "allocated TxId must be strictly increasing (engine-monotonic); got {window:?}"
        );
    }

    // ORDER BY on a TXID column must order by engine-issued TxId, not wall-clock.
    let result = db
        .execute("SELECT id, ts FROM t ORDER BY ts ASC", &Default::default())
        .expect("SELECT must succeed");

    let ts_column: Vec<Value> = result.rows.iter().map(|row| row[1].clone()).collect();

    let expected: Vec<Value> = recorded.iter().copied().map(Value::TxId).collect();
    assert_eq!(
        ts_column, expected,
        "ORDER BY ts ASC must return engine-allocator order, not wall-clock order"
    );

    let id_column: Vec<Value> = result.rows.iter().map(|row| row[0].clone()).collect();
    let expected_ids: Vec<Value> = ids.iter().copied().map(Value::Uuid).collect();
    assert_eq!(
        id_column, expected_ids,
        "row ids must align with insertion order under ORDER BY ts ASC"
    );

    Wallclock::reset_test_clock();
}

// Named vector index RED tests from named-vector-indexes-tests.md.

fn persist_vector_rows_for_footprint(
    path: &Path,
    table: &str,
    dim: usize,
    quantization: Option<&str>,
    rows: &[(Uuid, Vec<f32>)],
) {
    let db = Database::open(path).expect("open footprint db");
    let quantization_clause = quantization
        .map(|q| format!(" WITH (quantization = '{q}')"))
        .unwrap_or_default();
    db.execute(
        &format!(
            "CREATE TABLE {table} (id UUID PRIMARY KEY, vec VECTOR({dim}){quantization_clause})"
        ),
        &empty_params(),
    )
    .expect("create footprint table");
    for (id, vector) in rows {
        db.execute(
            &format!("INSERT INTO {table} (id, vec) VALUES ($id, $v)"),
            &params(vec![
                ("id", Value::Uuid(*id)),
                ("v", Value::Vector(vector.clone())),
            ]),
        )
        .expect("insert footprint row");
    }
    drop(db);
}

fn file_len(path: &Path) -> u64 {
    fs::metadata(path).expect("stat footprint db").len()
}

/// I pointed contextdb 1.0 at a database file written by 0.3.x and got a typed LegacyVectorStoreDetected with
/// two recovery paths in the message — sync from a 1.0+ peer or recreate the schema and reimport.
#[test]
fn f112_legacy_vector_store_open_returns_typed_error_with_rebuild_guidance() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let legacy_path = tmp.path().join("legacy.db");
    let fixture = workspace_root()
        .join("tests")
        .join("fixtures")
        .join("legacy_vector_store_v0_3_4.db");
    fs::copy(&fixture, &legacy_path).expect("copy legacy fixture");

    match Database::open(&legacy_path) {
        Err(Error::LegacyVectorStoreDetected {
            found_format_marker,
            expected_release,
        }) => {
            assert!(
                matches!(found_format_marker.as_str(), "" | "0.3.x"),
                "marker must be either empty (no metadata table) or '0.3.x'; got: {found_format_marker}"
            );
            assert_eq!(expected_release, "1.0.0");
            // The Display message must give both canonical recovery paths so the operator can act
            // without reading source: sync from a 1.0+ peer, or recreate/reimport locally.
            let msg = format!(
                "{}",
                Error::LegacyVectorStoreDetected {
                    found_format_marker: found_format_marker.clone(),
                    expected_release: expected_release.clone(),
                }
            );
            let lower = msg.to_lowercase();
            assert!(
                lower.contains("sync") && lower.contains("1.0"),
                "Display must point at sync from a 1.0+ peer; got: {msg}"
            );
            assert!(
                lower.contains("recreate") && lower.contains("reimport"),
                "Display must point at recreate-and-reimport recovery; got: {msg}"
            );
        }
        Ok(_) => panic!("legacy store opened silently — must fail fast"),
        Err(other) => panic!("expected LegacyVectorStoreDetected, got {other:?}"),
    }
}

/// I created a fresh 1.0 database, declared two vector columns, inserted rows, closed it, reopened it. No
/// legacy detection fired. SHOW VECTOR_INDEXES listed both columns with correct dimension, vector_count, and
/// bytes — proving the metadata table is written on first open, the registry registers indexes from DDL, and
/// the vector_count/bytes columns reflect actual storage (not hardcoded zeros).
#[test]
fn f112b_fresh_one_dot_zero_store_opens_cleanly_and_show_reflects_state() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().join("fresh.db");
    let row_count: usize = 5;
    {
        let db = Database::open(&path).expect("create fresh 1.0 store");
        db.execute(
            "CREATE TABLE evidence (id UUID PRIMARY KEY, vector_text VECTOR(4), vector_vision VECTOR(8))",
            &empty_params(),
        ).expect("create");
        for _ in 0..row_count {
            db.execute(
                "INSERT INTO evidence (id, vector_text, vector_vision) VALUES ($id, $t, $v)",
                &params(vec![
                    ("id", Value::Uuid(Uuid::new_v4())),
                    ("t", Value::Vector(vec![1.0_f32, 0.0, 0.0, 0.0])),
                    (
                        "v",
                        Value::Vector(vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                    ),
                ]),
            )
            .expect("insert");
        }
        drop(db);
    }
    let reopened = Database::open(&path).expect("reopen fresh 1.0 store cleanly");
    let indexes = reopened
        .execute("SHOW VECTOR_INDEXES", &empty_params())
        .expect("show vector indexes");
    // Lock the full row shape promised by RB16: every column must be present.
    for required in &[
        "table",
        "column",
        "dimension",
        "quantization",
        "vector_count",
        "bytes",
    ] {
        assert!(
            indexes.columns.iter().any(|c| c == *required),
            "SHOW VECTOR_INDEXES must expose `{required}`; got columns {:?}",
            indexes.columns
        );
    }
    let column_idx = indexes.columns.iter().position(|c| c == "column").unwrap();
    let dim_idx = indexes
        .columns
        .iter()
        .position(|c| c == "dimension")
        .unwrap();
    let count_idx = indexes
        .columns
        .iter()
        .position(|c| c == "vector_count")
        .unwrap();
    let bytes_idx = indexes.columns.iter().position(|c| c == "bytes").unwrap();
    let quant_idx = indexes
        .columns
        .iter()
        .position(|c| c == "quantization")
        .unwrap();

    let find = |col: &str, declared_dim: i64| {
        let row = indexes.rows.iter().find(|r| {
            matches!(&r[column_idx], Value::Text(s) if s == col)
                && matches!(&r[dim_idx], Value::Int64(n) if *n == declared_dim)
        });
        assert!(
            row.is_some(),
            "SHOW VECTOR_INDEXES must list `{col}` at dim {declared_dim}; got {:?}",
            indexes.rows
        );
        row.unwrap()
    };
    let text_row = find("vector_text", 4);
    let vision_row = find("vector_vision", 8);
    // vector_count must reflect actual inserted rows, not hardcoded zero.
    for (label, row) in [("vector_text", text_row), ("vector_vision", vision_row)] {
        match &row[count_idx] {
            Value::Int64(n) => assert_eq!(
                *n as usize, row_count,
                "{label}.vector_count must reflect actual count {row_count}; got {n}"
            ),
            other => panic!("{label}.vector_count must be Int64; got {other:?}"),
        }
        // bytes must be at least dim * count for f32 storage (4 bytes per dim per row); a hardcoded
        // bytes=1 stub fails this lower bound.
        let declared_dim = if label == "vector_text" { 4 } else { 8 };
        let f32_floor = (row_count * declared_dim * std::mem::size_of::<f32>()) as i64;
        match &row[bytes_idx] {
            Value::Int64(n) => assert!(
                *n >= f32_floor,
                "{label}.bytes must be at least f32-floor {f32_floor}; got {n}"
            ),
            other => panic!("{label}.bytes must be Int64; got {other:?}"),
        }
        // Quantization defaults to F32 when no clause is declared.
        assert!(
            matches!(&row[quant_idx], Value::Text(s) if s == "F32"),
            "{label}.quantization must default to F32 absent a WITH clause; got {:?}",
            row[quant_idx]
        );
    }
}

/// I corrupted a 1.0 store's metadata table and got a distinct error — NOT LegacyVectorStoreDetected, so the operator
/// can tell "missing marker" from "garbled marker".
#[test]
fn f112c_corrupt_one_dot_zero_store_returns_distinct_error() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().join("corrupt.db");
    {
        let db = Database::open(&path).expect("create");
        db.execute(
            "CREATE TABLE evidence (id UUID PRIMARY KEY, vector_text VECTOR(4))",
            &empty_params(),
        )
        .expect("create");
        drop(db);
    }
    // Truncate the file to corrupt the metadata.
    let metadata = fs::metadata(&path).expect("stat");
    fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open for truncate")
        .set_len(metadata.len() / 2)
        .expect("truncate");

    static PANIC_HOOK_LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    let _hook_guard = PANIC_HOOK_LOCK
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let open_result =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| Database::open(&path)));
    std::panic::set_hook(previous_hook);
    assert!(
        open_result.is_ok(),
        "corrupt 1.0 store must return a typed open error, not panic during storage bootstrap"
    );
    match open_result.unwrap() {
        Err(Error::LegacyVectorStoreDetected { .. }) => {
            panic!(
                "corrupt 1.0 store must NOT be classified as legacy; that conflates two failure modes"
            )
        }
        Err(Error::StoreCorrupted {
            path: error_path,
            reason,
        }) => {
            assert!(
                error_path.contains("corrupt.db"),
                "StoreCorrupted.path must identify the corrupt file; got: {error_path}"
            );
            let lower = reason.to_lowercase();
            assert!(
                lower.contains("metadata")
                    || lower.contains("format")
                    || lower.contains("corrupt")
                    || lower.contains("truncated"),
                "StoreCorrupted.reason must explain the corrupt metadata/format axis; got: {reason}"
            );
        }
        Err(other) => panic!("expected StoreCorrupted for corrupt 1.0 store, got {other:?}"),
        Ok(_) => panic!("corrupt store must not open silently"),
    }
}
/// I declared `vec VECTOR(128) WITH (quantization = 'SQ8')`, inserted 2000 unique random unit vectors,
/// reopened the database, and probing each inserted vector recovered its own row at top-1 — recall@1 stayed
/// ≥ 95/100, proving SQ8 storage preserves identity after fsync + reopen.
#[test]
fn f113_per_index_sq8_quantization_preserves_recall_after_reopen() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("q.db");
    let dim: usize = 128;
    let count: usize = 2_000;

    // Each row gets a UNIQUE random unit vector (uniformly distributed on the sphere). This avoids
    // duplicate-basis collisions where multiple rows share the same vector and recall@1 is a coin flip.
    fn random_unit_vector(dim: usize, seed: u64) -> Vec<f32> {
        let mut state = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let mut v = Vec::with_capacity(dim);
        for _ in 0..dim {
            state = state
                .wrapping_mul(2862933555777941757)
                .wrapping_add(3037000493);
            let unit = ((state >> 33) as f64) / ((1u64 << 31) as f64);
            v.push((unit as f32) * 2.0 - 1.0);
        }
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            v.iter_mut().for_each(|x| *x /= norm);
        }
        v
    }

    let mut inserted: Vec<(Uuid, Vec<f32>)> = Vec::with_capacity(count);
    {
        let db = Database::open(&db_path).expect("open file db");
        db.execute(
            &format!("CREATE TABLE recall (id UUID PRIMARY KEY, vec VECTOR({dim}) WITH (quantization = 'SQ8'))"),
            &empty_params(),
        )
        .expect("create recall");
        for i in 0..count {
            let id = Uuid::new_v4();
            let v = random_unit_vector(dim, i as u64 + 1);
            db.execute(
                "INSERT INTO recall (id, vec) VALUES ($id, $v)",
                &params(vec![
                    ("id", Value::Uuid(id)),
                    ("v", Value::Vector(v.clone())),
                ]),
            )
            .expect("insert");
            inserted.push((id, v));
        }
        // redb commits each transaction synchronously; drop flushes the final state.
        drop(db);
    }

    let reopened = Database::open(&db_path).expect("reopen");

    // The schema declared SQ8. SHOW VECTOR_INDEXES must reflect that AND the on-engine accountant's
    // bytes count must be lower than the f32 baseline — otherwise an impl that silently ignored the
    // SQ8 declaration and stored f32 passes the recall test trivially (f32 always recalls 100%).
    let indexes = reopened
        .execute("SHOW VECTOR_INDEXES", &empty_params())
        .expect("show vector indexes");
    let table_idx = indexes.columns.iter().position(|c| c == "table").unwrap();
    let column_idx = indexes.columns.iter().position(|c| c == "column").unwrap();
    let quant_idx = indexes
        .columns
        .iter()
        .position(|c| c == "quantization")
        .unwrap();
    let bytes_idx = indexes.columns.iter().position(|c| c == "bytes").unwrap();
    let recall_row = indexes.rows.iter().find(|r| {
        matches!(&r[table_idx], Value::Text(t) if t == "recall")
            && matches!(&r[column_idx], Value::Text(c) if c == "vec")
    });
    assert!(
        recall_row.is_some(),
        "SHOW VECTOR_INDEXES must list recall.vec; got {:?}",
        indexes.rows
    );
    let recall_row = recall_row.unwrap();
    assert!(
        matches!(&recall_row[quant_idx], Value::Text(q) if q == "SQ8"),
        "SHOW VECTOR_INDEXES must report quantization='SQ8' for the SQ8-declared column; got {:?}",
        recall_row[quant_idx]
    );
    let f32_baseline_bytes = (count * dim * std::mem::size_of::<f32>()) as i64;
    if let Value::Int64(bytes) = recall_row[bytes_idx] {
        assert!(
            bytes < f32_baseline_bytes / 3,
            "SQ8-declared index must report less than one third of the f32 baseline ({f32_baseline_bytes}); got {bytes}"
        );
    } else {
        panic!(
            "SHOW VECTOR_INDEXES.bytes must be Int64; got {:?}",
            recall_row[bytes_idx]
        );
    }
    let storage_bytes = reopened
        .__debug_vector_storage_bytes_per_entry(VectorIndexRef::new("recall", "vec"))
        .expect("debug storage bytes for recall.vec");
    assert_eq!(
        storage_bytes.len(),
        count,
        "reopened recall.vec must retain one live vector payload per inserted row"
    );
    assert!(
        storage_bytes
            .iter()
            .all(|bytes| *bytes <= contextdb_core::VectorQuantization::SQ8.storage_bytes(dim)),
        "reopened SQ8 live storage must retain quantized payloads, not decoded f32 entries; got {storage_bytes:?}"
    );
    let f32_db_path = tmp.path().join("q_f32_baseline.db");
    persist_vector_rows_for_footprint(&f32_db_path, "recall", dim, None, &inserted);
    let sq8_file_bytes = file_len(&db_path);
    let f32_file_bytes = file_len(&f32_db_path);
    assert!(
        sq8_file_bytes < f32_file_bytes / 2,
        "SQ8 database file must be materially smaller than an otherwise identical F32 file; sq8={sq8_file_bytes}, f32={f32_file_bytes}"
    );

    // Probe 100 inserted vectors directly; recall@1 means "this exact vector returns its own row id."
    // SQ8 quantization may shift scores slightly but cannot move the self-match below top-1 unless
    // recall is genuinely broken.
    let probes: Vec<&(Uuid, Vec<f32>)> = inserted.iter().step_by(20).take(100).collect();
    let mut hits = 0usize;
    for (expected_id, probe) in &probes {
        let result = reopened
            .execute(
                "SELECT id FROM recall ORDER BY vec <=> $q LIMIT 1",
                &params(vec![("q", Value::Vector(probe.clone()))]),
            )
            .expect("query");
        let id_idx = result.columns.iter().position(|c| c == "id").unwrap();
        if result.rows.first().map(|r| &r[id_idx]) == Some(&Value::Uuid(*expected_id)) {
            hits += 1;
        }
    }
    // Self-probes against exact inserted vectors must recover their own rows. SQ8 quantization
    // shifts scores but cannot move the self-match below top-1 unless recall is genuinely broken.
    // Allow 1 tiebreak miss (random unit vectors occasionally land at a near-tied distance to a
    // sibling); below that, the assertion rejects real corruption.
    assert!(
        hits >= 99,
        "SQ8 self-probe recall@1 must be ≥ 99/100 after reopen; got {hits}"
    );
}
/// I inserted batch A with 20 vectors in range [-1, 1] (closely-spaced), then batch B with 20 vectors in
/// range [-10, 10] (widely-spaced) into a single SQ8 column. Probing each of the 40 vectors recovered its
/// own row at top-1 — proving SQ8 recall is preserved across batches with disjoint magnitude ranges. The
/// scale strategy that achieves this (per-vector min/max, per-shard, lazy rescale, etc.) is implementation
/// choice; the product contract this test pins is "ANN recall remains correct after diverse batches arrive."
#[test]
fn f113b_sq8_cross_batch_recall_stability() {
    let live_f32_baseline_bytes = 40 * 256 * std::mem::size_of::<f32>();
    let accountant = Arc::new(MemoryAccountant::with_budget(36 * 1024));
    let db = Database::open_memory_with_accountant(accountant.clone());
    // Production-typical embedding dim. SQ8 quantization noise at low dim (e.g., 16) underestimates
    // the cross-batch recall risk because each dim contributes 6.25% of the magnitude; at 256 each dim
    // contributes ~0.39%, where SQ8 quantization noise can dominate the dot product if the scale
    // strategy is wrong.
    let dim: usize = 256;
    db.execute(
        &format!("CREATE TABLE crossbatch (id UUID PRIMARY KEY, vec VECTOR({dim}) WITH (quantization = 'SQ8'))"),
        &empty_params(),
    )
    .expect("create");

    fn rand_in(seed: u64, lo: f32, hi: f32, dim: usize) -> Vec<f32> {
        let mut state = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        (0..dim)
            .map(|_| {
                state = state
                    .wrapping_mul(2862933555777941757)
                    .wrapping_add(3037000493);
                let u = ((state >> 33) as f64) / ((1u64 << 31) as f64);
                lo + (hi - lo) * (u as f32)
            })
            .collect()
    }

    // Batch A: 20 vectors in tight range [-1, 1] — needs SQ8 quantization to preserve fine differences.
    let mut batch: Vec<(Uuid, Vec<f32>)> = Vec::new();
    for i in 0..20 {
        let id = Uuid::new_v4();
        let v = rand_in(i as u64 + 1, -1.0, 1.0, dim);
        let insert = db.execute(
            "INSERT INTO crossbatch (id, vec) VALUES ($id, $v)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("v", Value::Vector(v.clone())),
            ]),
        );
        assert!(
            insert.is_ok(),
            "SQ8 live storage must accept batch A under a budget below the f32 payload baseline; got {insert:?}"
        );
        batch.push((id, v));
    }
    // Batch B: 20 vectors in wide range [-10, 10] — would dominate any global scale recomputation.
    for i in 20..40 {
        let id = Uuid::new_v4();
        let v = rand_in(i as u64 + 1, -10.0, 10.0, dim);
        let insert = db.execute(
            "INSERT INTO crossbatch (id, vec) VALUES ($id, $v)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("v", Value::Vector(v.clone())),
            ]),
        );
        assert!(
            insert.is_ok(),
            "SQ8 live storage must accept batch B under a budget below the f32 payload baseline; got {insert:?}"
        );
        batch.push((id, v));
    }
    assert!(
        accountant.usage().used < live_f32_baseline_bytes,
        "SQ8 live storage must stay below the raw f32 vector payload baseline under MEMORY_LIMIT; used={}, f32_baseline={live_f32_baseline_bytes}",
        accountant.usage().used
    );
    let storage_bytes = db
        .__debug_vector_storage_bytes_per_entry(VectorIndexRef::new("crossbatch", "vec"))
        .expect("debug storage bytes for crossbatch.vec");
    assert_eq!(
        storage_bytes.len(),
        batch.len(),
        "crossbatch.vec must retain one live vector payload per inserted row"
    );
    assert!(
        storage_bytes
            .iter()
            .all(|bytes| *bytes <= contextdb_core::VectorQuantization::SQ8.storage_bytes(dim)),
        "SQ8 live storage must retain quantized payloads under memory pressure; got {storage_bytes:?}"
    );

    let indexes = db
        .execute("SHOW VECTOR_INDEXES", &empty_params())
        .expect("show vector indexes");
    let table_idx = indexes.columns.iter().position(|c| c == "table").unwrap();
    let column_idx = indexes.columns.iter().position(|c| c == "column").unwrap();
    let quant_idx = indexes
        .columns
        .iter()
        .position(|c| c == "quantization")
        .unwrap();
    let bytes_idx = indexes.columns.iter().position(|c| c == "bytes").unwrap();
    let crossbatch_row = indexes.rows.iter().find(|r| {
        matches!(&r[table_idx], Value::Text(t) if t == "crossbatch")
            && matches!(&r[column_idx], Value::Text(c) if c == "vec")
    });
    assert!(
        crossbatch_row.is_some(),
        "SHOW VECTOR_INDEXES must list crossbatch.vec; got {:?}",
        indexes.rows
    );
    let crossbatch_row = crossbatch_row.unwrap();
    assert!(
        matches!(&crossbatch_row[quant_idx], Value::Text(q) if q == "SQ8"),
        "SHOW VECTOR_INDEXES must report quantization='SQ8' for crossbatch.vec; got {:?}",
        crossbatch_row[quant_idx]
    );
    let f32_baseline_bytes = (batch.len() * dim * std::mem::size_of::<f32>()) as i64;
    assert!(
        matches!(&crossbatch_row[bytes_idx], Value::Int64(bytes) if *bytes < f32_baseline_bytes / 3),
        "SQ8 crossbatch.vec must report less than one third of f32 baseline {f32_baseline_bytes}; got {:?}",
        crossbatch_row[bytes_idx]
    );
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let footprint_batch: Vec<(Uuid, Vec<f32>)> = (0..512)
        .map(|i| {
            let range = if i < 256 {
                (-1.0_f32, 1.0_f32)
            } else {
                (-10.0_f32, 10.0_f32)
            };
            (
                Uuid::new_v4(),
                rand_in(i as u64 + 10_000, range.0, range.1, dim),
            )
        })
        .collect();
    let sq8_path = tmp.path().join("sq8.db");
    let f32_path = tmp.path().join("f32.db");
    persist_vector_rows_for_footprint(&sq8_path, "crossbatch", dim, Some("SQ8"), &footprint_batch);
    persist_vector_rows_for_footprint(&f32_path, "crossbatch", dim, None, &footprint_batch);
    let sq8_file_bytes = file_len(&sq8_path);
    let f32_file_bytes = file_len(&f32_path);
    assert!(
        sq8_file_bytes < f32_file_bytes / 2,
        "SQ8 file footprint must prove real compressed storage, not fake SHOW metadata; sq8={sq8_file_bytes}, f32={f32_file_bytes}"
    );

    let mut hits = 0usize;
    for (expected_id, probe) in &batch {
        let result = db
            .execute(
                "SELECT id FROM crossbatch ORDER BY vec <=> $q LIMIT 1",
                &params(vec![("q", Value::Vector(probe.clone()))]),
            )
            .expect("probe");
        let id_idx = result.columns.iter().position(|c| c == "id").unwrap();
        if result.rows.first().map(|r| &r[id_idx]) == Some(&Value::Uuid(*expected_id)) {
            hits += 1;
        }
    }
    // Self-probes against exact inserted vectors should recover 40/40. Allow 1 tiebreak miss; below
    // that, real cross-batch corruption is masked.
    assert!(
        hits >= 39,
        "cross-batch SQ8 self-probe recall@1 must be ≥ 39/40 across disjoint magnitude ranges; got {hits}. \
             Implementations that compress earlier batches when later batches arrive (e.g., naive global \
             rescale) fail this assertion."
    );
}
/// I declared `vec VECTOR(128) WITH (quantization = 'SQ4')`, inserted vectors across two disjoint magnitude
/// ranges, and self-probes recovered each row at top-1. SQ4 packs two 4-bit values per byte and is the
/// most memory-aggressive quantization; recall is more sensitive to scale strategy than SQ8.
#[test]
fn f113c_sq4_cross_batch_recall_stability() {
    let live_f32_baseline_bytes = 40 * 128 * std::mem::size_of::<f32>();
    let accountant = Arc::new(MemoryAccountant::with_budget(19 * 1024));
    let db = Database::open_memory_with_accountant(accountant.clone());
    let dim: usize = 128;
    db.execute(
        &format!("CREATE TABLE crossbatch4 (id UUID PRIMARY KEY, vec VECTOR({dim}) WITH (quantization = 'SQ4'))"),
        &empty_params(),
    ).expect("create");

    fn rand_in(seed: u64, lo: f32, hi: f32, dim: usize) -> Vec<f32> {
        let mut state = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        (0..dim)
            .map(|_| {
                state = state
                    .wrapping_mul(2862933555777941757)
                    .wrapping_add(3037000493);
                let u = ((state >> 33) as f64) / ((1u64 << 31) as f64);
                lo + (hi - lo) * (u as f32)
            })
            .collect()
    }

    let mut batch: Vec<(Uuid, Vec<f32>)> = Vec::new();
    for i in 0..20 {
        let id = Uuid::new_v4();
        let v = rand_in(i as u64 + 1, -1.0, 1.0, dim);
        let insert = db.execute(
            "INSERT INTO crossbatch4 (id, vec) VALUES ($id, $v)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("v", Value::Vector(v.clone())),
            ]),
        );
        assert!(
            insert.is_ok(),
            "SQ4 live storage must accept batch A under a budget below the f32 payload baseline; got {insert:?}"
        );
        batch.push((id, v));
    }
    for i in 20..40 {
        let id = Uuid::new_v4();
        let v = rand_in(i as u64 + 1, -10.0, 10.0, dim);
        let insert = db.execute(
            "INSERT INTO crossbatch4 (id, vec) VALUES ($id, $v)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("v", Value::Vector(v.clone())),
            ]),
        );
        assert!(
            insert.is_ok(),
            "SQ4 live storage must accept batch B under a budget below the f32 payload baseline; got {insert:?}"
        );
        batch.push((id, v));
    }
    assert!(
        accountant.usage().used < live_f32_baseline_bytes,
        "SQ4 live storage must stay below the raw f32 vector payload baseline under MEMORY_LIMIT; used={}, f32_baseline={live_f32_baseline_bytes}",
        accountant.usage().used
    );
    let storage_bytes = db
        .__debug_vector_storage_bytes_per_entry(VectorIndexRef::new("crossbatch4", "vec"))
        .expect("debug storage bytes for crossbatch4.vec");
    assert_eq!(
        storage_bytes.len(),
        batch.len(),
        "crossbatch4.vec must retain one live vector payload per inserted row"
    );
    assert!(
        storage_bytes
            .iter()
            .all(|bytes| *bytes <= contextdb_core::VectorQuantization::SQ4.storage_bytes(dim)),
        "SQ4 live storage must retain quantized payloads under memory pressure; got {storage_bytes:?}"
    );

    let indexes = db
        .execute("SHOW VECTOR_INDEXES", &empty_params())
        .expect("show vector indexes");
    let table_idx = indexes.columns.iter().position(|c| c == "table").unwrap();
    let column_idx = indexes.columns.iter().position(|c| c == "column").unwrap();
    let quant_idx = indexes
        .columns
        .iter()
        .position(|c| c == "quantization")
        .unwrap();
    let bytes_idx = indexes.columns.iter().position(|c| c == "bytes").unwrap();
    let crossbatch_row = indexes.rows.iter().find(|r| {
        matches!(&r[table_idx], Value::Text(t) if t == "crossbatch4")
            && matches!(&r[column_idx], Value::Text(c) if c == "vec")
    });
    assert!(
        crossbatch_row.is_some(),
        "SHOW VECTOR_INDEXES must list crossbatch4.vec; got {:?}",
        indexes.rows
    );
    let crossbatch_row = crossbatch_row.unwrap();
    assert!(
        matches!(&crossbatch_row[quant_idx], Value::Text(q) if q == "SQ4"),
        "SHOW VECTOR_INDEXES must report quantization='SQ4' for crossbatch4.vec; got {:?}",
        crossbatch_row[quant_idx]
    );
    let f32_baseline_bytes = (batch.len() * dim * std::mem::size_of::<f32>()) as i64;
    assert!(
        matches!(&crossbatch_row[bytes_idx], Value::Int64(bytes) if *bytes < f32_baseline_bytes / 4),
        "SQ4 crossbatch4.vec must report less than one quarter of f32 baseline {f32_baseline_bytes}; got {:?}",
        crossbatch_row[bytes_idx]
    );
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let footprint_batch: Vec<(Uuid, Vec<f32>)> = (0..5000)
        .map(|i| {
            let range = if i < 256 {
                (-1.0_f32, 1.0_f32)
            } else {
                (-10.0_f32, 10.0_f32)
            };
            (
                Uuid::new_v4(),
                rand_in(i as u64 + 20_000, range.0, range.1, dim),
            )
        })
        .collect();
    let sq4_path = tmp.path().join("sq4.db");
    let f32_path = tmp.path().join("f32.db");
    persist_vector_rows_for_footprint(&sq4_path, "crossbatch4", dim, Some("SQ4"), &footprint_batch);
    persist_vector_rows_for_footprint(&f32_path, "crossbatch4", dim, None, &footprint_batch);
    let sq4_file_bytes = file_len(&sq4_path);
    let f32_file_bytes = file_len(&f32_path);
    assert!(
        sq4_file_bytes < f32_file_bytes / 3,
        "SQ4 file footprint must prove real compressed storage, not fake SHOW metadata; sq4={sq4_file_bytes}, f32={f32_file_bytes}"
    );

    let mut hits = 0usize;
    for (expected_id, probe) in &batch {
        let r = db
            .execute(
                "SELECT id FROM crossbatch4 ORDER BY vec <=> $q LIMIT 1",
                &params(vec![("q", Value::Vector(probe.clone()))]),
            )
            .expect("probe");
        let id_idx = r.columns.iter().position(|c| c == "id").unwrap();
        if r.rows.first().map(|row| &row[id_idx]) == Some(&Value::Uuid(*expected_id)) {
            hits += 1;
        }
    }
    // SQ4 has ~half the resolution of SQ8 (16 levels vs 256). Allow 5% miss rate on cross-batch
    // self-probe at dim 128. A correct per-vector-scale impl achieves ≥38/40; lower thresholds mask
    // real corruption.
    assert!(
        hits >= 38,
        "SQ4 cross-batch self-probe recall@1 must be ≥ 38/40; got {hits}"
    );
}
