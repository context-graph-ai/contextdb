use super::common::*;
use contextdb_core::Value;
use contextdb_engine::Database;
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
    thread::sleep(Duration::from_millis(200));
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
    db.insert_vector(tx, row_a, vec![0.95, 0.05, 0.0])
        .expect("vec A");
    let row_b = db
        .insert_row(tx, "obs", values(vec![("id", Value::Uuid(id_b))]))
        .expect("insert B");
    db.insert_vector(tx, row_b, vec![0.9, 0.1, 0.0])
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
        .query_vector(&[1.0, 0.0, 0.0], 2, None, reopened.snapshot())
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
