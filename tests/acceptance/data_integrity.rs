use super::common::*;
use contextdb_core::Value;
use contextdb_engine::Database;
use tempfile::TempDir;
use uuid::Uuid;

/// I pushed a row with name and reading columns, and every field arrived on the server with the exact values I inserted — nothing was silently dropped.
#[tokio::test]
async fn f32_no_silent_data_loss_on_push() {
    let tmp = TempDir::new().expect("tempdir");
    let server_path = temp_db_file(&tmp, "f32-server.db");
    let edge_path = temp_db_file(&tmp, "f32-edge.db");
    let nats = start_nats().await;
    let mut server = spawn_server(&server_path, "f32", &nats.nats_url);
    let _ = run_cli_script(
        &edge_path,
        &["--tenant-id", "f32", "--nats-url", &nats.nats_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT, reading REAL)\nINSERT INTO sensors (id, name, reading) VALUES ('00000000-0000-0000-0000-000000000001', 'temp-1', 42.0)\n.sync push\n.quit\n",
    );
    stop_child(&mut server);
    let db = Database::open(&server_path).expect("server db");
    let row = db
        .point_lookup(
            "sensors",
            "id",
            &Value::Uuid(
                Uuid::parse_str("00000000-0000-0000-0000-000000000001").expect("uuid"),
            ),
            db.snapshot(),
        )
        .expect("lookup")
        .expect("row exists");
    assert_eq!(text_value(&row, "name"), "temp-1");
    assert_eq!(row.values.get("reading"), Some(&Value::Float64(42.0)));
}

/// I pushed vector embeddings, pulled onto another edge, and an ANN query returned the same top-1 result with cosine > 0.999.
#[tokio::test]
async fn f33_vector_data_round_trips_correctly_through_sync() {
    let tmp = TempDir::new().expect("tempdir");
    let server_path = temp_db_file(&tmp, "f33-server.db");
    let edge_path = temp_db_file(&tmp, "f33-edge.db");
    let nats = start_nats().await;
    let mut server = spawn_server(&server_path, "f33", &nats.nats_url);

    // Insert 10 vectors on edge, each with VECTOR(3)
    let mut script = String::from(
        "CREATE TABLE embeddings (id UUID PRIMARY KEY, embedding VECTOR(3))\n",
    );
    let vectors: Vec<[f32; 3]> = vec![
        [1.0, 0.0, 0.0],
        [0.0, 1.0, 0.0],
        [0.0, 0.0, 1.0],
        [0.7, 0.7, 0.0],
        [0.0, 0.7, 0.7],
        [0.7, 0.0, 0.7],
        [0.5, 0.5, 0.5],
        [1.0, 1.0, 0.0],
        [0.0, 1.0, 1.0],
        [1.0, 0.0, 1.0],
    ];
    let ids: Vec<String> = (1..=10)
        .map(|i| format!("00000000-0000-0000-0000-{:012}", i))
        .collect();
    for (i, v) in vectors.iter().enumerate() {
        script.push_str(&format!(
            "INSERT INTO embeddings (id, embedding) VALUES ('{}', [{}, {}, {}])\n",
            ids[i], v[0], v[1], v[2]
        ));
    }
    script.push_str(".sync push\n.quit\n");

    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", "f33", "--nats-url", &nats.nats_url],
        &script,
    );
    assert!(output.status.success());

    // Pull onto a fresh edge — do NOT create the table locally; the pull must
    // create it from the server's DDL. This guards against bugs where pull
    // silently skips DDL attributes because the table already exists.
    let fresh_path = edge_path.with_file_name("f33-fresh.db");
    let pull_output = run_cli_script(
        &fresh_path,
        &["--tenant-id", "f33", "--nats-url", &nats.nats_url],
        ".sync pull\n\
         SELECT id FROM embeddings ORDER BY embedding <=> [1.0, 0.0, 0.0] LIMIT 1\n\
         .quit\n",
    );
    stop_child(&mut server);

    let stdout = output_string(&pull_output.stdout);
    assert!(
        stdout.contains("00000000-0000-0000-0000-000000000001"),
        "ANN query on pulled edge should return the closest vector (id=1), got: {}",
        stdout
    );
}

/// I deleted 3 rows and pushed, and the server had exactly 7 remaining — deletions synced, not just inserts.
#[tokio::test]
async fn f34_row_deletion_syncs_correctly() {
    let tmp = TempDir::new().expect("tempdir");
    let server_path = temp_db_file(&tmp, "f34-server.db");
    let edge_path = temp_db_file(&tmp, "f34-edge.db");
    let nats = start_nats().await;
    let mut server = spawn_server(&server_path, "f34", &nats.nats_url);

    // Insert 10 rows with deterministic UUIDs
    let mut script =
        String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    for i in 1..=10 {
        script.push_str(&format!(
            "INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-{:012}', 'sensor-{}')\n",
            i, i
        ));
    }
    script.push_str(".sync push\n");

    // Delete 3 rows
    for i in 1..=3 {
        script.push_str(&format!(
            "DELETE FROM sensors WHERE id = '00000000-0000-0000-0000-{:012}'\n",
            i
        ));
    }
    script.push_str(".sync push\n.quit\n");

    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", "f34", "--nats-url", &nats.nats_url],
        &script,
    );
    assert!(output.status.success());
    stop_child(&mut server);

    assert_eq!(
        count_rows_from_file(&server_path, "sensors"),
        7,
        "server should have 7 rows after 3 deletions were pushed"
    );
}

/// I pushed graph edges, and a BFS query on the server found the neighbors I inserted.
#[tokio::test]
async fn f35_graph_edges_sync_correctly() {
    let tmp = TempDir::new().expect("tempdir");
    let server_path = temp_db_file(&tmp, "f35-server.db");
    let edge_path = temp_db_file(&tmp, "f35-edge.db");
    let nats = start_nats().await;
    let mut server = spawn_server(&server_path, "f35", &nats.nats_url);

    // Create a DAG: 5 nodes, 4 edges (A->B, A->C, B->D, C->E)
    let a = "00000000-0000-0000-0000-000000000001";
    let b = "00000000-0000-0000-0000-000000000002";
    let c = "00000000-0000-0000-0000-000000000003";
    let d = "00000000-0000-0000-0000-000000000004";
    let e = "00000000-0000-0000-0000-000000000005";

    let script = format!(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT)\n\
         INSERT INTO entities (id, name) VALUES ('{a}', 'A')\n\
         INSERT INTO entities (id, name) VALUES ('{b}', 'B')\n\
         INSERT INTO entities (id, name) VALUES ('{c}', 'C')\n\
         INSERT INTO entities (id, name) VALUES ('{d}', 'D')\n\
         INSERT INTO entities (id, name) VALUES ('{e}', 'E')\n\
         INSERT INTO __edges (source_id, target_id, edge_type) VALUES ('{a}', '{b}', 'DEPENDS_ON')\n\
         INSERT INTO __edges (source_id, target_id, edge_type) VALUES ('{a}', '{c}', 'DEPENDS_ON')\n\
         INSERT INTO __edges (source_id, target_id, edge_type) VALUES ('{b}', '{d}', 'DEPENDS_ON')\n\
         INSERT INTO __edges (source_id, target_id, edge_type) VALUES ('{c}', '{e}', 'DEPENDS_ON')\n\
         .sync push\n\
         .quit\n"
    );

    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", "f35", "--nats-url", &nats.nats_url],
        &script,
    );
    assert!(output.status.success());
    stop_child(&mut server);

    // Verify BFS on the server finds all reachable nodes from A
    let db = Database::open(&server_path).expect("server db");
    let start_id = Uuid::parse_str(a).expect("uuid");
    let bfs_result = db
        .query_bfs(
            start_id,
            Some(&["DEPENDS_ON".to_string()]),
            contextdb_core::Direction::Outgoing,
            5,
            db.snapshot(),
        )
        .expect("bfs should succeed");

    let found_ids: Vec<Uuid> = bfs_result.nodes.iter().map(|n| n.id).collect();
    let expected: Vec<Uuid> = [b, c, d, e]
        .iter()
        .map(|s| Uuid::parse_str(s).expect("uuid"))
        .collect();

    for expected_id in &expected {
        assert!(
            found_ids.contains(expected_id),
            "BFS from A should find node {expected_id}, found: {found_ids:?}"
        );
    }
}

/// I triggered a state propagation (parent archived -> children archived), pushed, and the propagated child states arrived on the server.
#[tokio::test]
async fn f35b_state_propagation_effects_sync_correctly() {
    let tmp = TempDir::new().expect("tempdir");
    let server_path = temp_db_file(&tmp, "f35b-server.db");
    let edge_path = temp_db_file(&tmp, "f35b-edge.db");
    let nats = start_nats().await;
    let mut server = spawn_server(&server_path, "f35b", &nats.nats_url);

    // Create parent and child tables with state machine + propagation
    let parent_id = "00000000-0000-0000-0000-000000000001";
    let child1_id = "00000000-0000-0000-0000-000000000002";
    let child2_id = "00000000-0000-0000-0000-000000000003";

    let script = format!(
        "CREATE TABLE parents (id UUID PRIMARY KEY, status TEXT STATE_MACHINE(status: active -> archived))\n\
         CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id) ON STATE archived PROPAGATE SET archived, status TEXT STATE_MACHINE(status: active -> archived))\n\
         INSERT INTO parents (id, status) VALUES ('{parent_id}', 'active')\n\
         INSERT INTO children (id, parent_id, status) VALUES ('{child1_id}', '{parent_id}', 'active')\n\
         INSERT INTO children (id, parent_id, status) VALUES ('{child2_id}', '{parent_id}', 'active')\n\
         UPDATE parents SET status = 'archived' WHERE id = '{parent_id}'\n\
         .sync push\n\
         .quit\n"
    );

    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", "f35b", "--nats-url", &nats.nats_url],
        &script,
    );
    assert!(output.status.success());
    stop_child(&mut server);

    // Verify propagated states on the server
    let db = Database::open(&server_path).expect("server db");

    let parent = db
        .point_lookup(
            "parents",
            "id",
            &Value::Uuid(Uuid::parse_str(parent_id).expect("uuid")),
            db.snapshot(),
        )
        .expect("lookup parent")
        .expect("parent row must exist");
    assert_eq!(
        text_value(&parent, "status"),
        "archived",
        "parent must be archived on server"
    );

    let child1 = db
        .point_lookup(
            "children",
            "id",
            &Value::Uuid(Uuid::parse_str(child1_id).expect("uuid")),
            db.snapshot(),
        )
        .expect("lookup child1")
        .expect("child1 row must exist");
    assert_eq!(
        text_value(&child1, "status"),
        "archived",
        "child1 must be archived via propagation on server"
    );

    let child2 = db
        .point_lookup(
            "children",
            "id",
            &Value::Uuid(Uuid::parse_str(child2_id).expect("uuid")),
            db.snapshot(),
        )
        .expect("lookup child2")
        .expect("child2 row must exist");
    assert_eq!(
        text_value(&child2, "status"),
        "archived",
        "child2 must be archived via propagation on server"
    );
}

/// I inserted entities and graph edges in a transaction, rolled it back, and neither the rows nor the edges existed afterward.
#[test]
fn f35c_graph_structure_atomicity_in_transactions() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT)",
        &empty_params(),
    )
    .expect("create entities");
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = Uuid::new_v4();
    let tx = db.begin();
    db.insert_row(
        tx,
        "entities",
        values(vec![
            ("id", Value::Uuid(a)),
            ("name", Value::Text("A".into())),
        ]),
    )
    .expect("insert A");
    db.insert_row(
        tx,
        "entities",
        values(vec![
            ("id", Value::Uuid(b)),
            ("name", Value::Text("B".into())),
        ]),
    )
    .expect("insert B");
    db.insert_row(
        tx,
        "entities",
        values(vec![
            ("id", Value::Uuid(c)),
            ("name", Value::Text("C".into())),
        ]),
    )
    .expect("insert C");
    db.insert_edge(tx, a, b, "EDGE".into(), Default::default())
        .expect("insert edge A->B");
    db.insert_edge(tx, a, c, "EDGE".into(), Default::default())
        .expect("insert edge A->C");
    db.rollback(tx).expect("rollback");
    assert_eq!(count_rows(&db, "entities"), 0);
    assert!(
        db.query_bfs(
            a,
            None,
            contextdb_core::Direction::Outgoing,
            5,
            db.snapshot()
        )
        .expect("bfs")
        .nodes
        .is_empty()
    );
}

/// I pushed graph + relational + vector data together, and all three subsystem counts matched on the server.
#[tokio::test]
async fn f109_sync_preserves_graph_vector_relational_atomicity_end_to_end() {
    let tmp = TempDir::new().expect("tempdir");
    let server_path = temp_db_file(&tmp, "f109-server.db");
    let edge_path = temp_db_file(&tmp, "f109-edge.db");
    let nats = start_nats().await;
    let mut server = spawn_server(&server_path, "f109", &nats.nats_url);

    let id1 = "00000000-0000-0000-0000-000000000001";
    let id2 = "00000000-0000-0000-0000-000000000002";
    let id3 = "00000000-0000-0000-0000-000000000003";

    let script = format!(
        "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT, embedding VECTOR(3))\n\
         INSERT INTO items (id, name, embedding) VALUES ('{id1}', 'alpha', [1.0, 0.0, 0.0])\n\
         INSERT INTO items (id, name, embedding) VALUES ('{id2}', 'beta', [0.0, 1.0, 0.0])\n\
         INSERT INTO items (id, name, embedding) VALUES ('{id3}', 'gamma', [0.0, 0.0, 1.0])\n\
         INSERT INTO __edges (source_id, target_id, edge_type) VALUES ('{id1}', '{id2}', 'RELATES_TO')\n\
         INSERT INTO __edges (source_id, target_id, edge_type) VALUES ('{id2}', '{id3}', 'RELATES_TO')\n\
         .sync push\n\
         .quit\n"
    );

    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", "f109", "--nats-url", &nats.nats_url],
        &script,
    );
    assert!(output.status.success());
    stop_child(&mut server);

    let db = Database::open(&server_path).expect("server db");

    // Relational: 3 rows
    assert_eq!(
        count_rows(&db, "items"),
        3,
        "server must have 3 relational rows"
    );

    // Graph: BFS from id1 should find id2 and id3
    let start_id = Uuid::parse_str(id1).expect("uuid");
    let bfs_result = db
        .query_bfs(
            start_id,
            None,
            contextdb_core::Direction::Outgoing,
            5,
            db.snapshot(),
        )
        .expect("bfs");
    let found_ids: Vec<Uuid> = bfs_result.nodes.iter().map(|n| n.id).collect();
    let expected_id2 = Uuid::parse_str(id2).expect("uuid");
    let expected_id3 = Uuid::parse_str(id3).expect("uuid");
    assert!(
        found_ids.contains(&expected_id2),
        "BFS from id1 must find id2, found: {found_ids:?}"
    );
    assert!(
        found_ids.contains(&expected_id3),
        "BFS from id1 must find id3, found: {found_ids:?}"
    );

    // Vector: ANN query for [1.0, 0.0, 0.0] should return id1
    let ann_result = db
        .execute(
            "SELECT id FROM items ORDER BY embedding <=> [1.0, 0.0, 0.0] LIMIT 1",
            &empty_params(),
        )
        .expect("ANN query");
    assert_eq!(ann_result.rows.len(), 1, "ANN query should return 1 row");
    let id_col_idx = ann_result
        .columns
        .iter()
        .position(|c| c == "id")
        .expect("id column in ANN result");
    let returned_id = &ann_result.rows[0][id_col_idx];
    assert_eq!(
        *returned_id,
        Value::Uuid(Uuid::parse_str(id1).expect("uuid")),
        "ANN nearest neighbor for [1,0,0] must be id1 (the exact match), got: {:?}",
        returned_id
    );
}

/// I created a table with PRIMARY KEY, NOT NULL, and UNIQUE constraints, pushed to a server, and the server's table retained all three.
#[tokio::test]
async fn sf01_ddl_column_attributes_preserved_through_sync() {
    let tmp = TempDir::new().expect("tempdir");
    let server_path = temp_db_file(&tmp, "sf01-server.db");
    let edge_path = temp_db_file(&tmp, "sf01-edge.db");
    let nats = start_nats().await;
    let mut server = spawn_server(&server_path, "sf01", &nats.nats_url);
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", "sf01", "--nats-url", &nats.nats_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT NOT NULL, code TEXT UNIQUE)\n\
         INSERT INTO sensors (id, name, code) VALUES ('00000000-0000-0000-0000-000000000001', 'a', 'S001')\n\
         .sync push\n\
         .quit\n",
    );
    assert!(output.status.success());
    stop_child(&mut server);

    let db = Database::open(&server_path).expect("server db");
    let meta = db
        .table_meta("sensors")
        .expect("sensors table must exist on server");

    let id_col = meta
        .columns
        .iter()
        .find(|c| c.name == "id")
        .expect("id column");
    assert!(
        id_col.primary_key,
        "id column must be PRIMARY KEY after sync"
    );

    let name_col = meta
        .columns
        .iter()
        .find(|c| c.name == "name")
        .expect("name column");
    assert!(
        !name_col.nullable,
        "name column must be NOT NULL after sync"
    );
    assert!(
        !name_col.primary_key,
        "name column must NOT be PRIMARY KEY — negative guard against all-true serializer"
    );
    assert!(
        !name_col.unique,
        "name column must NOT be UNIQUE — negative guard against all-true serializer"
    );

    let code_col = meta
        .columns
        .iter()
        .find(|c| c.name == "code")
        .expect("code column");
    assert!(code_col.unique, "code column must be UNIQUE after sync");
    assert!(
        !code_col.primary_key,
        "code column must NOT be PRIMARY KEY — negative guard against all-true serializer"
    );
    assert!(
        code_col.nullable,
        "code column must remain nullable (no NOT NULL declared) — negative guard"
    );
}

/// I created a table with constraints, pushed, restarted the server, and the constraints survived.
#[tokio::test]
async fn sf02_ddl_attributes_persist_across_server_restart() {
    let tmp = TempDir::new().expect("tempdir");
    let server_path = temp_db_file(&tmp, "sf02-server.db");
    let edge_path = temp_db_file(&tmp, "sf02-edge.db");
    let nats = start_nats().await;
    let mut server = spawn_server(&server_path, "sf02", &nats.nats_url);
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", "sf02", "--nats-url", &nats.nats_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT NOT NULL, code TEXT UNIQUE)\n\
         INSERT INTO sensors (id, name, code) VALUES ('00000000-0000-0000-0000-000000000001', 'a', 'S001')\n\
         .sync push\n\
         .quit\n",
    );
    assert!(output.status.success());
    stop_child(&mut server);

    // Reopen the server DB directly (simulates restart)
    let db = Database::open(&server_path).expect("server db after restart");
    let meta = db
        .table_meta("sensors")
        .expect("sensors table must exist after restart");

    let id_col = meta
        .columns
        .iter()
        .find(|c| c.name == "id")
        .expect("id column");
    assert!(
        id_col.primary_key,
        "id column must retain PRIMARY KEY after server restart"
    );

    let name_col = meta
        .columns
        .iter()
        .find(|c| c.name == "name")
        .expect("name column");
    assert!(
        !name_col.nullable,
        "name column must retain NOT NULL after server restart"
    );

    let code_col = meta
        .columns
        .iter()
        .find(|c| c.name == "code")
        .expect("code column");
    assert!(
        code_col.unique,
        "code column must retain UNIQUE after server restart"
    );
}

/// I pushed a row with NULL in a NOT NULL column, and the server rejected it.
#[tokio::test]
async fn sf03_not_null_constraint_enforced_on_server_during_push() {
    let tmp = TempDir::new().expect("tempdir");
    let server_path = temp_db_file(&tmp, "sf03-server.db");
    let edge1_path = temp_db_file(&tmp, "sf03-edge1.db");
    let edge2_path = temp_db_file(&tmp, "sf03-edge2.db");
    let nats = start_nats().await;
    let mut server = spawn_server(&server_path, "sf03", &nats.nats_url);

    // Edge1: create table WITH NOT NULL, push valid row to establish schema on server
    let setup = run_cli_script(
        &edge1_path,
        &["--tenant-id", "sf03", "--nats-url", &nats.nats_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT NOT NULL)\n\
         INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'valid')\n\
         .sync push\n\
         .quit\n",
    );
    assert!(setup.status.success());

    // Edge2: create table WITHOUT NOT NULL locally, insert NULL name, push
    // Server has NOT NULL from edge1's DDL, so it must reject this row
    let violation = run_cli_script(
        &edge2_path,
        &["--tenant-id", "sf03", "--nats-url", &nats.nats_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
         INSERT INTO sensors (id) VALUES ('00000000-0000-0000-0000-000000000002')\n\
         .sync push\n\
         .quit\n",
    );
    stop_child(&mut server);

    // The push output must report a conflict/constraint violation
    let stdout = output_string(&violation.stdout).to_lowercase();
    assert!(
        stdout.contains("constraint")
            || stdout.contains("not null")
            || stdout.contains("conflict"),
        "push of NULL into NOT NULL column must be rejected by server, got: {}",
        stdout
    );

    // Server must still have only 1 row (the valid one), not 2
    let db = Database::open(&server_path).expect("server db");
    assert_eq!(
        count_rows(&db, "sensors"),
        1,
        "server must reject the NULL row, keeping only the valid row"
    );
}

/// I pushed a duplicate UNIQUE value, and the server's table enforced the constraint.
#[tokio::test]
async fn sf04_unique_constraint_enforced_on_server_during_push() {
    let tmp = TempDir::new().expect("tempdir");
    let server_path = temp_db_file(&tmp, "sf04-server.db");
    let edge1_path = temp_db_file(&tmp, "sf04-edge1.db");
    let edge2_path = temp_db_file(&tmp, "sf04-edge2.db");
    let nats = start_nats().await;
    let mut server = spawn_server(&server_path, "sf04", &nats.nats_url);

    // Edge1: push table with UNIQUE constraint and one row
    let setup = run_cli_script(
        &edge1_path,
        &["--tenant-id", "sf04", "--nats-url", &nats.nats_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, code TEXT UNIQUE)\n\
         INSERT INTO sensors (id, code) VALUES ('00000000-0000-0000-0000-000000000001', 'ABC')\n\
         .sync push\n\
         .quit\n",
    );
    assert!(setup.status.success());

    // Edge2: push a DIFFERENT row with the SAME unique code 'ABC'
    // Server must reject this as a UNIQUE violation
    let violation = run_cli_script(
        &edge2_path,
        &["--tenant-id", "sf04", "--nats-url", &nats.nats_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, code TEXT UNIQUE)\n\
         INSERT INTO sensors (id, code) VALUES ('00000000-0000-0000-0000-000000000002', 'ABC')\n\
         .sync push\n\
         .quit\n",
    );
    stop_child(&mut server);

    // The push output must report a conflict/constraint violation
    let stdout = output_string(&violation.stdout).to_lowercase();
    assert!(
        stdout.contains("constraint")
            || stdout.contains("unique")
            || stdout.contains("conflict"),
        "push of duplicate UNIQUE value must be rejected by server, got: {}",
        stdout
    );

    // Server must still have only 1 row (the first one), not 2
    let db = Database::open(&server_path).expect("server db");
    assert_eq!(
        count_rows(&db, "sensors"),
        1,
        "server must reject the duplicate UNIQUE row, keeping only the first row"
    );
}
