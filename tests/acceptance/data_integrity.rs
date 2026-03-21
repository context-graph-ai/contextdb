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
            &Value::Uuid(Uuid::parse_str("00000000-0000-0000-0000-000000000001").expect("uuid")),
            db.snapshot(),
        )
        .expect("lookup")
        .expect("row exists");
    assert_eq!(text_value(&row, "name"), "temp-1");
    assert_eq!(row.values.get("reading"), Some(&Value::Float64(42.0)));
}

/// I pushed vector embeddings, pulled onto another edge, and an ANN query returned the same top-1 result with near-perfect similarity.
#[tokio::test]
async fn f33_vector_data_round_trips_correctly_through_sync() {
    panic!("ANN query on the pulled edge should return the original top-1 row with cosine > 0.999");
}

/// I deleted 3 rows and pushed, and the server had exactly 7 remaining — deletions synced, not just inserts.
#[tokio::test]
async fn f34_row_deletion_syncs_correctly() {
    panic!("server should retain 7 rows after 3 deletions are pushed");
}

/// I pushed graph edges, and a BFS query on the server found the neighbors I inserted.
#[tokio::test]
async fn f35_graph_edges_sync_correctly() {
    panic!("server-side BFS should find the synced graph neighbors");
}

/// I triggered a state propagation (parent archived its children), pushed, and the propagated child state arrived intact on the server.
#[tokio::test]
async fn f35b_state_propagation_effects_sync_correctly() {
    panic!("propagated child state should arrive intact after sync");
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

/// I pushed a combined graph + relational + vector structure, and the entire thing arrived atomically on the server — no partial state.
#[tokio::test]
async fn f109_sync_preserves_graph_vector_relational_atomicity_end_to_end() {
    panic!("combined graph + relational + vector structure should arrive atomically on the server");
}
