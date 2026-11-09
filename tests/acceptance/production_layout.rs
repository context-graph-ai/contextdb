//! Public behavior of ordinary stores, without private engine observations.
use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;

#[test]
fn transactions_publish_rows_and_vectors_together_and_rollback_leaves_neither() {
    let db = Database::open_memory();
    let params = HashMap::new();
    db.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR(2))",
        &params,
    )
    .unwrap();
    let rolled_back = db.begin().unwrap();
    db.execute_in_tx(
        rolled_back,
        "INSERT INTO items VALUES (1, [1.0, 0.0])",
        &params,
    )
    .unwrap();
    db.rollback(rolled_back).unwrap();
    for sql in [
        "SELECT id FROM items",
        "SELECT id FROM items ORDER BY embedding <=> [1.0, 0.0] LIMIT 10",
    ] {
        assert!(db.execute(sql, &params).unwrap().rows().is_empty());
    }
    let committed = db.begin().unwrap();
    db.execute_in_tx(
        committed,
        "INSERT INTO items VALUES (2, [1.0, 0.0])",
        &params,
    )
    .unwrap();
    db.commit(committed).unwrap();
    for sql in [
        "SELECT id FROM items",
        "SELECT id FROM items ORDER BY embedding <=> [1.0, 0.0] LIMIT 10",
    ] {
        assert_eq!(
            db.execute(sql, &params).unwrap().rows(),
            &[vec![Value::Int64(2)]]
        );
    }
}

#[test]
fn committed_rows_and_vector_answers_survive_close_and_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store.db");
    let db = Database::open(&path).unwrap();
    let params = HashMap::new();
    db.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR(2))",
        &params,
    )
    .unwrap();
    db.execute("INSERT INTO items VALUES (1, [1.0, 0.0])", &params)
        .unwrap();
    db.close().unwrap();
    let reopened = Database::open(&path).unwrap();
    for sql in [
        "SELECT id FROM items",
        "SELECT id FROM items ORDER BY embedding <=> [1.0, 0.0] LIMIT 10",
    ] {
        assert_eq!(
            reopened.execute(sql, &params).unwrap().rows(),
            &[vec![Value::Int64(1)]]
        );
    }
}
