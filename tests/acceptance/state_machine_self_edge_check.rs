//! tests/acceptance/state_machine_self_edge_check.rs
//!
//! §20 — StateMachineUndeclaredSelfEdgeNoop.

use std::collections::HashMap;

use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use tempfile::tempdir;
use uuid::Uuid;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn open_db() -> (tempfile::TempDir, Database) {
    let dir = tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("t20.db")).expect("open db");
    (dir, db)
}

fn setup_undeclared_self_edge(db: &Database) -> Uuid {
    db.execute(
        "CREATE TABLE invalidations (id UUID PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending') STATE MACHINE (status: pending -> [acknowledged], acknowledged -> [resolved, dismissed])",
        &empty(),
    )
    .unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO invalidations (id, status) VALUES ($id, 'pending')",
        &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
    )
    .unwrap();
    id
}

fn setup_declared_self_edge(db: &Database) -> Uuid {
    db.execute(
        "CREATE TABLE jobs (id UUID PRIMARY KEY, status TEXT NOT NULL DEFAULT 'queued') STATE MACHINE (status: queued -> [queued, running], running -> [done])",
        &empty(),
    )
    .unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO jobs (id, status) VALUES ($id, 'queued')",
        &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
    )
    .unwrap();
    id
}

#[test]
fn t20_01_undeclared_self_edge_rejects() {
    let (_dir, db) = open_db();
    let db = &db;
    let id = setup_undeclared_self_edge(db);
    let outcome = db.execute(
        "UPDATE invalidations SET status = 'pending' WHERE id = $id",
        &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
    );
    assert!(
        matches!(
            outcome,
            Err(Error::InvalidStateTransition(ref transition)) if transition == "pending -> pending"
        ),
        "undeclared self-edge must reject pending -> pending, got {outcome:?}"
    );
}

#[test]
fn t20_02_declared_self_edge_accepted() {
    let (_dir, db) = open_db();
    let db = &db;
    let id = setup_declared_self_edge(db);
    let outcome = db
        .execute(
            "UPDATE jobs SET status = 'queued' WHERE id = $id",
            &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
        )
        .unwrap();
    assert_eq!(outcome.rows_affected, 1);
}

#[test]
fn t20_03_declared_transition_progresses() {
    // REGRESSION GUARD.
    let (_dir, db) = open_db();
    let db = &db;
    let id = setup_undeclared_self_edge(db);
    let outcome = db
        .execute(
            "UPDATE invalidations SET status = 'acknowledged' WHERE id = $id",
            &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
        )
        .unwrap();
    assert_eq!(outcome.rows_affected, 1);
    let row = db
        .execute(
            "SELECT status FROM invalidations WHERE id = $id",
            &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
        )
        .unwrap();
    assert_eq!(row.rows[0][0], Value::Text("acknowledged".to_string()));
}

#[test]
fn t20_04_undeclared_distinct_transition_rejects() {
    // REGRESSION GUARD: undeclared old != new still rejects.
    let (_dir, db) = open_db();
    let db = &db;
    let id = setup_undeclared_self_edge(db);
    let err = db
        .execute(
            "UPDATE invalidations SET status = 'resolved' WHERE id = $id",
            &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
        )
        .unwrap_err();
    assert!(
        matches!(err, Error::InvalidStateTransition(ref transition) if transition == "pending -> resolved")
    );
}

#[test]
fn t20_05_upsert_insert_path_unaffected() {
    // REGRESSION GUARD.
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE invalidations (id UUID PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending') STATE MACHINE (status: pending -> [acknowledged], acknowledged -> [resolved, dismissed])",
        &empty(),
    )
    .unwrap();
    let id = Uuid::new_v4();
    let outcome = db
        .execute(
            "INSERT INTO invalidations (id, status) VALUES ($id, 'pending') ON CONFLICT (id) DO UPDATE SET status = 'pending'",
            &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
        )
        .unwrap();
    assert_eq!(outcome.rows_affected, 1);
}

#[test]
fn t20_06_non_state_machine_self_update_unaffected() {
    // REGRESSION GUARD.
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, label TEXT NOT NULL)",
        &empty(),
    )
    .unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO entities (id, label) VALUES ($id, 'a')",
        &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
    )
    .unwrap();
    let outcome = db
        .execute(
            "UPDATE entities SET label = 'a' WHERE id = $id",
            &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
        )
        .unwrap();
    assert_eq!(outcome.rows_affected, 1);
}
