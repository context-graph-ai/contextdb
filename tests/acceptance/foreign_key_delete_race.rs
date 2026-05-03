//! tests/acceptance/foreign_key_delete_race.rs
//!
//! §22 — ForeignKeyValidationDeleteRace.

use std::collections::HashMap;
use std::sync::{Arc, Barrier};
use std::thread;

use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use tempfile::tempdir;
use uuid::Uuid;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn open_db() -> (tempfile::TempDir, Database) {
    let dir = tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("t22.db")).expect("open db");
    (dir, db)
}

fn setup_parent_child(db: &Database, parent_id: Uuid) {
    db.execute("CREATE TABLE decisions (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE invalidations (id UUID PRIMARY KEY, decision_id UUID NOT NULL REFERENCES decisions(id))",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO decisions (id) VALUES ($id)",
        &[("id".to_string(), Value::Uuid(parent_id))]
            .into_iter()
            .collect(),
    )
    .unwrap();
}

fn assert_invalidations_decision_fk_error(err: &Error, label: &str) {
    assert!(
        matches!(
            err,
            Error::ForeignKeyViolation {
                table,
                column,
                ref_table,
            } if table == "invalidations" && column == "decision_id" && ref_table == "decisions"
        ),
        "{label} must report FK violation on invalidations.decision_id -> decisions, got {err:?}"
    );
}

fn assert_invalidations_decision_fk_violation(outcome: Result<(), Error>, label: &str) {
    match outcome {
        Err(err) => assert_invalidations_decision_fk_error(&err, label),
        Ok(()) => panic!(
            "{label} must report FK violation on invalidations.decision_id -> decisions, got Ok(())"
        ),
    }
}

#[test]
fn t22_01_concurrent_delete_parent_and_insert_child_serialize() {
    let (_dir, db) = open_db();
    let db = &db;
    let parent = Uuid::new_v4();
    setup_parent_child(db, parent);

    let child_tx = db.begin();
    let delete_tx = db.begin();
    db.execute_in_tx(
        child_tx,
        "INSERT INTO invalidations (id, decision_id) VALUES ($id, $decision_id)",
        &[
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(parent)),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();
    db.execute_in_tx(
        delete_tx,
        "DELETE FROM decisions WHERE id = $id",
        &[("id".to_string(), Value::Uuid(parent))]
            .into_iter()
            .collect(),
    )
    .unwrap();

    db.commit(child_tx).expect("child insert commits first");
    let outcome = db.commit(delete_tx);
    assert_invalidations_decision_fk_violation(outcome, "parent delete loser");

    let (_dir, db) = open_db();
    let db = &db;
    let parent = Uuid::new_v4();
    setup_parent_child(db, parent);

    let child_tx = db.begin();
    let delete_tx = db.begin();
    db.execute_in_tx(
        child_tx,
        "INSERT INTO invalidations (id, decision_id) VALUES ($id, $decision_id)",
        &[
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(parent)),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();
    db.execute_in_tx(
        delete_tx,
        "DELETE FROM decisions WHERE id = $id",
        &[("id".to_string(), Value::Uuid(parent))]
            .into_iter()
            .collect(),
    )
    .unwrap();

    db.commit(delete_tx).expect("parent delete commits first");
    let outcome = db.commit(child_tx);
    assert_invalidations_decision_fk_violation(outcome, "child insert loser");
}

#[test]
fn t22_02_after_concurrent_race_no_dangling_child_via_join() {
    let (_dir, db) = open_db();
    let db = &db;
    let parent = Uuid::new_v4();
    setup_parent_child(db, parent);

    let child_tx = db.begin();
    let delete_tx = db.begin();
    db.execute_in_tx(
        child_tx,
        "INSERT INTO invalidations (id, decision_id) VALUES ($id, $decision_id)",
        &[
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(parent)),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();
    db.execute_in_tx(
        delete_tx,
        "DELETE FROM decisions WHERE id = $id",
        &[("id".to_string(), Value::Uuid(parent))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    db.commit(child_tx).unwrap();
    let _ = db.commit(delete_tx);

    // Every committed invalidation row must reference a live decisions row.
    let dangling = db
        .execute(
            "SELECT i.id FROM invalidations i WHERE i.decision_id NOT IN (SELECT d.id FROM decisions d)",
            &empty(),
        )
        .unwrap()
        .rows
        .len();
    assert_eq!(
        dangling, 0,
        "post-race read must show no dangling invalidations"
    );
}

#[test]
fn t22_03_sequential_delete_then_insert_rejects() {
    // REGRESSION GUARD.
    let (_dir, db) = open_db();
    let db = &db;
    let parent = Uuid::new_v4();
    setup_parent_child(db, parent);
    db.execute(
        "DELETE FROM decisions WHERE id = $id",
        &[("id".to_string(), Value::Uuid(parent))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    let err = db
        .execute(
            "INSERT INTO invalidations (id, decision_id) VALUES ($id, $decision_id)",
            &[
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("decision_id".to_string(), Value::Uuid(parent)),
            ]
            .into_iter()
            .collect(),
        )
        .unwrap_err();
    assert_invalidations_decision_fk_error(&err, "sequential child insert");
}

#[test]
fn t22_04_single_tx_fk_violation_rolls_back_full_writeset() {
    // REGRESSION GUARD.
    let (_dir, db) = open_db();
    let db = &db;
    db.execute("CREATE TABLE decisions (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE invalidations (id UUID PRIMARY KEY, decision_id UUID NOT NULL REFERENCES decisions(id))",
        &empty(),
    )
    .unwrap();

    let tx = db.begin();
    db.execute_in_tx(
        tx,
        "INSERT INTO invalidations (id, decision_id) VALUES ($id, $decision_id)",
        &[
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(Uuid::new_v4())),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();
    let err = db.commit(tx).unwrap_err();
    assert_invalidations_decision_fk_error(&err, "single-tx child insert");
    let count = db
        .execute("SELECT id FROM invalidations", &empty())
        .unwrap()
        .rows
        .len();
    assert_eq!(count, 0);
}

#[test]
fn t22_05_concurrent_child_inserts_against_live_parent_both_commit() {
    // REGRESSION GUARD: validator must not spuriously reject FK validations
    // when the parent is live for both writers.
    let (_dir, db) = open_db();
    let db = &db;
    let parent = Uuid::new_v4();
    setup_parent_child(db, parent);

    let barrier = Arc::new(Barrier::new(3));
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..2 {
            let barrier = Arc::clone(&barrier);
            handles.push(scope.spawn(move || {
                barrier.wait();
                db.execute(
                    "INSERT INTO invalidations (id, decision_id) VALUES ($id, $decision_id)",
                    &[
                        ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                        ("decision_id".to_string(), Value::Uuid(parent)),
                    ]
                    .into_iter()
                    .collect(),
                )
            }));
        }
        barrier.wait();
        let outcomes: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert!(outcomes.iter().all(|r| r.is_ok()));
    });

    let count = db
        .execute("SELECT id FROM invalidations", &empty())
        .unwrap()
        .rows
        .len();
    assert_eq!(count, 2);
}
