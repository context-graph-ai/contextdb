//! tests/acceptance/conditional_update_serializable.rs
//!
//! §16 — ConditionalUpdateConflictSerializable.

use std::collections::HashMap;
use std::sync::{Arc, Barrier};
use std::thread;

use contextdb_core::Value;
use contextdb_engine::Database;
use tempfile::tempdir;
use uuid::Uuid;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn open_db() -> (tempfile::TempDir, Database) {
    let dir = tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("t16.db")).expect("open db");
    (dir, db)
}

fn setup_lifecycle_table(db: &Database) -> Uuid {
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

fn invalidation_status(db: &Database, id: Uuid) -> String {
    let rows = db
        .execute(
            "SELECT status FROM invalidations WHERE id = $id",
            &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
        )
        .unwrap()
        .rows;
    match &rows[0][0] {
        Value::Text(s) => s.clone(),
        other => panic!("expected text status, got {other:?}"),
    }
}

#[test]
fn t16_01_two_parallel_conditional_updates_one_winner() {
    let (_dir, db) = open_db();
    let db = &db;
    let id = setup_lifecycle_table(db);
    let barrier = Arc::new(Barrier::new(3));

    thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..2 {
            let barrier = Arc::clone(&barrier);
            handles.push(scope.spawn(move || {
                barrier.wait();
                db.execute(
                    "UPDATE invalidations SET status = 'acknowledged' WHERE id = $id AND status = 'pending'",
                    &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
                )
                .unwrap()
                .rows_affected
            }));
        }
        barrier.wait();
        let affected: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(
            affected.iter().sum::<u64>(),
            1,
            "exactly one writer matches"
        );
    });
    assert_eq!(invalidation_status(db, id), "acknowledged");
}

#[test]
fn t16_02_six_parallel_conditional_updates_one_winner() {
    let (_dir, db) = open_db();
    let db = &db;
    let id = setup_lifecycle_table(db);
    let barrier = Arc::new(Barrier::new(7));

    thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..6 {
            let barrier = Arc::clone(&barrier);
            handles.push(scope.spawn(move || {
                barrier.wait();
                db.execute(
                    "UPDATE invalidations SET status = 'acknowledged' WHERE id = $id AND status = 'pending'",
                    &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
                )
                .unwrap()
                .rows_affected
            }));
        }
        barrier.wait();
        let affected: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(affected.iter().sum::<u64>(), 1);
    });
    assert_eq!(invalidation_status(db, id), "acknowledged");
}

#[test]
fn t16_03_concurrent_same_target_value_one_winner() {
    let (_dir, db) = open_db();
    let db = &db;
    let id = setup_lifecycle_table(db);
    db.execute(
        "UPDATE invalidations SET status = 'acknowledged' WHERE id = $id",
        &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
    )
    .unwrap();

    let barrier = Arc::new(Barrier::new(3));
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..2 {
            let barrier = Arc::clone(&barrier);
            handles.push(scope.spawn(move || {
                barrier.wait();
                db.execute(
                    "UPDATE invalidations SET status = 'resolved' WHERE id = $id AND status = 'acknowledged'",
                    &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
                )
                .unwrap()
                .rows_affected
            }));
        }
        barrier.wait();
        let affected: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(affected.iter().sum::<u64>(), 1);
    });
    assert_eq!(invalidation_status(db, id), "resolved");
}

#[test]
fn t16_04_sequential_stale_where_returns_zero() {
    // REGRESSION GUARD.
    let (_dir, db) = open_db();
    let db = &db;
    let id = setup_lifecycle_table(db);
    let params: HashMap<String, Value> =
        [("id".to_string(), Value::Uuid(id))].into_iter().collect();
    let first = db
        .execute(
            "UPDATE invalidations SET status = 'acknowledged' WHERE id = $id AND status = 'pending'",
            &params,
        )
        .unwrap();
    assert_eq!(first.rows_affected, 1);
    let second = db
        .execute(
            "UPDATE invalidations SET status = 'acknowledged' WHERE id = $id AND status = 'pending'",
            &params,
        )
        .unwrap();
    assert_eq!(second.rows_affected, 0);
}

#[test]
fn t16_05_conditional_update_non_state_machine_column_one_winner() {
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE counters (id UUID PRIMARY KEY, value INTEGER NOT NULL)",
        &empty(),
    )
    .unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO counters (id, value) VALUES ($id, $value)",
        &[
            ("id".to_string(), Value::Uuid(id)),
            ("value".to_string(), Value::Int64(0)),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();

    let tx_one = db.begin();
    let tx_two = db.begin();
    for (tx, new_value) in [(tx_one, 1), (tx_two, 2)] {
        let staged = db
            .execute_in_tx(
                tx,
                "UPDATE counters SET value = $new WHERE id = $id AND value = $expected",
                &[
                    ("id".to_string(), Value::Uuid(id)),
                    ("new".to_string(), Value::Int64(new_value)),
                    ("expected".to_string(), Value::Int64(0)),
                ]
                .into_iter()
                .collect(),
            )
            .unwrap();
        assert_eq!(staged.rows_affected, 1);
    }
    db.commit(tx_one).expect("first conditional update commits");
    db.commit(tx_two)
        .expect("second conditional update commits as commit-time no-op");
    let rows = db
        .execute(
            "SELECT value FROM counters WHERE id = $id",
            &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
        )
        .unwrap()
        .rows;
    assert_eq!(rows[0][0], Value::Int64(1));
}

#[test]
fn t16_06_non_overlapping_conditional_updates_both_commit() {
    // REGRESSION GUARD.
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE counters (id UUID PRIMARY KEY, value INTEGER NOT NULL)",
        &empty(),
    )
    .unwrap();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    for id in [a, b] {
        db.execute(
            "INSERT INTO counters (id, value) VALUES ($id, $value)",
            &[
                ("id".to_string(), Value::Uuid(id)),
                ("value".to_string(), Value::Int64(0)),
            ]
            .into_iter()
            .collect(),
        )
        .unwrap();
    }

    let barrier = Arc::new(Barrier::new(3));
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for id in [a, b] {
            let barrier = Arc::clone(&barrier);
            handles.push(scope.spawn(move || {
                barrier.wait();
                db.execute(
                    "UPDATE counters SET value = $new WHERE id = $id AND value = $expected",
                    &[
                        ("id".to_string(), Value::Uuid(id)),
                        ("new".to_string(), Value::Int64(1)),
                        ("expected".to_string(), Value::Int64(0)),
                    ]
                    .into_iter()
                    .collect(),
                )
                .unwrap()
                .rows_affected
            }));
        }
        barrier.wait();
        let affected: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(affected.iter().sum::<u64>(), 2);
    });
}
