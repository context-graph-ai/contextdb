//! tests/acceptance/single_pk_concurrent_update.rs
//!
//! §25 — SinglePkUpdateConcurrentVersionCollapse.

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
    let db = Database::open(dir.path().join("t25.db")).expect("open db");
    (dir, db)
}

fn setup_decision(db: &Database) -> Uuid {
    db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, basis_json TEXT NOT NULL)",
        &empty(),
    )
    .unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO decisions (id, basis_json) VALUES ($id, $basis)",
        &[
            ("id".to_string(), Value::Uuid(id)),
            ("basis".to_string(), Value::Text("{\"v\":0}".into())),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();
    id
}

#[test]
fn t25_01_six_parallel_pk_updates_keep_one_visible_row() {
    let (_dir, db) = open_db();
    let db = &db;
    let id = setup_decision(db);
    let barrier = Arc::new(Barrier::new(7));

    thread::scope(|scope| {
        let mut handles = Vec::new();
        for n in 0..6 {
            let barrier = Arc::clone(&barrier);
            handles.push(scope.spawn(move || {
                barrier.wait();
                db.execute(
                    "UPDATE decisions SET basis_json = $basis WHERE id = $id",
                    &[
                        ("id".to_string(), Value::Uuid(id)),
                        ("basis".to_string(), Value::Text(format!("{{\"v\":{n}}}"))),
                    ]
                    .into_iter()
                    .collect(),
                )
            }));
        }
        barrier.wait();
        for h in handles {
            let _ = h.join().unwrap();
        }
    });

    let rows = db
        .execute(
            "SELECT basis_json FROM decisions WHERE id = $id",
            &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
        )
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 1, "PK uniqueness collapsed to one visible row");
}

#[test]
fn t25_02_final_value_is_one_of_proposed() {
    let (_dir, db) = open_db();
    let db = &db;
    let id = setup_decision(db);
    let barrier = Arc::new(Barrier::new(7));

    let proposed: Vec<String> = (0..6).map(|n| format!("{{\"v\":{n}}}")).collect();
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for value in proposed.iter().cloned() {
            let barrier = Arc::clone(&barrier);
            handles.push(scope.spawn(move || {
                barrier.wait();
                let _ = db.execute(
                    "UPDATE decisions SET basis_json = $basis WHERE id = $id",
                    &[
                        ("id".to_string(), Value::Uuid(id)),
                        ("basis".to_string(), Value::Text(value)),
                    ]
                    .into_iter()
                    .collect(),
                );
            }));
        }
        barrier.wait();
        for h in handles {
            h.join().unwrap();
        }
    });

    let final_value = match &db
        .execute(
            "SELECT basis_json FROM decisions WHERE id = $id",
            &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
        )
        .unwrap()
        .rows[0][0]
    {
        Value::Text(s) => s.clone(),
        other => panic!("expected text, got {other:?}"),
    };
    assert!(
        proposed.contains(&final_value),
        "final value must be one of the proposed writes, got {final_value}"
    );
}

#[test]
fn t25_03_concurrent_update_plus_delete_collapse() {
    let (_dir, db) = open_db();
    let db = &db;
    let id = setup_decision(db);
    let barrier = Arc::new(Barrier::new(3));

    thread::scope(|scope| {
        let bar1 = Arc::clone(&barrier);
        let _ = scope.spawn(move || {
            bar1.wait();
            let _ = db.execute(
                "UPDATE decisions SET basis_json = '{\"v\":99}' WHERE id = $id",
                &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
            );
        });
        let bar2 = Arc::clone(&barrier);
        let _ = scope.spawn(move || {
            bar2.wait();
            let _ = db.execute(
                "DELETE FROM decisions WHERE id = $id",
                &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
            );
        });
        barrier.wait();
    });

    let rows = db
        .execute(
            "SELECT id FROM decisions WHERE id = $id",
            &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
        )
        .unwrap()
        .rows;
    assert!(rows.len() <= 1, "PK-keyed read-back must be 0 or 1");
}

#[test]
fn t25_04_sequential_pk_updates_both_succeed() {
    // REGRESSION GUARD.
    let (_dir, db) = open_db();
    let db = &db;
    let id = setup_decision(db);
    for n in 0..3 {
        let outcome = db
            .execute(
                "UPDATE decisions SET basis_json = $basis WHERE id = $id",
                &[
                    ("id".to_string(), Value::Uuid(id)),
                    ("basis".to_string(), Value::Text(format!("{{\"v\":{n}}}"))),
                ]
                .into_iter()
                .collect(),
            )
            .unwrap();
        assert_eq!(outcome.rows_affected, 1);
    }
}

#[test]
fn t25_05_concurrent_updates_different_pks_each_keep_one_row() {
    // REGRESSION GUARD.
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, basis_json TEXT NOT NULL)",
        &empty(),
    )
    .unwrap();
    let mut ids = Vec::new();
    for _ in 0..3 {
        let id = Uuid::new_v4();
        db.execute(
            "INSERT INTO decisions (id, basis_json) VALUES ($id, '{}')",
            &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
        )
        .unwrap();
        ids.push(id);
    }

    let barrier = Arc::new(Barrier::new(4));
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for id in ids.iter().copied() {
            let barrier = Arc::clone(&barrier);
            handles.push(scope.spawn(move || {
                barrier.wait();
                db.execute(
                    "UPDATE decisions SET basis_json = '{\"v\":1}' WHERE id = $id",
                    &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
                )
                .unwrap()
                .rows_affected
            }));
        }
        barrier.wait();
        let affected: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(affected.iter().sum::<u64>(), 3);
    });

    for id in ids {
        let rows = db
            .execute(
                "SELECT id FROM decisions WHERE id = $id",
                &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
            )
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 1);
    }
}
