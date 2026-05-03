//! tests/acceptance/state_machine_concurrent_legal_transition.rs
//!
//! §23 — ConcurrentLegalTransitionConflictSerializable.

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
    let db = Database::open(dir.path().join("t23.db")).expect("open db");
    (dir, db)
}

fn setup_acknowledged(db: &Database) -> Uuid {
    db.execute(
        "CREATE TABLE invalidations (id UUID PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending') STATE MACHINE (status: pending -> [acknowledged], acknowledged -> [resolved, dismissed, archived])",
        &empty(),
    )
    .unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO invalidations (id, status) VALUES ($id, 'pending')",
        &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
    )
    .unwrap();
    db.execute(
        "UPDATE invalidations SET status = 'acknowledged' WHERE id = $id",
        &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
    )
    .unwrap();
    id
}

fn invalidation_status(db: &Database, id: Uuid) -> String {
    let row = db
        .execute(
            "SELECT status FROM invalidations WHERE id = $id",
            &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
        )
        .unwrap();
    match &row.rows[0][0] {
        Value::Text(s) => s.clone(),
        other => panic!("expected text status, got {other:?}"),
    }
}

#[test]
fn t23_01_two_parallel_writers_different_legal_targets_one_winner() {
    let (_dir, db) = open_db();
    let db = &db;
    let id = setup_acknowledged(db);
    let barrier = Arc::new(Barrier::new(3));

    thread::scope(|scope| {
        let mut handles = Vec::new();
        for terminal in ["resolved", "dismissed"] {
            let barrier = Arc::clone(&barrier);
            let terminal = terminal.to_string();
            handles.push(scope.spawn(move || {
                barrier.wait();
                let affected = db.execute(
                    "UPDATE invalidations SET status = $status WHERE id = $id AND status = 'acknowledged'",
                    &[
                        ("id".to_string(), Value::Uuid(id)),
                        ("status".to_string(), Value::Text(terminal.clone())),
                    ]
                    .into_iter()
                    .collect(),
                )
                .unwrap()
                .rows_affected;
                (terminal, affected)
            }));
        }
        barrier.wait();
        let outcomes: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(
            outcomes.iter().map(|(_, affected)| *affected).sum::<u64>(),
            1
        );
        let winner = outcomes
            .iter()
            .find_map(|(terminal, affected)| (*affected == 1).then_some(terminal.as_str()))
            .expect("one winner");
        assert_eq!(invalidation_status(db, id), winner);
    });
}

#[test]
fn t23_02_post_race_row_is_in_one_terminal_state() {
    let (_dir, db) = open_db();
    let db = &db;
    let id = setup_acknowledged(db);
    let resolved_tx = db.begin();
    let dismissed_tx = db.begin();
    for (tx, terminal) in [(resolved_tx, "resolved"), (dismissed_tx, "dismissed")] {
        let staged = db
            .execute_in_tx(
                tx,
                "UPDATE invalidations SET status = $status WHERE id = $id AND status = 'acknowledged'",
                &[
                    ("id".to_string(), Value::Uuid(id)),
                    ("status".to_string(), Value::Text(terminal.into())),
                ]
                .into_iter()
                .collect(),
            )
            .unwrap();
        assert_eq!(staged.rows_affected, 1);
    }
    db.commit(resolved_tx)
        .expect("first terminal transition commits");
    db.commit(dismissed_tx)
        .expect("second terminal transition commits as commit-time no-op");
    assert_eq!(
        invalidation_status(db, id),
        "resolved",
        "final state must stay at the first committed transition"
    );
}

#[test]
fn t23_03_six_parallel_writers_three_legal_targets_one_winner() {
    let (_dir, db) = open_db();
    let db = &db;
    let id = setup_acknowledged(db);
    let barrier = Arc::new(Barrier::new(7));

    thread::scope(|scope| {
        let mut handles = Vec::new();
        for terminal in [
            "resolved",
            "resolved",
            "dismissed",
            "dismissed",
            "archived",
            "archived",
        ] {
            let barrier = Arc::clone(&barrier);
            let terminal = terminal.to_string();
            handles.push(scope.spawn(move || {
                barrier.wait();
                let affected = db.execute(
                    "UPDATE invalidations SET status = $status WHERE id = $id AND status = 'acknowledged'",
                    &[
                        ("id".to_string(), Value::Uuid(id)),
                        ("status".to_string(), Value::Text(terminal.clone())),
                    ]
                    .into_iter()
                    .collect(),
                )
                .unwrap()
                .rows_affected;
                (terminal, affected)
            }));
        }
        barrier.wait();
        let outcomes: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(
            outcomes.iter().map(|(_, affected)| *affected).sum::<u64>(),
            1
        );
        let winner = outcomes
            .iter()
            .find_map(|(terminal, affected)| (*affected == 1).then_some(terminal.as_str()))
            .expect("one winner");
        assert_eq!(invalidation_status(db, id), winner);
    });
}

#[test]
fn t23_04_sequential_second_transition_with_stale_source_returns_zero() {
    // REGRESSION GUARD.
    let (_dir, db) = open_db();
    let db = &db;
    let id = setup_acknowledged(db);
    let first = db
        .execute(
            "UPDATE invalidations SET status = 'resolved' WHERE id = $id AND status = 'acknowledged'",
            &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
        )
        .unwrap();
    assert_eq!(first.rows_affected, 1);
    let second = db
        .execute(
            "UPDATE invalidations SET status = 'dismissed' WHERE id = $id AND status = 'acknowledged'",
            &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
        )
        .unwrap();
    assert_eq!(second.rows_affected, 0);
}
