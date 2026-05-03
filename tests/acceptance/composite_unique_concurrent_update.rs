//! tests/acceptance/composite_unique_concurrent_update.rs
//!
//! §24 — CompositeUniqueConcurrentUpdateMvccTear.

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
    let db = Database::open(dir.path().join("t24.db")).expect("open db");
    (dir, db)
}

fn setup_acl(db: &Database) -> (Uuid, Uuid) {
    db.execute(
        "CREATE TABLE acl (id UUID PRIMARY KEY, principal TEXT NOT NULL, context_id UUID NOT NULL, permission TEXT NOT NULL, UNIQUE (principal, context_id))",
        &empty(),
    )
    .unwrap();
    let ctx = Uuid::new_v4();
    let row_id = Uuid::new_v4();
    db.execute(
        "INSERT INTO acl (id, principal, context_id, permission) VALUES ($id, $principal, $context_id, $permission)",
        &[
            ("id".to_string(), Value::Uuid(row_id)),
            ("principal".to_string(), Value::Text("alice".into())),
            ("context_id".to_string(), Value::Uuid(ctx)),
            ("permission".to_string(), Value::Text("read".into())),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();
    (ctx, row_id)
}

#[test]
fn t24_01_concurrent_updates_on_composite_unique_protected_row_keep_one_visible() {
    let (_dir, db) = open_db();
    let db = &db;
    let (ctx, row_id) = setup_acl(db);
    let barrier = Arc::new(Barrier::new(7));

    thread::scope(|scope| {
        let mut handles = Vec::new();
        for n in 0..6 {
            let barrier = Arc::clone(&barrier);
            handles.push(scope.spawn(move || {
                barrier.wait();
                db.execute(
                    "UPDATE acl SET permission = $permission WHERE id = $id",
                    &[
                        ("id".to_string(), Value::Uuid(row_id)),
                        ("permission".to_string(), Value::Text(format!("level-{n}"))),
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
            "SELECT permission FROM acl WHERE principal = $principal AND context_id = $context_id",
            &[
                ("principal".to_string(), Value::Text("alice".into())),
                ("context_id".to_string(), Value::Uuid(ctx)),
            ]
            .into_iter()
            .collect(),
        )
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 1, "exactly one visible row per composite key");

    // Stable-read assertion: re-reading the row must return the same value
    // every time. A torn MVCC state where multiple committed versions are
    // alternately visible would break this.
    let mut seen = Vec::new();
    for _ in 0..3 {
        let probe = db
            .execute(
                "SELECT permission FROM acl WHERE principal = $principal AND context_id = $context_id",
                &[
                    ("principal".to_string(), Value::Text("alice".into())),
                    ("context_id".to_string(), Value::Uuid(ctx)),
                ]
                .into_iter()
                .collect(),
            )
            .unwrap()
            .rows;
        assert_eq!(probe.len(), 1);
        seen.push(probe[0][0].clone());
    }
    assert!(
        seen.windows(2).all(|w| w[0] == w[1]),
        "row value must be stable across reads, observed {seen:?}"
    );
}

#[test]
fn t24_02_concurrent_updates_yield_one_final_value() {
    let (_dir, db) = open_db();
    let db = &db;
    let (_ctx, row_id) = setup_acl(db);
    let barrier = Arc::new(Barrier::new(3));

    thread::scope(|scope| {
        let mut handles = Vec::new();
        for permission in ["write", "admin"] {
            let barrier = Arc::clone(&barrier);
            let permission = permission.to_string();
            handles.push(scope.spawn(move || {
                barrier.wait();
                let _ = db.execute(
                    "UPDATE acl SET permission = $permission WHERE id = $id",
                    &[
                        ("id".to_string(), Value::Uuid(row_id)),
                        ("permission".to_string(), Value::Text(permission)),
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

    let rows = db
        .execute(
            "SELECT permission FROM acl WHERE id = $id",
            &[("id".to_string(), Value::Uuid(row_id))]
                .into_iter()
                .collect(),
        )
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        Value::Text(s) => assert!(s == "write" || s == "admin"),
        other => panic!("expected text, got {other:?}"),
    }
}

#[test]
fn t24_03_concurrent_updates_moving_composite_key_columns_one_winner() {
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE acl (id UUID PRIMARY KEY, principal TEXT NOT NULL, context_id UUID NOT NULL, permission TEXT NOT NULL, UNIQUE (principal, context_id))",
        &empty(),
    )
    .unwrap();
    let ctx = Uuid::new_v4();
    for principal in ["alice", "bob"] {
        db.execute(
            "INSERT INTO acl (id, principal, context_id, permission) VALUES ($id, $principal, $context_id, $permission)",
            &[
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("principal".to_string(), Value::Text(principal.into())),
                ("context_id".to_string(), Value::Uuid(ctx)),
                ("permission".to_string(), Value::Text("read".into())),
            ]
            .into_iter()
            .collect(),
        )
        .unwrap();
    }

    let alice_tx = db.begin();
    let bob_tx = db.begin();
    for (tx, principal) in [(alice_tx, "alice"), (bob_tx, "bob")] {
        let staged = db
            .execute_in_tx(
                tx,
                "UPDATE acl SET principal = 'carol' WHERE principal = $principal AND context_id = $context_id",
                &[
                    ("principal".to_string(), Value::Text(principal.into())),
                    ("context_id".to_string(), Value::Uuid(ctx)),
                ]
                .into_iter()
                .collect(),
            )
            .unwrap();
        assert_eq!(staged.rows_affected, 1);
    }
    db.commit(alice_tx)
        .expect("first composite-key move commits");
    let outcome = db.commit(bob_tx);
    assert!(
        matches!(
            outcome,
            Err(Error::UniqueViolation { ref table, ref column })
                if table == "acl" && column == "principal"
        ),
        "second composite-key move must report UniqueViolation on acl.principal, got {outcome:?}"
    );

    let rows = db
        .execute(
            "SELECT id FROM acl WHERE principal = 'carol' AND context_id = $context_id",
            &[("context_id".to_string(), Value::Uuid(ctx))]
                .into_iter()
                .collect(),
        )
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 1);

    let principals: Vec<String> = db
        .execute(
            "SELECT principal FROM acl WHERE context_id = $context_id",
            &[("context_id".to_string(), Value::Uuid(ctx))]
                .into_iter()
                .collect(),
        )
        .unwrap()
        .rows
        .into_iter()
        .map(|row| match &row[0] {
            Value::Text(s) => s.clone(),
            other => panic!("expected text principal, got {other:?}"),
        })
        .collect();
    assert_eq!(
        principals.len(),
        2,
        "the losing row must remain present, got {principals:?}"
    );
    assert_eq!(
        principals.iter().filter(|p| p.as_str() == "carol").count(),
        1
    );
    assert_eq!(
        principals
            .iter()
            .filter(|p| matches!(p.as_str(), "alice" | "bob"))
            .count(),
        1,
        "loser must remain as alice or bob, got {principals:?}"
    );
}

#[test]
fn t24_04_sequential_updates_both_commit() {
    // REGRESSION GUARD.
    let (_dir, db) = open_db();
    let db = &db;
    let (_ctx, row_id) = setup_acl(db);
    let first = db
        .execute(
            "UPDATE acl SET permission = 'write' WHERE id = $id",
            &[("id".to_string(), Value::Uuid(row_id))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    let second = db
        .execute(
            "UPDATE acl SET permission = 'admin' WHERE id = $id",
            &[("id".to_string(), Value::Uuid(row_id))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    assert_eq!(first.rows_affected, 1);
    assert_eq!(second.rows_affected, 1);
}

#[test]
fn t24_05_concurrent_updates_against_different_composite_keys_both_commit() {
    // REGRESSION GUARD.
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE acl (id UUID PRIMARY KEY, principal TEXT NOT NULL, context_id UUID NOT NULL, permission TEXT NOT NULL, UNIQUE (principal, context_id))",
        &empty(),
    )
    .unwrap();
    let mut ids = Vec::new();
    let ctx = Uuid::new_v4();
    for principal in ["alice", "bob"] {
        let id = Uuid::new_v4();
        ids.push((id, principal));
        db.execute(
            "INSERT INTO acl (id, principal, context_id, permission) VALUES ($id, $principal, $context_id, $permission)",
            &[
                ("id".to_string(), Value::Uuid(id)),
                ("principal".to_string(), Value::Text(principal.into())),
                ("context_id".to_string(), Value::Uuid(ctx)),
                ("permission".to_string(), Value::Text("read".into())),
            ]
            .into_iter()
            .collect(),
        )
        .unwrap();
    }

    let barrier = Arc::new(Barrier::new(3));
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for (id, _) in ids.iter().copied() {
            let barrier = Arc::clone(&barrier);
            handles.push(scope.spawn(move || {
                barrier.wait();
                db.execute(
                    "UPDATE acl SET permission = 'admin' WHERE id = $id",
                    &[("id".to_string(), Value::Uuid(id))].into_iter().collect(),
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
