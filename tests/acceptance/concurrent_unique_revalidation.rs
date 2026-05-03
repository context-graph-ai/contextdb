//! tests/acceptance/concurrent_unique_revalidation.rs
//!
//! §15 — ConcurrentUniqueConstraintEnforcement.

use std::collections::HashMap;
use std::sync::{Arc, Barrier};
use std::thread;

use contextdb_core::{Error, Value};
use contextdb_engine::{Database, QueryResult};
use tempfile::tempdir;
use uuid::Uuid;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn open_db() -> (tempfile::TempDir, Database) {
    let dir = tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("t15.db")).expect("open db");
    (dir, db)
}

fn row_count(db: &Database, table: &str) -> usize {
    db.execute(&format!("SELECT id FROM {table}"), &empty())
        .expect("count rows")
        .rows
        .len()
}

fn assert_unique_collision_outcomes(
    outcomes: &[Result<QueryResult, Error>],
    expected_winners: usize,
    expected_losers: usize,
    expected_table: &str,
    expected_column: &str,
) {
    assert_eq!(
        outcomes
            .iter()
            .filter(|r| matches!(r, Ok(qr) if qr.rows_affected == 1))
            .count(),
        expected_winners,
        "winners must report rows_affected=1, got {outcomes:?}"
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|r| {
                matches!(
                    r,
                    Err(Error::UniqueViolation { table, column })
                        if table == expected_table && column == expected_column
                )
            })
            .count(),
        expected_losers,
        "losers must report UniqueViolation on {expected_table}.{expected_column}, got {outcomes:?}"
    );
}

#[test]
fn t15_01_concurrent_single_column_unique_insert_keeps_one_winner() {
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE observations (id UUID PRIMARY KEY, content_hash TEXT NOT NULL UNIQUE)",
        &empty(),
    )
    .unwrap();
    let hash = "sha256:fixed".to_string();
    let tx_a = db.begin();
    let tx_b = db.begin();
    for tx in [tx_a, tx_b] {
        let staged = db
            .execute_in_tx(
                tx,
                "INSERT INTO observations (id, content_hash) VALUES ($id, $hash)",
                &[
                    ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("hash".to_string(), Value::Text(hash.clone())),
                ]
                .into_iter()
                .collect(),
            )
            .unwrap();
        assert_eq!(staged.rows_affected, 1);
    }
    let outcomes = [db.commit(tx_a), db.commit(tx_b)];
    assert_eq!(outcomes.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|r| {
                matches!(
                    r,
                    Err(Error::UniqueViolation { table, column })
                        if table == "observations" && column == "content_hash"
                )
            })
            .count(),
        1,
        "loser must report UniqueViolation on observations.content_hash at commit, got {outcomes:?}"
    );

    assert_eq!(row_count(db, "observations"), 1);
}

#[test]
fn t15_02_concurrent_composite_unique_insert_collapses_to_one_row() {
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE bindings (id UUID PRIMARY KEY, space TEXT NOT NULL, table_name TEXT NOT NULL, vector_column TEXT NOT NULL, UNIQUE (space, table_name, vector_column))",
        &empty(),
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
                    "INSERT INTO bindings (id, space, table_name, vector_column) VALUES ($id, $space, $table_name, $vector_column)",
                    &[
                        ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                        ("space".to_string(), Value::Text("text_default".into())),
                        ("table_name".to_string(), Value::Text("evidence".into())),
                        ("vector_column".to_string(), Value::Text("vector_text".into())),
                    ]
                    .into_iter()
                    .collect(),
                )
            }));
        }
        barrier.wait();
        let outcomes: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_unique_collision_outcomes(&outcomes, 1, 1, "bindings", "space");
    });

    assert_eq!(row_count(db, "bindings"), 1);
}

#[test]
fn t15_03_concurrent_pk_collision_six_threads_keeps_one_winner() {
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, label TEXT NOT NULL)",
        &empty(),
    )
    .unwrap();
    let id = Uuid::new_v4();
    let barrier = Arc::new(Barrier::new(7));

    thread::scope(|scope| {
        let mut handles = Vec::new();
        for n in 0..6 {
            let barrier = Arc::clone(&barrier);
            handles.push(scope.spawn(move || {
                barrier.wait();
                db.execute(
                    "INSERT INTO entities (id, label) VALUES ($id, $label)",
                    &[
                        ("id".to_string(), Value::Uuid(id)),
                        ("label".to_string(), Value::Text(format!("writer-{n}"))),
                    ]
                    .into_iter()
                    .collect(),
                )
            }));
        }
        barrier.wait();
        let outcomes: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_unique_collision_outcomes(&outcomes, 1, 5, "entities", "id");
    });

    assert_eq!(row_count(db, "entities"), 1);
}

#[test]
fn t15_04_immutable_unique_sequential_duplicate_insert_rejects() {
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE evidence_log (id UUID PRIMARY KEY, evidence_id UUID NOT NULL UNIQUE, status TEXT NOT NULL) IMMUTABLE",
        &empty(),
    )
    .unwrap();
    let evidence_id = Uuid::new_v4();
    let row = |id: Uuid| -> HashMap<String, Value> {
        [
            ("id".to_string(), Value::Uuid(id)),
            ("evidence_id".to_string(), Value::Uuid(evidence_id)),
            ("status".to_string(), Value::Text("retained".into())),
        ]
        .into_iter()
        .collect()
    };
    db.execute(
        "INSERT INTO evidence_log (id, evidence_id, status) VALUES ($id, $evidence_id, $status)",
        &row(Uuid::new_v4()),
    )
    .unwrap();
    let outcome = db.execute(
        "INSERT INTO evidence_log (id, evidence_id, status) VALUES ($id, $evidence_id, $status)",
        &row(Uuid::new_v4()),
    );
    assert!(
        matches!(
            outcome,
            Err(Error::UniqueViolation { ref table, ref column })
                if table == "evidence_log" && column == "evidence_id"
        ),
        "IMMUTABLE+UNIQUE must hard-reject duplicate on evidence_log.evidence_id, got {outcome:?}"
    );
    assert_eq!(row_count(db, "evidence_log"), 1);
}

#[test]
fn t15_05_immutable_unique_concurrent_insert_one_winner() {
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE evidence_log (id UUID PRIMARY KEY, evidence_id UUID NOT NULL UNIQUE, status TEXT NOT NULL) IMMUTABLE",
        &empty(),
    )
    .unwrap();
    let evidence_id = Uuid::new_v4();
    let barrier = Arc::new(Barrier::new(3));
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..2 {
            let barrier = Arc::clone(&barrier);
            handles.push(scope.spawn(move || {
                barrier.wait();
                db.execute(
                    "INSERT INTO evidence_log (id, evidence_id, status) VALUES ($id, $evidence_id, $status)",
                    &[
                        ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                        ("evidence_id".to_string(), Value::Uuid(evidence_id)),
                        ("status".to_string(), Value::Text("retained".into())),
                    ]
                    .into_iter()
                    .collect(),
                )
            }));
        }
        barrier.wait();
        let outcomes: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_unique_collision_outcomes(&outcomes, 1, 1, "evidence_log", "evidence_id");
    });
    assert_eq!(row_count(db, "evidence_log"), 1);
}

#[test]
fn t15_06_non_immutable_sequential_duplicate_insert_returns_zero_affected() {
    // REGRESSION GUARD: the v0.3.4 duplicate-insert no-op semantic stays for
    // non-IMMUTABLE UNIQUE-keyed tables. Tier 2 must NOT make this branch reject.
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
        &empty(),
    )
    .unwrap();
    let name = "alice".to_string();
    let row = |id: Uuid| -> HashMap<String, Value> {
        [
            ("id".to_string(), Value::Uuid(id)),
            ("name".to_string(), Value::Text(name.clone())),
        ]
        .into_iter()
        .collect()
    };
    let id = Uuid::new_v4();
    let first = db
        .execute(
            "INSERT INTO entities (id, name) VALUES ($id, $name)",
            &row(id),
        )
        .unwrap();
    assert_eq!(first.rows_affected, 1);
    let second = db
        .execute(
            "INSERT INTO entities (id, name) VALUES ($id, $name)",
            &row(Uuid::new_v4()),
        )
        .unwrap();
    assert_eq!(
        second.rows_affected, 0,
        "non-IMMUTABLE duplicate-insert keeps the no-op semantic"
    );
    assert_eq!(row_count(db, "entities"), 1);
}

#[test]
fn t15_07_concurrent_upserts_same_natural_key_collapse_to_one_row() {
    // RED. UPSERT semantically allows both writers to succeed, but the final
    // committed state must still collapse to one visible row for the key.
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT NOT NULL UNIQUE, label TEXT NOT NULL)",
        &empty(),
    )
    .unwrap();
    let insert_tx = db.begin();
    let update_tx = db.begin();
    for (tx, label) in [(insert_tx, "insert-first"), (update_tx, "update-second")] {
        let staged = db
            .execute_in_tx(
                tx,
                "INSERT INTO entities (id, name, label) VALUES ($id, $name, $label) ON CONFLICT (name) DO UPDATE SET label = $label",
                &[
                    ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("name".to_string(), Value::Text("alice".into())),
                    ("label".to_string(), Value::Text(label.into())),
                ]
                .into_iter()
                .collect(),
            )
            .unwrap();
        assert_eq!(staged.rows_affected, 1);
    }
    db.commit(insert_tx).expect("first UPSERT inserts");
    db.commit(update_tx).expect("second UPSERT updates");
    assert_eq!(row_count(db, "entities"), 1);
    let rows = db
        .execute(
            "SELECT label FROM entities WHERE name = $name",
            &[("name".to_string(), Value::Text("alice".into()))]
                .into_iter()
                .collect(),
        )
        .unwrap()
        .rows;
    assert_eq!(rows[0][0], Value::Text("update-second".into()));
}

#[test]
fn t15_09_concurrent_delete_and_insert_same_unique_key_serialize() {
    // REGRESSION GUARD: delete-vs-reinsert may serialize as either "delete
    // wins" or "replacement wins", but it must never leave two live rows for
    // the same UNIQUE key.
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
        &empty(),
    )
    .unwrap();
    let original = Uuid::new_v4();
    let replacement = Uuid::new_v4();
    db.execute(
        "INSERT INTO entities (id, name) VALUES ($id, $name)",
        &[
            ("id".to_string(), Value::Uuid(original)),
            ("name".to_string(), Value::Text("alice".into())),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();

    let barrier = Arc::new(Barrier::new(3));
    thread::scope(|scope| {
        let bar1 = Arc::clone(&barrier);
        let deleter = scope.spawn(move || {
            bar1.wait();
            db.execute(
                "DELETE FROM entities WHERE id = $id",
                &[("id".to_string(), Value::Uuid(original))]
                    .into_iter()
                    .collect(),
            )
        });
        let bar2 = Arc::clone(&barrier);
        let inserter = scope.spawn(move || {
            bar2.wait();
            db.execute(
                "INSERT INTO entities (id, name) VALUES ($id, $name)",
                &[
                    ("id".to_string(), Value::Uuid(replacement)),
                    ("name".to_string(), Value::Text("alice".into())),
                ]
                .into_iter()
                .collect(),
            )
        });
        barrier.wait();
        let _del = deleter.join().unwrap();
        let _ins = inserter.join().unwrap();
    });

    // Either outcome is acceptable; what MUST NOT happen is two live rows with
    // name='alice' simultaneously.
    let alice_rows = db
        .execute(
            "SELECT id FROM entities WHERE name = $name",
            &[("name".to_string(), Value::Text("alice".into()))]
                .into_iter()
                .collect(),
        )
        .unwrap()
        .rows;
    assert!(
        alice_rows.len() <= 1,
        "UNIQUE index must hold at most one live entry for name='alice', got {}",
        alice_rows.len()
    );
}

#[test]
fn t15_10_concurrent_insert_with_fk_and_unique_constraints_serialize() {
    // RED: a child table with both a UNIQUE constraint and a FK to a parent.
    // Two concurrent INSERTs target the same UNIQUE key with a live parent.
    // The fix must serialize: one commits, one fails. The parent is live for
    // both writers, so the loser must be the duplicate reason_hash writer.
    let (_dir, db) = open_db();
    let db = &db;
    db.execute("CREATE TABLE decisions (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE invalidations (id UUID PRIMARY KEY, decision_id UUID NOT NULL REFERENCES decisions(id), reason_hash TEXT NOT NULL UNIQUE)",
        &empty(),
    )
    .unwrap();
    let parent = Uuid::new_v4();
    db.execute(
        "INSERT INTO decisions (id) VALUES ($id)",
        &[("id".to_string(), Value::Uuid(parent))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    let reason = "sha256:fixed".to_string();

    let barrier = Arc::new(Barrier::new(3));
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..2 {
            let barrier = Arc::clone(&barrier);
            let reason = reason.clone();
            handles.push(scope.spawn(move || {
                barrier.wait();
                db.execute(
                    "INSERT INTO invalidations (id, decision_id, reason_hash) VALUES ($id, $decision_id, $reason_hash)",
                    &[
                        ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                        ("decision_id".to_string(), Value::Uuid(parent)),
                        ("reason_hash".to_string(), Value::Text(reason)),
                    ]
                    .into_iter()
                    .collect(),
                )
            }));
        }
        barrier.wait();
        let outcomes: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_unique_collision_outcomes(&outcomes, 1, 1, "invalidations", "reason_hash");
    });
    assert_eq!(row_count(db, "invalidations"), 1);
}

#[test]
fn t15_11_rejected_tx_produces_no_subscription_event() {
    use std::time::Duration;
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
        &empty(),
    )
    .unwrap();
    let receiver = db.subscribe();
    let name = "alice".to_string();

    let barrier = Arc::new(Barrier::new(3));
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..2 {
            let barrier = Arc::clone(&barrier);
            let name = name.clone();
            handles.push(scope.spawn(move || {
                barrier.wait();
                db.execute(
                    "INSERT INTO entities (id, name) VALUES ($id, $name)",
                    &[
                        ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                        ("name".to_string(), Value::Text(name)),
                    ]
                    .into_iter()
                    .collect(),
                )
            }));
        }
        barrier.wait();
        let outcomes: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_unique_collision_outcomes(&outcomes, 1, 1, "entities", "name");
    });

    // Drain the subscriber. Rejected tx must produce no event.
    let mut events = Vec::new();
    while let Ok(event) = receiver.recv_timeout(Duration::from_millis(200)) {
        events.push(event);
    }
    assert_eq!(
        events.len(),
        1,
        "exactly one CommitEvent for the winner; rejected tx produces none"
    );
}

#[test]
fn t15_08_non_conflicting_concurrent_inserts_both_commit() {
    // REGRESSION GUARD: the validator must not spuriously reject non-overlapping inserts.
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
        &empty(),
    )
    .unwrap();
    let barrier = Arc::new(Barrier::new(3));
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for n in 0..2 {
            let barrier = Arc::clone(&barrier);
            handles.push(scope.spawn(move || {
                barrier.wait();
                db.execute(
                    "INSERT INTO entities (id, name) VALUES ($id, $name)",
                    &[
                        ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                        ("name".to_string(), Value::Text(format!("name-{n}"))),
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
    assert_eq!(row_count(db, "entities"), 2);
}
