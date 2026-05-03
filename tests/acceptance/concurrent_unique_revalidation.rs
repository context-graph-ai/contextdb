//! tests/acceptance/concurrent_unique_revalidation.rs
//!
//! §15 — ConcurrentUniqueConstraintEnforcement.

use std::collections::HashMap;
use std::sync::{Arc, Barrier};
use std::thread;

use contextdb_core::{Error, Value, VectorIndexRef};
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
fn t15_12_commit_time_upsert_update_expr_reads_conflict_row() {
    // REGRESSION GUARD. Commit-time UPSERT must evaluate SET column references
    // against the committed conflict row, not the losing INSERT payload.
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
                "INSERT INTO entities (id, name, label) VALUES ($id, $name, $label) ON CONFLICT (name) DO UPDATE SET label = label",
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
    db.commit(update_tx)
        .expect("second UPSERT updates from committed row");
    let rows = db
        .execute(
            "SELECT label FROM entities WHERE name = $name",
            &[("name".to_string(), Value::Text("alice".into()))]
                .into_iter()
                .collect(),
        )
        .unwrap()
        .rows;
    assert_eq!(rows, vec![vec![Value::Text("insert-first".into())]]);
}

#[test]
fn t15_13_commit_time_upsert_rejects_immutable_update_column() {
    // REGRESSION GUARD. The commit-time UPSERT rewrite must not bypass the
    // same immutable-column guard as the normal ON CONFLICT DO UPDATE path.
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT NOT NULL UNIQUE, frozen TEXT NOT NULL IMMUTABLE)",
        &empty(),
    )
    .unwrap();
    let insert_tx = db.begin();
    let update_tx = db.begin();
    for (tx, frozen) in [(insert_tx, "insert-first"), (update_tx, "update-second")] {
        let staged = db
            .execute_in_tx(
                tx,
                "INSERT INTO entities (id, name, frozen) VALUES ($id, $name, $frozen) ON CONFLICT (name) DO UPDATE SET frozen = $frozen",
                &[
                    ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("name".to_string(), Value::Text("alice".into())),
                    ("frozen".to_string(), Value::Text(frozen.into())),
                ]
                .into_iter()
                .collect(),
            )
            .unwrap();
        assert_eq!(staged.rows_affected, 1);
    }

    db.commit(insert_tx).expect("first UPSERT inserts");
    let err = db
        .commit(update_tx)
        .expect_err("second UPSERT must reject immutable update");
    assert!(
        matches!(
            err,
            Error::ImmutableColumn {
                ref table,
                ref column
            } if table == "entities" && column == "frozen"
        ),
        "expected ImmutableColumn(entities.frozen), got {err:?}"
    );
}

#[test]
fn t15_14_commit_time_upsert_rejects_staged_unique_post_image_collision() {
    // REGRESSION GUARD. A commit-time UPSERT post-image can be different from
    // its staged INSERT image; UNIQUE validation must check the rewritten row
    // against the rest of the same write-set before apply.
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT NOT NULL UNIQUE, slug TEXT NOT NULL UNIQUE)",
        &empty(),
    )
    .unwrap();

    let upsert_tx = db.begin();
    db.execute_in_tx(
        upsert_tx,
        "INSERT INTO entities (id, name, slug) VALUES ($id, 'other', 'dup')",
        &[("id".to_string(), Value::Uuid(Uuid::new_v4()))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    db.execute_in_tx(
        upsert_tx,
        "INSERT INTO entities (id, name, slug) VALUES ($id, 'alice', 'incoming') ON CONFLICT (name) DO UPDATE SET slug = $slug",
        &[
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("slug".to_string(), Value::Text("dup".into())),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();

    db.execute(
        "INSERT INTO entities (id, name, slug) VALUES ($id, 'alice', 'base')",
        &[("id".to_string(), Value::Uuid(Uuid::new_v4()))]
            .into_iter()
            .collect(),
    )
    .expect("conflict row commits before deferred UPSERT");

    let err = db
        .commit(upsert_tx)
        .expect_err("deferred UPSERT post-image must not duplicate staged slug");
    assert!(
        matches!(
            err,
            Error::UniqueViolation {
                ref table,
                ref column
            } if table == "entities" && column == "slug"
        ),
        "expected UniqueViolation(entities.slug), got {err:?}"
    );
    let dup_rows = db
        .execute("SELECT id FROM entities WHERE slug = 'dup'", &empty())
        .unwrap()
        .rows;
    assert!(dup_rows.is_empty(), "failed tx must not partially apply");
}

#[test]
fn t15_15_composite_on_conflict_matches_full_natural_key() {
    // REGRESSION GUARD. context-graph upserts use composite natural keys such
    // as (source_id, content_hash). Matching only the first conflict column
    // would overwrite a distinct row instead of inserting the new key.
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE memories (id UUID PRIMARY KEY, source_id TEXT NOT NULL, content_hash TEXT NOT NULL, label TEXT NOT NULL, UNIQUE (source_id, content_hash))",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO memories (id, source_id, content_hash, label) VALUES ($id, 'source-a', 'hash-one', 'original')",
        &[("id".to_string(), Value::Uuid(Uuid::new_v4()))]
            .into_iter()
            .collect(),
    )
    .unwrap();

    let result = db
        .execute(
            "INSERT INTO memories (id, source_id, content_hash, label) VALUES ($id, 'source-a', 'hash-two', 'new') ON CONFLICT (source_id, content_hash) DO UPDATE SET label = $label",
            &[
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("label".to_string(), Value::Text("updated".into())),
            ]
            .into_iter()
            .collect(),
        )
        .unwrap();
    assert_eq!(result.rows_affected, 1);

    let rows = db
        .execute(
            "SELECT content_hash, label FROM memories WHERE source_id = 'source-a' ORDER BY content_hash",
            &empty(),
        )
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Text("hash-one".into()),
                Value::Text("original".into())
            ],
            vec![Value::Text("hash-two".into()), Value::Text("new".into())],
        ],
        "composite conflict target must require the full key to match"
    );
}

#[test]
fn t15_16_commit_time_upsert_preserves_later_same_tx_update() {
    // REGRESSION GUARD. If a transaction stages an UPSERT insert and then
    // updates that staged logical row, the commit-time conflict rewrite must
    // preserve the later same-transaction edit.
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT NOT NULL UNIQUE, label TEXT NOT NULL)",
        &empty(),
    )
    .unwrap();

    let winner_tx = db.begin();
    let upsert_tx = db.begin();
    let staged_id = Uuid::new_v4();
    db.execute_in_tx(
        winner_tx,
        "INSERT INTO entities (id, name, label) VALUES ($id, 'alice', 'winner')",
        &[("id".to_string(), Value::Uuid(Uuid::new_v4()))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    db.execute_in_tx(
        upsert_tx,
        "INSERT INTO entities (id, name, label) VALUES ($id, 'alice', $label) ON CONFLICT (name) DO UPDATE SET label = $label",
        &[
            ("id".to_string(), Value::Uuid(staged_id)),
            ("label".to_string(), Value::Text("upsert-first".into())),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();
    db.execute_in_tx(
        upsert_tx,
        "UPDATE entities SET label = $label WHERE id = $id",
        &[
            ("id".to_string(), Value::Uuid(staged_id)),
            ("label".to_string(), Value::Text("same-tx-latest".into())),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();

    db.commit(winner_tx).expect("winner commits first");
    db.commit(upsert_tx)
        .expect("deferred UPSERT preserves later same-tx update");
    let rows = db
        .execute("SELECT label FROM entities WHERE name = 'alice'", &empty())
        .unwrap()
        .rows;
    assert_eq!(rows, vec![vec![Value::Text("same-tx-latest".into())]]);
    assert_eq!(row_count(db, "entities"), 1);
}

#[test]
fn t15_17_commit_time_upsert_triggers_state_propagation() {
    // REGRESSION GUARD. A deferred UPSERT update is still an update of the
    // committed conflict row and must drive the same FK propagation as the
    // immediate ON CONFLICT DO UPDATE path.
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE intentions (id UUID PRIMARY KEY, slug TEXT NOT NULL UNIQUE, status TEXT NOT NULL) STATE MACHINE (status: active -> [archived])",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, intention_id UUID REFERENCES intentions(id) ON STATE archived PROPAGATE SET invalidated, status TEXT NOT NULL) STATE MACHINE (status: active -> [invalidated])",
        &empty(),
    )
    .unwrap();

    let parent_id = Uuid::new_v4();
    let child_id = Uuid::new_v4();
    let winner_tx = db.begin();
    let upsert_tx = db.begin();
    db.execute_in_tx(
        upsert_tx,
        "INSERT INTO intentions (id, slug, status) VALUES ($id, 'intent-a', 'archived') ON CONFLICT (slug) DO UPDATE SET status = 'archived'",
        &[("id".to_string(), Value::Uuid(Uuid::new_v4()))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    db.execute_in_tx(
        winner_tx,
        "INSERT INTO intentions (id, slug, status) VALUES ($id, 'intent-a', 'active')",
        &[("id".to_string(), Value::Uuid(parent_id))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    db.commit(winner_tx).expect("winner commits parent");
    db.execute(
        "INSERT INTO decisions (id, intention_id, status) VALUES ($id, $parent, 'active')",
        &[
            ("id".to_string(), Value::Uuid(child_id)),
            ("parent".to_string(), Value::Uuid(parent_id)),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();

    db.commit(upsert_tx)
        .expect("deferred UPSERT propagates archived state");
    let rows = db
        .execute(
            "SELECT status FROM decisions WHERE id = $id",
            &[("id".to_string(), Value::Uuid(child_id))]
                .into_iter()
                .collect(),
        )
        .unwrap()
        .rows;
    assert_eq!(rows, vec![vec![Value::Text("invalidated".into())]]);
}

#[test]
fn t15_18_conditional_vector_noop_keeps_commit_time_upsert_vector() {
    // REGRESSION GUARD. Conditional UPDATE guard slices are captured before
    // commit validation. A commit-time UPSERT vector rewrite must not reorder
    // vector_inserts first and cause stale conditional cleanup to remove the
    // UPSERT vector.
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE docs (id UUID PRIMARY KEY, natural TEXT NOT NULL UNIQUE, status TEXT NOT NULL, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
    let conditional_id = Uuid::new_v4();
    let conflict_id = Uuid::new_v4();
    db.execute(
        "INSERT INTO docs (id, natural, status, embedding) VALUES ($id, 'conditional', 'open', $embedding)",
        &[
            ("id".to_string(), Value::Uuid(conditional_id)),
            ("embedding".to_string(), Value::Vector(vec![1.0, 0.0, 0.0])),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();

    let tx = db.begin();
    db.execute_in_tx(
        tx,
        "INSERT INTO docs (id, natural, status, embedding) VALUES ($id, 'upsert-key', 'open', $embedding) ON CONFLICT (natural) DO UPDATE SET embedding = $embedding",
        &[
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("embedding".to_string(), Value::Vector(vec![0.0, 0.0, 1.0])),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();
    db.execute_in_tx(
        tx,
        "UPDATE docs SET status = 'changed', embedding = $embedding WHERE id = $id AND status = 'open'",
        &[
            ("id".to_string(), Value::Uuid(conditional_id)),
            ("embedding".to_string(), Value::Vector(vec![1.0, 1.0, 0.0])),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();

    db.execute(
        "INSERT INTO docs (id, natural, status, embedding) VALUES ($id, 'upsert-key', 'open', $embedding)",
        &[
            ("id".to_string(), Value::Uuid(conflict_id)),
            ("embedding".to_string(), Value::Vector(vec![0.0, 1.0, 0.0])),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();
    db.execute(
        "UPDATE docs SET status = 'closed' WHERE id = $id",
        &[("id".to_string(), Value::Uuid(conditional_id))]
            .into_iter()
            .collect(),
    )
    .unwrap();

    db.commit(tx)
        .expect("stale conditional update becomes no-op without dropping UPSERT vector");
    let conditional_rows = db
        .execute(
            "SELECT status FROM docs WHERE id = $id",
            &[("id".to_string(), Value::Uuid(conditional_id))]
                .into_iter()
                .collect(),
        )
        .unwrap()
        .rows;
    assert_eq!(conditional_rows, vec![vec![Value::Text("closed".into())]]);

    let upsert_row = db
        .point_lookup("docs", "id", &Value::Uuid(conflict_id), db.snapshot())
        .unwrap()
        .expect("conflict row must survive as UPSERT target");
    let hits = db
        .query_vector(
            VectorIndexRef::new("docs", "embedding"),
            &[0.0, 0.0, 1.0],
            1,
            None,
            db.snapshot(),
        )
        .unwrap();
    assert_eq!(
        hits.first().map(|(row_id, _)| *row_id),
        Some(upsert_row.row_id),
        "commit-time UPSERT vector must remain indexed after stale conditional cleanup"
    );
}

#[test]
fn t15_19_commit_time_upsert_rechecks_unique_after_propagation_shifts_inserts() {
    // REGRESSION GUARD. State propagation from a commit-time UPSERT can remove
    // an earlier staged replacement row. UNIQUE validation must restart rather
    // than skip the rewritten UPSERT post-image after that index shift.
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE nodes (id UUID PRIMARY KEY, slug TEXT NOT NULL UNIQUE, alias TEXT NOT NULL UNIQUE, status TEXT NOT NULL, note TEXT) STATE MACHINE (status: active -> [archived, invalidated]) PROPAGATE ON EDGE CITES OUTGOING STATE archived SET invalidated",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) DAG('CITES')",
        &empty(),
    )
    .unwrap();

    let root_id = Uuid::new_v4();
    let target_id = Uuid::new_v4();
    let holder_id = Uuid::new_v4();
    db.execute(
        "INSERT INTO nodes (id, slug, alias, status, note) VALUES ($id, 'target', 'target-alias', 'active', 'base')",
        &[("id".to_string(), Value::Uuid(target_id))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO nodes (id, slug, alias, status, note) VALUES ($id, 'holder', 'taken', 'active', 'holder')",
        &[("id".to_string(), Value::Uuid(holder_id))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    let edge_tx = db.begin();
    db.insert_edge(edge_tx, root_id, target_id, "CITES".into(), HashMap::new())
        .unwrap();
    db.commit(edge_tx).unwrap();

    let upsert_tx = db.begin();
    db.execute_in_tx(
        upsert_tx,
        "UPDATE nodes SET note = 'earlier-staged-update' WHERE id = $id",
        &[("id".to_string(), Value::Uuid(target_id))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    db.execute_in_tx(
        upsert_tx,
        "INSERT INTO nodes (id, slug, alias, status, note) VALUES ($id, 'root', 'candidate', 'archived', 'deferred') ON CONFLICT (slug) DO UPDATE SET status = 'archived', alias = $alias",
        &[
            ("id".to_string(), Value::Uuid(root_id)),
            ("alias".to_string(), Value::Text("taken".into())),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();

    db.execute(
        "INSERT INTO nodes (id, slug, alias, status, note) VALUES ($id, 'root', 'root-alias', 'active', 'winner')",
        &[("id".to_string(), Value::Uuid(root_id))]
            .into_iter()
            .collect(),
    )
    .unwrap();

    let err = db
        .commit(upsert_tx)
        .expect_err("rewritten UPSERT post-image must re-check alias uniqueness");
    assert!(
        matches!(
            err,
            Error::UniqueViolation {
                ref table,
                ref column
            } if table == "nodes" && column == "alias"
        ),
        "expected UniqueViolation(nodes.alias), got {err:?}"
    );
    let rows = db
        .execute("SELECT slug FROM nodes WHERE alias = 'taken'", &empty())
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![vec![Value::Text("holder".into())]],
        "failed UPSERT must not commit a duplicate alias"
    );
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
