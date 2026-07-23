// Every test in this file exercises the `test-seams` pause-window seam, so the
// whole binary is gated on that feature. (The one previously-ungated test,
// `reopen_per_index_hnsw_rebuild_yields_same_live_set`, was folded into
// `per_index_independent_progress_reopen_flake_is_fixed_under_repeat` in
// hnsw_rebuild_determinism_tests.rs: the folded test's top-50 assertion is
// strictly implied by that test's bitwise-sequence assertion, proven by a
// decisive mutant both tests detect.)
#![cfg(feature = "test-seams")]

use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::Database;
use std::collections::HashMap;
#[cfg(feature = "test-seams")]
use std::sync::Arc;
#[cfg(feature = "test-seams")]
use std::sync::mpsc::{self, TryRecvError};
#[cfg(feature = "test-seams")]
use std::thread;
#[cfg(feature = "test-seams")]
use std::time::Duration;
use tempfile::TempDir;
use uuid::Uuid;

const REOPEN_ROWS: usize = 1024;
#[cfg(feature = "test-seams")]
const MIN_EXACT_COSINE: f64 = 0.99999;
#[cfg(feature = "test-seams")]
const REOPEN_TIMEOUT: Duration = Duration::from_secs(5);

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn ranked3(rank: usize) -> Vec<f32> {
    let score = (1.0 - rank as f32 * 0.0005).clamp(0.05, 1.0);
    vec![score, (1.0 - score * score).max(0.0).sqrt(), 0.0]
}

fn axis3(axis: usize) -> Vec<f32> {
    let mut vector = vec![0.0; 3];
    vector[axis.min(2)] = 1.0;
    vector
}

fn create_reopen_tables(db: &Database) {
    db.execute(
        "CREATE TABLE table_text (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE table_face (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
}

fn seed_reopen_tables(db: &Database) -> (Vec<Uuid>, Vec<Uuid>) {
    let text_ids = (0..REOPEN_ROWS)
        .map(|i| Uuid::from_u128(100_000 + i as u128))
        .collect::<Vec<_>>();
    let face_ids = (0..REOPEN_ROWS)
        .map(|i| Uuid::from_u128(200_000 + i as u128))
        .collect::<Vec<_>>();
    for i in 0..REOPEN_ROWS {
        db.execute(
            "INSERT INTO table_text (id, embedding) VALUES ($id, $embedding)",
            &params(vec![
                ("id", Value::Uuid(text_ids[i])),
                ("embedding", Value::Vector(ranked3(i))),
            ]),
        )
        .unwrap();
        db.execute(
            "INSERT INTO table_face (id, embedding) VALUES ($id, $embedding)",
            &params(vec![
                ("id", Value::Uuid(face_ids[i])),
                ("embedding", Value::Vector(ranked3(i))),
            ]),
        )
        .unwrap();
    }
    (text_ids, face_ids)
}

fn top_ranked(db: &Database, table: &str, limit: usize) -> Vec<(Uuid, f64)> {
    let ids = db
        .execute(
            &format!("SELECT id FROM {table} ORDER BY embedding <=> $query LIMIT {limit}"),
            &params(vec![("query", Value::Vector(axis3(0)))]),
        )
        .unwrap();
    let id_idx = ids
        .columns
        .iter()
        .position(|column| column == "id")
        .expect("vector result should include id");
    let ids = ids
        .rows
        .into_iter()
        .map(|row| match row.get(id_idx) {
            Some(Value::Uuid(id)) => *id,
            other => panic!("expected UUID id, got {other:?}"),
        })
        .collect::<Vec<_>>();
    let scores = db
        .query_vector(
            VectorIndexRef::new(table, "embedding"),
            &axis3(0),
            limit,
            None,
            db.snapshot(),
        )
        .unwrap();
    assert_eq!(
        ids.len(),
        scores.len(),
        "id and score result lengths differ"
    );
    ids.into_iter()
        .zip(scores)
        .map(|(id, (_, score))| (id, score as f64))
        .collect()
}

#[cfg(feature = "test-seams")]
fn assert_top_ranked(result: &[(Uuid, f64)], expected_id: Uuid) {
    assert_eq!(result.first().map(|(id, _)| *id), Some(expected_id));
    assert!(
        result.first().map(|(_, score)| *score).unwrap_or_default() >= MIN_EXACT_COSINE,
        "top row cosine too low: {result:?}"
    );
}

#[cfg(feature = "test-seams")]
#[test]
fn reopened_store_two_refs_both_enter_build_windows_concurrently() {
    use contextdb_vector::test_seam::PauseWindow;

    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("per-index-reopen.db");
    {
        let db = Database::open(&path).unwrap();
        create_reopen_tables(&db);
        seed_reopen_tables(&db);
        db.close().unwrap();
    }

    let db = Arc::new(Database::open(&path).unwrap());
    let vector_store = db.vector_store_for_test();
    let text_ref = VectorIndexRef::new("table_text", "embedding");
    let face_ref = VectorIndexRef::new("table_face", "embedding");
    let text_pause = vector_store.arm_maintenance_pause_for_test(&text_ref, PauseWindow::Build);
    let face_pause = vector_store.arm_maintenance_pause_for_test(&face_ref, PauseWindow::Build);

    let (done_text_tx, done_text_rx) = mpsc::channel();
    let db_text = db.clone();
    thread::spawn(move || {
        done_text_tx
            .send(top_ranked(&db_text, "table_text", 1))
            .unwrap();
    });
    assert!(text_pause.wait_until_reached(REOPEN_TIMEOUT));

    let (done_face_tx, done_face_rx) = mpsc::channel();
    let db_face = db.clone();
    thread::spawn(move || {
        done_face_tx
            .send(top_ranked(&db_face, "table_face", 1))
            .unwrap();
    });
    let face_reached_before_text_release = face_pause.wait_until_reached(REOPEN_TIMEOUT);
    let text_done_before_release = done_text_rx.try_recv();
    let face_done_before_release = done_face_rx.try_recv();

    // Release both indexes unconditionally so the worker threads always
    // finish and the recv_timeouts below return; the ordering promise is
    // asserted afterward. (An earlier "recovery" branch here waited again on
    // the face pause when the ordering had already failed — dead code, since
    // the unconditional assert below fails that run regardless.)
    face_pause.release();
    text_pause.release();
    let text_result = done_text_rx.recv_timeout(REOPEN_TIMEOUT).unwrap();
    let face_result = done_face_rx.recv_timeout(REOPEN_TIMEOUT).unwrap();

    assert!(face_reached_before_text_release);
    assert!(matches!(text_done_before_release, Err(TryRecvError::Empty)));
    assert!(matches!(face_done_before_release, Err(TryRecvError::Empty)));
    assert_top_ranked(&text_result, Uuid::from_u128(100_000));
    assert_top_ranked(&face_result, Uuid::from_u128(200_000));
    assert!(vector_store.has_hnsw_index_for(&text_ref));
    assert!(vector_store.has_hnsw_index_for(&face_ref));
}

// NOTE: `reopen_per_index_hnsw_rebuild_yields_same_live_set` was folded into
// `per_index_independent_progress_reopen_flake_is_fixed_under_repeat` in
// hnsw_rebuild_determinism_tests.rs — that test asserts the full bitwise search
// sequence + HNSW observation identity on the identical two-table reopen, which
// strictly implies this test's top-50 ranked-results match. Proven by a
// reopen-load-drop decisive mutant that both tests detect.
