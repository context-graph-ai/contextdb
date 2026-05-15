use contextdb_core::Value;
#[cfg(feature = "test-seams")]
use contextdb_core::VectorIndexRef;
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

fn top_ids(db: &Database, table: &str, limit: usize) -> Vec<Uuid> {
    let result = db
        .execute(
            &format!("SELECT id FROM {table} ORDER BY embedding <=> $query LIMIT {limit}"),
            &params(vec![("query", Value::Vector(axis3(0)))]),
        )
        .unwrap();
    result
        .rows
        .into_iter()
        .map(|row| match row[0] {
            Value::Uuid(id) => id,
            ref other => panic!("expected UUID id, got {other:?}"),
        })
        .collect()
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
            .send(top_ids(&db_text, "table_text", 1))
            .unwrap();
    });
    assert!(text_pause.wait_until_reached(REOPEN_TIMEOUT));

    let (done_face_tx, done_face_rx) = mpsc::channel();
    let db_face = db.clone();
    thread::spawn(move || {
        done_face_tx
            .send(top_ids(&db_face, "table_face", 1))
            .unwrap();
    });
    let face_reached_before_text_release = face_pause.wait_until_reached(REOPEN_TIMEOUT);
    let text_done_before_release = done_text_rx.try_recv();
    let face_done_before_release = done_face_rx.try_recv();

    if !face_reached_before_text_release {
        text_pause.release();
        let _ = face_pause.wait_until_reached(REOPEN_TIMEOUT);
    }
    face_pause.release();
    if face_reached_before_text_release {
        text_pause.release();
    }
    let text_result = done_text_rx.recv_timeout(REOPEN_TIMEOUT).unwrap();
    let face_result = done_face_rx.recv_timeout(REOPEN_TIMEOUT).unwrap();

    assert!(face_reached_before_text_release);
    assert!(matches!(text_done_before_release, Err(TryRecvError::Empty)));
    assert!(matches!(face_done_before_release, Err(TryRecvError::Empty)));
    assert_eq!(text_result, vec![Uuid::from_u128(100_000)]);
    assert_eq!(face_result, vec![Uuid::from_u128(200_000)]);
    assert!(vector_store.has_hnsw_index_for(&text_ref));
    assert!(vector_store.has_hnsw_index_for(&face_ref));
}

#[test]
fn reopen_per_index_hnsw_rebuild_yields_same_live_set() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("per-index-reopen-live-set.db");
    let (pre_text, pre_face) = {
        let db = Database::open(&path).unwrap();
        create_reopen_tables(&db);
        seed_reopen_tables(&db);
        let pre_text = top_ids(&db, "table_text", 50);
        let pre_face = top_ids(&db, "table_face", 50);
        db.close().unwrap();
        (pre_text, pre_face)
    };

    let reopened = Database::open(&path).unwrap();
    let post_text = top_ids(&reopened, "table_text", 50);
    let post_face = top_ids(&reopened, "table_face", 50);
    assert_eq!(post_text, pre_text);
    assert_eq!(post_face, pre_face);
}
