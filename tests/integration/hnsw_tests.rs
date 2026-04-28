use contextdb_core::RowId;
use contextdb_core::*;
use contextdb_engine::Database;
use roaring::RoaringTreemap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;
use uuid::Uuid;

const DIMENSION: usize = 32;

fn values(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn random_unit_vector(dim: usize, seed: u64) -> Vec<f32> {
    let mut state = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    let mut vec = Vec::with_capacity(dim);
    for _ in 0..dim {
        state = state
            .wrapping_mul(2862933555777941757)
            .wrapping_add(3037000493);
        let unit = ((state >> 33) as f64) / ((1u64 << 31) as f64);
        vec.push((unit as f32) * 2.0 - 1.0);
    }
    normalize(&vec)
}

fn normalize(input: &[f32]) -> Vec<f32> {
    let norm = input
        .iter()
        .map(|v| (*v as f64) * (*v as f64))
        .sum::<f64>()
        .sqrt() as f32;
    if norm == 0.0 {
        return vec![0.0; input.len()];
    }
    input.iter().map(|v| *v / norm).collect()
}

fn perturb_vector(base: &[f32], scale: f32, seed: u64) -> Vec<f32> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut result = base.to_vec();
    for (i, val) in result.iter_mut().enumerate() {
        let mut h = DefaultHasher::new();
        (seed, i as u64).hash(&mut h);
        let noise = ((h.finish() % 10000) as f32 / 10000.0 - 0.5) * 2.0 * scale;
        *val += noise;
    }
    let normed = normalize(&result);
    result.copy_from_slice(&normed);
    result
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let dot = a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>();
    let an = a.iter().map(|v| v * v).sum::<f32>().sqrt();
    let bn = b.iter().map(|v| v * v).sum::<f32>().sqrt();
    if an == 0.0 || bn == 0.0 {
        0.0
    } else {
        dot / (an * bn)
    }
}

fn brute_force_top_k(vectors: &[(RowId, Vec<f32>)], query: &[f32], k: usize) -> Vec<RowId> {
    let mut scored = vectors
        .iter()
        .map(|(rid, vec)| (*rid, cosine(vec, query)))
        .collect::<Vec<_>>();
    scored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    scored.into_iter().take(k).map(|(rid, _)| rid).collect()
}

fn measure_avg_recall(
    db: &Database,
    all_vectors: &[(RowId, Vec<f32>)],
    k: usize,
    seeds: std::ops::Range<u64>,
) -> f64 {
    let mut total_recall = 0.0;
    let mut queries = 0usize;

    for seed in seeds {
        let query = random_unit_vector(DIMENSION, seed);
        let results = db
            .query_vector(
                contextdb_core::VectorIndexRef::new("items", "embedding"),
                &query,
                k,
                None,
                db.snapshot(),
            )
            .expect("vector search");
        let result_ids = results.iter().map(|r| r.0).collect::<HashSet<_>>();
        let truth_ids = brute_force_top_k(all_vectors, &query, k)
            .into_iter()
            .collect::<HashSet<_>>();
        let matches = result_ids.intersection(&truth_ids).count();
        total_recall += matches as f64 / k as f64;
        queries += 1;
    }

    total_recall / queries as f64
}

fn setup_items_table(db: &Database, dimension: usize) {
    db.execute(
        &format!("CREATE TABLE items (id UUID PRIMARY KEY, embedding VECTOR({dimension}))"),
        &HashMap::new(),
    )
    .expect("create table");
}

fn insert_items(db: &Database, count: u64, start_seed: u64) -> Vec<(RowId, Vec<f32>)> {
    let tx = db.begin();
    let mut rows = Vec::with_capacity(count as usize);
    for i in 0..count {
        let rid = db
            .insert_row(
                tx,
                "items",
                values(vec![("id", Value::Uuid(Uuid::new_v4()))]),
            )
            .expect("insert row");
        let vec = random_unit_vector(DIMENSION, start_seed + i);
        db.insert_vector(
            tx,
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            rid,
            vec.clone(),
        )
        .expect("insert vector");
        rows.push((rid, vec));
    }
    db.commit(tx).expect("commit");
    rows
}

#[test]
fn h01_below_threshold_uses_vector_search() {
    let db = Database::open_memory();
    setup_items_table(&db, DIMENSION);
    let all_vectors = insert_items(&db, 500, 0);

    let explain = db
        .explain("SELECT * FROM items ORDER BY embedding <=> $q LIMIT 5")
        .expect("explain");
    let query = random_unit_vector(DIMENSION, 9_999);
    let results = db
        .query_vector(
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            &query,
            5,
            None,
            db.snapshot(),
        )
        .expect("query");

    assert!(explain.contains("VectorSearch"));
    assert!(!explain.contains("HNSWSearch"));
    assert_eq!(results.len(), 5);

    let result_ids = results.iter().map(|r| r.0).collect::<Vec<_>>();
    let truth_ids = brute_force_top_k(&all_vectors, &query, 5);
    assert_eq!(result_ids, truth_ids);
}

#[test]
fn h02_at_threshold_explain_shows_hnsw_search() {
    let db = Database::open_memory();
    setup_items_table(&db, DIMENSION);
    insert_items(&db, 1_000, 0);

    let explain = db
        .explain("SELECT * FROM items ORDER BY embedding <=> $q LIMIT 10")
        .expect("explain");

    assert!(explain.contains("HNSWSearch"));
    assert!(!explain.contains("VectorSearch"));
}

#[test]
fn h03_transition_across_threshold_switches_explain() {
    let db = Database::open_memory();
    setup_items_table(&db, DIMENSION);
    insert_items(&db, 999, 0);

    let explain_before = db
        .explain("SELECT * FROM items ORDER BY embedding <=> $q LIMIT 10")
        .expect("explain before");
    let tx = db.begin();
    let rid = db
        .insert_row(
            tx,
            "items",
            values(vec![("id", Value::Uuid(Uuid::new_v4()))]),
        )
        .expect("insert row");
    db.insert_vector(
        tx,
        contextdb_core::VectorIndexRef::new("items", "embedding"),
        rid,
        random_unit_vector(DIMENSION, 999),
    )
    .expect("insert vector");
    db.commit(tx).expect("commit");
    let explain_after = db
        .explain("SELECT * FROM items ORDER BY embedding <=> $q LIMIT 10")
        .expect("explain after");

    assert!(explain_before.contains("VectorSearch"));
    assert!(!explain_before.contains("HNSWSearch"));
    assert!(explain_after.contains("HNSWSearch"));
}

#[test]
fn h04_hnsw_recall_is_at_least_ninety_five_percent() {
    let db = Database::open_memory();
    setup_items_table(&db, DIMENSION);
    let all_vectors = insert_items(&db, 1_500, 0);

    let explain = db
        .explain("SELECT * FROM items ORDER BY embedding <=> $q LIMIT 10")
        .expect("explain");
    let avg_recall = measure_avg_recall(&db, &all_vectors, 10, 5_000..5_020);

    assert!(explain.contains("HNSWSearch"));
    assert!(avg_recall >= 0.95, "avg recall was {avg_recall}");
}

#[test]
fn h05_hnsw_recall_is_stable_after_reopen() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("hnsw_reopen.db");
    let db = Database::open(&path).expect("open");
    setup_items_table(&db, DIMENSION);
    let all_vectors = insert_items(&db, 1_500, 0);
    db.close().expect("close");

    let db2 = Database::open(&path).expect("reopen");
    let explain = db2
        .explain("SELECT * FROM items ORDER BY embedding <=> $q LIMIT 10")
        .expect("explain");
    let avg_recall = measure_avg_recall(&db2, &all_vectors, 10, 5_000..5_020);

    assert!(explain.contains("HNSWSearch"));
    assert!(avg_recall >= 0.95, "avg recall was {avg_recall}");
}

#[test]
fn h06_deleted_vectors_are_excluded_under_hnsw() {
    let db = Database::open_memory();
    setup_items_table(&db, DIMENSION);
    let all_vectors = insert_items(&db, 1_200, 0);
    let all_rids = all_vectors.iter().map(|(rid, _)| *rid).collect::<Vec<_>>();

    let tx = db.begin();
    let deleted_rids = all_rids[..300].iter().copied().collect::<HashSet<_>>();
    for rid in &all_rids[..300] {
        db.delete_row(tx, "items", *rid).expect("delete row");
        db.delete_vector(
            tx,
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            *rid,
        )
        .expect("delete vector");
    }
    db.commit(tx).expect("commit");

    let explain = db
        .explain("SELECT * FROM items ORDER BY embedding <=> $q LIMIT 10")
        .expect("explain");
    let query = random_unit_vector(DIMENSION, 9_999);
    let results = db
        .query_vector(
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            &query,
            10,
            None,
            db.snapshot(),
        )
        .expect("query");

    assert!(explain.contains("HNSWSearch"));
    assert_eq!(results.len(), 10);
    for (rid, _) in &results {
        assert!(!deleted_rids.contains(rid));
    }
}

#[test]
fn h07_snapshot_isolation_uses_only_rows_visible_in_snapshot() {
    let db = Database::open_memory();
    setup_items_table(&db, DIMENSION);

    let tx1 = db.begin();
    let mut tx1_rids = HashSet::new();
    for i in 0..1_000 {
        let rid = db
            .insert_row(
                tx1,
                "items",
                values(vec![("id", Value::Uuid(Uuid::new_v4()))]),
            )
            .expect("insert row");
        db.insert_vector(
            tx1,
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            rid,
            random_unit_vector(DIMENSION, i),
        )
        .expect("insert vector");
        tx1_rids.insert(rid);
    }
    db.commit(tx1).expect("commit tx1");
    let snap_after_tx1 = db.snapshot();

    let tx2 = db.begin();
    for i in 1_000..1_200 {
        let rid = db
            .insert_row(
                tx2,
                "items",
                values(vec![("id", Value::Uuid(Uuid::new_v4()))]),
            )
            .expect("insert row");
        db.insert_vector(
            tx2,
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            rid,
            random_unit_vector(DIMENSION, i),
        )
        .expect("insert vector");
    }
    db.commit(tx2).expect("commit tx2");

    let explain = db
        .explain("SELECT * FROM items ORDER BY embedding <=> $q LIMIT 10")
        .expect("explain");
    let query = random_unit_vector(DIMENSION, 9_999);
    let results = db
        .query_vector(
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            &query,
            10,
            None,
            snap_after_tx1,
        )
        .expect("query");

    assert!(explain.contains("HNSWSearch"));
    for (rid, _) in &results {
        assert!(tx1_rids.contains(rid));
    }
}

#[test]
fn h08_prefiltered_search_respects_candidate_bitmap() {
    let db = Database::open_memory();
    setup_items_table(&db, DIMENSION);
    let all_vectors = insert_items(&db, 1_200, 0);

    let mut candidates = RoaringTreemap::new();
    for (rid, _) in all_vectors.iter().step_by(2) {
        candidates.insert(rid.0);
    }

    let explain = db
        .explain("SELECT * FROM items ORDER BY embedding <=> $q LIMIT 10")
        .expect("explain");
    let query = random_unit_vector(DIMENSION, 9_999);
    let results = db
        .query_vector(
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            &query,
            10,
            Some(&candidates),
            db.snapshot(),
        )
        .expect("query");

    assert!(explain.contains("HNSWSearch"));
    assert!(results.len() <= 10);
    for (rid, _) in &results {
        assert!(candidates.contains(rid.0));
    }
}

#[test]
fn h09_hnsw_rebuild_after_reopen_returns_same_results() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("hnsw_rebuild.db");
    let db = Database::open(&path).expect("open");
    setup_items_table(&db, DIMENSION);

    let query = random_unit_vector(DIMENSION, 9_999);

    let tx = db.begin();
    // 10 vectors CLOSE to query (cosine > 0.99)
    for i in 0..10u64 {
        let rid = db
            .insert_row(
                tx,
                "items",
                values(vec![("id", Value::Uuid(Uuid::new_v4()))]),
            )
            .expect("insert row");
        let close_vec = perturb_vector(&query, 0.01, i);
        db.insert_vector(
            tx,
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            rid,
            close_vec,
        )
        .expect("insert vector");
    }
    // 1090 vectors FAR from query
    for i in 10..1100 {
        let rid = db
            .insert_row(
                tx,
                "items",
                values(vec![("id", Value::Uuid(Uuid::new_v4()))]),
            )
            .expect("insert row");
        db.insert_vector(
            tx,
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            rid,
            random_unit_vector(DIMENSION, i + 50_000),
        )
        .expect("insert vector");
    }
    db.commit(tx).expect("commit");

    let pre_results = db
        .query_vector(
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            &query,
            10,
            None,
            db.snapshot(),
        )
        .expect("pre query");
    let pre_ids = pre_results.iter().map(|r| r.0).collect::<HashSet<_>>();
    assert_eq!(pre_ids.len(), 10);
    db.close().expect("close");

    let db2 = Database::open(&path).expect("reopen");
    let explain = db2
        .explain("SELECT * FROM items ORDER BY embedding <=> $q LIMIT 10")
        .expect("explain");
    let post_results = db2
        .query_vector(
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            &query,
            10,
            None,
            db2.snapshot(),
        )
        .expect("post query");
    let post_ids = post_results.iter().map(|r| r.0).collect::<HashSet<_>>();

    assert!(explain.contains("HNSWSearch"));
    assert_eq!(pre_ids, post_ids);
}

#[test]
fn h10_row_id_continuity_survives_reopen() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("hnsw_counter.db");
    let db = Database::open(&path).expect("open");
    setup_items_table(&db, DIMENSION);

    let tx = db.begin();
    let mut pre_rids = HashSet::new();
    for i in 0..1_100 {
        let rid = db
            .insert_row(
                tx,
                "items",
                values(vec![("id", Value::Uuid(Uuid::new_v4()))]),
            )
            .expect("insert row");
        db.insert_vector(
            tx,
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            rid,
            random_unit_vector(DIMENSION, i),
        )
        .expect("insert vector");
        pre_rids.insert(rid);
    }
    db.commit(tx).expect("commit");
    db.close().expect("close");

    let db2 = Database::open(&path).expect("reopen");
    let tx2 = db2.begin();
    let mut post_rids = HashSet::new();
    for i in 1_100..1_200 {
        let rid = db2
            .insert_row(
                tx2,
                "items",
                values(vec![("id", Value::Uuid(Uuid::new_v4()))]),
            )
            .expect("insert row");
        db2.insert_vector(
            tx2,
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            rid,
            random_unit_vector(DIMENSION, i),
        )
        .expect("insert vector");
        post_rids.insert(rid);
    }
    db2.commit(tx2).expect("commit");

    assert_eq!(pre_rids.intersection(&post_rids).count(), 0);
    let rows = db2.scan("items", db2.snapshot()).expect("scan");
    assert_eq!(rows.len(), 1_200);
}

#[test]
fn h11_concurrent_reads_during_hnsw_search_do_not_panic() {
    let db = Arc::new(Database::open_memory());
    setup_items_table(&db, DIMENSION);
    insert_items(&db, 1_100, 0);

    let mut handles = Vec::new();
    for t in 0..4 {
        let db_ref = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            let query = random_unit_vector(DIMENSION, 7_000 + t);
            let snap = db_ref.snapshot();
            let results = db_ref
                .query_vector(
                    contextdb_core::VectorIndexRef::new("items", "embedding"),
                    &query,
                    10,
                    None,
                    snap,
                )
                .expect("query");
            assert_eq!(results.len(), 10);
        }));
    }

    for handle in handles {
        handle.join().expect("thread join");
    }
}

#[test]
fn h12_insert_after_hnsw_activation_is_searchable() {
    let db = Database::open_memory();
    setup_items_table(&db, DIMENSION);
    insert_items(&db, 1_000, 0);

    let explain = db
        .explain("SELECT * FROM items ORDER BY embedding <=> $q LIMIT 10")
        .expect("explain");
    let tx = db.begin();
    let target_vec = random_unit_vector(DIMENSION, 9_999);
    let rid_new = db
        .insert_row(
            tx,
            "items",
            values(vec![("id", Value::Uuid(Uuid::new_v4()))]),
        )
        .expect("insert row");
    db.insert_vector(
        tx,
        contextdb_core::VectorIndexRef::new("items", "embedding"),
        rid_new,
        target_vec.clone(),
    )
    .expect("insert vector");
    db.commit(tx).expect("commit");

    let results = db
        .query_vector(
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            &target_vec,
            1,
            None,
            db.snapshot(),
        )
        .expect("query");

    assert!(explain.contains("HNSWSearch"));
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, rid_new);
    assert!(results[0].1 > 0.99);
}

#[test]
fn h13_exact_threshold_has_hnsw_and_high_recall() {
    let db = Database::open_memory();
    setup_items_table(&db, DIMENSION);
    let all_vectors = insert_items(&db, 1_000, 0);

    let explain = db
        .explain("SELECT * FROM items ORDER BY embedding <=> $q LIMIT 10")
        .expect("explain");
    let avg_recall = measure_avg_recall(&db, &all_vectors, 10, 5_000..5_020);

    assert!(explain.contains("HNSWSearch"));
    assert!(avg_recall >= 0.95, "avg recall was {avg_recall}");
}

#[test]
fn h14_all_deleted_vectors_return_empty_results() {
    let db = Database::open_memory();
    setup_items_table(&db, DIMENSION);
    let all_vectors = insert_items(&db, 1_100, 0);
    let all_rids = all_vectors.iter().map(|(rid, _)| *rid).collect::<Vec<_>>();

    let tx = db.begin();
    for rid in &all_rids {
        db.delete_row(tx, "items", *rid).expect("delete row");
        db.delete_vector(
            tx,
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            *rid,
        )
        .expect("delete vector");
    }
    db.commit(tx).expect("commit");

    let explain = db
        .explain("SELECT * FROM items ORDER BY embedding <=> $q LIMIT 10")
        .expect("explain");
    let query = random_unit_vector(DIMENSION, 9_999);
    let results = db
        .query_vector(
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            &query,
            10,
            None,
            db.snapshot(),
        )
        .expect("query");

    assert!(explain.contains("HNSWSearch"));
    assert!(results.is_empty());
}

#[test]
fn h15_single_surviving_vector_is_returned() {
    let db = Database::open_memory();
    setup_items_table(&db, DIMENSION);

    let tx1 = db.begin();
    let mut all_rids = Vec::with_capacity(1_100);
    let survivor_vec = random_unit_vector(DIMENSION, 999);
    let mut survivor_rid = RowId(0);
    for i in 0..1_100 {
        let rid = db
            .insert_row(
                tx1,
                "items",
                values(vec![("id", Value::Uuid(Uuid::new_v4()))]),
            )
            .expect("insert row");
        let vec = if i == 500 {
            survivor_vec.clone()
        } else {
            random_unit_vector(DIMENSION, i as u64)
        };
        db.insert_vector(
            tx1,
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            rid,
            vec,
        )
        .expect("insert vector");
        if i == 500 {
            survivor_rid = rid;
        }
        all_rids.push(rid);
    }
    db.commit(tx1).expect("commit tx1");

    let tx2 = db.begin();
    for rid in &all_rids {
        if *rid != survivor_rid {
            db.delete_row(tx2, "items", *rid).expect("delete row");
            db.delete_vector(
                tx2,
                contextdb_core::VectorIndexRef::new("items", "embedding"),
                *rid,
            )
            .expect("delete vector");
        }
    }
    db.commit(tx2).expect("commit tx2");

    let explain = db
        .explain("SELECT * FROM items ORDER BY embedding <=> $q LIMIT 1")
        .expect("explain");
    let results = db
        .query_vector(
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            &survivor_vec,
            1,
            None,
            db.snapshot(),
        )
        .expect("query");

    assert!(explain.contains("HNSWSearch"));
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, survivor_rid);
}

#[test]
fn h16_dimension_mismatch_is_rejected_when_hnsw_would_be_active() {
    let db = Database::open_memory();
    setup_items_table(&db, 3);

    let tx1 = db.begin();
    for _ in 0..1_000 {
        let rid = db
            .insert_row(
                tx1,
                "items",
                values(vec![("id", Value::Uuid(Uuid::new_v4()))]),
            )
            .expect("insert row");
        db.insert_vector(
            tx1,
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            rid,
            vec![1.0, 0.0, 0.0],
        )
        .expect("insert vector");
    }
    db.commit(tx1).expect("commit");

    let tx2 = db.begin();
    let rid = db
        .insert_row(
            tx2,
            "items",
            values(vec![("id", Value::Uuid(Uuid::new_v4()))]),
        )
        .expect("insert row");
    let result = db.insert_vector(
        tx2,
        contextdb_core::VectorIndexRef::new("items", "embedding"),
        rid,
        vec![1.0, 0.0, 0.0, 0.0, 0.0],
    );

    assert!(matches!(
        result,
        Err(Error::VectorIndexDimensionMismatch {
            index,
            expected: 3,
            actual: 5
        }) if index == contextdb_core::VectorIndexRef::new("items", "embedding")
    ));
}

#[test]
fn h17_below_threshold_explain_stays_vector_search() {
    let db = Database::open_memory();
    setup_items_table(&db, DIMENSION);
    insert_items(&db, 100, 0);

    let explain = db
        .explain("SELECT * FROM items ORDER BY embedding <=> $q LIMIT 5")
        .expect("explain");

    assert!(explain.contains("VectorSearch"));
    assert!(!explain.contains("HNSWSearch"));
}

#[test]
fn h19_relational_graph_and_vector_atomicity_hold_under_hnsw() {
    let db = Database::open_memory();
    setup_items_table(&db, DIMENSION);

    let tx = db.begin();
    let mut item_uuids = Vec::with_capacity(1_000);
    for i in 0..1_000 {
        let uuid = Uuid::new_v4();
        let rid = db
            .insert_row(tx, "items", values(vec![("id", Value::Uuid(uuid))]))
            .expect("insert row");
        db.insert_vector(
            tx,
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            rid,
            random_unit_vector(DIMENSION, i),
        )
        .expect("insert vector");
        item_uuids.push(uuid);
    }
    db.commit(tx).expect("commit");

    let explain = db
        .explain("SELECT * FROM items ORDER BY embedding <=> $q LIMIT 10")
        .expect("explain");

    let tx_commit = db.begin();
    let uuid_commit = Uuid::new_v4();
    let rid_commit = db
        .insert_row(
            tx_commit,
            "items",
            values(vec![("id", Value::Uuid(uuid_commit))]),
        )
        .expect("insert row");
    db.insert_edge(
        tx_commit,
        uuid_commit,
        item_uuids[0],
        "LINKS".to_string(),
        HashMap::new(),
    )
    .expect("insert edge");
    let vec_commit = random_unit_vector(DIMENSION, 8_888);
    db.insert_vector(
        tx_commit,
        contextdb_core::VectorIndexRef::new("items", "embedding"),
        rid_commit,
        vec_commit.clone(),
    )
    .expect("insert vector");
    db.commit(tx_commit).expect("commit tx");
    let snapshot = db.snapshot();

    assert!(explain.contains("HNSWSearch"));
    assert!(
        db.point_lookup("items", "id", &Value::Uuid(uuid_commit), snapshot)
            .expect("point lookup")
            .is_some()
    );
    let bfs = db
        .query_bfs(
            uuid_commit,
            Some(&["LINKS".to_string()]),
            Direction::Outgoing,
            1,
            snapshot,
        )
        .expect("bfs");
    assert_eq!(bfs.nodes.len(), 1);
    assert_eq!(bfs.nodes[0].id, item_uuids[0]);
    let vec_results = db
        .query_vector(
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            &vec_commit,
            1,
            None,
            snapshot,
        )
        .expect("vector query");
    assert_eq!(vec_results[0].0, rid_commit);

    let tx_rb = db.begin();
    let uuid_rb = Uuid::new_v4();
    let rid_rb = db
        .insert_row(tx_rb, "items", values(vec![("id", Value::Uuid(uuid_rb))]))
        .expect("insert row");
    db.insert_edge(
        tx_rb,
        uuid_rb,
        item_uuids[0],
        "LINKS".to_string(),
        HashMap::new(),
    )
    .expect("insert edge");
    let vec_rb = random_unit_vector(DIMENSION, 7_777);
    db.insert_vector(
        tx_rb,
        contextdb_core::VectorIndexRef::new("items", "embedding"),
        rid_rb,
        vec_rb.clone(),
    )
    .expect("insert vector");
    db.rollback(tx_rb).expect("rollback");
    let snapshot2 = db.snapshot();

    assert!(
        db.point_lookup("items", "id", &Value::Uuid(uuid_rb), snapshot2)
            .expect("point lookup")
            .is_none()
    );
    let bfs_rb = db
        .query_bfs(
            uuid_rb,
            Some(&["LINKS".to_string()]),
            Direction::Outgoing,
            1,
            snapshot2,
        )
        .expect("bfs");
    assert!(bfs_rb.nodes.is_empty());
    let vec_results_rb = db
        .query_vector(
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            &vec_rb,
            10,
            None,
            snapshot2,
        )
        .expect("vector query");
    assert!(vec_results_rb.iter().all(|(rid, _)| *rid != rid_rb));
}

#[test]
fn h20_hnsw_and_other_data_persist_across_reopen() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("hnsw_persistence.db");
    let db = Database::open(&path).expect("open");
    setup_items_table(&db, DIMENSION);

    let tx = db.begin();
    let mut all_uuids = Vec::with_capacity(1_100);
    for i in 0..1_100 {
        let uuid = Uuid::new_v4();
        let rid = db
            .insert_row(tx, "items", values(vec![("id", Value::Uuid(uuid))]))
            .expect("insert row");
        db.insert_vector(
            tx,
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            rid,
            random_unit_vector(DIMENSION, i),
        )
        .expect("insert vector");
        all_uuids.push(uuid);
    }
    db.commit(tx).expect("commit");

    let explain_pre = db
        .explain("SELECT * FROM items ORDER BY embedding <=> $q LIMIT 10")
        .expect("explain");
    assert!(explain_pre.contains("HNSWSearch"));

    let tx2 = db.begin();
    for i in 0..10 {
        db.insert_edge(
            tx2,
            all_uuids[i],
            all_uuids[10],
            "LINKS".to_string(),
            HashMap::new(),
        )
        .expect("insert edge");
    }
    db.commit(tx2).expect("commit");
    db.close().expect("close");

    let db2 = Database::open(&path).expect("reopen");
    let snapshot = db2.snapshot();
    let explain_post = db2
        .explain("SELECT * FROM items ORDER BY embedding <=> $q LIMIT 10")
        .expect("explain");
    let query = random_unit_vector(DIMENSION, 9_999);
    let results = db2
        .query_vector(
            contextdb_core::VectorIndexRef::new("items", "embedding"),
            &query,
            10,
            None,
            snapshot,
        )
        .expect("query");
    let row = db2
        .point_lookup("items", "id", &Value::Uuid(all_uuids[0]), snapshot)
        .expect("point lookup");
    let bfs = db2
        .query_bfs(
            all_uuids[0],
            Some(&["LINKS".to_string()]),
            Direction::Outgoing,
            1,
            snapshot,
        )
        .expect("bfs");

    assert!(explain_post.contains("HNSWSearch"));
    assert_eq!(results.len(), 10);
    assert!(row.is_some());
    assert!(bfs.nodes.iter().any(|node| node.id == all_uuids[10]));
}
