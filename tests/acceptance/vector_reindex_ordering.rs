use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn batch_reindex_vector(i: usize, total: usize) -> Vec<f32> {
    let theta = (i as f32) * std::f32::consts::TAU / (total as f32);
    vec![0.0, theta.cos(), theta.sin()]
}

/// RED — t17_01: same-row delete+insert in one tx; recall returns NEW vector.
#[test]
fn t17_01_general_delete_then_insert_recall_returns_new_vector() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE memories (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();

    let id = Uuid::from_u128(1);
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(id));
    p.insert("v".into(), Value::Vector(vec![1.0, 0.0, 0.0]));
    db.execute("INSERT INTO memories (id, embedding) VALUES ($id, $v)", &p)
        .unwrap();

    // Find the row_id allocated to that UUID.
    let scan = db.scan("memories", db.snapshot()).unwrap();
    assert_eq!(scan.len(), 1, "fixture row missing");
    let row_id = scan[0].row_id;

    // General delete+insert in one tx using engine API.
    let idx = VectorIndexRef::new("memories", "embedding");
    let tx = db.begin();
    db.delete_vector(tx, idx.clone(), row_id).unwrap();
    db.insert_vector(tx, idx.clone(), row_id, vec![0.0, 0.0, 1.0])
        .unwrap();
    db.commit(tx).unwrap();

    let hits = db
        .query_vector(idx.clone(), &[0.0, 0.0, 1.0], 1, None, db.snapshot())
        .unwrap();
    assert_eq!(hits.len(), 1, "expected one nearest neighbor");
    assert_eq!(hits[0].0, row_id, "nearest must be the reindexed row");
    assert!(
        hits[0].1 > 0.99,
        "cosine similarity to NEW vector [0,0,1] must be near 1.0; got {} \
         (if ~0.0, the OLD vector [1,0,0] is still indexed — apply order bug)",
        hits[0].1
    );

    // Negative — OLD-vector-gone: querying the OLD vector must NOT rank the
    // reindexed row as a high-similarity hit. A "ignore deletes, append" fix
    // would still match here.
    let old_hits = db
        .query_vector(idx, &[1.0, 0.0, 0.0], 1, None, db.snapshot())
        .unwrap();
    if !old_hits.is_empty() && old_hits[0].0 == row_id {
        assert!(
            old_hits[0].1 < 0.5,
            "reindexed row's similarity to OLD vector [1,0,0] must be < 0.5; got {} \
             — old vector still indexed (ignore-delete-append wrong fix)",
            old_hits[0].1
        );
    }
}

/// RED — t17_02: 50 same-row delete+insert pairs in one tx; every row resolves to its NEW vector.
#[test]
fn t17_02_batch_reindex_all_rows_resolve_to_new_vectors() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE memories (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();

    let mut row_ids = Vec::new();
    for i in 0..50 {
        let id = Uuid::from_u128((i + 1) as u128);
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(id));
        p.insert("v".into(), Value::Vector(vec![1.0, 0.0, 0.0]));
        db.execute("INSERT INTO memories (id, embedding) VALUES ($id, $v)", &p)
            .unwrap();
        let scan = db.scan("memories", db.snapshot()).unwrap();
        row_ids.push(scan.last().unwrap().row_id);
    }

    let idx = VectorIndexRef::new("memories", "embedding");
    let tx = db.begin();
    for (i, row_id) in row_ids.iter().enumerate() {
        let new_vec = batch_reindex_vector(i, row_ids.len());
        db.delete_vector(tx, idx.clone(), *row_id).unwrap();
        db.insert_vector(tx, idx.clone(), *row_id, new_vec).unwrap();
    }
    db.commit(tx).unwrap();

    // Every row must respond to a nearest-vector query against its NEW value.
    for (i, row_id) in row_ids.iter().enumerate() {
        let q = batch_reindex_vector(i, row_ids.len());
        let hits = db
            .query_vector(idx.clone(), &q, 1, None, db.snapshot())
            .unwrap();
        assert!(
            !hits.is_empty() && hits[0].0 == *row_id && hits[0].1 > 0.99,
            "row {} (row_id={:?}) failed to recall new vector — apply order bug",
            i,
            row_id
        );
    }

    // Negative — OLD-vector-gone: querying [1,0,0] (the old shared vector)
    // must NOT rank any of the 50 reindexed rows at high similarity.
    let old_hits = db
        .query_vector(idx, &[1.0, 0.0, 0.0], 50, None, db.snapshot())
        .unwrap();
    let row_id_set: std::collections::HashSet<_> = row_ids.iter().copied().collect();
    for (rid, score) in &old_hits {
        if row_id_set.contains(rid) {
            assert!(
                *score < 0.5,
                "row_id={:?} still matches OLD vector [1,0,0] with score {} — old-vector-gone failed",
                rid,
                score
            );
        }
    }
}

/// RED — t17_08: same-row reindex must invalidate/rebuild the HNSW ANN path too.
#[test]
fn t17_08_hnsw_delete_then_insert_recall_returns_new_vector() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE memories (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();

    let target_id = Uuid::from_u128(1);
    let mut target = HashMap::new();
    target.insert("id".into(), Value::Uuid(target_id));
    target.insert("v".into(), Value::Vector(vec![1.0, 0.0, 0.0]));
    db.execute(
        "INSERT INTO memories (id, embedding) VALUES ($id, $v)",
        &target,
    )
    .unwrap();

    // Cross the in-memory executor's HNSW threshold. Fillers are intentionally
    // close to the NEW vector, while the target's OLD vector is orthogonal.
    for i in 0..1_000u128 {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::from_u128(i + 2)));
        p.insert("v".into(), Value::Vector(vec![0.0, 0.2, 0.98]));
        db.execute("INSERT INTO memories (id, embedding) VALUES ($id, $v)", &p)
            .unwrap();
    }

    let idx = VectorIndexRef::new("memories", "embedding");
    let scan = db.scan("memories", db.snapshot()).unwrap();
    let target_row_id = scan
        .iter()
        .find(|r| matches!(r.values.get("id"), Some(Value::Uuid(u)) if *u == target_id))
        .unwrap()
        .row_id;

    let warm = db
        .query_vector(idx.clone(), &[0.0, 0.0, 1.0], 10, None, db.snapshot())
        .unwrap();
    assert!(
        !warm.is_empty(),
        "warmup ANN query should return filler rows"
    );
    let stats = db
        .__debug_vector_hnsw_stats(idx.clone())
        .expect("fixture must build the HNSW graph before reindex");
    assert_eq!(
        stats.point_count, 1_001,
        "fixture must exercise a raw HNSW graph for all live vectors before reindex; stats={stats:?}"
    );
    assert!(
        stats.layer0_neighbor_edges > 0,
        "fixture must expose real HNSW neighbor edges, not only a fake point count; stats={stats:?}"
    );
    assert_eq!(
        db.__debug_vector_hnsw_raw_entry_count_for_row_for_test(idx.clone(), target_row_id)
            .expect("target row must have one raw HNSW entry before reindex"),
        1,
        "target should have exactly one raw HNSW entry before reindex"
    );

    let tx = db.begin();
    db.delete_vector(tx, idx.clone(), target_row_id).unwrap();
    db.insert_vector(tx, idx.clone(), target_row_id, vec![0.0, 0.0, 1.0])
        .unwrap();
    db.commit(tx).unwrap();

    let hits = db
        .query_vector(idx.clone(), &[0.0, 0.0, 1.0], 10, None, db.snapshot())
        .unwrap();
    assert!(
        !hits.is_empty() && hits[0].0 == target_row_id && hits[0].1 > 0.99,
        "HNSW reindex must rank the reindexed row's NEW vector first; hits={hits:?}"
    );

    let raw_hnsw_hits = db
        .__debug_vector_hnsw_raw_search_for_test(idx.clone(), &[0.0, 0.0, 1.0], 10)
        .expect("HNSW graph must remain built after reindex, not fall back to brute force only");
    assert!(
        !raw_hnsw_hits.is_empty()
            && raw_hnsw_hits[0].0 == target_row_id
            && raw_hnsw_hits[0].1 > 0.99,
        "raw HNSW graph itself must contain the reindexed row's NEW vector; raw_hnsw_hits={raw_hnsw_hits:?}"
    );
    let post_stats = db
        .__debug_vector_hnsw_stats(idx.clone())
        .expect("HNSW graph must remain built after reindex");
    assert_eq!(
        post_stats.point_count, 1_001,
        "HNSW graph must be rebuilt from live entries, not append the new vector beside the stale one; stats={post_stats:?}"
    );
    assert_eq!(
        db.__debug_vector_hnsw_raw_entry_count_for_row_for_test(idx.clone(), target_row_id)
            .expect("target row must have one raw HNSW entry after reindex"),
        1,
        "raw HNSW graph must not retain both stale and current entries for the reindexed row"
    );

    let old_hits = db
        .query_vector(idx, &[1.0, 0.0, 0.0], 10, None, db.snapshot())
        .unwrap();
    for (row_id, score) in &old_hits {
        if *row_id == target_row_id {
            assert!(
                *score < 0.5,
                "HNSW still returns the reindexed row for the OLD vector with score {score}; hits={old_hits:?}"
            );
        }
    }
}

/// REGRESSION GUARD — t17_03: delete-only on row A + insert on row B in same tx; both visible at commit.
#[test]
fn t17_03_delete_one_insert_other_in_same_tx_both_visible() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE memories (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();

    let id_a = Uuid::from_u128(1);
    let id_b = Uuid::from_u128(2);
    for (id, v) in [(id_a, vec![1.0, 0.0, 0.0]), (id_b, vec![0.0, 1.0, 0.0])] {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(id));
        p.insert("v".into(), Value::Vector(v));
        db.execute("INSERT INTO memories (id, embedding) VALUES ($id, $v)", &p)
            .unwrap();
    }
    let scan = db.scan("memories", db.snapshot()).unwrap();
    let row_id_a = scan
        .iter()
        .find(|r| matches!(r.values.get("id"), Some(Value::Uuid(u)) if *u == id_a))
        .unwrap()
        .row_id;
    // row B not pre-inserted into vector index — we'll insert via engine API in the tx
    let row_id_b = scan
        .iter()
        .find(|r| matches!(r.values.get("id"), Some(Value::Uuid(u)) if *u == id_b))
        .unwrap()
        .row_id;

    let idx = VectorIndexRef::new("memories", "embedding");
    // Pre-condition: A's vector is queryable.
    let pre = db
        .query_vector(idx.clone(), &[1.0, 0.0, 0.0], 1, None, db.snapshot())
        .unwrap();
    assert_eq!(pre[0].0, row_id_a);

    let tx = db.begin();
    db.delete_vector(tx, idx.clone(), row_id_a).unwrap();
    db.insert_vector(tx, idx.clone(), row_id_b, vec![0.0, 0.0, 1.0])
        .unwrap();
    db.commit(tx).unwrap();

    // After commit: A is gone, B has the new vector
    let q_old = db
        .query_vector(idx.clone(), &[1.0, 0.0, 0.0], 1, None, db.snapshot())
        .unwrap();
    if !q_old.is_empty() {
        assert!(q_old[0].1 < 0.5, "row A vector should be gone");
    }
    let q_new = db
        .query_vector(idx, &[0.0, 0.0, 1.0], 1, None, db.snapshot())
        .unwrap();
    assert_eq!(q_new.len(), 1);
    assert_eq!(q_new[0].0, row_id_b);
    assert!(q_new[0].1 > 0.99);

    let q_b_old = db
        .query_vector(
            VectorIndexRef::new("memories", "embedding"),
            &[0.0, 1.0, 0.0],
            1,
            None,
            db.snapshot(),
        )
        .unwrap();
    if !q_b_old.is_empty() && q_b_old[0].0 == row_id_b {
        assert!(
            q_b_old[0].1 < 0.5,
            "row B's original vector must be replaced, not duplicated"
        );
    }
}

/// REGRESSION GUARD — t17_04: insert without preceding delete still visible at commit.
#[test]
fn t17_04_pure_insert_visible_at_commit() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE memories (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
    let id = Uuid::from_u128(1);
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(id));
    p.insert("v".into(), Value::Vector(vec![0.5, 0.5, 0.0]));
    db.execute("INSERT INTO memories (id, embedding) VALUES ($id, $v)", &p)
        .unwrap();

    let idx = VectorIndexRef::new("memories", "embedding");
    let hits = db
        .query_vector(idx, &[0.5, 0.5, 0.0], 1, None, db.snapshot())
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert!(hits[0].1 > 0.99);
}

/// RED — t17_05: per-index isolation; reindex on index A keeps index B's row vector intact.
#[test]
fn t17_05_per_index_isolation_for_same_row() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (id UUID PRIMARY KEY, vector_text VECTOR(3), vector_vision VECTOR(3))",
        &empty(),
    ).unwrap();

    let id = Uuid::from_u128(1);
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(id));
    p.insert("vt".into(), Value::Vector(vec![1.0, 0.0, 0.0]));
    p.insert("vv".into(), Value::Vector(vec![0.0, 1.0, 0.0]));
    db.execute(
        "INSERT INTO evidence (id, vector_text, vector_vision) VALUES ($id, $vt, $vv)",
        &p,
    )
    .unwrap();

    let scan = db.scan("evidence", db.snapshot()).unwrap();
    let row_id = scan[0].row_id;

    let idx_text = VectorIndexRef::new("evidence", "vector_text");
    let idx_vision = VectorIndexRef::new("evidence", "vector_vision");

    // Reindex only the text vector.
    let tx = db.begin();
    db.delete_vector(tx, idx_text.clone(), row_id).unwrap();
    db.insert_vector(tx, idx_text.clone(), row_id, vec![0.0, 0.0, 1.0])
        .unwrap();
    db.commit(tx).unwrap();

    // Text resolves to NEW vector
    let h_text = db
        .query_vector(idx_text.clone(), &[0.0, 0.0, 1.0], 1, None, db.snapshot())
        .unwrap();
    assert_eq!(
        h_text.len(),
        1,
        "text reindex must return one hit for the new vector"
    );
    assert_eq!(h_text[0].0, row_id);
    assert!(
        h_text[0].1 > 0.99,
        "text reindex should resolve to NEW vector"
    );

    // Negative — OLD-vector-gone for idx_text: querying [1,0,0] must not rank
    // the reindexed row at high similarity on idx_text.
    let h_text_old = db
        .query_vector(idx_text, &[1.0, 0.0, 0.0], 1, None, db.snapshot())
        .unwrap();
    if !h_text_old.is_empty() && h_text_old[0].0 == row_id {
        assert!(
            h_text_old[0].1 < 0.5,
            "old text vector still indexed for reindexed row; got {}",
            h_text_old[0].1
        );
    }

    // Vision still resolves to ORIGINAL vector
    let h_vision = db
        .query_vector(idx_vision, &[0.0, 1.0, 0.0], 1, None, db.snapshot())
        .unwrap();
    assert_eq!(
        h_vision.len(),
        1,
        "vision index must still return one hit for the original vector"
    );
    assert_eq!(h_vision[0].0, row_id);
    assert!(
        h_vision[0].1 > 0.99,
        "vision vector must remain untouched by text reindex"
    );
}

/// RED — t17_06: file-backed reindex survives drop+reopen.
#[test]
fn t17_06_general_delete_insert_survives_reopen() {
    use tempfile::TempDir;
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("reindex.redb");

    let row_id;
    {
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE memories (id UUID PRIMARY KEY, embedding VECTOR(3))",
            &empty(),
        )
        .unwrap();
        let id = Uuid::from_u128(1);
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(id));
        p.insert("v".into(), Value::Vector(vec![1.0, 0.0, 0.0]));
        db.execute("INSERT INTO memories (id, embedding) VALUES ($id, $v)", &p)
            .unwrap();

        let scan = db.scan("memories", db.snapshot()).unwrap();
        row_id = scan[0].row_id;

        let idx = VectorIndexRef::new("memories", "embedding");
        let tx = db.begin();
        db.delete_vector(tx, idx.clone(), row_id).unwrap();
        db.insert_vector(tx, idx, row_id, vec![0.0, 0.0, 1.0])
            .unwrap();
        db.commit(tx).unwrap();
        db.close().unwrap();
    }

    let db2 = Database::open(&path).unwrap();
    let idx = VectorIndexRef::new("memories", "embedding");
    let hits = db2
        .query_vector(idx.clone(), &[0.0, 0.0, 1.0], 1, None, db2.snapshot())
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(
        hits[0].0, row_id,
        "reindexed row must persist across reopen"
    );
    assert!(hits[0].1 > 0.99, "new vector must survive reopen");

    let old = db2
        .query_vector(idx, &[1.0, 0.0, 0.0], 1, None, db2.snapshot())
        .unwrap();
    if !old.is_empty() && old[0].0 == row_id {
        assert!(old[0].1 < 0.5, "old vector must be gone after reopen too");
    }
}

/// REGRESSION GUARD — t17_07: SQL UPDATE embedding rewrites recall (the cg `reindex` API surface).
#[test]
fn t17_07_update_embedding_via_sql_recall_returns_new_vector() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE memories (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
    let id = Uuid::from_u128(1);
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(id));
    p.insert("v".into(), Value::Vector(vec![1.0, 0.0, 0.0]));
    db.execute("INSERT INTO memories (id, embedding) VALUES ($id, $v)", &p)
        .unwrap();

    // SQL UPDATE — the path cg's reindex API actually issues.
    let mut p2 = HashMap::new();
    p2.insert("id".into(), Value::Uuid(id));
    p2.insert("v".into(), Value::Vector(vec![0.0, 0.0, 1.0]));
    db.execute("UPDATE memories SET embedding = $v WHERE id = $id", &p2)
        .unwrap();

    let scan = db.scan("memories", db.snapshot()).unwrap();
    let row_id = scan[0].row_id;

    let idx = VectorIndexRef::new("memories", "embedding");
    let hits = db
        .query_vector(idx.clone(), &[0.0, 0.0, 1.0], 1, None, db.snapshot())
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].0, row_id);
    assert!(
        hits[0].1 > 0.99,
        "SQL UPDATE-embedding must rewrite recall to NEW vector"
    );

    // OLD-vector-gone via SQL path.
    let old = db
        .query_vector(idx, &[1.0, 0.0, 0.0], 1, None, db.snapshot())
        .unwrap();
    if !old.is_empty() && old[0].0 == row_id {
        assert!(
            old[0].1 < 0.5,
            "SQL UPDATE must not leave the old vector behind"
        );
    }
}

/// REGRESSION GUARD — t17_09: delete cancels all same-tx pending inserts for the row.
#[test]
fn t17_09_same_tx_pending_inserts_then_delete_leave_no_vector_after_reopen() {
    use tempfile::TempDir;
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("insert_then_delete.redb");

    let row_id;
    {
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE memories (id UUID PRIMARY KEY, embedding VECTOR(3))",
            &empty(),
        )
        .unwrap();

        let tx = db.begin();
        let mut values = HashMap::new();
        values.insert("id".into(), Value::Uuid(Uuid::from_u128(1)));
        row_id = db.insert_row(tx, "memories", values).unwrap();
        db.commit(tx).unwrap();

        let idx = VectorIndexRef::new("memories", "embedding");
        let tx = db.begin();
        db.insert_vector(tx, idx.clone(), row_id, vec![1.0, 0.0, 0.0])
            .unwrap();
        db.insert_vector(tx, idx.clone(), row_id, vec![0.0, 0.0, 1.0])
            .unwrap();
        db.delete_vector(tx, idx.clone(), row_id).unwrap();
        db.commit(tx).unwrap();

        let hits = db
            .query_vector(idx, &[0.0, 0.0, 1.0], 10, None, db.snapshot())
            .unwrap();
        assert!(
            hits.is_empty(),
            "insert+insert+delete in one tx must leave no live vector; hits={hits:?}"
        );
        db.close().unwrap();
    }

    let db = Database::open(&path).unwrap();
    let idx = VectorIndexRef::new("memories", "embedding");
    let hits = db
        .query_vector(idx, &[1.0, 0.0, 0.0], 10, None, db.snapshot())
        .unwrap();
    assert!(
        hits.is_empty(),
        "same-tx canceled inserts must not reappear after reopen; row_id={row_id:?}, hits={hits:?}"
    );
}

/// REGRESSION GUARD — t17_10: repeated same-row inserts in one tx are last-write-wins.
#[test]
fn t17_10_same_tx_repeated_inserts_keep_only_latest_after_reopen() {
    use tempfile::TempDir;
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("repeated_inserts.redb");

    let row_id;
    {
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE memories (id UUID PRIMARY KEY, embedding VECTOR(3))",
            &empty(),
        )
        .unwrap();

        let tx = db.begin();
        let mut values = HashMap::new();
        values.insert("id".into(), Value::Uuid(Uuid::from_u128(1)));
        row_id = db.insert_row(tx, "memories", values).unwrap();
        db.commit(tx).unwrap();

        let idx = VectorIndexRef::new("memories", "embedding");
        let tx = db.begin();
        db.insert_vector(tx, idx.clone(), row_id, vec![1.0, 0.0, 0.0])
            .unwrap();
        db.insert_vector(tx, idx.clone(), row_id, vec![0.0, 0.0, 1.0])
            .unwrap();
        db.commit(tx).unwrap();

        let old_hits = db
            .query_vector(idx.clone(), &[1.0, 0.0, 0.0], 10, None, db.snapshot())
            .unwrap();
        for (hit_row_id, score) in &old_hits {
            assert!(
                *hit_row_id != row_id || *score < 0.5,
                "old same-tx insert must not stay live before reopen; hits={old_hits:?}"
            );
        }
        db.close().unwrap();
    }

    let db = Database::open(&path).unwrap();
    let idx = VectorIndexRef::new("memories", "embedding");
    let hits = db
        .query_vector(idx.clone(), &[0.0, 0.0, 1.0], 10, None, db.snapshot())
        .unwrap();
    assert!(
        !hits.is_empty() && hits[0].0 == row_id && hits[0].1 > 0.99,
        "latest same-tx insert must survive reopen; hits={hits:?}"
    );
    let old_hits = db
        .query_vector(idx, &[1.0, 0.0, 0.0], 10, None, db.snapshot())
        .unwrap();
    for (hit_row_id, score) in &old_hits {
        assert!(
            *hit_row_id != row_id || *score < 0.5,
            "old same-tx insert must not reappear after reopen; hits={old_hits:?}"
        );
    }
}

/// REGRESSION GUARD — t17_11: a pending vector move cannot resurrect an old vector after replacement.
#[test]
fn t17_11_move_then_vector_replacement_in_same_tx_does_not_resurrect_old_vector() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE memories (id UUID PRIMARY KEY, label TEXT, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();

    let id = Uuid::from_u128(1);
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(id));
    p.insert("label".into(), Value::Text("old".into()));
    p.insert("v".into(), Value::Vector(vec![1.0, 0.0, 0.0]));
    db.execute(
        "INSERT INTO memories (id, label, embedding) VALUES ($id, $label, $v)",
        &p,
    )
    .unwrap();

    let tx = db.begin();
    let mut p1 = HashMap::new();
    p1.insert("id".into(), Value::Uuid(id));
    p1.insert("label".into(), Value::Text("renamed".into()));
    db.execute_in_tx(tx, "UPDATE memories SET label = $label WHERE id = $id", &p1)
        .unwrap();

    let mut p2 = HashMap::new();
    p2.insert("id".into(), Value::Uuid(id));
    p2.insert("v".into(), Value::Vector(vec![0.0, 0.0, 1.0]));
    db.execute_in_tx(tx, "UPDATE memories SET embedding = $v WHERE id = $id", &p2)
        .unwrap();
    db.commit(tx).unwrap();

    let idx = VectorIndexRef::new("memories", "embedding");
    let hits = db
        .query_vector(idx.clone(), &[0.0, 0.0, 1.0], 10, None, db.snapshot())
        .unwrap();
    assert!(
        !hits.is_empty() && hits[0].1 > 0.99,
        "replacement vector must be live after move+replace tx; hits={hits:?}"
    );

    let old_hits = db
        .query_vector(idx, &[1.0, 0.0, 0.0], 10, None, db.snapshot())
        .unwrap();
    for (_, score) in &old_hits {
        assert!(
            *score < 0.5,
            "pending move resurrected the old vector after replacement; hits={old_hits:?}"
        );
    }
}

/// REGRESSION GUARD — t17_12: chained row rewrites in one tx keep the vector on the final row.
#[test]
fn t17_12_chained_same_tx_updates_move_vector_to_final_row_after_reopen() {
    use tempfile::TempDir;
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("chained_moves.redb");

    let final_row_id;
    {
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE memories (id UUID PRIMARY KEY, label TEXT, embedding VECTOR(3))",
            &empty(),
        )
        .unwrap();

        let id = Uuid::from_u128(1);
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(id));
        p.insert("label".into(), Value::Text("a".into()));
        p.insert("v".into(), Value::Vector(vec![1.0, 0.0, 0.0]));
        db.execute(
            "INSERT INTO memories (id, label, embedding) VALUES ($id, $label, $v)",
            &p,
        )
        .unwrap();

        let tx = db.begin();
        let mut p1 = HashMap::new();
        p1.insert("id".into(), Value::Uuid(id));
        p1.insert("label".into(), Value::Text("b".into()));
        db.execute_in_tx(tx, "UPDATE memories SET label = $label WHERE id = $id", &p1)
            .unwrap();

        let mut p2 = HashMap::new();
        p2.insert("id".into(), Value::Uuid(id));
        p2.insert("label".into(), Value::Text("c".into()));
        db.execute_in_tx(tx, "UPDATE memories SET label = $label WHERE id = $id", &p2)
            .unwrap();
        db.commit(tx).unwrap();

        let rows = db.scan("memories", db.snapshot()).unwrap();
        assert_eq!(rows.len(), 1, "fixture should leave one final row");
        final_row_id = rows[0].row_id;
        assert_eq!(
            rows[0].values.get("label"),
            Some(&Value::Text("c".into())),
            "final row should be the second update"
        );

        let idx = VectorIndexRef::new("memories", "embedding");
        let hits = db
            .query_vector(idx, &[1.0, 0.0, 0.0], 10, None, db.snapshot())
            .unwrap();
        assert!(
            !hits.is_empty() && hits[0].0 == final_row_id && hits[0].1 > 0.99,
            "vector must move through chained same-tx updates to the final row; hits={hits:?}"
        );
        db.close().unwrap();
    }

    let db = Database::open(&path).unwrap();
    let idx = VectorIndexRef::new("memories", "embedding");
    let hits = db
        .query_vector(idx, &[1.0, 0.0, 0.0], 10, None, db.snapshot())
        .unwrap();
    assert!(
        !hits.is_empty() && hits[0].0 == final_row_id && hits[0].1 > 0.99,
        "chained vector move must survive reopen on the final row; hits={hits:?}"
    );
}
