//! Public SQL companions of the clustered-index unit-test ports. Maintenance
//! advances to its public ready report; no sleeps or cycle-count readiness.
#![cfg(feature = "test-seams")]
#![expect(
    clippy::result_large_err,
    reason = "The public error retains inline typed refusal details."
)]
use contextdb_core::{Error, Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::vector_ivf_observations::{
    NativeIvfWorkPoint, with_native_ivf_work_observer_for_test,
};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::{
    collections::HashMap,
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    },
};
fn empty() -> HashMap<String, Value> {
    HashMap::new()
}
fn open(path: &std::path::Path) -> Database {
    Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap()
}
fn ready(db: &Database) {
    loop {
        let r = db.run_maintenance_cycle().unwrap();
        assert!(
            r.vector.first_failure.is_none(),
            "public maintenance failure: {:?}",
            r.vector
        );
        if r.vector.remaining_partitions == 0 && r.vector.ready_partitions > 0 {
            return;
        }
    }
}
fn create(db: &Database, d: usize, quantization: &str) {
    create_as(db, d, quantization, "INDEXED");
}
/// `create` with the column's declared search mode.
fn create_as(db: &Database, d: usize, quantization: &str, search_mode: &str) {
    let quantization = if quantization.is_empty() {
        String::new()
    } else {
        format!("WITH (quantization = '{quantization}')")
    };
    db.execute(&format!("CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR({d}) {quantization} SEARCH_MODE {search_mode} SEARCH (TARGET_ACCURACY = 95, EXPECTED_VECTORS = DEFAULT) IVF (LISTS = 1, PROBES = 1, REFINE_FACTOR = 200.00)) HISTORY ALL"),&empty()).unwrap();
}
fn insert(db: &Database, id: i64, v: Vec<f32>) {
    db.execute(
        "INSERT INTO items VALUES ($id,$v)",
        &HashMap::from([
            ("id".into(), Value::Int64(id)),
            ("v".into(), Value::Vector(v)),
        ]),
    )
    .unwrap();
}
fn vector(seed: usize, d: usize) -> Vec<f32> {
    (0..d)
        .map(|a| (((seed * 13 + a * 7) % 29) as f32) / 29.)
        .collect()
}
fn query(db: &Database, q: &[f32], k: usize) -> Vec<i64> {
    query_as(db, q, k, "INDEXED")
}
/// `query` under the named USE VECTOR mode.
fn query_as(db: &Database, q: &[f32], k: usize, mode: &str) -> Vec<i64> {
    let r = db
        .execute(
            &format!("SELECT id FROM items ORDER BY embedding <=> $q USE VECTOR {mode} LIMIT {k}"),
            &HashMap::from([("q".into(), Value::Vector(q.to_vec()))]),
        )
        .unwrap();
    r.rows()
        .iter()
        .map(|r| match r[0] {
            Value::Int64(id) => id,
            _ => panic!("integer id"),
        })
        .collect()
}
fn fixture(db: &Database, d: usize, n: usize) {
    create(db, d, "");
    let tx = db.begin_or_panic();
    for i in 0..n {
        db.execute_in_tx(
            tx,
            "INSERT INTO items VALUES ($id,$v)",
            &HashMap::from([
                ("id".into(), Value::Int64(i as i64 + 1)),
                ("v".into(), Value::Vector(vector(i, d))),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    ready(db);
}
#[test]
#[ignore = "the vector index route is switched off in this release"]
fn a_page_read_does_not_stop_another_search_over_the_same_generation() {
    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(open(&root.path().join("concurrent.db")));
    fixture(&db, 768, 600);
    let q = vector(603, 768);
    let alone = query(&db, &q, 10);
    let parked = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let (p, r, d, q2) = (parked.clone(), release.clone(), db.clone(), q.clone());
    let thread = std::thread::spawn(move || {
        let once = AtomicBool::new(false);
        with_native_ivf_work_observer_for_test(
            Arc::new(move |point| {
                if point == NativeIvfWorkPoint::ListPageRead && !once.swap(true, Ordering::SeqCst) {
                    p.wait();
                    r.wait();
                }
            }),
            || query(&d, &q2, 10),
        )
    });
    parked.wait();
    let concurrent = query(&db, &q, 10);
    release.wait();
    let resumed = thread.join().unwrap();
    assert_eq!(concurrent, alone);
    assert_eq!(resumed, alone);
}
#[test]
#[ignore = "the vector index route is switched off in this release"]
fn a_parked_walk_and_the_insertions_beside_it_all_finish() {
    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(open(&root.path().join("insertion.db")));
    fixture(&db, 32, 150);
    let q = vector(77, 32);
    let parked = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let (p, r, d, q2) = (parked.clone(), release.clone(), db.clone(), q.clone());
    let thread = std::thread::spawn(move || {
        let once = AtomicBool::new(false);
        with_native_ivf_work_observer_for_test(
            Arc::new(move |point| {
                if point == NativeIvfWorkPoint::ListPageRead && !once.swap(true, Ordering::SeqCst) {
                    p.wait();
                    r.wait();
                }
            }),
            || query(&d, &q2, 10),
        )
    });
    parked.wait();
    insert(&db, 10000, vector(99, 32));
    insert(&db, 10001, vector(5, 32));
    release.wait();
    let answer = thread.join().unwrap();
    assert!(answer.len() >= 10);
    assert!(!answer.contains(&10000) && !answer.contains(&10001));
    let count = db.execute("SELECT COUNT(*) FROM items", &empty()).unwrap();
    assert_eq!(count.rows()[0][0], Value::Int64(152));
}
#[test]
#[ignore = "the vector index route is switched off in this release"]
fn one_buffer_generation_preserves_canonical_bytes_for_every_encoding() {
    for encoding in ["F32", "SQ8", "SQ4"] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("canonical.db");
        let db = open(&path);
        create(&db, 64, encoding);
        let tx = db.begin_or_panic();
        for ordinal in 0..192 {
            let v = (0..64)
                .map(|c| ((ordinal % 23 + c) % 31) as f32 / 31.)
                .collect();
            db.execute_in_tx(
                tx,
                "INSERT INTO items VALUES ($id,$v)",
                &HashMap::from([
                    ("id".into(), Value::Int64(192 - ordinal)),
                    ("v".into(), Value::Vector(v)),
                ]),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();
        insert(&db, 300, vec![0.25; 64]);
        let before = db
            .execute("SELECT id, embedding FROM items ORDER BY id", &empty())
            .unwrap()
            .rows()
            .to_vec();
        ready(&db);
        db.close().unwrap();
        drop(db);
        let db = open(&path);
        let after = db
            .execute("SELECT id, embedding FROM items ORDER BY id", &empty())
            .unwrap()
            .rows()
            .to_vec();
        assert_eq!(after, before, "canonical stored bytes for {encoding}");
        assert_eq!(after.len(), 193);
        db.close().unwrap();
    }
}
#[test]
#[ignore = "the vector index route is switched off in this release"]
fn durable_generation_refuses_corruption_and_incompatible_column_definition() {
    let root = tempfile::tempdir().unwrap();
    let db = open(&root.path().join("corruption.db"));
    fixture(&db, 3, 3);
    let index = VectorIndexRef::new("items", "embedding");
    let tuple = VectorPartitionKey::unpartitioned();
    let mut page = None;
    db.with_native_ivf_read_for_test(&index, &tuple, |scope| {
        scope.visit_physical_pages(&mut |p| {
            if p.key.kind == contextdb_vector::ivf::storage::PageKind::Codebook
                && p.key.ordinal == 1
            {
                page = Some((p.storage, p.key));
            }
        })
    })
    .unwrap();
    let (storage, key) = page.unwrap();
    db.corrupt_native_ivf_page_for_test(storage, key, true, 0, 1)
        .unwrap();
    let result = db.execute(
        "SELECT id FROM items ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1",
        &HashMap::from([("q".into(), Value::Vector(vec![1., 0., 0.]))]),
    );
    assert!(
        matches!(result, Err(Error::StoreCorrupted { .. })),
        "damaged generation must be refused: {result:?}"
    );
    let wrong = db.execute(
        "SELECT id FROM items ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1",
        &HashMap::from([("q".into(), Value::Vector(vec![1., 0., 0., 0.]))]),
    );
    assert!(wrong.is_err(), "incompatible dimension must refuse");
}
#[test]
#[ignore = "the vector index route is switched off in this release"]
fn mutable_quantized_canonical_insertion_without_rebuilding() {
    for quantization in ["SQ8", "SQ4"] {
        let root = tempfile::tempdir().unwrap();
        let db = open(&root.path().join("quantized.db"));
        create(&db, 3, quantization);
        insert(&db, 11, vec![1., 0., 0.]);
        ready(&db);
        let index = VectorIndexRef::new("items", "embedding");
        let tuple = VectorPartitionKey::unpartitioned();
        let before = db
            .native_ivf_serving_descriptor_for_test(&index, &tuple)
            .unwrap()
            .unwrap()
            .manifest
            .generation;
        insert(&db, 29, vec![0., 1., 0.]);
        assert_eq!(query(&db, &[0., 1., 0.], 1), [29]);
        assert_eq!(query(&db, &[0., 1., 0.], 2).len(), 2);
        let after = db
            .native_ivf_serving_descriptor_for_test(&index, &tuple)
            .unwrap()
            .unwrap()
            .manifest
            .generation;
        assert_eq!(before, after, "insertion does not rebuild {quantization}");
        db.close().unwrap();
    }
}
#[test]
fn predicate_search_for_every_canonical_encoding() {
    for quantization in ["F32", "SQ8", "SQ4"] {
        let root = tempfile::tempdir().unwrap();
        let db = open(&root.path().join("predicate.db"));
        create_as(&db, 3, quantization, "AUTO");
        insert(&db, 1, vec![1., 0., 0.]);
        insert(&db, 2, vec![0., 1., 0.]);
        for mode in ["EXACT", "AUTO"] {
            assert_eq!(
                query_as(&db, &[0.9, 0.1, 0.], 1, mode),
                [1],
                "{quantization} {mode}"
            );
            let result = db
                .execute(
                    &format!("SELECT id FROM items WHERE id = 99 ORDER BY embedding <=> $q USE VECTOR {mode} LIMIT 1"),
                    &HashMap::from([("q".into(), Value::Vector(vec![0.9, 0.1, 0.]))]),
                )
                .unwrap();
            assert!(result.rows().is_empty(), "{quantization} {mode}");
        }
        db.close().unwrap();
    }
}
/// The native observation of the predicate search above: absent candidates
/// receive no distance on the index route.
#[test]
#[ignore = "the vector index route is switched off in this release"]
fn indexed_predicate_search_gives_absent_candidates_no_distance() {
    for quantization in ["F32", "SQ8", "SQ4"] {
        let root = tempfile::tempdir().unwrap();
        let db = open(&root.path().join("predicate.db"));
        create(&db, 3, quantization);
        insert(&db, 1, vec![1., 0., 0.]);
        insert(&db, 2, vec![0., 1., 0.]);
        ready(&db);
        assert_eq!(query(&db, &[0.9, 0.1, 0.], 1), [1]);
        let distances = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = distances.clone();
        let result=with_native_ivf_work_observer_for_test(Arc::new(move|point|if point==NativeIvfWorkPoint::CandidateScoring{observed.fetch_add(1,Ordering::SeqCst);}),||db.execute("SELECT id FROM items WHERE id = 99 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1",&HashMap::from([("q".into(),Value::Vector(vec![0.9,0.1,0.]))]))).unwrap();
        assert!(result.rows().is_empty());
        assert_eq!(
            distances.load(Ordering::SeqCst),
            0,
            "absent candidates receive no distances"
        );
        db.close().unwrap();
    }
}

fn canonical_body_score(vector: &[f32], query: &[f32]) -> f32 {
    let (dot, aa, bb) =
        vector
            .iter()
            .zip(query)
            .fold((0.0_f64, 0.0_f64, 0.0_f64), |(dot, aa, bb), (&a, &b)| {
                let (a, b) = (f64::from(a), f64::from(b));
                (dot + a * b, aa + a * a, bb + b * b)
            });
    if aa == 0.0 || bb == 0.0 {
        0.0
    } else {
        (dot / (aa.sqrt() * bb.sqrt())) as f32
    }
}

/// Every public route publishes the canonical stored-body F64 rerank rounded
/// to F32. This companion makes the frozen checkpoint's Rust divergence RED.
#[test]
fn public_sql_and_rust_scores_publish_the_canonical_body_rerank() {
    use contextdb_core::VectorSearchMode;
    canonical_public_scores(
        "AUTO",
        &[
            ("EXACT", VectorSearchMode::Exact),
            ("AUTO", VectorSearchMode::Auto),
        ],
    );
}
/// The successful INDEXED arm of the canonical public score test above.
#[test]
#[ignore = "the vector index route is switched off in this release"]
fn indexed_sql_and_rust_scores_publish_the_canonical_body_rerank() {
    use contextdb_core::VectorSearchMode;
    canonical_public_scores("INDEXED", &[("INDEXED", VectorSearchMode::Indexed)]);
}
fn canonical_public_scores(search_mode: &str, modes: &[(&str, contextdb_core::VectorSearchMode)]) {
    use contextdb_engine::SemanticQuery;
    let root = tempfile::tempdir().unwrap();
    let db = open(&root.path().join("canonical-public-score.db"));
    let dimensions = 96;
    create_as(&db, dimensions, "F32", search_mode);
    for id in 1..=5 {
        insert(
            &db,
            id,
            (0..dimensions)
                .map(|c| (((c as u64 * 31 + id as u64 * 17) % 1021) as f32 / 1021.) - 0.5)
                .collect(),
        );
    }
    if search_mode == "INDEXED" {
        ready(&db);
    }
    let stored = db
        .execute("SELECT id, embedding FROM items ORDER BY id", &empty())
        .unwrap();
    let query = (0..dimensions)
        .map(|c| (((c * 23 + 5) % 1013) as f32 / 1013.) - 0.5)
        .collect::<Vec<_>>();
    let expected = stored
        .rows()
        .iter()
        .map(|row| {
            let (Value::Int64(id), Value::Vector(vector)) = (&row[0], &row[1]) else {
                panic!("stored canonical body and identity")
            };
            (*id, canonical_body_score(vector, &query).to_bits())
        })
        .collect::<HashMap<_, _>>();
    let mut failures = Vec::new();
    for &(name, mode) in modes {
        let result = db.execute(&format!("SELECT id, score FROM items ORDER BY embedding <=> $q USE VECTOR {name} LIMIT {}",expected.len()),&HashMap::from([("q".into(),Value::Vector(query.clone()))])).unwrap();
        assert_eq!(result.rows().len(), expected.len());
        for row in result.rows() {
            let (Value::Int64(id), Value::Float64(score)) = (&row[0], &row[1]) else {
                panic!("SQL publishes identity and score")
            };
            if (*score as f32).to_bits() != expected[id] {
                failures.push(format!(
                    "SQL {name} row={id} actual={} expected={}",
                    (*score as f32).to_bits(),
                    expected[id]
                ));
            }
        }
        let mut request = SemanticQuery::new("items", "embedding", query.clone(), expected.len());
        request.search_mode = Some(mode);
        match db.semantic_search(request) {
            Ok(rows) => {
                assert_eq!(rows.len(), expected.len());
                for row in rows {
                    let Some(Value::Int64(id)) = row.values.get("id") else {
                        panic!("Rust publishes identity")
                    };
                    if row.vector_score.to_bits() != expected[id] {
                        failures.push(format!(
                            "Rust {name} row={id} actual={} expected={}",
                            row.vector_score.to_bits(),
                            expected[id]
                        ));
                    }
                }
            }
            Err(error) => failures.push(format!(
                "Rust {name} must publish canonical score, refused: {error:?}"
            )),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    db.close().unwrap();
}
