//! Public SQL companions of the clustered-index unit-test ports. Maintenance
//! advances to its public ready report; no sleeps or cycle-count readiness.
#![cfg(feature = "test-seams")]
use contextdb_core::Value;
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;
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
    // A build that never settles fails with the cycle count and the last
    // report instead of hanging the binary.
    const MAX_CYCLES: usize = 4096;
    for cycle in 1..=MAX_CYCLES {
        let r = db.run_maintenance_cycle().unwrap();
        assert!(
            r.vector.first_failure.is_none(),
            "public maintenance failure: {:?}",
            r.vector
        );
        if r.vector.remaining_partitions == 0 && r.vector.ready_partitions > 0 {
            return;
        }
        assert!(
            cycle < MAX_CYCLES,
            "maintenance did not complete within {MAX_CYCLES} cycles: {:?}",
            r.vector
        );
    }
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
