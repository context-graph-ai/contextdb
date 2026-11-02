//! Ranking uses canonical cosine distance even when exposed f32 scores tie.
//! This release ships exact search only: EXACT and AUTO rank by that distance
//! on every door, and INDEXED is refused by name.
use contextdb_core::{Error, Value, VectorQuantization, VectorSearchMode};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy, SemanticQuery};
use std::collections::HashMap;

const REFUSAL: &str = "no vector index ships in this release; use EXACT or AUTO";

fn open(root: &tempfile::TempDir) -> Database {
    Database::open_with_options(
        root.path().join("precision.db"),
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap()
}

fn query_params(query: &[f32]) -> HashMap<String, Value> {
    HashMap::from([("q".to_string(), Value::Vector(query.to_vec()))])
}

fn row_params(id: i64, scope: i64, vector: &[f32]) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_string(), Value::Int64(id)),
        ("scope".to_string(), Value::Int64(scope)),
        ("v".to_string(), Value::Vector(vector.to_vec())),
    ])
}

fn sql_ids(result: &contextdb_engine::QueryResult) -> Vec<Value> {
    result.rows().iter().map(|row| row[0].clone()).collect()
}

fn semantic_ids(results: &[contextdb_engine::SearchResult]) -> Vec<Value> {
    results
        .iter()
        .map(|result| result.values.get("id").cloned().unwrap())
        .collect()
}

fn semantic(table: &str, query: &[f32], limit: usize, mode: VectorSearchMode) -> SemanticQuery {
    SemanticQuery {
        search_mode: Some(mode),
        ..SemanticQuery::new(table, "v", query.to_vec(), limit)
    }
}

/// The canonical arithmetic, written out independently: f64 per coordinate,
/// zero norm is distance 1.0.
fn reference_distance(a: &[f32], b: &[f32]) -> f64 {
    let (mut dot, mut aa, mut bb) = (0.0_f64, 0.0_f64, 0.0_f64);
    for (&x, &y) in a.iter().zip(b) {
        let (x, y) = (f64::from(x), f64::from(y));
        dot += x * y;
        aa += x * x;
        bb += y * y;
    }
    if aa == 0.0 || bb == 0.0 {
        1.0
    } else {
        1.0 - dot / (aa.sqrt() * bb.sqrt())
    }
}

fn assert_refused(result: contextdb_core::Result<impl std::fmt::Debug>, door: &str) {
    match result {
        Err(error @ Error::VectorIndexedRouteUnavailable { .. }) => {
            assert_eq!(error.to_string(), REFUSAL, "{door}");
        }
        other => panic!("{door}: INDEXED must be refused by name, got {other:?}"),
    }
}

#[test]
fn native_queries_keep_canonical_precision_when_display_scores_tie() {
    for partitioned in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let db = open(&root);
        let partition = if partitioned {
            "PARTITION_KEY (scope)"
        } else {
            ""
        };
        db.execute(&format!("CREATE TABLE vectors (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(2) {partition} SEARCH_MODE INDEXED IVF (LISTS = 1, PROBES = 1, REFINE_FACTOR = 2.00) WITH (training_samples = 1, training_iterations = 1))"),&HashMap::new()).unwrap();
        for (id, axis) in [(1, 0.0002_f32), (2, 0.0001_f32)] {
            db.execute(
                "INSERT INTO vectors VALUES ($id,$scope,$v)",
                &row_params(id, if partitioned { id } else { 0 }, &[1.0, axis]),
            )
            .unwrap();
        }
        let mut ready = false;
        for _ in 0..16 {
            if db
                .run_maintenance_cycle()
                .unwrap()
                .vector
                .remaining_partitions
                == 0
            {
                ready = true;
                break;
            }
        }
        assert!(
            ready,
            "both one-vector native families finish their finite build"
        );
        for mode in ["EXACT", "AUTO"] {
            for limit in [2, 1] {
                let result = db
                    .execute(
                        &format!(
                            "SELECT id FROM vectors ORDER BY v <=> $q USE VECTOR {mode} LIMIT {limit}"
                        ),
                        &query_params(&[1.0, 0.0]),
                    )
                    .unwrap();
                assert_eq!(
                    sql_ids(&result),
                    vec![Value::Int64(2), Value::Int64(1)][..limit].to_vec(),
                    "{mode} LIMIT {limit}, partitioned={partitioned}: canonical distances differ even though their displayed f32 scores are equal"
                );
            }
        }
        assert_refused(
            db.execute(
                "SELECT id FROM vectors ORDER BY v <=> $q USE VECTOR INDEXED LIMIT 2",
                &query_params(&[1.0, 0.0]),
            ),
            "USE VECTOR INDEXED",
        );
        db.close().unwrap();
    }
}

/// Displayed scores: the parent's native route displayed (1.0 - distance) as
/// f32. These bit patterns were read from parent 88a33059 on this fixture.
const PARENT_DISPLAY_BITS: [(i64, u32); 3] = [(3, 0x3f80_0000), (1, 0x3f19_999a), (2, 0x0000_0000)];

#[test]
fn displayed_scores_keep_the_native_parent_bits() {
    let root = tempfile::tempdir().unwrap();
    let db = open(&root);
    db.execute(
        "CREATE TABLE bits (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(2))",
        &HashMap::new(),
    )
    .unwrap();
    // A nontrivial score (0.6), a zero stored vector, and a near-one score.
    for (id, vector) in [(1, [0.6_f32, 0.8]), (2, [0.0, 0.0]), (3, [1.0, 0.0001])] {
        db.execute(
            "INSERT INTO bits VALUES ($id,$scope,$v)",
            &row_params(id, 0, &vector),
        )
        .unwrap();
    }
    for mode in [VectorSearchMode::Exact, VectorSearchMode::Auto] {
        let results = db
            .semantic_search(semantic("bits", &[1.0, 0.0], 3, mode))
            .unwrap();
        let bits: Vec<(i64, u32)> = results
            .iter()
            .map(|result| {
                let Some(Value::Int64(id)) = result.values.get("id") else {
                    panic!("an id column");
                };
                assert_eq!(result.rank.to_bits(), result.vector_score.to_bits());
                (*id, result.vector_score.to_bits())
            })
            .collect();
        assert_eq!(
            bits,
            PARENT_DISPLAY_BITS.to_vec(),
            "{mode:?}: displayed scores are bit-identical to the native parent's"
        );
    }
}

#[test]
fn quantized_near_ties_rank_by_their_stored_coordinates() {
    // Challenge 133's witness: against [1, 1] the f32 comparison scores both
    // rows 1.0. The third coordinate takes interior quantization codes, so the
    // stored decoder's own expression decides those coordinates.
    let rows: [(i64, [f32; 3]); 3] = [
        (1, [1.0, 0.9998, 0.99991]),
        (2, [1.0, 0.9999, 0.99987]),
        (3, [1.0, 0.99985, 0.99993]),
    ];
    let query = [1.0_f32, 1.0, 1.0];
    for (quantization, name) in [
        (VectorQuantization::SQ8, "SQ8"),
        (VectorQuantization::SQ4, "SQ4"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let db = open(&root);
        db.execute(
            &format!("CREATE TABLE sq (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(3) WITH (quantization = '{name}'))"),
            &HashMap::new(),
        )
        .unwrap();
        for (id, vector) in &rows {
            db.execute(
                "INSERT INTO sq VALUES ($id,$scope,$v)",
                &row_params(*id, 0, vector),
            )
            .unwrap();
        }
        // The oracle ranks what the store holds, with the reference arithmetic.
        let mut expected: Vec<(i64, f64)> = rows
            .iter()
            .map(|(id, vector)| {
                let stored = contextdb_vector::stored_vector_value(vector, quantization);
                (*id, 1.0 - reference_distance(&query, &stored))
            })
            .collect();
        expected.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        let expected: Vec<Value> = expected.iter().map(|(id, _)| Value::Int64(*id)).collect();
        for mode in [VectorSearchMode::Exact, VectorSearchMode::Auto] {
            for limit in [1, 3] {
                let results = db
                    .semantic_search(semantic("sq", &query, limit, mode))
                    .unwrap();
                assert_eq!(
                    semantic_ids(&results),
                    expected[..limit].to_vec(),
                    "{name} {mode:?} LIMIT {limit}: rows rank by canonical distance over their stored coordinates"
                );
            }
        }
        let results = db
            .semantic_search(semantic("sq", &query, 3, VectorSearchMode::Exact))
            .unwrap();
        assert!(
            results
                .windows(2)
                .any(|pair| pair[0].vector_score == pair[1].vector_score),
            "{name}: the fixture is a trap only if some displayed scores tie"
        );
    }
}

/// Committed and staged candidates in the near-tie trap, both directions, a
/// replaced (moved) row, one global LIMIT across two partitions, through the
/// in-transaction Rust door and the in-transaction SQL door.
#[test]
fn transaction_overlays_rank_committed_and_staged_rows_by_canonical_distance() {
    const NEAR: [f32; 2] = [1.0, 0.0001];
    const FAR: [f32; 2] = [1.0, 0.0002];
    let query = [1.0_f32, 0.0];
    // (staged with SQL, committed id and vector, staged id and vector)
    for sql_staging in [true, false] {
        for (committed, staged) in [((2, NEAR), (1, FAR)), ((1, FAR), (2, NEAR))] {
            let root = tempfile::tempdir().unwrap();
            let db = open(&root);
            db.execute(
                "CREATE TABLE overlay (id INT PRIMARY KEY, scope INT NOT NULL, tag TEXT, v VECTOR(2) PARTITION_KEY (scope))",
                &HashMap::new(),
            )
            .unwrap();
            db.execute(
                "INSERT INTO overlay (id, scope, v) VALUES ($id,$scope,$v)",
                &row_params(committed.0, committed.0, &committed.1),
            )
            .unwrap();
            let before = sql_ids(
                &db.execute(
                    "SELECT id FROM overlay ORDER BY v <=> $q USE VECTOR EXACT LIMIT 2",
                    &query_params(&query),
                )
                .unwrap(),
            );
            let expected = [Value::Int64(2), Value::Int64(1)];
            for rollback in [true, false] {
                let tx = db.begin().unwrap();
                if sql_staging {
                    db.execute_in_tx(
                        tx,
                        "INSERT INTO overlay (id, scope, v) VALUES ($id,$scope,$v)",
                        &row_params(staged.0, staged.0, &staged.1),
                    )
                    .unwrap();
                    // Replace the committed row inside the transaction: its
                    // vector moves to the new row version.
                    let mut replace =
                        HashMap::from([("id".to_string(), Value::Int64(committed.0))]);
                    replace.insert("tag".to_string(), Value::Text("moved".to_string()));
                    db.execute_in_tx(tx, "UPDATE overlay SET tag = $tag WHERE id = $id", &replace)
                        .unwrap();
                } else {
                    db.insert_row(
                        tx,
                        "overlay",
                        HashMap::from([
                            ("id".to_string(), Value::Int64(staged.0)),
                            ("scope".to_string(), Value::Int64(staged.0)),
                            ("v".to_string(), Value::Vector(staged.1.to_vec())),
                        ]),
                    )
                    .unwrap();
                }
                for limit in [1, 2] {
                    let rust = db
                        .__semantic_search_in_tx_for_test(
                            tx,
                            semantic("overlay", &query, limit, VectorSearchMode::Exact),
                        )
                        .unwrap();
                    assert_eq!(
                        semantic_ids(&rust),
                        expected[..limit].to_vec(),
                        "Rust door, sql_staging={sql_staging}, committed={}, LIMIT {limit}",
                        committed.0
                    );
                    for mode in ["EXACT", "AUTO"] {
                        let sql = db
                            .execute_in_tx(
                                tx,
                                &format!(
                                    "SELECT id FROM overlay ORDER BY v <=> $q USE VECTOR {mode} LIMIT {limit}"
                                ),
                                &query_params(&query),
                            )
                            .unwrap();
                        assert_eq!(
                            sql_ids(&sql),
                            expected[..limit].to_vec(),
                            "SQL door {mode}, sql_staging={sql_staging}, committed={}, LIMIT {limit}",
                            committed.0
                        );
                    }
                }
                if rollback {
                    db.rollback(tx).unwrap();
                    // Rollback restores the committed state, without the
                    // staged row.
                    let after = sql_ids(
                        &db.execute(
                            "SELECT id FROM overlay ORDER BY v <=> $q USE VECTOR EXACT LIMIT 2",
                            &query_params(&query),
                        )
                        .unwrap(),
                    );
                    assert_eq!(after, before, "rollback restores the committed answer");
                    assert_eq!(after, vec![Value::Int64(committed.0)]);
                } else {
                    db.commit(tx).unwrap();
                    for limit in [1, 2] {
                        let after = sql_ids(
                            &db.execute(
                                &format!(
                                    "SELECT id FROM overlay ORDER BY v <=> $q USE VECTOR EXACT LIMIT {limit}"
                                ),
                                &query_params(&query),
                            )
                            .unwrap(),
                        );
                        assert_eq!(
                            after,
                            expected[..limit].to_vec(),
                            "after commit, LIMIT {limit}"
                        );
                    }
                }
            }
            db.close().unwrap();
        }
    }
}
